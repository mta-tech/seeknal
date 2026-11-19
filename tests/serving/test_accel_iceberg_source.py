"""Tests for seeknal.serving.accel.iceberg_source and end-to-end Iceberg caching."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid

import duckdb
import pytest
import yaml

from seeknal.serving.accel.__main__ import main as accel_main
from seeknal.serving.accel.iceberg_source import (
    IcebergSource,
    IcebergSourceConfig,
    build_iceberg_table_specs,
    create_iceberg_target_factory,
    execute_iceberg_source_setup,
    load_iceberg_source_config,
    plan_iceberg_count_sql,
    plan_iceberg_load_sql,
    plan_iceberg_source_setup,
)
from seeknal.serving.accel.loader import (
    BadInputError,
    LoadError,
    RowCountMismatchError,
    clear_registered_secrets,
    run_load,
    scrub_text,
)
from seeknal.serving.duckdb_policy import connect_hardened, probe


def _make_fake_secrets() -> dict[str, str]:
    tag = uuid.uuid4().hex[:12]
    return {
        "TEST_LK_CLIENT_ID": f"FAKE_OAUTH_ID_{tag}",
        "TEST_LK_CLIENT_SECRET": f"FAKE_OAUTH_SECRET_{tag}",
        "TEST_S3_KEY_ID": f"FAKE_S3_KID_{tag}",
        "TEST_S3_SECRET": f"FAKE_S3_SEC_{tag}",
    }


def _sample_iceberg_dict(
    ext_dir: Path,
    *,
    alias: str = "lake",
    warehouse: str = "analytics_wh",
    access_delegation: str = "vended",
    url_style: str = "path",
    snapshot_id: int | None = 8877665544332211,
    secrets_env_file: str | None = None,
) -> dict:
    cfg: dict = {
        "extension_directory": str(ext_dir),
        "extensions": ["httpfs", "iceberg"],
        "allowed_s3_prefixes": ["s3://synthetic-warehouse/curated/"],
        "iceberg_catalogs": {
            alias: {
                "endpoint": "https://lakekeeper.synthetic.internal/catalog",
                "warehouse": warehouse,
                "token_uri": "https://idp.synthetic.internal/oauth2/token",
                "client_id_env": "TEST_LK_CLIENT_ID",
                "client_secret_env": "TEST_LK_CLIENT_SECRET",
                "scope": "catalog:read",
                "access_delegation": access_delegation,
                "s3": {
                    "endpoint": "s3.synthetic.internal:9000",
                    "region": "ap-southeast-1",
                    "url_style": url_style,
                    "key_id_env": "TEST_S3_KEY_ID",
                    "secret_env": "TEST_S3_SECRET",
                    "use_ssl": True,
                },
            }
        },
        "pii_columns": {
            "customer_email": {
                "source_form": "sha256",
                "mask": "redact",
            }
        },
        "tables": [
            {
                "view": "daily_orders",
                "source": f"{alias}.analytics.daily_orders",
                "snapshot_id": snapshot_id,
                "keys": ["order_id"],
                "columns": [
                    {"name": "order_id", "type": "bigint"},
                    {"name": "order_date", "type": "date"},
                    {"name": "net_amount", "type": "decimal(18,2)"},
                    {
                        "name": "customer_email",
                        "type": "varchar",
                        "pii": "customer_email",
                    },
                ],
            }
        ],
    }
    if secrets_env_file is not None:
        cfg["secrets_env_file"] = secrets_env_file
    return cfg


def test_plan_iceberg_source_setup_strict_ordering_and_redaction(tmp_path):
    clear_registered_secrets()
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    secrets = _make_fake_secrets()

    try:
        cfg_dict = _sample_iceberg_dict(ext_dir)
        steps = plan_iceberg_source_setup(cfg_dict, env=secrets)

        names = [s.name for s in steps]
        assert names == [
            "set_autoinstall_off",
            "set_autoload_off",
            "set_allow_persistent_secrets_off",
            "set_extension_directory",
            "set_custom_extension_repository",
            "load_extension_httpfs",
            "load_extension_iceberg",
            "create_secret_s3_lake",
            "create_secret_iceberg_lake",
            "attach_lake",
        ]

        # Exact SQL checks for ordering before ATTACH
        assert steps[0].sql == "SET autoinstall_known_extensions = false"
        assert steps[1].sql == "SET autoload_known_extensions = false"
        assert steps[2].sql == "SET allow_persistent_secrets = false"
        assert steps[3].sql == f"SET extension_directory = '{ext_dir}'"
        assert (
            steps[4].sql
            == "SET custom_extension_repository = '/nonexistent_extension_repository'"
        )
        assert steps[5].sql == "LOAD httpfs"
        assert steps[6].sql == "LOAD iceberg"

        # NEVER emits INSTALL statement
        for s in steps:
            assert not s.sql.lstrip().upper().startswith("INSTALL")

        # Secrets are TEMPORARY, never PERSISTENT
        s3_step = steps[7]
        ice_step = steps[8]
        assert s3_step.sql.startswith("CREATE TEMPORARY SECRET s3_lake (TYPE S3, ")
        assert ice_step.sql.startswith(
            "CREATE TEMPORARY SECRET iceberg_lake (TYPE ICEBERG, "
        )
        assert "PERSISTENT" not in s3_step.sql.upper()
        assert "PERSISTENT" not in ice_step.sql.upper()

        # ATTACH is strictly after LOAD and CREATE TEMPORARY SECRET, and is READ_ONLY
        attach_step = steps[9]
        assert attach_step.sql == (
            "ATTACH 'analytics_wh' AS lake ("
            "TYPE ICEBERG, "
            "ENDPOINT 'https://lakekeeper.synthetic.internal/catalog', "
            "SECRET iceberg_lake, "
            "ACCESS_DELEGATION_MODE 'vended-credentials', "
            "READ_ONLY)"
        )

        # Secret values NEVER appear in safe_sql, redacted_sql, repr(step), or scrub_text
        for step in steps:
            for sec_val in secrets.values():
                assert sec_val not in step.safe_sql
                assert sec_val not in repr(step)
                if step.redacted_sql is not None:
                    assert sec_val not in step.redacted_sql

        for sec_val in secrets.values():
            assert sec_val not in scrub_text(f"Error containing {sec_val} in message")
    finally:
        clear_registered_secrets()


def test_validation_and_quoting_rules_match_iceberg_direct(tmp_path):
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    secrets = _make_fake_secrets()

    # 1. Invalid & reserved catalog aliases rejected
    for bad_alias in ("BadUpper", "1lake", "lake-dash", "lake;drop", "system", "temp", "memory", "main"):
        bad = _sample_iceberg_dict(ext_dir, alias=bad_alias)
        with pytest.raises(BadInputError, match="alias"):
            IcebergSourceConfig.from_dict(bad)

    # 2. Endpoint must start with http:// or https://
    bad_ep = _sample_iceberg_dict(ext_dir)
    bad_ep["iceberg_catalogs"]["lake"]["endpoint"] = "ftp://bad.internal/catalog"
    with pytest.raises(BadInputError, match="http:// or https://"):
        IcebergSourceConfig.from_dict(bad_ep)

    # 3. Warehouse must be non-empty; single quotes are SQL-escaped
    empty_wh = _sample_iceberg_dict(ext_dir, warehouse="   ")
    with pytest.raises(BadInputError, match="warehouse"):
        IcebergSourceConfig.from_dict(empty_wh)

    quoted_wh = _sample_iceberg_dict(ext_dir, warehouse="wh'with'quotes", access_delegation="none", url_style="vhost")
    steps = plan_iceberg_source_setup(quoted_wh, env=secrets)
    attach_sql = steps[-1].sql
    assert "ATTACH 'wh''with''quotes' AS lake" in attach_sql
    assert "ACCESS_DELEGATION_MODE 'none'" in attach_sql
    assert "URL_STYLE 'vhost'" in steps[7].safe_sql

    # 4. Invalid access_delegation & url_style rejected
    bad_del = _sample_iceberg_dict(ext_dir, access_delegation="invalid_mode")
    with pytest.raises(BadInputError, match="access_delegation"):
        IcebergSourceConfig.from_dict(bad_del)

    bad_style = _sample_iceberg_dict(ext_dir, url_style="bucket")
    with pytest.raises(BadInputError, match="url_style"):
        IcebergSourceConfig.from_dict(bad_style)

    # 5. Literal secret keys and Postgres/attach keys rejected without echoing secret values
    literal_leak = f"LITERAL_SECRET_DO_NOT_ECHO_{uuid.uuid4().hex}"
    bad_cat_sec = _sample_iceberg_dict(ext_dir)
    bad_cat_sec["iceberg_catalogs"]["lake"]["client_secret"] = literal_leak
    with pytest.raises(BadInputError) as exc_info_1:
        IcebergSourceConfig.from_dict(bad_cat_sec)
    assert literal_leak not in str(exc_info_1.value)

    bad_s3_sec = _sample_iceberg_dict(ext_dir)
    bad_s3_sec["iceberg_catalogs"]["lake"]["s3"]["secret"] = literal_leak
    with pytest.raises(BadInputError) as exc_info_2:
        IcebergSourceConfig.from_dict(bad_s3_sec)
    assert literal_leak not in str(exc_info_2.value)

    bad_pg = _sample_iceberg_dict(ext_dir)
    bad_pg["postgres_dsn"] = literal_leak
    with pytest.raises(BadInputError) as exc_info_3:
        IcebergSourceConfig.from_dict(bad_pg)
    assert literal_leak not in str(exc_info_3.value)
    clear_registered_secrets()


def test_secrets_env_file_mode_0600_enforced(tmp_path):
    clear_registered_secrets()
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    secrets = _make_fake_secrets()

    env_file = tmp_path / "iceberg.env"
    env_lines = "\n".join(f"{k}={v}" for k, v in secrets.items()) + "\n"
    env_file.write_text(env_lines, encoding="utf-8")

    # Group/world readable mode 0644 is rejected
    os.chmod(env_file, 0o644)
    cfg_raw = _sample_iceberg_dict(ext_dir, secrets_env_file=str(env_file))
    with pytest.raises(BadInputError, match="0600"):
        IcebergSourceConfig.from_dict(cfg_raw)

    # Mode 0600 succeeds and resolves secrets without needing process os.environ
    os.chmod(env_file, 0o600)
    try:
        parsed = IcebergSourceConfig.from_dict(cfg_raw)
        steps = plan_iceberg_source_setup(parsed, env={})
        assert len(steps) == 10
        for sec_val in secrets.values():
            assert sec_val not in steps[7].safe_sql
            assert sec_val not in steps[8].safe_sql
    finally:
        clear_registered_secrets()


def test_plan_iceberg_load_and_count_sql_with_snapshot_pin_validates_in_duckdb_parser(tmp_path):
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    cfg = IcebergSourceConfig.from_dict(
        _sample_iceberg_dict(ext_dir, snapshot_id=8877665544332211)
    )
    specs_null = build_iceberg_table_specs(cfg, pii_mode="null")
    specs_excl = build_iceberg_table_specs(cfg, pii_mode="exclude")

    load_null = plan_iceberg_load_sql(
        specs_null, pii_mode="null", allowed_aliases={"lake"}
    )
    assert load_null == [
        "CREATE SCHEMA rpt",
        "CREATE TABLE rpt.daily_orders AS SELECT order_id, order_date, net_amount, CAST(NULL AS VARCHAR) AS customer_email FROM lake.analytics.daily_orders AT (VERSION => 8877665544332211)",
    ]

    load_excl = plan_iceberg_load_sql(
        specs_excl, pii_mode="exclude", allowed_aliases={"lake"}
    )
    assert load_excl == [
        "CREATE SCHEMA rpt",
        "CREATE TABLE rpt.daily_orders AS SELECT order_id, order_date, net_amount FROM lake.analytics.daily_orders AT (VERSION => 8877665544332211)",
    ]

    count_sql = plan_iceberg_count_sql(specs_null[0], allowed_aliases={"lake"})
    assert (
        count_sql
        == "SELECT COUNT(*) FROM lake.analytics.daily_orders AT (VERSION => 8877665544332211)"
    )

    # Verify DuckDB 2.0.0 parser accepts every generated SQL statement
    con = duckdb.connect(":memory:")
    try:
        for stmt in (*load_null, *load_excl, count_sql):
            parsed_stmts = con.extract_statements(stmt)
            assert len(parsed_stmts) == 1
    finally:
        con.close()


def _create_standin_iceberg_catalog(
    tmp_path: Path, pii_sentinel: str, pinned_snap_id: int = 700100
) -> Path:
    """Create a local DuckDB stand-in catalog simulating latest (5 rows) and pinned snapshot (3 rows)."""
    standin_path = tmp_path / "lake_standin.duckdb"
    conn = duckdb.connect(str(standin_path))
    conn.execute("CREATE SCHEMA analytics")

    # Latest table has 5 rows
    conn.execute(
        "CREATE TABLE analytics.daily_orders ("
        "order_id BIGINT, order_date DATE, net_amount DECIMAL(18,2), customer_email VARCHAR"
        ")"
    )
    conn.execute(
        "INSERT INTO analytics.daily_orders VALUES "
        "(1, DATE '2026-11-18', 100.00, ?), "
        "(2, DATE '2026-11-18', 200.50, ?), "
        "(3, DATE '2026-11-18', 300.25, ?), "
        "(4, DATE '2026-11-19', 400.00, ?), "
        "(5, DATE '2026-11-19', 500.75, ?)",
        [pii_sentinel] * 5,
    )

    # Pinned historical snapshot has 3 rows
    conn.execute(
        f"CREATE TABLE analytics.daily_orders__snap_{pinned_snap_id} AS "
        "SELECT * FROM analytics.daily_orders WHERE order_id <= 3"
    )
    conn.close()
    return standin_path


def test_end_to_end_offline_standin_snapshot_pin_readonly_pii_and_serving_probe(
    tmp_path, capsys
):
    clear_registered_secrets()
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    secrets = _make_fake_secrets()
    pii_sentinel = f"PII_ICEBERG_EMAIL_{uuid.uuid4().hex}@synthetic.test"
    pinned_snap = 700100
    standin_file = _create_standin_iceberg_catalog(
        tmp_path, pii_sentinel=pii_sentinel, pinned_snap_id=pinned_snap
    )
    standin_map = {"lake": str(standin_file)}

    try:
        cfg = IcebergSourceConfig.from_dict(
            _sample_iceberg_dict(ext_dir, snapshot_id=pinned_snap)
        )

        # Verify attached stand-in catalog is READ_ONLY and secrets are TEMPORARY only
        check_conn = duckdb.connect(":memory:")
        try:
            execute_iceberg_source_setup(
                check_conn,
                cfg,
                env=secrets,
                standin_catalogs=standin_map,
            )
            dbs = dict(
                check_conn.execute(
                    "SELECT database_name, readonly FROM duckdb_databases()"
                ).fetchall()
            )
            assert dbs["lake"] is True

            sec_rows = check_conn.execute(
                "SELECT name, persistent FROM duckdb_secrets()"
            ).fetchall()
            assert {r[0] for r in sec_rows} == {"s3_lake", "iceberg_lake"}
            assert all(r[1] is False for r in sec_rows)

            with pytest.raises(duckdb.Error):
                check_conn.execute("CREATE TABLE lake.analytics.illegal_write (x INT)")
        finally:
            check_conn.close()

        # 1. End-to-end load with pii_mode="null" and snapshot_id=700100 (3 rows, not 5!)
        cache_null = tmp_path / "cache_null"
        specs_null = build_iceberg_table_specs(cfg, pii_mode="null")
        source_null = IcebergSource(
            cfg,
            env=secrets,
            standin_catalogs=standin_map,
        )
        factory_null = create_iceberg_target_factory(
            cfg,
            env=secrets,
            standin_catalogs=standin_map,
        )

        out_null = run_load(
            specs=specs_null,
            source=source_null,
            target_factory=factory_null,
            cache_dir=cache_null,
            clock=lambda: datetime(2026, 11, 19, 14, 0, 0, tzinfo=timezone.utc),
            as_of_date="2026-11-19",
            pii_mode="null",
            source_type="iceberg",
        )
        assert source_null.executed_count_sqls == [
            f"SELECT COUNT(*) FROM lake.analytics.daily_orders AT (VERSION => {pinned_snap})"
        ]
        source_null.close()

        # Verify freshness.json records exact snapshot_id and 3 rows
        freshness_text = (cache_null / "freshness.json").read_text(encoding="utf-8")
        freshness = json.loads(freshness_text)
        assert freshness["tables"] == {"rpt.daily_orders": 3}
        assert freshness["snapshots"] == {"rpt.daily_orders": pinned_snap}
        assert freshness["as_of_date"] == "2026-11-19"
        assert freshness["duckdb_version"] == duckdb.__version__

        # Verify PII value is absent from query results AND raw file bytes
        assert pii_sentinel.encode("utf-8") not in out_null.read_bytes()

        # Verify output mart.duckdb opens cleanly under Serving's connect_hardened + probe!
        hardened_conn = connect_hardened(
            str(cache_null / "mart.duckdb"),
            read_only=True,
            allowed_directories=[f"{cache_null.resolve()}/"],
        )
        try:
            probe_report = probe(hardened_conn)
            assert all(probe_report.values()), f"Serving probe failed: {probe_report}"
            rows = hardened_conn.execute(
                "SELECT order_id, customer_email FROM rpt.daily_orders ORDER BY order_id"
            ).fetchall()
            assert rows == [(1, None), (2, None), (3, None)]
        finally:
            hardened_conn.close()

        # 2. End-to-end load with pii_mode="exclude"
        cache_excl = tmp_path / "cache_excl"
        specs_excl = build_iceberg_table_specs(cfg, pii_mode="exclude")
        source_excl = IcebergSource(
            cfg,
            env=secrets,
            standin_catalogs=standin_map,
        )
        factory_excl = create_iceberg_target_factory(
            cfg,
            env=secrets,
            standin_catalogs=standin_map,
        )
        out_excl = run_load(
            specs=specs_excl,
            source=source_excl,
            target_factory=factory_excl,
            cache_dir=cache_excl,
            clock=lambda: datetime(2026, 11, 19, 14, 15, 0, tzinfo=timezone.utc),
            pii_mode="exclude",
            source_type="iceberg",
        )
        source_excl.close()

        assert pii_sentinel.encode("utf-8") not in out_excl.read_bytes()
        ro_excl = duckdb.connect(str(cache_excl / "mart.duckdb"), read_only=True)
        try:
            cols = [
                r[0] for r in ro_excl.execute("DESCRIBE rpt.daily_orders").fetchall()
            ]
            assert cols == ["order_id", "order_date", "net_amount"]
            assert "customer_email" not in cols
        finally:
            ro_excl.close()

        # Verify secrets are completely absent from stderr logs and freshness.json
        stderr_all = capsys.readouterr().err
        for sec_val in secrets.values():
            assert sec_val not in stderr_all
            assert sec_val not in freshness_text
    finally:
        clear_registered_secrets()


def test_iceberg_row_count_mismatch_and_setup_failure_never_leak_secrets_or_swap(
    tmp_path, capsys
):
    clear_registered_secrets()
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    secrets = _make_fake_secrets()
    standin_file = _create_standin_iceberg_catalog(
        tmp_path, pii_sentinel="p@synthetic.test", pinned_snap_id=700100
    )
    standin_map = {"lake": str(standin_file)}
    cache_dir = tmp_path / "cache"

    try:
        cfg = IcebergSourceConfig.from_dict(
            _sample_iceberg_dict(ext_dir, snapshot_id=700100)
        )
        specs = build_iceberg_table_specs(cfg, pii_mode="exclude")

        # Initial valid load
        src_ok = IcebergSource(cfg, env=secrets, standin_catalogs=standin_map)
        fac_ok = create_iceberg_target_factory(
            cfg, env=secrets, standin_catalogs=standin_map
        )
        initial_file = run_load(
            specs=specs,
            source=src_ok,
            target_factory=fac_ok,
            cache_dir=cache_dir,
            clock=lambda: datetime(2026, 11, 19, 15, 0, 0, tzinfo=timezone.utc),
            pii_mode="exclude",
            source_type="iceberg",
        )
        src_ok.close()

        # Mismatched Iceberg source row count aborts without updating symlink
        class MismatchedIcebergSource(IcebergSource):
            def get_row_count(self, table_or_view: str, snapshot_id: int | None = None) -> int:
                return 99999

        bad_src = MismatchedIcebergSource(cfg, env=secrets, standin_catalogs=standin_map)
        with pytest.raises(RowCountMismatchError):
            run_load(
                specs=specs,
                source=bad_src,
                target_factory=fac_ok,
                cache_dir=cache_dir,
                clock=lambda: datetime(2026, 11, 19, 15, 30, 0, tzinfo=timezone.utc),
                pii_mode="exclude",
                source_type="iceberg",
            )
        bad_src.close()

        assert os.readlink(cache_dir / "mart.duckdb") == initial_file.name
        assert list(cache_dir.glob("mart-*.duckdb")) == [initial_file]

        # Setup failure (missing un-staged httpfs/iceberg extensions without stand-in)
        # must fail cleanly, clean up temp file, keep symlink, and NEVER leak secrets!
        fac_fail = create_iceberg_target_factory(cfg, env=secrets, standin_catalogs=None)
        with pytest.raises(LoadError) as exc_info:
            run_load(
                specs=specs,
                source=src_ok,
                target_factory=fac_fail,
                cache_dir=cache_dir,
                clock=lambda: datetime(2026, 11, 19, 16, 0, 0, tzinfo=timezone.utc),
                pii_mode="exclude",
                source_type="iceberg",
            )

        err_msg = str(exc_info.value)
        stderr_logs = capsys.readouterr().err
        for sec_val in secrets.values():
            assert sec_val not in err_msg
            assert sec_val not in stderr_logs

        assert os.readlink(cache_dir / "mart.duckdb") == initial_file.name
        assert list(cache_dir.glob("mart-*.duckdb")) == [initial_file]
    finally:
        clear_registered_secrets()


def test_cli_iceberg_dry_run_yaml_and_json_redacts_secrets(
    tmp_path, monkeypatch, capsys
):
    clear_registered_secrets()
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    secrets = _make_fake_secrets()
    for k, v in secrets.items():
        monkeypatch.setenv(k, v)

    try:
        cfg_dict = _sample_iceberg_dict(ext_dir, snapshot_id=5544332211)
        yaml_cfg = tmp_path / "iceberg.yaml"
        json_cfg = tmp_path / "iceberg.json"
        yaml_cfg.write_text(yaml.safe_dump(cfg_dict), encoding="utf-8")
        json_cfg.write_text(json.dumps(cfg_dict), encoding="utf-8")

        cache_dir = tmp_path / "dry_cache"

        for cfg_file, mode in ((yaml_cfg, "exclude"), (json_cfg, "null")):
            loaded = load_iceberg_source_config(cfg_file)
            assert "lake" in loaded.iceberg_catalogs

            rc = accel_main(
                [
                    "load",
                    "--source-type",
                    "iceberg",
                    "--source-config",
                    str(cfg_file),
                    "--cache-dir",
                    str(cache_dir),
                    "--pii-mode",
                    mode,
                    "--dry-run",
                ]
            )
            assert rc == 0
            captured = capsys.readouterr()
            assert "SET autoinstall_known_extensions = false" in captured.out
            assert "LOAD httpfs" in captured.out
            assert "LOAD iceberg" in captured.out
            assert "CREATE TEMPORARY SECRET s3_lake" in captured.out
            assert "'<redacted>'" in captured.out
            assert "AT (VERSION => 5544332211)" in captured.out
            if mode == "exclude":
                assert "customer_email" not in captured.out
            else:
                assert "CAST(NULL AS VARCHAR) AS customer_email" in captured.out

            for sec_val in secrets.values():
                assert sec_val not in captured.out
                assert sec_val not in captured.err

        assert not cache_dir.exists()
    finally:
        clear_registered_secrets()


def test_build_iceberg_table_specs_with_external_columns_and_default_namespace(tmp_path):
    ext_dir = tmp_path / "extensions"
    ext_dir.mkdir()
    cfg_dict = _sample_iceberg_dict(ext_dir)
    cfg_dict.pop("tables")
    cfg_dict["default_catalog"] = "lake"
    cfg_dict["default_namespace"] = "curated"
    cfg_dict["tables"] = {"daily_orders": {"snapshot_id": 123456789}}

    columns = {
        "views": {
            "daily_orders": {
                "keys": ["order_id"],
                "columns": [
                    {"name": "order_id", "type": "bigint"},
                    {"name": "customer_email", "type": "varchar", "pii": "customer_email"},
                ],
            }
        }
    }
    pii = {"customer_email": {"source_form": "sha256", "mask": "redact"}}
    registry = {"reports": [{"view": "daily_orders", "status": "ready"}]}

    specs = build_iceberg_table_specs(
        cfg_dict,
        columns=columns,
        pii_policy=pii,
        registry=registry,
        pii_mode="exclude",
    )
    assert len(specs) == 1
    assert specs[0].source == "lake.curated.daily_orders"
    assert specs[0].snapshot_id == 123456789
    assert [c.name for c in specs[0].columns] == ["order_id"]


"""Tests for DuckDB iceberg_direct hardening plan, step ordering, quoting, and probe profile."""

from __future__ import annotations

import duckdb
import pytest

from seeknal.serving.duckdb_policy import (
    DuckDBHolder,
    IcebergDirectHardeningError,
    _IcebergLockedCursor,
    harden_iceberg_direct,
    plan_iceberg_direct,
    probe,
    probe_iceberg_direct,
    probe_iceberg_direct_ok,
)
from seeknal.serving.server.config import (
    DuckDBConnectionConfig,
    IcebergCatalogConfig,
    IcebergS3Config,
)


def _sample_iceberg_cfg(
    tmp_path,
    *,
    extensions: tuple[str, ...] = ("httpfs", "iceberg"),
    external_access_after_attach: str = "off",
    alias: str = "lake",
    warehouse: str = "main_warehouse",
    secrets_env: dict[str, str] | None = None,
) -> DuckDBConnectionConfig:
    ext_dir = tmp_path / "ext"
    ext_dir.mkdir(exist_ok=True)
    temp_dir = tmp_path / "tmp_spill"
    temp_dir.mkdir(exist_ok=True)

    sec_env = (
        secrets_env
        if secrets_env is not None
        else {
            "LK_CID": "FAKE_CLIENT_ID_SENTINEL_111",
            "LK_CSEC": "FAKE_CLIENT_SECRET_SENTINEL_222",
            "S3_KID": "FAKE_S3_KEY_ID_SENTINEL_333",
            "S3_SEC": "FAKE_S3_SECRET_SENTINEL_444",
        }
    )

    cat = IcebergCatalogConfig(
        alias=alias,
        endpoint="https://lakekeeper.internal.example/catalog",
        warehouse=warehouse,
        token_uri="https://idp.internal.example/oauth2/token",
        client_id_env="LK_CID",
        client_secret_env="LK_CSEC",
        scope="lakekeeper:read",
        access_delegation="vended",
        s3=IcebergS3Config(
            endpoint="s3.internal.example:9000",
            region="ap-southeast-1",
            url_style="path",
            key_id_env="S3_KID",
            secret_env="S3_SEC",
            use_ssl=True,
        ),
    )
    return DuckDBConnectionConfig(
        profile="iceberg_direct",
        path=":memory:",
        extension_directory=str(ext_dir),
        extensions=extensions,
        iceberg_catalogs={alias: cat},
        allowed_s3_prefixes=("s3://warehouse-bucket/analytics/",),
        recycle_seconds=180.0,
        token_lifetime_seconds=300.0,
        external_access_after_attach=external_access_after_attach,
        memory_limit="256MB",
        threads=2,
        temp_directory=str(temp_dir),
        max_temp_directory_size="512MB",
        secrets_env=sec_env,
    )


def test_plan_iceberg_direct_exact_phase_ordering_and_redaction(tmp_path):
    cfg = _sample_iceberg_cfg(tmp_path)
    steps = plan_iceberg_direct(cfg)

    phases = [s.phase for s in steps]
    # Must follow exact phase order: a -> b -> c -> d -> e -> f -> g
    collapsed: list[str] = []
    for p in phases:
        if not collapsed or collapsed[-1] != p:
            collapsed.append(p)
    assert collapsed == ["a", "b", "c", "d", "e", "f", "g"]

    # Check individual step details
    names = [s.name for s in steps]
    assert names[0] == "a_allowed_directories"
    assert names[1] == "a_allowed_paths"
    assert "b_autoinstall_off" in names
    assert "b_autoload_off" in names
    assert "b_extension_directory" in names
    assert "b_load_extension_httpfs" in names
    assert "b_load_extension_iceberg" in names
    assert "c_secret_s3_lake" in names
    assert "c_secret_oauth_lake" in names
    assert "d_attach_lake" in names
    assert "e_disable_external_access" in names
    assert "f_memory_limit" in names
    assert "f_threads" in names
    assert "f_temp_directory" in names
    assert "f_max_temp_directory_size" in names
    assert names[-1] == "g_lock_configuration"

    # Never emits INSTALL
    for s in steps:
        assert not s.sql.upper().startswith("INSTALL")

    # Secrets must NEVER appear in safe_sql / redacted_sql or repr(Step)
    sentinels = [
        "FAKE_CLIENT_ID_SENTINEL_111",
        "FAKE_CLIENT_SECRET_SENTINEL_222",
        "FAKE_S3_KEY_ID_SENTINEL_333",
        "FAKE_S3_SECRET_SENTINEL_444",
    ]
    for s in steps:
        for secret in sentinels:
            assert secret not in s.safe_sql
            assert secret not in repr(s)

    # Every secret step uses CREATE TEMPORARY SECRET (never persistent)
    secret_steps = [s for s in steps if s.phase == "c"]
    assert len(secret_steps) == 2
    for s in secret_steps:
        assert s.sql.startswith("CREATE TEMPORARY SECRET ")
        assert "PERSISTENT" not in s.sql

    # ATTACH step uses TYPE ICEBERG and READ_ONLY
    attach_step = next(s for s in steps if s.name == "d_attach_lake")
    assert "TYPE ICEBERG" in attach_step.sql
    assert "READ_ONLY" in attach_step.sql
    assert "AS lake (" in attach_step.sql


def test_plan_iceberg_direct_external_access_on_omits_step_e(tmp_path):
    cfg = _sample_iceberg_cfg(tmp_path, external_access_after_attach="on")
    steps = plan_iceberg_direct(cfg)
    phases = [s.phase for s in steps]
    assert "e" not in phases
    assert not any(s.name == "e_disable_external_access" for s in steps)


def test_plan_iceberg_direct_quoting_and_alias_rejection(tmp_path):
    # Warehouse or secret with single quotes is safely SQL-escaped
    cfg = _sample_iceberg_cfg(
        tmp_path,
        warehouse="wh'with'quotes",
        secrets_env={
            "LK_CID": "id'quote",
            "LK_CSEC": "sec'quote",
            "S3_KID": "kid'quote",
            "S3_SEC": "s3sec'quote",
        },
    )
    steps = plan_iceberg_direct(cfg)
    attach_step = next(s for s in steps if s.name == "d_attach_lake")
    assert "'wh''with''quotes'" in attach_step.sql

    # Invalid alias rejected in plan_iceberg_direct
    for bad_alias in ["BadAlias", "1alias", "lake-1", "lake;DROP", "system"]:
        bad_cfg = _sample_iceberg_cfg(tmp_path, alias=bad_alias)
        with pytest.raises(ValueError, match="alias"):
            plan_iceberg_direct(bad_cfg)


def test_harden_iceberg_direct_real_duckdb_with_offline_standin_and_probe(tmp_path):
    """Real-DuckDB test using built-in extensions (json, parquet) and a READ_ONLY attached local DuckDB stand-in catalog."""
    cat_dir = tmp_path / "standin_cat"
    cat_dir.mkdir()
    cat_file = cat_dir / "lake_standin.duckdb"
    c_init = duckdb.connect(str(cat_file))
    c_init.execute("CREATE SCHEMA analytics")
    c_init.execute("CREATE TABLE analytics.orders (order_id INT, amount DOUBLE)")
    c_init.execute("INSERT INTO analytics.orders VALUES (10, 99.5), (20, 149.0)")
    c_init.close()

    # Use built-in extensions ('json', 'parquet') that need no network download
    cfg = _sample_iceberg_cfg(tmp_path, extensions=("json", "parquet"))

    raw_conn = duckdb.connect(":memory:")
    caps = harden_iceberg_direct(
        raw_conn,
        cfg,
        standin_catalogs={"lake": str(cat_file)},
    )
    assert caps["s3_prefix_confinement"] is True
    assert caps["external_access_after_attach"] == "off"

    conn = _IcebergLockedCursor(raw_conn, ("lake",))
    setattr(conn, "_iceberg_direct_capabilities", caps)
    try:
        # 1. Probe profile passes all checks (including persistent secrets, autoinstall/autoload, custom repo lock)
        report = probe_iceberg_direct(conn, cfg)
        assert report["external_access_disabled"] is True
        assert report["allow_persistent_secrets_disabled"] is True
        assert report["autoinstall_autoload_disabled"] is True
        assert report["custom_extension_repository_locked"] is True
        assert report["config_locked"] is True
        assert report["read_text_outside_refused"] is True
        assert report["copy_to_refused"] is True
        assert report["install_refused"] is True
        assert report["new_attach_refused"] is True
        assert report["detach_refused"] is True
        assert report["create_secret_refused"] is True
        assert report["catalog_set_exact"] is True
        assert report["load_unlisted_refused"] is True
        assert report["secrets_temporary_only"] is True
        assert report["writes_refused"] is True
        assert report["s3_outside_prefix_refused"] is True
        assert report["select_ok"] is True
        assert probe_iceberg_direct_ok(report, cfg) is True

        # 2. SELECT on the attached catalog works with external access off
        rows = conn.execute(
            "SELECT order_id, amount FROM lake.analytics.orders ORDER BY order_id"
        ).fetchall()
        assert rows == [(10, 99.5), (20, 149.0)]

        # 3. After lock: new ATTACH, DETACH, CREATE SECRET, INSTALL, LOAD unlisted,
        # SET of locked settings, reads outside allowed dirs, and writes to attached catalog are all refused
        other_db = tmp_path / "other.duckdb"
        c_other = duckdb.connect(str(other_db))
        c_other.execute("CREATE TABLE t (x INT)")
        c_other.close()

        with pytest.raises(duckdb.Error):
            conn.execute(f"ATTACH '{other_db}' AS unauthorized_cat")

        with pytest.raises(duckdb.Error):
            conn.execute("DETACH lake")

        with pytest.raises(duckdb.Error):
            conn.execute("CREATE TEMPORARY SECRET sneaky (TYPE HTTP)")

        with pytest.raises(duckdb.Error):
            conn.execute("CREATE PERSISTENT SECRET sneaky_pers (TYPE HTTP)")

        with pytest.raises(duckdb.Error):
            conn.execute("INSTALL fts")

        with pytest.raises(duckdb.Error):
            conn.execute("LOAD fts")

        with pytest.raises(duckdb.Error):
            conn.execute("SET enable_external_access = true")

        with pytest.raises(duckdb.Error):
            conn.execute("SET threads = 8")

        outside_txt = tmp_path / "outside_secret.txt"
        outside_txt.write_text("top-secret-file", encoding="utf-8")
        with pytest.raises(duckdb.Error):
            conn.execute(f"SELECT * FROM read_text('{outside_txt}')")

        with pytest.raises(duckdb.Error):
            conn.execute("INSERT INTO lake.analytics.orders VALUES (30, 1.0)")

        with pytest.raises(duckdb.Error):
            conn.execute("DELETE FROM lake.analytics.orders WHERE order_id = 10")

        with pytest.raises(duckdb.Error):
            conn.execute("CREATE TABLE lake.analytics.hacked (x INT)")
    finally:
        conn.close()


def test_harden_iceberg_direct_surfaces_external_access_after_attach_on(tmp_path):
    cat_file = tmp_path / "cat_on.duckdb"
    c_init = duckdb.connect(str(cat_file))
    c_init.execute("CREATE TABLE t (id INT)")
    c_init.close()

    cfg = _sample_iceberg_cfg(
        tmp_path,
        extensions=("json",),
        external_access_after_attach="on",
    )
    raw_conn = duckdb.connect(":memory:")
    caps = harden_iceberg_direct(
        raw_conn,
        cfg,
        standin_catalogs={"lake": str(cat_file)},
    )
    conn = _IcebergLockedCursor(raw_conn, ("lake",))
    setattr(conn, "_iceberg_direct_capabilities", caps)
    try:
        report = probe_iceberg_direct(conn, cfg)
        assert report["external_access_after_attach"] == "on"
        assert report["external_access_disabled"] is False
        assert report["config_locked"] is True
        assert report["detach_refused"] is True
        assert report["create_secret_refused"] is True
    finally:
        conn.close()


def test_harden_failure_closes_connection_and_raises_secret_safe_step_error(tmp_path):
    sentinel = "ULTRA_SECRET_OAUTH_TOKEN_SENTINEL_777"
    cfg = _sample_iceberg_cfg(
        tmp_path,
        # Force failure at LOAD step by specifying an extension not present in empty ext_dir
        extensions=("httpfs",),
        secrets_env={
            "LK_CID": sentinel,
            "LK_CSEC": sentinel,
            "S3_KID": sentinel,
            "S3_SEC": sentinel,
        },
    )

    raw_conn = duckdb.connect(":memory:")
    with pytest.raises(IcebergDirectHardeningError) as exc_info:
        harden_iceberg_direct(raw_conn, cfg)

    assert exc_info.value.step_name == "b_load_extension_httpfs"
    assert sentinel not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None

    # Connection must already be closed
    with pytest.raises(duckdb.ConnectionException):
        raw_conn.execute("SELECT 1")


def test_post_lock_allowlist_blocks_all_bypass_vectors_and_entrypoints(tmp_path):
    """Verify _IcebergLockedCursor allowlist blocks comments, multi-statements, TEMP SECRET, quoted DETACH, ATTACH ':memory:', DDL in memory, COPY TO inside allowed dirs, and .sql()/.query()/.executemany()."""
    cat_file = tmp_path / "standin_bypass.duckdb"
    c_init = duckdb.connect(str(cat_file))
    c_init.execute("CREATE SCHEMA analytics")
    c_init.execute("CREATE TABLE analytics.orders (order_id INT, amount DOUBLE)")
    c_init.execute("INSERT INTO analytics.orders VALUES (1, 10.0)")
    c_init.close()

    cfg = _sample_iceberg_cfg(tmp_path, extensions=("json", "parquet"))
    raw_conn = duckdb.connect(":memory:")
    caps = harden_iceberg_direct(
        raw_conn,
        cfg,
        standin_catalogs={"lake": str(cat_file)},
    )
    conn = _IcebergLockedCursor(raw_conn, ("lake",))
    setattr(conn, "_iceberg_direct_capabilities", caps)

    try:
        # Allowed single-statement SELECTs (plain, comment-prefixed, CTE) work across execute/sql/query
        assert conn.execute("/* comment */ SELECT order_id FROM lake.analytics.orders").fetchall() == [(1,)]
        assert conn.execute("-- line comment\nWITH c AS (SELECT 42 AS v) SELECT v FROM c").fetchall() == [(42,)]
        assert conn.sql("SELECT 7").fetchall() == [(7,)]
        assert conn.query("SELECT 9").fetchall() == [(9,)]

        inside_ext_file = f"{cfg.extension_directory}/forged.duckdb"
        inside_tmp_csv = f"{cfg.temp_directory}/exfil.csv"

        bypass_statements = [
            "/* leading block comment */ DETACH lake",
            "-- leading line comment\nDETACH lake",
            'DETACH "lake"',
            "SELECT 1; DETACH lake",
            "ATTACH ':memory:' AS lake2",
            "/* comment */ ATTACH ':memory:' AS lake",
            f"ATTACH '{inside_ext_file}' AS forged_ext",
            "CREATE TEMP SECRET s_temp (TYPE HTTP)",
            "/* comment */ CREATE TEMP SECRET s_temp2 (TYPE HTTP)",
            "DROP TEMP SECRET IF EXISTS s3_lake",
            "DROP SECRET IF EXISTS s3_lake",
            "SELECT 1; CREATE TEMP SECRET s_multi (TYPE HTTP)",
            "CREATE TABLE forged_mem_table (id INT)",
            "CREATE TEMP TABLE forged_temp_table (id INT)",
            "CREATE VIEW forged_view AS SELECT 1",
            "CREATE MACRO forged_macro(x) AS x + 1",
            f"COPY (SELECT 1) TO '{inside_tmp_csv}'",
            "PRAGMA database_list",
            "CALL duckdb_databases()",
            "EXPLAIN SELECT 1",
            "PREPARE p AS SELECT 1",
        ]

        for bad_sql in bypass_statements:
            for method_name in ("execute", "sql", "query", "from_query", "executemany"):
                fn = getattr(conn, method_name)
                with pytest.raises(duckdb.PermissionException):
                    if method_name == "executemany":
                        fn(bad_sql, [])
                    else:
                        fn(bad_sql)

        # executemany is refused even for SELECT
        with pytest.raises(duckdb.PermissionException):
            conn.executemany("SELECT ?", [(1,)])

        # Direct extension/table mutation helper methods on DuckDBPyConnection are blocked by __getattr__
        for forbidden_attr in (
            "install_extension",
            "load_extension",
            "register",
            "unregister",
            "create_function",
            "remove_function",
            "read_csv",
            "read_json",
            "read_parquet",
            "append",
            "table",
            "view",
            "_raw",
        ):
            if forbidden_attr == "_raw":
                continue
            with pytest.raises(duckdb.PermissionException):
                getattr(conn, forbidden_attr)

        # Catalog set and data remain completely intact
        assert conn.execute("SELECT order_id, amount FROM lake.analytics.orders").fetchall() == [(1, 10.0)]
    finally:
        conn.close()


def test_holder_refuses_to_serve_when_probe_fails(tmp_path, monkeypatch):
    DuckDBHolder.reset()
    try:
        cat_file = tmp_path / "cat_fail.duckdb"
        c_init = duckdb.connect(str(cat_file))
        c_init.execute("CREATE TABLE t (id INT)")
        c_init.close()

        cfg = _sample_iceberg_cfg(tmp_path, extensions=("json",))
        holder = DuckDBHolder()

        # Simulate a probe check failing
        monkeypatch.setattr(
            "seeknal.serving.duckdb_policy.probe_iceberg_direct",
            lambda conn, c=None: {"select_ok": False, "config_locked": True},
        )
        with pytest.raises(RuntimeError, match="DuckDB probe conformance check failed"):
            holder.get_iceberg_cursor(
                cfg,
                standin_catalogs={"lake": str(cat_file)},
            )
        assert len(holder._instances_by_dev_ino) == 0
    finally:
        DuckDBHolder.reset()

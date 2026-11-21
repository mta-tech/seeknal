"""Tests for seeknal.serving.accel.loader, engines, settings, and CLI."""

from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import uuid

import duckdb
import pytest

from seeknal.serving.accel.__main__ import main as accel_main
from seeknal.serving.accel.engines import (
    DuckDBTarget,
    LakebaseSource,
    check_duckdb_version,
    create_target_factory,
)
from seeknal.serving.accel.loader import (
    LoadError,
    RowCountMismatchError,
    clear_registered_secrets,
    log_event,
    register_secret_values,
    run_load,
    scrub_text,
)
from seeknal.serving.accel.settings import (
    DEFAULT_EXPECTED_DUCKDB_VERSION,
    validate_extension_dir,
    validate_memory_limit,
    validate_threads,
)
from seeknal.serving.accel.tables import ColumnSpec, TableSpec


def _create_pg_standin_db(tmp_path: Path, pii_sentinel: str) -> Path:
    """Create a local DuckDB stand-in attached as `pg` with schema `rpt_base`."""
    pg_path = tmp_path / "pg_standin.duckdb"
    conn = duckdb.connect(str(pg_path))
    conn.execute("CREATE SCHEMA rpt_base")
    conn.execute(
        "CREATE TABLE rpt_base.order_metrics ("
        "order_id BIGINT, region VARCHAR, customer_email VARCHAR, amount DOUBLE"
        ")"
    )
    conn.execute(
        "INSERT INTO rpt_base.order_metrics VALUES "
        "(1, 'north', ?, 120.5), (2, 'south', ?, 240.0)",
        [pii_sentinel, pii_sentinel],
    )
    conn.close()
    return pg_path


def _sample_specs(with_pii_nulled: bool = True) -> list[TableSpec]:
    return [
        TableSpec(
            name="rpt.order_metrics",
            source="rpt_base.order_metrics",
            view="order_metrics",
            keys=("order_id",),
            columns=(
                ColumnSpec("order_id", "BIGINT", is_nulled=False),
                ColumnSpec("region", "VARCHAR", is_nulled=False),
                ColumnSpec(
                    "customer_email",
                    "VARCHAR",
                    is_nulled=with_pii_nulled,
                    is_pii=True,
                ),
                ColumnSpec("amount", "DOUBLE", is_nulled=False),
            ),
        )
    ]


def test_settings_and_duckdb_version_pin_check(tmp_path):
    assert DEFAULT_EXPECTED_DUCKDB_VERSION == "2.0.0"
    assert validate_memory_limit("512MB") == "512MB"
    assert validate_threads(4) == 4

    with pytest.raises(ValueError):
        validate_memory_limit("invalid_mem")
    with pytest.raises(ValueError):
        validate_threads(0)
    with pytest.raises(ValueError):
        validate_extension_dir("relative/path")

    # Matching version passes
    check_duckdb_version(expected_version=duckdb.__version__)

    # Mismatched version raises RuntimeError unless allow_mismatch=True
    with pytest.raises(RuntimeError, match="does not match expected version"):
        check_duckdb_version(expected_version="0.9.9", allow_mismatch=False)

    check_duckdb_version(expected_version="0.9.9", allow_mismatch=True)


def test_lakebase_source_and_duckdb_target_postgres_attach_sql():
    recorded: list[str] = []

    class FakeConn:
        def execute(self, stmt: str):
            recorded.append(stmt)
            return self

        def fetchone(self):
            return (17,)

        def close(self):
            pass

    lb = LakebaseSource(conn=None)
    # Inject a pre-created FakeConn to verify count query
    fake = FakeConn()
    lb_with_conn = LakebaseSource(conn=fake)
    assert lb_with_conn.get_row_count("rpt_base.order_metrics") == 17
    assert recorded == ["SELECT COUNT(*) FROM pg.rpt_base.order_metrics"]
    lb.close()


def test_run_load_pii_null_and_exclude_modes_never_write_pii_bytes(tmp_path):
    pii_sentinel = f"PII_SECRET_EMAIL_{uuid.uuid4().hex}@synthetic.test"
    pg_standin = _create_pg_standin_db(tmp_path, pii_sentinel)
    attach_sql = f"ATTACH '{pg_standin}' AS pg (READ_ONLY)"

    # 1. pii_mode = "null"
    cache_null = tmp_path / "cache_null"
    src_conn_1 = duckdb.connect(":memory:")
    src_conn_1.execute(attach_sql)
    source_1 = LakebaseSource(conn=src_conn_1)
    factory_1 = create_target_factory(attach_pg=False, attach_sql=attach_sql)

    out_null = run_load(
        specs=_sample_specs(with_pii_nulled=True),
        source=source_1,
        target_factory=factory_1,
        cache_dir=cache_null,
        clock=lambda: datetime(2026, 11, 19, 10, 0, 0, tzinfo=timezone.utc),
        pii_mode="null",
    )
    source_1.close()

    assert (cache_null / "mart.duckdb").is_symlink()
    assert os.readlink(cache_null / "mart.duckdb") == out_null.name

    ro_null = duckdb.connect(str(cache_null / "mart.duckdb"), read_only=True)
    rows_null = ro_null.execute(
        "SELECT order_id, region, customer_email, amount FROM rpt.order_metrics ORDER BY order_id"
    ).fetchall()
    ro_null.close()
    assert rows_null == [(1, "north", None, 120.5), (2, "south", None, 240.0)]
    assert pii_sentinel.encode("utf-8") not in out_null.read_bytes()

    fresh_null = json.loads((cache_null / "freshness.json").read_text(encoding="utf-8"))
    assert fresh_null["file_name"] == out_null.name
    assert fresh_null["tables"] == {"rpt.order_metrics": 2}
    assert fresh_null["as_of_date"] == "2026-11-19"
    assert fresh_null["duckdb_version"] == duckdb.__version__

    # 2. pii_mode = "exclude"
    cache_excl = tmp_path / "cache_excl"
    src_conn_2 = duckdb.connect(":memory:")
    src_conn_2.execute(attach_sql)
    source_2 = LakebaseSource(conn=src_conn_2)
    factory_2 = create_target_factory(attach_pg=False, attach_sql=attach_sql)

    out_excl = run_load(
        specs=_sample_specs(with_pii_nulled=True),
        source=source_2,
        target_factory=factory_2,
        cache_dir=cache_excl,
        clock=lambda: datetime(2026, 11, 19, 10, 5, 0, tzinfo=timezone.utc),
        pii_mode="exclude",
    )
    source_2.close()

    ro_excl = duckdb.connect(str(cache_excl / "mart.duckdb"), read_only=True)
    cols_excl = [
        r[0]
        for r in ro_excl.execute("DESCRIBE rpt.order_metrics").fetchall()
    ]
    rows_excl = ro_excl.execute(
        "SELECT order_id, region, amount FROM rpt.order_metrics ORDER BY order_id"
    ).fetchall()
    ro_excl.close()

    assert "customer_email" not in cols_excl
    assert cols_excl == ["order_id", "region", "amount"]
    assert rows_excl == [(1, "north", 120.5), (2, "south", 240.0)]
    assert pii_sentinel.encode("utf-8") not in out_excl.read_bytes()


def test_row_count_mismatch_And_failure_leave_previous_symlink_intact(tmp_path):
    pg_standin = _create_pg_standin_db(tmp_path, "dummy@synthetic.test")
    attach_sql = f"ATTACH '{pg_standin}' AS pg (READ_ONLY)"
    cache_dir = tmp_path / "cache"

    # Initial good load
    src_conn = duckdb.connect(":memory:")
    src_conn.execute(attach_sql)
    good_source = LakebaseSource(conn=src_conn)
    factory = create_target_factory(attach_pg=False, attach_sql=attach_sql)

    first_file = run_load(
        specs=_sample_specs(),
        source=good_source,
        target_factory=factory,
        cache_dir=cache_dir,
        clock=lambda: datetime(2026, 11, 19, 11, 0, 0, tzinfo=timezone.utc),
    )
    good_source.close()
    initial_freshness = (cache_dir / "freshness.json").read_text(encoding="utf-8")

    # Second load: source reports mismatched row count (999 != 2)
    class MismatchedSource:
        def get_row_count(self, table_or_view: str) -> int:
            return 999

    with pytest.raises(RowCountMismatchError, match="source=999, target=2"):
        run_load(
            specs=_sample_specs(),
            source=MismatchedSource(),
            target_factory=factory,
            cache_dir=cache_dir,
            clock=lambda: datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc),
        )

    # Symlink still points to first_file, and temp file from failed run was cleaned up
    assert os.readlink(cache_dir / "mart.duckdb") == first_file.name
    assert first_file.exists()
    assert list(cache_dir.glob("mart-*.duckdb")) == [first_file]
    assert (cache_dir / "freshness.json").read_text(encoding="utf-8") == initial_freshness

    # Third load: SQL execution error mid-load
    broken_spec = TableSpec(
        name="rpt.missing_view",
        source="rpt_base.nonexistent_table",
        view="missing_view",
        columns=(ColumnSpec("id", "BIGINT", False),),
    )
    with pytest.raises(LoadError):
        run_load(
            specs=[broken_spec],
            source=MismatchedSource(),
            target_factory=factory,
            cache_dir=cache_dir,
            clock=lambda: datetime(2026, 11, 19, 13, 0, 0, tzinfo=timezone.utc),
        )

    assert os.readlink(cache_dir / "mart.duckdb") == first_file.name
    assert list(cache_dir.glob("mart-*.duckdb")) == [first_file]


def test_lock_contention_and_regular_file_guard(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    # 1. Pre-existing regular file named mart.duckdb refuses without deleting it
    reg_file = cache_dir / "mart.duckdb"
    reg_file.write_text("do_not_delete", encoding="utf-8")

    class DummySource:
        def get_row_count(self, _: str) -> int:
            return 0

    with pytest.raises(LoadError, match="Pre-existing regular file"):
        run_load(
            specs=[],
            source=DummySource(),
            target_factory=lambda p: DuckDBTarget(p, attach_pg=False),
            cache_dir=cache_dir,
        )
    assert reg_file.read_text(encoding="utf-8") == "do_not_delete"
    reg_file.unlink()

    # 2. Exclusive lock held by another process/descriptor causes immediate LoadError
    lock_fd = open(cache_dir / ".loader.lock", "a", encoding="utf-8")
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(LoadError, match="lock held on .loader.lock"):
            run_load(
                specs=[],
                source=DummySource(),
                target_factory=lambda p: DuckDBTarget(p, attach_pg=False),
                cache_dir=cache_dir,
            )
    finally:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
        lock_fd.close()


def test_prune_keeps_n_files_and_never_deletes_active_symlink_target(tmp_path):
    pg_standin = _create_pg_standin_db(tmp_path, "x@synthetic.test")
    attach_sql = f"ATTACH '{pg_standin}' AS pg (READ_ONLY)"
    cache_dir = tmp_path / "cache"

    src_conn = duckdb.connect(":memory:")
    src_conn.execute(attach_sql)
    source = LakebaseSource(conn=src_conn)
    factory = create_target_factory(attach_pg=False, attach_sql=attach_sql)

    timestamps = [
        datetime(2026, 11, 19, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 19, 2, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 19, 3, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 19, 4, 0, 0, tzinfo=timezone.utc),
    ]
    produced: list[Path] = []
    for ts in timestamps:
        f = run_load(
            specs=_sample_specs(),
            source=source,
            target_factory=factory,
            cache_dir=cache_dir,
            clock=lambda t=ts: t,
            keep=2,
        )
        produced.append(f)

    # Only newest 2 files remain
    remaining = sorted(p.name for p in cache_dir.glob("mart-*.duckdb"))
    assert remaining == sorted([produced[2].name, produced[3].name])

    # Even with keep=0, active symlink target is NEVER deleted
    fifth = run_load(
        specs=_sample_specs(),
        source=source,
        target_factory=factory,
        cache_dir=cache_dir,
        clock=lambda: datetime(2026, 11, 19, 5, 0, 0, tzinfo=timezone.utc),
        keep=0,
    )
    source.close()
    assert fifth.exists()
    assert os.readlink(cache_dir / "mart.duckdb") == fifth.name
    assert [p.name for p in cache_dir.glob("mart-*.duckdb")] == [fifth.name]


def test_secret_scrubbing_in_scrub_text_and_log_event(monkeypatch, capsys):
    clear_registered_secrets()
    pg_pass = f"PG_SUPER_SECRET_{uuid.uuid4().hex}"
    ice_sec = f"ICEBERG_S3_SECRET_{uuid.uuid4().hex}"
    monkeypatch.setenv("PGPASSWORD", pg_pass)
    register_secret_values(ice_sec)

    try:
        raw = (
            f"Connection failed: postgresql://reader:{pg_pass}@10.24.88.9:5432/db "
            f"password={pg_pass} s3_key={ice_sec}"
        )
        scrubbed = scrub_text(raw)
        assert pg_pass not in scrubbed
        assert ice_sec not in scrubbed
        assert "10.24.88.9" not in scrubbed
        assert "[REDACTED]" in scrubbed
        assert "[REDACTED_IP]" in scrubbed

        log_event(
            "test_error",
            level="ERROR",
            client_secret=ice_sec,
            message=raw,
        )
        err_line = capsys.readouterr().err.strip()
        assert pg_pass not in err_line
        assert ice_sec not in err_line
        parsed = json.loads(err_line)
        assert "client_secret" not in parsed
    finally:
        clear_registered_secrets()


def test_cli_dry_run_and_version_mismatch_refusal(tmp_path, capsys):
    cols = {
        "views": {
            "order_metrics": {
                "keys": ["order_id"],
                "columns": [
                    {"name": "order_id", "type": "bigint"},
                    {"name": "customer_email", "type": "varchar", "pii": "customer_email"},
                ],
            }
        }
    }
    pii = {"customer_email": {"source_form": "sha256", "mask": "redact"}}
    reg = {"reports": [{"view": "order_metrics", "status": "ready"}]}

    cols_f = tmp_path / "columns.json"
    pii_f = tmp_path / "pii.json"
    reg_f = tmp_path / "registry.json"
    cols_f.write_text(json.dumps(cols), encoding="utf-8")
    pii_f.write_text(json.dumps(pii), encoding="utf-8")
    reg_f.write_text(json.dumps(reg), encoding="utf-8")

    cache_dir = tmp_path / "untouched_cache"

    # Dry-run with --pii-mode exclude prints SQL and does NOT create cache_dir
    rc = accel_main(
        [
            "load",
            "--columns",
            str(cols_f),
            "--pii",
            str(pii_f),
            "--registry",
            str(reg_f),
            "--cache-dir",
            str(cache_dir),
            "--pii-mode",
            "exclude",
            "--dry-run",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "CREATE TABLE rpt.order_metrics AS SELECT order_id FROM pg.rpt_base.order_metrics" in out
    assert "customer_email" not in out
    assert not cache_dir.exists()

    # Version mismatch exits 2 and does NOT create cache_dir
    rc_mismatch = accel_main(
        [
            "load",
            "--columns",
            str(cols_f),
            "--pii",
            str(pii_f),
            "--registry",
            str(reg_f),
            "--cache-dir",
            str(cache_dir),
            "--expect-duckdb-version",
            "0.0.1-mismatch",
        ]
    )
    assert rc_mismatch == 2
    assert not cache_dir.exists()


def test_orphan_wal_cleanup_and_buildinfo_single_source_of_truth(tmp_path, monkeypatch):
    import importlib
    import sys
    import types

    import seeknal.serving.accel.settings as accel_settings
    from seeknal.serving.accel.loader import _prune_older_files

    # 1. Verify settings.py defines DEFAULT_EXPECTED_DUCKDB_VERSION = "2.0.0" independently of buildinfo
    fake_buildinfo = types.ModuleType("seeknal.serving.buildinfo")
    fake_buildinfo.EXPECTED_DUCKDB_VERSION = "9.9.9-mutated-buildinfo"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "seeknal.serving.buildinfo", fake_buildinfo)
    reloaded = importlib.reload(accel_settings)
    assert reloaded.DEFAULT_EXPECTED_DUCKDB_VERSION == "2.0.0"

    # Also verify get_extension_dir fallback order: override -> ACCEL_EXTENSION_DIR -> SEEKNAL_EXTENSION_DIRECTORY
    monkeypatch.delenv("ACCEL_EXTENSION_DIR", raising=False)
    monkeypatch.setenv("SEEKNAL_EXTENSION_DIRECTORY", "/opt/seeknal/extensions")
    assert reloaded.get_extension_dir() == "/opt/seeknal/extensions"
    monkeypatch.setenv("ACCEL_EXTENSION_DIR", "/opt/accel/extensions")
    assert reloaded.get_extension_dir() == "/opt/accel/extensions"
    assert reloaded.get_extension_dir("/opt/override/extensions") == "/opt/override/extensions"

    # Restore original settings module state
    monkeypatch.delitem(sys.modules, "seeknal.serving.buildinfo", raising=False)
    importlib.reload(accel_settings)
    assert accel_settings.DEFAULT_EXPECTED_DUCKDB_VERSION == "2.0.0"

    # 2. Verify orphan .wal and uncheckpointed .duckdb+.wal cleanup never touches active symlink target
    cache_dir = tmp_path / "wal_cache"
    cache_dir.mkdir()
    active = cache_dir / "mart-20261119T010000Z-11111111.duckdb"
    active.write_bytes(b"active")
    os.symlink(active.name, cache_dir / "mart.duckdb")

    uncheckpointed = cache_dir / "mart-20261119T020000Z-22222222.duckdb"
    uncheckpointed_wal = cache_dir / "mart-20261119T020000Z-22222222.duckdb.wal"
    uncheckpointed.write_bytes(b"partial")
    uncheckpointed_wal.write_bytes(b"wal")

    orphan_wal = cache_dir / "mart-20261119T030000Z-33333333.duckdb.wal"
    orphan_wal.write_bytes(b"orphan_wal")

    _prune_older_files(cache_dir, keep=3)
    assert active.exists()
    assert not uncheckpointed.exists()
    assert not uncheckpointed_wal.exists()
    assert not orphan_wal.exists()


def test_b_l3_scrub_text_redacts_sql_escaped_doubled_single_quote_secrets(monkeypatch):
    from seeknal.serving.accel.loader import register_secret_env_vars

    clear_registered_secrets()
    tag = uuid.uuid4().hex[:10]
    pg_secret = f"pg'pass'secret'{tag}"
    env_secret = f"oauth'client'sec'{tag}"
    extra_secret = f"extra's3'key'{tag}"

    monkeypatch.setenv("PGPASSWORD", pg_secret)
    monkeypatch.setenv("CUSTOM_SENSITIVE_SECRET_ENV", env_secret)
    register_secret_env_vars("CUSTOM_SENSITIVE_SECRET_ENV")

    try:
        sql_escaped_msg = (
            f"DuckDB Error near '{pg_secret.replace(chr(39), chr(39)*2)}' "
            f"and '{env_secret.replace(chr(39), chr(39)*2)}' "
            f"and '{extra_secret.replace(chr(39), chr(39)*2)}' "
            f"raw={pg_secret},{env_secret},{extra_secret}"
        )
        scrubbed = scrub_text(sql_escaped_msg, extra_secrets=[extra_secret])

        for raw_val in (pg_secret, env_secret, extra_secret):
            escaped_val = raw_val.replace("'", "''")
            assert raw_val not in scrubbed
            assert escaped_val not in scrubbed
            assert tag not in scrubbed
        assert "[REDACTED]" in scrubbed
    finally:
        clear_registered_secrets()



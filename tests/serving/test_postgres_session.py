"""Unit tests for shared PostgreSQL-wire session hardening, TLS, role guard, and transaction preamble."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from seeknal.serving.adapters import EngineError
from seeknal.serving.postgres_session import (
    EngineStartingError,
    PostgresAccountConfig,
    PostgresSessionConfig,
    PostgresTLSConfig,
    begin_query_transaction,
    build_connect_kwargs,
    effective_storage_class,
    is_postgres_wire,
    role_guard_sql,
    verify_and_harden_session,
)


class ScriptedCursor:
    """Deterministic DB-API fake cursor recording (sql, params) and returning scripted rows."""

    def __init__(self, conn: ScriptedConnection) -> None:
        self.conn = conn
        self.closed = False
        self._pending_row: Any = None

    def execute(self, sql: str, params: Any = None) -> None:
        if self.conn.closed:
            raise RuntimeError("Cursor execute on closed connection")
        if self.conn.fail_on_execute is not None:
            raise self.conn.fail_on_execute
        self.conn.executed.append((sql, params))
        sql_norm = " ".join(sql.strip().split())
        if "pg_roles" in sql_norm:
            self._pending_row = self.conn.role_row
        elif sql_norm.upper().startswith("SHOW "):
            guc = sql_norm.split()[1].lower()
            self._pending_row = self.conn.show_rows.get(guc)
        else:
            self._pending_row = None

    def fetchone(self) -> Any:
        return self._pending_row

    def close(self) -> None:
        self.closed = True


class ScriptedConnection:
    """Deterministic DB-API fake connection tracking close/commit/rollback and SQL history."""

    def __init__(
        self,
        *,
        role_row: Any = (False, False),
        show_rows: dict[str, Any] | None = None,
        fail_on_execute: Exception | None = None,
    ) -> None:
        self.role_row = role_row
        self.show_rows: dict[str, Any] = {
            "default_transaction_read_only": ("on",),
            "statement_timeout": ("30s",),
            "lock_timeout": ("2000ms",),
            "idle_in_transaction_session_timeout": ("10000",),
            "search_path": ("pg_catalog",),
        }
        if show_rows:
            self.show_rows.update(show_rows)
        self.fail_on_execute = fail_on_execute
        self.executed: list[tuple[str, Any]] = []
        self.closed = False
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> ScriptedCursor:
        if self.closed:
            raise RuntimeError("Cannot open cursor on closed connection")
        return ScriptedCursor(self)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_dataclasses_defaults_and_immutability():
    sess = PostgresSessionConfig()
    assert sess.statement_timeout_ms == 30000
    assert sess.lock_timeout_ms == 2000
    assert sess.idle_in_transaction_session_timeout_ms == 10000
    assert sess.search_path == "pg_catalog"
    assert sess.application_name == "seeknal-serving"
    assert sess.startup_options is True
    assert sess.connection_limit is None

    with pytest.raises(Exception):
        sess.statement_timeout_ms = 5000  # type: ignore[misc]

    tls = PostgresTLSConfig()
    assert tls.sslmode == "verify-full"
    assert tls.sslrootcert_file is None
    assert tls.sslcert_file is None
    assert tls.sslkey_file is None

    acc_env = PostgresAccountConfig(dsn_env="PG_DSN")
    assert acc_env.dsn_env == "PG_DSN"
    assert acc_env.dsn_file is None

    acc_file = PostgresAccountConfig(dsn_file="/etc/seeknal/pg.dsn")
    assert acc_file.dsn_file == "/etc/seeknal/pg.dsn"
    assert acc_file.dsn_env is None

    with pytest.raises(ValueError):
        PostgresAccountConfig(dsn_env="A", dsn_file="/b")
    with pytest.raises(ValueError):
        PostgresAccountConfig()
    with pytest.raises(ValueError):
        PostgresSessionConfig(statement_timeout_ms=0)
    with pytest.raises(ValueError):
        PostgresSessionConfig(search_path="pg_catalog; DROP TABLE x")
    with pytest.raises(ValueError):
        PostgresTLSConfig(sslmode="invalid-mode")


def test_is_postgres_wire():
    assert is_postgres_wire("postgres") is True
    assert is_postgres_wire("lakebase") is True
    assert is_postgres_wire("duckdb") is False
    assert is_postgres_wire("starrocks") is False
    assert is_postgres_wire("cube") is False
    assert is_postgres_wire(None) is False
    assert is_postgres_wire(123) is False


def test_build_connect_kwargs_hardened_read_only_and_tls():
    planted_dsn = "postgresql://svc_bi:PLANTED_SECRET_PW_9911@db.internal:5432/cbn"
    sess = PostgresSessionConfig(
        statement_timeout_ms=30000,
        lock_timeout_ms=2000,
        idle_in_transaction_session_timeout_ms=10000,
        search_path="pg_catalog",
        application_name="seeknal-serving",
        startup_options=True,
    )

    # 1. With session and tls=None on postgres: defaults sslmode to verify-full, omits dsn
    kw_ro = build_connect_kwargs(
        planted_dsn,
        connect_timeout=4.2,
        session=sess,
        tls=None,
        read_only=True,
    )
    assert "dsn" not in kw_ro
    assert "PLANTED_SECRET_PW_9911" not in repr(kw_ro)
    assert kw_ro["connect_timeout"] == 5
    assert kw_ro["application_name"] == "seeknal-serving"
    assert kw_ro["sslmode"] == "verify-full"
    assert kw_ro["options"] == (
        "-c default_transaction_read_only=on "
        "-c statement_timeout=30000 "
        "-c lock_timeout=2000 "
        "-c idle_in_transaction_session_timeout=10000 "
        "-c search_path=pg_catalog"
    )

    # 2. Writable mode omits default_transaction_read_only=on
    kw_rw = build_connect_kwargs(
        planted_dsn,
        connect_timeout=10.0,
        session=sess,
        tls=PostgresTLSConfig(
            sslmode="verify-full",
            sslrootcert_file="/etc/tls/ca.pem",
            sslcert_file="/etc/tls/client.crt",
            sslkey_file="/etc/tls/client.key",
        ),
        read_only=False,
    )
    assert kw_rw["connect_timeout"] == 10
    assert kw_rw["sslmode"] == "verify-full"
    assert kw_rw["sslrootcert"] == "/etc/tls/ca.pem"
    assert kw_rw["sslcert"] == "/etc/tls/client.crt"
    assert kw_rw["sslkey"] == "/etc/tls/client.key"
    assert kw_rw["options"] == (
        "-c statement_timeout=30000 "
        "-c lock_timeout=2000 "
        "-c idle_in_transaction_session_timeout=10000 "
        "-c search_path=pg_catalog"
    )

    # 3. startup_options=False omits 'options' from connect kwargs
    sess_no_opts = PostgresSessionConfig(startup_options=False)
    kw_pooler = build_connect_kwargs(
        planted_dsn,
        connect_timeout=5.0,
        session=sess_no_opts,
        read_only=True,
    )
    assert "options" not in kw_pooler
    assert kw_pooler["application_name"] == "seeknal-serving"
    assert kw_pooler["sslmode"] == "verify-full"

    # 4. Legacy unhardened writable: only connect_timeout
    kw_legacy = build_connect_kwargs(planted_dsn, connect_timeout=10.0, read_only=False)
    assert kw_legacy == {"connect_timeout": 10}


def test_verify_and_harden_session_role_guard():
    # 1. Non-privileged postgres role passes
    conn_ok = ScriptedConnection(role_row=(False, False))
    verify_and_harden_session(conn_ok, kind="postgres", role_guard=True)
    assert not conn_ok.closed
    assert any("pg_roles" in sql for sql, _ in conn_ok.executed)

    # 2. String 'f' values from text-format DB-API drivers pass (not treated as truthy!)
    conn_str_false = ScriptedConnection(role_row=("f", "f"))
    verify_and_harden_session(conn_str_false, kind="postgres", role_guard=True)
    assert not conn_str_false.closed

    # 3. rolsuper=True refused and connection closed
    conn_super = ScriptedConnection(role_row=(True, False))
    with pytest.raises(EngineError, match="Role guard refused connection"):
        verify_and_harden_session(conn_super, kind="postgres", role_guard=True)
    assert conn_super.closed is True

    # 4. rolbypassrls=True refused and connection closed
    conn_bypass = ScriptedConnection(role_row=(False, True))
    with pytest.raises(EngineError, match="Role guard refused connection"):
        verify_and_harden_session(conn_bypass, kind="postgres", role_guard=True)
    assert conn_bypass.closed is True

    # 5. Lakebase neon_superuser=True refused even if rolsuper=False and rolbypassrls=False
    conn_neon = ScriptedConnection(role_row=(False, False, True))
    with pytest.raises(EngineError, match="Role guard refused connection"):
        verify_and_harden_session(conn_neon, kind="lakebase", role_guard=False)
    assert conn_neon.closed is True
    assert "neon_superuser" in role_guard_sql("lakebase")

    # 6. role_guard=False on postgres skips role guard
    conn_skip = ScriptedConnection(role_row=(True, True))
    verify_and_harden_session(conn_skip, kind="postgres", role_guard=False, session=None)
    assert not conn_skip.closed
    assert conn_skip.executed == []

    # 7. Legacy bare MagicMock cursor returning MagicMock does not break legacy lakebase tests,
    #    whereas a tuple with True still fails closed!
    legacy_mock = MagicMock()
    verify_and_harden_session(
        legacy_mock,
        kind="lakebase",
        role_guard=True,
        session=None,
        is_legacy_lakebase=True,
    )
    assert not legacy_mock.close.called

    legacy_super = ScriptedConnection(role_row=(True, False, False))
    with pytest.raises(EngineError, match="Role guard refused connection"):
        verify_and_harden_session(
            legacy_super,
            kind="lakebase",
            role_guard=True,
            session=None,
            is_legacy_lakebase=True,
        )
    assert legacy_super.closed is True


def test_verify_and_harden_session_show_checks_and_mismatch_fail_closed():
    sess = PostgresSessionConfig(
        statement_timeout_ms=60000,
        lock_timeout_ms=2000,
        idle_in_transaction_session_timeout_ms=10000,
        search_path="pg_catalog",
        startup_options=True,
    )

    # Valid SHOW responses across PG unit formats ('1min', '2s', '10000ms')
    conn_valid = ScriptedConnection(
        role_row=(False, False),
        show_rows={
            "default_transaction_read_only": ("on",),
            "statement_timeout": ("1min",),
            "lock_timeout": ("2s",),
            "idle_in_transaction_session_timeout": ("10000ms",),
            "search_path": ("pg_catalog",),
        },
    )
    verify_and_harden_session(
        conn_valid, kind="postgres", read_only=True, session=sess
    )
    assert not conn_valid.closed
    executed_sqls = [s for s, _ in conn_valid.executed]
    assert not any(s.startswith("SET ") for s in executed_sqls)
    assert "SHOW default_transaction_read_only" in executed_sqls
    assert "SHOW statement_timeout" in executed_sqls
    assert "SHOW lock_timeout" in executed_sqls
    assert "SHOW idle_in_transaction_session_timeout" in executed_sqls
    assert "SHOW search_path" in executed_sqls

    # Mismatch on each SHOW setting fails closed and closes conn
    mismatch_cases = [
        {"default_transaction_read_only": ("off",)},
        {"statement_timeout": ("0",)},
        {"statement_timeout": ("45s",)},
        {"lock_timeout": ("5000ms",)},
        {"idle_in_transaction_session_timeout": ("30s",)},
        {"search_path": ('"$user", public',)},
    ]
    for override in mismatch_cases:
        base_rows = {
            "default_transaction_read_only": ("on",),
            "statement_timeout": ("60s",),
            "lock_timeout": ("2000ms",),
            "idle_in_transaction_session_timeout": ("10s",),
            "search_path": ("pg_catalog",),
        }
        base_rows.update(override)
        conn_bad = ScriptedConnection(role_row=(False, False), show_rows=base_rows)
        with pytest.raises(EngineError, match="Session hardening SHOW verification failed"):
            verify_and_harden_session(
                conn_bad, kind="postgres", read_only=True, session=sess
            )
        assert conn_bad.closed is True


def test_verify_and_harden_session_startup_options_false_runs_sets():
    sess = PostgresSessionConfig(
        statement_timeout_ms=30000,
        lock_timeout_ms=2000,
        idle_in_transaction_session_timeout_ms=10000,
        search_path="pg_catalog",
        startup_options=False,
    )
    conn = ScriptedConnection(role_row=(False, False))
    verify_and_harden_session(conn, kind="postgres", read_only=True, session=sess)
    assert not conn.closed
    assert conn.committed is True

    executed_sqls = [s for s, _ in conn.executed]
    assert "SET default_transaction_read_only = on" in executed_sqls
    assert "SET statement_timeout = 30000" in executed_sqls
    assert "SET lock_timeout = 2000" in executed_sqls
    assert "SET idle_in_transaction_session_timeout = 10000" in executed_sqls
    assert "SET search_path TO pg_catalog" in executed_sqls


def test_verify_and_harden_session_never_leaks_driver_dsn_on_error():
    planted_pw = "PLANTED_DRIVER_PW_778899"
    conn = ScriptedConnection(
        fail_on_execute=RuntimeError(
            f"connection to postgresql://user:{planted_pw}@10.0.0.5:5432/prod failed"
        )
    )
    with pytest.raises(EngineError) as exc_info:
        verify_and_harden_session(conn, kind="postgres", role_guard=True)
    assert conn.closed is True
    assert planted_pw not in str(exc_info.value)


def test_begin_query_transaction_hardened_and_legacy():
    # 1. Hardened read-only transaction uses BEGIN READ ONLY and parameterised set_config
    conn_ro = ScriptedConnection()
    cur_ro = conn_ro.cursor()
    begin_query_transaction(
        cur_ro,
        read_only=True,
        timeout_ms=12500,
        workload="bi",
        audit_id="jti-abc-123",
        hardened=True,
    )
    assert conn_ro.executed == [
        ("BEGIN READ ONLY", None),
        ("SET LOCAL statement_timeout = 12500", None),
        (
            "SELECT set_config('application_name', %s, true)",
            ("seeknal:bi:jti-abc-123",),
        ),
    ]

    # 2. Hardened read-write transaction uses BEGIN and parameterised set_config
    conn_rw = ScriptedConnection()
    cur_rw = conn_rw.cursor()
    begin_query_transaction(
        cur_rw,
        read_only=False,
        timeout_ms=5000,
        workload="interactive",
        audit_id="jti-rw-999",
        hardened=True,
    )
    assert conn_rw.executed == [
        ("BEGIN", None),
        ("SET LOCAL statement_timeout = 5000", None),
        (
            "SELECT set_config('application_name', %s, true)",
            ("seeknal:interactive:jti-rw-999",),
        ),
    ]

    # 3. Legacy unhardened lakebase SELECT vs WRITE
    conn_leg_sel = ScriptedConnection()
    begin_query_transaction(
        conn_leg_sel.cursor(),
        read_only=False,
        timeout_ms=3000,
        workload="interactive",
        audit_id="jti-leg-1",
        hardened=False,
        has_row_limit=True,
    )
    assert [s for s, _ in conn_leg_sel.executed] == [
        "BEGIN",
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = 3000",
    ]

    conn_leg_wr = ScriptedConnection()
    begin_query_transaction(
        conn_leg_wr.cursor(),
        read_only=False,
        timeout_ms=3000,
        workload="interactive",
        audit_id="jti-leg-2",
        hardened=False,
        has_row_limit=False,
    )
    assert [s for s, _ in conn_leg_wr.executed] == [
        "BEGIN",
        "SET LOCAL statement_timeout = 3000",
    ]


def test_engine_starting_error_and_effective_storage_class_exports():
    err = EngineStartingError()
    assert isinstance(err, EngineError)
    assert err.code == "engine_starting"
    assert err.status_code == 503
    assert err.retry_after == 3

    class DummyConn:
        def __init__(self, kind: str, storage_class: str | None = None, lakebase: Any = None):
            self.kind = kind
            self.storage_class = storage_class
            self.lakebase = lakebase

    assert effective_storage_class(DummyConn("postgres")) == "local"
    assert effective_storage_class(DummyConn("postgres", "object_storage")) == "object_storage"
    assert effective_storage_class(DummyConn("lakebase", lakebase=object())) == "object_storage"
    assert effective_storage_class(DummyConn("lakebase", lakebase=None)) == "unknown"

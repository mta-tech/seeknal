"""Unit tests for Lakebase endpoint config, storage class, role guard, snapshot freshness, and cold-start wake."""

from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from seeknal.serving.adapters import EngineError
from seeknal.serving.errors import clear_registered_config_secrets, scrub_error_detail
from seeknal.serving.lakebase_endpoint import (
    ColdStartConnector,
    EngineStartingError,
    LakebaseColdStartConfig,
    LakebaseEndpointConfig,
    LakebaseFreshnessConfig,
    check_role_guard,
    effective_storage_class,
    parse_lakebase_block,
    role_guard_sql,
    snapshot_freshness,
    validate_lakebase_tls,
)

VALID_PROJECT_ID = "prj_0123456789abcdefghij"
VALID_BRANCH_ID = "br_0123456789abcdefghij"
VALID_ENDPOINT_ID = "ep_0123456789abcdefghij"


def _minimal_raw_block(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "project_id": VALID_PROJECT_ID,
        "branch_id": VALID_BRANCH_ID,
        "endpoint_id": VALID_ENDPOINT_ID,
    }
    base.update(overrides)
    return base


class FakeClock:
    """Thread-safe deterministic clock for zero-sleep cold-start tests."""

    def __init__(self, initial: float = 1000.0) -> None:
        self._lock = threading.Lock()
        self._now = float(initial)
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> float:
        with self._lock:
            self._now += max(0.0, float(seconds))
            return self._now

    def sleep(self, seconds: float) -> None:
        dt = max(0.0, float(seconds))
        with self._lock:
            self.sleeps.append(dt)
            self._now += dt


class FakeMetrics:
    """Thread-safe fake metrics recorder capturing Lakebase cold-start & state calls."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.wakes: list[tuple[str, str]] = []
        self.connect_retries: list[str] = []
        self.cold_start_durations: list[tuple[str, float]] = []
        self.states: list[tuple[str, str]] = []

    def record_lakebase_wake(self, engine: str, outcome: str) -> None:
        with self._lock:
            self.wakes.append((engine, outcome))

    def record_lakebase_connect_retry(self, engine: str) -> None:
        with self._lock:
            self.connect_retries.append(engine)

    def observe_lakebase_cold_start(self, engine: str, seconds: float) -> None:
        with self._lock:
            self.cold_start_durations.append((engine, seconds))

    def set_lakebase_state(self, engine: str, state: str) -> None:
        with self._lock:
            self.states.append((engine, state))


class FakeDBCursor:
    def __init__(self, conn: FakeDBConnection) -> None:
        self._conn = conn
        self.closed = False
        self._last_row: Any = None

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.executed.append((sql, params))
        if self._conn.query_exc is not None:
            raise self._conn.query_exc
        self._last_row = self._conn.next_row

    def fetchone(self) -> Any:
        return self._last_row

    def close(self) -> None:
        self.closed = True


class FakeDBConnection:
    def __init__(
        self,
        *,
        conn_id: int = 1,
        next_row: Any = None,
        query_exc: Exception | None = None,
    ) -> None:
        self.conn_id = conn_id
        self.next_row = next_row
        self.query_exc = query_exc
        self.closed = False
        self.executed: list[tuple[str, Any]] = []

    def cursor(self) -> FakeDBCursor:
        return FakeDBCursor(self)

    def close(self) -> None:
        self.closed = True


class CustomConfigError(Exception):
    """Custom exception type to verify error_cls parameter support."""


# ---------------------------------------------------------------------------
# 1. Lakebase Block Parsing & Validation
# ---------------------------------------------------------------------------


class TestParseLakebaseBlock:
    def test_minimal_block_defaults(self) -> None:
        cfg = parse_lakebase_block(_minimal_raw_block())
        assert isinstance(cfg, LakebaseEndpointConfig)
        assert cfg.project_id == VALID_PROJECT_ID
        assert cfg.branch_id == VALID_BRANCH_ID
        assert cfg.endpoint_id == VALID_ENDPOINT_ID
        assert cfg.endpoint_type == "read_write"
        assert cfg.cu is None
        assert cfg.max_connections is None
        assert cfg.storage_class == "object_storage"

        # Omitted cold_start defaults wake to 'none'
        assert isinstance(cfg.cold_start, LakebaseColdStartConfig)
        assert cfg.cold_start.wake == "none"
        assert cfg.cold_start.connect_timeout_seconds == 5.0
        assert cfg.cold_start.api_url is None
        assert cfg.cold_start.api_token_env is None
        assert cfg.cold_start.max_query_wait_seconds == 10.0
        assert cfg.cold_start.max_wake_seconds == 60.0
        assert cfg.cold_start.retry_backoff_ms == (500, 1000, 2000, 4000)

        # Freshness defaults sync_state_table ON for lakebase
        assert isinstance(cfg.freshness, LakebaseFreshnessConfig)
        assert cfg.freshness.sync_state_table == "public._lakebase_sync_state"
        assert cfg.freshness.freshness_sql is None

    def test_full_block_and_cold_start_default_wake_api(self) -> None:
        cfg = parse_lakebase_block(
            _minimal_raw_block(
                endpoint_type="read_only",
                cu=2.5,
                cold_start={
                    "api_url": "https://lakebase-cp.internal.example/",
                    "api_token_env": "LB_OPS_PROJECT_TOKEN",
                    "connect_timeout_seconds": 4,
                    "max_query_wait_seconds": 8.5,
                    "max_wake_seconds": 45,
                    "retry_backoff_ms": [250, 500, 1000],
                },
                freshness={
                    "sync_state_table": "ops._lakebase_sync_state",
                    "freshness_sql": "  SELECT max(updated_at) FROM ops.load_ctl  ",
                },
            )
        )
        assert cfg.endpoint_type == "read_only"
        assert cfg.cu == 2.5
        assert cfg.max_connections == 250
        assert cfg.cold_start.wake == "api"
        assert cfg.cold_start.api_url == "https://lakebase-cp.internal.example"
        assert cfg.cold_start.api_token_env == "LB_OPS_PROJECT_TOKEN"
        assert cfg.cold_start.connect_timeout_seconds == 4.0
        assert cfg.cold_start.max_query_wait_seconds == 8.5
        assert cfg.cold_start.max_wake_seconds == 45.0
        assert cfg.cold_start.retry_backoff_ms == (250, 500, 1000)
        assert cfg.freshness.sync_state_table == "ops._lakebase_sync_state"
        assert cfg.freshness.freshness_sql == "SELECT max(updated_at) FROM ops.load_ctl"

    @pytest.mark.parametrize(
        ("cu", "expected_max_conn"),
        [
            (0.1, 25),
            (0.25, 25),
            (0.5, 50),
            (1, 100),
            (4, 400),
        ],
    )
    def test_cu_max_connections(self, cu: float, expected_max_conn: int) -> None:
        cfg = parse_lakebase_block(_minimal_raw_block(cu=cu))
        assert cfg.max_connections == expected_max_conn

    @pytest.mark.parametrize("bad_cu", [True, False, 0, -1, -0.5, float("nan"), float("inf"), "1"])
    def test_invalid_cu_rejected(self, bad_cu: Any) -> None:
        with pytest.raises(ValueError):
            parse_lakebase_block(_minimal_raw_block(cu=bad_cu))

    @pytest.mark.parametrize(
        ("field", "bad_val"),
        [
            ("project_id", None),
            ("project_id", ""),
            ("project_id", "prj_short"),
            ("project_id", "prj_0123456789abcdefghijk"),  # 25 chars
            ("project_id", "prj_0123456789ABCDEFGHIJ"),  # uppercase
            ("project_id", "br_0123456789abcdefghij"),   # wrong prefix
            ("project_id", f"{VALID_PROJECT_ID}\n"),      # trailing newline
            ("branch_id", None),
            ("branch_id", "br_short"),
            ("branch_id", "br_0123456789ABCDEFGHIJ"),
            ("branch_id", "prj_0123456789abcdefghij"),
            ("branch_id", "br_0123456789abcdefghijk"),
            ("endpoint_id", None),
            ("endpoint_id", "ep_short"),
            ("endpoint_id", "ep_0123456789ABCDEFGHIJ"),
            ("endpoint_id", "br_0123456789abcdefghij"),
            ("endpoint_id", "ep_0123456789_bcdefghij"),
        ],
    )
    def test_invalid_ids_rejected(self, field: str, bad_val: Any) -> None:
        raw = _minimal_raw_block(**{field: bad_val})
        with pytest.raises(CustomConfigError):
            parse_lakebase_block(raw, error_cls=CustomConfigError)

    @pytest.mark.parametrize("bad_type", ["rw", "ro", "READ_WRITE", None, 123, True])
    def test_invalid_endpoint_type_rejected(self, bad_type: Any) -> None:
        with pytest.raises(ValueError):
            parse_lakebase_block(_minimal_raw_block(endpoint_type=bad_type))

    @pytest.mark.parametrize("sc_val", ["object_storage", "local", "unknown", None])
    def test_setting_storage_class_in_block_is_error(self, sc_val: Any) -> None:
        with pytest.raises(
            CustomConfigError,
            match="Lakebase storage_class is fixed to 'object_storage' and cannot be set",
        ):
            parse_lakebase_block(
                _minimal_raw_block(storage_class=sc_val),
                error_cls=CustomConfigError,
            )

    @pytest.mark.parametrize("secret_key", ["api_token", "token", "secret", "password"])
    def test_cold_start_rejects_literal_token_keys_without_leaking_value(
        self, secret_key: str
    ) -> None:
        canary = "PLANTED_SECRET_LB_918273"
        raw = _minimal_raw_block(
            cold_start={
                "wake": "api",
                "api_url": "https://cp.internal.example",
                secret_key: canary,
            }
        )
        with pytest.raises(ValueError) as exc_info:
            parse_lakebase_block(raw)
        assert canary not in str(exc_info.value)

    @pytest.mark.parametrize(
        "bad_url",
        [
            None,
            "",
            "ftp://cp.internal.example",
            "cp.internal.example",
            "https://",
            "http://cp.internal.example",
            "http://10.0.0.5:8080/api",
            "https://admin:SUPER_SECRET_PASS@cp.internal.example",
            "http://user@cp.internal.example",
            "https://cp.internal.example?token=secret",
            "https://cp.internal.example#frag",
        ],
    )
    def test_cold_start_rejects_invalid_or_credentialed_api_url(
        self, bad_url: Any
    ) -> None:
        raw = _minimal_raw_block(
            cold_start={
                "wake": "api",
                "api_url": bad_url,
                "api_token_env": "LB_TOKEN",
            }
        )
        with pytest.raises(ValueError) as exc_info:
            parse_lakebase_block(raw)
        assert "SUPER_SECRET_PASS" not in str(exc_info.value)

    @pytest.mark.parametrize(
        "loopback_or_https_url,expected",
        [
            ("https://cp.internal.example/", "https://cp.internal.example"),
            ("http://127.0.0.1:8080/", "http://127.0.0.1:8080"),
            ("http://localhost:8080/v0/", "http://localhost:8080/v0"),
            ("http://[::1]:8080/", "http://[::1]:8080"),
        ],
    )
    def test_cold_start_accepts_https_and_loopback_http_api_url(
        self, loopback_or_https_url: str, expected: str
    ) -> None:
        cfg = parse_lakebase_block(
            _minimal_raw_block(
                cold_start={
                    "wake": "api",
                    "api_url": loopback_or_https_url,
                    "api_token_env": "LB_TOKEN",
                }
            )
        )
        assert cfg.cold_start.api_url == expected

    @pytest.mark.parametrize(
        "bad_env",
        [None, "", "123_TOKEN", "BAD-ENV-NAME", "A" * 129, "TOKEN\n", 42],
    )
    def test_cold_start_rejects_invalid_api_token_env(self, bad_env: Any) -> None:
        raw = _minimal_raw_block(
            cold_start={
                "wake": "api",
                "api_url": "https://cp.internal.example",
                "api_token_env": bad_env,
            }
        )
        with pytest.raises(ValueError):
            parse_lakebase_block(raw)

    def test_cold_start_wake_none_allows_omitting_api_fields(self) -> None:
        cfg = parse_lakebase_block(
            _minimal_raw_block(
                cold_start={
                    "wake": "none",
                    "max_query_wait_seconds": 3,
                    "max_wake_seconds": 15,
                    "retry_backoff_ms": [100, 200],
                }
            )
        )
        assert cfg.cold_start.wake == "none"
        assert cfg.cold_start.api_url is None
        assert cfg.cold_start.api_token_env is None
        assert cfg.cold_start.retry_backoff_ms == (100, 200)

    @pytest.mark.parametrize(
        "bad_backoff",
        [[], [0], [-100], [True], ["500"], "500", 500, {"a": 1}],
    )
    def test_cold_start_invalid_retry_backoff_rejected(self, bad_backoff: Any) -> None:
        raw = _minimal_raw_block(
            cold_start={"wake": "none", "retry_backoff_ms": bad_backoff}
        )
        with pytest.raises(ValueError):
            parse_lakebase_block(raw)

    def test_freshness_sync_state_validation_and_disable(self) -> None:
        disabled = parse_lakebase_block(
            _minimal_raw_block(freshness={"sync_state_table": None})
        )
        assert disabled.freshness.sync_state_table is None

        unqualified = parse_lakebase_block(
            _minimal_raw_block(freshness={"sync_state_table": "_lakebase_sync_state"})
        )
        assert unqualified.freshness.sync_state_table == "_lakebase_sync_state"

        for bad_tbl in [
            "",
            "Public._lakebase_sync_state",
            "a.b.c",
            "public.bad-table",
            "public._lakebase_sync_state; DROP TABLE x",
        ]:
            with pytest.raises(ValueError):
                parse_lakebase_block(
                    _minimal_raw_block(freshness={"sync_state_table": bad_tbl})
                )

    def test_unknown_keys_rejected_in_all_blocks(self) -> None:
        with pytest.raises(ValueError, match="Unknown key"):
            parse_lakebase_block(_minimal_raw_block(extra_field=1))

        with pytest.raises(ValueError, match="Unknown key"):
            parse_lakebase_block(
                _minimal_raw_block(cold_start={"wake": "none", "bogus": 1})
            )

        with pytest.raises(ValueError, match="Unknown key"):
            parse_lakebase_block(_minimal_raw_block(freshness={"bogus": 1}))


# ---------------------------------------------------------------------------
# 2. Mandatory verify-full TLS Validation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FakeTLSObj:
    sslmode: str
    sslrootcert_file: str | None = None


class TestValidateLakebaseTLS:
    def test_accepts_verify_full_with_ca_dict_and_object(self) -> None:
        validate_lakebase_tls(
            {"sslmode": "verify-full", "sslrootcert_file": "/etc/seeknal/tls/ca.pem"}
        )
        validate_lakebase_tls(
            FakeTLSObj(sslmode="verify-full", sslrootcert_file="/etc/seeknal/tls/ca.pem")
        )

    @pytest.mark.parametrize(
        "bad_tls",
        [
            None,
            {},
            {"sslmode": "disable", "sslrootcert_file": "/etc/ca.pem"},
            {"sslmode": "prefer", "sslrootcert_file": "/etc/ca.pem"},
            {"sslmode": "require", "sslrootcert_file": "/etc/ca.pem"},
            {"sslmode": "verify-ca", "sslrootcert_file": "/etc/ca.pem"},
            {"sslmode": "verify-full"},
            {"sslmode": "verify-full", "sslrootcert_file": ""},
            {"sslmode": "verify-full", "sslrootcert_file": "   "},
            FakeTLSObj(sslmode="require", sslrootcert_file="/etc/ca.pem"),
            FakeTLSObj(sslmode="verify-full", sslrootcert_file=None),
        ],
    )
    def test_rejects_weaker_sslmode_or_missing_ca(self, bad_tls: Any) -> None:
        with pytest.raises(CustomConfigError):
            validate_lakebase_tls(bad_tls, eng_name="lb_ops", error_cls=CustomConfigError)


# ---------------------------------------------------------------------------
# 3. Effective Storage Class
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FakeConnCfg:
    kind: str
    storage_class: str | None = None
    lakebase: LakebaseEndpointConfig | None = None
    profile: str | None = None


class TestEffectiveStorageClass:
    def test_postgres_storage_class(self) -> None:
        assert effective_storage_class({"kind": "postgres"}) == "local"
        assert effective_storage_class(FakeConnCfg(kind="postgres")) == "local"
        assert (
            effective_storage_class({"kind": "postgres", "storage_class": "local"})
            == "local"
        )
        assert (
            effective_storage_class(
                {"kind": "postgres", "storage_class": "object_storage"}
            )
            == "object_storage"
        )
        assert (
            effective_storage_class(
                FakeConnCfg(kind="postgres", storage_class="object_storage")
            )
            == "object_storage"
        )

    def test_lakebase_with_block_fixed_object_storage(self) -> None:
        lb_cfg = parse_lakebase_block(_minimal_raw_block())
        assert effective_storage_class({"kind": "lakebase", "lakebase": _minimal_raw_block()}) == "object_storage"
        assert effective_storage_class(FakeConnCfg(kind="lakebase", lakebase=lb_cfg)) == "object_storage"
        assert effective_storage_class(lb_cfg) == "object_storage"

        for bad_sc in ("local", "object_storage"):
            with pytest.raises(ValueError, match="fixed to 'object_storage'"):
                effective_storage_class(
                    {
                        "kind": "lakebase",
                        "lakebase": _minimal_raw_block(),
                        "storage_class": bad_sc,
                    }
                )
            with pytest.raises(ValueError, match="fixed to 'object_storage'"):
                effective_storage_class(
                    FakeConnCfg(kind="lakebase", lakebase=lb_cfg, storage_class=bad_sc)
                )

    def test_legacy_lakebase_storage_class(self) -> None:
        assert effective_storage_class({"kind": "lakebase"}) == "unknown"
        assert effective_storage_class(FakeConnCfg(kind="lakebase")) == "unknown"
        assert (
            effective_storage_class({"kind": "lakebase", "storage_class": "local"})
            == "local"
        )
        assert (
            effective_storage_class(
                FakeConnCfg(kind="lakebase", storage_class="object_storage")
            )
            == "object_storage"
        )

    def test_duckdb_profiles_storage_class(self) -> None:
        assert effective_storage_class({"kind": "duckdb"}) == "local"
        assert (
            effective_storage_class({"kind": "duckdb", "profile": "local_file"})
            == "local"
        )
        assert (
            effective_storage_class(
                {
                    "kind": "duckdb",
                    "profile": "local_file",
                    "storage_class": "object_storage",
                }
            )
            == "object_storage"
        )
        assert (
            effective_storage_class({"kind": "duckdb", "profile": "iceberg_direct"})
            == "object_storage"
        )
        assert (
            effective_storage_class(
                FakeConnCfg(kind="duckdb", profile="iceberg_direct")
            )
            == "object_storage"
        )
        with pytest.raises(ValueError):
            effective_storage_class(
                {
                    "kind": "duckdb",
                    "profile": "iceberg_direct",
                    "storage_class": "local",
                }
            )
        with pytest.raises(ValueError):
            effective_storage_class(
                FakeConnCfg(
                    kind="duckdb",
                    profile="iceberg_direct",
                    storage_class="local",
                )
            )

    def test_starrocks_and_cube_fail_closed_to_object_storage(self) -> None:
        for kind in ("starrocks", "cube"):
            assert effective_storage_class({"kind": kind}) == "object_storage"
            assert effective_storage_class(FakeConnCfg(kind=kind)) == "object_storage"
            assert (
                effective_storage_class({"kind": kind, "storage_class": "local"})
                == "local"
            )
            assert (
                effective_storage_class(FakeConnCfg(kind=kind, storage_class="local"))
                == "local"
            )


# ---------------------------------------------------------------------------
# 4. Role Guard
# ---------------------------------------------------------------------------


class TestRoleGuard:
    def test_role_guard_sql_variants(self) -> None:
        from seeknal.serving.lakebase_endpoint import DEFAULT_DENY_ROLES

        assert DEFAULT_DENY_ROLES == (
            "neon_superuser",
            "rds_superuser",
            "cloudsqlsuperuser",
            "azure_pg_admin",
        )

        lb_sql = role_guard_sql("lakebase")
        assert "rolsuper" in lb_sql
        assert "rolbypassrls" in lb_sql
        assert "session_user" in lb_sql
        assert "pg_has_role(current_user, r.oid, 'MEMBER')" in lb_sql
        assert "pg_has_role(session_user, r.oid, 'MEMBER')" in lb_sql
        for denied in DEFAULT_DENY_ROLES:
            assert f"'{denied}'" in lb_sql

        pg_sql = role_guard_sql("postgres")
        assert "rolsuper" in pg_sql
        assert "rolbypassrls" in pg_sql
        assert "session_user" in pg_sql
        assert "pg_has_role(current_user, r.oid, 'MEMBER')" in pg_sql
        assert "pg_has_role(session_user, r.oid, 'MEMBER')" in pg_sql
        for denied in DEFAULT_DENY_ROLES:
            assert f"'{denied}'" in pg_sql

        empty_deny_sql = role_guard_sql("postgres", deny_roles=())
        assert "neon_superuser" not in empty_deny_sql
        assert "rds_superuser" not in empty_deny_sql

        custom_sql = role_guard_sql(
            "postgres", deny_roles=("custom_dba_role", "tenant-admin")
        )
        assert "'custom_dba_role'" in custom_sql
        assert "'tenant-admin'" in custom_sql
        assert "neon_superuser" not in custom_sql

        with pytest.raises(ValueError):
            role_guard_sql("duckdb")
        with pytest.raises(ValueError):
            role_guard_sql("postgres", deny_roles=("bad'; DROP TABLE x; --",))

    def test_check_role_guard_refuses_default_and_custom_deny_roles(self) -> None:
        from tests.serving.fake_postgres_catalog import FakePostgresCatalog, FakeRoleInfo

        for cloud_admin in (
            "neon_superuser",
            "rds_superuser",
            "cloudsqlsuperuser",
            "azure_pg_admin",
        ):
            cat = FakePostgresCatalog()
            cat.set_role(
                rolname="app_login",
                member_roles=[FakeRoleInfo(rolname=cloud_admin)],
            )
            conn = cat.connect()
            with pytest.raises(EngineError, match="Role guard refused connection"):
                check_role_guard(conn, "postgres")
            assert conn.closed is True

        # Custom deny_roles refuses custom role and allows when not in deny_roles
        cat_custom = FakePostgresCatalog()
        cat_custom.set_role(
            rolname="app_login",
            member_roles=[FakeRoleInfo(rolname="platform_dba")],
        )
        conn_allowed = cat_custom.connect()
        check_role_guard(conn_allowed, "postgres")
        assert conn_allowed.closed is False

        conn_denied = cat_custom.connect()
        with pytest.raises(EngineError, match="Role guard refused connection"):
            check_role_guard(
                conn_denied,
                "postgres",
                deny_roles=("platform_dba", "rds_superuser"),
            )
        assert conn_denied.closed is True

    def test_check_role_guard_refuses_session_user_and_inherited_role_memberships(
        self,
    ) -> None:
        from tests.serving.fake_postgres_catalog import FakePostgresCatalog, FakeRoleInfo

        # 1. Privileged session_user after SET ROLE unpriv -> refused
        cat_sess = FakePostgresCatalog()
        cat_sess.set_role(
            rolname="unpriv_current",
            session_role=FakeRoleInfo(rolname="orig_admin", rolsuper=True),
        )
        conn_sess = cat_sess.connect()
        with pytest.raises(EngineError, match="Role guard refused connection"):
            check_role_guard(conn_sess, "postgres")
        assert conn_sess.closed is True

        # 2. Granted BYPASSRLS group role via pg_has_role(..., r.oid, 'MEMBER') -> refused
        cat_mem = FakePostgresCatalog()
        cat_mem.set_role(
            rolname="login_role",
            member_roles=[FakeRoleInfo(rolname="bypass_grp", rolbypassrls=True)],
        )
        conn_mem = cat_mem.connect()
        with pytest.raises(EngineError, match="Role guard refused connection"):
            check_role_guard(conn_mem, "postgres")
        assert conn_mem.closed is True

        # 3. session_user with neon_superuser on lakebase -> refused
        cat_lb = FakePostgresCatalog()
        cat_lb.set_role(
            rolname="unpriv_current",
            session_role=FakeRoleInfo(rolname="api_owner", neon_superuser=True),
        )
        conn_lb = cat_lb.connect()
        with pytest.raises(EngineError, match="Role guard refused connection"):
            check_role_guard(conn_lb, "lakebase")
        assert conn_lb.closed is True

    def test_check_role_guard_allows_least_privilege_sql_role(self) -> None:
        lb_conn = FakeDBConnection(next_row=(False, False, False))
        check_role_guard(lb_conn, "lakebase")
        assert not lb_conn.closed
        assert len(lb_conn.executed) == 1

        pg_conn = FakeDBConnection(next_row=(False, False))
        check_role_guard(pg_conn, "postgres")
        assert not pg_conn.closed

    @pytest.mark.parametrize(
        ("kind", "row"),
        [
            ("lakebase", (True, False, False)),   # rolsuper
            ("lakebase", (False, True, False)),   # rolbypassrls
            ("lakebase", (False, False, True)),   # API-created neon_superuser member
            ("postgres", (True, False)),
            ("postgres", (False, True)),
        ],
    )
    def test_check_role_guard_refuses_privileged_roles_and_closes_conn(
        self, kind: str, row: tuple[bool, ...]
    ) -> None:
        conn = FakeDBConnection(next_row=row)
        with pytest.raises(
            EngineError,
            match="Role guard refused connection: role has superuser, BYPASSRLS, or neon_superuser privileges",
        ):
            check_role_guard(conn, kind)
        assert conn.closed is True

    def test_check_role_guard_fails_closed_on_query_error_or_empty_row(self) -> None:
        err_conn = FakeDBConnection(query_exc=RuntimeError("db error"))
        with pytest.raises(EngineError):
            check_role_guard(err_conn, "lakebase")
        assert err_conn.closed is True

        empty_conn = FakeDBConnection(next_row=None)
        with pytest.raises(EngineError):
            check_role_guard(empty_conn, "lakebase")
        assert empty_conn.closed is True


# ---------------------------------------------------------------------------
# 5. Read-Only Snapshot Freshness
# ---------------------------------------------------------------------------


class TestSnapshotFreshness:
    def test_returns_none_for_read_write_without_querying(self) -> None:
        rw_cfg = parse_lakebase_block(_minimal_raw_block(endpoint_type="read_write"))
        conn = FakeDBConnection(next_row=(datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc),))
        assert snapshot_freshness(conn, rw_cfg) is None
        assert snapshot_freshness(conn, FakeConnCfg(kind="lakebase", lakebase=rw_cfg)) is None
        assert conn.executed == []

    def test_computes_postmaster_age_for_read_only_endpoint(self) -> None:
        ro_cfg = parse_lakebase_block(_minimal_raw_block(endpoint_type="read_only"))
        start_ts = datetime(2026, 11, 19, 10, 0, 0, tzinfo=timezone.utc)
        now_ts = start_ts + timedelta(seconds=125.5)

        conn = FakeDBConnection(next_row=(start_ts,))
        lag = snapshot_freshness(conn, ro_cfg, now=now_ts)
        assert lag == pytest.approx(125.5)
        assert conn.executed == [("SELECT pg_catalog.pg_postmaster_start_time()", None)]

        # Also works when passed wrapper conn_cfg and epoch float `now` + ISO timestamp
        conn_iso = FakeDBConnection(next_row=("2026-11-19T10:00:00+00:00",))
        lag_iso = snapshot_freshness(
            conn_iso,
            FakeConnCfg(kind="lakebase", lakebase=ro_cfg),
            now=now_ts.timestamp(),
        )
        assert lag_iso == pytest.approx(125.5)

        # Clamps negative clock skew to 0.0
        conn_skew = FakeDBConnection(next_row=(start_ts,))
        assert (
            snapshot_freshness(conn_skew, ro_cfg, now=start_ts - timedelta(seconds=5))
            == 0.0
        )


# ---------------------------------------------------------------------------
# 6. ColdStartConnector & 20-Waiter Single-Flight Oracle
# ---------------------------------------------------------------------------


def _make_wake_cfg(
    *,
    wake: str = "api",
    max_query_wait_seconds: float = 10.0,
    max_wake_seconds: float = 60.0,
    retry_backoff_ms: list[int] | None = None,
) -> LakebaseEndpointConfig:
    cs: dict[str, Any] = {
        "wake": wake,
        "connect_timeout_seconds": 5.0,
        "max_query_wait_seconds": max_query_wait_seconds,
        "max_wake_seconds": max_wake_seconds,
        "retry_backoff_ms": retry_backoff_ms or [500, 1000, 2000, 4000],
    }
    if wake == "api":
        cs["api_url"] = "https://lakebase-cp.internal.example"
        cs["api_token_env"] = "LB_OPS_PROJECT_TOKEN"
    return parse_lakebase_block(_minimal_raw_block(cold_start=cs))


class TestColdStartConnector:
    def teardown_method(self) -> None:
        clear_registered_config_secrets()

    def test_engine_starting_error_contract(self) -> None:
        err = EngineStartingError()
        assert isinstance(err, EngineError)
        assert err.code == "engine_starting"
        assert err.status_code == 503
        assert err.retry_after == 3
        assert err.headers == {"Retry-After": "3"}

        err_custom = EngineStartingError(retry_after=5)
        assert err_custom.retry_after == 5
        assert err_custom.headers == {"Retry-After": "5"}

    def test_warm_connect_succeeds_without_wake(self) -> None:
        clock = FakeClock()
        metrics = FakeMetrics()
        http_calls: list[str] = []
        client = httpx.Client(
            transport=httpx.MockTransport(
                lambda req: (
                    http_calls.append(str(req.url))
                    or httpx.Response(200, json={"state": "running"})
                )
            )
        )
        cfg = _make_wake_cfg()

        connector = ColdStartConnector(
            lambda **kw: FakeDBConnection(conn_id=7),
            cfg,
            metrics=metrics,
            http_client=client,
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-warm"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        conn = connector.connect(connect_timeout=5)
        assert conn.conn_id == 7
        assert connector.state == "ok"
        assert connector.last_cold_start is False
        assert http_calls == []
        assert metrics.wakes == []
        assert metrics.states == [("lb_ops", "ok")]

    def test_single_flight_api_wake_polls_and_retries_connect(self) -> None:
        clock = FakeClock()
        metrics = FakeMetrics()
        start_calls = 0
        poll_calls = 0
        endpoint_running = False

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal start_calls, poll_calls, endpoint_running
            assert request.headers.get("Authorization") == "Bearer token-123"
            if request.method == "POST" and request.url.path.endswith("/start"):
                start_calls += 1
                return httpx.Response(202, json={"state": "starting"})
            if request.method == "GET":
                poll_calls += 1
                if poll_calls < 3:
                    return httpx.Response(200, json={"endpoint": {"state": "starting"}})
                endpoint_running = True
                return httpx.Response(200, json={"endpoint": {"state": "running"}})
            return httpx.Response(404)

        post_running_connect_attempts = 0

        def connect_fn(**kwargs: Any) -> FakeDBConnection:
            nonlocal post_running_connect_attempts
            if not endpoint_running:
                raise ConnectionRefusedError("endpoint suspended")
            post_running_connect_attempts += 1
            if post_running_connect_attempts == 1:
                raise ConnectionRefusedError("compute router warming up")
            return FakeDBConnection(conn_id=42)

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "token-123"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        conn = connector.connect()
        assert conn.conn_id == 42
        assert connector.state == "ok"
        assert connector.last_cold_start is True
        assert start_calls == 1
        assert poll_calls == 3
        assert metrics.wakes == [("lb_ops", "ok")]
        assert len(metrics.connect_retries) == 1
        assert len(metrics.cold_start_durations) == 1
        assert metrics.cold_start_durations[0][0] == "lb_ops"
        assert metrics.cold_start_durations[0][1] == pytest.approx(2.0)  # 0.5 + 1.0 + 0.5
        assert ("lb_ops", "starting") in metrics.states
        assert metrics.states[-1] == ("lb_ops", "ok")

    def test_twenty_concurrent_callers_share_single_flight_wake_and_respect_deadlines(
        self,
    ) -> None:
        """Oracle: 20 concurrent callers trigger ONE POST /start and ONE poll sequence."""
        clock = FakeClock(initial=500.0)
        metrics = FakeMetrics()
        lock = threading.Lock()
        post_count = 0
        get_count = 0
        is_running = False
        next_conn_id = 0

        all_entered = threading.Barrier(20)
        wake_gate = threading.Event()
        post_started = threading.Event()

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_count, get_count, is_running
            if request.method == "POST":
                with lock:
                    post_count += 1
                post_started.set()
                # Hold wake until all 20 callers are waiting on the single-flight op
                wake_gate.wait(timeout=2.0)
                return httpx.Response(202, json={"state": "starting"})
            if request.method == "GET":
                with lock:
                    get_count += 1
                    current_get = get_count
                if current_get < 3:
                    return httpx.Response(200, json={"state": "starting"})
                with lock:
                    is_running = True
                return httpx.Response(200, json={"state": "running"})
            return httpx.Response(400)

        def connect_fn(**kwargs: Any) -> FakeDBConnection:
            nonlocal next_conn_id
            with lock:
                if not is_running:
                    raise ConnectionRefusedError("connection refused")
                next_conn_id += 1
                cid = next_conn_id
            return FakeDBConnection(conn_id=cid)

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(max_query_wait_seconds=10.0, max_wake_seconds=60.0),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "token-20-way"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        # 5 callers have tight per-request deadlines (0.6s), 15 callers have 10s deadlines.
        # Total wake takes 0.5s + 1.0s = 1.5s on FakeClock.
        results: list[Any] = [None] * 20

        def worker(idx: int) -> None:
            my_deadline = 500.6 if idx < 5 else 510.0
            all_entered.wait(timeout=2.0)
            try:
                conn = connector.connect(deadline=my_deadline)
                finish_t = clock()
                results[idx] = ("ok", conn.conn_id, finish_t, my_deadline)
            except EngineStartingError as exc:
                finish_t = clock()
                results[idx] = ("starting", exc, finish_t, my_deadline)

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            futs = [pool.submit(worker, i) for i in range(20)]
            assert post_started.wait(timeout=2.0)
            for _ in range(200):
                with connector._lock:
                    if (
                        connector._active_wake is not None
                        and connector._active_wake.active_waiters == 20
                    ):
                        break
                threading.Event().wait(0.002)
            wake_gate.set()
            for f in futs:
                f.result(timeout=5.0)

        assert post_count == 1
        assert get_count == 3
        assert metrics.wakes == [("lb_ops", "ok")]

        # Tight-deadline callers got EngineStartingError without exceeding their deadline
        for idx in range(5):
            tag, exc, finish_t, my_deadline = results[idx]
            assert tag == "starting"
            assert isinstance(exc, EngineStartingError)
            assert exc.code == "engine_starting"
            assert exc.status_code == 503
            assert exc.retry_after >= 1
            assert finish_t <= my_deadline + 1.0  # bounded by discrete fake sleep step

        # Ample-deadline callers all received distinct live connections
        conn_ids = set()
        for idx in range(5, 20):
            tag, cid, finish_t, my_deadline = results[idx]
            assert tag == "ok"
            assert finish_t <= my_deadline
            conn_ids.add(cid)
        assert len(conn_ids) == 15

    def test_query_wait_budget_expires_with_engine_starting_while_background_wake_completes(
        self,
    ) -> None:
        clock = FakeClock(initial=100.0)
        metrics = FakeMetrics()
        is_running = False
        allow_finish = threading.Event()
        wake_started = threading.Event()
        opened_conns: list[FakeDBConnection] = []

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal is_running
            if request.method == "POST":
                wake_started.set()
                return httpx.Response(202, json={"state": "starting"})
            # First 4 polls return 'starting' (0.5 + 1.0 + 2.0 + 4.0 = 7.5s > max_query_wait 2.0s)
            if clock() < 103.0:
                return httpx.Response(200, json={"state": "starting"})
            allow_finish.wait(timeout=2.0)
            is_running = True
            return httpx.Response(200, json={"state": "running"})

        def connect_fn(**kwargs: Any) -> FakeDBConnection:
            if not is_running:
                raise ConnectionRefusedError("suspended")
            c = FakeDBConnection(conn_id=len(opened_conns) + 1)
            opened_conns.append(c)
            return c

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(max_query_wait_seconds=2.0, max_wake_seconds=60.0),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "token-bg"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        with pytest.raises(EngineStartingError) as exc_info:
            connector.connect()

        assert exc_info.value.code == "engine_starting"
        assert exc_info.value.status_code == 503
        assert exc_info.value.retry_after >= 1
        assert connector.state == "starting"

        # Let the background wake finish while zero callers are waiting -> orphan conn closed cleanly
        allow_finish.set()
        for _ in range(100):
            if connector.state == "ok":
                break
            threading.Event().wait(0.005)

        assert connector.state == "ok"
        assert len(opened_conns) == 1
        assert opened_conns[0].closed is True  # Unclaimed background probe connection was closed!

        # Subsequent caller connects immediately on warm endpoint
        c2 = connector.connect()
        assert c2.conn_id == 2
        assert c2.closed is False

    def test_wake_api_error_marks_engine_unavailable(self) -> None:
        clock = FakeClock()
        metrics = FakeMetrics()

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={"error": "control plane down"})

        connector = ColdStartConnector(
            lambda **kw: (_ for _ in ()).throw(ConnectionRefusedError("refused")),
            _make_wake_cfg(),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "token-err"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        with pytest.raises(EngineError, match="Engine connection failed") as exc_info:
            connector.connect()
        assert not isinstance(exc_info.value, EngineStartingError)
        assert connector.state == "unavailable"
        assert metrics.wakes == [("lb_ops", "error")]
        assert metrics.states[-1] == ("lb_ops", "unavailable")

    def test_exhausted_max_wake_seconds_marks_unavailable_with_timeout_outcome(
        self,
    ) -> None:
        clock = FakeClock()
        metrics = FakeMetrics()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST":
                return httpx.Response(202, json={"state": "starting"})
            return httpx.Response(200, json={"state": "starting"})

        connector = ColdStartConnector(
            lambda **kw: (_ for _ in ()).throw(ConnectionRefusedError("refused")),
            _make_wake_cfg(max_query_wait_seconds=30.0, max_wake_seconds=3.0),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "token-timeout"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        with pytest.raises(EngineError, match="Engine connection failed") as exc_info:
            connector.connect()
        assert not isinstance(exc_info.value, EngineStartingError)
        assert connector.state == "unavailable"
        assert metrics.wakes == [("lb_ops", "timeout")]
        assert metrics.states[-1] == ("lb_ops", "unavailable")

    def test_wake_none_skips_http_and_uses_bounded_connect_retry(self) -> None:
        clock = FakeClock()
        metrics = FakeMetrics()
        http_called = False

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal http_called
            http_called = True
            return httpx.Response(500)

        attempts = 0

        def connect_fn(**kwargs: Any) -> FakeDBConnection:
            nonlocal attempts
            attempts += 1
            if attempts < 4:
                raise ConnectionRefusedError("restarting")
            return FakeDBConnection(conn_id=99)

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(wake="none", max_query_wait_seconds=10.0, max_wake_seconds=15.0),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            clock=clock,
            sleep_fn=clock.sleep,
        )

        conn = connector.connect()
        assert conn.conn_id == 99
        assert http_called is False
        assert connector.state == "ok"
        assert connector.last_cold_start is True
        assert metrics.wakes == [("lb_ops", "ok")]
        assert len(metrics.connect_retries) == 3

    def test_control_plane_token_never_leaks_in_logs_errors_or_metrics(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        canary_token = "SECRET_LB_CP_JWT_9988776655"
        clock = FakeClock()
        metrics = FakeMetrics()

        def failing_transport(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError(
                f"Failed to connect with header Authorization: Bearer {canary_token}"
            )

        connector = ColdStartConnector(
            lambda **kw: (_ for _ in ()).throw(ConnectionRefusedError("refused")),
            _make_wake_cfg(),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(failing_transport)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": canary_token},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        with caplog.at_level(logging.DEBUG):
            with pytest.raises(EngineError) as exc_info:
                connector.connect()

        assert canary_token not in str(exc_info.value)
        assert canary_token not in repr(exc_info.value)
        assert canary_token not in caplog.text
        assert canary_token not in repr(metrics.wakes)
        assert canary_token not in repr(metrics.states)
        # Also verify global error scrubber registered the token
        assert canary_token not in scrub_error_detail(RuntimeError(canary_token))

    def test_auth_role_guard_show_and_tls_errors_never_trigger_wake(self) -> None:
        from seeknal.serving.adapters import PostgresSessionHardeningError

        class BadPasswordError(Exception):
            pgcode = "28P01"

        planted_pw = "SECRET_BAD_PW_28P01_777"
        non_wakeable_errors = [
            BadPasswordError(f'FATAL: password authentication failed for user "svc" ({planted_pw})'),
            PostgresSessionHardeningError("Role guard refused connection"),
            PostgresSessionHardeningError("Session hardening SHOW verification failed"),
            RuntimeError("SSL error: certificate verify failed"),
        ]

        for err in non_wakeable_errors:
            clock = FakeClock()
            metrics = FakeMetrics()
            http_calls: list[str] = []
            client = httpx.Client(
                transport=httpx.MockTransport(
                    lambda req: (
                        http_calls.append(f"{req.method} {req.url}")
                        or httpx.Response(202, json={"state": "starting"})
                    )
                )
            )
            connector = ColdStartConnector(
                lambda **kw: (_ for _ in ()).throw(err),
                _make_wake_cfg(),
                metrics=metrics,
                http_client=client,
                engine_name="lb_ops",
                env={"LB_OPS_PROJECT_TOKEN": "tok-no-wake"},
                clock=clock,
                sleep_fn=clock.sleep,
            )

            with pytest.raises(EngineError) as exc_info:
                connector.connect()

            assert not isinstance(exc_info.value, EngineStartingError)
            assert http_calls == []
            assert metrics.wakes == []
            assert connector.state == "ok"
            assert planted_pw not in str(exc_info.value)

    def test_failed_wake_negative_cache_suppresses_new_wakes_until_cooldown_expires(
        self,
    ) -> None:
        clock = FakeClock(initial=1000.0)
        metrics = FakeMetrics()
        post_calls = 0
        cp_healthy = False
        db_running = False

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_calls, db_running
            if request.method == "POST":
                post_calls += 1
                if not cp_healthy:
                    return httpx.Response(503, json={"error": "outage"})
                return httpx.Response(202, json={"state": "starting"})
            db_running = True
            return httpx.Response(200, json={"state": "running"})

        def connect_fn(**kwargs: Any) -> FakeDBConnection:
            if not db_running:
                raise ConnectionRefusedError("endpoint suspended")
            return FakeDBConnection(conn_id=77)

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(max_query_wait_seconds=10.0, max_wake_seconds=30.0),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-cooldown"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        # 1st request fails wake -> enters 30s negative cache cooldown
        with pytest.raises(EngineError, match="Engine connection failed"):
            connector.connect()
        assert post_calls == 1
        assert connector.state == "unavailable"

        # Requests during the 30s cooldown fail immediately with ZERO new POST /start calls!
        clock.advance(10.0)
        with pytest.raises(EngineError, match="Engine connection failed"):
            connector.connect()
        clock.advance(19.9)
        with pytest.raises(EngineError, match="Engine connection failed"):
            connector.connect()
        assert post_calls == 1
        assert metrics.wakes == [("lb_ops", "error")]

        # Once cooldown (30s) expires and control plane recovers, a single new wake succeeds
        cp_healthy = True
        clock.advance(0.2)
        conn = connector.connect()
        assert conn.conn_id == 77
        assert post_calls == 2
        assert connector.state == "ok"

    def test_shared_connector_across_distinct_account_connect_fns_isolates_connections(
        self,
    ) -> None:
        clock = FakeClock(initial=200.0)
        post_calls = 0
        running = False
        barrier = threading.Barrier(2)
        wake_gate = threading.Event()
        post_entered = threading.Event()

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_calls, running
            if request.method == "POST":
                post_calls += 1
                post_entered.set()
                wake_gate.wait(timeout=2.0)
                return httpx.Response(202, json={"state": "starting"})
            running = True
            return httpx.Response(200, json={"state": "running"})

        def connect_interactive(**kw: Any) -> FakeDBConnection:
            if not running:
                raise ConnectionRefusedError("suspended")
            c = FakeDBConnection(conn_id=101)
            c.role_tag = "interactive"  # type: ignore[attr-defined]
            return c

        def connect_bi(**kw: Any) -> FakeDBConnection:
            if not running:
                raise ConnectionRefusedError("suspended")
            c = FakeDBConnection(conn_id=202)
            c.role_tag = "bi"  # type: ignore[attr-defined]
            return c

        connector = ColdStartConnector(
            connect_interactive,
            _make_wake_cfg(),
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-shared-ep"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        out: dict[str, Any] = {}

        def call_acc(name: str, fn: Any) -> None:
            barrier.wait(timeout=2.0)
            out[name] = connector.connect(connect_fn=fn)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(call_acc, "interactive", connect_interactive)
            f2 = ex.submit(call_acc, "bi", connect_bi)
            assert post_entered.wait(timeout=2.0)
            wake_gate.set()
            f1.result(timeout=5.0)
            f2.result(timeout=5.0)

        assert post_calls == 1
        assert out["interactive"].role_tag == "interactive"
        assert out["bi"].role_tag == "bi"

    def test_wake_failure_published_state_transition_is_deterministic_and_cannot_remain_starting(
        self,
    ) -> None:
        """Prove that after a wake failure, published metrics state cannot stick or regress to 'starting'."""
        clock = FakeClock(initial=100.0)
        metrics = FakeMetrics()
        post_calls = 0
        wake_started = threading.Event()
        wake_finished = threading.Event()

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_calls
            if request.method == "POST":
                post_calls += 1
                wake_started.set()
                return httpx.Response(503, json={"error": "control plane outage"})
            return httpx.Response(503, json={"error": "control plane outage"})

        def connect_fn(**kwargs: Any) -> FakeDBConnection:
            raise ConnectionRefusedError("endpoint down")

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(max_query_wait_seconds=10.0, max_wake_seconds=60.0),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-bm5"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        with pytest.raises(EngineError, match="Engine connection failed"):
            connector.connect()

        # The waiter unblocks only AFTER state is published under connector lock.
        # It must be unavailable, never sticking in 'starting'.
        assert post_calls == 1
        assert connector.state == "unavailable"
        assert metrics.states[-1] == ("lb_ops", "unavailable")
        assert ("lb_ops", "starting") in metrics.states
        # Verify sequence: starting followed by unavailable, nothing after
        assert metrics.states == [("lb_ops", "starting"), ("lb_ops", "unavailable")]

        if connector._last_wake_worker is not None and connector._last_wake_worker.is_alive():
            connector._last_wake_worker.join(timeout=2.0)

    def test_f4_readyz_probe_no_wake_sets_local_statement_timeout_and_no_wake(
        self,
    ) -> None:
        """Prove probe_no_wake sets SET LOCAL statement_timeout in ms and makes zero wake calls."""
        clock = FakeClock(initial=100.0)
        post_calls = 0
        recorded_connect_timeouts: list[Any] = []
        recording_conn = FakeDBConnection(conn_id=909)

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_calls
            if request.method == "POST":
                post_calls += 1
            return httpx.Response(200, json={"state": "running"})

        def connect_fn(*, connect_timeout: Any = None, **kwargs: Any) -> FakeDBConnection:
            recorded_connect_timeouts.append(connect_timeout)
            return recording_conn

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(max_query_wait_seconds=10.0, max_wake_seconds=60.0),
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-f4"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        with connector._lock:
            connector._state = "unavailable"

        probe_state = connector.probe_no_wake(timeout_seconds=2.5)
        assert probe_state == "ok"
        assert post_calls == 0  # Zero wake calls!

        # Connect timeout must be no longer than probe timeout (2.5)
        assert len(recorded_connect_timeouts) == 1
        assert recorded_connect_timeouts[0] <= 2.5

        # Check executed statements on fake connection:
        # BEGIN, SELECT set_config('statement_timeout', %s, true), SELECT 1, ROLLBACK
        stmts = [item[0] for item in recording_conn.executed]
        assert stmts == [
            "BEGIN",
            "SELECT set_config('statement_timeout', %s, true)",
            "SELECT 1",
            "ROLLBACK",
        ]
        assert recording_conn.executed[1][1] == ("2500",)

        # Also test via ReadinessProbe /readyz flow
        from seeknal.serving.adapters import DBAPIAdapter
        from seeknal.serving.ops import ReadinessProbe, create_ops_app
        from starlette.testclient import TestClient

        recording_conn2 = FakeDBConnection(conn_id=910)

        def connect_fn2(*, connect_timeout: Any = None, **kwargs: Any) -> FakeDBConnection:
            return recording_conn2

        connector2 = ColdStartConnector(
            connect_fn2,
            _make_wake_cfg(max_query_wait_seconds=10.0, max_wake_seconds=60.0),
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-f4-2"},
            clock=clock,
            sleep_fn=clock.sleep,
        )
        with connector2._lock:
            connector2._state = "unavailable"

        adapter = DBAPIAdapter(
            lambda **kw: connector2.connect(**kw),
            kind="lakebase",
            cold_start_connector=connector2,
        )
        readiness = ReadinessProbe(
            {"lb_ops": adapter},
            timeout_seconds=1.5,
            cache_seconds=0.0,
            clock=clock,
        )
        ops_app = create_ops_app(readiness=readiness)
        with TestClient(ops_app) as client:
            resp = client.get("/readyz")
            assert resp.status_code == 200
            assert resp.json()["ready"] is True
            assert post_calls == 0
            stmts2 = [item[0] for item in recording_conn2.executed]
            assert stmts2 == [
                "BEGIN",
                "SELECT set_config('statement_timeout', %s, true)",
                "SELECT 1",
                "ROLLBACK",
            ]
            assert recording_conn2.executed[1][1] == ("1500",)

    @pytest.mark.parametrize("run_idx", range(200))
    def test_b_m5_plain_connect_during_cooldown_recovers_state_without_new_wake(
        self,
        run_idx: int,
    ) -> None:
        clock = FakeClock(initial=500.0)
        metrics = FakeMetrics()
        post_calls = 0
        db_running = False

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_calls
            if request.method == "POST":
                post_calls += 1
                return httpx.Response(503, json={"error": "control plane outage"})
            return httpx.Response(503, json={"error": "control plane outage"})

        def connect_fn(**kwargs: Any) -> FakeDBConnection:
            if not db_running:
                raise ConnectionRefusedError("endpoint down")
            return FakeDBConnection(conn_id=505)

        connector = ColdStartConnector(
            connect_fn,
            _make_wake_cfg(max_query_wait_seconds=10.0, max_wake_seconds=60.0),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-bm5"},
            clock=clock,
            sleep_fn=clock.sleep,
        )

        # 1. Failed wake puts connector into "unavailable" with 60s cooldown
        with pytest.raises(EngineError, match="Engine connection failed"):
            connector.connect()
        assert post_calls == 1
        assert connector.state == "unavailable"
        assert metrics.states[-1] == ("lb_ops", "unavailable")

        if connector._last_wake_worker is not None and connector._last_wake_worker.is_alive():
            connector._last_wake_worker.join(timeout=2.0)

        # 2. Still down during cooldown (t + 5s): plain connect fails, wake suppressed
        clock.advance(5.0)
        with pytest.raises(EngineError, match="Engine connection failed"):
            connector.connect()
        assert post_calls == 1
        assert connector.state == "unavailable"

        # 3. DB comes up during cooldown (t + 10s, well before 60s cooldown expires):
        # plain connect succeeds immediately without calling POST .../start!
        db_running = True
        clock.advance(5.0)
        conn = connector.connect()
        assert conn.conn_id == 505
        assert post_calls == 1
        assert connector.state == "ok"
        assert connector.last_cold_start is False
        assert connector.consume_thread_cold_start() is False
        assert metrics.states[-1] == ("lb_ops", "ok")

    def test_b_l3_bad_password_on_bi_account_does_not_fail_interactive_waiter_or_poison_state(
        self,
    ) -> None:
        import time

        clock = FakeClock(initial=800.0)
        metrics = FakeMetrics()
        post_calls = 0
        running = False
        post_entered = threading.Event()
        wake_gate = threading.Event()
        planted_bi_pw = "SECRET_BAD_BI_PW_28P01_8899"

        class BadPasswordError(Exception):
            pgcode = "28P01"

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal post_calls, running
            if request.method == "POST":
                post_calls += 1
                post_entered.set()
                wake_gate.wait(timeout=2.0)
                return httpx.Response(202, json={"state": "starting"})
            running = True
            return httpx.Response(200, json={"state": "running"})

        def connect_bi(**kw: Any) -> FakeDBConnection:
            if not running:
                raise ConnectionRefusedError("endpoint suspended")
            raise BadPasswordError(
                f'FATAL: password authentication failed for user "bi_role" ({planted_bi_pw})'
            )

        def connect_interactive(**kw: Any) -> FakeDBConnection:
            if not running:
                raise ConnectionRefusedError("endpoint suspended")
            c = FakeDBConnection(conn_id=909)
            c.role_tag = "interactive"  # type: ignore[attr-defined]
            return c

        connector = ColdStartConnector(
            connect_bi,
            _make_wake_cfg(),
            metrics=metrics,
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            engine_name="lb_ops",
            env={"LB_OPS_PROJECT_TOKEN": "tok-bl3"},
            clock=clock,
            sleep_fn=clock.sleep,
        )
        connector.register_connect_fn(connect_interactive, preferred=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            # bi starts the wake as opener
            f_bi = ex.submit(lambda: connector.connect(connect_fn=connect_bi))
            assert post_entered.wait(timeout=2.0)
            # interactive joins the in-flight wake as a waiter
            f_int = ex.submit(
                lambda: connector.connect(connect_fn=connect_interactive)
            )
            time.sleep(0.02)
            wake_gate.set()

            with pytest.raises(EngineError, match="Engine authentication failed") as bi_exc:
                f_bi.result(timeout=5.0)
            assert planted_bi_pw not in str(bi_exc.value)

            int_conn = f_int.result(timeout=5.0)
            assert int_conn.conn_id == 909
            assert int_conn.role_tag == "interactive"

        assert post_calls == 1
        assert connector.state == "ok"

        # Warm bad-password failure on bi still keeps connector.state == "ok" for interactive
        with pytest.raises(EngineError, match="Engine authentication failed"):
            connector.connect(connect_fn=connect_bi)
        assert connector.state == "ok"
        warm_int_conn = connector.connect(connect_fn=connect_interactive)
        assert warm_int_conn.conn_id == 909
        assert post_calls == 1


def test_l1_executor_validation_and_failed_launch_finalises_op():
    cfg = _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0)

    # 1. Unusable executor rejected with TypeError
    with pytest.raises(TypeError, match="executor must be callable or have a submit method"):
        ColdStartConnector(
            lambda **kw: None,
            cfg,
            executor="not_an_executor",
            engine_name="lb_l1",
        )

    # 2. Executor whose submit raises: waiters get a typed error promptly and state is "unavailable"
    class FailingExecutor:
        def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("threadpool submission failed")

    def failing_connect(**kw: Any) -> Any:
        raise ConnectionRefusedError("endpoint asleep")

    clock = FakeClock(initial=100.0)
    connector = ColdStartConnector(
        failing_connect,
        cfg,
        executor=FailingExecutor(),
        engine_name="lb_l1",
        env={"LB_OPS_PROJECT_TOKEN": "tok-l1"},
        clock=clock,
        sleep_fn=clock.sleep,
    )

    t0 = time.monotonic()
    with pytest.raises(EngineError) as exc_info:
        connector.connect()
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0
    assert "threadpool submission failed" in str(exc_info.value)
    assert connector.state == "unavailable"


def test_l2_plain_connect_failure_does_not_overwrite_newer_wake_state():
    """L2: A plain connect failing while a newer wake is in flight leaves state 'starting'."""
    cfg = _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0)

    entered = threading.Event()
    release = threading.Event()

    def connect_fn(**kw: Any) -> Any:
        entered.set()
        release.wait(timeout=2.0)
        raise ConnectionRefusedError("plain connect failed")

    connector = ColdStartConnector(
        connect_fn,
        cfg,
        engine_name="lb_l2",
        env={"LB_OPS_PROJECT_TOKEN": "tok-l2"},
    )

    from seeknal.serving.lakebase_endpoint import _WakeOperation

    # Initial wake that completed with ok (no claimed_conn so fallthrough to plain connect)
    initial_op = _WakeOperation(
        started_at=0.0,
        wake_deadline=10.0,
        connect_kwargs={},
        opener_connect_fn=connect_fn,
    )
    initial_op.done = True
    initial_op.outcome = "ok"
    initial_op.finished_at = 1.0

    newer_op = _WakeOperation(
        started_at=2.0,
        wake_deadline=12.0,
        connect_kwargs={},
        opener_connect_fn=connect_fn,
    )

    with connector._cond:
        connector._active_wake = initial_op

    def run() -> None:
        try:
            connector.connect(connect_fn=connect_fn)
        except EngineError:
            pass

    t = threading.Thread(target=run)
    t.start()
    assert entered.wait(timeout=2.0)

    # While plain connect is running, a newer wake is in flight:
    with connector._lock:
        connector._active_wake = newer_op
        connector._state = "starting"

    release.set()
    t.join(timeout=2.0)

    # State must remain "starting", not overwritten to "unavailable"
    assert connector.state == "starting"


def test_l3_probe_connect_timeout_never_above_probe_timeout():
    """L3: Connect timeout never above the probe timeout (sub-second probe timeout enforced via hard deadline)."""
    cfg = _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0)

    def slow_connect(**kw: Any) -> Any:
        time.sleep(1.0)
        return "connected"

    connector = ColdStartConnector(
        slow_connect,
        cfg,
        engine_name="lb_l3",
        env={"LB_OPS_PROJECT_TOKEN": "tok-l3"},
    )
    with connector._lock:
        connector._state = "unavailable"

    t0 = time.monotonic()
    state = connector.probe_no_wake(timeout_seconds=0.2)
    elapsed = time.monotonic() - t0

    assert elapsed < 0.5
    assert state == "unavailable"


def test_l3_positive_control_subsecond_probe_returns_ok():
    """M2 positive control: sub-second timeout, fast connect, SELECT 1 returns 'ok'."""
    recording_conn = FakeDBConnection(conn_id=42)

    connector = ColdStartConnector(
        lambda **kw: recording_conn,
        _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0),
        engine_name="lb_l3_pos",
        env={"LB_OPS_PROJECT_TOKEN": "tok-l3"},
    )
    with connector._lock:
        connector._state = "unavailable"

    state = connector.probe_no_wake(timeout_seconds=0.5)
    assert state == "ok"
    assert connector.state == "ok"
    stmts = [item[0] for item in recording_conn.executed]
    assert stmts == [
        "BEGIN",
        "SELECT set_config('statement_timeout', %s, true)",
        "SELECT 1",
        "ROLLBACK",
    ]


def test_l3_slow_connect_times_out_and_closes_late_connection():
    """M2: Slow connect beyond deadline gives 'unavailable' within timeout + 0.2s and late conn is closed."""
    late_conn = FakeDBConnection(conn_id=999)

    def slow_connect(**kw: Any) -> Any:
        time.sleep(0.3)
        return late_conn

    connector = ColdStartConnector(
        slow_connect,
        _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0),
        engine_name="lb_l3_slow",
        env={"LB_OPS_PROJECT_TOKEN": "tok-l3"},
    )
    with connector._lock:
        connector._state = "unavailable"

    t0 = time.monotonic()
    state = connector.probe_no_wake(timeout_seconds=0.1)
    elapsed = time.monotonic() - t0

    assert state == "unavailable"
    assert elapsed < 0.3  # within timeout + 0.2s

    # Wait for the background worker to finish and verify late_conn was closed
    time.sleep(0.35)
    assert late_conn.closed is True


def test_l3_multiple_candidates_bounded_by_single_overall_deadline():
    """M2: Several candidate functions are bounded by one overall deadline."""
    call_counts = [0, 0, 0]

    def make_candidate(idx: int):
        def _connect(**kw: Any) -> Any:
            call_counts[idx] += 1
            time.sleep(0.25)
            return FakeDBConnection(conn_id=idx)
        return _connect

    connector = ColdStartConnector(
        make_candidate(0),
        _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0),
        engine_name="lb_l3_multi",
        env={"LB_OPS_PROJECT_TOKEN": "tok-l3"},
    )
    connector.register_connect_fn(make_candidate(1))
    connector.register_connect_fn(make_candidate(2))

    with connector._lock:
        connector._state = "unavailable"

    t0 = time.monotonic()
    state = connector.probe_no_wake(timeout_seconds=0.15)
    elapsed = time.monotonic() - t0

    assert state == "unavailable"
    assert elapsed < 0.35  # bounded by single 0.15s deadline, not 3 * 0.15s
    assert call_counts[2] == 0



def test_l4_bad_timeout_inf_and_nan_do_not_disable_probe_forever(caplog):
    """L4: Bad timeout (inf, nan) returns 'unavailable', logs warning, and does not leave flag stuck."""
    def conn_fn(**kw: Any) -> Any:
        return FakeDBConnection()

    cfg = _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0)
    connector = ColdStartConnector(
        conn_fn,
        cfg,
        engine_name="lb_l4",
        env={"LB_OPS_PROJECT_TOKEN": "tok-l4"},
    )
    with connector._lock:
        connector._state = "unavailable"

    # Test inf: returns "unavailable", logs warning, does NOT leave flag stuck
    with caplog.at_level(logging.WARNING):
        res_inf = connector.probe_no_wake(timeout_seconds=float("inf"))
    assert res_inf == "unavailable"
    assert connector._no_wake_probe_in_flight is False
    assert any("Invalid probe timeout_seconds" in r.message for r in caplog.records)

    # Probe still works after inf
    state_after_inf = connector.probe_no_wake(timeout_seconds=2.0, min_interval_seconds=0.0)
    assert state_after_inf == "ok"

    # Test nan: returns "unavailable", logs warning, does NOT leave flag stuck
    with connector._lock:
        connector._state = "unavailable"
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        res_nan = connector.probe_no_wake(timeout_seconds=float("nan"))
    assert res_nan == "unavailable"
    assert connector._no_wake_probe_in_flight is False
    assert any("Invalid probe timeout_seconds" in r.message for r in caplog.records)

    # Probe still works after nan
    state_after_nan = connector.probe_no_wake(timeout_seconds=2.0, min_interval_seconds=0.0)
    assert state_after_nan == "ok"


def test_p1_probe_queue_never_grows_beyond_n():
    """P1: With a blocked connect and N = max_workers, run 50 probes; queue never exceeds N, and after release total connects <= N."""
    block_event = threading.Event()
    connect_started = 0
    lock = threading.Lock()

    def blocking_connect(**kw: Any) -> Any:
        nonlocal connect_started
        with lock:
            connect_started += 1
        block_event.wait()
        return FakeDBConnection()

    cfg = _make_wake_cfg(max_query_wait_seconds=5.0, max_wake_seconds=10.0)
    connector = ColdStartConnector(
        blocking_connect,
        cfg,
        engine_name="lb_p1",
        env={"LB_OPS_PROJECT_TOKEN": "tok-p1"},
    )
    with connector._lock:
        connector._state = "unavailable"

    n_workers = connector._probe_max_workers

    # Run 50 probes
    for _ in range(50):
        st = connector.probe_no_wake(timeout_seconds=0.02, min_interval_seconds=0.0)
        assert st == "unavailable"

    # Number of started connect calls never exceeds N
    with lock:
        assert connect_started <= n_workers

    # Release the block
    block_event.set()
    time.sleep(0.1)

    # After release, verify no further connects were queued or ran
    with lock:
        assert connect_started <= n_workers









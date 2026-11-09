"""Tests for Atlas Runtime P1-S5: governed connections cannot be overridden or bypassed.

Validates that in governed mode:
- PostgreSQL source requires a granted connection and refuses inline credentials {host, port, database, user, password, sslmode}
- StarRocks source requires a granted connection, loads vended profile, and refuses inline credentials
- Materialization dispatcher refuses inline host/credential overrides in target config
- File sources refuse URL schemes and paths resolving outside the project root
- Non-governed mode preserves inline credential overrides and bypasses (regression)
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.dag.manifest import Node, NodeType
from seeknal.runtime.catalog_io import (
    CatalogIO,
    GovernedConfigError,
    GovernedContext,
    get_catalog_io,
    set_catalog_io,
)
from seeknal.serving.grant_token import sign_grant_token
from seeknal.workflow.executors.base import ExecutionContext
from seeknal.workflow.executors.source_executor import SourceExecutor
from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher


def _make_jwks_and_grant(
    now: int | None = None,
    connections: list[dict[str, Any]] | None = None,
    **claims_overrides: Any,
) -> tuple[str, str]:
    """Generate a valid compact JWS run grant and matching JWKS JSON string."""
    t = int(time.time()) if now is None else int(now)
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    kid = "test-governed-key-1"

    numbers = pub.public_numbers()
    x_bytes = numbers.x.to_bytes(32, "big")
    y_bytes = numbers.y.to_bytes(32, "big")
    x_b64 = base64.urlsafe_b64encode(x_bytes).rstrip(b"=").decode("ascii")
    y_b64 = base64.urlsafe_b64encode(y_bytes).rstrip(b"=").decode("ascii")

    jwks = {
        "keys": [
            {
                "kty": "EC",
                "crv": "P-256",
                "kid": kid,
                "x": x_b64,
                "y": y_b64,
            }
        ]
    }

    claims = {
        "typ": "run",
        "iss": "atlas-catalog",
        "aud": "atlas-rt-run",
        "sub": "user:test-operator",
        "principal": "svc:orders:prod",
        "ws": "ws-prod",
        "jti": "550e8400-e29b-41d4-a716-446655440001",
        "release": "rel-2026-11",
        "plan_sha256": "0" * 64,
        "trigger": {"type": "schedule", "id": "daily-00"},
        "iat": t,
        "nbf": t,
        "exp": t + 3600,
        "reads": [],
        "writes": [],
        "connections": connections if connections is not None else [
            {"name": "pg_main", "actions": ["read", "write"]},
        ],
    }
    claims.update(claims_overrides)
    token = sign_grant_token(claims, priv, kid=kid)
    return token, json.dumps(jwks)


@pytest.fixture
def governed_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Set up governed environment with grant containing pg_main (read/write)."""
    token, jwks_json = _make_jwks_and_grant(
        connections=[{"name": "pg_main", "actions": ["read", "write"]}]
    )

    grant_file = tmp_path / "run_grant.jwt"
    grant_file.write_text(token, encoding="utf-8")
    grant_file.chmod(0o600)

    jwks_file = tmp_path / "jwks.json"
    jwks_file.write_text(jwks_json, encoding="utf-8")
    jwks_file.chmod(0o600)

    lakekeeper_token_file = tmp_path / "lakekeeper_token.txt"
    lakekeeper_token_file.write_text("test-token-12345\n", encoding="utf-8")
    lakekeeper_token_file.chmod(0o600)

    conns_dir = tmp_path / "connections"
    conns_dir.mkdir(mode=0o700, exist_ok=True)

    pg_main_data = {
        "host": "vended.pg.internal",
        "port": 5432,
        "database": "vended_db",
        "user": "vended_reader",
        "password": "vended_secret_password",
    }
    pg_main_file = conns_dir / "pg_main.json"
    pg_main_file.write_text(json.dumps(pg_main_data), encoding="utf-8")
    pg_main_file.chmod(0o600)

    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    monkeypatch.setenv("ATLAS_RUN_GRANT_FILE", str(grant_file))
    monkeypatch.setenv("ATLAS_RUN_GRANT_JWKS_FILE", str(jwks_file))
    monkeypatch.setenv("ATLAS_LAKEKEEPER_TOKEN_FILE", str(lakekeeper_token_file))
    monkeypatch.setenv("ATLAS_LAKEKEEPER_URI", "https://catalog.atlas.local/catalog")
    monkeypatch.setenv("ATLAS_LAKEKEEPER_WAREHOUSE", "wh")
    monkeypatch.setenv("ATLAS_RUN_CONTROLLER_URL", "https://catalog.atlas.local")
    monkeypatch.setenv("ATLAS_CONNECTIONS_DIR", str(conns_dir))

    for var in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "KEYCLOAK_CLIENT_SECRET",
        "LAKEKEEPER_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)

    ctx = GovernedContext.from_env()
    cio = CatalogIO(ctx)
    set_catalog_io(cio)

    try:
        yield {
            "ctx": ctx,
            "cio": cio,
            "conns_dir": conns_dir,
            "pg_main_file": pg_main_file,
            "pg_main_data": pg_main_data,
        }
    finally:
        set_catalog_io(None)


@pytest.fixture
def execution_context(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    return ExecutionContext(
        project_name="test_proj",
        workspace_path=project_dir,
        target_path=project_dir / "target",
        dry_run=False,
        verbose=False,
    )


# --------------------------------------------------------------------------
# 1. Inline host override refused; error does not contain password
# --------------------------------------------------------------------------

def test_governed_postgres_inline_host_override_refused_no_password(
    governed_env, execution_context
):
    """PostgreSQL source refuses inline host override and error does not leak password."""
    node = Node(
        id="source.pg_test",
        name="pg_test",
        node_type=NodeType.SOURCE,
        config={"source": "postgresql", "connection": "pg_main"},
    )
    executor = SourceExecutor(node, execution_context)

    secret_pw = "super_secret_attacker_pw_123"
    with pytest.raises(GovernedConfigError) as exc_info:
        executor._resolve_postgresql_config({
            "connection": "pg_main",
            "host": "evil.exfiltrate.com",
            "password": secret_pw,
        })

    err_msg = str(exc_info.value)
    assert "host" in err_msg
    assert secret_pw not in err_msg


def test_governed_postgres_inline_other_credentials_refused(
    governed_env, execution_context
):
    """PostgreSQL source refuses port, database, user, password, sslmode overrides."""
    node = Node(
        id="source.pg_test",
        name="pg_test",
        node_type=NodeType.SOURCE,
        config={"source": "postgresql", "connection": "pg_main"},
    )
    executor = SourceExecutor(node, execution_context)

    for forbidden_key in ("port", "database", "user", "password", "sslmode"):
        with pytest.raises(GovernedConfigError) as exc_info:
            executor._resolve_postgresql_config({
                "connection": "pg_main",
                forbidden_key: "forbidden_value",
            })
        err_msg = str(exc_info.value)
        assert forbidden_key in err_msg
        assert "forbidden_value" not in err_msg


def test_governed_materialization_dispatcher_refuses_inline_overrides(
    governed_env
):
    """Dispatcher refuses inline host/credential overrides in target config."""
    dispatcher = MaterializationDispatcher()
    con = duckdb.connect(":memory:")

    secret_pw = "mat_secret_password_xyz"
    target_config = {
        "type": "postgresql",
        "connection": "pg_main",
        "table": "public.orders",
        "host": "exfiltrate.internal",
        "password": secret_pw,
    }

    with pytest.raises(GovernedConfigError) as exc_info:
        dispatcher._materialize_postgresql(con, "v_orders", target_config)

    err_msg = str(exc_info.value)
    assert "host" in err_msg
    assert secret_pw not in err_msg


# --------------------------------------------------------------------------
# 2. Missing connection is refused for both PostgreSQL and StarRocks
# --------------------------------------------------------------------------

def test_governed_postgres_missing_connection_refused(
    governed_env, execution_context
):
    """PostgreSQL source without connection raises GovernedConfigError."""
    node = Node(
        id="source.pg_no_conn",
        name="pg_no_conn",
        node_type=NodeType.SOURCE,
        config={
            "source": "postgresql",
            "table": "orders",
            "params": {"host": "localhost", "port": 5432},
        },
    )
    executor = SourceExecutor(node, execution_context)

    with pytest.raises(GovernedConfigError, match="governed mode: database sources must name a granted connection"):
        executor._resolve_postgresql_config({"host": "localhost", "port": 5432})


def test_governed_starrocks_missing_connection_refused(
    governed_env, execution_context
):
    """StarRocks source without connection raises GovernedConfigError."""
    node = Node(
        id="source.sr_no_conn",
        name="sr_no_conn",
        node_type=NodeType.SOURCE,
        config={
            "source": "starrocks",
            "table": "orders",
            "params": {"host": "localhost", "port": 9030},
        },
    )
    executor = SourceExecutor(node, execution_context)
    con = duckdb.connect(":memory:")

    with pytest.raises(GovernedConfigError, match="governed mode: database sources must name a granted connection"):
        executor._load_starrocks(con, "orders", {"host": "localhost", "port": 9030})


# --------------------------------------------------------------------------
# 3. StarRocks source uses the vended profile
# --------------------------------------------------------------------------

def test_governed_starrocks_uses_vended_profile(
    governed_env, execution_context
):
    """StarRocks source loads host from vended profile (pg_main.json data)."""
    node = Node(
        id="source.sr_test",
        name="sr_test",
        node_type=NodeType.SOURCE,
        config={"source": "starrocks", "connection": "pg_main"},
    )
    executor = SourceExecutor(node, execution_context)
    con = duckdb.connect(":memory:")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("val",)]
    mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]
    mock_conn.cursor.return_value = mock_cursor

    with patch(
        "seeknal.connections.starrocks.create_starrocks_connection",
        return_value=mock_conn,
    ) as mock_create_conn:
        row_count = executor._load_starrocks(con, "orders", {"connection": "pg_main"})

        assert mock_create_conn.called
        conn_arg = mock_create_conn.call_args[0][0]
        # Assert host comes from vended data (pg_main.json)
        assert conn_arg["host"] == "vended.pg.internal"
        assert conn_arg["database"] == "vended_db"
        assert conn_arg["user"] == "vended_reader"
        assert row_count == 2


def test_governed_starrocks_refuses_inline_overrides(
    governed_env, execution_context
):
    """StarRocks source refuses inline host/port/database/user/password overrides."""
    node = Node(
        id="source.sr_test",
        name="sr_test",
        node_type=NodeType.SOURCE,
        config={"source": "starrocks", "connection": "pg_main"},
    )
    executor = SourceExecutor(node, execution_context)
    con = duckdb.connect(":memory:")

    secret_pw = "sr_inline_secret_pw_999"
    with pytest.raises(GovernedConfigError) as exc_info:
        executor._load_starrocks(con, "orders", {
            "connection": "pg_main",
            "host": "sr.attacker.com",
            "password": secret_pw,
        })

    err_msg = str(exc_info.value)
    assert "host" in err_msg
    assert secret_pw not in err_msg


# --------------------------------------------------------------------------
# 4. Schema is still allowed inline
# --------------------------------------------------------------------------

def test_governed_postgres_allows_inline_schema_and_connect_timeout(
    governed_env, execution_context
):
    """PostgreSQL source allows schema and connect_timeout to be specified inline."""
    node = Node(
        id="source.pg_test",
        name="pg_test",
        node_type=NodeType.SOURCE,
        config={"source": "postgresql", "connection": "pg_main"},
    )
    executor = SourceExecutor(node, execution_context)

    config = executor._resolve_postgresql_config({
        "connection": "pg_main",
        "schema": "custom_analytics",
        "connect_timeout": 45,
    })

    assert config.host == "vended.pg.internal"
    assert config.database == "vended_db"
    assert config.user == "vended_reader"
    assert config.schema == "custom_analytics"
    assert config.connect_timeout == 45


# --------------------------------------------------------------------------
# 5. File sources: s3:// and ../outside.csv refused; project-relative loads
# --------------------------------------------------------------------------

def test_governed_file_sources_refuse_url_schemes(
    governed_env, execution_context
):
    """File sources refuse URL schemes matching ^[a-z][a-z0-9+.-]*://."""
    node = Node(
        id="source.csv_test",
        name="csv_test",
        node_type=NodeType.SOURCE,
        config={"source": "csv", "table": "s3://my-bucket/orders.csv"},
    )
    executor = SourceExecutor(node, execution_context)
    con = duckdb.connect(":memory:")

    for url_path in (
        "s3://my-bucket/orders.csv",
        "gs://gcp-bucket/data.parquet",
        "az://azure-blob/data.json",
        "https://example.com/data.csv",
        "http://insecure.site/data.csv",
    ):
        with pytest.raises(GovernedConfigError, match="URL schemes not allowed"):
            executor._load_csv(con, url_path, {})


def test_governed_file_sources_refuse_outside_paths(
    governed_env, execution_context, tmp_path: Path
):
    """File sources refuse paths that resolve outside project root."""
    node = Node(
        id="source.csv_test",
        name="csv_test",
        node_type=NodeType.SOURCE,
        config={"source": "csv", "table": "../outside.csv"},
    )
    executor = SourceExecutor(node, execution_context)
    con = duckdb.connect(":memory:")

    # Relative path escaping project root
    with pytest.raises(GovernedConfigError, match="falls outside project root"):
        executor._load_csv(con, "../outside.csv", {})

    # Absolute path outside project root
    outside_file = tmp_path / "other_dir" / "secret.csv"
    outside_file.parent.mkdir(parents=True, exist_ok=True)
    outside_file.write_text("a,b\n1,2\n")

    with pytest.raises(GovernedConfigError, match="falls outside project root"):
        executor._load_csv(con, str(outside_file), {})


def test_governed_file_sources_project_relative_loads(
    governed_env, execution_context
):
    """Project-relative CSV within project root loads successfully."""
    proj_dir = execution_context.workspace_path
    csv_file = proj_dir / "valid_data.csv"
    csv_file.write_text("id,name,value\n1,alice,100\n2,bob,200\n", encoding="utf-8")

    node = Node(
        id="source.valid_orders",
        name="valid_orders",
        node_type=NodeType.SOURCE,
        config={"source": "csv", "table": "valid_data.csv"},
    )
    executor = SourceExecutor(node, execution_context)
    con = duckdb.connect(":memory:")

    row_count = executor._load_csv(con, "valid_data.csv", {})
    assert row_count == 2

    # Verify DuckDB view content
    res = con.execute("SELECT id, name, value FROM source.valid_orders ORDER BY id").fetchall()
    assert res == [(1, "alice", 100), (2, "bob", 200)]


def test_governed_sqlite_source_path_governance(
    governed_env, execution_context, tmp_path: Path
):
    """SQLite source also enforces path governance in governed mode."""
    node = Node(
        id="source.sqlite_test",
        name="sqlite_test",
        node_type=NodeType.SOURCE,
        config={"source": "sqlite", "table": "../outside.db"},
    )
    executor = SourceExecutor(node, execution_context)
    con = duckdb.connect(":memory:")

    with pytest.raises(GovernedConfigError, match="falls outside project root"):
        executor._load_sqlite(con, "../outside.db", {})

    with pytest.raises(GovernedConfigError, match="URL schemes not allowed"):
        executor._load_sqlite(con, "s3://bucket/test.db", {})


# --------------------------------------------------------------------------
# 6. Non-governed mode keeps inline overrides working (regression)
# --------------------------------------------------------------------------

def test_nongoverned_mode_keeps_inline_overrides_and_bypass(
    execution_context, monkeypatch: pytest.MonkeyPatch
):
    """Non-governed mode allows inline database configs, profile overrides, and starrocks inline."""
    monkeypatch.delenv("ATLAS_RUNTIME_MODE", raising=False)
    set_catalog_io(None)
    assert get_catalog_io() is None

    # 1. PostgreSQL source without connection uses inline params directly
    node_pg = Node(
        id="source.pg_inline",
        name="pg_inline",
        node_type=NodeType.SOURCE,
        config={
            "source": "postgresql",
            "params": {
                "host": "my-local-host",
                "port": 5433,
                "database": "analytics",
                "user": "developer",
                "password": "devpassword",
            },
        },
    )
    executor_pg = SourceExecutor(node_pg, execution_context)
    pg_config = executor_pg._resolve_postgresql_config(node_pg.config["params"])

    assert pg_config.host == "my-local-host"
    assert pg_config.port == 5433
    assert pg_config.database == "analytics"
    assert pg_config.user == "developer"
    assert pg_config.password == "devpassword"

    # 2. PostgreSQL source with connection profile allows inline overrides
    mock_profile = {
        "type": "postgresql",
        "host": "profile-host",
        "port": 5432,
        "database": "profile-db",
        "user": "profile-user",
        "password": "profile-pass",
    }
    with patch(
        "seeknal.workflow.materialization.profile_loader.ProfileLoader.load_connection_profile",
        return_value=mock_profile,
    ):
        override_config = executor_pg._resolve_postgresql_config({
            "connection": "my_pg",
            "host": "overridden-host",
            "port": 5434,
        })
        assert override_config.host == "overridden-host"
        assert override_config.port == 5434
        assert override_config.database == "profile-db"

    # 3. StarRocks source without connection uses inline params
    node_sr = Node(
        id="source.sr_inline",
        name="sr_inline",
        node_type=NodeType.SOURCE,
        config={
            "source": "starrocks",
            "params": {"host": "sr-inline-host", "port": 9030, "user": "root"},
        },
    )
    executor_sr = SourceExecutor(node_sr, execution_context)
    con = duckdb.connect(":memory:")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("x",)]
    mock_cursor.fetchall.return_value = [(10,)]
    mock_conn.cursor.return_value = mock_cursor

    with patch(
        "seeknal.connections.starrocks.create_starrocks_connection",
        return_value=mock_conn,
    ) as mock_create_sr:
        row_count = executor_sr._load_starrocks(
            con, "my_sr_table", {"host": "sr-inline-host", "port": 9030}
        )
        assert mock_create_sr.called
        sr_cfg = mock_create_sr.call_args[0][0]
        assert sr_cfg["host"] == "sr-inline-host"
        assert row_count == 1

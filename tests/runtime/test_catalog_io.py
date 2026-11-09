"""Tests for CatalogIO and GovernedContext."""

import base64
import json
import os
import stat
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.runtime.catalog_io import (
    CatalogIO,
    GovernedConfigError,
    GovernedContext,
    NotInRunGrant,
    REQUIRED_GOVERNED_VARS,
    STATIC_CREDENTIAL_VARS,
    admit_run,
    get_catalog_io,
    set_catalog_io,
)
from seeknal.runtime.run_grant import TableAccess, ConnectionAccess
from seeknal.serving.grant_token import sign_grant_token
from seeknal.serving.server.config import ServerConfigError


class FakeConnection:
    """Fake DuckDB connection that records all executed SQL strings."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(sql)
        return self


def _make_jwks_and_grant(
    now: int | None = None,
    reads: list[dict[str, Any]] | None = None,
    writes: list[dict[str, Any]] | None = None,
    connections: list[dict[str, Any]] | None = None,
    **claims_overrides: Any,
) -> tuple[str, str]:
    """Generate a valid compact JWS run grant and matching JWKS JSON string."""
    t = int(time.time()) if now is None else int(now)

    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    kid = "test-catalog-key-1"

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
        "jti": "550e8400-e29b-41d4-a716-446655440000",
        "release": "rel-2026-11",
        "plan_sha256": "a" * 64,
        "trigger": {"type": "schedule", "id": "daily-00"},
        "iat": t,
        "nbf": t,
        "exp": t + 3600,
        "reads": reads if reads is not None else [
            {"table": "warehouse.raw.customers", "actions": ["read"]},
            {"table": "warehouse.raw.orders", "actions": ["read"]},
        ],
        "writes": writes if writes is not None else [
            {"table": "warehouse.analytics.orders_summary", "actions": ["write"]},
        ],
        "connections": connections if connections is not None else [
            {"name": "pg_sink", "actions": ["write"]},
            {"name": "pg_source", "actions": ["read"]},
        ],
    }
    claims.update(claims_overrides)
    token = sign_grant_token(claims, priv, kid=kid)
    return token, json.dumps(jwks)


@pytest.fixture
def governed_env_setup(tmp_path: Path):
    """Create valid secure files and environment for GovernedContext."""
    now = int(time.time())
    token, jwks_json = _make_jwks_and_grant(now)

    grant_file = tmp_path / "run_grant.jwt"
    grant_file.write_text(token, encoding="utf-8")
    grant_file.chmod(0o600)

    jwks_file = tmp_path / "jwks.json"
    jwks_file.write_text(jwks_json, encoding="utf-8")
    jwks_file.chmod(0o600)

    lakekeeper_token_file = tmp_path / "lakekeeper_token.txt"
    lakekeeper_token_file.write_text("initial-lakekeeper-token-12345\n", encoding="utf-8")
    lakekeeper_token_file.chmod(0o600)

    conns_dir = tmp_path / "connections"
    conns_dir.mkdir(mode=0o700)

    pg_source_file = conns_dir / "pg_source.json"
    pg_source_file.write_text(
        json.dumps({"host": "pg.internal", "port": 5432, "database": "prod_db", "user": "reader"}),
        encoding="utf-8",
    )
    pg_source_file.chmod(0o600)

    pg_sink_file = conns_dir / "pg_sink.json"
    pg_sink_file.write_text(
        json.dumps({"host": "pg.internal", "port": 5432, "database": "prod_db", "user": "writer"}),
        encoding="utf-8",
    )
    pg_sink_file.chmod(0o600)

    env = {
        "ATLAS_RUNTIME_MODE": "governed",
        "ATLAS_RUN_GRANT_FILE": str(grant_file),
        "ATLAS_RUN_GRANT_JWKS_FILE": str(jwks_file),
        "ATLAS_LAKEKEEPER_TOKEN_FILE": str(lakekeeper_token_file),
        "ATLAS_LAKEKEEPER_URI": "https://lakekeeper.internal:8181",
        "ATLAS_LAKEKEEPER_WAREHOUSE": "prod_warehouse",
        "ATLAS_RUN_CONTROLLER_URL": "https://controller.internal:8000",
        "ATLAS_CONNECTIONS_DIR": str(conns_dir),
    }

    return {
        "env": env,
        "grant_file": grant_file,
        "jwks_file": jwks_file,
        "token_file": lakekeeper_token_file,
        "conns_dir": conns_dir,
    }


def test_from_env_success(governed_env_setup):
    env = governed_env_setup["env"]
    ctx = GovernedContext.from_env(env)
    assert ctx.grant.typ == "run"
    assert ctx.grant.principal == "svc:orders:prod"
    assert ctx.lakekeeper_warehouse == "prod_warehouse"
    assert ctx.lakekeeper_uri == "https://lakekeeper.internal:8181"
    assert ctx.controller_client.base_url == "https://controller.internal:8000"


@pytest.mark.parametrize("missing_var", REQUIRED_GOVERNED_VARS)
def test_from_env_refuses_missing_required_var(governed_env_setup, missing_var):
    env = dict(governed_env_setup["env"])
    del env[missing_var]
    with pytest.raises(GovernedConfigError, match=missing_var):
        GovernedContext.from_env(env)


@pytest.mark.parametrize("static_var", STATIC_CREDENTIAL_VARS)
def test_from_env_refuses_static_credential_var(governed_env_setup, static_var):
    env = dict(governed_env_setup["env"])
    env[static_var] = "secret-value"
    with pytest.raises(GovernedConfigError, match=static_var):
        GovernedContext.from_env(env)


def test_from_env_refuses_mode_not_governed(governed_env_setup):
    env = dict(governed_env_setup["env"])
    env["ATLAS_RUNTIME_MODE"] = "standard"
    with pytest.raises(GovernedConfigError, match="governed"):
        GovernedContext.from_env(env)


def test_from_env_refuses_0644_grant_file(governed_env_setup):
    grant_file = governed_env_setup["grant_file"]
    grant_file.chmod(0o644)
    with pytest.raises((GovernedConfigError, ServerConfigError), match="group- or world-readable"):
        GovernedContext.from_env(governed_env_setup["env"])


def test_from_env_refuses_0644_token_file(governed_env_setup):
    token_file = governed_env_setup["token_file"]
    token_file.chmod(0o644)
    with pytest.raises((GovernedConfigError, ServerConfigError), match="group- or world-readable"):
        GovernedContext.from_env(governed_env_setup["env"])


def test_from_env_refuses_invalid_grant_signature(governed_env_setup):
    grant_file = governed_env_setup["grant_file"]
    # Tamper with the grant token payload
    token = grant_file.read_text().strip()
    parts = token.split(".")
    tampered_token = f"{parts[0]}.eyJhIjoxfQ.{parts[2]}"
    grant_file.write_text(tampered_token)
    with pytest.raises(GovernedConfigError):
        GovernedContext.from_env(governed_env_setup["env"])


def test_exact_attach_sql_text_built(governed_env_setup):
    ctx = GovernedContext.from_env(governed_env_setup["env"])
    cio = CatalogIO(ctx)
    con = FakeConnection()

    cio.attach_iceberg(con, "raw_catalog")

    # Verify execution calls
    assert "INSTALL httpfs" in con.calls
    assert "LOAD httpfs" in con.calls
    assert "INSTALL iceberg" in con.calls
    assert "LOAD iceberg" in con.calls

    # Find the ATTACH statement
    attach_calls = [c for c in con.calls if c.strip().startswith("ATTACH ")]
    assert len(attach_calls) == 1
    attach_sql = attach_calls[0]

    # Must contain vended_credentials and warehouse/endpoint
    assert "ACCESS_DELEGATION_MODE 'vended_credentials'" in attach_sql
    assert "ATTACH 'prod_warehouse' AS raw_catalog" in attach_sql
    assert "ENDPOINT 'https://lakekeeper.internal:8181/catalog'" in attach_sql
    assert "AUTHORIZATION_TYPE 'oauth2'" in attach_sql
    assert "TOKEN 'initial-lakekeeper-token-12345'" in attach_sql

    # Must NEVER configure static S3 keys
    all_sql_text = " ".join(con.calls).lower()
    assert "s3_access_key_id" not in all_sql_text
    assert "s3_secret_access_key" not in all_sql_text
    assert "s3_use_ssl" not in all_sql_text


def test_token_file_change_triggers_reattach(governed_env_setup):
    ctx = GovernedContext.from_env(governed_env_setup["env"])
    cio = CatalogIO(ctx)
    con = FakeConnection()

    cio.attach_iceberg(con, "raw_catalog")
    initial_calls_count = len(con.calls)

    # Calling again with unchanged token file: no-op
    cio.attach_iceberg(con, "raw_catalog")
    assert len(con.calls) == initial_calls_count

    # Atomically rewrite token file with refreshed token
    token_file = governed_env_setup["token_file"]
    token_file.write_text("refreshed-lakekeeper-token-67890\n", encoding="utf-8")
    token_file.chmod(0o600)

    # Calling attach again must trigger DETACH and new ATTACH
    cio.attach_iceberg(con, "raw_catalog")
    assert len(con.calls) > initial_calls_count

    new_calls = con.calls[initial_calls_count:]
    detach_calls = [c for c in new_calls if c.strip().startswith("DETACH ")]
    attach_calls = [c for c in new_calls if c.strip().startswith("ATTACH ")]

    assert len(detach_calls) == 1
    assert "DETACH raw_catalog" in detach_calls[0]
    assert len(attach_calls) == 1
    assert "TOKEN 'refreshed-lakekeeper-token-67890'" in attach_calls[0]


def test_check_read_allow_and_deny(governed_env_setup):
    ctx = GovernedContext.from_env(governed_env_setup["env"])
    cio = CatalogIO(ctx)

    # Allowed reads (case-insensitive)
    cio.check_read("warehouse.raw.customers")
    cio.check_read("WAREHOUSE.RAW.CUSTOMERS")
    cio.check_read("warehouse.raw.orders")

    # Denied reads
    with pytest.raises(NotInRunGrant, match="warehouse.analytics.orders_summary"):
        cio.check_read("warehouse.analytics.orders_summary")

    with pytest.raises(NotInRunGrant, match="warehouse.raw.other"):
        cio.check_read("warehouse.raw.other")


def test_check_write_allow_and_deny(governed_env_setup):
    ctx = GovernedContext.from_env(governed_env_setup["env"])
    cio = CatalogIO(ctx)

    # Allowed writes (case-insensitive)
    cio.check_write("warehouse.analytics.orders_summary")
    cio.check_write("WAREHOUSE.ANALYTICS.ORDERS_SUMMARY")

    # Denied writes
    with pytest.raises(NotInRunGrant, match="warehouse.raw.customers"):
        cio.check_write("warehouse.raw.customers")

    with pytest.raises(NotInRunGrant, match="warehouse.other.table"):
        cio.check_write("warehouse.other.table")


def test_connection_allow_and_deny(governed_env_setup):
    ctx = GovernedContext.from_env(governed_env_setup["env"])
    cio = CatalogIO(ctx)

    # Allowed connection actions
    source_cfg = cio.connection("pg_source", "read")
    assert source_cfg["user"] == "reader"
    assert source_cfg["database"] == "prod_db"

    sink_cfg = cio.connection("pg_sink", "write")
    assert sink_cfg["user"] == "writer"

    # Action not permitted in grant
    with pytest.raises(NotInRunGrant, match="action 'write'"):
        cio.connection("pg_source", "write")

    with pytest.raises(NotInRunGrant, match="action 'read'"):
        cio.connection("pg_sink", "read")

    # Connection not in grant
    with pytest.raises(NotInRunGrant, match="pg_unknown"):
        cio.connection("pg_unknown", "read")


def test_connection_refuses_0644_connection_file(governed_env_setup):
    ctx = GovernedContext.from_env(governed_env_setup["env"])
    cio = CatalogIO(ctx)

    conns_dir = governed_env_setup["conns_dir"]
    (conns_dir / "pg_source.json").chmod(0o644)

    with pytest.raises((GovernedConfigError, ServerConfigError), match="group- or world-readable"):
        cio.connection("pg_source", "read")


def test_global_get_and_set_catalog_io(governed_env_setup, monkeypatch):
    set_catalog_io(None)
    monkeypatch.delenv("ATLAS_RUNTIME_MODE", raising=False)
    assert get_catalog_io() is None

    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    for k, v in governed_env_setup["env"].items():
        monkeypatch.setenv(k, v)

    cio = get_catalog_io()
    assert cio is not None
    assert isinstance(cio, CatalogIO)

    set_catalog_io(None)


def test_node_start_propagates_controller_exceptions(governed_env_setup):
    ctx = GovernedContext.from_env(governed_env_setup["env"])
    cio = CatalogIO(ctx)

    from seeknal.runtime.controller_client import NodeNotAuthorized, RunControllerUnavailable, RunRevoked

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctx.controller_client, "node_start", MagicMock(side_effect=RunRevoked("revoked")))
        with pytest.raises(RunRevoked, match="revoked"):
            cio.node_start("source.orders")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctx.controller_client, "node_start", MagicMock(side_effect=NodeNotAuthorized("denied")))
        with pytest.raises(NodeNotAuthorized, match="denied"):
            cio.node_start("source.orders")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(ctx.controller_client, "node_start", MagicMock(side_effect=RunControllerUnavailable("offline")))
        with pytest.raises(RunControllerUnavailable, match="offline"):
            cio.node_start("source.orders")


def test_admit_run_success_and_uncovered(governed_env_setup, monkeypatch):
    from unittest.mock import MagicMock
    from seeknal.runtime.run_plan import RunPlan, PlanNode, TableAccess
    from seeknal.runtime.run_grant import plan_sha256

    env = governed_env_setup["env"]
    ctx = GovernedContext.from_env(env)

    # Build plan that matches grant
    reads = [TableAccess(table=r.table, actions=r.actions) for r in ctx.grant.reads]
    writes = [TableAccess(table=w.table, actions=w.actions) for w in ctx.grant.writes]
    conns = [ConnectionAccess(name=c.name, actions=c.actions) for c in ctx.grant.connections]
    nodes = [PlanNode(id="source.customers", kind="source", reads=("warehouse.raw.customers",), writes=(), connections=())]
    matching_plan = RunPlan(reads=tuple(reads), writes=tuple(writes), connections=tuple(conns), nodes=tuple(nodes))

    # Update grant file with token having matching plan_sha256
    matching_hash = plan_sha256(matching_plan)
    token, jwks_json = _make_jwks_and_grant(
        plan_sha256=matching_hash,
        reads=[{"table": r.table, "actions": list(r.actions)} for r in reads],
        writes=[{"table": w.table, "actions": list(w.actions)} for w in writes],
        connections=[{"name": c.name, "actions": list(c.actions)} for c in conns],
    )
    governed_env_setup["grant_file"].write_text(token)
    governed_env_setup["jwks_file"].write_text(jwks_json)

    with patch("seeknal.runtime.catalog_io.build_run_plan", return_value=matching_plan):
        cio = admit_run(Path("."), env=env)
        assert cio is not None
        assert get_catalog_io() is cio

    # Uncovered plan
    uncovered_plan = RunPlan(
        reads=(TableAccess(table="warehouse.raw.unknown", actions=("read",)),),
        writes=(),
        connections=(),
        nodes=(),
    )
    from seeknal.runtime.run_grant import RunGrantError
    with patch("seeknal.runtime.catalog_io.build_run_plan", return_value=uncovered_plan):
        with pytest.raises(RunGrantError):
            admit_run(Path("."), env=env)

    set_catalog_io(None)

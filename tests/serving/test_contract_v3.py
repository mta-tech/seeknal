"""Tests for Seeknal Serving contract v3: workloads ask and job, audit-only act, azp, and origin claims."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import (
    EnforcementError,
    EngineConfigError,
    Grant,
    Placement,
    QueryResult,
    ServingPipeline,
    WorkloadRoutedAdapter,
    create_app,
    sign_grant_token,
)
from seeknal.serving.server.config import ServerConfig, ServerConfigError
from seeknal.serving.service import PreparedQuery


class FakeEngineAdapter:
    def __init__(self, tag: str) -> None:
        self.tag = tag
        self.executed_queries: list[PreparedQuery] = []

    def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
        self.executed_queries.append(prepared)
        return QueryResult(
            columns=("id",),
            rows=[(1,)],
            truncated=False,
            engine=prepared.engine,
            elapsed_ms=1.5,
        )


def _base_claims(*, sql: str = "SELECT id FROM users", **kwargs: Any) -> dict[str, Any]:
    now = time.time()
    claims: dict[str, Any] = {
        "sub": "analyst@example.com",
        "ws": "ws-analytics",
        "jti": f"jti-contract-v3-{time.time_ns()}",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 1000,
        "tables": {
            "users": {
                "columns": ["id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    claims.update(kwargs)
    return claims


def _setup_app(
    adapters_by_workload: dict[str, Any],
    pub_key: Any,
) -> tuple[TestClient, ServingPipeline]:
    placement = Placement.from_dict({
        "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 1000,
                "serve": {"sr": "users"},
            }
        },
        "cubes": {},
    })
    schemas = {"users": ["id"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-1": pub_key},
    )
    routed_adapter = WorkloadRoutedAdapter(adapters_by_workload)
    app = create_app(pipeline, {"sr": routed_adapter})
    client = TestClient(app)
    return client, pipeline


def test_ask_with_valid_act_and_origin_routed_to_ask_account():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    adapter_interactive = FakeEngineAdapter("interactive")
    adapter_ask = FakeEngineAdapter("ask")
    adapter_job = FakeEngineAdapter("job")

    client, _ = _setup_app(
        {
            "interactive": adapter_interactive,
            "ask": adapter_ask,
            "job": adapter_job,
        },
        pub,
    )

    sql = "SELECT id FROM users"
    token = sign_grant_token(
        _base_claims(
            sql=sql,
            workload="ask",
            act={"sub": "session-ask-101", "typ": "ask_session"},
            origin="ask",
            azp="seeknal-ask-client",
        ),
        priv,
        kid="key-1",
    )

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 200
    assert len(adapter_ask.executed_queries) == 1
    assert adapter_ask.executed_queries[0].workload == "ask"
    assert len(adapter_interactive.executed_queries) == 0
    assert len(adapter_job.executed_queries) == 0


@pytest.mark.parametrize(
    "bad_overrides",
    [
        {"workload": "ask", "origin": "ask"},  # ask without act
        {"workload": "ask", "origin": "ask", "act": {"sub": "sess-1", "typ": "run"}},  # ask with act.typ=run
        {"workload": "ask", "origin": "superset", "act": {"sub": "sess-1", "typ": "ask_session"}},  # ask with origin=superset
        {"workload": "ask", "origin": "job", "act": {"sub": "sess-1", "typ": "ask_session"}},  # ask with origin=job
    ],
)
def test_ask_refused_conditions(bad_overrides: dict[str, Any]):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    adapter_ask = FakeEngineAdapter("ask")
    client, _ = _setup_app({"interactive": FakeEngineAdapter("interactive"), "ask": adapter_ask}, pub)

    sql = "SELECT id FROM users"
    claims = _base_claims(sql=sql, **bad_overrides)

    # Unit check: Grant.from_claims raises EnforcementError
    with pytest.raises(EnforcementError):
        Grant.from_claims(claims)

    # HTTP check: 403 enforcement_error
    token = sign_grant_token(claims, priv, kid="key-1")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 403
    assert resp.json()["error"] == "enforcement_error"
    assert len(adapter_ask.executed_queries) == 0


def test_job_with_valid_act_and_origin_routed_to_job_account():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    adapter_interactive = FakeEngineAdapter("interactive")
    adapter_job = FakeEngineAdapter("job")

    client, _ = _setup_app(
        {
            "interactive": adapter_interactive,
            "job": adapter_job,
        },
        pub,
    )

    sql = "SELECT id FROM users"
    token = sign_grant_token(
        _base_claims(
            sql=sql,
            workload="job",
            act={"sub": "job-run-777", "typ": "run"},
            origin="job",
            azp="pipeline-runner",
        ),
        priv,
        kid="key-1",
    )

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 200
    assert len(adapter_job.executed_queries) == 1
    assert adapter_job.executed_queries[0].workload == "job"
    assert len(adapter_interactive.executed_queries) == 0


@pytest.mark.parametrize(
    "bad_overrides",
    [
        {"workload": "job", "origin": "job"},  # job without act
        {"workload": "job", "origin": "job", "act": {"sub": "run-1", "typ": "ask_session"}},  # job with act.typ=ask_session
        {"workload": "job", "origin": "ask", "act": {"sub": "run-1", "typ": "run"}},  # job with origin=ask
        {"workload": "job", "origin": "superset", "act": {"sub": "run-1", "typ": "run"}},  # job with origin=superset
    ],
)
def test_job_refused_conditions(bad_overrides: dict[str, Any]):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    adapter_job = FakeEngineAdapter("job")
    client, _ = _setup_app({"interactive": FakeEngineAdapter("interactive"), "job": adapter_job}, pub)

    sql = "SELECT id FROM users"
    claims = _base_claims(sql=sql, **bad_overrides)

    with pytest.raises(EnforcementError):
        Grant.from_claims(claims)

    token = sign_grant_token(claims, priv, kid="key-1")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 403
    assert resp.json()["error"] == "enforcement_error"
    assert len(adapter_job.executed_queries) == 0


@pytest.mark.parametrize("wl", ["bi", "interactive", "report"])
def test_act_on_non_ask_job_workloads_refused(wl: str):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    client, _ = _setup_app({"interactive": FakeEngineAdapter("interactive"), "bi": FakeEngineAdapter("bi")}, pub)

    sql = "SELECT id FROM users"
    claims = _base_claims(
        sql=sql,
        workload=wl,
        act={"sub": "sess-xyz", "typ": "ask_session"},
        origin="portal",
    )

    with pytest.raises(EnforcementError, match="act claim is not allowed for workload"):
        Grant.from_claims(claims)

    token = sign_grant_token(claims, priv, kid="key-1")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 403
    assert resp.json()["error"] == "enforcement_error"


@pytest.mark.parametrize("bad_origin", ["unknown", "tableau", "", 123, True, [], {}])
def test_unknown_origin_refused(bad_origin: Any):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    client, _ = _setup_app({"interactive": FakeEngineAdapter("interactive")}, pub)

    sql = "SELECT id FROM users"
    claims = _base_claims(sql=sql, workload="interactive", origin=bad_origin)

    with pytest.raises(EnforcementError):
        Grant.from_claims(claims)

    token = sign_grant_token(claims, priv, kid="key-1")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 403
    assert resp.json()["error"] == "enforcement_error"


@pytest.mark.parametrize(
    "bad_act",
    [
        "not-a-dict",
        123,
        True,
        [],
        {"sub": "s1"},  # missing typ
        {"typ": "run"},  # missing sub
        {"sub": "", "typ": "run"},  # empty sub
        {"sub": "s1", "typ": "invalid"},  # invalid typ
        {"sub": "s1", "typ": "run", "extra": "field"},  # extra field
    ],
)
def test_malformed_act_shape_refused(bad_act: Any):
    claims = _base_claims(workload="job", origin="job", act=bad_act)
    with pytest.raises(EnforcementError):
        Grant.from_claims(claims)


@pytest.mark.parametrize("bad_azp", ["", 123, True, [], {}])
def test_invalid_azp_refused(bad_azp: Any):
    claims = _base_claims(workload="interactive", azp=bad_azp)
    with pytest.raises(EnforcementError):
        Grant.from_claims(claims)


def test_ask_grant_with_no_ask_account_gets_503_workload_account_missing():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    # Only interactive and bi configured, no ask
    client, _ = _setup_app(
        {
            "interactive": FakeEngineAdapter("interactive"),
            "bi": FakeEngineAdapter("bi"),
        },
        pub,
    )

    sql = "SELECT id FROM users"
    token = sign_grant_token(
        _base_claims(
            sql=sql,
            workload="ask",
            act={"sub": "session-123", "typ": "ask_session"},
            origin="ask",
        ),
        priv,
        kid="key-1",
    )

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 503
    data = resp.json()
    assert data["error"] == "workload_account_missing"
    assert "no engine account configured for workload 'ask'" in data["message"]


def test_config_accepts_ask_and_job_accounts_and_rejects_unknown(tmp_path: Any):
    jwks_path = tmp_path / "keys.jwks.json"
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pn = pub.public_numbers()
    import base64
    x_b64 = base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    y_b64 = base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    jwks_path.write_text(json.dumps({"keys": [{"kty": "EC", "crv": "P-256", "x": x_b64, "y": y_b64, "kid": "k1"}]}))

    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  sr: {kind: starrocks, dialect: starrocks}
tables: {}
cubes: {}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text("{}")

    base_config = {
        "grant_keys": {"jwks_file": str(jwks_path)},
        "placement": {
            "static": {
                "placement_file": str(pf),
                "schemas_file": str(sf),
            }
        },
        "connections": {
            "sr": {
                "kind": "starrocks",
                "accounts": {
                    "interactive": {
                        "host": "localhost",
                        "port": 9030,
                        "user": "root",
                        "password_env": "SR_PW",
                    },
                    "ask": {
                        "host": "localhost",
                        "port": 9030,
                        "user": "ask_user",
                        "password_env": "SR_PW",
                    },
                    "job": {
                        "host": "localhost",
                        "port": 9030,
                        "user": "job_user",
                        "password_env": "SR_PW",
                    },
                },
            }
        },
    }

    # 1. Valid config with ask and job accepted
    cfg = ServerConfig.from_dict(base_config)
    sr_accounts = cfg.connections["sr"].accounts
    assert "interactive" in sr_accounts
    assert "ask" in sr_accounts
    assert "job" in sr_accounts

    # 2. Config with unknown account key rejected
    bad_config = json.loads(json.dumps(base_config))
    bad_config["connections"]["sr"]["accounts"]["unknown_workload"] = {
        "host": "localhost",
        "port": 9030,
        "user": "u",
        "password_env": "SR_PW",
    }
    with pytest.raises(ServerConfigError, match="invalid account"):
        ServerConfig.from_dict(bad_config)


def test_audit_log_line_contains_azp_origin_act_sub(caplog: pytest.LogCaptureFixture):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    adapter_ask = FakeEngineAdapter("ask")
    client, _ = _setup_app(
        {
            "interactive": FakeEngineAdapter("interactive"),
            "ask": adapter_ask,
        },
        pub,
    )

    sql = "SELECT id FROM users"
    token = sign_grant_token(
        _base_claims(
            sql=sql,
            workload="ask",
            azp="client-azp-999",
            origin="ask",
            act={"sub": "act-sub-session-888", "typ": "ask_session"},
        ),
        priv,
        kid="key-1",
    )

    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql},
        )
        assert resp.status_code == 200

        # Assert audit line fields in caplog
        assert "workload=ask" in caplog.text
        assert "azp=client-azp-999" in caplog.text
        assert "origin=ask" in caplog.text
        assert "act_sub=act-sub-session-888" in caplog.text
        assert "act_typ=ask_session" in caplog.text


def test_v2_grant_no_new_claims_behaves_exactly_as_before(caplog: pytest.LogCaptureFixture):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    adapter_interactive = FakeEngineAdapter("interactive")
    client, _ = _setup_app({"interactive": adapter_interactive}, pub)

    sql = "SELECT id FROM users"
    # Pure v2 grant without workload, act, azp, or origin
    claims = _base_claims(sql=sql)
    assert "workload" not in claims
    assert "act" not in claims
    assert "azp" not in claims
    assert "origin" not in claims

    grant = Grant.from_claims(claims)
    assert grant.workload == "interactive"
    assert grant.act is None
    assert grant.azp is None
    assert grant.origin is None

    token = sign_grant_token(claims, priv, kid="key-1")

    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql},
        )
        assert resp.status_code == 200
        assert len(adapter_interactive.executed_queries) == 1

        # Must not contain new v3 claim audit tags
        assert "azp=" not in caplog.text
        assert "origin=" not in caplog.text
        assert "act_sub=" not in caplog.text
        assert "act_typ=" not in caplog.text

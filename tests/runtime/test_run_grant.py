"""Tests for RunGrant verification, claim rules, and coverage checking."""

import time
from typing import Any
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.runtime.run_grant import (
    RunGrant,
    RunGrantError,
    require_plan_covered,
    verify_run_grant,
)
from seeknal.runtime.run_plan import (
    ConnectionAccess,
    PlanNode,
    RunPlan,
    TableAccess,
    plan_sha256,
)
from seeknal.serving.grant_token import sign_grant_token


@pytest.fixture
def key_pair():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    kid = "catalog-es256-key-1"
    return priv, {kid: pub}, kid


def make_valid_claims(now: int | None = None, **overrides) -> dict[str, Any]:
    t = int(time.time()) if now is None else int(now)
    claims = {
        "typ": "run",
        "iss": "atlas-catalog",
        "aud": "atlas-rt-run",
        "sub": "user:project-sp-1",
        "principal": "svc:analytics:prod",
        "ws": "ws-main",
        "jti": "550e8400-e29b-41d4-a716-446655440000",
        "release": "rel-2026-11",
        "plan_sha256": "e" * 64,
        "trigger": {"type": "schedule", "id": "cron-daily"},
        "iat": t,
        "nbf": t,
        "exp": t + 3600,
        "reads": [{"table": "warehouse.raw.customers", "actions": ["read"]}],
        "writes": [{"table": "warehouse.analytics.orders", "actions": ["write"]}],
        "connections": [{"name": "pg_conn", "actions": ["read", "write"]}],
    }
    claims.update(overrides)
    return claims


def test_valid_grant(key_pair):
    priv, public_keys, kid = key_pair
    now = 1700000000
    claims = make_valid_claims(now=now)
    token = sign_grant_token(claims, priv, kid=kid)

    grant = verify_run_grant(token, public_keys, now=now)
    assert grant.typ == "run"
    assert grant.iss == "atlas-catalog"
    assert grant.aud == "atlas-rt-run"
    assert grant.sub == "user:project-sp-1"
    assert grant.principal == "svc:analytics:prod"
    assert grant.jti == "550e8400-e29b-41d4-a716-446655440000"
    assert grant.plan_sha256 == "e" * 64
    assert grant.trigger == {"type": "schedule", "id": "cron-daily"}
    assert grant.exp - grant.iat == 3600
    assert len(grant.reads) == 1
    assert grant.reads[0].table == "warehouse.raw.customers"
    assert len(grant.writes) == 1
    assert grant.writes[0].table == "warehouse.analytics.orders"
    assert len(grant.connections) == 1
    assert grant.connections[0].name == "pg_conn"
    assert grant.connections[0].actions == ("read", "write")


@pytest.mark.parametrize(
    "claim_key,bad_value,expected_substring",
    [
        ("typ", "custom", "typ"),
        ("iss", "wrong-catalog", "iss"),
        ("aud", "wrong-audience", "aud"),
        ("sub", "", "sub"),
        ("principal", "", "principal"),
        ("ws", "", "ws"),
        ("jti", "", "jti"),
        ("release", "", "release"),
        ("plan_sha256", "E" * 64, "plan_sha256"),  # uppercase hex
        ("plan_sha256", "short", "plan_sha256"),
        ("trigger", {"type": "unknown", "id": "1"}, "trigger"),
        ("trigger", {"type": "schedule"}, "trigger"),
        ("nbf", 1700000010, "nbf"),  # iat + 10 exceeds iat + 5
        ("exp", 1700000059, "bounds"),  # exp - iat = 59 < 60
        ("exp", 1700086401, "bounds"),  # exp - iat = 86401 > 86400
        ("reads", [{"table": "not_three_parts", "actions": ["read"]}], "reads"),
        ("reads", [{"table": "w.n.t", "actions": ["write"]}], "reads"),
        (
            "reads",
            [
                {"table": "w.b.t", "actions": ["read"]},
                {"table": "w.a.t", "actions": ["read"]},
            ],
            "sorted",
        ),
        ("writes", [{"table": "w.n.t", "actions": ["read"]}], "writes"),
        ("connections", [{"name": "conn", "actions": ["delete"]}], "connections"),
    ],
)
def test_each_claim_rule_violated(key_pair, claim_key, bad_value, expected_substring):
    priv, public_keys, kid = key_pair
    now = 1700000000
    claims = make_valid_claims(now=now, **{claim_key: bad_value})
    token = sign_grant_token(claims, priv, kid=kid)

    with pytest.raises(RunGrantError) as exc_info:
        verify_run_grant(token, public_keys, now=now)
    assert expected_substring.lower() in str(exc_info.value).lower()


def test_serving_shaped_grant_refused(key_pair):
    """A Serving-shaped grant (aud=seeknal-serving, typ=grant) is refused."""
    priv, public_keys, kid = key_pair
    now = 1700000000
    claims = {
        "typ": "grant",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "sub": "user:serving-client",
        "iat": now,
        "exp": now + 300,
        "max_rows": 1000,
    }
    token = sign_grant_token(claims, priv, kid=kid)

    with pytest.raises(RunGrantError) as exc_info:
        verify_run_grant(token, public_keys, now=now)
    assert "typ" in str(exc_info.value).lower()


def test_expired_grant(key_pair):
    priv, public_keys, kid = key_pair
    now = 1700000000
    claims = make_valid_claims(now=now - 5000, exp=now - 100)
    token = sign_grant_token(claims, priv, kid=kid)

    with pytest.raises(RunGrantError) as exc_info:
        verify_run_grant(token, public_keys, now=now)
    assert "expired" in str(exc_info.value).lower()


def test_nbf_in_the_future(key_pair):
    priv, public_keys, kid = key_pair
    now = 1700000000
    # iat is past (now - 2), nbf is future (now + 3 <= iat + 5)
    claims = make_valid_claims(now=now - 2, iat=now - 2, nbf=now + 3, exp=now + 3600)
    token = sign_grant_token(claims, priv, kid=kid)

    with pytest.raises(RunGrantError) as exc_info:
        verify_run_grant(token, public_keys, now=now, leeway_seconds=0)
    assert "nbf" in str(exc_info.value).lower()


def test_coverage_ok():
    plan = RunPlan(
        reads=(TableAccess(table="w.ns.orders", actions=("read",)),),
        writes=(TableAccess(table="w.ns.analytics", actions=("write",)),),
        connections=(ConnectionAccess(name="pg_main", actions=("read",)),),
        nodes=(),
    )
    digest = plan_sha256(plan)

    grant = RunGrant.parse({
        "typ": "run",
        "iss": "atlas-catalog",
        "aud": "atlas-rt-run",
        "sub": "user:1",
        "principal": "svc:p:e",
        "ws": "ws-1",
        "jti": "jti-1",
        "release": "rel-1",
        "plan_sha256": digest,
        "trigger": {"type": "user", "id": "admin"},
        "iat": 1000,
        "nbf": 1000,
        "exp": 2000,
        "reads": [{"table": "w.ns.orders", "actions": ["read"]}],
        "writes": [{"table": "w.ns.analytics", "actions": ["write"]}],
        "connections": [{"name": "pg_main", "actions": ["read", "write"]}],
    })

    require_plan_covered(grant, plan)


def test_coverage_missing_write():
    plan = RunPlan(
        reads=(),
        writes=(TableAccess(table="w.ns.uncovered_sink", actions=("write",)),),
        connections=(),
        nodes=(),
    )
    digest = plan_sha256(plan)

    grant = RunGrant.parse({
        "typ": "run",
        "iss": "atlas-catalog",
        "aud": "atlas-rt-run",
        "sub": "user:1",
        "principal": "svc:p:e",
        "ws": "ws-1",
        "jti": "jti-1",
        "release": "rel-1",
        "plan_sha256": digest,
        "trigger": {"type": "user", "id": "admin"},
        "iat": 1000,
        "nbf": 1000,
        "exp": 2000,
        "reads": [],
        "writes": [{"table": "w.ns.other_sink", "actions": ["write"]}],
        "connections": [],
    })

    with pytest.raises(RunGrantError) as exc_info:
        require_plan_covered(grant, plan)
    assert "uncovered write table: 'w.ns.uncovered_sink'" in str(exc_info.value)


def test_coverage_mismatched_digest():
    plan = RunPlan(reads=(), writes=(), connections=(), nodes=())
    grant = RunGrant.parse({
        "typ": "run",
        "iss": "atlas-catalog",
        "aud": "atlas-rt-run",
        "sub": "user:1",
        "principal": "svc:p:e",
        "ws": "ws-1",
        "jti": "jti-1",
        "release": "rel-1",
        "plan_sha256": "f" * 64,
        "trigger": {"type": "user", "id": "admin"},
        "iat": 1000,
        "nbf": 1000,
        "exp": 2000,
        "reads": [],
        "writes": [],
        "connections": [],
    })

    with pytest.raises(RunGrantError) as exc_info:
        require_plan_covered(grant, plan)
    assert "mismatch" in str(exc_info.value).lower()

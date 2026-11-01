"""Tests for Seeknal Serving contract v2: workload claims and per-workload accounts."""

from __future__ import annotations

import hashlib
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
    build_workload_adapters,
    create_app,
    sign_grant_token,
)
from seeknal.serving.service import PreparedQuery


def _base_claims(*, sql: str = "SELECT id FROM users", **kwargs: Any) -> dict[str, Any]:
    now = time.time()
    claims: dict[str, Any] = {
        "sub": "analyst@example.com",
        "ws": "ws-analytics",
        "jti": "jti-workload-01",
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


def test_workload_claim_absent_defaults_to_interactive():
    claims = _base_claims()
    assert "workload" not in claims
    grant = Grant.from_claims(claims)
    assert grant.workload == "interactive"


@pytest.mark.parametrize("wl", ["interactive", "bi", "report"])
def test_workload_claim_valid_values(wl: str):
    claims = _base_claims(workload=wl)
    grant = Grant.from_claims(claims)
    assert grant.workload == wl


@pytest.mark.parametrize("wl", ["batch", "adhoc", "unknown", "", "INTERACTIVE", 123, True, None, []])
def test_workload_claim_invalid_or_non_string_refused(wl: Any):
    claims = _base_claims(workload=wl)
    with pytest.raises(EnforcementError):
        Grant.from_claims(claims)


def test_grant_post_init_validates_workload():
    claims = _base_claims()
    valid_grant = Grant.from_claims(claims)
    # direct constructor with invalid workload
    with pytest.raises(EnforcementError):
        Grant(
            subject=valid_grant.subject,
            workspace=valid_grant.workspace,
            audit_id=valid_grant.audit_id,
            sql_sha256=valid_grant.sql_sha256,
            max_rows=valid_grant.max_rows,
            expires_at=valid_grant.expires_at,
            tables=valid_grant.tables,
            workload="invalid_workload",
        )


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


def test_workload_routed_adapter_delegation():
    adapter_interactive = FakeEngineAdapter("interactive")
    adapter_bi = FakeEngineAdapter("bi")
    adapter_report = FakeEngineAdapter("report")

    routed = WorkloadRoutedAdapter({
        "interactive": adapter_interactive,
        "bi": adapter_bi,
        "report": adapter_report,
    })

    prep_bi = PreparedQuery(
        engine="sr",
        engine_kind="starrocks",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        rule="test",
        reason="test",
        row_limit=10,
        masked_columns=(),
        redacted_sql="SELECT 1",
        subject="user@test.com",
        workspace="ws",
        audit_id="audit-1",
        cube_security_context=None,
        workload="bi",
    )

    result = routed.execute(prep_bi, timeout_seconds=10.0)
    assert result.rows == [(1,)]
    assert len(adapter_bi.executed_queries) == 1
    assert len(adapter_interactive.executed_queries) == 0
    assert len(adapter_report.executed_queries) == 0


def test_workload_routed_adapter_missing_account_raises_engine_config_error():
    routed = WorkloadRoutedAdapter({
        "interactive": FakeEngineAdapter("interactive"),
    })

    prep_report = PreparedQuery(
        engine="sr",
        engine_kind="starrocks",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        rule="test",
        reason="test",
        row_limit=10,
        masked_columns=(),
        redacted_sql="SELECT 1",
        subject="user@test.com",
        workspace="ws",
        audit_id="audit-1",
        cube_security_context=None,
        workload="report",
    )

    with pytest.raises(EngineConfigError) as exc_info:
        routed.execute(prep_report, timeout_seconds=10.0)
    assert "no engine account configured for workload 'report'" in str(exc_info.value)


def test_build_workload_adapters_helper():
    routed = build_workload_adapters(
        "duckdb",
        {
            "interactive": lambda: None,
            "bi": lambda: None,
        },
    )
    assert isinstance(routed, WorkloadRoutedAdapter)
    assert "interactive" in routed.by_workload
    assert "bi" in routed.by_workload


def test_http_workload_routed_execution_and_503_missing_account():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

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
        public_keys={"key-1": pub},
    )

    adapter_interactive = FakeEngineAdapter("interactive")
    adapter_bi = FakeEngineAdapter("bi")
    routed_adapter = WorkloadRoutedAdapter({
        "interactive": adapter_interactive,
        "bi": adapter_bi,
    })

    app = create_app(pipeline, {"sr": routed_adapter})
    client = TestClient(app)

    # 1. Query with workload="bi" succeeds
    sql = "SELECT id FROM users"
    token_bi = sign_grant_token(_base_claims(sql=sql, workload="bi"), priv, kid="key-1")
    resp_bi = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token_bi}"},
        json={"sql": sql},
    )
    assert resp_bi.status_code == 200
    assert len(adapter_bi.executed_queries) == 1
    assert adapter_bi.executed_queries[0].workload == "bi"

    # 2. Query with workload="report" gives 503 workload_account_missing
    token_report = sign_grant_token(_base_claims(sql=sql, workload="report"), priv, kid="key-1")
    resp_report = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token_report}"},
        json={"sql": sql},
    )
    assert resp_report.status_code == 503
    data = resp_report.json()
    assert data["error"] == "workload_account_missing"
    assert "no engine account configured for workload 'report'" in data["message"]

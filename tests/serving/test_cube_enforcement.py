"""Tests for Cube query enforcement in Seeknal Serving.

Verifies:
- refuse is the default mode;
- refuse mode fails closed when masks or row filters are present;
- clear-only grants pass in refuse mode;
- in jwt mode, masks and row filters are permitted and populate cube_security_context;
- in jwt mode, masked members are prohibited in WHERE, HAVING, GROUP BY, ORDER BY,
  JOIN ON, functions, and expressions;
- in jwt mode, masked members are allowed as bare or aliased top-level projections;
- non-Cube targets remain unaffected.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

import pytest

from seeknal.serving.enforce import (
    EnforcementError,
    Grant,
    RowFilter,
    TableGrant,
    enforce,
)
from seeknal.serving.placement import Placement
from seeknal.serving.service import ServingPipeline


def _cube_claims_fixture(
    sql: str,
    *,
    masks: dict[str, str] | None = None,
    row_filters: list[dict[str, Any]] | None = None,
    columns: list[str] | None = None,
    table_name: str = "claims_cube",
) -> tuple[Grant, dict[str, list[str]]]:
    """Helper creating a Grant and schema for Cube testing."""
    if columns is None:
        columns = ["claim_id", "province", "idrg_code", "amount", "total_amount"]
    if masks is None:
        masks = {"patient_nik": "hash"}
    if row_filters is None:
        row_filters = [{"column": "province", "op": "in", "values": ["JB"]}]

    claims = {
        "sub": "user:cube-analyst",
        "ws": "claims-ws",
        "jti": "jti-cube-001",
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 1000,
        "exp": time.time() + 3600.0,
        "tables": {
            table_name: {
                "columns": columns,
                "masks": masks,
                "row_filters": row_filters,
                "actions": ["select"],
            }
        },
    }
    schemas = {
        table_name: [
            "claim_id",
            "patient_nik",
            "patient_name",
            "province",
            "idrg_code",
            "amount",
            "total_amount",
        ]
    }
    return Grant.from_claims(claims), schemas


def test_cube_refuse_is_default_in_enforce_and_pipeline():
    sql = "SELECT province, total_amount FROM claims_cube"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    # enforce defaults to refuse mode
    with pytest.raises(EnforcementError, match="cube target has masks or row filters; cube enforcement is not enabled"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube")

    # ServingPipeline defaults to refuse mode
    placement = Placement.from_dict({
        "engines": {"cube": {"kind": "cube", "dialect": "postgres"}},
        "tables": {},
        "cubes": {
            "claims_cube": {
                "engine": "cube",
                "measures": ["total_amount"],
                "dimensions": ["province"],
            }
        },
    })
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={},
    )
    assert pipeline.cube_mode == "refuse"


def test_cube_query_with_masks_refused_in_refuse_mode():
    sql = "SELECT province, total_amount FROM claims_cube"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="cube target has masks or row filters; cube enforcement is not enabled"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="refuse")


def test_cube_query_with_row_filters_refused_in_refuse_mode():
    sql = "SELECT province, total_amount FROM claims_cube"
    grant, schemas = _cube_claims_fixture(
        sql,
        masks={},
        row_filters=[{"column": "province", "op": "eq", "values": ["JB"]}],
    )

    with pytest.raises(EnforcementError, match="cube target has masks or row filters; cube enforcement is not enabled"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="refuse")


def test_cube_clear_only_grants_still_pass_in_refuse_mode():
    sql = "SELECT province, total_amount FROM claims_cube"
    grant, schemas = _cube_claims_fixture(sql, masks={}, row_filters=[])

    res = enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="refuse")
    assert "SELECT province, total_amount FROM claims_cube" in res.sql
    assert res.cube_security_context is not None
    assert res.cube_security_context["masks"] == {}
    assert res.cube_security_context["row_filters"] == {}


def test_cube_jwt_mode_masks_and_filters_pass_and_context_populated():
    sql = "SELECT province, total_amount, patient_nik FROM claims_cube"
    grant, schemas = _cube_claims_fixture(
        sql,
        masks={"patient_nik": "hash"},
        row_filters=[{"column": "province", "op": "in", "values": ["JB"]}],
    )

    res = enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")
    assert res.cube_security_context is not None
    assert res.cube_security_context["subject"] == "user:cube-analyst"
    assert "claims_cube" in res.cube_security_context["masks"]
    assert res.cube_security_context["masks"]["claims_cube"]["patient_nik"] == "hash"
    assert "claims_cube" in res.cube_security_context["row_filters"]
    assert len(res.cube_security_context["row_filters"]["claims_cube"]) == 1
    assert res.cube_security_context["row_filters"]["claims_cube"][0]["column"] == "province"


def test_cube_jwt_mode_masked_member_in_where_refused():
    sql = "SELECT total_amount FROM claims_cube WHERE patient_nik = 'probe'"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_in_having_refused():
    sql = "SELECT province, sum(amount) FROM claims_cube GROUP BY province HAVING patient_nik = 'probe'"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_in_group_by_refused():
    sql = "SELECT province FROM claims_cube GROUP BY patient_nik"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_in_order_by_refused():
    sql = "SELECT province FROM claims_cube ORDER BY patient_nik"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_in_join_on_refused():
    sql = "SELECT c1.claim_id FROM claims_cube c1 JOIN claims_cube c2 ON c1.patient_nik = c2.patient_nik"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_inside_function_refused():
    sql = "SELECT count(patient_nik) FROM claims_cube"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_inside_expression_refused():
    sql = "SELECT patient_nik || '-masked' FROM claims_cube"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik'"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_alias_used_in_clause_refused():
    sql = "SELECT patient_nik AS p_nik FROM claims_cube ORDER BY p_nik"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_masked_member_ordinal_order_by_refused():
    sql = "SELECT patient_nik, total_amount FROM claims_cube ORDER BY 1"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    with pytest.raises(EnforcementError, match="masked member 'patient_nik' in ORDER BY is not permitted"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_cube_jwt_mode_bare_top_level_projection_allowed():
    sql = "SELECT claim_id, patient_nik FROM claims_cube"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    res = enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")
    assert "patient_nik" in res.sql
    assert "claims_cube" in res.sql


def test_cube_jwt_mode_aliased_top_level_projection_allowed():
    sql = "SELECT claim_id, patient_nik AS masked_nik FROM claims_cube"
    grant, schemas = _cube_claims_fixture(sql, masks={"patient_nik": "hash"}, row_filters=[])

    res = enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")
    assert "masked_nik" in res.sql


def test_non_cube_targets_unaffected():
    # On non-cube targets like duckdb or starrocks, masks in WHERE are rewritten to secure-view subqueries
    sql = "SELECT claim_id FROM claims.claim WHERE patient_nik = 'test'"
    claims = {
        "sub": "user:7f3c",
        "ws": "claims-jabar",
        "jti": "aud-0192",
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 1000,
        "exp": time.time() + 3600.0,
        "tables": {
            "claims.claim": {
                "columns": ["claim_id"],
                "masks": {"patient_nik": "hash"},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    schemas = {"claims.claim": ["claim_id", "patient_nik"]}
    grant = Grant.from_claims(claims)

    for target_kind, target_dialect in [("duckdb", "duckdb"), ("starrocks", "starrocks"), ("lakebase", "postgres")]:
        res = enforce(
            sql,
            grant,
            schemas=schemas,
            target_dialect=target_dialect,
            target_kind=target_kind,
            cube_mode="refuse",
        )
        assert res.cube_security_context is None
        # Tables are rewritten into derived table subquery
        assert "FROM (" in res.sql

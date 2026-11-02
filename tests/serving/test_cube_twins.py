"""Tests for Cube twin masking, discovery schemas, probe-rule fix, connect timeout, and audit refusals (G3f)."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import time
from typing import Any
from types import MappingProxyType

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import (
    CubeAdapter,
    CubeSecurityConfig,
    DBAPIAdapter,
    EnforcementError,
    EngineError,
    EngineTimeout,
    Grant,
    Placement,
    PlacementError,
    ServingPipeline,
    create_app,
    enforce,
    sign_grant_token,
)
from seeknal.serving.adapters import _is_timeout_error
from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.cube import CubeMeta, CubeSource
from seeknal.serving.placement import CubeEntry
from seeknal.serving.router import RoutingError
from seeknal.serving.server.app import _make_cube_connect


# ==============================================================================
# Helper fixtures
# ==============================================================================


@pytest.fixture
def ec_key():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_cube_grant(
    sql: str,
    *,
    masks: dict[str, str] | None = None,
    row_filters: list[dict[str, Any]] | None = None,
    columns: list[str] | None = None,
    cube_name: str = "products_cube",
    jti: str = "jti-twin-001",
    sub: str = "user:cube-analyst",
) -> tuple[Grant, dict[str, list[str]]]:
    if masks is None:
        masks = {"product": "redact"}
    if columns is None:
        all_cols = ["product_id", "product", "category", "price", "total_sales"]
        columns = [c for c in all_cols if c not in masks]
    if row_filters is None:
        row_filters = []

    claims = {
        "sub": sub,
        "ws": "ws-cube",
        "jti": jti,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 1000,
        "exp": time.time() + 3600.0,
        "tables": {
            cube_name: {
                "columns": columns,
                "masks": masks,
                "row_filters": row_filters,
                "actions": ["select"],
            }
        },
    }
    schemas = {
        cube_name: [
            "product_id",
            "product",
            "category",
            "price",
            "total_sales",
        ]
    }
    return Grant.from_claims(claims), schemas


# ==============================================================================
# Part A: Twin members (placement and discovery)
# ==============================================================================


def test_cube_entry_mask_variants_success_and_read_only():
    entry = CubeEntry(
        name="sales_cube",
        engine="cube",
        measures=("total_sales",),
        dimensions=("product", "category"),
        mask_variants={"product": ["redact", "null"]},
    )
    assert isinstance(entry.mask_variants, MappingProxyType)
    assert entry.mask_variants["product"] == ("redact", "null")
    with pytest.raises(TypeError):
        entry.mask_variants["product"] = ("year",)  # read-only check


def test_cube_entry_mask_variants_empty_when_absent():
    entry = CubeEntry(
        name="sales_cube",
        engine="cube",
        measures=("total_sales",),
        dimensions=("product",),
    )
    assert isinstance(entry.mask_variants, MappingProxyType)
    assert len(entry.mask_variants) == 0


def test_cube_entry_invalid_mask_method_raises_placement_error():
    with pytest.raises(PlacementError, match="invalid method 'hash'"):
        CubeEntry(
            name="sales_cube",
            engine="cube",
            measures=("total_sales",),
            dimensions=("product",),
            mask_variants={"product": ["hash"]},
        )


def test_cube_entry_base_not_in_dimensions_raises_placement_error():
    with pytest.raises(PlacementError, match="base 'nonexistent' is not a dimension"):
        CubeEntry(
            name="sales_cube",
            engine="cube",
            measures=("total_sales",),
            dimensions=("product",),
            mask_variants={"nonexistent": ["redact"]},
        )


def test_placement_from_dict_and_yaml_with_mask_variants():
    data = {
        "engines": {"cube": {"kind": "cube", "dialect": "postgres"}},
        "tables": {},
        "cubes": {
            "products_cube": {
                "engine": "cube",
                "measures": ["total_sales"],
                "dimensions": ["product", "category"],
                "mask_variants": {
                    "product": ["redact", "last4"],
                },
            }
        },
    }
    p = Placement.from_dict(data)
    cube = p.find_cube("products_cube")
    assert cube is not None
    assert cube.mask_variants["product"] == ("redact", "last4")


def test_cube_source_discovery_splits_twins_and_preserves_non_twins():
    raw_meta = {
        "cubes": [
            {
                "name": "products_cube",
                "measures": [{"name": "products_cube.total_sales"}],
                "dimensions": [
                    {"name": "products_cube.product"},
                    {"name": "products_cube.product__redact"},
                    {"name": "products_cube.product__null"},
                    {"name": "products_cube.orphan__redact"},
                    {"name": "products_cube.x__hash"},
                    {"name": "products_cube.created_date"},
                    {"name": "products_cube.created_date__year"},
                ],
            }
        ]
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=raw_meta)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    from seeknal.serving.discovery.config import EngineConfig

    eng_cfg = EngineConfig(
        name="cube",
        kind="cube",
        dialect="postgres",
        url="http://cube:4000",
    )
    source = CubeSource(eng_cfg, client)
    cubes = source.discover()

    assert len(cubes) == 1
    cm = cubes[0]
    assert cm.name == "products_cube"
    assert cm.measures == ("total_sales",)
    # Ordinary dimensions only (no twins). orphan__redact and x__hash are ordinary because
    # orphan has no base dimension and hash is not a twin method.
    assert cm.dimensions == ("product", "orphan__redact", "x__hash", "created_date")
    assert cm.mask_variants == {
        "product": ("redact", "null"),
        "created_date": ("year",),
    }


def test_discovery_build_populates_cubes_and_schemas_without_twins():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/cubejs-api/v1/meta":
            return httpx.Response(
                200,
                json={
                    "cubes": [
                        {
                            "name": "sales_cube",
                            "measures": [{"name": "sales_cube.revenue"}],
                            "dimensions": [
                                {"name": "sales_cube.product"},
                                {"name": "sales_cube.product__redact"},
                            ],
                        }
                    ]
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = {
        "default_catalog": "lakekeeper",
        "engines": {
            "cube": {"kind": "cube", "dialect": "postgres", "url": "http://cube:4000"},
        },
    }
    res = discover(cfg, http_client=client)

    # placement["cubes"] gets ordinary dimensions and mask_variants
    cube_p = res.placement["cubes"]["sales_cube"]
    assert cube_p["measures"] == ["revenue"]
    assert cube_p["dimensions"] == ["product"]
    assert cube_p["mask_variants"] == {"product": ["redact"]}

    # schemas gets ordinary dimensions + measures (no twins) so cube queries resolve
    assert "sales_cube" in res.schemas
    assert res.schemas["sales_cube"] == ["product", "revenue"]


# ==============================================================================
# Part B: Enforcement in cube_mode="jwt" for cube targets
# ==============================================================================


def test_enforce_without_cube_mask_variants_behaves_as_if_no_twins_exist():
    sql = "SELECT product FROM products_cube"
    grant, schemas = _cube_claims_fixture_twins(sql, masks={"product": "redact"})

    # When called without cube_mask_variants (None), behaves as if no twins exist -> EnforcementError
    with pytest.raises(EnforcementError, match="twin member 'product__redact' for member 'product' with method 'redact' not found in mask variants"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt")


def test_hash_mask_on_cube_target_refused():
    sql = "SELECT product FROM products_cube"
    grant, schemas = _cube_claims_fixture_twins(sql, masks={"product": "hash"})

    with pytest.raises(EnforcementError, match="hash mask method is not supported on cube target"):
        enforce(
            sql,
            grant,
            schemas=schemas,
            target_dialect="postgres",
            target_kind="cube",
            cube_mode="jwt",
            cube_mask_variants={"products_cube": {"product": ("hash",)}},
        )


def test_measure_masked_on_cube_target_refused():
    sql = "SELECT total_sales FROM products_cube"
    grant, schemas = _cube_claims_fixture_twins(sql, masks={"total_sales": "redact"})

    with pytest.raises(EnforcementError, match="measure 'total_sales' cannot be masked"):
        enforce(
            sql,
            grant,
            schemas=schemas,
            target_dialect="postgres",
            target_kind="cube",
            cube_mode="jwt",
            cube_mask_variants={"products_cube": {"total_sales": ("redact",)}},
            cube_measures={"products_cube": {"total_sales"}},
        )


def test_twin_not_in_cube_mask_variants_fails_closed():
    sql = "SELECT product FROM products_cube"
    grant, schemas = _cube_claims_fixture_twins(sql, masks={"product": "year"})

    with pytest.raises(EnforcementError, match="twin member 'product__year' for member 'product' with method 'year' not found in mask variants"):
        enforce(
            sql,
            grant,
            schemas=schemas,
            target_dialect="postgres",
            target_kind="cube",
            cube_mode="jwt",
            cube_mask_variants={"products_cube": {"product": ("redact",)}},
        )


def _cube_claims_fixture_twins(sql: str, *, masks: dict[str, str] | None = None):
    return _make_cube_grant(sql, masks=masks)


def test_projection_rewrite_bare_aliased_qualified_and_star():
    cube_vars = {"products_cube": {"product": ("redact",)}}

    # 1. Bare: product -> product__redact AS product
    sql1 = "SELECT product FROM products_cube"
    g1, s1 = _cube_claims_fixture_twins(sql1, masks={"product": "redact"})
    res1 = enforce(sql1, g1, schemas=s1, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "product__redact AS product" in res1.sql
    assert "SELECT product__redact AS product FROM products_cube" in res1.sql

    # 2. Aliased: product AS p -> product__redact AS p
    sql2 = "SELECT product AS p FROM products_cube"
    g2, s2 = _cube_claims_fixture_twins(sql2, masks={"product": "redact"})
    res2 = enforce(sql2, g2, schemas=s2, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "product__redact AS p" in res2.sql

    # 3. Qualified: t.product -> t.product__redact AS product
    sql3 = "SELECT t.product FROM products_cube AS t"
    g3, s3 = _cube_claims_fixture_twins(sql3, masks={"product": "redact"})
    res3 = enforce(sql3, g3, schemas=s3, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "t.product__redact AS product" in res3.sql

    # 4. Qualified and aliased: t.product AS my_p -> t.product__redact AS my_p
    sql4 = "SELECT t.product AS my_p FROM products_cube AS t"
    g4, s4 = _cube_claims_fixture_twins(sql4, masks={"product": "redact"})
    res4 = enforce(sql4, g4, schemas=s4, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "t.product__redact AS my_p" in res4.sql

    # 5. Star projection: SELECT * FROM products_cube
    sql5 = "SELECT * FROM products_cube"
    g5, s5 = _cube_claims_fixture_twins(sql5, masks={"product": "redact"})
    res5 = enforce(sql5, g5, schemas=s5, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "product__redact AS product" in res5.sql


def test_after_rewrite_no_raw_masked_column_in_ast():
    cube_vars = {"products_cube": {"product": ("redact",)}}
    sql = "SELECT product, total_sales FROM products_cube GROUP BY product"
    grant, schemas = _cube_claims_fixture_twins(sql, masks={"product": "redact"})
    res = enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)

    from sqlglot import exp
    # No exp.Column named 'product'
    col_names = [col.name.lower() for col in res.statement.find_all(exp.Column)]
    assert "product" not in col_names
    assert "product__redact" in col_names


def test_group_by_allowed_forms_and_rewrites():
    cube_vars = {"products_cube": {"product": ("redact",)}}

    # 1. Named member: rewritten to twin
    sql1 = "SELECT product, count(*) FROM products_cube GROUP BY product"
    g1, s1 = _cube_claims_fixture_twins(sql1, masks={"product": "redact"})
    res1 = enforce(sql1, g1, schemas=s1, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "GROUP BY product__redact" in res1.sql
    assert "product__redact AS product" in res1.sql

    # 2. Qualified member: rewritten to twin with qualifier
    sql2 = "SELECT c.product, count(*) FROM products_cube AS c GROUP BY c.product"
    g2, s2 = _cube_claims_fixture_twins(sql2, masks={"product": "redact"})
    res2 = enforce(sql2, g2, schemas=s2, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "GROUP BY c.product__redact" in res2.sql

    # 3. Alias: left as is
    sql3 = "SELECT product AS p, count(*) FROM products_cube GROUP BY p"
    g3, s3 = _cube_claims_fixture_twins(sql3, masks={"product": "redact"})
    res3 = enforce(sql3, g3, schemas=s3, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "GROUP BY p" in res3.sql
    assert "product__redact AS p" in res3.sql

    # 4. Ordinal: left as is
    sql4 = "SELECT product, count(*) FROM products_cube GROUP BY 1"
    g4, s4 = _cube_claims_fixture_twins(sql4, masks={"product": "redact"})
    res4 = enforce(sql4, g4, schemas=s4, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)
    assert "GROUP BY 1" in res4.sql
    assert "product__redact AS product" in res4.sql


def test_group_by_invalid_uses_refused():
    cube_vars = {"products_cube": {"product": ("redact",)}}

    # 1. Expression in GROUP BY: upper(product)
    sql1 = "SELECT product, count(*) FROM products_cube GROUP BY upper(product)"
    g1, s1 = _cube_claims_fixture_twins(sql1, masks={"product": "redact"})
    with pytest.raises(EnforcementError, match="masked member 'product'"):
        enforce(sql1, g1, schemas=s1, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)

    # 2. Masked member not projected bare in SELECT
    sql2 = "SELECT count(*) FROM products_cube GROUP BY product"
    g2, s2 = _cube_claims_fixture_twins(sql2, masks={"product": "redact"})
    with pytest.raises(EnforcementError, match="masked member 'product'"):
        enforce(sql2, g2, schemas=s2, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)


def test_probe_rule_bypass_security_checks():
    """Verify that node identity prevents structural comparison bypasses."""
    cube_vars = {"products_cube": {"product": ("redact",)}}

    # 1. Bare projection with WHERE product = 'x'
    sql1 = "SELECT product FROM products_cube WHERE product = 'probe'"
    g1, s1 = _cube_claims_fixture_twins(sql1, masks={"product": "redact"})
    with pytest.raises(EnforcementError, match="masked member 'product'"):
        enforce(sql1, g1, schemas=s1, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)

    # 2. Bare projection with ORDER BY product
    sql2 = "SELECT product FROM products_cube ORDER BY product"
    g2, s2 = _cube_claims_fixture_twins(sql2, masks={"product": "redact"})
    with pytest.raises(EnforcementError, match="masked member 'product'"):
        enforce(sql2, g2, schemas=s2, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)

    # 3. Bare projection with GROUP BY upper(product)
    sql3 = "SELECT product FROM products_cube GROUP BY upper(product)"
    g3, s3 = _cube_claims_fixture_twins(sql3, masks={"product": "redact"})
    with pytest.raises(EnforcementError, match="masked member 'product'"):
        enforce(sql3, g3, schemas=s3, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)


def test_direct_naming_of_twin_dimension_refused():
    cube_vars = {"products_cube": {"product": ("redact",)}}
    # product__redact is not in schemas
    sql = "SELECT product__redact FROM products_cube"
    grant, schemas = _cube_claims_fixture_twins(sql, masks={"product": "redact"})

    with pytest.raises(EnforcementError, match="not found|does not resolve"):
        enforce(sql, grant, schemas=schemas, target_dialect="postgres", target_kind="cube", cube_mode="jwt", cube_mask_variants=cube_vars)


# ==============================================================================
# Part C: Connect timeout
# ==============================================================================


def test_cube_adapter_connect_timeout_driver_positive_int():
    recorded = {}

    class FakeCursor:
        def __init__(self):
            self.description = [("col", 23)]
        def execute(self, sql, params):
            pass
        def fetchmany(self, limit):
            return [(1,)]
        def close(self):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()
        def close(self):
            pass

    def fake_connect(**kwargs):
        recorded.update(kwargs)
        return FakeConn()

    sec = CubeSecurityConfig(secret=b"dummy-secret-bytes-1234567890123")
    prep = _make_sample_prepared_query()

    # 7 -> 7
    ad1 = CubeAdapter(fake_connect, sec, connect_timeout_seconds=7)
    ad1.execute(prep, timeout_seconds=5)
    assert recorded["connect_timeout"] == 7
    assert isinstance(recorded["connect_timeout"], int)

    # 7.0 -> 7
    ad2 = CubeAdapter(fake_connect, sec, connect_timeout_seconds=7.0)
    ad2.execute(prep, timeout_seconds=5)
    assert recorded["connect_timeout"] == 7
    assert isinstance(recorded["connect_timeout"], int)

    # 2.5 -> 3
    ad3 = CubeAdapter(fake_connect, sec, connect_timeout_seconds=2.5)
    ad3.execute(prep, timeout_seconds=5)
    assert recorded["connect_timeout"] == 3
    assert isinstance(recorded["connect_timeout"], int)

    # 10.0 -> 10
    ad4 = CubeAdapter(fake_connect, sec, connect_timeout_seconds=10.0)
    ad4.execute(prep, timeout_seconds=5)
    assert recorded["connect_timeout"] == 10
    assert isinstance(recorded["connect_timeout"], int)

    # 0.2 -> 1
    ad5 = CubeAdapter(fake_connect, sec, connect_timeout_seconds=0.2)
    ad5.execute(prep, timeout_seconds=5)
    assert recorded["connect_timeout"] == 1
    assert isinstance(recorded["connect_timeout"], int)


def test_make_cube_connect_helper_converts_float_to_int(monkeypatch):
    recorded = {}

    import psycopg2
    def fake_pg_connect(**kwargs):
        recorded.update(kwargs)
        return object()

    monkeypatch.setattr(psycopg2, "connect", fake_pg_connect)

    conn_fn = _make_cube_connect(
        host="localhost",
        port=15432,
        dbname="cube",
        user="seeknal",
        connect_timeout=2.5,
    )
    conn_fn()
    assert recorded["connect_timeout"] == 3
    assert isinstance(recorded["connect_timeout"], int)


def _make_sample_prepared_query():
    from seeknal.serving.service import PreparedQuery
    return PreparedQuery(
        engine="cube",
        engine_kind="cube",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        rule="test",
        reason="test",
        row_limit=10,
        masked_columns=(),
        redacted_sql="SELECT 1",
        subject="test-user",
        workspace="test-ws",
        audit_id="audit-123",
        cube_security_context={"masks": {}, "row_filters": {}},
    )


# ==============================================================================
# Part D: Timeout classification
# ==============================================================================


def test_failure_while_connecting_is_never_engine_timeout():
    sec = CubeSecurityConfig(secret=b"dummy-secret-bytes-1234567890123")
    prep = _make_sample_prepared_query()

    def connect_with_timeout_msg(**kwargs):
        raise RuntimeError("connection timed out after connect_timeout=10.0")

    adapter = CubeAdapter(connect_with_timeout_msg, sec)

    with pytest.raises(EngineError) as exc_info:
        adapter.execute(prep, timeout_seconds=5)

    assert not isinstance(exc_info.value, EngineTimeout)
    assert "Engine connection failed" in str(exc_info.value)


def test_is_timeout_error_classification():
    # 1. Allowed phrases
    assert _is_timeout_error(Exception("statement timeout expired")) is True
    assert _is_timeout_error(Exception("query timeout reached")) is True
    assert _is_timeout_error(Exception("exceeded time limit")) is True
    assert _is_timeout_error(Exception("execution was interrupted")) is True

    # 2. pymysql error codes
    class PyMySQLError(Exception):
        pass
    assert _is_timeout_error(PyMySQLError(1317, "interrupted")) is True
    assert _is_timeout_error(PyMySQLError(3024, "query execution was interrupted")) is True

    # 3. psycopg2 QueryCanceled
    class QueryCanceled(Exception):
        pass
    assert _is_timeout_error(QueryCanceled("cancelled")) is True

    # 4. Negative cases: option names or bare word timeout
    assert _is_timeout_error(Exception('invalid integer value "10.0" for connection option "connect_timeout"')) is False
    assert _is_timeout_error(Exception("connect_timeout parameter invalid")) is False
    assert _is_timeout_error(Exception("socket timeout on read")) is False
    assert _is_timeout_error(Exception("operation timeout")) is False
    assert _is_timeout_error(Exception("generic timeout occurred")) is False


# ==============================================================================
# Part E: Audit on refusals
# ==============================================================================


def test_audit_id_and_subject_logged_on_enforcement_error(ec_key, caplog):
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {"cube": {"kind": "cube", "dialect": "postgres"}},
        "tables": {},
        "cubes": {
            "products_cube": {
                "engine": "cube",
                "measures": ["total_sales"],
                "dimensions": ["product"],
                "mask_variants": {"product": ["redact"]},
            }
        },
    })
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"products_cube": ["product", "total_sales"]},
        public_keys={"k1": pub},
        cube_mode="jwt",
    )
    app = create_app(pipeline, {"cube": object()})
    client = TestClient(app)

    # Query with probe in WHERE -> triggers EnforcementError
    sql = "SELECT product FROM products_cube WHERE product = 'probe'"
    claims = {
        "sub": "audited-analyst@corp.org",
        "ws": "ws-audit",
        "jti": "jti-audit-refusal-777",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": time.time(),
        "exp": time.time() + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 100,
        "tables": {
            "products_cube": {
                "columns": ["product"],
                "masks": {"product": "redact"},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    token = sign_grant_token(claims, priv, kid="k1")

    # 1. /v1/query endpoint
    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql},
        )
        assert resp.status_code == 403
        log_text = caplog.text
        assert "audit_id=jti-audit-refusal-777" in log_text
        assert "subject=audited-analyst@corp.org" in log_text
        assert "status=403" in log_text
        # Never log SQL literals
        assert "'probe'" not in log_text

    # 2. /v1/explain endpoint
    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp_exp = client.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql},
        )
        assert resp_exp.status_code == 403
        log_text_exp = caplog.text
        assert "audit_id=jti-audit-refusal-777" in log_text_exp
        assert "subject=audited-analyst@corp.org" in log_text_exp
        assert "status=403" in log_text_exp
        assert "'probe'" not in log_text_exp


def test_token_verification_failure_keeps_dash_in_audit_log(caplog):
    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {},
        "cubes": {},
    })
    pipeline = ServingPipeline(
        placement=placement,
        schemas={},
        public_keys={},
    )
    app = create_app(pipeline, {})
    client = TestClient(app)

    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": "Bearer invalid.token.value"},
            json={"sql": "SELECT 1"},
        )
        assert resp.status_code == 401
        assert "audit_id=- subject=-" in caplog.text

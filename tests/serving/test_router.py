"""Tests for seeknal.serving.router."""

from __future__ import annotations

import pytest
import yaml

from seeknal.serving.placement import Placement
from seeknal.serving.router import Router, RoutingError

ROUTING_YAML = """
engines:
  sr:   {kind: starrocks, dialect: starrocks}
  cube: {kind: cube, dialect: postgres}
  lb:   {kind: lakebase, dialect: postgres}
  duck: {kind: duckdb, dialect: duckdb}
tables:
  claims.claim:
    keys: [claim_id]
    row_count: 120000000
    serve:
      sr: lakekeeper.claims.claim
      lb: {name: public.claim, max_lag_seconds: 300}
  ref.province:
    keys: [province_code]
    row_count: 38
    serve:
      sr: lakekeeper.ref.province
      lb: public.province
      duck: province
cubes:
  claims_cube:
    engine: cube
    measures: [total_amount, claim_count]
    dimensions: [province, idrg_code, admit_month]
policy:
  point_lookup_max_keys: 100
  analytics_min_rows: 1000000
"""


@pytest.fixture
def router():
    placement = Placement.from_dict(yaml.safe_load(ROUTING_YAML))
    return Router(placement)


def test_rule_point_lookup(router):
    decision = router.route("SELECT * FROM claims.claim WHERE claim_id = 42")
    assert decision.engine == "lb"
    assert decision.rule == "point_lookup"
    assert "filters claim_id by 1 value on table claims.claim" in decision.reason
    assert "public.claim AS claim" in decision.sql


def test_rule_point_lookup_in_list(router):
    decision = router.route(
        "SELECT * FROM claims.claim WHERE claim_id IN (10, 20, 30)"
    )
    assert decision.engine == "lb"
    assert decision.rule == "point_lookup"
    assert "filters claim_id by 3 values on table claims.claim" in decision.reason


def test_point_lookup_fall_through_when_lakebase_infeasible(router):
    # Lakebase has max_lag_seconds 300, request max_staleness 10
    decision = router.route(
        "SELECT * FROM claims.claim WHERE claim_id = 42",
        max_staleness_seconds=10,
    )
    # Lakebase rejected for staleness, falls through to analytics (120M rows)
    assert decision.engine == "sr"
    assert decision.rule == "analytics"
    assert "lb" in decision.rejected
    assert "staleness" in decision.rejected["lb"].lower()


def test_rule_analytics_due_to_row_count(router):
    # claims.claim has 120M rows >= 1M threshold
    decision = router.route("SELECT * FROM claims.claim")
    assert decision.engine == "sr"
    assert decision.rule == "analytics"
    assert "120000000 >= 1000000" in decision.reason


def test_rule_analytics_due_to_aggregation(router):
    # ref.province is small (38 rows), but query aggregates
    decision = router.route("SELECT count(*) FROM ref.province")
    assert decision.engine == "sr"
    assert decision.rule == "analytics"
    assert "aggregation" in decision.reason


def test_rule_analytics_due_to_join(router):
    decision = router.route(
        "SELECT * FROM ref.province p1 JOIN ref.province p2 ON p1.province_code = p2.province_code"
    )
    assert decision.engine == "sr"
    assert decision.rule == "analytics"
    assert "join" in decision.reason


def test_rule_analytics_due_to_distinct(router):
    decision = router.route("SELECT DISTINCT province_code FROM ref.province")
    assert decision.engine == "sr"
    assert decision.rule == "analytics"
    assert "distinct" in decision.reason


def test_rule_analytics_due_to_window(router):
    decision = router.route(
        "SELECT row_number() OVER () FROM ref.province"
    )
    assert decision.engine == "sr"
    assert decision.rule == "analytics"
    assert "window" in decision.reason


def test_rule_small_scan(router):
    # Small table (38 rows), no agg/join/group/window/distinct, no key equality
    decision = router.route("SELECT * FROM ref.province")
    assert decision.engine == "lb"  # first in small_scan_order ("lakebase", "starrocks", "duckdb")
    assert decision.rule == "small_scan"
    assert "public.province AS province" in decision.sql


def test_rule_semantic(router):
    decision = router.route("SELECT total_amount FROM claims_cube")
    assert decision.engine == "cube"
    assert decision.rule == "semantic"
    assert "claims_cube" in decision.sql


def test_rule_write(router):
    for sql in [
        "INSERT INTO ref.province (province_code) VALUES ('ON')",
        "UPDATE ref.province SET province_code = 'QC' WHERE province_code = 'ON'",
        "DELETE FROM ref.province WHERE province_code = 'QC'",
    ]:
        decision = router.route(sql)
        assert decision.engine == "lb"
        assert decision.rule == "write"


def test_rule_write_no_lakebase_raises():
    no_lb_yaml = """
engines:
  sr: {kind: starrocks, dialect: starrocks}
tables:
  t:
    keys: [id]
    serve:
      sr: lakekeeper.t
"""
    r = Router(Placement.from_dict(yaml.safe_load(no_lb_yaml)))
    with pytest.raises(RoutingError) as exc_info:
        r.route("INSERT INTO t VALUES (1)")
    assert "lakebase" in str(exc_info.value).lower()


def test_hint_argument_valid(router):
    decision = router.route("SELECT * FROM ref.province", hint="duck")
    assert decision.engine == "duck"
    assert decision.rule == "hint"


def test_hint_comment_valid(router):
    decision = router.route(
        "/* seeknal:engine=duck */ SELECT * FROM ref.province"
    )
    assert decision.engine == "duck"
    assert decision.rule == "hint"
    assert "seeknal:engine" not in decision.sql


def test_hint_comment_whitespace_tolerant(router):
    decision = router.route(
        "/*   seeknal:engine  =  duck   */ SELECT * FROM ref.province"
    )
    assert decision.engine == "duck"
    assert decision.rule == "hint"
    assert "seeknal:engine" not in decision.sql


def test_hint_argument_overrides_comment(router):
    decision = router.route(
        "/* seeknal:engine=duck */ SELECT * FROM ref.province", hint="sr"
    )
    assert decision.engine == "sr"
    assert decision.rule == "hint"
    assert "seeknal:engine" not in decision.sql


def test_hint_unknown_engine_raises(router):
    with pytest.raises(RoutingError) as exc_info:
        router.route("SELECT * FROM ref.province", hint="ghost_engine")
    assert "ghost_engine" in str(exc_info.value)

    with pytest.raises(RoutingError) as exc_info:
        router.route("/* seeknal:engine=ghost_engine */ SELECT * FROM ref.province")
    assert "ghost_engine" in str(exc_info.value)


def test_hint_infeasible_engine_raises(router):
    # duck does not serve claims.claim
    with pytest.raises(RoutingError) as exc_info:
        router.route("SELECT * FROM claims.claim", hint="duck")
    assert "duck" in str(exc_info.value)
    assert "not feasible" in str(exc_info.value)


def test_staleness_rejection(router):
    decision = router.route(
        "SELECT * FROM claims.claim", max_staleness_seconds=60
    )
    assert decision.engine == "sr"
    assert "lb" in decision.rejected
    assert "staleness" in decision.rejected["lb"].lower()


def test_unknown_table_raises(router):
    with pytest.raises(RoutingError) as exc_info:
        router.route("SELECT * FROM nonexistent_table")
    assert "nonexistent_table" in str(exc_info.value)


def test_cube_and_table_mixing_raises(router):
    with pytest.raises(RoutingError) as exc_info:
        router.route(
            "SELECT * FROM claims_cube JOIN ref.province ON claims_cube.province = ref.province.province_code"
        )
    assert "mix" in str(exc_info.value).lower()


def test_cubes_on_different_engines_raises():
    two_cubes_yaml = """
engines:
  c1: {kind: cube, dialect: postgres}
  c2: {kind: cube, dialect: postgres}
cubes:
  cube_a: {engine: c1, measures: [], dimensions: []}
  cube_b: {engine: c2, measures: [], dimensions: []}
"""
    r = Router(Placement.from_dict(yaml.safe_load(two_cubes_yaml)))
    with pytest.raises(RoutingError) as exc_info:
        r.route("SELECT * FROM cube_a JOIN cube_b ON 1=1")
    assert "different engines" in str(exc_info.value).lower()


def test_ddl_and_other_refusal(router):
    with pytest.raises(RoutingError) as exc_info:
        router.route("CREATE TABLE my_table (id INT)")
    assert "not served" in str(exc_info.value)

    with pytest.raises(RoutingError) as exc_info:
        router.route("DROP TABLE ref.province")
    assert "not served" in str(exc_info.value)

    with pytest.raises(RoutingError) as exc_info:
        router.route("SET work_mem = '64MB'")
    assert "not served" in str(exc_info.value)


def test_multiple_statements_and_empty(router):
    with pytest.raises(RoutingError) as exc_info:
        router.route("SELECT * FROM ref.province; SELECT * FROM claims.claim;")
    assert "1 statement" in str(exc_info.value)

    with pytest.raises(RoutingError) as exc_info:
        router.route("")
    assert "Empty" in str(exc_info.value)

    with pytest.raises(RoutingError) as exc_info:
        router.route("   ;   ")
    assert "Empty" in str(exc_info.value)


def test_cte_names_not_treated_as_tables(router):
    sql = "WITH c AS (SELECT * FROM ref.province) SELECT * FROM c"
    decision = router.route(sql)
    assert decision.engine == "lb"
    assert "public.province AS province" in decision.sql
    assert "c" not in decision.features.tables


def test_alias_preservation_in_rewrite(router):
    # With explicit alias
    sql1 = "SELECT cl.claim_id FROM claims.claim AS cl WHERE cl.claim_id = 1"
    d1 = router.route(sql1)
    assert "public.claim AS cl" in d1.sql

    # Without explicit alias: original last name segment added as alias
    sql2 = "SELECT claim.claim_id FROM claims.claim WHERE claim.claim_id = 1"
    d2 = router.route(sql2)
    assert "public.claim AS claim" in d2.sql


def test_dialect_rendering_starrocks(router):
    # StarRocks output for an analytics query
    sql = "SELECT count(*) AS cnt FROM claims.claim"
    decision = router.route(sql)
    assert decision.engine == "sr"
    assert "lakekeeper.claims.claim AS claim" in decision.sql
    # StarRocks dialect capitalizes COUNT(*)
    assert "COUNT(*)" in decision.sql


def test_explain_output(router):
    decision = router.route(
        "SELECT * FROM claims.claim WHERE claim_id = 1", max_staleness_seconds=10
    )
    explanation = decision.explain()
    assert "Engine: sr" in explanation
    assert "Rule: analytics" in explanation
    assert "Reason:" in explanation
    assert "Rejected engines:" in explanation
    assert "lb:" in explanation
    assert "staleness" in explanation


# ==============================================================================
# Table-Function Routing & End-to-End HTTP Enforcement Block Tests
# ==============================================================================

_TABLE_FUNCTION_CALLS: tuple[tuple[str, str], ...] = (
    ("iceberg_scan", "iceberg_scan('s3://bucket/table')"),
    ("read_parquet", "read_parquet('s3://bucket/table.parquet')"),
    ("read_csv", "read_csv('s3://bucket/table.csv')"),
    ("read_csv_auto", "read_csv_auto('s3://bucket/table.csv')"),
)

_TABLE_FUNCTION_SHAPES: tuple[tuple[str, str], ...] = (
    ("standalone", "SELECT * FROM {fn}"),
    ("join", "SELECT * FROM orders JOIN {fn} AS t ON orders.id = t.id"),
    ("cte", "WITH c AS (SELECT * FROM {fn}) SELECT * FROM c"),
    ("subquery", "SELECT * FROM (SELECT * FROM {fn}) AS sub"),
)


class RecordingFakeAdapter:
    """Fake adapter that records every engine call."""

    def __init__(self, engine: str) -> None:
        self.engine = engine
        self.calls: list[tuple] = []

    def execute(self, prepared, *, timeout_seconds: float, **kwargs):
        from seeknal.serving.adapters import QueryResult

        self.calls.append(("execute", prepared, timeout_seconds))
        return QueryResult(
            columns=("id", "amount"),
            rows=[(1, 50)],
            truncated=False,
            engine=self.engine,
            elapsed_ms=0.5,
        )

    def cancel(self, *args, **kwargs) -> None:
        self.calls.append(("cancel", args, kwargs))


@pytest.fixture(scope="module")
def engine_profile_setups(tmp_path_factory):
    import json
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from seeknal.serving.server.config import (
        DuckDBConnectionConfig,
        LakebaseConnectionConfig,
        ServerConfig,
        StarRocksConnectionConfig,
    )
    from seeknal.serving.service import ServingPipeline

    base = tmp_path_factory.mktemp("router_tf_e2e")
    priv = ec.generate_private_key(ec.SECP256R1())
    pub_pem = (
        priv.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("ascii")
    )
    pem_path = base / "pub.pem"
    pem_path.write_text(pub_pem, encoding="utf-8")

    ext_dir = base / "extensions"
    ext_dir.mkdir()
    temp_dir = base / "duck_tmp"
    temp_dir.mkdir()
    db_file = base / "local.duckdb"
    db_file.touch()

    env = {
        "LK_CLIENT_ID": "fake_cid",
        "LK_CLIENT_SECRET": "fake_csec",
        "S3_ACCESS_KEY_ID": "fake_akid",
        "S3_SECRET_ACCESS_KEY": "fake_ssec",
        "LAKEBASE_DSN": "postgresql://u:p@127.0.0.1:5432/appdb",
        "STARROCKS_URL": "mysql://u:p@127.0.0.1:9030/analytics",
    }

    specs = {
        "duckdb_local_file": (
            "duck_local",
            {"duck_local": {"kind": "duckdb", "dialect": "duckdb"}},
            {"duck_local": "main.orders"},
            {
                "duck_local": {
                    "kind": "duckdb",
                    "profile": "local_file",
                    "path": str(db_file),
                }
            },
        ),
        "duckdb_iceberg_direct": (
            "duck_ice",
            {"duck_ice": {"kind": "duckdb", "dialect": "duckdb"}},
            {
                "duck_ice": {
                    "name": "lake.analytics.orders",
                    "max_lag_seconds": 60.0,
                    "snapshot_id": 8877665544332211,
                }
            },
            {
                "duck_ice": {
                    "kind": "duckdb",
                    "profile": "iceberg_direct",
                    "extension_directory": str(ext_dir),
                    "extensions": ["httpfs", "avro", "iceberg"],
                    "allowed_s3_prefixes": ["s3://bucket/table/"],
                    "recycle_seconds": 240,
                    "token_lifetime_seconds": 300,
                    "temp_directory": str(temp_dir),
                    "max_temp_directory_size": "512MB",
                    "memory_limit": "256MB",
                    "threads": 2,
                    "iceberg_catalogs": {
                        "lake": {
                            "endpoint": "https://lakekeeper.internal.example/catalog",
                            "warehouse": "main_warehouse",
                            "token_uri": "https://idp.internal.example/oauth2/token",
                            "client_id_env": "LK_CLIENT_ID",
                            "client_secret_env": "LK_CLIENT_SECRET",
                            "scope": "lakekeeper:read",
                            "access_delegation": "vended",
                            "s3": {
                                "endpoint": "s3.internal.example:9000",
                                "region": "ap-southeast-1",
                                "url_style": "path",
                                "key_id_env": "S3_ACCESS_KEY_ID",
                                "secret_env": "S3_SECRET_ACCESS_KEY",
                                "use_ssl": True,
                            },
                        }
                    },
                }
            },
        ),
        "lakebase": (
            "lb",
            {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            {"lb": "public.orders"},
            {"lb": {"kind": "lakebase", "dsn_env": "LAKEBASE_DSN"}},
        ),
        "starrocks": (
            "sr",
            {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
            {"sr": "lakekeeper.analytics.orders"},
            {
                "sr": {
                    "kind": "starrocks",
                    "accounts": {"interactive": {"url_env": "STARROCKS_URL"}},
                }
            },
        ),
    }

    built: dict[str, tuple[str, ServingPipeline]] = {}
    for profile_key, (eng_name, engines, serve, connections) in specs.items():
        pf = base / f"{profile_key}_placement.json"
        sf = base / f"{profile_key}_schemas.json"
        pf.write_text(
            json.dumps(
                {
                    "engines": engines,
                    "tables": {
                        "orders": {"keys": ["id"], "row_count": 100, "serve": serve}
                    },
                    "cubes": {},
                }
            ),
            encoding="utf-8",
        )
        sf.write_text(
            json.dumps({"orders": ["id", "amount"]}),
            encoding="utf-8",
        )
        cfg = ServerConfig.from_dict(
            {
                "grant_keys": {"pem_files": {"k1": str(pem_path)}},
                "placement": {
                    "static": {
                        "placement_file": str(pf),
                        "schemas_file": str(sf),
                    }
                },
                "connections": connections,
            },
            env=env,
        )
        conn_cfg = cfg.connections[eng_name]
        if profile_key == "duckdb_local_file":
            assert isinstance(conn_cfg, DuckDBConnectionConfig)
            assert conn_cfg.profile == "local_file"
        elif profile_key == "duckdb_iceberg_direct":
            assert isinstance(conn_cfg, DuckDBConnectionConfig)
            assert conn_cfg.profile == "iceberg_direct"
        elif profile_key == "lakebase":
            assert isinstance(conn_cfg, LakebaseConnectionConfig)
        elif profile_key == "starrocks":
            assert isinstance(conn_cfg, StarRocksConnectionConfig)

        preflight = cfg.validate_runtime_wiring(env=env)
        assert preflight.placement is not None
        assert preflight.schemas is not None
        pipeline = ServingPipeline(
            placement=preflight.placement,
            schemas=preflight.schemas,
            public_keys=cfg.load_grant_keys(),
        )
        built[profile_key] = (eng_name, pipeline)

    return priv, built


@pytest.mark.parametrize(
    "engine_profile",
    [
        "duckdb_local_file",
        "duckdb_iceberg_direct",
        "lakebase",
        "starrocks",
    ],
)
def test_table_functions_produce_no_router_table_refs_and_never_rewrite_as_table(
    engine_profile_setups,
    engine_profile: str,
):
    _, setups = engine_profile_setups
    eng_name, pipeline = setups[engine_profile]
    r = pipeline.router

    for _, fn_call in _TABLE_FUNCTION_CALLS:
        for shape_name, tpl in _TABLE_FUNCTION_SHAPES:
            sql = tpl.format(fn=fn_call)
            decision = r.route(sql)
            assert decision.engine == eng_name
            if shape_name == "join":
                assert decision.features.tables == ("orders",)
            else:
                assert decision.features.tables == ()

    # Even if a table function's string argument matches a placed table name ('orders'),
    # Router.rewrite must not replace the table function with the physical table name.
    d_csv = r.route("SELECT * FROM read_csv('orders')")
    assert d_csv.features.tables == ()
    assert "read_csv('orders')" in d_csv.sql.lower()


@pytest.mark.parametrize(
    "engine_profile",
    [
        "duckdb_local_file",
        "duckdb_iceberg_direct",
        "lakebase",
        "starrocks",
    ],
)
def test_recording_fake_adapter_records_allowed_query(
    engine_profile_setups,
    engine_profile: str,
):
    import hashlib
    import time
    from starlette.testclient import TestClient
    from seeknal.serving.grant_token import sign_grant_token
    from seeknal.serving.http import create_app

    priv, setups = engine_profile_setups
    eng_name, pipeline = setups[engine_profile]
    adapter = RecordingFakeAdapter(eng_name)
    client = TestClient(create_app(pipeline, {eng_name: adapter}))

    sql = "SELECT id, amount FROM orders"
    now = time.time()
    jti = f"jti-sanity-ok-{engine_profile}"
    token = sign_grant_token(
        {
            "typ": "grant",
            "sub": "analyst@atlas.example",
            "ws": "ws-router-tf",
            "jti": jti,
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 60.0,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "max_rows": 100,
            "tables": {
                "orders": {
                    "columns": ["id", "amount"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        },
        priv,
        kid="k1",
    )
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 200
    assert len(adapter.calls) == 1


@pytest.mark.parametrize(
    "engine_profile",
    [
        "duckdb_local_file",
        "duckdb_iceberg_direct",
        "lakebase",
        "starrocks",
    ],
)
@pytest.mark.parametrize("endpoint", ["/v1/query", "/v1/explain"])
@pytest.mark.parametrize(
    "fn_name,fn_call",
    _TABLE_FUNCTION_CALLS,
    ids=[name for name, _ in _TABLE_FUNCTION_CALLS],
)
@pytest.mark.parametrize(
    "shape_name,sql_template",
    _TABLE_FUNCTION_SHAPES,
    ids=[name for name, _ in _TABLE_FUNCTION_SHAPES],
)
def test_http_query_and_explain_reject_table_functions_across_all_engines(
    engine_profile_setups,
    engine_profile: str,
    endpoint: str,
    fn_name: str,
    fn_call: str,
    shape_name: str,
    sql_template: str,
):
    import hashlib
    import time
    from starlette.testclient import TestClient
    from seeknal.serving.errors import ERROR_CATALOGUE
    from seeknal.serving.grant_token import sign_grant_token
    from seeknal.serving.http import create_app

    priv, setups = engine_profile_setups
    eng_name, pipeline = setups[engine_profile]
    adapter = RecordingFakeAdapter(eng_name)
    client = TestClient(create_app(pipeline, {eng_name: adapter}))

    sql = sql_template.format(fn=fn_call)
    now = time.time()
    ep_slug = endpoint.strip("/").replace("/", "-")
    jti = f"jti-router-tf-{engine_profile}-{ep_slug}-{fn_name}-{shape_name}"
    token = sign_grant_token(
        {
            "typ": "grant",
            "sub": "analyst@atlas.example",
            "ws": "ws-router-tf",
            "jti": jti,
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 60.0,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "max_rows": 100,
            "tables": {
                "orders": {
                    "columns": ["id", "amount"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        },
        priv,
        kid="k1",
    )

    resp = client.post(
        endpoint,
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )

    assert adapter.calls == []
    assert resp.status_code == 403
    body = resp.json()
    spec = ERROR_CATALOGUE["access_denied"]
    assert body["code"] == "access_denied"
    assert body["error"] == "enforcement_error"
    assert body["message"] == spec.message
    assert body["next_step"] == spec.next_step
    assert body["audit_id"] == jti
    assert "s3://bucket" not in resp.text


def test_postgres_wire_writability_and_preference_in_router():
    placement = Placement.from_dict(
        {
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "pg_ro": {"kind": "postgres", "dialect": "postgres"},
                "duck": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "orders": {
                    "keys": ["id"],
                    "row_count": 200,
                    "serve": {
                        "lb": "public.orders",
                        "pg_ro": "rpt.orders",
                        "duck": "orders",
                    },
                },
                "ro_only": {
                    "keys": ["id"],
                    "row_count": 50,
                    "serve": {"pg_ro": "rpt.ro_only"},
                },
            },
        }
    )
    r = Router(placement)

    # Read point_lookup prefers 'postgres' over 'lakebase' even when 'lb' is declared first
    d_pt = r.route("SELECT * FROM orders WHERE id = 1")
    assert d_pt.engine == "pg_ro"
    assert d_pt.rule == "point_lookup"

    # Read small_scan prefers 'postgres' over 'lakebase'
    d_scan = r.route("SELECT * FROM orders")
    assert d_scan.engine == "pg_ro"
    assert d_scan.rule == "small_scan"

    # Write goes to writable 'lb' (skipping read-only 'pg_ro')
    d_wr = r.route("INSERT INTO orders (id) VALUES (1)")
    assert d_wr.engine == "lb"
    assert d_wr.rule == "write"

    # Write to table only on read-only Postgres raises RoutingError before any connection
    with pytest.raises(RoutingError, match="not writable|no feasible writable"):
        r.route("INSERT INTO ro_only (id) VALUES (1)")

    # Hinted write to read-only Postgres raises RoutingError
    with pytest.raises(RoutingError, match="not writable"):
        r.route("INSERT INTO orders (id) VALUES (1)", hint="pg_ro")



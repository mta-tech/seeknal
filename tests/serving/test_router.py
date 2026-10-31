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

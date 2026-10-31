"""Tests for seeknal.serving.placement."""

from __future__ import annotations

import tempfile

import pytest
import yaml

from seeknal.serving.placement import (
    Engine,
    Placement,
    PlacementError,
    Policy,
)

FIXTURE_YAML = """
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


def test_placement_from_dict_and_yaml():
    data = yaml.safe_load(FIXTURE_YAML)
    p_dict = Placement.from_dict(data)

    with tempfile.NamedTemporaryFile("w+", suffix=".yaml") as tmp:
        tmp.write(FIXTURE_YAML)
        tmp.flush()
        p_yaml = Placement.from_yaml(tmp.name)

    for p in (p_dict, p_yaml):
        assert len(p.engines) == 4
        assert p.engines["sr"] == Engine(
            name="sr", kind="starrocks", dialect="starrocks"
        )
        assert p.engines["cube"] == Engine(name="cube", kind="cube", dialect="postgres")
        assert p.engines["lb"] == Engine(
            name="lb", kind="lakebase", dialect="postgres"
        )
        assert p.engines["duck"] == Engine(name="duck", kind="duckdb", dialect="duckdb")

        assert p.policy.point_lookup_max_keys == 100
        assert p.policy.analytics_min_rows == 1_000_000
        assert p.policy.small_scan_order == ("lakebase", "starrocks", "duckdb")
        assert p.policy.analytics_order == ("starrocks", "duckdb")

        # Table lookup
        tbl = p.find_table("claims.claim")
        assert tbl is not None
        assert tbl.name == "claims.claim"
        assert tbl.keys == ("claim_id",)
        assert tbl.row_count == 120_000_000
        assert len(tbl.serves) == 2

        # Case-insensitivity
        assert p.find_table("CLAIMS.CLAIM") is tbl
        assert p.find_table("Claims.Claim") is tbl
        # No default schema guessing
        assert p.find_table("claim") is None

        # Cube lookup
        cube = p.find_cube("claims_cube")
        assert cube is not None
        assert cube.name == "claims_cube"
        assert cube.engine == "cube"
        assert p.find_cube("CLAIMS_CUBE") is cube
        assert p.find_cube("Claims_Cube") is cube


def test_policy_defaults():
    minimal = {
        "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
        "tables": {
            "t": {"keys": ["id"], "serve": {"lb": "public.t"}},
        },
    }
    p = Placement.from_dict(minimal)
    assert p.policy == Policy()
    assert p.policy.point_lookup_max_keys == 100
    assert p.policy.analytics_min_rows == 1_000_000
    assert p.policy.small_scan_order == ("lakebase", "starrocks", "duckdb")
    assert p.policy.analytics_order == ("starrocks", "duckdb")


def test_placement_error_unknown_engine_kind():
    data = yaml.safe_load(FIXTURE_YAML)
    data["engines"]["bad"] = {"kind": "invalid_engine", "dialect": "postgres"}
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "invalid_engine" in str(exc_info.value)
    assert "bad" in str(exc_info.value)


def test_placement_error_table_serve_undefined_engine():
    data = yaml.safe_load(FIXTURE_YAML)
    data["tables"]["claims.claim"]["serve"]["nonexistent"] = "lakekeeper.claim"
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "claims.claim" in str(exc_info.value)
    assert "nonexistent" in str(exc_info.value)


def test_placement_error_cube_undefined_engine():
    data = yaml.safe_load(FIXTURE_YAML)
    data["cubes"]["claims_cube"]["engine"] = "nonexistent"
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "claims_cube" in str(exc_info.value)
    assert "nonexistent" in str(exc_info.value)


def test_placement_error_cube_engine_not_cube_kind():
    data = yaml.safe_load(FIXTURE_YAML)
    data["cubes"]["claims_cube"]["engine"] = "sr"
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "claims_cube" in str(exc_info.value)
    assert "sr" in str(exc_info.value)


def test_placement_error_table_has_no_serve():
    data = yaml.safe_load(FIXTURE_YAML)
    data["tables"]["claims.claim"]["serve"] = {}
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "claims.claim" in str(exc_info.value)
    assert "no serve" in str(exc_info.value)


@pytest.mark.parametrize("empty_val", ["", "   "])
def test_placement_error_empty_engine_name(empty_val):
    data = yaml.safe_load(FIXTURE_YAML)
    data["engines"][empty_val] = {"kind": "duckdb", "dialect": "duckdb"}
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "Engine name" in str(exc_info.value)


@pytest.mark.parametrize("empty_val", ["", "   "])
def test_placement_error_empty_table_name(empty_val):
    data = yaml.safe_load(FIXTURE_YAML)
    data["tables"][empty_val] = {
        "keys": ["k"],
        "serve": {"duck": "t"},
    }
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "Table name" in str(exc_info.value)


@pytest.mark.parametrize("empty_val", ["", "   "])
def test_placement_error_empty_cube_name(empty_val):
    data = yaml.safe_load(FIXTURE_YAML)
    data["cubes"][empty_val] = {
        "engine": "cube",
        "measures": [],
        "dimensions": [],
    }
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "Cube name" in str(exc_info.value)


def test_placement_error_empty_serve_physical_name():
    data = yaml.safe_load(FIXTURE_YAML)
    data["tables"]["claims.claim"]["serve"]["lb"] = {"name": "  "}
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    assert "claims.claim" in str(exc_info.value)


def test_placement_error_table_case_collision():
    data = yaml.safe_load(FIXTURE_YAML)
    data["tables"]["Claims.Claim"] = {
        "keys": ["id"],
        "serve": {"duck": "claim"},
    }
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    msg = str(exc_info.value)
    assert "Claims.Claim" in msg or "claims.claim" in msg
    assert "collides" in msg


def test_placement_error_cube_case_collision():
    data = yaml.safe_load(FIXTURE_YAML)
    data["cubes"]["CLAIMS_CUBE"] = {
        "engine": "cube",
        "measures": [],
        "dimensions": [],
    }
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    msg = str(exc_info.value)
    assert "CLAIMS_CUBE" in msg or "claims_cube" in msg
    assert "collides" in msg


def test_placement_error_table_cube_collision():
    data = yaml.safe_load(FIXTURE_YAML)
    data["cubes"]["claims.claim"] = {
        "engine": "cube",
        "measures": [],
        "dimensions": [],
    }
    with pytest.raises(PlacementError) as exc_info:
        Placement.from_dict(data)
    msg = str(exc_info.value)
    assert "claims.claim" in msg
    assert "collides" in msg

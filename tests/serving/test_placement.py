"""Tests for seeknal.serving.placement."""

from __future__ import annotations

import tempfile

import pytest
import yaml

from seeknal.serving.placement import (
    VALID_ACCESS_PATTERNS,
    VALID_ENGINE_KINDS,
    Engine,
    Placement,
    PlacementError,
    Policy,
    TableAccess,
    TableEntry,
    TableServe,
    is_postgres_wire,
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


def test_postgres_engine_kind_and_is_postgres_wire():
    assert VALID_ENGINE_KINDS == frozenset(
        {"starrocks", "cube", "lakebase", "postgres", "duckdb"}
    )
    assert is_postgres_wire("postgres") is True
    assert is_postgres_wire("lakebase") is True
    assert is_postgres_wire("duckdb") is False
    assert is_postgres_wire("starrocks") is False
    assert is_postgres_wire("cube") is False
    assert is_postgres_wire(None) is False


def test_engine_mode_endpoint_type_and_is_writable():
    # Default writability: lakebase -> True, postgres -> False, others -> False
    assert Engine("lb", "lakebase", "postgres").is_writable is True
    assert Engine("lb_rw", "lakebase", "postgres", mode="read_write").is_writable is True
    assert Engine("lb_ro", "lakebase", "postgres", mode="read_only").is_writable is False
    assert Engine("pg", "postgres", "postgres").is_writable is False
    assert Engine("pg_ro", "postgres", "postgres", mode="read_only").is_writable is False
    assert Engine("pg_rw", "postgres", "postgres", mode="read_write").is_writable is True
    assert Engine("sr", "starrocks", "starrocks").is_writable is False
    assert Engine("duck", "duckdb", "duckdb").is_writable is False

    # endpoint_type alias in Placement.from_dict
    p = Placement.from_dict(
        {
            "engines": {
                "lb_snap": {
                    "kind": "lakebase",
                    "dialect": "postgres",
                    "endpoint_type": "read_only",
                },
                "pg_rw": {
                    "kind": "postgres",
                    "dialect": "postgres",
                    "mode": "read_write",
                },
            },
            "tables": {"t": {"keys": ["id"], "serve": {"pg_rw": "public.t"}}},
        }
    )
    assert p.engines["lb_snap"].mode == "read_only"
    assert p.engines["lb_snap"].is_writable is False
    assert p.engines["pg_rw"].mode == "read_write"
    assert p.engines["pg_rw"].is_writable is True

    with pytest.raises(PlacementError, match="invalid mode"):
        Engine("pg", "postgres", "postgres", mode="rw")


def test_placement_rejects_storage_class_in_engines():
    with pytest.raises(
        PlacementError,
        match=(
            r"Engine 'pg' cannot declare 'storage_class' in placement\.engines; "
            r"declare 'storage_class' on connections\.pg instead"
        ),
    ):
        Placement.from_dict(
            {
                "engines": {
                    "pg": {
                        "kind": "postgres",
                        "dialect": "postgres",
                        "storage_class": "local",
                    }
                },
                "tables": {"t": {"keys": ["id"], "serve": {"pg": "public.t"}}},
            }
        )


def test_table_access_validation_and_key_inheritance():
    assert VALID_ACCESS_PATTERNS == frozenset({"point", "filter", "scan"})

    # Default require_filter from pattern
    a_point = TableAccess.from_dict({"pattern": "point", "keys": ["id"]})
    assert a_point.require_filter is True
    a_filter = TableAccess.from_dict(
        {"pattern": "filter", "filterable": ["cust_id"]}
    )
    assert a_filter.require_filter is True
    a_scan = TableAccess.from_dict({"pattern": "scan"})
    assert a_scan.require_filter is False

    # Explicit override of require_filter
    a_filter_opt = TableAccess.from_dict(
        {"pattern": "filter", "filterable": ["cust_id"], "require_filter": False}
    )
    assert a_filter_opt.require_filter is False

    # Point pattern without keys fails
    with pytest.raises(PlacementError, match="point.*keys"):
        TableAccess.from_dict({"pattern": "point"})

    # Bidirectional key inheritance in Placement.from_dict and TableEntry
    p = Placement.from_dict(
        {
            "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
            "tables": {
                "t_from_access": {
                    "serve": {"pg": "public.t1"},
                    "access": {"pattern": "point", "keys": ["k_acc"]},
                },
                "t_from_top": {
                    "keys": ["k_top"],
                    "serve": {"pg": "public.t2"},
                    "access": {"pattern": "point"},
                },
            },
        }
    )
    t1 = p.find_table("t_from_access")
    t2 = p.find_table("t_from_top")
    assert t1 is not None and t1.keys == ("k_acc",) and t1.access.keys == ("k_acc",)
    assert t2 is not None and t2.keys == ("k_top",) and t2.access.keys == ("k_top",)

    # Direct TableEntry inheritance
    te = TableEntry(
        name="direct_t",
        keys=("id",),
        row_count=10,
        serves=(TableServe("pg", "public.dt"),),
        access=TableAccess(pattern="point"),
    )
    assert te.access is not None and te.access.keys == ("id",)

    # Invalid TableAccess fields
    with pytest.raises(PlacementError, match="pattern"):
        TableAccess.from_dict({"pattern": "hash"})
    with pytest.raises(PlacementError, match="filterable"):
        TableAccess.from_dict({"pattern": "filter", "filterable": [""]})
    with pytest.raises(PlacementError, match="filterable"):
        TableAccess.from_dict({"pattern": "filter", "filterable": "col"})
    with pytest.raises(PlacementError, match="require_filter"):
        TableAccess.from_dict({"pattern": "scan", "require_filter": "yes"})
    for bad_max in (0, -5, True, False, "50"):
        with pytest.raises(PlacementError, match="max_rows"):
            TableAccess.from_dict({"pattern": "scan", "max_rows": bad_max})


def test_tokenized_pii_table_exempt_from_single_serve_and_duckdb_pii_engines():
    p = Placement.from_dict(
        {
            "engines": {
                "pg": {"kind": "postgres", "dialect": "postgres"},
                "duck": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "tok_customer": {
                    "keys": ["id"],
                    "pii": True,
                    "tokenized": True,
                    "serve": {
                        "pg": "public.tok_customer",
                        "duck": "lake.tok_customer",
                    },
                }
            },
        }
    )
    tbl = p.find_table("tok_customer")
    assert tbl is not None
    assert tbl.pii is True
    assert tbl.tokenized is True
    assert len(tbl.serves) == 2


def test_policy_new_fields_and_validation():
    p = Placement.from_dict(
        {
            "engines": {
                "pg": {"kind": "postgres", "dialect": "postgres"},
                "lb": {"kind": "lakebase", "dialect": "postgres"},
            },
            "tables": {"t": {"keys": ["id"], "serve": {"pg": "public.t"}}},
            "policy": {
                "pii_object_storage_engines": ["lb"],
                "require_filter_min_rows": 50_000,
                "max_in_list": 500,
                "max_offset": 2_000,
                "guard_mode": "warn",
            },
        }
    )
    assert p.policy.pii_object_storage_engines == ("lb",)
    assert p.policy.require_filter_min_rows == 50_000
    assert p.policy.max_in_list == 500
    assert p.policy.max_offset == 2_000
    assert p.policy.guard_mode == "warn"

    # Defaults
    default_pol = Policy()
    assert default_pol.pii_object_storage_engines == ()
    assert default_pol.require_filter_min_rows == 100_000
    assert default_pol.max_in_list == 1_000
    assert default_pol.max_offset == 10_000
    assert default_pol.guard_mode == "enforce"

    # Undefined engine in pii_object_storage_engines rejected
    with pytest.raises(PlacementError, match="pii_object_storage_engines.*ghost"):
        Placement.from_dict(
            {
                "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
                "tables": {"t": {"keys": ["id"], "serve": {"pg": "public.t"}}},
                "policy": {"pii_object_storage_engines": ["ghost"]},
            }
        )

    # Invalid guard_mode rejected
    with pytest.raises(PlacementError, match="guard_mode"):
        Policy(guard_mode="ignore")


"""Tests for RunPlan compilation, normalization, and plan_sha256 calculation."""

from pathlib import Path
import pytest

from seeknal.runtime.run_plan import (
    ConnectionAccess,
    PlanNode,
    RunPlan,
    RunPlanError,
    TableAccess,
    build_run_plan,
    canonical_plan_json,
    plan_sha256,
)


def test_iceberg_source_transform_iceberg_materialization(tmp_path: Path):
    """An Iceberg source -> transform -> Iceberg materialization."""
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: iceberg
table: warehouse.raw.customers
""")

    transforms_dir = tmp_path / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)
    (transforms_dir / "clean.yml").write_text("""\
kind: transform
name: clean
transform: SELECT * FROM source.raw
inputs:
  - ref: source.raw
materializations:
  - type: iceberg
    table: warehouse.analytics.clean_customers
    enabled: true
""")

    plan = build_run_plan(tmp_path)
    assert len(plan.nodes) == 2
    assert plan.reads == (
        TableAccess(table="warehouse.raw.customers", actions=("read",)),
    )
    assert plan.writes == (
        TableAccess(table="warehouse.analytics.clean_customers", actions=("write",)),
    )
    assert plan.connections == ()
    assert plan.has_python_nodes is False

    node_ids = [n.id for n in plan.nodes]
    assert node_ids == ["source.raw", "transform.clean"]

    raw_node = plan.nodes[0]
    assert raw_node.kind == "source"
    assert raw_node.reads == ("warehouse.raw.customers",)
    assert raw_node.writes == ()
    assert raw_node.connections == ()

    clean_node = plan.nodes[1]
    assert clean_node.kind == "transform"
    assert clean_node.reads == ()
    assert clean_node.writes == ("warehouse.analytics.clean_customers",)
    assert clean_node.connections == ()

    plan_dict = plan.to_dict()
    assert plan_dict["reads"] == [{"table": "warehouse.raw.customers", "actions": ["read"]}]
    assert plan_dict["writes"] == [{"table": "warehouse.analytics.clean_customers", "actions": ["write"]}]
    assert plan_dict["connections"] == []


def test_postgres_source_with_connection(tmp_path: Path):
    """A Postgres source with a connection and Postgres sink."""
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "pg_in.yml").write_text("""\
kind: source
name: pg_in
source: postgresql
table: public.users
params:
  connection: production_pg
""")

    transforms_dir = tmp_path / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)
    (transforms_dir / "pg_out.yml").write_text("""\
kind: transform
name: pg_out
transform: SELECT * FROM source.pg_in
inputs:
  - ref: source.pg_in
materializations:
  - type: postgresql
    connection: production_pg
    table: public.users_archive
    enabled: true
""")

    plan = build_run_plan(tmp_path)
    assert plan.reads == ()
    assert plan.writes == ()
    assert plan.connections == (
        ConnectionAccess(name="production_pg", actions=("read", "write")),
    )

    node0 = plan.nodes[0]
    assert node0.connections == ("production_pg",)
    node1 = plan.nodes[1]
    assert node1.connections == ("production_pg",)


def test_disabled_materialization_skipped(tmp_path: Path):
    """A disabled materialization target is skipped."""
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

    transforms_dir = tmp_path / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)
    (transforms_dir / "node.yml").write_text("""\
kind: transform
name: node
transform: SELECT * FROM source.raw
inputs:
  - ref: source.raw
materializations:
  - type: iceberg
    table: warehouse.skipped.table
    enabled: false
  - type: postgresql
    connection: skipped_conn
    table: public.skipped
    enabled: false
""")

    plan = build_run_plan(tmp_path)
    assert plan.writes == ()
    assert plan.connections == ()
    transform_node = plan.nodes[1]
    assert transform_node.writes == ()
    assert transform_node.connections == ()


def test_env_prefix_applied(tmp_path: Path):
    """The env prefix is applied to Iceberg materializations."""
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: iceberg
table: warehouse.raw.items
""")

    transforms_dir = tmp_path / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)
    (transforms_dir / "node.yml").write_text("""\
kind: transform
name: node
transform: SELECT * FROM source.raw
inputs:
  - ref: source.raw
materializations:
  - type: iceberg
    table: warehouse.analytics.prefixed_items
    enabled: true
""")

    plan = build_run_plan(tmp_path, env="staging")
    assert plan.writes == (
        TableAccess(table="warehouse.staging_analytics.prefixed_items", actions=("write",)),
    )
    # Source table is NOT prefixed by --env
    assert plan.reads == (
        TableAccess(table="warehouse.raw.items", actions=("read",)),
    )


def test_non_3_part_iceberg_table_raises_error(tmp_path: Path):
    """A source or sink whose Iceberg table is not 3-part raises RunPlanError naming the node."""
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "bad_source.yml").write_text("""\
kind: source
name: bad_source
source: iceberg
table: two_part.table
""")

    with pytest.raises(RunPlanError) as exc_info:
        build_run_plan(tmp_path)
    assert "source.bad_source" in str(exc_info.value)
    assert "3-part" in str(exc_info.value)

    # Now fix source and introduce bad sink
    (sources_dir / "bad_source.yml").write_text("""\
kind: source
name: bad_source
source: iceberg
table: warehouse.raw.good
""")

    transforms_dir = tmp_path / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)
    (transforms_dir / "bad_sink.yml").write_text("""\
kind: transform
name: bad_sink
transform: SELECT * FROM source.bad_source
inputs:
  - ref: source.bad_source
materializations:
  - type: iceberg
    table: single_part_table
    enabled: true
""")

    with pytest.raises(RunPlanError) as exc_info2:
        build_run_plan(tmp_path)
    assert "transform.bad_sink" in str(exc_info2.value)
    assert "3-part" in str(exc_info2.value)


def test_deterministic_plan_sha256_independent_of_file_order(tmp_path: Path):
    """plan_sha256 is deterministic and identical regardless of creation order."""
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    transforms_dir = tmp_path / "seeknal" / "transforms"
    transforms_dir.mkdir(parents=True)

    (sources_dir / "b_source.yml").write_text("""\
kind: source
name: b_source
source: iceberg
table: warehouse.db.b_table
""")
    (sources_dir / "a_source.yml").write_text("""\
kind: source
name: a_source
source: iceberg
table: warehouse.db.a_table
""")

    (transforms_dir / "z_trans.yml").write_text("""\
kind: transform
name: z_trans
transform: SELECT * FROM source.b_source
inputs:
  - ref: source.b_source
materializations:
  - type: iceberg
    table: warehouse.db.z_out
    enabled: true
""")
    (transforms_dir / "y_trans.yml").write_text("""\
kind: transform
name: y_trans
transform: SELECT * FROM source.a_source
inputs:
  - ref: source.a_source
materializations:
  - type: iceberg
    table: warehouse.db.y_out
    enabled: true
""")

    plan1 = build_run_plan(tmp_path)
    sha1 = plan_sha256(plan1)

    plan2 = build_run_plan(tmp_path)
    sha2 = plan_sha256(plan2)
    assert sha1 == sha2
    assert len(sha1) == 64


def test_python_node_included(tmp_path: Path):
    """PYTHON nodes are included with their declared materializations."""
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "raw.yml").write_text("""\
kind: source
name: raw
source: csv
table: data.csv
""")

    pipelines_dir = tmp_path / "seeknal" / "pipelines"
    pipelines_dir.mkdir(parents=True)
    (pipelines_dir / "py_step.py").write_text("""\
from seeknal.pipeline.decorators import transform, materialize

@transform(name="py_calc", inputs=["source.raw"])
@materialize(type="iceberg", table="warehouse.analytics.py_output")
def py_calc(ctx):
    return ctx.ref("source.raw")
""")

    plan = build_run_plan(tmp_path)
    assert plan.has_python_nodes is True

    node_ids = [n.id for n in plan.nodes]
    assert "transform.py_calc" in node_ids

    calc_node = next(n for n in plan.nodes if n.id == "transform.py_calc")
    assert calc_node.kind == "python"
    assert calc_node.writes == ("warehouse.analytics.py_output",)

    assert plan.writes == (
        TableAccess(table="warehouse.analytics.py_output", actions=("write",)),
    )

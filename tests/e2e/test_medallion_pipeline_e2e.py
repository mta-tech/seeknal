"""
End-to-end tests for Medallion Architecture pipelines.

Tests both YAML and Python pipeline approaches for the telecom
medallion architecture (Bronze -> Silver -> Gold) with Iceberg
materialization.
"""

import pytest
import tempfile
from pathlib import Path
from textwrap import dedent

from seeknal.pipeline.decorators import clear_registry
from seeknal.workflow.dag import DAGBuilder


# =============================================================================
# YAML content for the medallion pipeline (simplified — no common config)
# =============================================================================

YAML_SOURCE_TRAFFIC = dedent("""\
    kind: source
    name: traffic_daily
    source: iceberg
    table: "atlas.test_ns.traffic_daily"
    params:
      catalog_uri: http://localhost:8181
      warehouse: test-warehouse
    tags: ["bronze"]
""")

YAML_SOURCE_SUBSCRIBER = dedent("""\
    kind: source
    name: subscriber_daily
    source: iceberg
    table: "atlas.test_ns.subscriber_daily"
    params:
      catalog_uri: http://localhost:8181
      warehouse: test-warehouse
    tags: ["bronze"]
""")

YAML_SOURCE_CELLS = dedent("""\
    kind: source
    name: cell_sites
    source: iceberg
    table: "atlas.test_ns.cell_sites"
    params:
      catalog_uri: http://localhost:8181
      warehouse: test-warehouse
    tags: ["bronze"]
""")

YAML_TRANSFORM_TRAFFIC_ENRICHED = dedent("""\
    kind: transform
    name: traffic_enriched
    inputs:
      - ref: source.traffic_daily
      - ref: source.cell_sites
    transform: |
      SELECT t.*, c.region, c.city
      FROM ref('source.traffic_daily') t
      JOIN ref('source.cell_sites') c ON t.cell_id = c.cell_id
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.silver_traffic_enriched
    tags: ["silver"]
""")

YAML_TRANSFORM_SUBSCRIBER_PROFILE = dedent("""\
    kind: transform
    name: subscriber_profile
    inputs:
      - ref: source.subscriber_daily
    transform: |
      SELECT *, 'Standard' AS subscriber_segment
      FROM ref('source.subscriber_daily')
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.silver_subscriber_profile
    tags: ["silver"]
""")

YAML_TRANSFORM_ACTIVE_TRAFFIC = dedent("""\
    kind: transform
    name: active_traffic
    inputs:
      - ref: transform.traffic_enriched
      - ref: source.subscriber_daily
    transform: |
      SELECT t.*, s.plan_type
      FROM ref('transform.traffic_enriched') t
      JOIN ref('source.subscriber_daily') s
        ON t.msisdn = s.msisdn AND t.date_id = s.date_id
      WHERE s.status = 'Active'
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.silver_active_traffic
    tags: ["silver"]
""")

YAML_TRANSFORM_VOICE_METRICS = dedent("""\
    kind: transform
    name: daily_voice_metrics
    inputs:
      - ref: transform.active_traffic
    transform: |
      SELECT date_id, msisdn, CAST(COUNT(*) AS BIGINT) AS call_count
      FROM ref('transform.active_traffic')
      WHERE service_type = 'Voice'
      GROUP BY date_id, msisdn
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.gold_daily_voice_metrics
    tags: ["gold"]
""")

YAML_TRANSFORM_DATA_USAGE = dedent("""\
    kind: transform
    name: daily_data_usage
    inputs:
      - ref: transform.active_traffic
    transform: |
      SELECT date_id, msisdn, CAST(COUNT(*) AS BIGINT) AS data_sessions
      FROM ref('transform.active_traffic')
      WHERE service_type = 'Data'
      GROUP BY date_id, msisdn
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.gold_daily_data_usage
    tags: ["gold"]
""")

YAML_TRANSFORM_SERVICE_MIX = dedent("""\
    kind: transform
    name: service_mix_daily
    inputs:
      - ref: transform.active_traffic
    transform: |
      SELECT date_id, msisdn, CAST(SUM(revenue) AS BIGINT) AS total_revenue
      FROM ref('transform.active_traffic')
      GROUP BY date_id, msisdn
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.gold_service_mix_daily
    tags: ["gold"]
""")

YAML_TRANSFORM_REGIONAL_KPI = dedent("""\
    kind: transform
    name: regional_kpi
    inputs:
      - ref: transform.active_traffic
    transform: |
      SELECT date_id, region, CAST(COUNT(*) AS BIGINT) AS total_txn
      FROM ref('transform.active_traffic')
      GROUP BY date_id, region
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.gold_regional_kpi
    tags: ["gold"]
""")

YAML_TRANSFORM_SUBSCRIBER_360 = dedent("""\
    kind: transform
    name: subscriber_360
    inputs:
      - ref: transform.subscriber_profile
      - ref: transform.service_mix_daily
    transform: |
      SELECT p.*, COALESCE(m.total_revenue, 0) AS total_revenue
      FROM ref('transform.subscriber_profile') p
      LEFT JOIN ref('transform.service_mix_daily') m
        ON p.msisdn = m.msisdn AND p.date_id = m.date_id
    materialization:
      enabled: true
      mode: overwrite
      table: atlas.test_ns.gold_subscriber_360
    tags: ["gold"]
""")


# =============================================================================
# Python pipeline content for the medallion pipeline
# =============================================================================

PYTHON_MEDALLION_PIPELINE = dedent('''\
    """Python medallion pipeline for e2e testing."""
    from seeknal.pipeline.decorators import source, transform
    from seeknal.pipeline.materialization_config import MaterializationConfig

    # Bronze
    @source(name="traffic_daily", source="iceberg", table="atlas.test_ns.traffic_daily",
            tags=["bronze"], catalog_uri="http://localhost:8181", warehouse="test-warehouse")
    def traffic_daily():
        pass

    @source(name="subscriber_daily", source="iceberg", table="atlas.test_ns.subscriber_daily",
            tags=["bronze"], catalog_uri="http://localhost:8181", warehouse="test-warehouse")
    def subscriber_daily():
        pass

    @source(name="cell_sites", source="iceberg", table="atlas.test_ns.cell_sites",
            tags=["bronze"], catalog_uri="http://localhost:8181", warehouse="test-warehouse")
    def cell_sites():
        pass

    # Silver
    @transform(name="traffic_enriched", inputs=["source.traffic_daily", "source.cell_sites"],
               tags=["silver"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.silver_traffic_enriched", mode="overwrite"))
    def traffic_enriched(ctx):
        t = ctx.ref("source.traffic_daily")
        c = ctx.ref("source.cell_sites")
        return ctx.duckdb.sql("SELECT t.*, c.region, c.city FROM t JOIN c ON t.cell_id = c.cell_id").df()

    @transform(name="subscriber_profile", inputs=["source.subscriber_daily"],
               tags=["silver"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.silver_subscriber_profile", mode="overwrite"))
    def subscriber_profile(ctx):
        ctx.ref("source.subscriber_daily")
        return ctx.duckdb.sql("SELECT *, \'Standard\' AS subscriber_segment FROM subscriber_daily").df()

    @transform(name="active_traffic", inputs=["transform.traffic_enriched", "source.subscriber_daily"],
               tags=["silver"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.silver_active_traffic", mode="overwrite"))
    def active_traffic(ctx):
        t = ctx.ref("transform.traffic_enriched")
        s = ctx.ref("source.subscriber_daily")
        return ctx.duckdb.sql("SELECT t.*, s.plan_type FROM t JOIN s ON t.msisdn = s.msisdn AND t.date_id = s.date_id WHERE s.status = \'Active\'").df()

    # Gold
    @transform(name="daily_voice_metrics", inputs=["transform.active_traffic"],
               tags=["gold"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.gold_daily_voice_metrics", mode="overwrite"))
    def daily_voice_metrics(ctx):
        ctx.ref("transform.active_traffic")
        return ctx.duckdb.sql("SELECT date_id, msisdn, CAST(COUNT(*) AS BIGINT) AS call_count FROM active_traffic WHERE service_type = \'Voice\' GROUP BY date_id, msisdn").df()

    @transform(name="daily_data_usage", inputs=["transform.active_traffic"],
               tags=["gold"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.gold_daily_data_usage", mode="overwrite"))
    def daily_data_usage(ctx):
        ctx.ref("transform.active_traffic")
        return ctx.duckdb.sql("SELECT date_id, msisdn, CAST(COUNT(*) AS BIGINT) AS data_sessions FROM active_traffic WHERE service_type = \'Data\' GROUP BY date_id, msisdn").df()

    @transform(name="service_mix_daily", inputs=["transform.active_traffic"],
               tags=["gold"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.gold_service_mix_daily", mode="overwrite"))
    def service_mix_daily(ctx):
        ctx.ref("transform.active_traffic")
        return ctx.duckdb.sql("SELECT date_id, msisdn, CAST(SUM(revenue) AS BIGINT) AS total_revenue FROM active_traffic GROUP BY date_id, msisdn").df()

    @transform(name="regional_kpi", inputs=["transform.active_traffic"],
               tags=["gold"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.gold_regional_kpi", mode="overwrite"))
    def regional_kpi(ctx):
        ctx.ref("transform.active_traffic")
        return ctx.duckdb.sql("SELECT date_id, region, CAST(COUNT(*) AS BIGINT) AS total_txn FROM active_traffic GROUP BY date_id, region").df()

    @transform(name="subscriber_360", inputs=["transform.subscriber_profile", "transform.service_mix_daily"],
               tags=["gold"],
               materialization=MaterializationConfig(enabled=True, table="atlas.test_ns.gold_subscriber_360", mode="overwrite"))
    def subscriber_360(ctx):
        p = ctx.ref("transform.subscriber_profile")
        m = ctx.ref("transform.service_mix_daily")
        return ctx.duckdb.sql("SELECT p.*, COALESCE(m.total_revenue, 0) AS total_revenue FROM p LEFT JOIN m ON p.msisdn = m.msisdn AND p.date_id = m.date_id").df()
''')


# =============================================================================
# Helpers
# =============================================================================

def _create_yaml_project(tmpdir: Path) -> Path:
    """Create a YAML-based medallion pipeline project in a temp directory."""
    project_path = tmpdir
    seeknal_dir = project_path / "seeknal"
    sources_dir = seeknal_dir / "sources"
    transforms_dir = seeknal_dir / "transforms"
    sources_dir.mkdir(parents=True)
    transforms_dir.mkdir(parents=True)

    # Write sources
    (sources_dir / "traffic_daily.yml").write_text(YAML_SOURCE_TRAFFIC)
    (sources_dir / "subscriber_daily.yml").write_text(YAML_SOURCE_SUBSCRIBER)
    (sources_dir / "cell_sites.yml").write_text(YAML_SOURCE_CELLS)

    # Write transforms
    (transforms_dir / "traffic_enriched.yml").write_text(YAML_TRANSFORM_TRAFFIC_ENRICHED)
    (transforms_dir / "subscriber_profile.yml").write_text(YAML_TRANSFORM_SUBSCRIBER_PROFILE)
    (transforms_dir / "active_traffic.yml").write_text(YAML_TRANSFORM_ACTIVE_TRAFFIC)
    (transforms_dir / "daily_voice_metrics.yml").write_text(YAML_TRANSFORM_VOICE_METRICS)
    (transforms_dir / "daily_data_usage.yml").write_text(YAML_TRANSFORM_DATA_USAGE)
    (transforms_dir / "service_mix_daily.yml").write_text(YAML_TRANSFORM_SERVICE_MIX)
    (transforms_dir / "regional_kpi.yml").write_text(YAML_TRANSFORM_REGIONAL_KPI)
    (transforms_dir / "subscriber_360.yml").write_text(YAML_TRANSFORM_SUBSCRIBER_360)

    return project_path


def _create_python_project(tmpdir: Path) -> Path:
    """Create a Python-based medallion pipeline project in a temp directory."""
    project_path = tmpdir
    seeknal_dir = project_path / "seeknal"
    pipelines_dir = seeknal_dir / "pipelines"
    pipelines_dir.mkdir(parents=True)

    (pipelines_dir / "medallion.py").write_text(PYTHON_MEDALLION_PIPELINE)

    return project_path


# Expected node structure
EXPECTED_SOURCE_IDS = {
    "source.traffic_daily",
    "source.subscriber_daily",
    "source.cell_sites",
}

EXPECTED_TRANSFORM_IDS = {
    "transform.traffic_enriched",
    "transform.subscriber_profile",
    "transform.active_traffic",
    "transform.daily_voice_metrics",
    "transform.daily_data_usage",
    "transform.service_mix_daily",
    "transform.regional_kpi",
    "transform.subscriber_360",
}

EXPECTED_ALL_IDS = EXPECTED_SOURCE_IDS | EXPECTED_TRANSFORM_IDS

# Expected edges (from -> to)
EXPECTED_EDGES = {
    ("source.traffic_daily", "transform.traffic_enriched"),
    ("source.cell_sites", "transform.traffic_enriched"),
    ("source.subscriber_daily", "transform.subscriber_profile"),
    ("source.subscriber_daily", "transform.active_traffic"),
    ("transform.traffic_enriched", "transform.active_traffic"),
    ("transform.active_traffic", "transform.daily_voice_metrics"),
    ("transform.active_traffic", "transform.daily_data_usage"),
    ("transform.active_traffic", "transform.service_mix_daily"),
    ("transform.active_traffic", "transform.regional_kpi"),
    ("transform.subscriber_profile", "transform.subscriber_360"),
    ("transform.service_mix_daily", "transform.subscriber_360"),
}


# =============================================================================
# Test Class 1: YAML Medallion Pipeline DAG
# =============================================================================

class TestYAMLMedallionDAG:
    """Test YAML-based medallion pipeline DAG construction."""

    def test_yaml_dag_builds_all_nodes(self):
        """YAML pipeline should produce 11 nodes (3 sources + 8 transforms)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_yaml_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            assert len(builder.nodes) == 11
            assert set(builder.nodes.keys()) == EXPECTED_ALL_IDS

    def test_yaml_dag_source_nodes_are_iceberg(self):
        """Source nodes should have source=iceberg."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_yaml_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            for source_id in EXPECTED_SOURCE_IDS:
                node = builder.nodes[source_id]
                assert node.config["source"] == "iceberg"

    def test_yaml_dag_edges_match_expected(self):
        """DAG edges should match the medallion dependency graph."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_yaml_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            actual_edges = set()
            for edge in builder.edges:
                actual_edges.add((edge.from_node, edge.to_node))

            assert EXPECTED_EDGES == actual_edges

    def test_yaml_dag_materialization_on_transforms(self):
        """All transforms should have materialization enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_yaml_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            for transform_id in EXPECTED_TRANSFORM_IDS:
                node = builder.nodes[transform_id]
                mat = node.yaml_data.get("materialization") or node.config.get("materialization")
                assert mat is not None, f"{transform_id} missing materialization"
                assert mat["enabled"] is True, f"{transform_id} materialization not enabled"
                assert mat["mode"] == "overwrite"

    def test_yaml_dag_topological_order(self):
        """Topological sort should place sources before transforms."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_yaml_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            order = builder.topological_sort()

            # All sources must come before all transforms that depend on them
            for src, dst in EXPECTED_EDGES:
                src_idx = order.index(src)
                dst_idx = order.index(dst)
                assert src_idx < dst_idx, f"{src} should come before {dst}"

    def test_yaml_dag_sources_have_no_materialization(self):
        """Source nodes should NOT have materialization (read-only Iceberg)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_yaml_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            for source_id in EXPECTED_SOURCE_IDS:
                node = builder.nodes[source_id]
                mat = node.yaml_data.get("materialization") or node.config.get("materialization")
                assert mat is None, f"{source_id} should not have materialization"


# =============================================================================
# Test Class 2: Python Medallion Pipeline DAG
# =============================================================================

class TestPythonMedallionDAG:
    """Test Python-based medallion pipeline DAG construction."""

    def setup_method(self):
        clear_registry()

    def test_python_dag_builds_all_nodes(self):
        """Python pipeline should produce 11 nodes (3 sources + 8 transforms)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_python_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            assert len(builder.nodes) == 11
            assert set(builder.nodes.keys()) == EXPECTED_ALL_IDS

    def test_python_dag_source_nodes_are_iceberg(self):
        """Source nodes should have source=iceberg."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_python_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            for source_id in EXPECTED_SOURCE_IDS:
                node = builder.nodes[source_id]
                assert node.config["source"] == "iceberg"

    def test_python_dag_edges_match_expected(self):
        """DAG edges should match the medallion dependency graph."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_python_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            actual_edges = set()
            for edge in builder.edges:
                actual_edges.add((edge.from_node, edge.to_node))

            assert EXPECTED_EDGES == actual_edges

    def test_python_dag_materialization_on_transforms(self):
        """All transforms should have materialization enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_python_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            for transform_id in EXPECTED_TRANSFORM_IDS:
                node = builder.nodes[transform_id]
                mat = node.yaml_data.get("materialization") or node.config.get("materialization")
                assert mat is not None, f"{transform_id} missing materialization"
                assert mat["enabled"] is True
                assert mat["mode"] == "overwrite"

    def test_python_dag_topological_order(self):
        """Topological sort should place sources before transforms."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_python_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            order = builder.topological_sort()

            for src, dst in EXPECTED_EDGES:
                src_idx = order.index(src)
                dst_idx = order.index(dst)
                assert src_idx < dst_idx, f"{src} should come before {dst}"

    def test_python_dag_sources_have_no_materialization(self):
        """Source nodes should NOT have materialization (read-only Iceberg)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_python_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            for source_id in EXPECTED_SOURCE_IDS:
                node = builder.nodes[source_id]
                mat = node.yaml_data.get("materialization") or node.config.get("materialization")
                assert mat is None, f"{source_id} should not have materialization"

    def test_python_dag_transform_inputs_declared(self):
        """All transforms should have inputs declared in decorator."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _create_python_project(Path(tmpdir))
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # active_traffic depends on traffic_enriched + subscriber_daily
            deps = builder.get_upstream("transform.active_traffic")
            assert "transform.traffic_enriched" in deps
            assert "source.subscriber_daily" in deps


# =============================================================================
# Test Class 3: YAML vs Python Parity
# =============================================================================

class TestMedallionParity:
    """Test that YAML and Python pipelines produce equivalent DAG structures."""

    def setup_method(self):
        clear_registry()

    def test_same_node_count(self):
        """Both approaches should produce the same number of nodes."""
        with tempfile.TemporaryDirectory() as yaml_dir, \
             tempfile.TemporaryDirectory() as py_dir:
            yaml_path = _create_yaml_project(Path(yaml_dir))
            py_path = _create_python_project(Path(py_dir))

            yaml_builder = DAGBuilder(project_path=yaml_path)
            yaml_builder.build()

            clear_registry()

            py_builder = DAGBuilder(project_path=py_path)
            py_builder.build()

            assert len(yaml_builder.nodes) == len(py_builder.nodes)

    def test_same_node_ids(self):
        """Both approaches should produce the same node IDs."""
        with tempfile.TemporaryDirectory() as yaml_dir, \
             tempfile.TemporaryDirectory() as py_dir:
            yaml_path = _create_yaml_project(Path(yaml_dir))
            py_path = _create_python_project(Path(py_dir))

            yaml_builder = DAGBuilder(project_path=yaml_path)
            yaml_builder.build()

            clear_registry()

            py_builder = DAGBuilder(project_path=py_path)
            py_builder.build()

            assert set(yaml_builder.nodes.keys()) == set(py_builder.nodes.keys())

    def test_same_edge_structure(self):
        """Both approaches should produce the same dependency edges."""
        with tempfile.TemporaryDirectory() as yaml_dir, \
             tempfile.TemporaryDirectory() as py_dir:
            yaml_path = _create_yaml_project(Path(yaml_dir))
            py_path = _create_python_project(Path(py_dir))

            yaml_builder = DAGBuilder(project_path=yaml_path)
            yaml_builder.build()

            clear_registry()

            py_builder = DAGBuilder(project_path=py_path)
            py_builder.build()

            def get_edges(builder):
                edges = set()
                for edge in builder.edges:
                    edges.add((edge.from_node, edge.to_node))
                return edges

            assert get_edges(yaml_builder) == get_edges(py_builder)

    def test_same_materialization_tables(self):
        """Both approaches should materialize to the same Iceberg tables."""
        with tempfile.TemporaryDirectory() as yaml_dir, \
             tempfile.TemporaryDirectory() as py_dir:
            yaml_path = _create_yaml_project(Path(yaml_dir))
            py_path = _create_python_project(Path(py_dir))

            yaml_builder = DAGBuilder(project_path=yaml_path)
            yaml_builder.build()

            clear_registry()

            py_builder = DAGBuilder(project_path=py_path)
            py_builder.build()

            def get_mat_tables(builder):
                tables = {}
                for node_id, node in builder.nodes.items():
                    mat = node.yaml_data.get("materialization") or node.config.get("materialization")
                    if mat and mat.get("enabled"):
                        tables[node_id] = mat["table"]
                return tables

            yaml_tables = get_mat_tables(yaml_builder)
            py_tables = get_mat_tables(py_builder)

            assert set(yaml_tables.keys()) == set(py_tables.keys())
            # Same table names
            for node_id in yaml_tables:
                assert yaml_tables[node_id] == py_tables[node_id], \
                    f"Table mismatch for {node_id}: YAML={yaml_tables[node_id]} vs Python={py_tables[node_id]}"

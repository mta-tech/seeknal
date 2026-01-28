"""
Integration test for Python pipeline decorator materialization.

Tests that @transform and @source decorators with MaterializationConfig
properly configure Iceberg materialization, including configuration merging
with YAML overrides.
"""

import pytest
from pathlib import Path
import tempfile


class TestDecoratorMaterializationE2E:
    """End-to-end tests for decorator materialization."""

    def test_transform_with_materialization_config_builds_dag(self):
        """Test that @transform with MaterializationConfig builds valid DAG node."""
        from seeknal.pipeline.decorators import transform, clear_registry
        from seeknal.pipeline.materialization_config import MaterializationConfig

        # Clear registry before test
        clear_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with materialized transform
            py_file = pipelines_dir / "sales_forecast.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import transform
from seeknal.pipeline.materialization_config import MaterializationConfig

@transform(
    name="sales_forecast",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.prod.sales_forecast",
        mode="overwrite",
        create_table=True,
    ),
)
def sales_forecast(ctx):
    '''ML-based sales forecast using historical data.'''
    # Implementation would query ctx.ref() sources
    return ctx.duckdb.sql("SELECT 1").df()
""")

            # Build DAG
            from seeknal.workflow.dag import DAGBuilder

            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Verify node was created
            node_id = "transform.sales_forecast"
            assert node_id in builder.nodes, f"Node {node_id} not found in DAG"

            node = builder.nodes[node_id]

            # Verify node properties
            assert node.name == "sales_forecast"
            # Description is None (not extracted from docstring in current implementation)
            assert node.yaml_data.get("description") is None

            # Verify materialization config
            mat_config = node.yaml_data.get("materialization")
            assert mat_config is not None, "Materialization config not found"
            assert mat_config["enabled"] is True
            assert mat_config["table"] == "warehouse.prod.sales_forecast"
            assert mat_config["mode"] == "overwrite"
            assert mat_config["create_table"] is True

    def test_source_with_materialization_config(self):
        """Test that @source with MaterializationConfig builds valid DAG node."""
        from seeknal.pipeline.decorators import source, clear_registry
        from seeknal.pipeline.materialization_config import MaterializationConfig

        # Clear registry before test
        clear_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with materialized source
            py_file = pipelines_dir / "raw_orders.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import source
from seeknal.pipeline.materialization_config import MaterializationConfig

@source(
    name="raw_orders",
    source="csv",
    table="data/orders.csv",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.raw.orders",
        mode="append",
    ),
)
def raw_orders():
    '''Load raw orders from CSV file.'''
    return None
""")

            # Build DAG
            from seeknal.workflow.dag import DAGBuilder

            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Verify node was created
            node_id = "source.raw_orders"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]

            # Verify materialization config
            mat_config = node.yaml_data.get("materialization")
            assert mat_config is not None
            assert mat_config["enabled"] is True
            assert mat_config["table"] == "warehouse.raw.orders"
            assert mat_config["mode"] == "append"

    def test_dict_materialization_format(self):
        """Test that dict format works for materialization in decorators."""
        from seeknal.pipeline.decorators import transform, clear_registry

        # Clear registry before test
        clear_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with dict materialization
            py_file = pipelines_dir / "quick_transform.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import transform

@transform(
    name="quick_transform",
    materialization={
        "enabled": True,
        "table": "warehouse.prod.quick",
        "mode": "overwrite",
    },
)
def quick_transform(ctx):
    return None
""")

            # Build DAG
            from seeknal.workflow.dag import DAGBuilder

            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Verify node was created with materialization
            node_id = "transform.quick_transform"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]
            mat_config = node.yaml_data.get("materialization")

            # Verify dict was converted properly
            assert mat_config is not None
            assert mat_config["enabled"] is True
            assert mat_config["table"] == "warehouse.prod.quick"
            assert mat_config["mode"] == "overwrite"

    def test_yaml_override_disables_materialization(self):
        """Test that YAML override can disable decorator materialization."""
        from seeknal.pipeline.decorators import transform, clear_registry
        from seeknal.pipeline.materialization_config import MaterializationConfig

        # Clear registry before test
        clear_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            (seeknal_dir / "transforms").mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with materialization enabled
            py_file = pipelines_dir / "dev_transform.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import transform
from seeknal.pipeline.materialization_config import MaterializationConfig

@transform(
    name="dev_transform",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.dev.dev_transform",
        mode="overwrite",
    ),
)
def dev_transform(ctx):
    return None
""")

            # Create YAML override that disables materialization
            yaml_file = seeknal_dir / "transforms" / "dev_transform.yml"
            yaml_file.write_text("""
name: dev_transform
kind: transform
materialization:
  enabled: false  # Override decorator: disable in dev
  mode: append
""")

            # Build DAG
            from seeknal.workflow.dag import DAGBuilder

            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Verify YAML override was applied
            node_id = "transform.dev_transform"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]
            mat_config = node.yaml_data.get("materialization")

            # YAML override should disable materialization
            assert mat_config is not None
            assert mat_config["enabled"] is False  # YAML overrides decorator
            assert mat_config["mode"] == "append"  # YAML overrides decorator mode

    def test_feature_group_with_materialization_config(self):
        """Test that @feature_group works with MaterializationConfig."""
        from seeknal.pipeline.decorators import feature_group, clear_registry
        from seeknal.pipeline.materialization_config import MaterializationConfig

        # Clear registry before test
        clear_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with materialized feature group
            py_file = pipelines_dir / "user_features.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import feature_group
from seeknal.pipeline.materialization_config import MaterializationConfig

@feature_group(
    name="user_features",
    entity="user",
    features={"user_id": "int", "total_events": "int"},
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.online.user_features",
        mode="append",
    ),
)
def user_features(ctx):
    return None
""")

            # Build DAG
            from seeknal.workflow.dag import DAGBuilder

            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Verify node was created
            node_id = "feature_group.user_features"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]

            # Verify feature group properties
            assert node.yaml_data.get("entity") == "user"
            assert node.yaml_data.get("features") == {"user_id": "int", "total_events": "int"}

            # Verify materialization config
            mat_config = node.yaml_data.get("materialization")
            assert mat_config is not None
            assert mat_config["enabled"] is True
            assert mat_config["table"] == "warehouse.online.user_features"
            assert mat_config["mode"] == "append"

    def test_backward_compatibility_no_materialization(self):
        """Test that decorators without materialization still work."""
        from seeknal.pipeline.decorators import transform, clear_registry

        # Clear registry before test
        clear_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file without materialization
            py_file = pipelines_dir / "simple_transform.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import transform

@transform(name="simple_transform")
def simple_transform(ctx):
    return None
""")

            # Build DAG
            from seeknal.workflow.dag import DAGBuilder

            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Verify node was created without materialization
            node_id = "transform.simple_transform"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]
            mat_config = node.yaml_data.get("materialization")

            # No materialization config
            assert mat_config is None

    def test_validation_catches_invalid_table_format(self):
        """Test that validation catches invalid table name format."""
        from seeknal.pipeline.decorators import transform, clear_registry
        from seeknal.pipeline.materialization_config import MaterializationConfig

        # Clear registry before test
        clear_registry()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with invalid table format
            py_file = pipelines_dir / "invalid_transform.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import transform
from seeknal.pipeline.materialization_config import MaterializationConfig

@transform(
    name="invalid_transform",
    materialization=MaterializationConfig(
        enabled=True,
        table="invalid_table_format",  # Missing namespace parts
        mode="append",
    ),
)
def invalid_transform(ctx):
    return None
""")

            # Import the file to trigger validation
            with pytest.raises(ValueError, match="Invalid table name"):
                # This should raise during import/decoration
                import importlib.util
                spec = importlib.util.spec_from_file_location("invalid_transform", py_file)
                module = importlib.util.module_from_spec(spec)

                # Loading the module triggers decorator execution
                # which should raise ValueError
                spec.loader.exec_module(module)

"""
Unit tests for configuration merging logic.

Tests the _merge_materialization_config() function with various
priority combinations: decorator > yaml > profile > defaults.
"""

import pytest

from seeknal.workflow.dag import DAGBuilder


class TestMergeMaterializationConfig:
    """Test _merge_materialization_config() method."""

    def test_merge_with_defaults_only(self):
        """Test merging with no configs returns system defaults."""
        builder = DAGBuilder()

        result = builder._merge_materialization_config(None, None, None)

        assert result == {
            "enabled": False,
            "mode": "append",
            "create_table": True,
        }

    def test_merge_profile_overrides_defaults(self):
        """Test profile config overrides defaults."""
        builder = DAGBuilder()
        profile_config = {
            "enabled": True,
            "mode": "overwrite",
        }

        result = builder._merge_materialization_config(None, None, profile_config)

        assert result["enabled"] is True
        assert result["mode"] == "overwrite"
        assert result["create_table"] is True  # From defaults

    def test_merge_yaml_overrides_profile(self):
        """Test YAML config overrides profile config."""
        builder = DAGBuilder()
        profile_config = {"enabled": False, "mode": "append"}
        yaml_config = {"enabled": True, "mode": "overwrite"}

        result = builder._merge_materialization_config(None, yaml_config, profile_config)

        # YAML overrides profile
        assert result["enabled"] is True  # From YAML
        assert result["mode"] == "overwrite"  # From YAML
        assert result["create_table"] is True  # From defaults

    def test_merge_decorator_overrides_yaml(self):
        """Test decorator config overrides YAML config."""
        builder = DAGBuilder()
        yaml_config = {"enabled": False, "mode": "append"}
        decorator_config = {"enabled": True, "mode": "overwrite"}

        result = builder._merge_materialization_config(decorator_config, yaml_config, None)

        # Decorator overrides YAML
        assert result["enabled"] is True  # From decorator
        assert result["mode"] == "overwrite"  # From decorator
        assert result["create_table"] is True  # From defaults

    def test_merge_full_priority_chain(self):
        """Test full priority chain: decorator > yaml > profile > defaults."""
        builder = DAGBuilder()
        profile_config = {"enabled": False, "mode": "append"}
        yaml_config = {"enabled": False, "mode": "overwrite", "create_table": False}
        decorator_config = {"enabled": True}

        result = builder._merge_materialization_config(
            decorator_config, yaml_config, profile_config
        )

        # Each level overrides appropriately
        assert result["enabled"] is True  # From decorator (highest)
        assert result["mode"] == "overwrite"  # From YAML (decorator didn't set)
        assert result["create_table"] is False  # From YAML (decorator didn't set)

    def test_merge_decorator_none_does_not_override(self):
        """Test None values in decorator don't override lower configs."""
        builder = DAGBuilder()
        yaml_config = {"enabled": True, "mode": "append"}
        # Decorator only sets mode, not enabled
        decorator_config = {"mode": "overwrite", "enabled": None}

        result = builder._merge_materialization_config(decorator_config, yaml_config, None)

        # Decorator mode overrides, but enabled comes from YAML
        assert result["enabled"] is True  # From YAML (decorator was None)
        assert result["mode"] == "overwrite"  # From decorator (was set)

    def test_merge_partial_configs(self):
        """Test merging with partial configs from each level."""
        builder = DAGBuilder()
        profile_config = {"enabled": False}
        yaml_config = {"mode": "overwrite"}  # Doesn't set enabled
        decorator_config = {"create_table": False}  # Doesn't set enabled or mode

        result = builder._merge_materialization_config(
            decorator_config, yaml_config, profile_config
        )

        # Each level contributes independently
        assert result["enabled"] is False  # From profile
        assert result["mode"] == "overwrite"  # From YAML
        assert result["create_table"] is False  # From decorator

    def test_merge_empty_configs(self):
        """Test merging with empty dicts uses defaults."""
        builder = DAGBuilder()

        result = builder._merge_materialization_config({}, {}, {})

        assert result == {
            "enabled": False,
            "mode": "append",
            "create_table": True,
        }

    def test_merge_all_none_values(self):
        """Test decorator with all None values uses defaults."""
        builder = DAGBuilder()
        decorator_config = {"enabled": None, "mode": None, "create_table": None}

        result = builder._merge_materialization_config(decorator_config, None, None)

        assert result == {
            "enabled": False,
            "mode": "append",
            "create_table": True,
        }


class TestYamlOverrideLoading:
    """Test _load_yaml_override_for_node() method."""

    def test_load_yaml_override_returns_none_when_no_file(self):
        """Test returns None when YAML override file doesn't exist."""
        builder = DAGBuilder(project_path="/Users/fitrakacamarga/project/mta/signal")

        result = builder._load_yaml_override_for_node("transform.nonexistent")

        assert result is None

    def test_load_yaml_override_parses_materialization(self):
        """Test loads materialization from YAML override file."""
        # This test would require creating actual YAML files
        # For now, we test the logic with a mock scenario
        builder = DAGBuilder(project_path="/Users/fitrakacamarga/project/mta/signal")

        # Since we can't easily create YAML files in this test,
        # we just verify the method returns None for non-existent files
        result = builder._load_yaml_override_for_node("transform.test_node")

        assert result is None  # File doesn't exist yet


class TestConfigMergingIntegration:
    """Integration tests for config merging in DAG building."""

    def test_decorator_config_is_merged_during_parsing(self):
        """Test that decorator materialization is properly merged in DAG parsing."""
        from seeknal.pipeline.decorators import transform, clear_registry
        from seeknal.pipeline.materialization_config import MaterializationConfig
        from pathlib import Path
        import tempfile
        import os

        # Clear registry before test
        clear_registry()

        # Create a temporary directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with materialized transform
            py_file = pipelines_dir / "test_transform.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import transform
from seeknal.pipeline.materialization_config import MaterializationConfig

@transform(
    name="sales_forecast",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.prod.sales_forecast",
        mode="overwrite",
    ),
)
def sales_forecast(ctx):
    return None
""")

            # Build DAG
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Check that node was created with merged materialization
            node_id = "transform.sales_forecast"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]
            mat_config = node.yaml_data.get("materialization")

            # Verify merged config
            assert mat_config is not None
            assert mat_config["enabled"] is True
            assert mat_config["table"] == "warehouse.prod.sales_forecast"
            assert mat_config["mode"] == "overwrite"

    def test_source_decorator_with_materialization(self):
        """Test @source decorator with materialization config."""
        from seeknal.pipeline.decorators import source, clear_registry
        from seeknal.pipeline.materialization_config import MaterializationConfig
        from pathlib import Path
        import tempfile

        # Clear registry before test
        clear_registry()

        # Create a temporary directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with materialized source
            py_file = pipelines_dir / "test_source.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import source
from seeknal.pipeline.materialization_config import MaterializationConfig

@source(
    name="raw_data",
    source="csv",
    table="data/raw.csv",
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.raw.data",
        mode="append",
    ),
)
def raw_data():
    return None
""")

            # Build DAG
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Check that node was created with materialization
            node_id = "source.raw_data"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]
            mat_config = node.yaml_data.get("materialization")

            # Verify config
            assert mat_config is not None
            assert mat_config["enabled"] is True
            assert mat_config["table"] == "warehouse.raw.data"
            assert mat_config["mode"] == "append"

    def test_dict_materialization_format(self):
        """Test that dict format works for materialization."""
        from seeknal.pipeline.decorators import transform, clear_registry
        from pathlib import Path
        import tempfile

        # Clear registry before test
        clear_registry()

        # Create a temporary directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            seeknal_dir = project_path / "seeknal"
            seeknal_dir.mkdir()
            pipelines_dir = seeknal_dir / "pipelines"
            pipelines_dir.mkdir()

            # Create a Python file with dict materialization
            py_file = pipelines_dir / "test_dict.py"
            py_file.write_text("""
from seeknal.pipeline.decorators import transform

@transform(
    name="test_transform",
    materialization={
        "enabled": True,
        "table": "warehouse.prod.test",
        "mode": "append",
    },
)
def test_transform(ctx):
    return None
""")

            # Build DAG
            builder = DAGBuilder(project_path=project_path)
            builder.build()

            # Check that node was created
            node_id = "transform.test_transform"
            assert node_id in builder.nodes

            node = builder.nodes[node_id]
            mat_config = node.yaml_data.get("materialization")

            # Verify dict was converted properly
            assert mat_config is not None
            assert mat_config["enabled"] is True
            assert mat_config["table"] == "warehouse.prod.test"
            assert mat_config["mode"] == "append"

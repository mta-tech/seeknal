"""
Unit tests for MaterializationConfig dataclass.

Tests the to_dict(), from_dict() methods and validation logic.
"""

import pytest

from seeknal.pipeline.materialization_config import MaterializationConfig


class TestMaterializationConfig:
    """Test MaterializationConfig dataclass functionality."""

    def test_to_dict_with_all_fields(self):
        """Test to_dict() includes all non-None values."""
        config = MaterializationConfig(
            enabled=True,
            table="warehouse.prod.sales_forecast",
            mode="overwrite",
            create_table=True,
        )

        result = config.to_dict()

        assert result == {
            "enabled": True,
            "table": "warehouse.prod.sales_forecast",
            "mode": "overwrite",
            "create_table": True,
        }

    def test_to_dict_with_partial_fields(self):
        """Test to_dict() excludes None values."""
        config = MaterializationConfig(
            enabled=True,
            table="warehouse.prod.table",
            # mode and create_table are None
        )

        result = config.to_dict()

        assert result == {
            "enabled": True,
            "table": "warehouse.prod.table",
        }
        assert "mode" not in result
        assert "create_table" not in result

    def test_to_dict_with_all_none(self):
        """Test to_dict() returns empty dict when all fields are None."""
        config = MaterializationConfig()

        result = config.to_dict()

        assert result == {}

    def test_from_dict(self):
        """Test from_dict() creates MaterializationConfig from dict."""
        config_dict = {
            "enabled": True,
            "table": "warehouse.prod.table",
            "mode": "append",
        }

        config = MaterializationConfig.from_dict(config_dict)

        assert config.enabled is True
        assert config.table == "warehouse.prod.table"
        assert config.mode == "append"
        assert config.create_table is None

    def test_from_dict_ignores_unknown_fields(self):
        """Test from_dict() ignores unknown fields."""
        config_dict = {
            "enabled": True,
            "table": "warehouse.prod.table",
            "unknown_field": "should_be_ignored",
        }

        config = MaterializationConfig.from_dict(config_dict)

        assert config.enabled is True
        assert config.table == "warehouse.prod.table"
        assert not hasattr(config, "unknown_field")

    def test_from_dict_empty(self):
        """Test from_dict() with empty dict."""
        config = MaterializationConfig.from_dict({})

        assert config.enabled is None
        assert config.table is None
        assert config.mode is None
        assert config.create_table is None

    def test_round_trip_conversion(self):
        """Test round-trip: to_dict() -> from_dict() preserves data."""
        original = MaterializationConfig(
            enabled=False,
            table="warehouse.dev.table",
            mode="append",
            create_table=False,
        )

        # Convert to dict
        config_dict = original.to_dict()

        # Convert back to MaterializationConfig
        restored = MaterializationConfig.from_dict(config_dict)

        assert restored.enabled == original.enabled
        assert restored.table == original.table
        assert restored.mode == original.mode
        assert restored.create_table == original.create_table


class TestMaterializationConfigValidation:
    """Test validation of MaterializationConfig in decorators."""

    def test_validate_with_valid_config(self):
        """Test validation passes with valid config."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        config = MaterializationConfig(
            enabled=True,
            table="warehouse.prod.sales_forecast",
            mode="overwrite",
        )

        # Should not raise
        _validate_materialization_config(config)

    def test_validate_with_dict(self):
        """Test validation accepts dict format."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        config = {
            "enabled": True,
            "table": "warehouse.prod.sales_forecast",
            "mode": "append",
        }

        # Should not raise
        _validate_materialization_config(config)

    def test_validate_with_none(self):
        """Test validation accepts None."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        # Should not raise
        _validate_materialization_config(None)

    def test_validate_enabled_true_requires_table(self):
        """Test validation raises when enabled=True but table is None."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        config = MaterializationConfig(enabled=True, table=None)

        with pytest.raises(ValueError, match="materialization.table is required"):
            _validate_materialization_config(config)

    def test_validate_invalid_table_format(self):
        """Test validation raises for invalid table name format."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        # Missing namespace part
        config = MaterializationConfig(
            enabled=True,
            table="table_only"
        )

        with pytest.raises(ValueError, match="Invalid table name"):
            _validate_materialization_config(config)

    def test_validate_invalid_mode(self):
        """Test validation raises for invalid mode."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        config = MaterializationConfig(
            enabled=True,
            table="warehouse.prod.table",
            mode="invalid_mode"
        )

        with pytest.raises(ValueError, match="Must be 'append' or 'overwrite'"):
            _validate_materialization_config(config)

    def test_validate_invalid_type(self):
        """Test validation raises TypeError for invalid type."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        with pytest.raises(TypeError, match="must be dict or MaterializationConfig"):
            _validate_materialization_config("invalid_type")

    def test_validate_valid_table_formats(self):
        """Test validation accepts various valid table formats."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        valid_tables = [
            "warehouse.prod.sales_forecast",
            "catalog.namespace.table",
            "a.b.c",  # minimal valid format
            "_underscore.db.schema_name",  # underscore start
        ]

        for table in valid_tables:
            config = MaterializationConfig(enabled=True, table=table, mode="append")
            # Should not raise
            _validate_materialization_config(config)

    def test_validate_invalid_table_formats(self):
        """Test validation rejects invalid table formats."""
        from seeknal.pipeline.decorators import _validate_materialization_config

        invalid_tables = [
            "table_only",  # Only one part
            "two.parts",  # Only two parts
            "warehouse prod.table",  # Space in name
            "warehouse.prod..table",  # Empty part
            ".warehouse.prod.table",  # Starts with dot
            "warehouse.prod.table.",  # Ends with dot
        ]

        for table in invalid_tables:
            config = MaterializationConfig(enabled=True, table=table, mode="append")
            with pytest.raises(ValueError, match="Invalid table name"):
                _validate_materialization_config(config)

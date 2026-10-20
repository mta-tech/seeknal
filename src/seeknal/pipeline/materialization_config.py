"""
Materialization configuration for Python pipeline decorators.

Provides MaterializationConfig dataclass for inline Iceberg materialization
configuration in @transform, @source, and @feature_group decorators.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class MaterializationConfig:
    """Iceberg materialization configuration for pipeline nodes.

    Recommended approach for type-safe decorator configuration.
    Also accepts dict for backwards compatibility.

    This configures Iceberg table materialization for pipeline nodes.
    Configuration priority: Decorator > YAML > Profile > Defaults
    None values are omitted so lower-priority defaults remain active.

    Args:
        enabled: Enable/disable materialization (None uses lower-priority config)
        table: Iceberg table name (catalog.namespace.table format)
        mode: Write mode - "append", "overwrite", "upsert", or
            "insert_overwrite"
        create_table: Auto-create table if it doesn't exist
        unique_keys: Key columns required by upsert
        partition_by: Identity partition columns required by insert_overwrite
        max_batch_bytes: Maximum logical Arrow batch size for advanced modes
        when_matched_update_if: SQL predicate required to accept matched rows
        compare_columns: Additional target columns exposed to the predicate
        when_not_matched: Insert new keys or skip them

    Example:
        from seeknal.pipeline.materialization_config import MaterializationConfig

        @transform(
            name="sales_forecast",
            materialization=MaterializationConfig(
                enabled=True,
                table="warehouse.prod.sales_forecast",
                mode="overwrite",
            )
        )
        def sales_forecast(ctx):
            return df

    Or with dict:
        @transform(
            name="sales_forecast",
            materialization={
                "enabled": True,
                "table": "warehouse.prod.sales_forecast",
                "mode": "overwrite",
            }
        )
        def sales_forecast(ctx):
            return df
    """

    enabled: Optional[bool] = None
    table: Optional[str] = None
    mode: Optional[str] = None
    create_table: Optional[bool] = None
    unique_keys: Optional[list[str]] = None
    partition_by: Optional[list[str]] = None
    max_batch_bytes: Optional[int] = None
    when_matched_update_if: Optional[str] = None
    compare_columns: Optional[list[str]] = None
    when_not_matched: Optional[str] = None

    # schema_evolution: Optional[str] = None  # "safe", "auto", "strict"

    def to_dict(self) -> dict:
        """Convert to dict, excluding None values.

        Returns:
            Dictionary with non-None values suitable for merging with YAML config
        """
        return {
            k: v for k, v in [
                ("enabled", self.enabled),
                ("table", self.table),
                ("mode", self.mode),
                ("create_table", self.create_table),
                ("unique_keys", self.unique_keys),
                ("partition_by", self.partition_by),
                ("max_batch_bytes", self.max_batch_bytes),
                ("when_matched_update_if", self.when_matched_update_if),
                ("compare_columns", self.compare_columns),
                ("when_not_matched", self.when_not_matched),
            ] if v is not None
        }

    @classmethod
    def from_dict(cls, config: dict) -> "MaterializationConfig":
        """Create from dict. Useful for loading from YAML.

        Args:
            config: Dictionary with materialization configuration

        Returns:
            MaterializationConfig instance
        """
        return cls(**{
            k: v for k, v in config.items()
            if k in cls.__dataclass_fields__
        })

"""
Materialization dataclasses for Python pipeline API.

Provides type-safe configuration for offline/online stores and Iceberg tables.
Used in @feature_group decorator to specify materialization behavior.
"""

from dataclasses import dataclass, field
from typing import Optional, Literal


@dataclass
class OfflineConfig:
    """Configuration for offline materialization (batch storage).

    Attributes:
        format: Storage format (parquet, delta, or iceberg)
        partition_by: Optional list of columns to partition by
    """
    format: Literal["parquet", "delta", "iceberg"] = "parquet"
    partition_by: Optional[list[str]] = None


@dataclass
class OnlineConfig:
    """Configuration for online materialization (low-latency serving).

    Attributes:
        enabled: Whether online serving is enabled
        ttl_seconds: Optional time-to-live for cached features
    """
    enabled: bool = True
    ttl_seconds: Optional[int] = None


@dataclass
class IcebergTable:
    """Iceberg table configuration for feature groups.

    Attributes:
        namespace: Table namespace (e.g., "curated", "raw")
        name: Table name
        catalog: Iceberg catalog name (default: "atlas")
    """
    namespace: str
    name: str
    catalog: str = "atlas"


@dataclass
class Materialization:
    """Materialization configuration for feature groups.

    Combines offline and online store configuration with optional
    Iceberg table specification.

    Attributes:
        offline: Offline store configuration
        online: Online store configuration
        table: Optional Iceberg table configuration

    Example:
        mat = Materialization(
            offline=OfflineConfig(format="parquet", partition_by=["date"]),
            online=OnlineConfig(enabled=True, ttl_seconds=3600),
            table=IcebergTable(namespace="curated", name="user_features"),
        )
    """
    offline: Optional[OfflineConfig] = None
    online: Optional[OnlineConfig] = None
    table: Optional[IcebergTable] = None

    def to_dict(self) -> dict:
        """Convert to YAML-compatible dict for manifest.

        Returns:
            Dictionary representation suitable for YAML serialization
        """
        result = {}
        if self.offline:
            result["offline"] = {"format": self.offline.format}
            if self.offline.partition_by:
                result["offline"]["partition_by"] = self.offline.partition_by
        if self.online:
            result["online"] = {"enabled": self.online.enabled}
            if self.online.ttl_seconds:
                result["online"]["ttl_seconds"] = self.online.ttl_seconds
        if self.table:
            result["table"] = {
                "namespace": self.table.namespace,
                "name": self.table.name,
                "catalog": self.table.catalog,
            }
        return result

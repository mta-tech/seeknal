"""
Entity catalog for tracking consolidated feature store metadata.

The EntityCatalog records which feature groups contribute to each entity's
consolidated parquet, their schemas, and consolidation timestamps.
Persisted as _entity_catalog.json alongside features.parquet.
"""

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class FGCatalogEntry:
    """Metadata for a single feature group within an entity catalog.

    Attributes:
        name: Feature group name
        features: List of feature column names (excludes join_keys and event_time)
        event_time_col: Name of the event time column in the source FG
        last_updated: ISO timestamp of last successful FG execution
        row_count: Number of rows in the FG parquet
        schema: Column name -> DuckDB type string mapping
    """
    name: str
    features: List[str] = field(default_factory=list)
    event_time_col: str = "event_time"
    last_updated: Optional[str] = None
    row_count: int = 0
    schema: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FGCatalogEntry":
        return cls(
            name=data["name"],
            features=data.get("features", []),
            event_time_col=data.get("event_time_col", "event_time"),
            last_updated=data.get("last_updated"),
            row_count=data.get("row_count", 0),
            schema=data.get("schema", {}),
        )


@dataclass
class EntityCatalog:
    """Catalog for a consolidated entity in the feature store.

    Tracks all feature groups contributing to this entity, their schemas,
    and consolidation metadata. Persisted as _entity_catalog.json.

    Attributes:
        entity_name: Name of the entity (e.g. "customer")
        join_keys: List of join key column names shared by all FGs
        feature_groups: Map of FG name -> FGCatalogEntry
        consolidated_at: ISO timestamp of last consolidation
        schema_version: Catalog schema version for migration support
    """
    entity_name: str
    join_keys: List[str] = field(default_factory=list)
    feature_groups: Dict[str, FGCatalogEntry] = field(default_factory=dict)
    consolidated_at: Optional[str] = None
    schema_version: str = "1.0"

    def validate_join_keys(self, fg_name: str, fg_join_keys: List[str]) -> None:
        """Validate that a feature group's join keys match the entity's.

        Args:
            fg_name: Feature group name (for error messages)
            fg_join_keys: Join keys from the feature group

        Raises:
            ValueError: If join keys don't match
        """
        if not self.join_keys:
            # First FG sets the join keys
            self.join_keys = list(fg_join_keys)
            return

        if sorted(self.join_keys) != sorted(fg_join_keys):
            raise ValueError(
                f"Join key mismatch for entity '{self.entity_name}': "
                f"feature group '{fg_name}' has join_keys={fg_join_keys}, "
                f"but entity expects join_keys={self.join_keys}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_name": self.entity_name,
            "join_keys": self.join_keys,
            "feature_groups": {
                name: entry.to_dict()
                for name, entry in self.feature_groups.items()
            },
            "consolidated_at": self.consolidated_at,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EntityCatalog":
        fgs = {}
        for name, entry_data in data.get("feature_groups", {}).items():
            fgs[name] = FGCatalogEntry.from_dict(entry_data)
        return cls(
            entity_name=data["entity_name"],
            join_keys=data.get("join_keys", []),
            feature_groups=fgs,
            consolidated_at=data.get("consolidated_at"),
            schema_version=data.get("schema_version", "1.0"),
        )

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, json_str: str) -> "EntityCatalog":
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def load(cls, path: Path) -> Optional["EntityCatalog"]:
        """Load catalog from a JSON file.

        Args:
            path: Path to _entity_catalog.json

        Returns:
            EntityCatalog if file exists and is valid, None otherwise
        """
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return cls.from_json(f.read())
        except (json.JSONDecodeError, KeyError):
            return None

    def save(self, path: Path) -> None:
        """Save catalog to a JSON file with atomic write.

        Args:
            path: Path to write _entity_catalog.json
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(".tmp")
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(self.to_json())
            temp_path.replace(path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

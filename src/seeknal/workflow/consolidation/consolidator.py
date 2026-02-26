"""
Entity consolidation engine.

Merges per-feature-group parquet files into a single per-entity parquet
with DuckDB struct-namespaced columns for fast cross-FG retrieval.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from seeknal.workflow.consolidation.catalog import EntityCatalog, FGCatalogEntry

logger = logging.getLogger(__name__)


@dataclass
class FGMetadata:
    """Discovered feature group metadata from the DAG manifest.

    Attributes:
        name: Feature group name
        entity_name: Entity this FG belongs to
        join_keys: Entity join key columns
        event_time_col: Event time column name
        parquet_path: Path to the FG's intermediate parquet
        features: List of feature column names (auto-discovered from parquet)
    """
    name: str
    entity_name: str
    join_keys: List[str]
    event_time_col: str = "event_time"
    parquet_path: Optional[Path] = None
    features: List[str] = field(default_factory=list)


@dataclass
class ConsolidationResult:
    """Result of consolidating a single entity.

    Attributes:
        entity_name: Entity that was consolidated
        fg_count: Number of feature groups merged
        row_count: Number of rows in consolidated parquet
        output_path: Path to consolidated features.parquet
        catalog_path: Path to _entity_catalog.json
        duration_seconds: Time taken for consolidation
        error: Error message if consolidation failed
    """
    entity_name: str
    fg_count: int = 0
    row_count: int = 0
    output_path: Optional[Path] = None
    catalog_path: Optional[Path] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None


class EntityConsolidator:
    """Consolidates per-FG parquets into per-entity views with struct columns.

    The consolidator reads per-FG intermediate parquets, groups them by entity,
    performs LEFT JOINs on (join_keys, event_time), wraps each FG's feature
    columns in struct_pack(), and writes a consolidated parquet per entity.

    Args:
        target_path: Path to the target directory (contains intermediate/ and feature_store/)
    """

    def __init__(self, target_path: Path):
        self.target_path = target_path
        self.intermediate_path = target_path / "intermediate"
        self.feature_store_path = target_path / "feature_store"

    def discover_feature_groups(
        self,
        manifest_nodes: Dict[str, Any],
    ) -> Dict[str, List[FGMetadata]]:
        """Discover feature groups from the DAG manifest, grouped by entity.

        Scans the manifest for FEATURE_GROUP nodes, extracts entity info,
        and locates their intermediate parquet files.

        Args:
            manifest_nodes: Dict of node_id -> Node from the manifest

        Returns:
            Dict of entity_name -> list of FGMetadata
        """
        import duckdb

        entity_fgs: Dict[str, List[FGMetadata]] = {}

        for _, node in manifest_nodes.items():
            # Only process feature group nodes
            node_type = node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type)
            if node_type != "feature_group":
                continue

            config = node.config or {}
            entity_config = config.get("entity")
            if not entity_config:
                logger.debug("FG '%s' has no entity config, skipping", node.name)
                continue

            # Handle both dict and string entity config
            if isinstance(entity_config, str):
                entity_name = entity_config
                join_keys = [f"{entity_config}_id"]
            else:
                entity_name = entity_config.get("name", "")
                join_keys = entity_config.get("join_keys", [])
            if not entity_name or not join_keys:
                logger.debug("FG '%s' has incomplete entity config, skipping", node.name)
                continue

            mat_config = config.get("materialization", {})
            event_time_col = mat_config.get("event_time_col", "event_time")

            # Locate the intermediate parquet
            parquet_path = self.intermediate_path / f"feature_group_{node.name}.parquet"
            if not parquet_path.exists():
                logger.warning(
                    "FG '%s' intermediate parquet not found at %s, skipping",
                    node.name, parquet_path
                )
                continue

            # Discover feature columns from parquet schema
            features: List[str] = []
            try:
                con = duckdb.connect()
                try:
                    cols = con.execute(
                        f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{parquet_path}')"
                    ).fetchall()
                    exclude = set(join_keys) | {event_time_col, "event_time"}
                    features = [c[0] for c in cols if c[0] not in exclude]
                finally:
                    con.close()
            except Exception as e:
                logger.warning("Failed to read schema for FG '%s': %s", node.name, e)
                continue

            fg_meta = FGMetadata(
                name=node.name,
                entity_name=entity_name,
                join_keys=join_keys,
                event_time_col=event_time_col,
                parquet_path=parquet_path,
                features=features,
            )

            entity_fgs.setdefault(entity_name, []).append(fg_meta)

        # Sort FGs alphabetically within each entity for deterministic join order
        for entity_name in entity_fgs:
            entity_fgs[entity_name].sort(key=lambda fg: fg.name)

        return entity_fgs

    def consolidate_entity(
        self,
        entity_name: str,
        fg_list: List[FGMetadata],
    ) -> ConsolidationResult:
        """Consolidate all feature groups for a single entity.

        Performs LEFT JOINs across FGs on (join_keys, event_time),
        wraps each FG's columns in struct_pack(), and writes the
        consolidated parquet atomically.

        Args:
            entity_name: Entity name
            fg_list: List of FGMetadata for this entity

        Returns:
            ConsolidationResult with outcome
        """
        import duckdb

        start = time.time()

        if not fg_list:
            return ConsolidationResult(
                entity_name=entity_name,
                error="No feature groups to consolidate",
            )

        # Validate join keys consistency
        catalog = EntityCatalog(entity_name=entity_name)
        for fg in fg_list:
            try:
                catalog.validate_join_keys(fg.name, fg.join_keys)
            except ValueError as e:
                return ConsolidationResult(
                    entity_name=entity_name,
                    error=str(e),
                )

        join_keys = catalog.join_keys
        output_dir = self.feature_store_path / entity_name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "features.parquet"
        catalog_path = output_dir / "_entity_catalog.json"

        con = duckdb.connect()
        try:
            # Register each FG as a view
            for fg in fg_list:
                con.execute(
                    f"CREATE OR REPLACE VIEW fg_{fg.name} AS "
                    f"SELECT * FROM '{fg.parquet_path}'"
                )

            # Build the consolidation query:
            # Base table is the first FG, LEFT JOIN subsequent FGs
            base_fg = fg_list[0]
            base_alias = f"fg_{base_fg.name}"

            # Join key columns from base
            join_key_cols = ", ".join(f"{base_alias}.{k}" for k in join_keys)
            event_time_col_ref = f"{base_alias}.event_time"

            # Build struct_pack for each FG
            struct_cols = []
            for fg in fg_list:
                alias = f"fg_{fg.name}"
                if fg.features:
                    field_assignments = ", ".join(
                        f"{feat} := {alias}.{feat}" for feat in fg.features
                    )
                    struct_cols.append(
                        f"struct_pack({field_assignments}) AS {fg.name}"
                    )
                else:
                    # FG with no features (only join keys + event_time)
                    struct_cols.append(f"NULL AS {fg.name}")

            # Build JOIN clauses
            join_clauses = []
            for fg in fg_list[1:]:
                alias = f"fg_{fg.name}"
                conditions = " AND ".join(
                    f"{base_alias}.{k} = {alias}.{k}" for k in join_keys
                )
                conditions += f" AND {base_alias}.event_time = {alias}.event_time"
                join_clauses.append(
                    f"LEFT JOIN fg_{fg.name} AS {alias} ON {conditions}"
                )

            select_parts = [join_key_cols, event_time_col_ref + " AS event_time"]
            select_parts.extend(struct_cols)

            query = (
                f"SELECT {', '.join(select_parts)} "
                f"FROM fg_{base_fg.name} AS {base_alias} "
                + " ".join(join_clauses)
            )

            # Get row count
            count_result = con.execute(f"SELECT COUNT(*) FROM ({query}) t").fetchone()
            row_count = count_result[0] if count_result else 0

            # Atomic write: write to temp file, then rename
            temp_path = output_path.with_suffix(".parquet.tmp")
            con.execute(f"COPY ({query}) TO '{temp_path}' (FORMAT PARQUET)")
            temp_path.replace(output_path)

            # Update catalog
            for fg in fg_list:
                # Get per-FG row count
                fg_count_result = con.execute(
                    f"SELECT COUNT(*) FROM fg_{fg.name}"
                ).fetchone()
                fg_row_count = fg_count_result[0] if fg_count_result else 0

                # Get schema from DuckDB
                schema_rows = con.execute(
                    f"SELECT column_name, column_type FROM "
                    f"(DESCRIBE SELECT * FROM fg_{fg.name})"
                ).fetchall()
                schema = {row[0]: row[1] for row in schema_rows}

                catalog.feature_groups[fg.name] = FGCatalogEntry(
                    name=fg.name,
                    features=fg.features,
                    event_time_col=fg.event_time_col,
                    last_updated=datetime.now().isoformat(),
                    row_count=fg_row_count,
                    schema=schema,
                )

            catalog.consolidated_at = datetime.now().isoformat()
            catalog.save(catalog_path)

        except Exception as e:
            duration = time.time() - start
            # Clean up temp file if it exists
            temp_path = output_path.with_suffix(".parquet.tmp")
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            return ConsolidationResult(
                entity_name=entity_name,
                error=str(e),
                duration_seconds=duration,
            )
        finally:
            con.close()

        duration = time.time() - start
        logger.info(
            "Consolidated entity '%s': %d FGs, %d rows in %.2fs",
            entity_name, len(fg_list), row_count, duration,
        )

        return ConsolidationResult(
            entity_name=entity_name,
            fg_count=len(fg_list),
            row_count=row_count,
            output_path=output_path,
            catalog_path=catalog_path,
            duration_seconds=duration,
        )

    def consolidate_all(
        self,
        manifest_nodes: Dict[str, Any],
        changed_fgs: Optional[Set[str]] = None,
    ) -> List[ConsolidationResult]:
        """Consolidate all entities, optionally only for changed FGs.

        Args:
            manifest_nodes: Dict of node_id -> Node from the manifest
            changed_fgs: If provided, only consolidate entities containing
                these FG names. If None, consolidate all entities.

        Returns:
            List of ConsolidationResult, one per entity
        """
        entity_fgs = self.discover_feature_groups(manifest_nodes)

        if not entity_fgs:
            logger.info("No feature groups with entities found, skipping consolidation")
            return []

        results = []
        for entity_name, fg_list in sorted(entity_fgs.items()):
            # Filter to entities with changed FGs if specified
            if changed_fgs is not None:
                fg_names = {fg.name for fg in fg_list}
                if not fg_names & changed_fgs:
                    logger.debug(
                        "Entity '%s' has no changed FGs, skipping", entity_name
                    )
                    continue

            result = self.consolidate_entity(entity_name, fg_list)
            results.append(result)

        return results

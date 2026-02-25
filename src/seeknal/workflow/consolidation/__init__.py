"""
Entity-level feature consolidation for the offline store.

This module merges per-feature-group parquet files into consolidated
per-entity views with DuckDB struct-namespaced columns, enabling fast
cross-FG feature retrieval.
"""

from seeknal.workflow.consolidation.catalog import EntityCatalog, FGCatalogEntry
from seeknal.workflow.consolidation.consolidator import EntityConsolidator, ConsolidationResult

__all__ = [
    "EntityCatalog",
    "FGCatalogEntry",
    "EntityConsolidator",
    "ConsolidationResult",
]

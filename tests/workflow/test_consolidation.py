"""
Tests for entity-level feature consolidation (Phase 1).

Tests cover:
- EntityCatalog JSON round-trip
- FGCatalogEntry serialization
- EntityConsolidator.discover_feature_groups
- EntityConsolidator.consolidate_entity (single/multi FG)
- Join key validation
- Empty FG handling
- Schema evolution
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
import pytest

from seeknal.workflow.consolidation.catalog import EntityCatalog, FGCatalogEntry
from seeknal.workflow.consolidation.consolidator import (
    EntityConsolidator,
    FGMetadata,
)


# ---------------------------------------------------------------------------
# Helpers: Lightweight Node stub matching manifest.Node interface
# ---------------------------------------------------------------------------

class _NodeType(Enum):
    FEATURE_GROUP = "feature_group"
    SOURCE = "source"
    TRANSFORM = "transform"


@dataclass
class _StubNode:
    id: str
    name: str
    node_type: _NodeType
    config: Dict[str, Any] = field(default_factory=dict)
    file_path: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    columns: Dict[str, str] = field(default_factory=dict)


def _write_fg_parquet(
    target_path: Path,
    fg_name: str,
    df: pd.DataFrame,
) -> Path:
    """Write a DataFrame as a FG intermediate parquet."""
    intermediate = target_path / "intermediate"
    intermediate.mkdir(parents=True, exist_ok=True)
    path = intermediate / f"feature_group_{fg_name}.parquet"
    df.to_parquet(path, index=False)
    return path


def _make_fg_node(
    name: str,
    entity_name: str,
    join_keys: List[str],
    event_time_col: str = "event_time",
) -> _StubNode:
    """Create a stub FG node matching manifest structure."""
    return _StubNode(
        id=f"feature_group.{name}",
        name=name,
        node_type=_NodeType.FEATURE_GROUP,
        config={
            "entity": {
                "name": entity_name,
                "join_keys": join_keys,
            },
            "materialization": {
                "event_time_col": event_time_col,
                "offline": True,
            },
        },
    )


# ---------------------------------------------------------------------------
# EntityCatalog tests
# ---------------------------------------------------------------------------

class TestEntityCatalog:
    def test_json_round_trip(self, tmp_path):
        catalog = EntityCatalog(
            entity_name="customer",
            join_keys=["customer_id"],
            feature_groups={
                "customer_features": FGCatalogEntry(
                    name="customer_features",
                    features=["total_orders", "total_revenue"],
                    event_time_col="event_time",
                    last_updated="2026-02-26T00:00:00",
                    row_count=100,
                    schema={"total_orders": "BIGINT", "total_revenue": "DOUBLE"},
                ),
            },
            consolidated_at="2026-02-26T00:00:00",
        )

        # Write and read back
        path = tmp_path / "_entity_catalog.json"
        catalog.save(path)
        loaded = EntityCatalog.load(path)

        assert loaded is not None
        assert loaded.entity_name == "customer"
        assert loaded.join_keys == ["customer_id"]
        assert "customer_features" in loaded.feature_groups
        assert loaded.feature_groups["customer_features"].features == [
            "total_orders", "total_revenue"
        ]
        assert loaded.consolidated_at == "2026-02-26T00:00:00"

    def test_load_nonexistent_returns_none(self, tmp_path):
        assert EntityCatalog.load(tmp_path / "missing.json") is None

    def test_load_invalid_json_returns_none(self, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json")
        assert EntityCatalog.load(bad_file) is None

    def test_validate_join_keys_first_fg_sets_keys(self):
        catalog = EntityCatalog(entity_name="customer")
        catalog.validate_join_keys("fg1", ["customer_id"])
        assert catalog.join_keys == ["customer_id"]

    def test_validate_join_keys_matching(self):
        catalog = EntityCatalog(
            entity_name="customer", join_keys=["customer_id"]
        )
        catalog.validate_join_keys("fg2", ["customer_id"])  # Should not raise

    def test_validate_join_keys_mismatch_raises(self):
        catalog = EntityCatalog(
            entity_name="customer", join_keys=["customer_id"]
        )
        with pytest.raises(ValueError, match="Join key mismatch"):
            catalog.validate_join_keys("fg2", ["user_id"])

    def test_to_dict_from_dict_round_trip(self):
        catalog = EntityCatalog(
            entity_name="product",
            join_keys=["product_id"],
            feature_groups={
                "product_features": FGCatalogEntry(
                    name="product_features",
                    features=["price", "category"],
                    row_count=50,
                ),
            },
        )
        d = catalog.to_dict()
        restored = EntityCatalog.from_dict(d)
        assert restored.entity_name == "product"
        assert restored.feature_groups["product_features"].features == ["price", "category"]

    def test_atomic_save_cleans_temp_on_error(self, tmp_path, monkeypatch):
        catalog = EntityCatalog(entity_name="test")
        path = tmp_path / "_entity_catalog.json"

        # Force replace to fail (Path.replace takes self + target)
        def _bad_replace(self_path, target):
            raise OSError("disk full")

        monkeypatch.setattr(Path, "replace", _bad_replace)

        with pytest.raises(OSError):
            catalog.save(path)

        # Temp file should be cleaned up
        assert not path.with_suffix(".tmp").exists()


# ---------------------------------------------------------------------------
# FGCatalogEntry tests
# ---------------------------------------------------------------------------

class TestFGCatalogEntry:
    def test_to_dict_from_dict(self):
        entry = FGCatalogEntry(
            name="fg1",
            features=["a", "b"],
            event_time_col="event_time",
            last_updated="2026-01-01T00:00:00",
            row_count=42,
            schema={"a": "BIGINT", "b": "VARCHAR"},
        )
        d = entry.to_dict()
        restored = FGCatalogEntry.from_dict(d)
        assert restored.name == "fg1"
        assert restored.features == ["a", "b"]
        assert restored.row_count == 42
        assert restored.schema == {"a": "BIGINT", "b": "VARCHAR"}

    def test_defaults(self):
        entry = FGCatalogEntry(name="fg1")
        assert entry.features == []
        assert entry.event_time_col == "event_time"
        assert entry.row_count == 0


# ---------------------------------------------------------------------------
# EntityConsolidator tests
# ---------------------------------------------------------------------------

class TestEntityConsolidatorDiscover:
    def test_discover_groups_by_entity(self, tmp_path):
        """Two FGs for same entity are grouped together."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1", "C2"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "total_orders": [10, 20],
        })
        df_prod = pd.DataFrame({
            "customer_id": ["C1", "C2"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "avg_price": [9.99, 19.99],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)
        _write_fg_parquet(tmp_path, "product_features", df_prod)

        nodes = {
            "feature_group.customer_features": _make_fg_node(
                "customer_features", "customer", ["customer_id"]
            ),
            "feature_group.product_features": _make_fg_node(
                "product_features", "customer", ["customer_id"]
            ),
        }

        consolidator = EntityConsolidator(tmp_path)
        entity_fgs = consolidator.discover_feature_groups(nodes)

        assert "customer" in entity_fgs
        assert len(entity_fgs["customer"]) == 2
        names = [fg.name for fg in entity_fgs["customer"]]
        assert "customer_features" in names
        assert "product_features" in names

    def test_discover_skips_non_fg_nodes(self, tmp_path):
        """Source and transform nodes are ignored."""
        nodes = {
            "source.raw_data": _StubNode(
                id="source.raw_data",
                name="raw_data",
                node_type=_NodeType.SOURCE,
                config={},
            ),
        }
        consolidator = EntityConsolidator(tmp_path)
        entity_fgs = consolidator.discover_feature_groups(nodes)
        assert entity_fgs == {}

    def test_discover_skips_fg_without_entity(self, tmp_path):
        """FGs with no entity config are skipped."""
        df = pd.DataFrame({"x": [1]})
        _write_fg_parquet(tmp_path, "orphan_fg", df)

        nodes = {
            "feature_group.orphan_fg": _StubNode(
                id="feature_group.orphan_fg",
                name="orphan_fg",
                node_type=_NodeType.FEATURE_GROUP,
                config={},  # No entity
            ),
        }
        consolidator = EntityConsolidator(tmp_path)
        entity_fgs = consolidator.discover_feature_groups(nodes)
        assert entity_fgs == {}

    def test_discover_skips_missing_parquet(self, tmp_path):
        """FGs with no intermediate parquet are skipped with warning."""
        nodes = {
            "feature_group.missing_fg": _make_fg_node(
                "missing_fg", "customer", ["customer_id"]
            ),
        }
        consolidator = EntityConsolidator(tmp_path)
        entity_fgs = consolidator.discover_feature_groups(nodes)
        assert entity_fgs == {}

    def test_discover_multiple_entities(self, tmp_path):
        """FGs for different entities are separated."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "revenue": [100.0],
        })
        df_prod = pd.DataFrame({
            "product_id": ["P1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "price": [9.99],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)
        _write_fg_parquet(tmp_path, "product_features", df_prod)

        nodes = {
            "feature_group.customer_features": _make_fg_node(
                "customer_features", "customer", ["customer_id"]
            ),
            "feature_group.product_features": _make_fg_node(
                "product_features", "product", ["product_id"]
            ),
        }
        consolidator = EntityConsolidator(tmp_path)
        entity_fgs = consolidator.discover_feature_groups(nodes)
        assert "customer" in entity_fgs
        assert "product" in entity_fgs
        assert len(entity_fgs["customer"]) == 1
        assert len(entity_fgs["product"]) == 1


    def test_discover_string_entity_config(self, tmp_path):
        """String entity config (from Python decorators) is normalized to dict."""
        df = pd.DataFrame({
            "customer_id": ["C1", "C2"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "total_orders": [10, 20],
        })
        _write_fg_parquet(tmp_path, "customer_features", df)

        # Simulate Python decorator output: entity is a string, not a dict
        nodes = {
            "feature_group.customer_features": _StubNode(
                id="feature_group.customer_features",
                name="customer_features",
                node_type=_NodeType.FEATURE_GROUP,
                config={
                    "entity": "customer",  # string, not dict
                    "materialization": {"event_time_col": "event_time"},
                },
            ),
        }
        consolidator = EntityConsolidator(tmp_path)
        entity_fgs = consolidator.discover_feature_groups(nodes)

        assert "customer" in entity_fgs
        assert len(entity_fgs["customer"]) == 1
        fg = entity_fgs["customer"][0]
        assert fg.entity_name == "customer"
        assert fg.join_keys == ["customer_id"]
        assert "total_orders" in fg.features


class TestEntityConsolidatorConsolidate:
    def test_single_entity_two_fgs(self, tmp_path):
        """Two FGs for one entity produce a consolidated parquet with struct cols."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1", "C2", "C3"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "total_orders": [10, 20, 30],
            "total_revenue": [100.0, 200.0, 300.0],
        })
        df_prod = pd.DataFrame({
            "customer_id": ["C1", "C2", "C3"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "avg_price": [9.99, 19.99, 29.99],
            "num_products": [5, 10, 15],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)
        _write_fg_parquet(tmp_path, "product_features", df_prod)

        fg_list = [
            FGMetadata(
                name="customer_features",
                entity_name="customer",
                join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_customer_features.parquet",
                features=["total_orders", "total_revenue"],
            ),
            FGMetadata(
                name="product_features",
                entity_name="customer",
                join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_product_features.parquet",
                features=["avg_price", "num_products"],
            ),
        ]

        consolidator = EntityConsolidator(tmp_path)
        result = consolidator.consolidate_entity("customer", fg_list)

        assert result.success
        assert result.fg_count == 2
        assert result.row_count == 3
        assert result.output_path.exists()

        # Verify struct columns exist in the output
        con = duckdb.connect()
        try:
            df = con.execute(
                f"SELECT * FROM '{result.output_path}'"
            ).df()
            assert "customer_id" in df.columns
            assert "event_time" in df.columns
            assert "customer_features" in df.columns
            assert "product_features" in df.columns

            # Verify struct field access
            struct_df = con.execute(
                f"SELECT customer_features.total_orders, "
                f"product_features.avg_price "
                f"FROM '{result.output_path}' "
                f"ORDER BY customer_id"
            ).df()
            assert list(struct_df["total_orders"]) == [10, 20, 30]
            assert list(struct_df["avg_price"]) == [9.99, 19.99, 29.99]
        finally:
            con.close()

    def test_three_fgs_all_present(self, tmp_path):
        """Three FGs for one entity produce 3 struct columns."""
        base_data = {
            "customer_id": ["C1", "C2"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        }
        _write_fg_parquet(tmp_path, "fg_a", pd.DataFrame({
            **base_data, "feat_a": [1, 2],
        }))
        _write_fg_parquet(tmp_path, "fg_b", pd.DataFrame({
            **base_data, "feat_b": [3.0, 4.0],
        }))
        _write_fg_parquet(tmp_path, "fg_c", pd.DataFrame({
            **base_data, "feat_c": ["x", "y"],
        }))

        fg_list = [
            FGMetadata(
                name="fg_a", entity_name="customer", join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_fg_a.parquet",
                features=["feat_a"],
            ),
            FGMetadata(
                name="fg_b", entity_name="customer", join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_fg_b.parquet",
                features=["feat_b"],
            ),
            FGMetadata(
                name="fg_c", entity_name="customer", join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_fg_c.parquet",
                features=["feat_c"],
            ),
        ]

        consolidator = EntityConsolidator(tmp_path)
        result = consolidator.consolidate_entity("customer", fg_list)

        assert result.success
        assert result.fg_count == 3

        con = duckdb.connect()
        try:
            cols = con.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{result.output_path}')"
            ).fetchall()
            col_names = [c[0] for c in cols]
            assert "fg_a" in col_names
            assert "fg_b" in col_names
            assert "fg_c" in col_names
        finally:
            con.close()

    def test_mismatched_join_keys_error(self, tmp_path):
        """FGs with different join keys produce an error."""
        fg_list = [
            FGMetadata(
                name="fg1", entity_name="customer", join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_fg1.parquet",
                features=["a"],
            ),
            FGMetadata(
                name="fg2", entity_name="customer", join_keys=["user_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_fg2.parquet",
                features=["b"],
            ),
        ]
        consolidator = EntityConsolidator(tmp_path)
        result = consolidator.consolidate_entity("customer", fg_list)

        assert not result.success
        assert "Join key mismatch" in result.error

    def test_empty_fg_list_error(self, tmp_path):
        """Empty FG list produces an error."""
        consolidator = EntityConsolidator(tmp_path)
        result = consolidator.consolidate_entity("customer", [])
        assert not result.success

    def test_sparse_join_null_struct(self, tmp_path):
        """FG with rows not in base → NULL struct for missing rows."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1", "C2", "C3"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "revenue": [100.0, 200.0, 300.0],
        })
        # product_features only has C1 — C2/C3 should get NULL struct
        df_prod = pd.DataFrame({
            "customer_id": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "avg_price": [9.99],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)
        _write_fg_parquet(tmp_path, "product_features", df_prod)

        fg_list = [
            FGMetadata(
                name="customer_features", entity_name="customer",
                join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_customer_features.parquet",
                features=["revenue"],
            ),
            FGMetadata(
                name="product_features", entity_name="customer",
                join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_product_features.parquet",
                features=["avg_price"],
            ),
        ]

        consolidator = EntityConsolidator(tmp_path)
        result = consolidator.consolidate_entity("customer", fg_list)

        assert result.success
        assert result.row_count == 3

        # Verify NULL struct for non-matching rows
        con = duckdb.connect()
        try:
            null_df = con.execute(
                f"SELECT product_features.avg_price "
                f"FROM '{result.output_path}' "
                f"WHERE customer_id = 'C2'"
            ).df()
            assert null_df["avg_price"].isna().all()
        finally:
            con.close()

    def test_catalog_written_alongside_parquet(self, tmp_path):
        """Consolidation writes _entity_catalog.json alongside features.parquet."""
        df = pd.DataFrame({
            "customer_id": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "revenue": [100.0],
        })
        _write_fg_parquet(tmp_path, "fg1", df)

        fg_list = [
            FGMetadata(
                name="fg1", entity_name="customer", join_keys=["customer_id"],
                parquet_path=tmp_path / "intermediate" / "feature_group_fg1.parquet",
                features=["revenue"],
            ),
        ]

        consolidator = EntityConsolidator(tmp_path)
        result = consolidator.consolidate_entity("customer", fg_list)

        assert result.success
        assert result.catalog_path.exists()

        catalog = EntityCatalog.load(result.catalog_path)
        assert catalog is not None
        assert catalog.entity_name == "customer"
        assert catalog.join_keys == ["customer_id"]
        assert "fg1" in catalog.feature_groups
        assert catalog.feature_groups["fg1"].features == ["revenue"]
        assert catalog.consolidated_at is not None

    def test_first_run_creates_from_scratch(self, tmp_path):
        """First consolidation creates feature_store directory and files."""
        assert not (tmp_path / "feature_store").exists()

        df = pd.DataFrame({
            "cid": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "val": [42],
        })
        _write_fg_parquet(tmp_path, "fg1", df)

        fg_list = [
            FGMetadata(
                name="fg1", entity_name="test_entity", join_keys=["cid"],
                parquet_path=tmp_path / "intermediate" / "feature_group_fg1.parquet",
                features=["val"],
            ),
        ]

        consolidator = EntityConsolidator(tmp_path)
        result = consolidator.consolidate_entity("test_entity", fg_list)

        assert result.success
        assert (tmp_path / "feature_store" / "test_entity" / "features.parquet").exists()
        assert (tmp_path / "feature_store" / "test_entity" / "_entity_catalog.json").exists()


class TestEntityConsolidatorConsolidateAll:
    def test_consolidate_all_entities(self, tmp_path):
        """consolidate_all processes all entities."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "revenue": [100.0],
        })
        df_prod = pd.DataFrame({
            "product_id": ["P1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "price": [9.99],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)
        _write_fg_parquet(tmp_path, "product_features", df_prod)

        nodes = {
            "feature_group.customer_features": _make_fg_node(
                "customer_features", "customer", ["customer_id"]
            ),
            "feature_group.product_features": _make_fg_node(
                "product_features", "product", ["product_id"]
            ),
        }

        consolidator = EntityConsolidator(tmp_path)
        results = consolidator.consolidate_all(nodes)

        assert len(results) == 2
        entity_names = {r.entity_name for r in results}
        assert entity_names == {"customer", "product"}
        assert all(r.success for r in results)

    def test_consolidate_only_changed_fgs(self, tmp_path):
        """With changed_fgs filter, only affected entities are consolidated."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "revenue": [100.0],
        })
        df_prod = pd.DataFrame({
            "product_id": ["P1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "price": [9.99],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)
        _write_fg_parquet(tmp_path, "product_features", df_prod)

        nodes = {
            "feature_group.customer_features": _make_fg_node(
                "customer_features", "customer", ["customer_id"]
            ),
            "feature_group.product_features": _make_fg_node(
                "product_features", "product", ["product_id"]
            ),
        }

        consolidator = EntityConsolidator(tmp_path)
        # Only customer_features changed
        results = consolidator.consolidate_all(nodes, changed_fgs={"customer_features"})

        assert len(results) == 1
        assert results[0].entity_name == "customer"

    def test_consolidate_all_no_fg_nodes(self, tmp_path):
        """With no FG nodes, returns empty list."""
        nodes = {
            "source.raw": _StubNode(
                id="source.raw", name="raw", node_type=_NodeType.SOURCE,
            ),
        }
        consolidator = EntityConsolidator(tmp_path)
        results = consolidator.consolidate_all(nodes)
        assert results == []

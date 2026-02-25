"""
Tests for entity consolidation materialization (Phase 3).

Tests cover:
- PostgreSQL column name truncation + CRC32 hash
- Flatten SELECT generation
- HUGEINT detection
- ConsolidationMaterializer Iceberg write (mock)
- ConsolidationMaterializer PostgreSQL write (mock)
- Materialization failure → warning, doesn't block
"""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, call

import duckdb
import pytest

from seeknal.workflow.consolidation.materializer import (
    ConsolidationMaterializer,
    _truncate_pg_column_name,
    _build_flatten_select,
    _cast_hugeint_structs,
    PG_MAX_IDENTIFIER_LENGTH,
)


# ---------------------------------------------------------------------------
# Helper to create a consolidated parquet for tests
# ---------------------------------------------------------------------------

def _create_test_parquet(tmp_path: Path) -> Path:
    """Create a test consolidated parquet with struct columns."""
    store_dir = tmp_path / "feature_store" / "customer"
    store_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = store_dir / "features.parquet"

    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT
                    'C1' AS customer_id,
                    TIMESTAMP '2026-01-01' AS event_time,
                    struct_pack(revenue := 100.0, orders := 10) AS customer_features,
                    struct_pack(price := 9.99) AS product_features
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)
    finally:
        con.close()

    return parquet_path


# ---------------------------------------------------------------------------
# Column name truncation tests
# ---------------------------------------------------------------------------

class TestColumnNameTruncation:
    def test_short_name_unchanged(self):
        """Names within 63 chars are not modified."""
        result = _truncate_pg_column_name("fg1", "revenue")
        assert result == "fg1__revenue"
        assert len(result) <= PG_MAX_IDENTIFIER_LENGTH

    def test_long_name_truncated_with_hash(self):
        """Names exceeding 63 chars are truncated with CRC32 suffix."""
        long_fg = "very_long_feature_group_name_that_exceeds_limits"
        long_feat = "extremely_long_feature_column_name_that_also_exceeds"
        result = _truncate_pg_column_name(long_fg, long_feat)

        assert len(result) <= PG_MAX_IDENTIFIER_LENGTH
        assert "__" in result
        # Should end with 8-char hex hash (CRC32)
        parts = result.rsplit("_", 1)
        assert len(parts[-1]) == 8
        assert all(c in "0123456789abcdef" for c in parts[-1])

    def test_truncation_deterministic(self):
        """Same inputs always produce same truncated name."""
        name1 = _truncate_pg_column_name("long_fg_name_aaaa", "long_feat_name_bbbb_cccc_dddd_eeee_ffff_gggg")
        name2 = _truncate_pg_column_name("long_fg_name_aaaa", "long_feat_name_bbbb_cccc_dddd_eeee_ffff_gggg")
        assert name1 == name2

    def test_different_inputs_different_hashes(self):
        """Different inputs produce different truncated names."""
        name1 = _truncate_pg_column_name("a" * 40, "b" * 40)
        name2 = _truncate_pg_column_name("a" * 40, "c" * 40)
        assert name1 != name2


# ---------------------------------------------------------------------------
# Flatten SELECT tests
# ---------------------------------------------------------------------------

class TestFlattenSelect:
    def test_basic_flatten(self, tmp_path):
        """Generates correct SQL for flattening struct columns."""
        path = tmp_path / "test.parquet"
        sql = _build_flatten_select(
            path,
            join_keys=["customer_id"],
            fg_features={
                "customer_features": ["revenue", "orders"],
                "product_features": ["price"],
            },
        )

        assert "customer_id" in sql
        assert "event_time" in sql
        assert "customer_features.revenue AS customer_features__revenue" in sql
        assert "customer_features.orders AS customer_features__orders" in sql
        assert "product_features.price AS product_features__price" in sql

    def test_multiple_join_keys(self, tmp_path):
        """Multiple join keys are included."""
        path = tmp_path / "test.parquet"
        sql = _build_flatten_select(
            path,
            join_keys=["customer_id", "region_id"],
            fg_features={"fg1": ["feat_a"]},
        )
        assert "customer_id" in sql
        assert "region_id" in sql


# ---------------------------------------------------------------------------
# HUGEINT detection tests
# ---------------------------------------------------------------------------

class TestHugeintDetection:
    def test_no_hugeint_no_warnings(self, tmp_path):
        """Normal types produce no warnings."""
        parquet_path = _create_test_parquet(tmp_path)
        con = duckdb.connect()
        try:
            warnings = _cast_hugeint_structs(con, parquet_path)
            assert warnings == []
        finally:
            con.close()

    def test_hugeint_detected(self, tmp_path):
        """HUGEINT columns produce warnings."""
        parquet_path = tmp_path / "hugeint_test.parquet"
        # DuckDB converts HUGEINT to DOUBLE in parquet, so mock DESCRIBE
        con = MagicMock()
        con.execute.return_value.fetchall.return_value = [
            ("customer_id", "VARCHAR"),
            ("big_count", "HUGEINT"),
        ]
        warnings = _cast_hugeint_structs(con, parquet_path)
        assert len(warnings) == 1
        assert "HUGEINT" in warnings[0]


# ---------------------------------------------------------------------------
# ConsolidationMaterializer tests
# ---------------------------------------------------------------------------

class TestMaterializerIceberg:
    def test_iceberg_overwrite_success(self, tmp_path):
        """Iceberg materialization with overwrite mode succeeds."""
        parquet_path = _create_test_parquet(tmp_path)
        con = MagicMock()
        # Mock DESCRIBE to return no HUGEINT
        con.execute.return_value.fetchall.return_value = [
            ("customer_id", "VARCHAR"),
            ("event_time", "TIMESTAMP"),
            ("customer_features", "STRUCT(revenue DOUBLE, orders BIGINT)"),
        ]
        con.execute.return_value.fetchone.return_value = [1]

        materializer = ConsolidationMaterializer()
        result = materializer.materialize_iceberg(
            con=con,
            entity_name="customer",
            parquet_path=parquet_path,
            target_config={
                "table_pattern": "atlas.features.{entity}_entity",
                "mode": "overwrite",
            },
        )

        assert result["success"]
        assert result["table"] == "atlas.features.customer_entity"

    def test_iceberg_no_table_pattern_fails(self, tmp_path):
        """Missing table_pattern returns error."""
        parquet_path = _create_test_parquet(tmp_path)
        con = MagicMock()
        con.execute.return_value.fetchall.return_value = []

        materializer = ConsolidationMaterializer()
        result = materializer.materialize_iceberg(
            con=con,
            entity_name="customer",
            parquet_path=parquet_path,
            target_config={},
        )

        assert not result["success"]

    def test_iceberg_exception_returns_error(self, tmp_path):
        """DuckDB exceptions are caught and returned as error."""
        parquet_path = _create_test_parquet(tmp_path)
        con = MagicMock()
        con.execute.side_effect = Exception("Iceberg extension not loaded")

        materializer = ConsolidationMaterializer()
        result = materializer.materialize_iceberg(
            con=con,
            entity_name="customer",
            parquet_path=parquet_path,
            target_config={"table_pattern": "t.{entity}"},
        )

        assert not result["success"]
        assert "Iceberg extension" in result["error"]


class TestMaterializerPostgresql:
    def test_postgresql_full_mode(self, tmp_path):
        """PostgreSQL materialization with full mode (DROP+CREATE)."""
        parquet_path = _create_test_parquet(tmp_path)
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = [1]

        materializer = ConsolidationMaterializer()
        result = materializer.materialize_postgresql(
            con=con,
            entity_name="customer",
            parquet_path=parquet_path,
            join_keys=["customer_id"],
            fg_features={
                "customer_features": ["revenue", "orders"],
                "product_features": ["price"],
            },
            target_config={
                "table_pattern": "features.{entity}_entity",
                "connection": "local_pg",
                "mode": "full",
            },
        )

        assert result["success"]
        assert result["table"] == "local_pg.features.customer_entity"
        assert result["mode"] == "flattened"

    def test_postgresql_exception_returns_error(self, tmp_path):
        """PostgreSQL exceptions are caught and returned as error."""
        parquet_path = _create_test_parquet(tmp_path)
        con = MagicMock()
        con.execute.side_effect = Exception("Connection refused")

        materializer = ConsolidationMaterializer()
        result = materializer.materialize_postgresql(
            con=con,
            entity_name="customer",
            parquet_path=parquet_path,
            join_keys=["customer_id"],
            fg_features={"fg1": ["a"]},
            target_config={
                "table_pattern": "t.{entity}",
                "connection": "pg",
                "mode": "full",
            },
        )

        assert not result["success"]
        assert "Connection refused" in result["error"]

    def test_postgresql_no_table_pattern_fails(self, tmp_path):
        """Missing table_pattern returns error."""
        parquet_path = _create_test_parquet(tmp_path)
        con = MagicMock()

        materializer = ConsolidationMaterializer()
        result = materializer.materialize_postgresql(
            con=con,
            entity_name="customer",
            parquet_path=parquet_path,
            join_keys=["cid"],
            fg_features={"fg1": ["a"]},
            target_config={"connection": "pg"},
        )

        assert not result["success"]

"""Tests for entity consolidation CLI commands (Phase 4)."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
from typer.testing import CliRunner

from seeknal.cli.main import app

runner = CliRunner()


def _create_entity_store(tmpdir: str, entity_name: str = "customer") -> Path:
    """Create a minimal consolidated entity store for testing."""
    base = Path(tmpdir)
    entity_dir = base / "target" / "feature_store" / entity_name
    entity_dir.mkdir(parents=True, exist_ok=True)

    # Write catalog
    catalog = {
        "entity_name": entity_name,
        "join_keys": ["customer_id"],
        "feature_groups": {
            "customer_features": {
                "name": "customer_features",
                "features": ["revenue", "orders"],
                "event_time_col": "event_time",
                "last_updated": "2026-02-26T00:00:00",
                "row_count": 100,
                "schema": {"revenue": "DOUBLE", "orders": "BIGINT"},
            },
            "product_features": {
                "name": "product_features",
                "features": ["price"],
                "event_time_col": "event_time",
                "last_updated": "2026-02-26T00:00:00",
                "row_count": 50,
                "schema": {"price": "DOUBLE"},
            },
        },
        "consolidated_at": "2026-02-26T00:00:00",
        "schema_version": 1,
    }
    (entity_dir / "_entity_catalog.json").write_text(json.dumps(catalog))

    # Write a dummy parquet
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT 'C1' AS customer_id,
                       TIMESTAMP '2026-01-01' AS event_time,
                       struct_pack(revenue := 100.0, orders := 10) AS customer_features,
                       struct_pack(price := 9.99) AS product_features
            ) TO '{entity_dir / "features.parquet"}' (FORMAT PARQUET)
        """)
    finally:
        con.close()

    return base


# ---------------------------------------------------------------------------
# seeknal entity list
# ---------------------------------------------------------------------------


class TestEntityList:
    def test_entity_list_shows_entities(self):
        """entity list displays consolidated entities."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_entity_store(tmpdir)
            result = runner.invoke(app, [
                "entity", "list",
                "--path", tmpdir,
            ])
            assert result.exit_code == 0
            assert "customer" in result.output
            assert "2 FGs" in result.output
            assert "3 features" in result.output

    def test_entity_list_no_store(self):
        """entity list with no feature store shows info message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(app, [
                "entity", "list",
                "--path", tmpdir,
            ])
            assert result.exit_code == 0
            assert "No consolidated entities" in result.output

    def test_entity_list_multiple_entities(self):
        """entity list displays multiple entities."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_entity_store(tmpdir, "customer")
            _create_entity_store(tmpdir, "product")
            result = runner.invoke(app, [
                "entity", "list",
                "--path", tmpdir,
            ])
            assert result.exit_code == 0
            assert "customer" in result.output
            assert "product" in result.output


# ---------------------------------------------------------------------------
# seeknal entity show
# ---------------------------------------------------------------------------


class TestEntityShow:
    def test_entity_show_details(self):
        """entity show displays catalog details."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _create_entity_store(tmpdir)
            result = runner.invoke(app, [
                "entity", "show", "customer",
                "--path", tmpdir,
            ])
            assert result.exit_code == 0
            assert "Entity: customer" in result.output
            assert "customer_id" in result.output
            assert "customer_features" in result.output
            assert "product_features" in result.output
            assert "revenue" in result.output

    def test_entity_show_not_found(self):
        """entity show for nonexistent entity shows error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(app, [
                "entity", "show", "nonexistent",
                "--path", tmpdir,
            ])
            assert result.exit_code == 1
            assert "not found" in result.output


# ---------------------------------------------------------------------------
# seeknal consolidate
# ---------------------------------------------------------------------------


class TestConsolidateCommand:
    def test_consolidate_help(self):
        """consolidate command shows help."""
        result = runner.invoke(app, ["consolidate", "--help"])
        assert result.exit_code == 0
        assert "consolidate" in result.output.lower()
        assert "--prune" in result.output

    @patch("seeknal.workflow.dag.DAGBuilder")
    def test_consolidate_no_fg_nodes(self, mock_builder_class):
        """consolidate with no FG nodes shows info message."""
        mock_manifest = MagicMock()
        mock_manifest.nodes = {}
        mock_builder_class.return_value.build.return_value = mock_manifest

        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(app, [
                "consolidate",
                "--path", tmpdir,
            ])
            assert result.exit_code == 0
            assert "No feature groups" in result.output

    @patch("seeknal.workflow.consolidation.consolidator.EntityConsolidator")
    @patch("seeknal.workflow.dag.DAGBuilder")
    def test_consolidate_runs_successfully(self, mock_builder_class, mock_consolidator_class):
        """consolidate triggers consolidation for discovered entities."""
        mock_manifest = MagicMock()
        mock_builder_class.return_value.build.return_value = mock_manifest

        # Mock consolidator
        mock_consolidator = MagicMock()
        mock_consolidator_class.return_value = mock_consolidator
        mock_consolidator.discover_feature_groups.return_value = {
            "customer": [MagicMock()],
        }

        mock_result = MagicMock()
        mock_result.error = None
        mock_result.fg_count = 2
        mock_result.row_count = 100
        mock_result.duration_seconds = 0.5
        mock_consolidator.consolidate_entity.return_value = mock_result

        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(app, [
                "consolidate",
                "--path", tmpdir,
            ])
            assert result.exit_code == 0
            assert "1 entities to consolidate" in result.output
            assert "Consolidation complete" in result.output


# ---------------------------------------------------------------------------
# REPL consolidated entity registration
# ---------------------------------------------------------------------------


class TestREPLConsolidatedEntities:
    def test_repl_registers_entity_parquets(self):
        """REPL auto-registers consolidated entity parquets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = _create_entity_store(tmpdir)

            from seeknal.cli.repl import REPL

            repl = REPL.__new__(REPL)
            repl.project_path = base
            repl.conn = duckdb.connect()
            repl._registered_parquets = 0

            try:
                repl._register_consolidated_entities()
                assert repl._registered_parquets == 1

                # Verify the view is queryable
                rows = repl.conn.execute(
                    "SELECT * FROM entity_customer"
                ).fetchall()
                assert len(rows) == 1
            finally:
                repl.conn.close()

    def test_repl_no_feature_store(self):
        """REPL handles missing feature store gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from seeknal.cli.repl import REPL

            repl = REPL.__new__(REPL)
            repl.project_path = Path(tmpdir)
            repl.conn = duckdb.connect()
            repl._registered_parquets = 0

            try:
                repl._register_consolidated_entities()
                assert repl._registered_parquets == 0
            finally:
                repl.conn.close()

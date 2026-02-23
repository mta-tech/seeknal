"""Unit tests for the multi-target materialization dispatcher.

Tests cover:
- Single-target dispatch (PostgreSQL and Iceberg)
- Multi-target dispatch with mixed backends
- Best-effort semantics (first/second/all failures)
- DispatchResult counts and properties
- Unknown type handling
- Default type fallback
- Config validation errors
- Empty target list
- WriteResult preservation in results
- Logging for each target
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest  # ty: ignore[unresolved-import]

from seeknal.workflow.materialization.dispatcher import (  # ty: ignore[unresolved-import]
    DispatchResult,
    MaterializationDispatcher,
)
from seeknal.workflow.materialization.operations import WriteResult  # ty: ignore[unresolved-import]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_con():
    """A mock DuckDB connection."""
    return MagicMock()


@pytest.fixture
def dispatcher():
    """A dispatcher with a mocked profile loader."""
    return MaterializationDispatcher(profile_loader=MagicMock())


def _ok_result(row_count: int = 100) -> WriteResult:
    """Convenience helper: a successful WriteResult."""
    return WriteResult(success=True, row_count=row_count, duration_seconds=0.5)


# ---------------------------------------------------------------------------
# DispatchResult dataclass tests
# ---------------------------------------------------------------------------

class TestDispatchResult:
    """Tests for the DispatchResult dataclass."""

    def test_all_succeeded_true(self):
        """all_succeeded is True when total > 0 and failed == 0."""
        result = DispatchResult(total=2, succeeded=2, failed=0)
        assert result.all_succeeded is True

    def test_all_succeeded_false_on_failure(self):
        """all_succeeded is False when any target failed."""
        result = DispatchResult(total=2, succeeded=1, failed=1)
        assert result.all_succeeded is False

    def test_all_succeeded_false_on_empty(self):
        """all_succeeded is False when no targets were attempted."""
        result = DispatchResult(total=0, succeeded=0, failed=0)
        assert result.all_succeeded is False

    def test_defaults(self):
        """Default values are zero / empty."""
        result = DispatchResult()
        assert result.total == 0
        assert result.succeeded == 0
        assert result.failed == 0
        assert result.results == []


# ---------------------------------------------------------------------------
# Single-target dispatch
# ---------------------------------------------------------------------------

class TestSingleTargetDispatch:
    """Tests for dispatching a single target."""

    def test_dispatch_single_postgresql_target(self, dispatcher, mock_con):
        """Single PostgreSQL target routes to _materialize_postgresql."""
        wr = _ok_result(200)
        with patch.object(dispatcher, "_materialize_postgresql", return_value=wr):
            result = dispatcher.dispatch(
                mock_con,
                "my_view",
                [{"type": "postgresql", "connection": "pg1", "table": "public.orders"}],
            )
        assert result.total == 1
        assert result.succeeded == 1
        assert result.failed == 0
        assert result.results[0]["type"] == "postgresql"
        assert result.results[0]["success"] is True

    def test_dispatch_single_iceberg_target(self, dispatcher, mock_con):
        """Single Iceberg target routes to _materialize_iceberg."""
        wr = _ok_result(300)
        with patch.object(dispatcher, "_materialize_iceberg", return_value=wr):
            result = dispatcher.dispatch(
                mock_con,
                "my_view",
                [{"type": "iceberg", "table": "warehouse.ns.orders"}],
            )
        assert result.total == 1
        assert result.succeeded == 1
        assert result.failed == 0
        assert result.results[0]["type"] == "iceberg"

    def test_dispatch_default_type_is_iceberg(self, dispatcher, mock_con):
        """When type is omitted, defaults to iceberg."""
        wr = _ok_result()
        with patch.object(dispatcher, "_materialize_iceberg", return_value=wr) as mock_ice:
            result = dispatcher.dispatch(
                mock_con,
                "my_view",
                [{"table": "warehouse.ns.orders"}],  # no type key
            )
        mock_ice.assert_called_once()
        assert result.succeeded == 1
        assert result.results[0]["type"] == "iceberg"


# ---------------------------------------------------------------------------
# Multi-target dispatch
# ---------------------------------------------------------------------------

class TestMultiTargetDispatch:
    """Tests for dispatching to multiple targets in a single call."""

    def test_dispatch_multiple_targets(self, dispatcher, mock_con):
        """Both PostgreSQL and Iceberg targets succeed."""
        pg_wr = _ok_result(100)
        ice_wr = _ok_result(200)
        with (
            patch.object(dispatcher, "_materialize_postgresql", return_value=pg_wr),
            patch.object(dispatcher, "_materialize_iceberg", return_value=ice_wr),
        ):
            result = dispatcher.dispatch(
                mock_con,
                "my_view",
                [
                    {"type": "postgresql", "connection": "pg1", "table": "public.t"},
                    {"type": "iceberg", "table": "warehouse.ns.t"},
                ],
                node_id="transform.orders",
            )
        assert result.total == 2
        assert result.succeeded == 2
        assert result.failed == 0
        assert result.all_succeeded is True
        assert result.results[0]["type"] == "postgresql"
        assert result.results[1]["type"] == "iceberg"


# ---------------------------------------------------------------------------
# Best-effort failure handling
# ---------------------------------------------------------------------------

class TestBestEffort:
    """Tests for best-effort semantics: failures don't block other targets."""

    def test_best_effort_first_fails(self, dispatcher, mock_con):
        """First target fails, second still runs and succeeds."""
        wr = _ok_result()
        with (
            patch.object(
                dispatcher,
                "_materialize_postgresql",
                side_effect=RuntimeError("pg down"),
            ),
            patch.object(dispatcher, "_materialize_iceberg", return_value=wr),
        ):
            result = dispatcher.dispatch(
                mock_con,
                "v",
                [
                    {"type": "postgresql", "connection": "pg1", "table": "public.t"},
                    {"type": "iceberg", "table": "warehouse.ns.t"},
                ],
            )
        assert result.total == 2
        assert result.succeeded == 1
        assert result.failed == 1
        assert result.results[0]["success"] is False
        assert "pg down" in result.results[0]["error"]
        assert result.results[1]["success"] is True

    def test_best_effort_second_fails(self, dispatcher, mock_con):
        """First target succeeds, second fails."""
        wr = _ok_result()
        with (
            patch.object(dispatcher, "_materialize_postgresql", return_value=wr),
            patch.object(
                dispatcher,
                "_materialize_iceberg",
                side_effect=RuntimeError("iceberg timeout"),
            ),
        ):
            result = dispatcher.dispatch(
                mock_con,
                "v",
                [
                    {"type": "postgresql", "connection": "pg1", "table": "public.t"},
                    {"type": "iceberg", "table": "warehouse.ns.t"},
                ],
            )
        assert result.succeeded == 1
        assert result.failed == 1
        assert result.results[0]["success"] is True
        assert result.results[1]["success"] is False
        assert "iceberg timeout" in result.results[1]["error"]

    def test_best_effort_all_fail(self, dispatcher, mock_con):
        """All targets fail. DispatchResult reflects total failure."""
        with (
            patch.object(
                dispatcher,
                "_materialize_postgresql",
                side_effect=RuntimeError("pg fail"),
            ),
            patch.object(
                dispatcher,
                "_materialize_iceberg",
                side_effect=RuntimeError("ice fail"),
            ),
        ):
            result = dispatcher.dispatch(
                mock_con,
                "v",
                [
                    {"type": "postgresql", "connection": "pg1", "table": "public.t"},
                    {"type": "iceberg", "table": "warehouse.ns.t"},
                ],
            )
        assert result.total == 2
        assert result.succeeded == 0
        assert result.failed == 2
        assert result.all_succeeded is False


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Tests for edge cases and error paths."""

    def test_dispatch_unknown_type(self, dispatcher, mock_con):
        """Unknown type is caught and recorded as a failure."""
        result = dispatcher.dispatch(
            mock_con, "v", [{"type": "bigquery"}]
        )
        assert result.total == 1
        assert result.failed == 1
        assert "Unknown materialization type" in result.results[0]["error"]

    def test_dispatch_empty_targets(self, dispatcher, mock_con):
        """Empty target list returns DispatchResult(total=0)."""
        result = dispatcher.dispatch(mock_con, "v", [])
        assert result.total == 0
        assert result.succeeded == 0
        assert result.failed == 0
        assert result.all_succeeded is False

    def test_dispatch_preserves_write_result(self, dispatcher, mock_con):
        """Successful result dict contains the WriteResult object."""
        wr = WriteResult(
            success=True, snapshot_id="snap123", row_count=42, duration_seconds=1.23
        )
        with patch.object(dispatcher, "_materialize_iceberg", return_value=wr):
            result = dispatcher.dispatch(
                mock_con, "v", [{"type": "iceberg", "table": "t"}]
            )
        stored = result.results[0]["write_result"]
        assert stored is wr
        assert stored.snapshot_id == "snap123"
        assert stored.row_count == 42

    def test_dispatch_result_counts(self, dispatcher, mock_con):
        """total == succeeded + failed after mixed results."""
        wr = _ok_result()
        with (
            patch.object(dispatcher, "_materialize_postgresql", return_value=wr),
            patch.object(
                dispatcher,
                "_materialize_iceberg",
                side_effect=RuntimeError("fail"),
            ),
        ):
            result = dispatcher.dispatch(
                mock_con,
                "v",
                [
                    {"type": "postgresql", "connection": "pg1", "table": "public.t"},
                    {"type": "iceberg", "table": "t"},
                    {"type": "iceberg", "table": "t2"},
                ],
            )
        assert result.total == 3
        assert result.succeeded == 1
        assert result.failed == 2
        assert result.total == result.succeeded + result.failed

    def test_dispatch_postgresql_validates_config(self, dispatcher, mock_con):
        """Invalid PostgreSQL config is caught as a failure."""
        # Missing connection and table will fail validation inside _materialize_postgresql
        # We don't mock _materialize_postgresql, so the real code runs.
        # pg_config.from_dict + validate() should raise.
        result = dispatcher.dispatch(
            mock_con,
            "v",
            [{"type": "postgresql"}],  # missing connection + table
        )
        assert result.failed == 1
        assert result.results[0]["success"] is False
        assert "connection" in result.results[0]["error"].lower()

    def test_dispatch_node_id_in_labels(self, dispatcher, mock_con):
        """When node_id is provided it appears in target labels."""
        wr = _ok_result()
        with patch.object(dispatcher, "_materialize_iceberg", return_value=wr):
            result = dispatcher.dispatch(
                mock_con,
                "v",
                [{"type": "iceberg", "table": "t"}],
                node_id="source.orders",
            )
        assert "source.orders" in result.results[0]["target"]

    def test_dispatch_no_node_id_in_labels(self, dispatcher, mock_con):
        """When node_id is empty, label uses index only."""
        wr = _ok_result()
        with patch.object(dispatcher, "_materialize_iceberg", return_value=wr):
            result = dispatcher.dispatch(
                mock_con, "v", [{"type": "iceberg", "table": "t"}]
            )
        assert result.results[0]["target"] == "[0]:iceberg"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class TestLogging:
    """Tests for logging behaviour."""

    def test_dispatch_logs_per_target(self, dispatcher, mock_con, caplog):
        """Each target produces log messages."""
        wr = _ok_result(50)
        with (
            caplog.at_level(logging.INFO, logger="seeknal.workflow.materialization.dispatcher"),
            patch.object(dispatcher, "_materialize_postgresql", return_value=wr),
            patch.object(dispatcher, "_materialize_iceberg", return_value=wr),
        ):
            dispatcher.dispatch(
                mock_con,
                "v",
                [
                    {"type": "postgresql", "connection": "pg1", "table": "public.t"},
                    {"type": "iceberg", "table": "t"},
                ],
                node_id="n",
            )
        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        # Should have at least one "Materializing" and one "Materialized" per target
        assert sum("Materializing" in m for m in info_messages) >= 2
        assert sum("Materialized" in m for m in info_messages) >= 2

    def test_dispatch_logs_error_on_failure(self, dispatcher, mock_con, caplog):
        """Failed targets produce ERROR log messages."""
        with (
            caplog.at_level(logging.ERROR, logger="seeknal.workflow.materialization.dispatcher"),
            patch.object(
                dispatcher,
                "_materialize_iceberg",
                side_effect=RuntimeError("boom"),
            ),
        ):
            dispatcher.dispatch(mock_con, "v", [{"type": "iceberg", "table": "t"}])
        error_messages = [r.message for r in caplog.records if r.levelno == logging.ERROR]
        assert any("boom" in m for m in error_messages)


# ---------------------------------------------------------------------------
# Profile loader integration
# ---------------------------------------------------------------------------

class TestProfileLoaderIntegration:
    """Tests for profile_loader usage within the dispatcher."""

    def test_uses_injected_profile_loader_for_postgresql(self, mock_con):
        """Injected profile_loader.load_connection_profile is called."""
        loader = MagicMock()
        loader.load_connection_profile.return_value = {
            "host": "localhost",
            "port": 5432,
            "user": "u",
            "password": "p",
            "database": "db",
        }
        dispatcher = MaterializationDispatcher(profile_loader=loader)

        # Mock the downstream helper so we don't need a real PG
        with patch(
            "seeknal.workflow.materialization.dispatcher.MaterializationDispatcher._materialize_postgresql",
            wraps=dispatcher._materialize_postgresql,
        ):
            # We need to mock the PostgresMaterializationHelper.materialize call
            mock_helper_cls = MagicMock()
            mock_helper_cls.return_value.materialize.return_value = _ok_result()
            with (
                patch(
                    "seeknal.workflow.materialization.postgresql.PostgresMaterializationHelper",
                    mock_helper_cls,
                ),
                patch(
                    "seeknal.connections.postgresql.parse_postgresql_config",
                    return_value=MagicMock(),
                ),
            ):
                result = dispatcher.dispatch(
                    mock_con,
                    "v",
                    [
                        {
                            "type": "postgresql",
                            "connection": "my_pg",
                            "table": "public.orders",
                            "mode": "full",
                        }
                    ],
                )
        loader.load_connection_profile.assert_called_once_with("my_pg")
        assert result.succeeded == 1


# ---------------------------------------------------------------------------
# Iceberg catalog fallback chain
# ---------------------------------------------------------------------------

class TestIcebergCatalogFallback:
    """Tests for _materialize_iceberg catalog URI/warehouse fallback chain."""

    def _make_profile_config(self, uri="", warehouse=""):
        """Create a mock profile config with given catalog values."""
        catalog = MagicMock()
        catalog.uri = uri
        catalog.warehouse = warehouse
        catalog.bearer_token = None
        catalog.interpolate_env_vars.return_value = catalog
        config = MagicMock()
        config.catalog = catalog
        return config

    def test_iceberg_falls_back_to_source_defaults(self, mock_con):
        """When materialization.catalog is empty, falls back to source_defaults.iceberg."""
        loader = MagicMock()
        loader.load_profile.return_value = self._make_profile_config(uri="", warehouse="")
        loader.load_source_defaults.return_value = {
            "catalog_uri": "http://from-defaults:8181",
            "warehouse": "defaults-warehouse",
        }
        dispatcher = MaterializationDispatcher(profile_loader=loader)

        mock_ext = MagicMock()
        mock_ext.get_oauth2_token.return_value = "token123"
        mock_write = MagicMock(return_value=_ok_result(5))

        with (
            patch.dict("sys.modules", {
                "seeknal.workflow.materialization.operations": MagicMock(
                    DuckDBIcebergExtension=mock_ext,
                    write_to_iceberg=mock_write,
                ),
            }),
        ):
            # Re-import to pick up the patched module
            dispatcher._materialize_iceberg(
                mock_con, "my_view", {"table": "atlas.ns.tbl", "mode": "append"}
            )

        # Verify source_defaults was consulted
        loader.load_source_defaults.assert_called_once_with("iceberg")
        # Verify the resolved URI and warehouse were used
        mock_ext.attach_rest_catalog.assert_called_once()
        call_kwargs = mock_ext.attach_rest_catalog.call_args[1]
        assert call_kwargs["uri"] == "http://from-defaults:8181"
        assert call_kwargs["warehouse_path"] == "defaults-warehouse"

    def test_iceberg_falls_back_to_env_vars(self, mock_con, monkeypatch):
        """When both catalog and source_defaults are empty, falls back to env vars."""
        monkeypatch.setenv("LAKEKEEPER_URI", "http://from-env:8181")
        monkeypatch.setenv("LAKEKEEPER_WAREHOUSE", "env-warehouse")

        loader = MagicMock()
        loader.load_profile.return_value = self._make_profile_config(uri="", warehouse="")
        loader.load_source_defaults.return_value = {}
        dispatcher = MaterializationDispatcher(profile_loader=loader)

        mock_ext = MagicMock()
        mock_ext.get_oauth2_token.return_value = "token123"
        mock_write = MagicMock(return_value=_ok_result(5))

        with patch.dict("sys.modules", {
            "seeknal.workflow.materialization.operations": MagicMock(
                DuckDBIcebergExtension=mock_ext,
                write_to_iceberg=mock_write,
            ),
        }):
            dispatcher._materialize_iceberg(
                mock_con, "my_view", {"table": "atlas.ns.tbl", "mode": "append"}
            )

        call_kwargs = mock_ext.attach_rest_catalog.call_args[1]
        assert call_kwargs["uri"] == "http://from-env:8181"
        assert call_kwargs["warehouse_path"] == "env-warehouse"

    def test_iceberg_catalog_section_takes_precedence(self, mock_con, monkeypatch):
        """When materialization.catalog has values, they take precedence over defaults."""
        monkeypatch.setenv("LAKEKEEPER_URI", "http://from-env:8181")

        loader = MagicMock()
        loader.load_profile.return_value = self._make_profile_config(
            uri="http://from-catalog:8181", warehouse="catalog-warehouse"
        )
        dispatcher = MaterializationDispatcher(profile_loader=loader)

        mock_ext = MagicMock()
        mock_ext.get_oauth2_token.return_value = "token123"
        mock_write = MagicMock(return_value=_ok_result(5))

        with patch.dict("sys.modules", {
            "seeknal.workflow.materialization.operations": MagicMock(
                DuckDBIcebergExtension=mock_ext,
                write_to_iceberg=mock_write,
            ),
        }):
            dispatcher._materialize_iceberg(
                mock_con, "my_view", {"table": "atlas.ns.tbl", "mode": "append"}
            )

        call_kwargs = mock_ext.attach_rest_catalog.call_args[1]
        assert call_kwargs["uri"] == "http://from-catalog:8181"
        assert call_kwargs["warehouse_path"] == "catalog-warehouse"
        # source_defaults should NOT be consulted
        loader.load_source_defaults.assert_not_called()

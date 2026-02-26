"""Tests for REPL project-aware auto-registration."""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call


@pytest.fixture
def mock_duckdb():
    """Patch duckdb.connect to return a mock connection."""
    with patch("seeknal.cli.repl.duckdb") as mock_mod:
        mock_conn = MagicMock()
        mock_mod.connect.return_value = mock_conn
        yield mock_conn


# --- Project detection (CLI) ---


def test_repl_command_detects_project(tmp_path):
    """repl command sets project_path when seeknal_project.yml exists."""
    (tmp_path / "seeknal_project.yml").write_text("name: test")

    with (
        patch("seeknal.cli.main.Path") as MockPath,
        patch("seeknal.cli.repl.duckdb") as mock_duckdb_mod,
        patch("seeknal.cli.repl.REPL.run"),
    ):
        mock_duckdb_mod.connect.return_value = MagicMock()
        mock_cwd = MagicMock()
        mock_cwd.resolve.return_value = tmp_path
        mock_cwd.__truediv__ = lambda self, other: tmp_path / other
        MockPath.cwd.return_value = mock_cwd
        # Also handle Path(profile) call
        MockPath.side_effect = lambda x: Path(x)

        from seeknal.cli.repl import run_repl

        with patch("seeknal.cli.repl.REPL") as MockREPL:
            instance = MagicMock()
            MockREPL.return_value = instance
            run_repl(project_path=tmp_path, profile_path=None)
            MockREPL.assert_called_once_with(
                project_path=tmp_path, profile_path=None, env_name=None
            )


def test_repl_command_no_project(tmp_path):
    """repl command sets project_path=None when no seeknal_project.yml."""
    from seeknal.cli.repl import run_repl

    with patch("seeknal.cli.repl.REPL") as MockREPL:
        with patch("seeknal.cli.repl.duckdb"):
            instance = MagicMock()
            MockREPL.return_value = instance
            run_repl(project_path=None, profile_path=None)
            MockREPL.assert_called_once_with(
                project_path=None, profile_path=None, env_name=None
            )


# --- Profile flag ---


def test_repl_with_profile_path(tmp_path, mock_duckdb):
    """REPL accepts profile_path parameter."""
    from seeknal.cli.repl import REPL

    profile = tmp_path / "profiles.yml"
    r = REPL(project_path=None, profile_path=profile)
    assert r.profile_path == profile


# --- Parquet registration ---


def test_register_parquets(tmp_path, mock_duckdb):
    """Phase 1: parquet files in target/cache/ are registered as views."""
    from seeknal.cli.repl import REPL

    cache_dir = tmp_path / "target" / "cache" / "transform"
    cache_dir.mkdir(parents=True)
    (cache_dir / "clean_data.parquet").write_bytes(b"fake")
    (cache_dir / "agg_data.parquet").write_bytes(b"fake")

    r = REPL(project_path=tmp_path)
    assert r._registered_parquets == 2
    # Should have called CREATE VIEW for each parquet
    view_calls = [
        c for c in mock_duckdb.execute.call_args_list
        if "CREATE VIEW" in str(c)
    ]
    assert len(view_calls) == 2


def test_register_intermediate_parquets(tmp_path, mock_duckdb):
    """Phase 1: parquet files in target/intermediate/ are registered with kind prefix kept."""
    from seeknal.cli.repl import REPL

    intermediate_dir = tmp_path / "target" / "intermediate"
    intermediate_dir.mkdir(parents=True)
    (intermediate_dir / "transform_orders_cleaned.parquet").write_bytes(b"fake")
    (intermediate_dir / "source_raw_orders.parquet").write_bytes(b"fake")

    r = REPL(project_path=tmp_path)
    assert r._registered_parquets == 2
    # View names should keep the kind prefix to avoid collisions
    view_calls = [
        str(c) for c in mock_duckdb.execute.call_args_list
        if "CREATE VIEW" in str(c)
    ]
    assert any('"transform_orders_cleaned"' in c for c in view_calls)
    assert any('"source_raw_orders"' in c for c in view_calls)


def test_register_intermediate_deduplicates_with_cache(tmp_path, mock_duckdb):
    """Phase 1: intermediate/ takes priority over cache/ for same-named views."""
    from seeknal.cli.repl import REPL

    intermediate_dir = tmp_path / "target" / "intermediate"
    intermediate_dir.mkdir(parents=True)
    # Use same stem so intermediate and cache would produce the same view name
    (intermediate_dir / "orders.parquet").write_bytes(b"fake")

    cache_dir = tmp_path / "target" / "cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "orders.parquet").write_bytes(b"fake")

    r = REPL(project_path=tmp_path)
    # Should only register once (intermediate wins due to dedup)
    assert r._registered_parquets == 1


def test_register_parquets_no_cache_dir(tmp_path, mock_duckdb):
    """Phase 1: no error when target/cache/ doesn't exist."""
    from seeknal.cli.repl import REPL

    r = REPL(project_path=tmp_path)
    assert r._registered_parquets == 0


def test_register_parquets_skips_unreadable(tmp_path, mock_duckdb):
    """Phase 1: unreadable parquet files are silently skipped."""
    from seeknal.cli.repl import REPL

    cache_dir = tmp_path / "target" / "cache" / "source"
    cache_dir.mkdir(parents=True)
    (cache_dir / "good.parquet").write_bytes(b"fake")
    (cache_dir / "bad.parquet").write_bytes(b"fake")

    def side_effect(sql, *args, **kwargs):
        if "CREATE VIEW" in str(sql) and "bad" in str(sql):
            raise Exception("Cannot read file")
        return MagicMock()

    mock_duckdb.execute.side_effect = side_effect

    r = REPL(project_path=tmp_path)
    assert r._registered_parquets == 1


# --- PostgreSQL registration ---


def test_register_postgresql_connections(tmp_path, mock_duckdb):
    """Phase 2: PostgreSQL connections from profiles.yml are attached read-only."""
    from seeknal.cli.repl import REPL

    profile_data = {
        "connections": {
            "my_pg": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "user": "test",
                "password": "secret",
                "database": "analytics",
            },
            "my_mysql": {
                "type": "mysql",
                "host": "localhost",
            },
        }
    }

    with patch(
        "seeknal.workflow.materialization.profile_loader.ProfileLoader._load_profile_data",
        return_value=profile_data,
    ):
        r = REPL(project_path=tmp_path)

    assert r._registered_pg == 1
    assert "my_pg" in r.attached
    assert "my_mysql" not in r.attached
    # Verify ATTACH was called with READ_ONLY and connect_timeout
    attach_calls = [
        c for c in mock_duckdb.execute.call_args_list
        if "TYPE postgres" in str(c) and "READ_ONLY" in str(c)
    ]
    assert len(attach_calls) == 1
    assert "connect_timeout=5" in str(attach_calls[0])


def test_register_postgresql_connection_failure(tmp_path, mock_duckdb):
    """Phase 2: failed PostgreSQL connections are silently skipped."""
    from seeknal.cli.repl import REPL

    profile_data = {
        "connections": {
            "bad_pg": {
                "type": "postgresql",
                "host": "unreachable",
                "port": 5432,
                "user": "test",
                "password": "secret",
                "database": "db",
            }
        }
    }

    def execute_side_effect(sql, *args, **kwargs):
        if "TYPE postgres" in sql:
            raise Exception("Connection refused")
        return MagicMock()

    mock_duckdb.execute.side_effect = execute_side_effect

    with patch(
        "seeknal.workflow.materialization.profile_loader.ProfileLoader._load_profile_data",
        return_value=profile_data,
    ):
        r = REPL(project_path=tmp_path)

    assert r._registered_pg == 0
    assert "bad_pg" not in r.attached


def test_register_postgresql_no_profile(tmp_path, mock_duckdb):
    """Phase 2: no error when profiles.yml doesn't exist."""
    from seeknal.cli.repl import REPL

    with patch(
        "seeknal.workflow.materialization.profile_loader.ProfileLoader._load_profile_data",
        return_value={},
    ):
        r = REPL(project_path=tmp_path)

    assert r._registered_pg == 0


# --- Iceberg registration ---


def test_register_iceberg_catalog(tmp_path, mock_duckdb):
    """Phase 3: Iceberg catalog from profiles.yml is attached."""
    from seeknal.cli.repl import REPL

    profile_data = {
        "materialization": {
            "catalog": {
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "my-warehouse",
            }
        }
    }

    with (
        patch(
            "seeknal.workflow.materialization.profile_loader.ProfileLoader._load_profile_data",
            return_value=profile_data,
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.load_extension"
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.configure_s3"
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.get_oauth2_token",
            return_value="test-token",
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.attach_rest_catalog"
        ) as mock_attach,
    ):
        r = REPL(project_path=tmp_path)

    assert r._registered_iceberg == 1
    assert "iceberg" in r.attached
    mock_attach.assert_called_once_with(
        con=mock_duckdb,
        catalog_name="iceberg",
        uri="http://localhost:8181",
        warehouse_path="my-warehouse",
        bearer_token="test-token",
    )


def test_register_iceberg_no_oauth2(tmp_path, mock_duckdb):
    """Phase 3: Iceberg attaches without OAuth2 token when not configured."""
    from seeknal.cli.repl import REPL

    profile_data = {
        "materialization": {
            "catalog": {
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "my-warehouse",
            }
        }
    }

    with (
        patch(
            "seeknal.workflow.materialization.profile_loader.ProfileLoader._load_profile_data",
            return_value=profile_data,
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.load_extension"
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.configure_s3"
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.get_oauth2_token",
            side_effect=Exception("No OAuth2 configured"),
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.attach_rest_catalog"
        ) as mock_attach,
    ):
        r = REPL(project_path=tmp_path)

    assert r._registered_iceberg == 1
    mock_attach.assert_called_once_with(
        con=mock_duckdb,
        catalog_name="iceberg",
        uri="http://localhost:8181",
        warehouse_path="my-warehouse",
        bearer_token=None,
    )


def test_register_iceberg_no_catalog_config(tmp_path, mock_duckdb):
    """Phase 3: no error when no catalog config in profile."""
    from seeknal.cli.repl import REPL

    with patch(
        "seeknal.workflow.materialization.profile_loader.ProfileLoader._load_profile_data",
        return_value={"materialization": {}},
    ):
        r = REPL(project_path=tmp_path)

    assert r._registered_iceberg == 0


def test_register_iceberg_attach_failure(tmp_path, mock_duckdb):
    """Phase 3: Iceberg attach failure is caught as warning."""
    import warnings
    from seeknal.cli.repl import REPL

    profile_data = {
        "materialization": {
            "catalog": {
                "type": "rest",
                "uri": "http://localhost:8181",
                "warehouse": "my-warehouse",
            }
        }
    }

    with (
        patch(
            "seeknal.workflow.materialization.profile_loader.ProfileLoader._load_profile_data",
            return_value=profile_data,
        ),
        patch(
            "seeknal.workflow.materialization.operations.DuckDBIcebergExtension.load_extension",
            side_effect=Exception("Extension not available"),
        ),
        warnings.catch_warnings(record=True) as w,
    ):
        warnings.simplefilter("always")
        r = REPL(project_path=tmp_path)

    assert r._registered_iceberg == 0
    warning_msgs = [str(x.message) for x in w]
    assert any("Iceberg" in msg for msg in warning_msgs)


# --- Banner output ---


def test_banner_shows_project_context(tmp_path, mock_duckdb):
    """Banner displays project name, node count, last run, and registration details."""
    import json
    from seeknal.cli.repl import REPL

    cache_dir = tmp_path / "target" / "cache" / "transform"
    cache_dir.mkdir(parents=True)
    (cache_dir / "node1.parquet").write_bytes(b"fake")

    # Create run_state.json with node count and last_run
    state_dir = tmp_path / "target"
    state_data = {
        "schema_version": "2.0",
        "last_run": "2026-02-21T10:38:00",
        "nodes": {
            "source.raw_orders": {"status": "success"},
            "transform.enriched": {"status": "success"},
            "transform.summary": {"status": "success"},
        },
    }
    (state_dir / "run_state.json").write_text(json.dumps(state_data))

    r = REPL(project_path=tmp_path)

    with patch("builtins.input", side_effect=EOFError):
        with patch("builtins.print") as mock_print:
            r.run()

    printed = " ".join(str(c) for c in mock_print.call_args_list)
    assert tmp_path.name in printed
    assert "3 nodes" in printed
    assert "last run: 2026-02-21 10:38" in printed
    assert "Intermediate outputs:" in printed


def test_banner_shows_no_run_state(tmp_path, mock_duckdb):
    """Banner works correctly when run_state.json doesn't exist."""
    from seeknal.cli.repl import REPL

    cache_dir = tmp_path / "target" / "cache" / "transform"
    cache_dir.mkdir(parents=True)
    (cache_dir / "node1.parquet").write_bytes(b"fake")

    r = REPL(project_path=tmp_path)

    with patch("builtins.input", side_effect=EOFError):
        with patch("builtins.print") as mock_print:
            r.run()

    printed = " ".join(str(c) for c in mock_print.call_args_list)
    assert tmp_path.name in printed
    assert "nodes" not in printed
    assert "last run" not in printed


def test_banner_no_project(mock_duckdb):
    """Banner does not show project info when no project detected."""
    from seeknal.cli.repl import REPL

    r = REPL()

    with patch("builtins.input", side_effect=EOFError):
        with patch("builtins.print") as mock_print:
            r.run()

    printed = " ".join(str(c) for c in mock_print.call_args_list)
    assert "Project:" not in printed


# --- .tables with auto-registered views ---


def test_tables_shows_parquet_views(tmp_path, mock_duckdb):
    """`.tables` includes auto-registered parquet views."""
    from seeknal.cli.repl import REPL

    cache_dir = tmp_path / "target" / "cache" / "transform"
    cache_dir.mkdir(parents=True)
    (cache_dir / "my_view.parquet").write_bytes(b"fake")

    r = REPL(project_path=tmp_path)

    # Mock information_schema query for views
    def execute_side_effect(sql, *args, **kwargs):
        if "information_schema.tables" in sql and "VIEW" in sql:
            result = MagicMock()
            result.fetchall.return_value = [("my_view",)]
            return result
        return MagicMock()

    mock_duckdb.execute.side_effect = execute_side_effect

    with patch("builtins.print") as mock_print:
        r._show_tables()

    printed = " ".join(str(c) for c in mock_print.call_args_list)
    assert "my_view" in printed


def test_tables_empty_shows_help(mock_duckdb):
    """`.tables` shows help message when nothing is registered."""
    from seeknal.cli.repl import REPL

    r = REPL()

    with patch("builtins.print") as mock_print:
        r._show_tables()

    printed = " ".join(str(c) for c in mock_print.call_args_list)
    assert "No databases connected" in printed

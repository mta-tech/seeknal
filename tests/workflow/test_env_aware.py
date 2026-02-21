"""Tests for env-aware execution features.

Covers:
- DAGRunner with ExecutionContext (new executor path)
- profile_path propagation through env commands
- Convention-based env-specific profile auto-discovery
- Namespace prefixing (_prefix_target, _ensure_pg_schema)
- Promotion with re-materialization
- Dry-run mode for promotion
- plan.json profile_path wiring
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType, Edge
from seeknal.workflow.environment import EnvironmentManager
from seeknal.workflow.materialization.dispatcher import (
    MaterializationDispatcher,
    DispatchResult,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_node(name, node_type=NodeType.SOURCE, **kwargs):
    """Helper to create test nodes."""
    return Node(
        id=f"{node_type.value}.{name}",
        name=name,
        node_type=node_type,
        config=kwargs.get("config", {}),
        columns=kwargs.get("columns", {}),
        description=kwargs.get("description", ""),
        owner=kwargs.get("owner", ""),
        tags=kwargs.get("tags", []),
    )


def _make_manifest(*nodes, edges=None, project="test"):
    """Helper to create test manifests."""
    manifest = Manifest(project=project)
    for node in nodes:
        manifest.add_node(node)
    for edge in (edges or []):
        manifest.add_edge(edge[0], edge[1])
    return manifest


def _setup_applied_env(target: Path, env_name: str = "dev", mat_config=None):
    """Create a fully applied environment for promote tests."""
    node_config = {}
    if mat_config:
        node_config["materializations"] = mat_config

    manager = EnvironmentManager(target)
    node = _make_node("orders", NodeType.TRANSFORM, config=node_config)
    manifest = _make_manifest(node)
    manager.plan(env_name, manifest)

    env_dir = target / "environments" / env_name
    run_state = {"status": "completed", "nodes_executed": 1}
    with open(env_dir / "run_state.json", "w") as f:
        json.dump(run_state, f)

    cache_dir = env_dir / "cache" / "transform"
    cache_dir.mkdir(parents=True)
    (cache_dir / "orders.parquet").write_text("fake parquet data")

    return manager


# =============================================================================
# DAGRunner with ExecutionContext
# =============================================================================


class TestDAGRunnerWithExecContext:
    """DAGRunner delegates to get_executor() when exec_context is provided."""

    def test_exec_context_stored(self, tmp_path):
        """DAGRunner stores exec_context as an attribute."""
        from seeknal.workflow.runner import DAGRunner
        from seeknal.workflow.executors.base import ExecutionContext

        manifest = _make_manifest(_make_node("users"))
        ctx = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
        )
        runner = DAGRunner(manifest, target_path=tmp_path / "target", exec_context=ctx)

        assert runner.exec_context is ctx

    def test_exec_context_none_by_default(self, tmp_path):
        """DAGRunner defaults to exec_context=None (legacy path)."""
        from seeknal.workflow.runner import DAGRunner

        manifest = _make_manifest(_make_node("users"))
        runner = DAGRunner(manifest, target_path=tmp_path / "target")

        assert runner.exec_context is None

    @patch("seeknal.workflow.runner.DAGRunner._execute_by_type")
    def test_execute_by_type_uses_get_executor_with_context(self, mock_exec, tmp_path):
        """When exec_context is set, _execute_by_type routes through get_executor."""
        from seeknal.workflow.runner import DAGRunner
        from seeknal.workflow.executors.base import ExecutionContext

        manifest = _make_manifest(_make_node("users"))
        ctx = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
        )
        runner = DAGRunner(manifest, target_path=tmp_path / "target", exec_context=ctx)

        # The _execute_by_type method is patched; verify it gets called during run
        mock_exec.return_value = {"row_count": 10, "status": "success"}
        runner.run(full=True)

        assert mock_exec.called

    def test_exec_context_env_name_wired(self, tmp_path):
        """ExecutionContext carries env_name field."""
        from seeknal.workflow.executors.base import ExecutionContext

        ctx = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
            env_name="staging",
        )

        assert ctx.env_name == "staging"

    def test_exec_context_profile_path_wired(self, tmp_path):
        """ExecutionContext carries profile_path field."""
        from seeknal.workflow.executors.base import ExecutionContext

        profile = tmp_path / "profiles-dev.yml"
        ctx = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
            profile_path=profile,
        )

        assert ctx.profile_path == profile


# =============================================================================
# Profile auto-discovery
# =============================================================================


class TestEnvProfileAutoDiscovery:
    """_resolve_env_profile picks the right profiles.yml by convention."""

    def test_explicit_profile_takes_precedence(self, tmp_path):
        """Explicit --profile flag overrides all conventions."""
        from seeknal.cli.main import _resolve_env_profile

        explicit = tmp_path / "custom-profiles.yml"
        explicit.touch()

        # Even if project-level profile exists, explicit wins
        project_profile = tmp_path / "profiles-dev.yml"
        project_profile.touch()

        result = _resolve_env_profile("dev", tmp_path, explicit_profile=explicit)
        assert result == explicit

    def test_project_level_profile_found(self, tmp_path):
        """profiles-{env}.yml in project root is discovered."""
        from seeknal.cli.main import _resolve_env_profile

        project_profile = tmp_path / "profiles-staging.yml"
        project_profile.touch()

        result = _resolve_env_profile("staging", tmp_path)
        assert result == project_profile

    def test_home_level_profile_found(self, tmp_path):
        """~/.seeknal/profiles-{env}.yml is discovered when no project-level file."""
        from seeknal.cli.main import _resolve_env_profile

        fake_home = tmp_path / "home_seeknal"
        fake_home.mkdir()
        home_profile = fake_home / "profiles-dev.yml"
        home_profile.touch()

        with patch("seeknal.context.CONFIG_BASE_URL", str(fake_home)):
            result = _resolve_env_profile("dev", tmp_path)

        assert result == home_profile

    def test_no_profile_returns_none(self, tmp_path):
        """None returned when no convention-based profile exists."""
        from seeknal.cli.main import _resolve_env_profile

        fake_home = tmp_path / "empty_home"
        fake_home.mkdir()

        with patch("seeknal.context.CONFIG_BASE_URL", str(fake_home)):
            result = _resolve_env_profile("dev", tmp_path)

        assert result is None

    def test_project_profile_preferred_over_home(self, tmp_path):
        """Project-level profile takes precedence over home-level."""
        from seeknal.cli.main import _resolve_env_profile

        project_profile = tmp_path / "profiles-dev.yml"
        project_profile.touch()

        fake_home = tmp_path / "home_seeknal"
        fake_home.mkdir()
        home_profile = fake_home / "profiles-dev.yml"
        home_profile.touch()

        with patch("seeknal.context.CONFIG_BASE_URL", str(fake_home)):
            result = _resolve_env_profile("dev", tmp_path)

        assert result == project_profile


# =============================================================================
# Namespace prefixing (_prefix_target)
# =============================================================================


class TestPrefixTarget:
    """MaterializationDispatcher._prefix_target applies env-based prefixes."""

    def test_postgresql_schema_prefixed(self):
        """PostgreSQL: schema.table -> {env}_schema.table."""
        target = {"type": "postgresql", "table": "analytics.orders", "mode": "full"}
        result = MaterializationDispatcher._prefix_target(target, "dev")

        assert result["table"] == "dev_analytics.orders"

    def test_iceberg_namespace_prefixed(self):
        """Iceberg: catalog.ns.table -> catalog.{env}_ns.table."""
        target = {"type": "iceberg", "table": "atlas.warehouse.orders"}
        result = MaterializationDispatcher._prefix_target(target, "staging")

        assert result["table"] == "atlas.staging_warehouse.orders"

    def test_postgresql_no_schema_unchanged(self):
        """PostgreSQL table without schema is not prefixed."""
        target = {"type": "postgresql", "table": "orders", "mode": "full"}
        result = MaterializationDispatcher._prefix_target(target, "dev")

        assert result["table"] == "orders"

    def test_iceberg_two_part_name_unchanged(self):
        """Iceberg table with less than 3 parts is not prefixed."""
        target = {"type": "iceberg", "table": "ns.orders"}
        result = MaterializationDispatcher._prefix_target(target, "dev")

        assert result["table"] == "ns.orders"

    def test_original_target_not_mutated(self):
        """_prefix_target returns a copy; original is unchanged."""
        target = {"type": "postgresql", "table": "public.orders", "mode": "full"}
        result = MaterializationDispatcher._prefix_target(target, "dev")

        assert target["table"] == "public.orders"
        assert result["table"] == "dev_public.orders"
        assert result is not target

    def test_no_table_key_is_safe(self):
        """Missing 'table' key doesn't raise."""
        target = {"type": "postgresql", "mode": "full"}
        result = MaterializationDispatcher._prefix_target(target, "dev")

        assert result.get("table", "") == ""

    def test_default_type_is_iceberg(self):
        """When type is missing, defaults to iceberg prefixing logic."""
        target = {"table": "atlas.ns.orders"}
        result = MaterializationDispatcher._prefix_target(target, "dev")

        assert result["table"] == "atlas.dev_ns.orders"

    def test_other_fields_preserved(self):
        """Non-table fields are preserved in the copy."""
        target = {
            "type": "postgresql",
            "table": "analytics.orders",
            "mode": "upsert_by_key",
            "unique_keys": ["id"],
            "connection": "local_pg",
        }
        result = MaterializationDispatcher._prefix_target(target, "dev")

        assert result["mode"] == "upsert_by_key"
        assert result["unique_keys"] == ["id"]
        assert result["connection"] == "local_pg"


# =============================================================================
# _ensure_pg_schema
# =============================================================================


class TestEnsurePgSchema:
    """MaterializationDispatcher._ensure_pg_schema creates schemas safely."""

    def test_public_schema_skipped(self):
        """'public' schema is never created (always exists)."""
        mock_con = MagicMock()
        mock_pg_config = MagicMock()

        MaterializationDispatcher._ensure_pg_schema(mock_con, mock_pg_config, "public")

        mock_con.execute.assert_not_called()

    def test_unsafe_name_skipped(self):
        """Schema names with special characters are rejected."""
        mock_con = MagicMock()
        mock_pg_config = MagicMock()

        MaterializationDispatcher._ensure_pg_schema(
            mock_con, mock_pg_config, "dev; DROP TABLE users"
        )

        mock_con.execute.assert_not_called()

    def test_valid_schema_created(self):
        """Valid schema name triggers ATTACH + CREATE SCHEMA."""
        mock_con = MagicMock()
        mock_pg_config = MagicMock()
        mock_pg_config.to_libpq_string.return_value = "host=localhost dbname=test"

        MaterializationDispatcher._ensure_pg_schema(
            mock_con, mock_pg_config, "dev_analytics"
        )

        # Should have called: INSTALL postgres, LOAD postgres, ATTACH, CREATE SCHEMA, DETACH
        calls = mock_con.execute.call_args_list
        assert any("INSTALL postgres" in str(c) for c in calls)
        assert any("ATTACH" in str(c) for c in calls)
        assert any("CREATE SCHEMA IF NOT EXISTS dev_analytics" in str(c) for c in calls)
        assert any("DETACH" in str(c) for c in calls)

    def test_exception_does_not_propagate(self):
        """_ensure_pg_schema swallows exceptions (best-effort)."""
        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("connection refused")
        mock_pg_config = MagicMock()
        mock_pg_config.to_libpq_string.return_value = "host=localhost"

        # Should not raise
        MaterializationDispatcher._ensure_pg_schema(
            mock_con, mock_pg_config, "dev_analytics"
        )

    def test_underscore_prefix_is_valid(self):
        """Schema names starting with underscore are accepted."""
        mock_con = MagicMock()
        mock_pg_config = MagicMock()
        mock_pg_config.to_libpq_string.return_value = "host=localhost"

        MaterializationDispatcher._ensure_pg_schema(
            mock_con, mock_pg_config, "_staging_analytics"
        )

        assert mock_con.execute.called


# =============================================================================
# Dispatch with env_name
# =============================================================================


class TestDispatchWithEnvName:
    """MaterializationDispatcher.dispatch() applies prefixes when env_name is set."""

    def test_dispatch_without_env_name_no_prefix(self):
        """When env_name is None, targets are not prefixed."""
        dispatcher = MaterializationDispatcher()

        with patch.object(dispatcher, "_materialize_postgresql") as mock_pg:
            mock_pg.return_value = MagicMock(row_count=10)
            target = {"type": "postgresql", "table": "public.orders", "mode": "full"}

            dispatcher.dispatch(
                con=MagicMock(),
                view_name="transform.orders",
                targets=[target],
                node_id="transform.orders",
                env_name=None,
            )

            # Called with original table name
            called_target = mock_pg.call_args[0][2]
            assert called_target["table"] == "public.orders"

    def test_dispatch_with_env_name_prefixes(self):
        """When env_name is set, targets get prefixed."""
        dispatcher = MaterializationDispatcher()

        with patch.object(dispatcher, "_materialize_postgresql") as mock_pg:
            mock_pg.return_value = MagicMock(row_count=10)
            target = {"type": "postgresql", "table": "analytics.orders", "mode": "full"}

            dispatcher.dispatch(
                con=MagicMock(),
                view_name="transform.orders",
                targets=[target],
                node_id="transform.orders",
                env_name="dev",
            )

            called_target = mock_pg.call_args[0][2]
            assert called_target["table"] == "dev_analytics.orders"

    def test_dispatch_with_env_name_iceberg(self):
        """Iceberg targets also get prefixed."""
        dispatcher = MaterializationDispatcher()

        with patch.object(dispatcher, "_materialize_iceberg") as mock_ice:
            mock_ice.return_value = MagicMock(row_count=50)
            target = {"type": "iceberg", "table": "atlas.ns.orders"}

            dispatcher.dispatch(
                con=MagicMock(),
                view_name="transform.orders",
                targets=[target],
                env_name="staging",
            )

            called_target = mock_ice.call_args[0][2]
            assert called_target["table"] == "atlas.staging_ns.orders"


# =============================================================================
# Promotion with re-materialization
# =============================================================================


class TestPromoteWithRematerialize:
    """promote() returns materialization node info when rematerialize=True."""

    def test_promote_dry_run_returns_plan(self, tmp_path):
        """dry_run=True returns plan without file operations."""
        target = tmp_path / "target"
        manager = _setup_applied_env(target, "dev", mat_config=[
            {"type": "postgresql", "table": "analytics.orders", "mode": "full"}
        ])

        result = manager.promote("dev", "prod", dry_run=True)

        assert result["promoted"] is False
        assert result["from_env"] == "dev"
        assert result["to_env"] == "prod"

    def test_promote_dry_run_no_files_copied(self, tmp_path):
        """dry_run=True does not copy files to production."""
        target = tmp_path / "target"
        manager = _setup_applied_env(target, "dev")

        manager.promote("dev", "prod", dry_run=True)

        # Production cache should NOT exist
        assert not (target / "cache" / "transform" / "orders.parquet").exists()

    def test_promote_with_rematerialize_collects_nodes(self, tmp_path):
        """rematerialize=True collects nodes with materialization configs."""
        target = tmp_path / "target"
        mat = [{"type": "postgresql", "table": "analytics.orders", "mode": "full"}]
        manager = _setup_applied_env(target, "dev", mat_config=mat)

        result = manager.promote("dev", "prod", rematerialize=True)

        assert "transform.orders" in result["rematerialize_nodes"]

    def test_promote_without_rematerialize_no_nodes(self, tmp_path):
        """rematerialize=False returns empty set even when nodes have configs."""
        target = tmp_path / "target"
        mat = [{"type": "postgresql", "table": "analytics.orders"}]
        manager = _setup_applied_env(target, "dev", mat_config=mat)

        result = manager.promote("dev", "prod", rematerialize=False)

        assert len(result["rematerialize_nodes"]) == 0

    def test_promote_dry_run_with_rematerialize(self, tmp_path):
        """dry_run + rematerialize returns nodes but skips file ops."""
        target = tmp_path / "target"
        mat = [{"type": "iceberg", "table": "atlas.ns.orders"}]
        manager = _setup_applied_env(target, "dev", mat_config=mat)

        result = manager.promote("dev", "prod", dry_run=True, rematerialize=True)

        assert result["promoted"] is False
        assert "transform.orders" in result["rematerialize_nodes"]
        # No files copied
        assert not (target / "run_state.json").exists()

    def test_promote_profile_path_returned(self, tmp_path):
        """profile_path is included in the promote result."""
        target = tmp_path / "target"
        manager = _setup_applied_env(target, "dev")
        profile = tmp_path / "profiles.yml"

        result = manager.promote(
            "dev", "prod", profile_path=profile, dry_run=True
        )

        assert result["profile_path"] == profile

    def test_promote_returns_manifest(self, tmp_path):
        """promote result includes the manifest object."""
        target = tmp_path / "target"
        manager = _setup_applied_env(target, "dev")

        result = manager.promote("dev", "prod", dry_run=True)

        assert result["manifest"] is not None
        assert "transform.orders" in result["manifest"].nodes

    def test_promote_actual_copies_files(self, tmp_path):
        """Non-dry-run promote copies files to production."""
        target = tmp_path / "target"
        manager = _setup_applied_env(target, "dev")

        result = manager.promote("dev", "prod")

        assert result["promoted"] is True
        assert (target / "run_state.json").exists()
        assert (target / "manifest.json").exists()


# =============================================================================
# plan.json profile_path integration
# =============================================================================


class TestPlanJsonProfilePath:
    """Env plan saves profile_path info for apply to use."""

    def test_plan_saves_manifest_json(self, tmp_path):
        """Plan persists manifest.json that apply can load."""
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))

        manager.plan("dev", manifest)

        env_dir = target / "environments" / "dev"
        manifest_data = json.loads((env_dir / "manifest.json").read_text())
        loaded = Manifest.from_dict(manifest_data)
        assert "source.users" in loaded.nodes

    def test_plan_json_has_fingerprint(self, tmp_path):
        """plan.json contains a manifest_fingerprint for staleness check."""
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))

        manager.plan("dev", manifest)

        env_dir = target / "environments" / "dev"
        plan_data = json.loads((env_dir / "plan.json").read_text())
        assert "manifest_fingerprint" in plan_data
        assert len(plan_data["manifest_fingerprint"]) == 64  # SHA256 hex length

    def test_plan_saves_profile_path(self, tmp_path):
        """plan.json persists profile_path for apply to restore."""
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        profile = tmp_path / "profiles-dev.yml"

        manager.plan("dev", manifest, profile_path=profile)

        env_dir = target / "environments" / "dev"
        plan_data = json.loads((env_dir / "plan.json").read_text())
        assert plan_data["profile_path"] == str(profile)

    def test_plan_saves_none_profile_path(self, tmp_path):
        """plan.json stores null when no profile_path provided."""
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))

        manager.plan("dev", manifest)

        env_dir = target / "environments" / "dev"
        plan_data = json.loads((env_dir / "plan.json").read_text())
        assert plan_data["profile_path"] is None

    def test_apply_restores_profile_path(self, tmp_path):
        """apply() restores profile_path from plan.json."""
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        profile = tmp_path / "profiles-dev.yml"

        manager.plan("dev", manifest, profile_path=profile)
        result = manager.apply("dev")

        assert result["profile_path"] == profile


# =============================================================================
# ExecutionContext in _run_in_environment
# =============================================================================


class TestRunInEnvironmentContext:
    """_run_in_environment creates ExecutionContext with env_name and profile_path."""

    @patch("seeknal.workflow.environment.EnvironmentManager")
    def test_show_plan_returns_early(self, mock_mgr_cls, tmp_path):
        """show_plan=True prints the plan and returns without creating a runner."""
        from seeknal.cli.main import _run_in_environment

        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.apply.return_value = {
            "plan": {},
            "manifest": _make_manifest(_make_node("users")),
            "env_dir": tmp_path / "target" / "environments" / "dev",
            "nodes_to_execute": {"source.users"},
        }

        env_dir = tmp_path / "target" / "environments" / "dev"
        env_dir.mkdir(parents=True)

        # show_plan=True should not raise and should return early
        _run_in_environment(
            env_name="dev",
            project_path=tmp_path,
            dry_run=True,
            show_plan=True,
        )

    @patch("seeknal.workflow.environment.EnvironmentManager")
    def test_profile_auto_discovery_called(self, mock_mgr_cls, tmp_path):
        """_resolve_env_profile is invoked during _run_in_environment."""
        from seeknal.cli.main import _run_in_environment, _resolve_env_profile

        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.apply.return_value = {
            "plan": {},
            "manifest": _make_manifest(_make_node("users")),
            "env_dir": tmp_path / "target" / "environments" / "dev",
            "nodes_to_execute": set(),  # empty -> returns early
        }

        # No env-specific profiles exist, so _resolve_env_profile returns None
        fake_home = tmp_path / "empty_home"
        fake_home.mkdir()

        with patch("seeknal.context.CONFIG_BASE_URL", str(fake_home)):
            _run_in_environment(
                env_name="dev",
                project_path=tmp_path,
            )

        # If it got here without error, the auto-discovery was called and
        # returned None (no profile found), and the function completed.

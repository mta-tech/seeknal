"""
Tests for the unified developer experience (DX) commands.

Tests the following new/updated commands:
- seeknal plan [env]          — Unified plan command (production + environment)
- seeknal run --env <name>    — Run in isolated virtual environment
- seeknal promote <env> [target] — Promote environment to production
- Auto-parallel hint          — Suggest --parallel when layers have >3 nodes
- Updated init output         — Shows unified workflow in "Next steps"
- Backward compatibility      — seeknal env plan/apply/promote still work
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from typer.testing import CliRunner
from seeknal.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_project_path(tmp_path):
    """Create a temporary project path with seeknal directory structure."""
    project_path = tmp_path / "test_project"
    project_path.mkdir()

    (project_path / "seeknal" / "sources").mkdir(parents=True)
    (project_path / "seeknal" / "transforms").mkdir(parents=True)
    (project_path / "seeknal" / "feature_groups").mkdir(parents=True)

    return project_path


@pytest.fixture
def sample_yaml_files(temp_project_path):
    """Create sample YAML files for testing."""
    source_yaml = """
kind: source
name: raw_users
description: Raw user data
source: csv
params:
  path: data/users.csv
"""
    (temp_project_path / "seeknal" / "sources" / "raw_users.yml").write_text(source_yaml)

    transform_yaml = """
kind: transform
name: clean_users
description: Clean user data
transform: SELECT * FROM source.raw_users
inputs:
  - ref: source.raw_users
"""
    (temp_project_path / "seeknal" / "transforms" / "clean_users.yml").write_text(transform_yaml)

    feature_group_yaml = """
kind: feature_group
name: user_features
description: User feature group
entity: user
features:
  user_id:
    dtype: integer
  name:
    dtype: string
inputs:
  - ref: transform.clean_users
"""
    (temp_project_path / "seeknal" / "feature_groups" / "user_features.yml").write_text(
        feature_group_yaml
    )

    return temp_project_path


@pytest.fixture
def project_with_target(sample_yaml_files):
    """Create a project with an existing target directory and manifest."""
    target_path = sample_yaml_files / "target"
    target_path.mkdir(parents=True, exist_ok=True)
    return sample_yaml_files


def _make_mock_dag_builder(nodes_dict=None, edges=None, parse_errors=None):
    """Create a mock DAGBuilder with configurable nodes.

    Args:
        nodes_dict: dict of node_id -> mock node. If None, a default set is created.
        edges: dict of node_id -> set of downstream node_ids.
        parse_errors: list of parse error strings.

    Returns:
        A MagicMock configured as a DAGBuilder.
    """
    if nodes_dict is None:
        # Build default nodes
        source_node = MagicMock()
        source_node.name = "raw_users"
        source_node.kind = MagicMock()
        source_node.kind.value = "source"
        source_node.yaml_data = {"kind": "source", "name": "raw_users"}
        source_node.file_path = "seeknal/sources/raw_users.yml"
        source_node.tags = []

        transform_node = MagicMock()
        transform_node.name = "clean_users"
        transform_node.kind = MagicMock()
        transform_node.kind.value = "transform"
        transform_node.yaml_data = {"kind": "transform", "name": "clean_users"}
        transform_node.file_path = "seeknal/transforms/clean_users.yml"
        transform_node.tags = []

        fg_node = MagicMock()
        fg_node.name = "user_features"
        fg_node.kind = MagicMock()
        fg_node.kind.value = "feature_group"
        fg_node.yaml_data = {"kind": "feature_group", "name": "user_features"}
        fg_node.file_path = "seeknal/feature_groups/user_features.yml"
        fg_node.tags = []

        nodes_dict = {
            "source.raw_users": source_node,
            "transform.clean_users": transform_node,
            "feature_group.user_features": fg_node,
        }

    if edges is None:
        edges = {
            "source.raw_users": {"transform.clean_users"},
            "transform.clean_users": {"feature_group.user_features"},
            "feature_group.user_features": set(),
        }

    mock_builder = MagicMock()
    mock_builder.nodes = nodes_dict
    mock_builder.build.return_value = None
    mock_builder.get_node_count.return_value = len(nodes_dict)
    mock_builder.get_edge_count.return_value = sum(len(v) for v in edges.values())
    mock_builder.topological_sort.return_value = list(nodes_dict.keys())
    mock_builder.get_parse_errors.return_value = parse_errors or []

    def _get_downstream(node_id):
        return edges.get(node_id, set())

    def _get_upstream(node_id):
        upstream = set()
        for nid, ds in edges.items():
            if node_id in ds:
                upstream.add(nid)
        return upstream

    def _get_all_downstream(node_id):
        result = set()
        queue = list(_get_downstream(node_id))
        while queue:
            n = queue.pop(0)
            if n not in result:
                result.add(n)
                queue.extend(_get_downstream(n))
        return result

    mock_builder.get_downstream = MagicMock(side_effect=_get_downstream)
    mock_builder.get_upstream = MagicMock(side_effect=_get_upstream)
    mock_builder.get_all_downstream = MagicMock(side_effect=_get_all_downstream)

    return mock_builder


# ---------------------------------------------------------------------------
# Test: seeknal plan (no env) — production mode
# ---------------------------------------------------------------------------

class TestPlanWithoutEnv:
    """Test `seeknal plan` without environment argument (production mode)."""

    def test_plan_builds_dag_and_saves_manifest(self, project_with_target, monkeypatch):
        """Plan builds DAG, creates manifest, and saves to target/manifest.json."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.state.load_state", return_value=None):
                with patch("seeknal.workflow.state.calculate_node_hash", return_value="hash123"):
                    with patch("seeknal.workflow.state.get_nodes_to_run", return_value={"source.raw_users"}):
                        result = runner.invoke(app, ["plan"])

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        assert "DAG built" in result.output
        assert "Manifest saved" in result.output
        assert "Execution Plan" in result.output

    def test_plan_shows_first_run_message(self, project_with_target, monkeypatch):
        """When no previous manifest exists, show 'first run' message."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.state.load_state", return_value=None):
                with patch("seeknal.workflow.state.calculate_node_hash", return_value="hash123"):
                    with patch("seeknal.workflow.state.get_nodes_to_run", return_value=set()):
                        result = runner.invoke(app, ["plan"])

        assert result.exit_code == 0
        assert "No previous manifest" in result.output or "first run" in result.output

    def test_plan_shows_node_summary(self, project_with_target, monkeypatch):
        """Plan should show node counts by type."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.state.load_state", return_value=None):
                with patch("seeknal.workflow.state.calculate_node_hash", return_value="hash123"):
                    with patch("seeknal.workflow.state.get_nodes_to_run", return_value=set()):
                        result = runner.invoke(app, ["plan"])

        assert result.exit_code == 0
        assert "Node Summary" in result.output
        assert "source" in result.output
        assert "transform" in result.output
        assert "feature_group" in result.output


class TestPlanIncremental:
    """Test `seeknal plan` with existing manifest showing cached vs changed nodes."""

    def test_plan_with_existing_manifest_shows_diff(self, project_with_target, monkeypatch):
        """When a manifest exists, plan should compare and show changes."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        # Create a previous manifest file
        from seeknal.dag.manifest import Manifest, Node, NodeType
        old_manifest = Manifest(project="test_project")
        old_manifest.add_node(Node(
            id="source.raw_users",
            name="raw_users",
            node_type=NodeType.SOURCE,
        ))
        target_path = project_with_target / "target"
        target_path.mkdir(parents=True, exist_ok=True)
        old_manifest.save(str(target_path / "manifest.json"))

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.state.load_state", return_value=None):
                with patch("seeknal.workflow.state.calculate_node_hash", return_value="hash123"):
                    with patch("seeknal.workflow.state.get_nodes_to_run", return_value={"transform.clean_users", "feature_group.user_features"}):
                        result = runner.invoke(app, ["plan"])

        assert result.exit_code == 0
        # Should show changes since manifest differs (added nodes)
        assert "Changes detected" in result.output or "No changes" in result.output

    def test_plan_shows_run_and_cached_status(self, project_with_target, monkeypatch):
        """Plan shows RUN for changed nodes and CACHED for unchanged nodes."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.state.load_state", return_value=None):
                with patch("seeknal.workflow.state.calculate_node_hash", return_value="hash123"):
                    with patch("seeknal.workflow.state.get_nodes_to_run", return_value={"source.raw_users", "transform.clean_users", "feature_group.user_features"}):
                        result = runner.invoke(app, ["plan"])

        assert result.exit_code == 0
        assert "RUN" in result.output
        assert "raw_users" in result.output


# ---------------------------------------------------------------------------
# Test: seeknal plan <env> — environment mode
# ---------------------------------------------------------------------------

class TestPlanWithEnv:
    """Test `seeknal plan dev` for environment planning."""

    def test_plan_env_creates_environment_plan(self, project_with_target, monkeypatch):
        """seeknal plan dev should delegate to EnvironmentManager.plan()."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        mock_env_plan = MagicMock()
        mock_env_plan.categorized_changes = {
            "source.raw_users": "non_breaking",
            "transform.clean_users": "non_breaking",
        }
        mock_env_plan.added_nodes = ["source.raw_users", "transform.clean_users"]
        mock_env_plan.removed_nodes = []
        mock_env_plan.total_nodes_to_execute = 2

        mock_manager = MagicMock()
        mock_manager.plan.return_value = mock_env_plan

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
                result = runner.invoke(app, ["plan", "dev"])

        assert result.exit_code == 0, f"Unexpected exit: {result.output}"
        assert "dev" in result.output
        assert "Environment Plan" in result.output
        assert "Plan saved" in result.output
        mock_manager.plan.assert_called_once()

    def test_plan_env_shows_breaking_changes(self, project_with_target, monkeypatch):
        """Plan with env should show BREAKING change count."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        mock_env_plan = MagicMock()
        mock_env_plan.categorized_changes = {
            "source.raw_users": "breaking",
        }
        mock_env_plan.added_nodes = []
        mock_env_plan.removed_nodes = []
        mock_env_plan.total_nodes_to_execute = 1

        mock_manager = MagicMock()
        mock_manager.plan.return_value = mock_env_plan

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
                result = runner.invoke(app, ["plan", "dev"])

        assert result.exit_code == 0
        assert "BREAKING" in result.output

    def test_plan_env_no_changes(self, project_with_target, monkeypatch):
        """Plan with no changes shows success message."""
        monkeypatch.chdir(project_with_target)

        mock_builder = _make_mock_dag_builder()

        mock_env_plan = MagicMock()
        mock_env_plan.categorized_changes = {}
        mock_env_plan.added_nodes = []
        mock_env_plan.removed_nodes = []
        mock_env_plan.total_nodes_to_execute = 0

        mock_manager = MagicMock()
        mock_manager.plan.return_value = mock_env_plan

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
                result = runner.invoke(app, ["plan", "dev"])

        assert result.exit_code == 0
        assert "No changes detected" in result.output


# ---------------------------------------------------------------------------
# Test: seeknal run --env
# ---------------------------------------------------------------------------

class TestRunWithEnv:
    """Test `seeknal run --env <name>` delegation to _run_in_environment."""

    def test_run_env_delegates_to_run_in_environment(self, tmp_path, monkeypatch):
        """seeknal run --env dev should delegate to _run_in_environment."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._run_in_environment") as mock_run_env:
            result = runner.invoke(app, ["run", "--env", "dev"])

        assert result.exit_code == 0
        mock_run_env.assert_called_once()
        call_kwargs = mock_run_env.call_args
        assert call_kwargs.kwargs["env_name"] == "dev"

    def test_run_env_with_parallel(self, tmp_path, monkeypatch):
        """--parallel flag passes through when using --env."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._run_in_environment") as mock_run_env:
            result = runner.invoke(app, ["run", "--env", "dev", "--parallel"])

        assert result.exit_code == 0
        call_kwargs = mock_run_env.call_args
        assert call_kwargs.kwargs["parallel"] is True

    def test_run_env_with_max_workers(self, tmp_path, monkeypatch):
        """--max-workers flag passes through when using --env."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._run_in_environment") as mock_run_env:
            result = runner.invoke(app, ["run", "--env", "dev", "--parallel", "--max-workers", "8"])

        assert result.exit_code == 0
        call_kwargs = mock_run_env.call_args
        assert call_kwargs.kwargs["max_workers"] == 8

    def test_run_env_with_continue_on_error(self, tmp_path, monkeypatch):
        """--continue-on-error flag passes through when using --env."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._run_in_environment") as mock_run_env:
            result = runner.invoke(app, ["run", "--env", "dev", "--continue-on-error"])

        assert result.exit_code == 0
        call_kwargs = mock_run_env.call_args
        assert call_kwargs.kwargs["continue_on_error"] is True

    def test_run_env_with_dry_run(self, tmp_path, monkeypatch):
        """--dry-run flag passes through when using --env."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._run_in_environment") as mock_run_env:
            result = runner.invoke(app, ["run", "--env", "dev", "--dry-run"])

        assert result.exit_code == 0
        call_kwargs = mock_run_env.call_args
        assert call_kwargs.kwargs["dry_run"] is True

    def test_run_env_with_show_plan(self, tmp_path, monkeypatch):
        """--show-plan flag passes through when using --env."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._run_in_environment") as mock_run_env:
            result = runner.invoke(app, ["run", "--env", "dev", "--show-plan"])

        assert result.exit_code == 0
        call_kwargs = mock_run_env.call_args
        assert call_kwargs.kwargs["show_plan"] is True


class TestRunWithoutEnv:
    """Test `seeknal run` without --env preserves existing behavior."""

    def test_run_without_env_calls_yaml_pipeline(self, sample_yaml_files, monkeypatch):
        """Without --env, seeknal run should call _run_yaml_pipeline."""
        monkeypatch.chdir(sample_yaml_files)

        # Use --show-plan to avoid actual execution
        result = runner.invoke(app, ["run", "--show-plan"])

        assert result.exit_code == 0
        assert "Execution Plan" in result.output

    def test_run_help_shows_env_option(self):
        """--help should document the --env option."""
        result = runner.invoke(app, ["run", "--help"])

        assert result.exit_code == 0
        assert "--env" in result.output


# ---------------------------------------------------------------------------
# Test: _run_in_environment internals
# ---------------------------------------------------------------------------

class TestRunInEnvironmentHelper:
    """Test the _run_in_environment shared helper function."""

    def test_run_in_env_no_plan_raises_error(self, tmp_path, monkeypatch):
        """Running in an env without a plan should report an error."""
        monkeypatch.chdir(tmp_path)

        mock_manager = MagicMock()
        mock_manager.apply.side_effect = ValueError(
            "No plan found for environment 'dev'. "
            "Run 'seeknal env plan dev' first."
        )

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            from seeknal.cli.main import _run_in_environment
            from click.exceptions import Exit as ClickExit
            with pytest.raises(ClickExit):
                _run_in_environment(
                    env_name="dev",
                    project_path=tmp_path,
                )

    def test_run_in_env_no_nodes_to_execute(self, tmp_path, monkeypatch):
        """When all changes are metadata-only, should show success without running."""
        monkeypatch.chdir(tmp_path)

        mock_manager = MagicMock()
        mock_manager.apply.return_value = {
            "nodes_to_execute": set(),
            "env_dir": tmp_path / "target" / "environments" / "dev",
            "manifest": MagicMock(),
            "plan": {},
        }

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            result = runner.invoke(app, ["run", "--env", "dev"])

        assert result.exit_code == 0
        assert "No nodes to execute" in result.output or "metadata" in result.output.lower()

    def test_run_in_env_show_plan_lists_nodes(self, tmp_path, monkeypatch):
        """--show-plan in env mode should list nodes to execute without running."""
        monkeypatch.chdir(tmp_path)

        mock_manifest = MagicMock()
        mock_node = MagicMock()
        mock_node.name = "clean_users"
        mock_manifest.get_node.return_value = mock_node

        mock_manager = MagicMock()
        mock_manager.apply.return_value = {
            "nodes_to_execute": {"transform.clean_users"},
            "env_dir": tmp_path / "target" / "environments" / "dev",
            "manifest": mock_manifest,
            "plan": {},
        }

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            result = runner.invoke(app, ["run", "--env", "dev", "--show-plan"])

        assert result.exit_code == 0
        assert "clean_users" in result.output


# ---------------------------------------------------------------------------
# Test: seeknal promote
# ---------------------------------------------------------------------------

class TestPromote:
    """Test `seeknal promote <env> [target]` command."""

    def test_promote_dev_to_prod(self, tmp_path, monkeypatch):
        """seeknal promote dev should promote to production (default target)."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._promote_environment") as mock_promote:
            result = runner.invoke(app, ["promote", "dev"])

        assert result.exit_code == 0
        mock_promote.assert_called_once()
        args = mock_promote.call_args
        assert args[0][0] == "dev"
        assert args[0][1] == "prod"

    def test_promote_with_explicit_target(self, tmp_path, monkeypatch):
        """seeknal promote staging prod should use explicit target."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._promote_environment") as mock_promote:
            result = runner.invoke(app, ["promote", "staging", "prod"])

        assert result.exit_code == 0
        mock_promote.assert_called_once()
        args = mock_promote.call_args
        assert args[0][0] == "staging"
        assert args[0][1] == "prod"

    def test_promote_env_to_env(self, tmp_path, monkeypatch):
        """seeknal promote dev staging should promote between environments."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._promote_environment") as mock_promote:
            result = runner.invoke(app, ["promote", "dev", "staging"])

        assert result.exit_code == 0
        args = mock_promote.call_args
        assert args[0][0] == "dev"
        assert args[0][1] == "staging"


class TestPromoteEnvironmentHelper:
    """Test the _promote_environment shared helper function."""

    def test_promote_asks_confirmation(self, tmp_path, monkeypatch):
        """Promote should ask for confirmation before proceeding."""
        monkeypatch.chdir(tmp_path)

        # Set up environment directory with a plan file
        env_dir = tmp_path / "target" / "environments" / "dev"
        env_dir.mkdir(parents=True)
        plan_data = {
            "categorized_changes": {"source.raw_users": "non_breaking"},
            "added_nodes": [],
            "removed_nodes": [],
        }
        (env_dir / "plan.json").write_text(json.dumps(plan_data))

        mock_manager = MagicMock()

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            # Deny confirmation with "n"
            result = runner.invoke(app, ["promote", "dev"], input="n\n")

        # Should abort because user said no
        assert result.exit_code == 1 or "Aborted" in result.output

    def test_promote_before_apply_raises_error(self, tmp_path, monkeypatch):
        """Promoting an environment that hasn't been applied should error."""
        monkeypatch.chdir(tmp_path)

        mock_manager = MagicMock()
        mock_manager._load_json.return_value = None
        mock_manager.promote.side_effect = ValueError(
            "Environment 'dev' has not been applied."
        )

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            # Confirm with "y"
            result = runner.invoke(app, ["promote", "dev"], input="y\n")

        assert result.exit_code == 1
        assert "failed" in result.output.lower() or "not been applied" in result.output.lower()

    def test_promote_success(self, tmp_path, monkeypatch):
        """Successful promotion shows success message."""
        monkeypatch.chdir(tmp_path)

        # Set up environment directory with plan and run_state
        env_dir = tmp_path / "target" / "environments" / "dev"
        env_dir.mkdir(parents=True)
        plan_data = {
            "categorized_changes": {"source.raw_users": "non_breaking"},
            "added_nodes": [],
            "removed_nodes": [],
        }
        (env_dir / "plan.json").write_text(json.dumps(plan_data))

        mock_manager = MagicMock()
        mock_manager._load_json.return_value = plan_data
        promote_result = {
            "promoted": True,
            "rematerialize_nodes": set(),
            "manifest": None,
            "from_env": "dev",
            "to_env": "prod",
            "profile_path": None,
            "changed_filenames": {"source_raw_users.parquet"},
            "warnings": [],
        }
        mock_manager.promote.return_value = promote_result

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            result = runner.invoke(app, ["promote", "dev"], input="y\n")

        assert result.exit_code == 0
        assert "promoted" in result.output.lower()


# ---------------------------------------------------------------------------
# Test: Backward compatibility — seeknal env plan/apply/promote
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    """Verify that seeknal env plan/apply/promote still work."""

    def test_env_plan_still_works(self, tmp_path, monkeypatch):
        """seeknal env plan dev should still work (backward compatible)."""
        monkeypatch.chdir(tmp_path)

        mock_builder = _make_mock_dag_builder()

        mock_env_plan = MagicMock()
        mock_env_plan.categorized_changes = {"source.raw_users": "non_breaking"}
        mock_env_plan.added_nodes = ["source.raw_users"]
        mock_env_plan.removed_nodes = []
        mock_env_plan.total_nodes_to_execute = 1

        mock_manager = MagicMock()
        mock_manager.plan.return_value = mock_env_plan

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
                result = runner.invoke(app, ["env", "plan", "dev"])

        assert result.exit_code == 0
        assert "Plan saved" in result.output

    def test_env_apply_still_works(self, tmp_path, monkeypatch):
        """seeknal env apply dev should still delegate to _run_in_environment."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._run_in_environment") as mock_run_env:
            result = runner.invoke(app, ["env", "apply", "dev"])

        assert result.exit_code == 0
        mock_run_env.assert_called_once()

    def test_env_promote_still_works(self, tmp_path, monkeypatch):
        """seeknal env promote dev should still delegate to _promote_environment."""
        monkeypatch.chdir(tmp_path)

        with patch("seeknal.cli.main._promote_environment") as mock_promote:
            result = runner.invoke(app, ["env", "promote", "dev"])

        assert result.exit_code == 0
        mock_promote.assert_called_once()

    def test_env_list_command_exists(self, tmp_path, monkeypatch):
        """seeknal env list should still work."""
        monkeypatch.chdir(tmp_path)

        mock_manager = MagicMock()
        mock_manager.list_environments.return_value = []

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            result = runner.invoke(app, ["env", "list"])

        assert result.exit_code == 0
        assert "No environments found" in result.output

    def test_env_delete_command_exists(self, tmp_path, monkeypatch):
        """seeknal env delete dev should still work."""
        monkeypatch.chdir(tmp_path)

        mock_manager = MagicMock()

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            result = runner.invoke(app, ["env", "delete", "dev"], input="y\n")

        assert result.exit_code == 0
        mock_manager.delete_environment.assert_called_once_with("dev")


# ---------------------------------------------------------------------------
# Test: Auto-parallel hint
# ---------------------------------------------------------------------------

class TestAutoParallelHint:
    """Test auto-parallel suggestion when layers have >3 nodes."""

    def test_parallel_hint_shown_when_wide_layer(self, tmp_path, monkeypatch):
        """When a topological layer has >3 independent nodes, show parallel hint."""
        monkeypatch.chdir(tmp_path)

        # Create 5 independent source nodes (all in layer 0, no deps)
        nodes = {}
        edges = {}
        for i in range(5):
            node = MagicMock()
            node.name = f"source_{i}"
            node.kind = MagicMock()
            node.kind.value = "source"
            node.yaml_data = {"kind": "source", "name": f"source_{i}"}
            node.file_path = f"seeknal/sources/source_{i}.yml"
            node.tags = []
            nodes[f"source.source_{i}"] = node
            edges[f"source.source_{i}"] = set()

        mock_builder = _make_mock_dag_builder(nodes_dict=nodes, edges=edges)

        # Need to create the seeknal directory structure
        (tmp_path / "seeknal" / "sources").mkdir(parents=True)
        for i in range(5):
            (tmp_path / "seeknal" / "sources" / f"source_{i}.yml").write_text(
                f"kind: source\nname: source_{i}\nsource: csv\nparams:\n  path: data/s{i}.csv\n"
            )

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.state.load_state", return_value=None):
                with patch("seeknal.workflow.state.calculate_node_hash", return_value="hash"):
                    with patch("seeknal.workflow.state.get_nodes_to_run", return_value=set(nodes.keys())):
                        with patch("seeknal.workflow.state.include_upstream_sources", side_effect=lambda x, _: x):
                            with patch("seeknal.workflow.executors.get_executor") as mock_exec:
                                mock_result = MagicMock()
                                mock_result.is_success.return_value = True
                                mock_result.row_count = 10
                                mock_result.metadata = {}
                                mock_exec.return_value.run.return_value = mock_result

                                result = runner.invoke(app, ["run"])

        assert result.exit_code == 0
        assert "Tip" in result.output
        assert "--parallel" in result.output

    def test_parallel_hint_not_shown_when_parallel_flag_set(self, tmp_path, monkeypatch):
        """When --parallel is already set, no hint should appear."""
        monkeypatch.chdir(tmp_path)

        # Create 5 independent source nodes
        nodes = {}
        edges = {}
        for i in range(5):
            node = MagicMock()
            node.name = f"source_{i}"
            node.kind = MagicMock()
            node.kind.value = "source"
            node.yaml_data = {"kind": "source", "name": f"source_{i}"}
            node.file_path = f"seeknal/sources/source_{i}.yml"
            node.tags = []
            nodes[f"source.source_{i}"] = node
            edges[f"source.source_{i}"] = set()

        mock_builder = _make_mock_dag_builder(nodes_dict=nodes, edges=edges)

        (tmp_path / "seeknal" / "sources").mkdir(parents=True)
        for i in range(5):
            (tmp_path / "seeknal" / "sources" / f"source_{i}.yml").write_text(
                f"kind: source\nname: source_{i}\nsource: csv\nparams:\n  path: data/s{i}.csv\n"
            )

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            with patch("seeknal.workflow.state.load_state", return_value=None):
                with patch("seeknal.workflow.state.calculate_node_hash", return_value="hash"):
                    with patch("seeknal.workflow.state.get_nodes_to_run", return_value=set(nodes.keys())):
                        with patch("seeknal.workflow.state.include_upstream_sources", side_effect=lambda x, _: x):
                            with patch("seeknal.workflow.parallel.ParallelDAGRunner") as mock_prunner:
                                with patch("seeknal.workflow.parallel.print_parallel_summary"):
                                    with patch("seeknal.workflow.runner.DAGRunner"):
                                        mock_summary = MagicMock()
                                        mock_prunner.return_value.run.return_value = mock_summary

                                        result = runner.invoke(app, ["run", "--parallel"])

        # The hint text should NOT appear when --parallel is set
        assert "Tip" not in result.output

    def test_parallel_hint_not_shown_when_small_layer(self, sample_yaml_files, monkeypatch):
        """When no layer exceeds 3 nodes, no parallel hint should appear."""
        monkeypatch.chdir(sample_yaml_files)

        # Default sample files have 3 nodes in a chain (each layer = 1 node)
        result = runner.invoke(app, ["run", "--show-plan"])

        assert result.exit_code == 0
        assert "Tip" not in result.output


# ---------------------------------------------------------------------------
# Test: seeknal init output shows unified workflow
# ---------------------------------------------------------------------------

class TestInitOutput:
    """Test that init command shows unified workflow steps."""

    def test_init_shows_plan_step(self, tmp_path, monkeypatch):
        """Init output should include 'seeknal plan' in next steps."""
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["init", "--name", "test_proj"])

        assert result.exit_code == 0
        assert "seeknal plan" in result.output

    def test_init_shows_run_step(self, tmp_path, monkeypatch):
        """Init output should include 'seeknal run' in next steps."""
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["init", "--name", "test_proj"])

        assert result.exit_code == 0
        assert "seeknal run" in result.output

    def test_init_shows_env_workflow(self, tmp_path, monkeypatch):
        """Init output should include environment workflow (plan dev, run --env, promote)."""
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["init", "--name", "test_proj"])

        assert result.exit_code == 0
        assert "seeknal plan dev" in result.output or "--env dev" in result.output
        assert "seeknal promote" in result.output

    def test_init_shows_parallel_option(self, tmp_path, monkeypatch):
        """Init output should mention parallel execution."""
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["init", "--name", "test_proj"])

        assert result.exit_code == 0
        assert "--parallel" in result.output


# ---------------------------------------------------------------------------
# Test: Error cases
# ---------------------------------------------------------------------------

class TestErrorCases:
    """Test various error scenarios."""

    def test_plan_dag_build_failure(self, tmp_path, monkeypatch):
        """Plan should exit with error when DAG build fails."""
        monkeypatch.chdir(tmp_path)

        # No seeknal directory means no YAML files
        (tmp_path / "seeknal" / "sources").mkdir(parents=True)

        result = runner.invoke(app, ["plan"])

        # Should fail due to no YAML files
        assert result.exit_code == 1

    def test_plan_cycle_detection(self, tmp_path, monkeypatch):
        """Plan should detect cycles in DAG."""
        monkeypatch.chdir(tmp_path)

        from seeknal.workflow.dag import CycleDetectedError

        mock_builder = MagicMock()
        mock_builder.build.side_effect = CycleDetectedError(
            ["a", "b", "a"], "Cycle detected: a -> b -> a"
        )

        with patch("seeknal.workflow.dag.DAGBuilder", return_value=mock_builder):
            result = runner.invoke(app, ["plan"])

        assert result.exit_code == 1
        assert "Cycle detected" in result.output

    def test_run_env_without_plan(self, tmp_path, monkeypatch):
        """seeknal run --env dev without a plan should error gracefully."""
        monkeypatch.chdir(tmp_path)

        mock_manager = MagicMock()
        mock_manager.apply.side_effect = ValueError(
            "No plan found for environment 'dev'. "
            "Run 'seeknal env plan dev' first."
        )

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            result = runner.invoke(app, ["run", "--env", "dev"])

        # The error should be caught and displayed
        assert result.exit_code == 1 or "No plan found" in result.output

    def test_promote_nonexistent_environment(self, tmp_path, monkeypatch):
        """Promoting a nonexistent environment should error."""
        monkeypatch.chdir(tmp_path)

        mock_manager = MagicMock()
        mock_manager._load_json.return_value = None
        mock_manager.promote.side_effect = ValueError(
            "Environment 'nonexistent' not found"
        )

        with patch("seeknal.workflow.environment.EnvironmentManager", return_value=mock_manager):
            result = runner.invoke(app, ["promote", "nonexistent"], input="y\n")

        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Test: Help documentation
# ---------------------------------------------------------------------------

class TestHelpDocumentation:
    """Test that new commands appear in help output."""

    def test_plan_help(self):
        """seeknal plan --help should show usage."""
        result = runner.invoke(app, ["plan", "--help"])

        assert result.exit_code == 0
        assert "plan" in result.output.lower()
        assert "environment" in result.output.lower() or "env" in result.output.lower()

    def test_promote_help(self):
        """seeknal promote --help should show usage."""
        result = runner.invoke(app, ["promote", "--help"])

        assert result.exit_code == 0
        assert "promote" in result.output.lower()

    def test_run_help_includes_env(self):
        """seeknal run --help should document --env."""
        result = runner.invoke(app, ["run", "--help"])

        assert result.exit_code == 0
        assert "--env" in result.output

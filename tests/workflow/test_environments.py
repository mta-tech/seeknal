"""Tests for virtual environment management."""

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType, Edge
from seeknal.dag.diff import ChangeCategory
from seeknal.workflow.environment import (
    EnvironmentManager,
    EnvironmentConfig,
    EnvironmentPlan,
    EnvironmentRef,
)


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


def _save_production_manifest(target_path: Path, manifest: Manifest):
    """Save a manifest as the production manifest."""
    target_path.mkdir(parents=True, exist_ok=True)
    manifest.save(str(target_path / "manifest.json"))


# =============================================================================
# Plan tests
# =============================================================================


class TestPlanNoProduction:
    """Plan when there is no production manifest (first run)."""

    def test_all_nodes_as_non_breaking(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        node_a = _make_node("users")
        node_b = _make_node("clean_users", NodeType.TRANSFORM)
        manifest = _make_manifest(node_a, node_b)

        plan = manager.plan("dev", manifest)

        assert plan.env_name == "dev"
        assert len(plan.categorized_changes) == 2
        for cat in plan.categorized_changes.values():
            assert cat == ChangeCategory.NON_BREAKING.value

    def test_all_nodes_listed_as_added(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        node_a = _make_node("users")
        manifest = _make_manifest(node_a)

        plan = manager.plan("dev", manifest)

        assert "source.users" in plan.added_nodes
        assert plan.removed_nodes == []

    def test_total_nodes_to_execute(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        nodes = [_make_node(f"n{i}") for i in range(5)]
        manifest = _make_manifest(*nodes)

        plan = manager.plan("dev", manifest)

        assert plan.total_nodes_to_execute == 5


class TestPlanWithProduction:
    """Plan when a production manifest already exists."""

    def test_breaking_change_detected(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        # Production: node with columns
        old_node = _make_node("users", columns={"id": "INT", "name": "VARCHAR"})
        old_manifest = _make_manifest(old_node)
        _save_production_manifest(target, old_manifest)

        # New: column removed (BREAKING)
        new_node = _make_node("users", columns={"id": "INT"})
        new_manifest = _make_manifest(new_node)

        plan = manager.plan("dev", new_manifest)

        assert "source.users" in plan.categorized_changes
        assert plan.categorized_changes["source.users"] == ChangeCategory.BREAKING.value

    def test_metadata_change_no_execution(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        # Production
        old_node = _make_node("users", description="old desc")
        old_manifest = _make_manifest(old_node)
        _save_production_manifest(target, old_manifest)

        # New: only description changed (METADATA)
        new_node = _make_node("users", description="new desc")
        new_manifest = _make_manifest(new_node)

        plan = manager.plan("dev", new_manifest)

        assert plan.categorized_changes.get("source.users") == ChangeCategory.METADATA.value
        assert plan.total_nodes_to_execute == 0

    def test_non_breaking_change(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        old_node = _make_node("users", config={"sql": "SELECT 1"})
        old_manifest = _make_manifest(old_node)
        _save_production_manifest(target, old_manifest)

        new_node = _make_node("users", config={"sql": "SELECT 2"})
        new_manifest = _make_manifest(new_node)

        plan = manager.plan("dev", new_manifest)

        assert plan.categorized_changes.get("source.users") == ChangeCategory.NON_BREAKING.value

    def test_added_node_in_plan(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        old_node = _make_node("users")
        old_manifest = _make_manifest(old_node)
        _save_production_manifest(target, old_manifest)

        new_node_a = _make_node("users")
        new_node_b = _make_node("orders")
        new_manifest = _make_manifest(new_node_a, new_node_b)

        plan = manager.plan("dev", new_manifest)

        assert "source.orders" in plan.added_nodes

    def test_removed_node_in_plan(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        old_a = _make_node("users")
        old_b = _make_node("orders")
        old_manifest = _make_manifest(old_a, old_b)
        _save_production_manifest(target, old_manifest)

        new_a = _make_node("users")
        new_manifest = _make_manifest(new_a)

        plan = manager.plan("dev", new_manifest)

        assert "source.orders" in plan.removed_nodes


class TestPlanPersistence:
    """Plan saves to the correct directory structure."""

    def test_plan_saved_to_env_dir(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))

        manager.plan("dev", manifest)

        env_dir = target / "environments" / "dev"
        assert (env_dir / "plan.json").exists()
        assert (env_dir / "manifest.json").exists()
        assert (env_dir / "env_config.json").exists()

    def test_replan_overwrites_previous_plan(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        manifest_v1 = _make_manifest(_make_node("users"))
        plan_v1 = manager.plan("dev", manifest_v1)

        manifest_v2 = _make_manifest(_make_node("users"), _make_node("orders"))
        plan_v2 = manager.plan("dev", manifest_v2)

        assert plan_v2.total_nodes_to_execute == 2
        # Fingerprint should differ
        assert plan_v1.manifest_fingerprint != plan_v2.manifest_fingerprint

    def test_env_config_preserves_created_at_on_replan(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)

        env_dir = target / "environments" / "dev"
        with open(env_dir / "env_config.json") as f:
            config_1 = json.load(f)

        # Re-plan
        manager.plan("dev", manifest)
        with open(env_dir / "env_config.json") as f:
            config_2 = json.load(f)

        assert config_2["created_at"] == config_1["created_at"]


# =============================================================================
# Apply tests
# =============================================================================


class TestApply:
    """Apply validates plan and returns execution info."""

    def test_apply_with_valid_plan(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)

        result = manager.apply("dev")

        assert "plan" in result
        assert "manifest" in result
        assert "env_dir" in result
        assert "nodes_to_execute" in result
        assert "source.users" in result["nodes_to_execute"]

    def test_apply_stale_plan_raises(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)

        # Tamper with the manifest in the env dir to make plan stale
        env_dir = target / "environments" / "dev"
        new_manifest = _make_manifest(_make_node("users"), _make_node("orders"))
        with open(env_dir / "manifest.json", "w") as f:
            json.dump(new_manifest.to_dict(), f)

        with pytest.raises(ValueError, match="stale"):
            manager.apply("dev")

    def test_apply_stale_plan_force(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)

        # Tamper with manifest
        env_dir = target / "environments" / "dev"
        new_manifest = _make_manifest(_make_node("users"), _make_node("orders"))
        with open(env_dir / "manifest.json", "w") as f:
            json.dump(new_manifest.to_dict(), f)

        # Force should succeed
        result = manager.apply("dev", force=True)
        assert result is not None

    def test_apply_without_plan_raises(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        # Create env dir without plan
        env_dir = target / "environments" / "dev"
        env_dir.mkdir(parents=True)

        with pytest.raises(ValueError, match="No plan found"):
            manager.apply("dev")

    def test_apply_nonexistent_env_raises(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        with pytest.raises(ValueError, match="not found"):
            manager.apply("nonexistent")

    def test_apply_excludes_metadata_nodes(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        # Production manifest
        old_node = _make_node("users", description="old")
        old_manifest = _make_manifest(old_node)
        _save_production_manifest(target, old_manifest)

        # New: metadata-only change
        new_node = _make_node("users", description="new")
        new_manifest = _make_manifest(new_node)

        manager.plan("dev", new_manifest)
        result = manager.apply("dev")

        assert len(result["nodes_to_execute"]) == 0


# =============================================================================
# Promote tests
# =============================================================================


class TestPromote:
    """Promote copies environment outputs to production or another env."""

    def _setup_applied_env(self, target: Path, env_name: str = "dev"):
        """Create a fully applied environment for promote tests."""
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        manager.plan(env_name, manifest)

        env_dir = target / "environments" / env_name
        # Simulate that apply has run by creating run_state.json and cache
        run_state = {"status": "completed", "nodes_executed": 1}
        with open(env_dir / "run_state.json", "w") as f:
            json.dump(run_state, f)

        cache_dir = env_dir / "cache" / "source"
        cache_dir.mkdir(parents=True)
        (cache_dir / "users.parquet").write_text("fake parquet data")

        return manager

    def test_promote_copies_state_to_production(self, tmp_path):
        target = tmp_path / "target"
        manager = self._setup_applied_env(target)

        manager.promote("dev", "prod")

        assert (target / "run_state.json").exists()

    def test_promote_copies_cache_to_production(self, tmp_path):
        target = tmp_path / "target"
        manager = self._setup_applied_env(target)

        manager.promote("dev", "prod")

        assert (target / "cache" / "source" / "users.parquet").exists()

    def test_promote_copies_manifest_to_production(self, tmp_path):
        target = tmp_path / "target"
        manager = self._setup_applied_env(target)

        manager.promote("dev", "prod")

        assert (target / "manifest.json").exists()

    def test_promote_without_apply_raises(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)

        with pytest.raises(ValueError, match="has not been applied"):
            manager.promote("dev", "prod")

    def test_promote_nonexistent_env_raises(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        with pytest.raises(ValueError, match="not found"):
            manager.promote("nonexistent", "prod")

    def test_promote_copies_intermediate_to_production(self, tmp_path):
        """Promote must copy env intermediate/ outputs for changed nodes."""
        target = tmp_path / "target"
        manager = self._setup_applied_env(target)

        # Simulate env apply output in intermediate/ (matches source.users node)
        env_intermediate = target / "environments" / "dev" / "intermediate"
        env_intermediate.mkdir(parents=True)
        (env_intermediate / "source_users.parquet").write_text("new schema")

        # Production has old version
        prod_intermediate = target / "intermediate"
        prod_intermediate.mkdir(parents=True)
        (prod_intermediate / "source_users.parquet").write_text("old schema")

        manager.promote("dev", "prod")

        promoted = (prod_intermediate / "source_users.parquet").read_text()
        assert promoted == "new schema"

    def test_promote_to_another_env(self, tmp_path):
        target = tmp_path / "target"
        manager = self._setup_applied_env(target, "dev")

        manager.promote("dev", "staging")

        staging_dir = target / "environments" / "staging"
        assert (staging_dir / "run_state.json").exists()
        assert (staging_dir / "cache" / "source" / "users.parquet").exists()
        assert (staging_dir / "manifest.json").exists()

    def test_promote_to_another_env_copies_intermediate(self, tmp_path):
        """Promote to env must also copy intermediate/ outputs for changed nodes."""
        target = tmp_path / "target"
        manager = self._setup_applied_env(target, "dev")

        # Must match the changed node (source.users -> source_users.parquet)
        env_intermediate = target / "environments" / "dev" / "intermediate"
        env_intermediate.mkdir(parents=True)
        (env_intermediate / "source_users.parquet").write_text("env data")

        manager.promote("dev", "staging")

        staging_intermediate = target / "environments" / "staging" / "intermediate"
        assert (staging_intermediate / "source_users.parquet").exists()

    def test_promote_only_copies_changed_nodes(self, tmp_path):
        """Promote must not overwrite production files for unchanged nodes."""
        target = tmp_path / "target"
        manager = self._setup_applied_env(target)

        # Production has files for both changed and unchanged nodes
        prod_intermediate = target / "intermediate"
        prod_intermediate.mkdir(parents=True)
        (prod_intermediate / "source_users.parquet").write_text("old users")
        (prod_intermediate / "transform_orders.parquet").write_text("prod orders")

        # Env intermediate has changed users + copied ref for orders
        env_intermediate = target / "environments" / "dev" / "intermediate"
        env_intermediate.mkdir(parents=True)
        (env_intermediate / "source_users.parquet").write_text("new users")
        (env_intermediate / "transform_orders.parquet").write_text("stale copy")

        manager.promote("dev", "prod")

        # Changed node was updated
        assert (prod_intermediate / "source_users.parquet").read_text() == "new users"
        # Unchanged node was NOT overwritten (still has production data)
        assert (prod_intermediate / "transform_orders.parquet").read_text() == "prod orders"

    def test_promote_dry_run_returns_changed_filenames(self, tmp_path):
        """Dry run returns the set of files that would be promoted."""
        target = tmp_path / "target"
        manager = self._setup_applied_env(target)

        result = manager.promote("dev", "prod", dry_run=True)

        assert not result["promoted"]
        assert "source_users.parquet" in result["changed_filenames"]

    def test_promote_detects_production_drift(self, tmp_path):
        """Promote warns when production files changed since plan was created."""
        target = tmp_path / "target"

        # Create production outputs for both nodes
        prod_intermediate = target / "intermediate"
        prod_intermediate.mkdir(parents=True)
        (prod_intermediate / "source_users.parquet").write_text("users data")
        orders_file = prod_intermediate / "source_orders.parquet"
        orders_file.write_text("original orders data")

        # Production manifest has both users and orders
        manager = EnvironmentManager(target)
        users_node = _make_node("users")
        orders_node = _make_node("orders")
        prod_manifest = _make_manifest(users_node, orders_node)
        manager._save_json(target / "manifest.json", prod_manifest.to_dict())

        # Plan changes only "users" — "orders" stays unchanged and gets a ref
        new_users = _make_node("users", config={"columns": {"extra": "int"}})
        new_manifest = _make_manifest(new_users, orders_node)
        manager.plan("dev", new_manifest)

        # Simulate apply
        env_dir = target / "environments" / "dev"
        with open(env_dir / "run_state.json", "w") as f:
            json.dump({"status": "completed"}, f)
        env_cache = env_dir / "cache" / "source"
        env_cache.mkdir(parents=True)
        (env_cache / "users.parquet").write_text("env data")

        # Modify the UNCHANGED node's production file (simulate concurrent run)
        orders_file.write_text("someone ran seeknal run and changed orders")

        result = manager.promote("dev", "prod", dry_run=True)

        assert any("Production output changed" in w for w in result["warnings"])

    def test_promote_merges_run_state(self, tmp_path):
        """Promote merges env run_state into production, preserving unchanged nodes."""
        target = tmp_path / "target"
        manager = self._setup_applied_env(target)

        # Production has existing run state with other nodes
        prod_state = {
            "nodes": {"transform.orders": {"status": "success"}},
            "last_run": "2026-01-01T00:00:00",
        }
        with open(target / "run_state.json", "w") as f:
            json.dump(prod_state, f)

        # Env run state has only the changed node
        env_state = {
            "nodes": {"source.users": {"status": "success"}},
            "last_run": "2026-02-01T00:00:00",
        }
        env_dir = target / "environments" / "dev"
        with open(env_dir / "run_state.json", "w") as f:
            json.dump(env_state, f)

        manager.promote("dev", "prod")

        with open(target / "run_state.json") as f:
            merged = json.load(f)

        # Both nodes preserved
        assert "transform.orders" in merged["nodes"]
        assert "source.users" in merged["nodes"]
        # Timestamp updated
        assert merged["last_run"] == "2026-02-01T00:00:00"


# =============================================================================
# List tests
# =============================================================================


class TestListEnvironments:
    """List all environments."""

    def test_list_empty(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        envs = manager.list_environments()
        assert envs == []

    def test_list_with_environments(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)
        manager.plan("staging", manifest)

        envs = manager.list_environments()
        names = [e.name for e in envs]
        assert "dev" in names
        assert "staging" in names
        assert len(envs) == 2

    def test_list_returns_correct_config(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)

        envs = manager.list_environments()
        assert len(envs) == 1
        assert envs[0].name == "dev"
        assert envs[0].created_at is not None
        assert envs[0].last_accessed is not None


# =============================================================================
# Delete tests
# =============================================================================


class TestDeleteEnvironment:
    """Delete environment directories."""

    def test_delete_removes_directory(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        manifest = _make_manifest(_make_node("users"))
        manager.plan("dev", manifest)

        assert (target / "environments" / "dev").exists()

        manager.delete_environment("dev")

        assert not (target / "environments" / "dev").exists()

    def test_delete_nonexistent_raises(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        with pytest.raises(ValueError, match="not found"):
            manager.delete_environment("ghost")


# =============================================================================
# TTL / Cleanup tests
# =============================================================================


class TestTTLAndCleanup:
    """TTL expiration and cleanup."""

    def test_expired_environment_detected(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        manager.plan("old_env", manifest)

        # Patch the config to have expired timestamp
        config_path = target / "environments" / "old_env" / "env_config.json"
        with open(config_path) as f:
            config = json.load(f)
        # Set last_accessed to 8 days ago (> 7 day TTL)
        expired_time = time.time() - (8 * 86400)
        config["last_accessed"] = time.strftime(
            "%Y-%m-%dT%H:%M:%S", time.gmtime(expired_time)
        )
        with open(config_path, "w") as f:
            json.dump(config, f)

        envs = manager.list_environments()
        assert len(envs) == 1
        assert envs[0].is_expired()

    def test_non_expired_not_deleted(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))
        manager.plan("fresh_env", manifest)

        deleted = manager.cleanup_expired()
        assert deleted == []
        assert (target / "environments" / "fresh_env").exists()

    def test_cleanup_removes_expired(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)
        manifest = _make_manifest(_make_node("users"))

        # Create two envs
        manager.plan("expired_env", manifest)
        manager.plan("fresh_env", manifest)

        # Expire one
        config_path = target / "environments" / "expired_env" / "env_config.json"
        with open(config_path) as f:
            config = json.load(f)
        expired_time = time.time() - (8 * 86400)
        config["last_accessed"] = time.strftime(
            "%Y-%m-%dT%H:%M:%S", time.gmtime(expired_time)
        )
        with open(config_path, "w") as f:
            json.dump(config, f)

        deleted = manager.cleanup_expired()
        assert "expired_env" in deleted
        assert "fresh_env" not in deleted
        assert not (target / "environments" / "expired_env").exists()
        assert (target / "environments" / "fresh_env").exists()


# =============================================================================
# Production references tests
# =============================================================================


class TestProductionRefs:
    """Unchanged nodes should get production references."""

    def test_unchanged_node_gets_ref(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        # Production manifest with cached output
        node_a = _make_node("users")
        node_b = _make_node("orders")
        old_manifest = _make_manifest(node_a, node_b)
        _save_production_manifest(target, old_manifest)

        # Create a fake cached file for node_a
        cache_dir = target / "cache" / "source"
        cache_dir.mkdir(parents=True)
        (cache_dir / "users.parquet").write_bytes(b"fake parquet")

        # New manifest: only orders changed (config)
        new_node_a = _make_node("users")
        new_node_b = _make_node("orders", config={"sql": "SELECT *"})
        new_manifest = _make_manifest(new_node_a, new_node_b)

        manager.plan("dev", new_manifest)

        # Check refs.json
        refs_data = json.loads(
            (target / "environments" / "dev" / "refs.json").read_text()
        )
        ref_ids = [r["node_id"] for r in refs_data["refs"]]
        assert "source.users" in ref_ids

    def test_changed_node_has_no_ref(self, tmp_path):
        target = tmp_path / "target"
        manager = EnvironmentManager(target)

        node_a = _make_node("users", config={"sql": "SELECT 1"})
        old_manifest = _make_manifest(node_a)
        _save_production_manifest(target, old_manifest)

        cache_dir = target / "cache" / "source"
        cache_dir.mkdir(parents=True)
        (cache_dir / "users.parquet").write_bytes(b"fake")

        new_node_a = _make_node("users", config={"sql": "SELECT 2"})
        new_manifest = _make_manifest(new_node_a)

        manager.plan("dev", new_manifest)

        refs_data = json.loads(
            (target / "environments" / "dev" / "refs.json").read_text()
        )
        ref_ids = [r["node_id"] for r in refs_data["refs"]]
        assert "source.users" not in ref_ids

"""
Tests for seeknal diff engine.

Tests baseline resolution, diff generation, change classification,
and input sanitization.
"""

import json
import pytest
from unittest.mock import patch

from seeknal.dag.diff import ChangeCategory
from seeknal.workflow.diff_engine import (
    DiffEngine,
    _sanitize_node_identifier,
    load_applied_state,
    write_applied_state_entry,
)


# --- Fixtures ---


@pytest.fixture
def project_path(tmp_path):
    """Create a project structure with seeknal directories."""
    project = tmp_path / "test_project"
    project.mkdir()
    (project / "seeknal" / "sources").mkdir(parents=True)
    (project / "seeknal" / "transforms").mkdir(parents=True)
    (project / "seeknal" / "feature_groups").mkdir(parents=True)
    (project / "target").mkdir(parents=True)
    return project


@pytest.fixture
def source_yaml():
    return """kind: source
name: orders
description: Order data
columns:
  order_id: integer
  order_date: string
  customer_id: integer
"""


@pytest.fixture
def modified_source_yaml():
    return """kind: source
name: orders
description: Order data
columns:
  order_id: integer
  order_date: date
  customer_id: integer
  discount_amount: float
"""


@pytest.fixture
def project_with_source(project_path, source_yaml):
    """Project with an orders source file."""
    (project_path / "seeknal" / "sources" / "orders.yml").write_text(source_yaml)
    return project_path


@pytest.fixture
def project_with_applied_state(project_with_source, source_yaml):
    """Project with source file AND applied_state.json baseline."""
    write_applied_state_entry(
        project_with_source,
        "source.orders",
        "seeknal/sources/orders.yml",
        source_yaml,
    )
    return project_with_source


# --- Input Sanitization Tests ---


class TestSanitizeNodeIdentifier:
    def test_valid_identifier(self):
        assert _sanitize_node_identifier("sources/orders") == "sources/orders"

    def test_valid_with_underscores(self):
        assert _sanitize_node_identifier("feature_groups/user_features") == "feature_groups/user_features"

    def test_rejects_path_traversal(self):
        with pytest.raises(ValueError, match="path traversal"):
            _sanitize_node_identifier("../sources/orders")

    def test_rejects_backslash_traversal(self):
        with pytest.raises(ValueError, match="path traversal"):
            _sanitize_node_identifier("sources\\..\\orders")

    def test_rejects_invalid_format(self):
        with pytest.raises(ValueError, match="expected format"):
            _sanitize_node_identifier("orders")

    def test_rejects_nested_paths(self):
        with pytest.raises(ValueError, match="expected format"):
            _sanitize_node_identifier("sources/sub/orders")

    def test_strips_leading_slash(self):
        assert _sanitize_node_identifier("/sources/orders") == "sources/orders"


# --- Applied State Tests ---


class TestAppliedState:
    def test_load_nonexistent(self, project_path):
        state = load_applied_state(project_path)
        assert state == {}

    def test_write_and_load(self, project_path):
        write_applied_state_entry(
            project_path, "source.orders", "seeknal/sources/orders.yml", "content"
        )
        state = load_applied_state(project_path)
        assert "nodes" in state
        assert "source.orders" in state["nodes"]
        assert state["nodes"]["source.orders"]["yaml_content"] == "content"

    def test_incremental_update(self, project_path):
        write_applied_state_entry(
            project_path, "source.orders", "seeknal/sources/orders.yml", "v1"
        )
        write_applied_state_entry(
            project_path, "source.customers", "seeknal/sources/customers.yml", "v2"
        )
        state = load_applied_state(project_path)
        assert len(state["nodes"]) == 2
        assert state["nodes"]["source.orders"]["yaml_content"] == "v1"
        assert state["nodes"]["source.customers"]["yaml_content"] == "v2"

    def test_overwrite_existing_node(self, project_path):
        write_applied_state_entry(
            project_path, "source.orders", "seeknal/sources/orders.yml", "v1"
        )
        write_applied_state_entry(
            project_path, "source.orders", "seeknal/sources/orders.yml", "v2"
        )
        state = load_applied_state(project_path)
        assert state["nodes"]["source.orders"]["yaml_content"] == "v2"

    def test_schema_version_validated(self, project_path):
        # Write invalid schema version
        state_path = project_path / "target" / "applied_state.json"
        state_path.write_text(json.dumps({"schema_version": "999", "nodes": {}}))
        state = load_applied_state(project_path)
        assert state == {}

    def test_corrupted_json(self, project_path):
        state_path = project_path / "target" / "applied_state.json"
        state_path.write_text("not json{{{")
        state = load_applied_state(project_path)
        assert state == {}

    def test_content_hash_computed(self, project_path):
        write_applied_state_entry(
            project_path, "source.orders", "seeknal/sources/orders.yml", "test"
        )
        state = load_applied_state(project_path)
        assert "content_hash" in state["nodes"]["source.orders"]
        assert len(state["nodes"]["source.orders"]["content_hash"]) == 64  # SHA256 hex

    def test_applied_at_timestamp(self, project_path):
        write_applied_state_entry(
            project_path, "source.orders", "seeknal/sources/orders.yml", "test"
        )
        state = load_applied_state(project_path)
        assert "applied_at" in state["nodes"]["source.orders"]


# --- DiffEngine Baseline Resolution Tests ---


class TestBaselineResolution:
    def test_applied_state_takes_priority(self, project_with_applied_state, source_yaml):
        engine = DiffEngine(project_with_applied_state)
        content, source = engine._resolve_baseline(
            "source.orders", "seeknal/sources/orders.yml"
        )
        assert source == "applied_state"
        assert content == source_yaml

    def test_git_fallback_when_no_applied_state(self, project_with_source, source_yaml):
        engine = DiffEngine(project_with_source)

        with patch.object(engine, "_get_git_baseline", return_value=source_yaml):
            content, source = engine._resolve_baseline(
                "source.orders", "seeknal/sources/orders.yml"
            )
            assert source == "git"

    def test_manifest_fallback(self, project_with_source):
        engine = DiffEngine(project_with_source)

        with patch.object(engine, "_get_git_baseline", return_value=None):
            with patch.object(engine, "_get_manifest_baseline", return_value="reconstructed"):
                content, source = engine._resolve_baseline(
                    "source.orders", "seeknal/sources/orders.yml"
                )
                assert source == "manifest"
                assert content == "reconstructed"

    def test_none_when_no_baseline(self, project_with_source):
        engine = DiffEngine(project_with_source)

        with patch.object(engine, "_get_git_baseline", return_value=None):
            baseline_content, source = engine._resolve_baseline(
                "source.orders", "seeknal/sources/orders.yml"
            )
            assert source == "none"
            assert baseline_content is None


# --- DiffEngine Diff Generation Tests ---


class TestDiffGeneration:
    def test_no_changes_detected(self, project_with_applied_state, source_yaml):
        """File unchanged since apply → status=unchanged."""
        engine = DiffEngine(project_with_applied_state)
        result = engine.diff_node("sources/orders")
        assert result.status == "unchanged"

    def test_modified_file_detected(self, project_with_applied_state, modified_source_yaml):
        """File changed since apply → status=modified with diff."""
        # Modify the file
        orders_path = project_with_applied_state / "seeknal" / "sources" / "orders.yml"
        orders_path.write_text(modified_source_yaml)

        engine = DiffEngine(project_with_applied_state)
        result = engine.diff_node("sources/orders")

        assert result.status == "modified"
        assert result.baseline_source == "applied_state"
        assert result.unified_diff is not None
        assert result.insertions > 0 or result.deletions > 0

    def test_new_file_detected(self, project_path):
        """File exists but no baseline → status=new."""
        yaml_content = "kind: source\nname: products\n"
        (project_path / "seeknal" / "sources" / "products.yml").write_text(yaml_content)

        engine = DiffEngine(project_path)
        result = engine.diff_node("sources/products")
        assert result.status == "new"

    def test_deleted_file_detected(self, project_with_applied_state):
        """File removed but in applied_state → status=deleted."""
        # Delete the file
        (project_with_applied_state / "seeknal" / "sources" / "orders.yml").unlink()

        engine = DiffEngine(project_with_applied_state)
        result = engine.diff_node("sources/orders")
        assert result.status == "deleted"

    def test_diff_all_returns_changes(self, project_with_applied_state, modified_source_yaml):
        """diff_all returns list of changed files."""
        orders_path = project_with_applied_state / "seeknal" / "sources" / "orders.yml"
        orders_path.write_text(modified_source_yaml)

        engine = DiffEngine(project_with_applied_state)
        results = engine.diff_all()

        assert len(results) >= 1
        modified = [r for r in results if r.status == "modified"]
        assert len(modified) >= 1
        assert modified[0].node_id == "source.orders"

    def test_diff_all_empty_when_no_changes(self, project_with_applied_state):
        """diff_all returns empty list when nothing changed."""
        engine = DiffEngine(project_with_applied_state)
        results = engine.diff_all()
        assert results == []

    def test_diff_all_type_filter(self, project_with_applied_state, modified_source_yaml):
        """diff_all with type filter only returns matching type."""
        orders_path = project_with_applied_state / "seeknal" / "sources" / "orders.yml"
        orders_path.write_text(modified_source_yaml)

        engine = DiffEngine(project_with_applied_state)

        # Sources should have the change
        results = engine.diff_all(node_type="sources")
        assert len(results) >= 1

        # Transforms should have no changes
        results = engine.diff_all(node_type="transforms")
        assert len(results) == 0

    def test_invalid_node_identifier(self, project_path):
        engine = DiffEngine(project_path)
        with pytest.raises(ValueError, match="path traversal"):
            engine.diff_node("../etc/passwd")

    def test_unknown_node_type(self, project_path):
        engine = DiffEngine(project_path)
        with pytest.raises(ValueError, match="Unknown node type"):
            engine.diff_node("foobar/thing")


# --- Change Classification Tests ---


class TestChangeClassification:
    def test_column_removed_is_breaking(self, project_path):
        engine = DiffEngine(project_path)
        old = "kind: source\nname: test\ncolumns:\n  a: integer\n  b: string\n"
        new = "kind: source\nname: test\ncolumns:\n  a: integer\n"
        cat = engine._classify_change(old, new)
        assert cat == ChangeCategory.BREAKING

    def test_column_type_changed_is_breaking(self, project_path):
        engine = DiffEngine(project_path)
        old = "kind: source\nname: test\ncolumns:\n  a: string\n"
        new = "kind: source\nname: test\ncolumns:\n  a: integer\n"
        cat = engine._classify_change(old, new)
        assert cat == ChangeCategory.BREAKING

    def test_column_added_is_non_breaking(self, project_path):
        engine = DiffEngine(project_path)
        old = "kind: source\nname: test\ncolumns:\n  a: integer\n"
        new = "kind: source\nname: test\ncolumns:\n  a: integer\n  b: string\n"
        cat = engine._classify_change(old, new)
        assert cat == ChangeCategory.NON_BREAKING

    def test_entity_changed_is_breaking(self, project_path):
        engine = DiffEngine(project_path)
        old = "kind: feature_group\nname: test\nentity: user\n"
        new = "kind: feature_group\nname: test\nentity: order\n"
        cat = engine._classify_change(old, new)
        assert cat == ChangeCategory.BREAKING

    def test_description_only_is_metadata(self, project_path):
        engine = DiffEngine(project_path)
        old = "kind: source\nname: test\ndescription: old\n"
        new = "kind: source\nname: test\ndescription: new\n"
        cat = engine._classify_change(old, new)
        assert cat == ChangeCategory.METADATA

    def test_sql_changed_is_non_breaking(self, project_path):
        engine = DiffEngine(project_path)
        old = "kind: transform\nname: test\ntransform: SELECT a FROM t\n"
        new = "kind: transform\nname: test\ntransform: SELECT a, b FROM t\n"
        cat = engine._classify_change(old, new)
        assert cat == ChangeCategory.NON_BREAKING


# --- Unified Diff Tests ---


class TestUnifiedDiff:
    def test_diff_output_format(self, project_path):
        engine = DiffEngine(project_path)
        old = "line1\nline2\nline3\n"
        new = "line1\nmodified\nline3\n"
        diff = engine._generate_unified_diff(old, new, "old", "new")

        assert "--- old" in diff
        assert "+++ new" in diff
        assert "-line2" in diff
        assert "+modified" in diff

    def test_identical_content_empty_diff(self, project_path):
        engine = DiffEngine(project_path)
        content = "line1\nline2\n"
        diff = engine._generate_unified_diff(content, content, "a", "b")
        assert diff == ""


# --- Git Baseline Tests ---


class TestGitBaseline:
    def test_git_not_available(self, project_path):
        engine = DiffEngine(project_path)
        with patch("seeknal.workflow.diff_engine.subprocess.run", side_effect=FileNotFoundError):
            result = engine._get_git_baseline("seeknal/sources/orders.yml")
            assert result is None

    def test_git_file_not_tracked(self, project_path):
        engine = DiffEngine(project_path)
        mock_result = type("MockResult", (), {"returncode": 128, "stdout": ""})()
        with patch("seeknal.workflow.diff_engine.subprocess.run", return_value=mock_result):
            result = engine._get_git_baseline("seeknal/sources/orders.yml")
            assert result is None

    def test_git_returns_content(self, project_path):
        engine = DiffEngine(project_path)
        mock_result = type("MockResult", (), {"returncode": 0, "stdout": "yaml content"})()
        with patch("seeknal.workflow.diff_engine.subprocess.run", return_value=mock_result):
            result = engine._get_git_baseline("seeknal/sources/orders.yml")
            assert result == "yaml content"

    def test_git_path_prefixed_with_dot_slash(self, project_path):
        """Ensure file paths are prefixed with ./ to prevent flag injection."""
        engine = DiffEngine(project_path)
        mock_result = type("MockResult", (), {"returncode": 1, "stdout": ""})()

        with patch("seeknal.workflow.diff_engine.subprocess.run", return_value=mock_result) as mock_run:
            engine._get_git_baseline("seeknal/sources/orders.yml")
            args = mock_run.call_args[0][0]
            assert args[2] == "HEAD:./seeknal/sources/orders.yml"

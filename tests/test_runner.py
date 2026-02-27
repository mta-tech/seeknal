"""
Tests for workflow runner.
"""

import pytest  # ty: ignore[unresolved-import]
from pathlib import Path  # noqa: F401
from seeknal.dag.manifest import Manifest, Node, NodeType  # ty: ignore[unresolved-import]
from seeknal.workflow.runner import DAGRunner, ExecutionStatus, print_summary  # ty: ignore[unresolved-import]
from seeknal.workflow.state import (  # ty: ignore[unresolved-import]
    RunState, NodeState, NodeFingerprint, NodeStatus,
    compute_dag_fingerprints, save_state,
)


@pytest.fixture
def simple_manifest():
    """Create a simple manifest for testing."""
    manifest = Manifest(project="test_project")

    # Add nodes
    source = Node(
        id="source.users",
        name="users",
        node_type=NodeType.SOURCE,
        description="User data source",
        tags=["source"],
    )
    manifest.add_node(source)

    transform = Node(
        id="transform.clean_users",
        name="clean_users",
        node_type=NodeType.TRANSFORM,
        description="Clean user data",
        tags=["transform"],
    )
    manifest.add_node(transform)

    fg = Node(
        id="feature_group.user_features",
        name="user_features",
        node_type=NodeType.FEATURE_GROUP,
        description="User features",
        tags=["features"],
    )
    manifest.add_node(fg)

    # Add edges (dependencies)
    manifest.add_edge("source.users", "transform.clean_users")
    manifest.add_edge("transform.clean_users", "feature_group.user_features")

    return manifest


@pytest.fixture
def manifest_with_rule():
    """Create a manifest with source -> transform -> rule for testing."""
    manifest = Manifest(project="test_project")

    source = Node(
        id="source.orders",
        name="orders",
        node_type=NodeType.SOURCE,
        description="Order data",
        tags=["source"],
        config={"source": "csv", "table": "orders.csv"},
    )
    manifest.add_node(source)

    transform = Node(
        id="transform.clean_orders",
        name="clean_orders",
        node_type=NodeType.TRANSFORM,
        description="Clean order data",
        tags=["transform"],
        config={"transform": "SELECT * FROM input_0 WHERE amount > 0"},
    )
    manifest.add_node(transform)

    rule = Node(
        id="rule.check_orders",
        name="check_orders",
        node_type=NodeType.RULE,
        description="Validate order data quality",
        tags=["rule", "quality"],
        config={"rule_type": "sql_assertion", "query": "SELECT COUNT(*) FROM input_0 WHERE amount < 0"},
    )
    manifest.add_node(rule)

    manifest.add_edge("source.orders", "transform.clean_orders")
    manifest.add_edge("transform.clean_orders", "rule.check_orders")

    return manifest


def test_topological_order(simple_manifest):
    """Test that topological ordering works correctly."""
    runner = DAGRunner(simple_manifest)
    order = runner._get_topological_order()

    # Source should come before transform
    assert "source.users" in order
    assert "transform.clean_users" in order
    assert "feature_group.user_features" in order

    # Check ordering
    users_idx = order.index("source.users")
    clean_idx = order.index("transform.clean_users")
    features_idx = order.index("feature_group.user_features")

    assert users_idx < clean_idx < features_idx


def test_get_all_downstream(simple_manifest):
    """Test getting all downstream nodes."""
    runner = DAGRunner(simple_manifest)

    # From source, should get both downstream nodes
    downstream = runner._get_all_downstream("source.users")
    assert "transform.clean_users" in downstream
    assert "feature_group.user_features" in downstream

    # From transform, should only get feature group
    downstream = runner._get_all_downstream("transform.clean_users")
    assert "feature_group.user_features" in downstream
    assert "transform.clean_users" not in downstream


def test_get_nodes_to_run_full(simple_manifest):
    """Test that --full flag runs all nodes."""
    runner = DAGRunner(simple_manifest)
    to_run, _ = runner._get_nodes_to_run(full=True)

    assert len(to_run) == 3
    assert "source.users" in to_run
    assert "transform.clean_users" in to_run
    assert "feature_group.user_features" in to_run


def test_get_nodes_to_run_filter_by_type(simple_manifest):
    """Test filtering by node type."""
    runner = DAGRunner(simple_manifest)
    to_run, _ = runner._get_nodes_to_run(full=True, node_types=["source"])

    assert len(to_run) == 1
    assert "source.users" in to_run


def test_get_nodes_to_run_exclude_tags(simple_manifest):
    """Test excluding nodes by tags."""
    runner = DAGRunner(simple_manifest)
    to_run, _ = runner._get_nodes_to_run(full=True, exclude_tags=["source"])

    assert len(to_run) == 2
    assert "source.users" not in to_run
    assert "transform.clean_users" in to_run
    assert "feature_group.user_features" in to_run


def test_get_nodes_to_run_include_tags(simple_manifest):
    """Test including nodes by tags with upstream auto-include."""
    runner = DAGRunner(simple_manifest)
    # Tag "features" only on feature_group.user_features
    to_run, reasons = runner._get_nodes_to_run(tags=["features"])

    # Should include the tagged node plus all upstream deps
    assert "feature_group.user_features" in to_run
    assert "transform.clean_users" in to_run  # upstream
    assert "source.users" in to_run  # transitive upstream
    assert reasons["feature_group.user_features"] == "tag match (features)"
    assert "upstream" in reasons.get("source.users", "")


def test_get_nodes_to_run_include_tags_or_logic(simple_manifest):
    """Test --tags uses OR logic (matches any tag)."""
    runner = DAGRunner(simple_manifest)
    to_run, _ = runner._get_nodes_to_run(tags=["source", "features"])

    # Both source (tag:source) and feature_group (tag:features) match
    assert "source.users" in to_run
    assert "feature_group.user_features" in to_run
    assert "transform.clean_users" in to_run  # upstream of feature_group


def test_get_nodes_to_run_tags_no_match(simple_manifest):
    """Test --tags with no matching nodes returns empty set."""
    runner = DAGRunner(simple_manifest)
    to_run, _ = runner._get_nodes_to_run(tags=["nonexistent_tag"])

    assert len(to_run) == 0


def test_get_nodes_to_run_tags_and_exclude_tags(simple_manifest):
    """Test --tags + --exclude-tags compose: include first, then exclude."""
    runner = DAGRunner(simple_manifest)
    # Include "features" tag (feature_group + upstream), exclude "source" tag
    to_run, _ = runner._get_nodes_to_run(
        tags=["features"], exclude_tags=["source"]
    )

    # feature_group and transform should remain, source excluded
    assert "feature_group.user_features" in to_run
    assert "transform.clean_users" in to_run
    assert "source.users" not in to_run  # excluded by --exclude-tags


def test_get_nodes_to_run_tags_and_nodes_union(simple_manifest):
    """Test --tags + --nodes compose as union."""
    runner = DAGRunner(simple_manifest)
    # Tags match source (tag:source), nodes match clean_users (+ downstream)
    to_run, _ = runner._get_nodes_to_run(
        tags=["source"], nodes=["clean_users"]
    )

    # Source from tags, clean_users from --nodes, user_features as downstream
    assert "source.users" in to_run
    assert "transform.clean_users" in to_run
    assert "feature_group.user_features" in to_run


def test_get_nodes_to_run_full_overrides_tags(simple_manifest):
    """Test --full overrides --tags (runs everything)."""
    runner = DAGRunner(simple_manifest)
    to_run, _ = runner._get_nodes_to_run(full=True, tags=["features"])

    # --full runs all nodes regardless of tags
    assert len(to_run) == 3


def test_get_nodes_to_run_tags_skip_rules():
    """When filtering by tags, rules without the tag should NOT auto-include."""
    manifest = Manifest(project="test_project")
    source = Node(
        id="source.data", name="data",
        node_type=NodeType.SOURCE, tags=["etl"],
    )
    manifest.add_node(source)
    transform = Node(
        id="transform.clean", name="clean",
        node_type=NodeType.TRANSFORM, tags=["etl"],
    )
    manifest.add_node(transform)
    rule = Node(
        id="rule.check", name="check",
        node_type=NodeType.RULE, tags=["quality"],
    )
    manifest.add_node(rule)
    manifest.add_edge("source.data", "transform.clean")
    manifest.add_edge("transform.clean", "rule.check")

    runner = DAGRunner(manifest)
    to_run, _ = runner._get_nodes_to_run(tags=["etl"])

    # Only etl-tagged nodes should run; rule.check has "quality" tag, not "etl"
    assert "source.data" in to_run
    assert "transform.clean" in to_run
    assert "rule.check" not in to_run


def test_get_nodes_to_run_specific_nodes(simple_manifest):
    """Test running specific nodes."""
    runner = DAGRunner(simple_manifest)
    to_run, _ = runner._get_nodes_to_run(nodes=["users"])

    # Should include the source and all downstream
    assert "source.users" in to_run
    assert "transform.clean_users" in to_run
    assert "feature_group.user_features" in to_run


def test_dry_run(simple_manifest, tmp_path):
    """Test dry run mode."""
    runner = DAGRunner(simple_manifest, target_path=tmp_path)
    summary = runner.run(full=True, dry_run=True)

    assert summary.total_nodes == 3
    assert summary.successful_nodes == 3
    assert summary.failed_nodes == 0
    assert all(r.status == ExecutionStatus.SUCCESS for r in summary.results)


def test_cycle_detection():
    """Test that cycles are detected."""
    manifest = Manifest(project="test")

    # Create a cycle: A -> B -> C -> A
    a = Node(id="a", name="a", node_type=NodeType.SOURCE)
    b = Node(id="b", name="b", node_type=NodeType.TRANSFORM)
    c = Node(id="c", name="c", node_type=NodeType.FEATURE_GROUP)

    manifest.add_node(a)
    manifest.add_node(b)
    manifest.add_node(c)

    manifest.add_edge("a", "b")
    manifest.add_edge("b", "c")
    manifest.add_edge("c", "a")  # Creates cycle

    runner = DAGRunner(manifest)

    with pytest.raises(ValueError, match="cycle"):
        runner._get_topological_order()


def test_state_persistence(simple_manifest, tmp_path):
    """Test that state is persisted between runs."""
    # First run - with continue_on_error since transform has no SQL
    runner1 = DAGRunner(simple_manifest, target_path=tmp_path)
    summary1 = runner1.run(full=True, continue_on_error=True)

    # At least source should succeed
    assert summary1.successful_nodes >= 1

    # Check that state was saved for successful nodes
    assert len(runner1.run_state.nodes) > 0

    # Second run with same runner state should detect cache for unchanged nodes
    runner2 = DAGRunner(simple_manifest, target_path=tmp_path)

    # Check that source node is now cached
    assert runner2._is_cached("source.users")


def test_print_summary():
    """Test summary printing."""
    import io
    import sys

    summary = type('obj', (object,), {
        'total_nodes': 3,
        'changed_nodes': 2,
        'cached_nodes': 1,
        'successful_nodes': 2,
        'failed_nodes': 1,
        'skipped_nodes': 0,
        'total_duration': 1.5,
        'results': [],
    })()

    # Capture output
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    print_summary(summary)  # type: ignore[arg-type]
    captured = sys.stdout.getvalue()
    sys.stdout = old_stdout

    # Check that key info is present (without ANSI codes)
    assert "Total nodes:" in captured
    assert "3" in captured
    assert "Duration:" in captured
    assert "1.50s" in captured
    assert "Execution Summary" in captured


# --- Tests for always-run rule nodes ---


class TestRuleAlwaysRun:
    """Tests verifying RULE nodes always execute regardless of data changes."""

    def _build_fingerprints(self, manifest):
        """Build fingerprints for all nodes in a manifest."""
        upstream_map = {}
        for nid in manifest.nodes:
            upstream_map[nid] = manifest.get_upstream_nodes(nid)

        manifest_nodes = {}
        for nid, node in manifest.nodes.items():
            manifest_nodes[nid] = {
                "kind": node.node_type.value,
                "config": node.config,
                "file_path": node.file_path or "unknown.yml",
                "columns": node.columns,
            }

        return compute_dag_fingerprints(manifest_nodes, upstream_map)

    def _simulate_successful_run(self, manifest, target_path):
        """Simulate a successful first run by storing fingerprints and cache files."""
        fps = self._build_fingerprints(manifest)

        state = RunState()
        for nid, fp in fps.items():
            state.nodes[nid] = NodeState(
                hash=fp.combined,
                last_run="2026-02-24T10:00:00",
                status=NodeStatus.SUCCESS.value,
                duration_ms=100,
                row_count=10,
                fingerprint=fp,
            )

        save_state(state, target_path / "run_state.json")

        # Create cache parquet files so fingerprint detection doesn't flag "cache missing"
        for nid, node in manifest.nodes.items():
            cache_dir = target_path / "cache" / node.node_type.value
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / f"{node.name}.parquet"
            cache_file.write_bytes(b"PAR1")  # minimal placeholder

    def test_rule_included_when_no_changes(self, manifest_with_rule, tmp_path):
        """Rule nodes should always be in to_run even when fingerprints match."""
        self._simulate_successful_run(manifest_with_rule, tmp_path)

        runner = DAGRunner(manifest_with_rule, target_path=tmp_path)
        to_run, skip_reasons = runner._get_nodes_to_run()

        # Source and transform should NOT be in to_run (unchanged)
        assert "source.orders" not in to_run
        assert "transform.clean_orders" not in to_run

        # Rule MUST be in to_run despite no changes
        assert "rule.check_orders" in to_run
        assert skip_reasons["rule.check_orders"] == "always run (data quality)"

    def test_rule_included_alongside_changed_nodes(self, manifest_with_rule, tmp_path):
        """Rule nodes should be in to_run alongside any changed nodes."""
        self._simulate_successful_run(manifest_with_rule, tmp_path)

        # Modify source config to trigger a change (use hashed field)
        manifest_with_rule.nodes["source.orders"].config["table"] = "orders_v2.csv"

        runner = DAGRunner(manifest_with_rule, target_path=tmp_path)
        to_run, skip_reasons = runner._get_nodes_to_run()

        # Source changed, transform is downstream
        assert "source.orders" in to_run
        assert "transform.clean_orders" in to_run

        # Rule should also be in to_run (it's downstream AND always-run)
        assert "rule.check_orders" in to_run

    def test_rule_in_full_mode(self, manifest_with_rule, tmp_path):
        """Rule nodes should be included in --full mode."""
        runner = DAGRunner(manifest_with_rule, target_path=tmp_path)
        to_run, _ = runner._get_nodes_to_run(full=True)

        assert "rule.check_orders" in to_run
        assert len(to_run) == 3

    def test_rule_excluded_by_type_filter(self, manifest_with_rule, tmp_path):
        """Type filter should still be able to exclude rules."""
        self._simulate_successful_run(manifest_with_rule, tmp_path)

        runner = DAGRunner(manifest_with_rule, target_path=tmp_path)
        to_run, _ = runner._get_nodes_to_run(node_types=["source"])

        # Only source nodes should remain after type filter
        assert "rule.check_orders" not in to_run
        assert "source.orders" not in to_run  # Also not in to_run since unchanged

    def test_rule_excluded_by_tag_filter(self, manifest_with_rule, tmp_path):
        """Tag exclusion should still be able to exclude rules."""
        self._simulate_successful_run(manifest_with_rule, tmp_path)

        runner = DAGRunner(manifest_with_rule, target_path=tmp_path)
        to_run, _ = runner._get_nodes_to_run(exclude_tags=["quality"])

        # Rule has "quality" tag, should be excluded
        assert "rule.check_orders" not in to_run

    def test_multiple_rules_all_always_run(self, tmp_path):
        """All rule nodes should always run, not just one."""
        manifest = Manifest(project="test")

        source = Node(
            id="source.data",
            name="data",
            node_type=NodeType.SOURCE,
            config={"source": "csv", "table": "data.csv"},
        )
        manifest.add_node(source)

        rule1 = Node(
            id="rule.check_nulls",
            name="check_nulls",
            node_type=NodeType.RULE,
            config={"rule_type": "null", "check_columns": ["id"]},
        )
        manifest.add_node(rule1)

        rule2 = Node(
            id="rule.check_range",
            name="check_range",
            node_type=NodeType.RULE,
            config={"rule_type": "range", "check_column": "amount", "min": 0},
        )
        manifest.add_node(rule2)

        manifest.add_edge("source.data", "rule.check_nulls")
        manifest.add_edge("source.data", "rule.check_range")

        self._simulate_successful_run(manifest, tmp_path)

        runner = DAGRunner(manifest, target_path=tmp_path)
        to_run, skip_reasons = runner._get_nodes_to_run()

        # Source unchanged, should NOT run
        assert "source.data" not in to_run

        # Both rules MUST run
        assert "rule.check_nulls" in to_run
        assert "rule.check_range" in to_run
        assert skip_reasons["rule.check_nulls"] == "always run (data quality)"
        assert skip_reasons["rule.check_range"] == "always run (data quality)"

    def test_rule_on_first_run_no_state(self, manifest_with_rule, tmp_path):
        """On first run (no state), rules should be included as new nodes."""
        runner = DAGRunner(manifest_with_rule, target_path=tmp_path)
        to_run, _ = runner._get_nodes_to_run()

        # All nodes should run on first run (no stored fingerprints)
        assert "source.orders" in to_run
        assert "transform.clean_orders" in to_run
        assert "rule.check_orders" in to_run

    def test_rule_with_specific_nodes_selection(self, manifest_with_rule, tmp_path):
        """When selecting specific nodes, rules should still always run."""
        self._simulate_successful_run(manifest_with_rule, tmp_path)

        runner = DAGRunner(manifest_with_rule, target_path=tmp_path)
        # Select only the source node
        to_run, _ = runner._get_nodes_to_run(nodes=["orders"])

        # Source explicitly selected, transform and rule are downstream
        assert "source.orders" in to_run
        assert "transform.clean_orders" in to_run
        assert "rule.check_orders" in to_run


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

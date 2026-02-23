"""
Tests for workflow runner.
"""

import pytest
from pathlib import Path
from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.workflow.runner import DAGRunner, ExecutionStatus, print_summary


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
    to_run = runner._get_nodes_to_run(full=True)

    assert len(to_run) == 3
    assert "source.users" in to_run
    assert "transform.clean_users" in to_run
    assert "feature_group.user_features" in to_run


def test_get_nodes_to_run_filter_by_type(simple_manifest):
    """Test filtering by node type."""
    runner = DAGRunner(simple_manifest)
    to_run = runner._get_nodes_to_run(full=True, node_types=["source"])

    assert len(to_run) == 1
    assert "source.users" in to_run


def test_get_nodes_to_run_exclude_tags(simple_manifest):
    """Test excluding nodes by tags."""
    runner = DAGRunner(simple_manifest)
    to_run = runner._get_nodes_to_run(full=True, exclude_tags=["source"])

    assert len(to_run) == 2
    assert "source.users" not in to_run
    assert "transform.clean_users" in to_run
    assert "feature_group.user_features" in to_run


def test_get_nodes_to_run_specific_nodes(simple_manifest):
    """Test running specific nodes."""
    runner = DAGRunner(simple_manifest)
    to_run = runner._get_nodes_to_run(nodes=["users"])

    # Should include the source and all downstream
    assert "source.users" in to_run
    assert "transform.clean_users" in to_run
    assert "feature_group.user_features" in to_run


def test_dry_run(simple_manifest, tmp_path):
    """Test dry run mode."""
    runner = DAGRunner(simple_manifest, state_dir=tmp_path)
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
    runner1 = DAGRunner(simple_manifest, state_dir=tmp_path)
    summary1 = runner1.run(full=True, continue_on_error=True)

    # At least source should succeed
    assert summary1.successful_nodes >= 1

    # Check that state was saved for successful nodes
    assert len(runner1.node_state) > 0

    # Second run with same runner state should detect cache for unchanged nodes
    runner2 = DAGRunner(simple_manifest, state_dir=tmp_path)

    # Check that source node is now cached
    assert runner2._is_cached("source.users")


def test_print_summary(capsys, simple_manifest):
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
    print_summary(summary)
    captured = sys.stdout.getvalue()
    sys.stdout = old_stdout

    # Check that key info is present (without ANSI codes)
    assert "Total nodes:" in captured
    assert "3" in captured
    assert "Duration:" in captured
    assert "1.50s" in captured
    assert "Execution Summary" in captured


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

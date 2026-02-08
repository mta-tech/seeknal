"""
Example: Using the DAG Runner

This example demonstrates how to use the DAGRunner to execute
a Seeknal 2.0 DAG with various options.
"""

from pathlib import Path
from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.workflow.runner import DAGRunner, print_summary


def example_basic_run():
    """Basic DAG execution."""
    # Load manifest
    manifest = Manifest.load("target/manifest.json")

    # Create runner
    runner = DAGRunner(manifest)

    # Run only changed nodes
    summary = runner.run()

    # Print summary
    print_summary(summary)


def example_full_run():
    """Run all nodes regardless of state."""
    manifest = Manifest.load("target/manifest.json")
    runner = DAGRunner(manifest)

    # Run all nodes
    summary = runner.run(full=True)
    print_summary(summary)


def example_filtered_run():
    """Run only specific node types."""
    manifest = Manifest.load("target/manifest.json")
    runner = DAGRunner(manifest)

    # Run only feature_groups
    summary = runner.run(
        full=True,
        node_types=["feature_group"]
    )
    print_summary(summary)


def example_continue_on_error():
    """Continue execution even if some nodes fail."""
    manifest = Manifest.load("target/manifest.json")
    runner = DAGRunner(manifest)

    # Keep going on failures
    summary = runner.run(
        full=True,
        continue_on_error=True,
        retry=2,  # Retry failed nodes twice
    )
    print_summary(summary)


def example_dry_run():
    """Show what would be executed without running."""
    manifest = Manifest.load("target/manifest.json")
    runner = DAGRunner(manifest)

    # Just show the plan
    runner.print_plan(full=True)

    # Or dry run the execution
    summary = runner.run(full=True, dry_run=True)
    print_summary(summary)


def example_specific_nodes():
    """Run specific nodes and their dependencies."""
    manifest = Manifest.load("target/manifest.json")
    runner = DAGRunner(manifest)

    # Run specific nodes (will also run downstream dependencies)
    summary = runner.run(
        nodes=["user_features", "order_features"]
    )
    print_summary(summary)


def example_exclude_tags():
    """Skip nodes with certain tags."""
    manifest = Manifest.load("target/manifest.json")
    runner = DAGRunner(manifest)

    # Skip nodes tagged as 'expensive' or 'experimental'
    summary = runner.run(
        full=True,
        exclude_tags=["expensive", "experimental"]
    )
    print_summary(summary)


def example_with_change_detection():
    """Run only changed nodes based on manifest diff."""
    from seeknal.dag.diff import ManifestDiff

    # Load old and new manifests
    old_manifest = Manifest.load("target/manifest.json.old")
    new_manifest = Manifest.load("target/manifest.json")

    # Create runner with change detection
    runner = DAGRunner(
        new_manifest,
        old_manifest=old_manifest
    )

    # Show what changed
    if runner.diff:
        print(f"Changes detected: {runner.diff.summary()}")

        # Run only changed nodes and downstream
        summary = runner.run()
        print_summary(summary)
    else:
        print("No changes detected")


def example_create_simple_manifest():
    """Create a simple manifest for testing."""
    manifest = Manifest(project="example_project")

    # Add a source node
    source = Node(
        id="source.users",
        name="users",
        node_type=NodeType.SOURCE,
        description="User data source",
        tags=["source"],
        config={
            "source": "postgres",
            "table": "public.users",
            "columns": {
                "user_id": "User ID",
                "name": "User name",
                "email": "Email address",
            }
        }
    )
    manifest.add_node(source)

    # Add a transform node
    transform = Node(
        id="transform.clean_users",
        name="clean_users",
        node_type=NodeType.TRANSFORM,
        description="Clean user data",
        tags=["transform"],
        config={
            "transform": "SELECT * FROM {{ ref('users') }} WHERE active = true"
        }
    )
    manifest.add_node(transform)

    # Add a feature group node
    fg = Node(
        id="feature_group.user_features",
        name="user_features",
        node_type=NodeType.FEATURE_GROUP,
        description="User features",
        tags=["features", "ml"],
        config={
            "entity": "user",
            "features": {
                "user_id": {"dtype": "int"},
                "name_clean": {"dtype": "string"},
                "email_domain": {"dtype": "string"},
            }
        }
    )
    manifest.add_node(fg)

    # Add edges
    manifest.add_edge("source.users", "transform.clean_users")
    manifest.add_edge("transform.clean_users", "feature_group.user_features")

    # Save manifest
    manifest.save("target/manifest.json")

    return manifest


if __name__ == "__main__":
    import sys

    # Create a test manifest
    print("Creating test manifest...")
    example_create_simple_manifest()

    # Run examples
    print("\n" + "=" * 60)
    print("Example 1: Dry run (show plan)")
    print("=" * 60)
    example_dry_run()

    print("\n" + "=" * 60)
    print("Example 2: Full run")
    print("=" * 60)
    example_full_run()

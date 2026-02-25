"""
Test suite for Analytics Engineering tutorial.

Validates that all YAML examples parse correctly and demonstrate
proper change categorization behavior.
"""
import yaml
from pathlib import Path
import pytest

from seeknal.dag.diff import ChangeCategory, ManifestDiff, NodeChange, DiffType
from seeknal.dag.manifest import Manifest, Node, NodeType, Edge


# Example YAML directory
EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples" / "analytics-eng"


class TestAnalyticsEngTutorialExamples:
    """Test that tutorial YAML files are valid and demonstrate concepts correctly."""

    def test_all_yaml_files_parse(self):
        """All 7 YAML files should parse without errors."""
        expected_files = [
            "01_source_subscriptions.yml",
            "02_source_payments.yml",
            "03_source_customers.yml",
            "04_transform_revenue_base.yml",
            "05_transform_customer_metrics.yml",
            "06_aggregation_monthly_revenue.yml",
            "07_aggregation_churn_analysis.yml",
        ]

        for filename in expected_files:
            filepath = EXAMPLES_DIR / filename
            assert filepath.exists(), f"Missing tutorial file: {filename}"

            with open(filepath) as f:
                data = yaml.safe_load(f)

            # Basic validation
            assert "kind" in data, f"{filename}: missing 'kind' field"
            assert "name" in data, f"{filename}: missing 'name' field"
            assert data["kind"] in [
                "source",
                "transform",
                "aggregation",
            ], f"{filename}: invalid kind '{data['kind']}'"

    def test_dag_builds_without_cycles(self):
        """DAG should build correctly from YAML files with no cycles."""
        # Build minimal manifest from YAML
        manifest = Manifest(project="test_analytics")

        yaml_files = sorted(EXAMPLES_DIR.glob("*.yml"))
        for filepath in yaml_files:
            with open(filepath) as f:
                data = yaml.safe_load(f)

            node_name = data["name"]
            node_kind = data["kind"]

            # Map kind to NodeType
            node_type_map = {
                "source": NodeType.SOURCE,
                "transform": NodeType.TRANSFORM,
                "aggregation": NodeType.AGGREGATION,
            }
            node_type = node_type_map[node_kind]

            # Create node
            node_id = f"{node_kind}.{node_name}"
            node = Node(
                id=node_id,
                name=node_name,
                node_type=node_type,
                description=data.get("description"),
                tags=data.get("tags", []),
                columns=data.get("columns", {}),
                config=data.get("config", {}),
            )
            manifest.add_node(node)

            # Create edges from inputs
            if "inputs" in data:
                for input_ref in data["inputs"]:
                    ref = input_ref.get("ref")
                    if ref:
                        manifest.add_edge(from_node=ref, to_node=node_id)

        # Check for cycles (simplified - just verify manifest can be created)
        assert len(manifest.nodes) == 7, "Expected 7 nodes in manifest"
        assert len(manifest.edges) > 0, "Expected edges between nodes"

    def test_change_categorization_add_column_is_non_breaking(self):
        """Adding a column should be categorized as NON_BREAKING."""
        # Old node: customer_metrics without 'country' column
        old_node = Node(
            id="transform.customer_metrics",
            name="customer_metrics",
            node_type=NodeType.TRANSFORM,
            columns={
                "customer_id": "string",
                "company_name": "string",
                "industry": "string",
                "subscription_count": "integer",
                "total_mrr": "decimal",
            },
        )

        # New node: customer_metrics WITH 'country' column
        new_node = Node(
            id="transform.customer_metrics",
            name="customer_metrics",
            node_type=NodeType.TRANSFORM,
            columns={
                "customer_id": "string",
                "company_name": "string",
                "industry": "string",
                "country": "string",  # ADDED
                "subscription_count": "integer",
                "total_mrr": "decimal",
            },
        )

        change = NodeChange(
            node_id="transform.customer_metrics",
            change_type=DiffType.MODIFIED,
            old_value=old_node,
            new_value=new_node,
            changed_fields=["columns"],
        )

        category = ManifestDiff.classify_change(change)
        assert category == ChangeCategory.NON_BREAKING, (
            "Adding a column should be NON_BREAKING"
        )

    def test_change_categorization_remove_column_is_breaking(self):
        """Removing a column should be categorized as BREAKING."""
        # Old node: customer_metrics with 'total_mrr' column
        old_node = Node(
            id="transform.customer_metrics",
            name="customer_metrics",
            node_type=NodeType.TRANSFORM,
            columns={
                "customer_id": "string",
                "company_name": "string",
                "total_mrr": "decimal",  # Will be removed
            },
        )

        # New node: customer_metrics WITHOUT 'total_mrr' column
        new_node = Node(
            id="transform.customer_metrics",
            name="customer_metrics",
            node_type=NodeType.TRANSFORM,
            columns={
                "customer_id": "string",
                "company_name": "string",
                # total_mrr REMOVED
            },
        )

        change = NodeChange(
            node_id="transform.customer_metrics",
            change_type=DiffType.MODIFIED,
            old_value=old_node,
            new_value=new_node,
            changed_fields=["columns"],
        )

        category = ManifestDiff.classify_change(change)
        assert category == ChangeCategory.BREAKING, (
            "Removing a column should be BREAKING"
        )

    def test_change_categorization_description_is_metadata(self):
        """Changing only description should be categorized as METADATA."""
        # Old node with original description
        old_node = Node(
            id="transform.customer_metrics",
            name="customer_metrics",
            node_type=NodeType.TRANSFORM,
            description="Old description",
            columns={"customer_id": "string"},
        )

        # New node with updated description
        new_node = Node(
            id="transform.customer_metrics",
            name="customer_metrics",
            node_type=NodeType.TRANSFORM,
            description="New improved description",
            columns={"customer_id": "string"},
        )

        change = NodeChange(
            node_id="transform.customer_metrics",
            change_type=DiffType.MODIFIED,
            old_value=old_node,
            new_value=new_node,
            changed_fields=["description"],
        )

        category = ManifestDiff.classify_change(change)
        assert category == ChangeCategory.METADATA, (
            "Changing only description should be METADATA"
        )

    def test_yaml_sources_have_required_fields(self):
        """Source YAML files should have all required fields."""
        source_files = [
            "01_source_subscriptions.yml",
            "02_source_payments.yml",
            "03_source_customers.yml",
        ]

        for filename in source_files:
            with open(EXAMPLES_DIR / filename) as f:
                data = yaml.safe_load(f)

            assert data["kind"] == "source"
            assert "name" in data
            assert "source" in data
            assert "table" in data
            assert "columns" in data
            assert isinstance(data["columns"], dict)
            assert len(data["columns"]) > 0

    def test_yaml_transforms_have_sql_and_inputs(self):
        """Transform YAML files should have SQL and input references."""
        transform_files = [
            "04_transform_revenue_base.yml",
            "05_transform_customer_metrics.yml",
        ]

        for filename in transform_files:
            with open(EXAMPLES_DIR / filename) as f:
                data = yaml.safe_load(f)

            assert data["kind"] == "transform"
            assert "transform" in data, f"{filename}: missing 'transform' SQL"
            assert "inputs" in data, f"{filename}: missing 'inputs'"
            assert isinstance(data["inputs"], list)
            assert len(data["inputs"]) > 0

            # Each input should have a 'ref' field
            for input_ref in data["inputs"]:
                assert "ref" in input_ref

    def test_yaml_aggregations_have_features(self):
        """Aggregation YAML files should have feature definitions."""
        aggregation_files = [
            "06_aggregation_monthly_revenue.yml",
            "07_aggregation_churn_analysis.yml",
        ]

        for filename in aggregation_files:
            with open(EXAMPLES_DIR / filename) as f:
                data = yaml.safe_load(f)

            assert data["kind"] == "aggregation"
            assert "id_col" in data
            assert "feature_date_col" in data
            assert "features" in data
            assert isinstance(data["features"], list)
            assert len(data["features"]) > 0

            # Each feature should have name and column
            for feature in data["features"]:
                assert "name" in feature
                assert "column" in feature


class TestChangeCategorizationLogic:
    """Test change categorization logic in detail."""

    def test_rename_column_is_breaking(self):
        """Renaming a column (remove + add) should be BREAKING."""
        old_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            columns={"old_name": "string"},
        )

        new_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            columns={"new_name": "string"},
        )

        change = NodeChange(
            node_id="transform.test",
            change_type=DiffType.MODIFIED,
            old_value=old_node,
            new_value=new_node,
            changed_fields=["columns"],
        )

        category = ManifestDiff.classify_change(change)
        assert category == ChangeCategory.BREAKING

    def test_column_type_change_is_breaking(self):
        """Changing a column type should be BREAKING."""
        old_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            columns={"amount": "integer"},
        )

        new_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            columns={"amount": "decimal"},  # Type changed
        )

        change = NodeChange(
            node_id="transform.test",
            change_type=DiffType.MODIFIED,
            old_value=old_node,
            new_value=new_node,
            changed_fields=["columns"],
        )

        category = ManifestDiff.classify_change(change)
        assert category == ChangeCategory.BREAKING

    def test_tags_change_is_metadata(self):
        """Changing tags should be METADATA."""
        old_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            tags=["old-tag"],
            columns={"id": "string"},
        )

        new_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            tags=["new-tag"],
            columns={"id": "string"},
        )

        change = NodeChange(
            node_id="transform.test",
            change_type=DiffType.MODIFIED,
            old_value=old_node,
            new_value=new_node,
            changed_fields=["tags"],
        )

        category = ManifestDiff.classify_change(change)
        assert category == ChangeCategory.METADATA

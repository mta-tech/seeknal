"""Tests for change categorization in manifest diff."""

import pytest

from seeknal.dag.diff import (
    ChangeCategory,
    DiffType,
    ManifestDiff,
    NodeChange,
    _classify_column_change,
    _classify_config_change,
    _severity,
    METADATA_FIELDS,
    BREAKING_CONFIG_KEYS,
    METADATA_CONFIG_KEYS,
)
from seeknal.dag.manifest import Manifest, Node, NodeType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(
    node_id: str = "source.users",
    name: str = "users",
    node_type: NodeType = NodeType.SOURCE,
    description: str | None = None,
    owner: str | None = None,
    tags: list[str] | None = None,
    columns: dict[str, str] | None = None,
    config: dict | None = None,
) -> Node:
    return Node(
        id=node_id,
        name=name,
        node_type=node_type,
        description=description,
        owner=owner,
        tags=tags or [],
        columns=columns or {},
        config=config or {},
    )


def _make_modified_change(
    old_node: Node,
    new_node: Node,
    changed_fields: list[str],
) -> NodeChange:
    return NodeChange(
        node_id=old_node.id,
        change_type=DiffType.MODIFIED,
        old_value=old_node,
        new_value=new_node,
        changed_fields=changed_fields,
    )


def _make_chain_manifest() -> Manifest:
    """Build A -> B -> C manifest for downstream tests."""
    m = Manifest(project="test")
    m.add_node(_make_node("source.a", "a", NodeType.SOURCE))
    m.add_node(_make_node("transform.b", "b", NodeType.TRANSFORM))
    m.add_node(_make_node("feature_group.c", "c", NodeType.FEATURE_GROUP))
    m.add_edge("source.a", "transform.b")
    m.add_edge("transform.b", "feature_group.c")
    return m


# ===========================================================================
# 1. ChangeCategory enum values
# ===========================================================================

class TestChangeCategoryEnum:
    def test_enum_values(self):
        assert ChangeCategory.BREAKING.value == "breaking"
        assert ChangeCategory.NON_BREAKING.value == "non_breaking"
        assert ChangeCategory.METADATA.value == "metadata"

    def test_severity_ordering(self):
        assert _severity(ChangeCategory.METADATA) < _severity(ChangeCategory.NON_BREAKING)
        assert _severity(ChangeCategory.NON_BREAKING) < _severity(ChangeCategory.BREAKING)


# ===========================================================================
# Column-level classification
# ===========================================================================

class TestColumnClassification:
    def test_column_removed_is_breaking(self):
        """2. Column removed -> BREAKING"""
        old = _make_node(columns={"id": "INTEGER", "name": "VARCHAR"})
        new = _make_node(columns={"id": "INTEGER"})
        assert _classify_column_change(old, new) == ChangeCategory.BREAKING

    def test_column_added_is_non_breaking(self):
        """3. Column added -> NON_BREAKING"""
        old = _make_node(columns={"id": "INTEGER"})
        new = _make_node(columns={"id": "INTEGER", "email": "VARCHAR"})
        assert _classify_column_change(old, new) == ChangeCategory.NON_BREAKING

    def test_column_type_changed_is_breaking(self):
        """4. Column type changed -> BREAKING (conservative)"""
        old = _make_node(columns={"id": "INTEGER"})
        new = _make_node(columns={"id": "VARCHAR"})
        assert _classify_column_change(old, new) == ChangeCategory.BREAKING


# ===========================================================================
# Metadata-only classification
# ===========================================================================

class TestMetadataClassification:
    def test_description_changed_is_metadata(self):
        """5. Description changed -> METADATA"""
        old = _make_node(description="Old desc")
        new = _make_node(description="New desc")
        change = _make_modified_change(old, new, ["description"])
        assert ManifestDiff.classify_change(change) == ChangeCategory.METADATA

    def test_owner_changed_is_metadata(self):
        """6. Owner changed -> METADATA"""
        old = _make_node(owner="alice")
        new = _make_node(owner="bob")
        change = _make_modified_change(old, new, ["owner"])
        assert ManifestDiff.classify_change(change) == ChangeCategory.METADATA

    def test_tags_changed_is_metadata(self):
        """7. Tags changed -> METADATA"""
        old = _make_node(tags=["v1"])
        new = _make_node(tags=["v2"])
        change = _make_modified_change(old, new, ["tags"])
        assert ManifestDiff.classify_change(change) == ChangeCategory.METADATA


# ===========================================================================
# Config-level classification
# ===========================================================================

class TestConfigClassification:
    def test_sql_config_changed_is_non_breaking(self):
        """8. SQL config changed -> NON_BREAKING"""
        old = _make_node(config={"sql": "SELECT 1"})
        new = _make_node(config={"sql": "SELECT 2"})
        assert _classify_config_change(old, new) == ChangeCategory.NON_BREAKING

    def test_entity_changed_is_breaking(self):
        """9. Entity join key changed -> BREAKING"""
        old = _make_node(config={"entity": {"join_keys": ["user_id"]}})
        new = _make_node(config={"entity": {"join_keys": ["account_id"]}})
        assert _classify_config_change(old, new) == ChangeCategory.BREAKING

    def test_input_removed_is_breaking(self):
        """10. Input removed -> BREAKING"""
        old = _make_node(config={"inputs": ["a", "b"]})
        new = _make_node(config={"inputs": ["a"]})
        assert _classify_config_change(old, new) == ChangeCategory.BREAKING

    def test_input_added_is_non_breaking(self):
        """11. Input added -> NON_BREAKING"""
        old = _make_node(config={"inputs": ["a"]})
        new = _make_node(config={"inputs": ["a", "b"]})
        assert _classify_config_change(old, new) == ChangeCategory.NON_BREAKING

    def test_features_removed_is_breaking(self):
        """12. Features removed -> BREAKING"""
        old = _make_node(config={"features": {"f1": {}, "f2": {}}})
        new = _make_node(config={"features": {"f1": {}}})
        assert _classify_config_change(old, new) == ChangeCategory.BREAKING

    def test_features_added_is_non_breaking(self):
        """13. Features added -> NON_BREAKING"""
        old = _make_node(config={"features": {"f1": {}}})
        new = _make_node(config={"features": {"f1": {}, "f2": {}}})
        assert _classify_config_change(old, new) == ChangeCategory.NON_BREAKING

    def test_audits_changed_is_metadata(self):
        """14. Audits changed -> METADATA"""
        old = _make_node(config={"audits": ["check_nulls"]})
        new = _make_node(config={"audits": ["check_nulls", "check_unique"]})
        assert _classify_config_change(old, new) == ChangeCategory.METADATA

    def test_source_type_changed_is_breaking(self):
        """15. Source type changed -> BREAKING"""
        old = _make_node(config={"source_type": "postgres"})
        new = _make_node(config={"source_type": "mysql"})
        assert _classify_config_change(old, new) == ChangeCategory.BREAKING

    def test_unknown_config_key_is_non_breaking(self):
        """19. Unknown config key -> NON_BREAKING (default)"""
        old = _make_node(config={"some_new_key": "old"})
        new = _make_node(config={"some_new_key": "new"})
        assert _classify_config_change(old, new) == ChangeCategory.NON_BREAKING


# ===========================================================================
# Node-level field classification
# ===========================================================================

class TestNodeFieldClassification:
    def test_node_type_changed_is_breaking(self):
        """16. Node type changed -> BREAKING"""
        old = _make_node(node_type=NodeType.SOURCE)
        new = _make_node(node_type=NodeType.TRANSFORM)
        change = _make_modified_change(old, new, ["node_type"])
        assert ManifestDiff.classify_change(change) == ChangeCategory.BREAKING

    def test_name_changed_is_breaking(self):
        """17. Name changed -> BREAKING"""
        old = _make_node(name="old_name")
        new = _make_node(name="new_name")
        change = _make_modified_change(old, new, ["name"])
        assert ManifestDiff.classify_change(change) == ChangeCategory.BREAKING

    def test_mixed_description_and_column_removed_is_breaking(self):
        """18. Mixed changes (description + column removed) -> BREAKING (highest wins)"""
        old = _make_node(
            description="Old",
            columns={"id": "INTEGER", "name": "VARCHAR"},
        )
        new = _make_node(
            description="New",
            columns={"id": "INTEGER"},
        )
        change = _make_modified_change(old, new, ["description", "columns"])
        assert ManifestDiff.classify_change(change) == ChangeCategory.BREAKING

    def test_added_change_type_is_non_breaking(self):
        """ADDED changes are classified as NON_BREAKING."""
        change = NodeChange(
            node_id="source.new",
            change_type=DiffType.ADDED,
        )
        assert ManifestDiff.classify_change(change) == ChangeCategory.NON_BREAKING

    def test_removed_change_type_is_non_breaking(self):
        """REMOVED changes are classified as NON_BREAKING."""
        change = NodeChange(
            node_id="source.old",
            change_type=DiffType.REMOVED,
        )
        assert ManifestDiff.classify_change(change) == ChangeCategory.NON_BREAKING


# ===========================================================================
# Downstream propagation
# ===========================================================================

class TestDownstreamPropagation:
    def test_metadata_does_not_propagate_downstream(self):
        """20. METADATA does NOT propagate downstream"""
        manifest = _make_chain_manifest()

        old_a = _make_node("source.a", "a", NodeType.SOURCE, description="old")
        new_a = _make_node("source.a", "a", NodeType.SOURCE, description="new")

        diff = ManifestDiff()
        diff.modified_nodes["source.a"] = _make_modified_change(
            old_a, new_a, ["description"]
        )

        rebuild = diff.get_nodes_to_rebuild(manifest)

        assert rebuild["source.a"] == ChangeCategory.METADATA
        # Downstream should NOT be included
        assert "transform.b" not in rebuild
        assert "feature_group.c" not in rebuild

    def test_breaking_propagates_downstream(self):
        """21. BREAKING propagates downstream"""
        manifest = _make_chain_manifest()

        old_a = _make_node("source.a", "a", NodeType.SOURCE, columns={"id": "INT", "x": "VARCHAR"})
        new_a = _make_node("source.a", "a", NodeType.SOURCE, columns={"id": "INT"})

        diff = ManifestDiff()
        diff.modified_nodes["source.a"] = _make_modified_change(
            old_a, new_a, ["columns"]
        )

        rebuild = diff.get_nodes_to_rebuild(manifest)

        assert rebuild["source.a"] == ChangeCategory.BREAKING
        assert "transform.b" in rebuild
        assert "feature_group.c" in rebuild

    def test_non_breaking_propagates_downstream(self):
        """22. NON_BREAKING propagates downstream"""
        manifest = _make_chain_manifest()

        old_a = _make_node("source.a", "a", NodeType.SOURCE, config={"sql": "SELECT 1"})
        new_a = _make_node("source.a", "a", NodeType.SOURCE, config={"sql": "SELECT 2"})

        diff = ManifestDiff()
        diff.modified_nodes["source.a"] = _make_modified_change(
            old_a, new_a, ["config"]
        )

        rebuild = diff.get_nodes_to_rebuild(manifest)

        assert rebuild["source.a"] == ChangeCategory.NON_BREAKING
        assert "transform.b" in rebuild
        assert "feature_group.c" in rebuild

    def test_new_node_in_rebuild_as_non_breaking(self):
        """23. New node -> included in rebuild set as NON_BREAKING"""
        manifest = _make_chain_manifest()

        diff = ManifestDiff()
        diff.added_nodes["source.new"] = _make_node("source.new", "new")

        rebuild = diff.get_nodes_to_rebuild(manifest)
        assert rebuild["source.new"] == ChangeCategory.NON_BREAKING

    def test_removed_node_not_in_rebuild(self):
        """24. Removed node -> NOT in rebuild set (it's gone)"""
        manifest = _make_chain_manifest()

        diff = ManifestDiff()
        diff.removed_nodes["source.gone"] = _make_node("source.gone", "gone")

        rebuild = diff.get_nodes_to_rebuild(manifest)
        assert "source.gone" not in rebuild


# ===========================================================================
# Plan output formatting
# ===========================================================================

class TestPlanOutput:
    def test_format_plan_output_includes_categories(self):
        """25. Plan output formatting includes categories and downstream counts"""
        manifest = _make_chain_manifest()

        old_a = _make_node("source.a", "a", NodeType.SOURCE, columns={"id": "INT", "x": "VARCHAR"})
        new_a = _make_node("source.a", "a", NodeType.SOURCE, columns={"id": "INT"})

        diff = ManifestDiff()
        diff.modified_nodes["source.a"] = _make_modified_change(
            old_a, new_a, ["columns"]
        )
        diff.added_nodes["source.new"] = _make_node("source.new", "new")
        diff.removed_nodes["source.old"] = _make_node("source.old", "old")

        output = diff.format_plan_output(manifest)

        assert "BREAKING" in output
        assert "rebuilds downstream" in output
        assert "[NEW]" in output
        assert "[REMOVED]" in output
        assert "Downstream impact" in output
        assert "Summary:" in output


# ===========================================================================
# Empty diff and return type
# ===========================================================================

class TestEdgeCases:
    def test_empty_diff_no_changes(self):
        """26. Empty diff -> no changes"""
        manifest = _make_chain_manifest()

        diff = ManifestDiff()
        rebuild = diff.get_nodes_to_rebuild(manifest)
        assert rebuild == {}

    def test_get_nodes_to_rebuild_returns_dict(self):
        """27. get_nodes_to_rebuild returns dict[str, ChangeCategory]"""
        manifest = _make_chain_manifest()

        old_a = _make_node("source.a", "a", NodeType.SOURCE, config={"sql": "SELECT 1"})
        new_a = _make_node("source.a", "a", NodeType.SOURCE, config={"sql": "SELECT 2"})

        diff = ManifestDiff()
        diff.modified_nodes["source.a"] = _make_modified_change(
            old_a, new_a, ["config"]
        )

        rebuild = diff.get_nodes_to_rebuild(manifest)

        assert isinstance(rebuild, dict)
        for node_id, cat in rebuild.items():
            assert isinstance(node_id, str)
            assert isinstance(cat, ChangeCategory)

    def test_classify_change_populates_category_on_node_change(self):
        """classify_change sets category on the NodeChange."""
        old = _make_node(description="a")
        new = _make_node(description="b")
        change = _make_modified_change(old, new, ["description"])

        cat = ManifestDiff.classify_change(change)
        assert cat == ChangeCategory.METADATA

    def test_build_change_details_columns_added(self):
        """_build_change_details describes column additions."""
        old = _make_node(columns={"id": "INTEGER"})
        new = _make_node(columns={"id": "INTEGER", "email": "VARCHAR"})
        change = _make_modified_change(old, new, ["columns"])

        diff = ManifestDiff()
        details = diff._build_change_details(change)

        assert "columns" in details
        assert "added: email" in details["columns"]

    def test_build_change_details_config_keys(self):
        """_build_change_details lists changed config keys."""
        old = _make_node(config={"sql": "SELECT 1", "timeout": 30})
        new = _make_node(config={"sql": "SELECT 2", "timeout": 30})
        change = _make_modified_change(old, new, ["config"])

        diff = ManifestDiff()
        details = diff._build_change_details(change)

        assert "config" in details
        assert "sql" in details["config"]

    def test_bfs_downstream(self):
        """_bfs_downstream finds all transitive downstream nodes."""
        manifest = _make_chain_manifest()
        diff = ManifestDiff()
        downstream = diff._bfs_downstream("source.a", manifest)

        assert "transform.b" in downstream
        assert "feature_group.c" in downstream
        assert "source.a" not in downstream

    def test_metadata_fields_constant(self):
        """METADATA_FIELDS contains the expected fields."""
        assert METADATA_FIELDS == {"description", "owner", "tags"}

    def test_breaking_config_keys_constant(self):
        """BREAKING_CONFIG_KEYS contains the expected keys."""
        assert "entity" in BREAKING_CONFIG_KEYS
        assert "source_type" in BREAKING_CONFIG_KEYS

    def test_metadata_config_keys_constant(self):
        """METADATA_CONFIG_KEYS contains 'audits'."""
        assert "audits" in METADATA_CONFIG_KEYS

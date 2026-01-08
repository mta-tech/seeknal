"""Tests for the dependency registry."""
import pytest
from seeknal.dag.registry import DependencyRegistry, DependencyType


class TestDependencyRegistry:
    """Test the DependencyRegistry class."""

    def test_registry_is_singleton(self):
        """Registry should be a singleton."""
        reg1 = DependencyRegistry.get_instance()
        reg2 = DependencyRegistry.get_instance()
        assert reg1 is reg2

    def test_registry_reset(self):
        """Registry can be reset for testing."""
        reg = DependencyRegistry.get_instance()
        reg.register_dependency("node1", "source", "traffic_day", DependencyType.SOURCE)
        reg.reset()
        assert len(reg.get_all_dependencies()) == 0

    def test_register_source_dependency(self):
        """Can register a source dependency."""
        reg = DependencyRegistry.get_instance()
        reg.reset()

        reg.register_dependency(
            node_id="feature_group.user_features",
            dependency_id="traffic_day",
            dependency_type=DependencyType.SOURCE
        )

        deps = reg.get_dependencies_for_node("feature_group.user_features")
        assert len(deps) == 1
        assert deps[0].dependency_id == "traffic_day"
        assert deps[0].dependency_type == DependencyType.SOURCE

    def test_register_ref_dependency(self):
        """Can register a ref dependency."""
        reg = DependencyRegistry.get_instance()
        reg.reset()

        reg.register_dependency(
            node_id="model.churn_predictor",
            dependency_id="feature_group.user_features",
            dependency_type=DependencyType.REF
        )

        deps = reg.get_dependencies_for_node("model.churn_predictor")
        assert len(deps) == 1
        assert deps[0].dependency_type == DependencyType.REF

    def test_register_transform_dependency(self):
        """Can register a transform dependency."""
        reg = DependencyRegistry.get_instance()
        reg.reset()

        reg.register_dependency(
            node_id="feature_group.user_features",
            dependency_id="rename_subscriber_type",
            dependency_type=DependencyType.TRANSFORM
        )

        deps = reg.get_dependencies_for_node("feature_group.user_features")
        assert len(deps) == 1
        assert deps[0].dependency_type == DependencyType.TRANSFORM

    def test_register_rule_dependency(self):
        """Can register a rule dependency."""
        reg = DependencyRegistry.get_instance()
        reg.reset()

        reg.register_dependency(
            node_id="feature_group.user_features",
            dependency_id="callExpression",
            dependency_type=DependencyType.RULE
        )

        deps = reg.get_dependencies_for_node("feature_group.user_features")
        assert len(deps) == 1
        assert deps[0].dependency_type == DependencyType.RULE

    def test_get_all_dependencies(self):
        """Can get all registered dependencies."""
        reg = DependencyRegistry.get_instance()
        reg.reset()

        reg.register_dependency("node1", "source1", DependencyType.SOURCE)
        reg.register_dependency("node1", "node2", DependencyType.REF)
        reg.register_dependency("node2", "source2", DependencyType.SOURCE)

        all_deps = reg.get_all_dependencies()
        assert len(all_deps) == 3

    def test_set_current_node(self):
        """Can set the current node context."""
        reg = DependencyRegistry.get_instance()
        reg.reset()

        reg.set_current_node("feature_group.user_features")
        assert reg.get_current_node() == "feature_group.user_features"

    def test_clear_current_node(self):
        """Can clear the current node context."""
        reg = DependencyRegistry.get_instance()
        reg.reset()

        reg.set_current_node("feature_group.user_features")
        reg.clear_current_node()
        assert reg.get_current_node() is None

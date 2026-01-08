"""Tests for dependency declaration functions."""
import pytest
import pandas as pd
from seeknal.dag.functions import source, ref, use_transform, use_rule
from seeknal.dag.registry import DependencyRegistry, DependencyType


class TestSourceFunction:
    """Test the source() function."""

    def setup_method(self):
        """Reset registry before each test."""
        DependencyRegistry.get_instance().reset()

    def test_source_registers_dependency(self):
        """source() should register a SOURCE dependency."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        result = source("traffic_day")

        deps = reg.get_dependencies_for_node("feature_group.test")
        assert len(deps) == 1
        assert deps[0].dependency_id == "source.traffic_day"
        assert deps[0].dependency_type == DependencyType.SOURCE

    def test_source_returns_source_reference(self):
        """source() should return a SourceReference object."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        result = source("traffic_day")

        assert result.name == "traffic_day"
        assert result.source_type == "common_config"

    def test_source_with_inline_declaration(self):
        """source() should support inline source declaration."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        result = source("hive", "eureka_feateng.user_events")

        assert result.name == "eureka_feateng.user_events"
        assert result.source_type == "hive"


class TestRefFunction:
    """Test the ref() function."""

    def setup_method(self):
        """Reset registry before each test."""
        DependencyRegistry.get_instance().reset()

    def test_ref_registers_dependency(self):
        """ref() should register a REF dependency."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("model.churn_predictor")

        result = ref("user_features")

        deps = reg.get_dependencies_for_node("model.churn_predictor")
        assert len(deps) == 1
        assert deps[0].dependency_id == "user_features"
        assert deps[0].dependency_type == DependencyType.REF

    def test_ref_returns_node_reference(self):
        """ref() should return a NodeReference object."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("model.churn_predictor")

        result = ref("user_features")

        assert result.name == "user_features"


class TestUseTransformFunction:
    """Test the use_transform() function."""

    def setup_method(self):
        """Reset registry before each test."""
        DependencyRegistry.get_instance().reset()

    def test_use_transform_registers_dependency(self):
        """use_transform() should register a TRANSFORM dependency."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        df = pd.DataFrame({"a": [1, 2, 3]})
        result = use_transform("rename_subscriber_type", df)

        deps = reg.get_dependencies_for_node("feature_group.test")
        assert len(deps) == 1
        assert deps[0].dependency_id == "transform.rename_subscriber_type"
        assert deps[0].dependency_type == DependencyType.TRANSFORM

    def test_use_transform_with_params(self):
        """use_transform() should pass through parameters."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        df = pd.DataFrame({"a": [1, 2, 3]})
        result = use_transform(
            "rename_column_values",
            df,
            input_col="a",
            output_col="b"
        )

        deps = reg.get_dependencies_for_node("feature_group.test")
        assert deps[0].params["input_col"] == "a"
        assert deps[0].params["output_col"] == "b"


class TestUseRuleFunction:
    """Test the use_rule() function."""

    def setup_method(self):
        """Reset registry before each test."""
        DependencyRegistry.get_instance().reset()

    def test_use_rule_registers_dependency(self):
        """use_rule() should register a RULE dependency."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        result = use_rule("callExpression")

        deps = reg.get_dependencies_for_node("feature_group.test")
        assert len(deps) == 1
        assert deps[0].dependency_id == "rule.callExpression"
        assert deps[0].dependency_type == DependencyType.RULE

    def test_use_rule_with_params(self):
        """use_rule() should support parameterized rules."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        result = use_rule("filter_by_service", service_type="Voice")

        deps = reg.get_dependencies_for_node("feature_group.test")
        assert deps[0].params["service_type"] == "Voice"

    def test_use_rule_returns_expression(self):
        """use_rule() should return a RuleExpression for filtering."""
        reg = DependencyRegistry.get_instance()
        reg.set_current_node("feature_group.test")

        result = use_rule("callExpression")

        assert result.rule_id == "callExpression"

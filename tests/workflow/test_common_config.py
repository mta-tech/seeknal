"""Tests for the common configuration layer.

Covers:
- Pydantic models (SourceDef, RuleDef, TransformationDef)
- YAML loader (load_common_config)
- ParameterResolver integration (dotted expression resolution)
- DAG builder filtering (seeknal/common/ excluded from nodes)
- Error handling and suggestions
"""

import textwrap
from pathlib import Path

import pytest
import yaml

from seeknal.workflow.common.loader import load_common_config
from seeknal.workflow.common.models import (
    RuleDef,
    RulesConfig,
    SourceDef,
    SourcesConfig,
    TransformationDef,
    TransformationsConfig,
)
from seeknal.workflow.parameters.resolver import ParameterResolver


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestSourceDef:
    def test_basic_source(self):
        s = SourceDef(id="traffic", ref="source.traffic_daily", params={"dateCol": "date_id"})
        assert s.id == "traffic"
        assert s.ref == "source.traffic_daily"
        assert s.params == {"dateCol": "date_id"}

    def test_empty_params_default(self):
        s = SourceDef(id="traffic", ref="source.traffic_daily")
        assert s.params == {}

    def test_empty_id_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            SourceDef(id="", ref="source.x")

    def test_whitespace_id_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            SourceDef(id="   ", ref="source.x")

    def test_id_stripped(self):
        s = SourceDef(id=" traffic ", ref="source.x")
        assert s.id == "traffic"


class TestRuleDef:
    def test_basic_rule(self):
        r = RuleDef(id="callExpr", value="service_type = 'Voice'")
        assert r.id == "callExpr"
        assert r.value == "service_type = 'Voice'"

    def test_empty_id_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            RuleDef(id="", value="x")

    def test_empty_value_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            RuleDef(id="r", value="")

    def test_value_stripped(self):
        r = RuleDef(id="r", value="  x = 1  ")
        assert r.value == "x = 1"


class TestTransformationDef:
    def test_basic_transformation(self):
        t = TransformationDef(id="enrich", sql="LEFT JOIN t ON a.id = t.id")
        assert t.id == "enrich"
        assert t.sql == "LEFT JOIN t ON a.id = t.id"

    def test_empty_id_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            TransformationDef(id="", sql="SELECT 1")

    def test_empty_sql_rejected(self):
        with pytest.raises(ValueError, match="must not be empty"):
            TransformationDef(id="t", sql="")


class TestTopLevelConfigs:
    def test_sources_config(self):
        cfg = SourcesConfig(sources=[
            SourceDef(id="a", ref="source.a", params={"x": "1"}),
            SourceDef(id="b", ref="source.b"),
        ])
        assert len(cfg.sources) == 2

    def test_rules_config(self):
        cfg = RulesConfig(rules=[RuleDef(id="r", value="x = 1")])
        assert len(cfg.rules) == 1

    def test_transformations_config(self):
        cfg = TransformationsConfig(
            transformations=[TransformationDef(id="t", sql="SELECT 1")]
        )
        assert len(cfg.transformations) == 1

    def test_empty_lists_default(self):
        assert SourcesConfig().sources == []
        assert RulesConfig().rules == []
        assert TransformationsConfig().transformations == []


# ---------------------------------------------------------------------------
# Loader tests
# ---------------------------------------------------------------------------


@pytest.fixture
def common_dir(tmp_path):
    """Create a seeknal/common directory with sample YAML files."""
    d = tmp_path / "seeknal" / "common"
    d.mkdir(parents=True)
    return d


class TestLoadCommonConfig:
    def test_returns_none_when_dir_missing(self, tmp_path):
        result = load_common_config(tmp_path / "nonexistent")
        assert result is None

    def test_returns_none_when_dir_empty(self, common_dir):
        result = load_common_config(common_dir)
        assert result is None

    def test_loads_sources(self, common_dir):
        (common_dir / "sources.yml").write_text(
            yaml.dump({
                "sources": [
                    {
                        "id": "traffic",
                        "ref": "source.traffic_daily",
                        "params": {"dateCol": "date_id", "idCol": "msisdn"},
                    }
                ]
            })
        )
        result = load_common_config(common_dir)
        assert result is not None
        assert result["traffic.dateCol"] == "date_id"
        assert result["traffic.idCol"] == "msisdn"
        assert result["traffic.ref"] == "source.traffic_daily"

    def test_loads_rules(self, common_dir):
        (common_dir / "rules.yml").write_text(
            yaml.dump({
                "rules": [
                    {"id": "callExpression", "value": "service_type = 'Voice'"},
                    {"id": "dataFilter", "value": "date_id >= '2025-01-01'"},
                ]
            })
        )
        result = load_common_config(common_dir)
        assert result is not None
        assert result["rules.callExpression"] == "service_type = 'Voice'"
        assert result["rules.dataFilter"] == "date_id >= '2025-01-01'"

    def test_loads_transformations(self, common_dir):
        (common_dir / "transformations.yml").write_text(
            yaml.dump({
                "transformations": [
                    {"id": "enrichWithRegion", "sql": "LEFT JOIN regions r ON t.rid = r.id"},
                ]
            })
        )
        result = load_common_config(common_dir)
        assert result is not None
        assert result["transforms.enrichWithRegion"] == "LEFT JOIN regions r ON t.rid = r.id"

    def test_loads_all_files(self, common_dir):
        (common_dir / "sources.yml").write_text(
            yaml.dump({"sources": [{"id": "s", "ref": "source.s", "params": {"a": "1"}}]})
        )
        (common_dir / "rules.yml").write_text(
            yaml.dump({"rules": [{"id": "r", "value": "x = 1"}]})
        )
        (common_dir / "transformations.yml").write_text(
            yaml.dump({"transformations": [{"id": "t", "sql": "SELECT 1"}]})
        )
        result = load_common_config(common_dir)
        assert result is not None
        assert "s.a" in result
        assert "rules.r" in result
        assert "transforms.t" in result

    def test_strict_raises_on_invalid_yaml(self, common_dir):
        (common_dir / "sources.yml").write_text("not: valid: yaml: [")
        with pytest.raises(ValueError, match="Failed to parse"):
            load_common_config(common_dir, strict=True)

    def test_non_strict_logs_warning_on_invalid_yaml(self, common_dir):
        (common_dir / "sources.yml").write_text("not: valid: yaml: [")
        result = load_common_config(common_dir)
        # Should not raise, returns None since no valid entries
        assert result is None

    def test_multiple_sources(self, common_dir):
        (common_dir / "sources.yml").write_text(
            yaml.dump({
                "sources": [
                    {"id": "a", "ref": "source.a", "params": {"x": "1"}},
                    {"id": "b", "ref": "source.b", "params": {"y": "2"}},
                ]
            })
        )
        result = load_common_config(common_dir)
        assert result["a.x"] == "1"
        assert result["b.y"] == "2"

    def test_source_without_params(self, common_dir):
        (common_dir / "sources.yml").write_text(
            yaml.dump({"sources": [{"id": "s", "ref": "source.s"}]})
        )
        result = load_common_config(common_dir)
        assert result is not None
        # Only the .ref key should exist
        assert result["s.ref"] == "source.s"
        assert len(result) == 1

    def test_strict_raises_on_validation_error(self, common_dir):
        # Missing required field 'value'
        (common_dir / "rules.yml").write_text(
            yaml.dump({"rules": [{"id": "r"}]})
        )
        with pytest.raises(ValueError, match="Failed to parse"):
            load_common_config(common_dir, strict=True)


# ---------------------------------------------------------------------------
# ParameterResolver integration tests
# ---------------------------------------------------------------------------


class TestResolverCommonConfig:
    def test_dotted_expression_resolved(self):
        common = {"traffic.dateCol": "date_id", "rules.callExpr": "x = 1"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve_string("WHERE {{ traffic.dateCol }} > 0")
        assert result == "WHERE date_id > 0"

    def test_rule_expression_resolved(self):
        common = {"rules.callExpr": "service_type = 'Voice'"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve_string("WHERE {{ rules.callExpr }}")
        assert result == "WHERE service_type = 'Voice'"

    def test_transform_expression_resolved(self):
        common = {"transforms.enrich": "LEFT JOIN regions r ON t.rid = r.id"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve_string("SELECT * FROM t {{ transforms.enrich }}")
        assert result == "SELECT * FROM t LEFT JOIN regions r ON t.rid = r.id"

    def test_multiple_expressions_in_one_string(self):
        common = {"traffic.dateCol": "date_id", "traffic.idCol": "msisdn"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve_string(
            "SELECT {{ traffic.idCol }}, {{ traffic.dateCol }} FROM t"
        )
        assert result == "SELECT msisdn, date_id FROM t"

    def test_missing_key_raises_with_suggestions(self):
        common = {"traffic.dateCol": "date_id", "traffic.idCol": "msisdn"}
        resolver = ParameterResolver(common_config=common)
        with pytest.raises(ValueError, match="Unknown common config key"):
            resolver.resolve_string("{{ traffic.datCol }}")

    def test_missing_key_error_includes_available_keys(self):
        common = {"traffic.dateCol": "date_id"}
        resolver = ParameterResolver(common_config=common)
        with pytest.raises(ValueError, match="traffic.dateCol"):
            resolver.resolve_string("{{ traffic.badKey }}")

    def test_no_common_config_passthrough(self):
        """Without common config, dotted expressions pass through unchanged."""
        resolver = ParameterResolver()
        result = resolver.resolve_string("{{ traffic.dateCol }}")
        assert result == "{{ traffic.dateCol }}"

    def test_context_values_take_precedence(self):
        """Context values should resolve before common config lookup."""
        common = {"run_id": "should-not-win"}
        resolver = ParameterResolver(common_config=common, run_id="my-run-id")
        result = resolver.resolve_string("{{ run_id }}")
        assert result == "my-run-id"

    def test_function_takes_precedence_over_common(self):
        """Built-in functions should resolve before common config lookup."""
        from datetime import date

        common = {"today.value": "not-a-date"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve_string("{{ today }}")
        # 'today' is a function, not dotted -- it should call the function
        assert result == date.today().isoformat()

    def test_resolve_dict_with_common_config(self):
        common = {"traffic.dateCol": "date_id"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve({"col": "{{ traffic.dateCol }}"})
        assert result == {"col": "date_id"}

    def test_resolve_string_no_type_conversion(self):
        """resolve_string should NOT convert '100' to int."""
        common = {"metric.threshold": "100"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve_string("{{ metric.threshold }}")
        assert result == "100"
        assert isinstance(result, str)

    def test_mixed_common_and_builtin(self):
        from datetime import date

        common = {"traffic.dateCol": "date_id"}
        resolver = ParameterResolver(common_config=common)
        result = resolver.resolve_string(
            "WHERE {{ traffic.dateCol }} = '{{ today }}'"
        )
        assert f"WHERE date_id = '{date.today().isoformat()}'" == result


# ---------------------------------------------------------------------------
# DAG builder filtering tests
# ---------------------------------------------------------------------------


class TestDAGBuilderCommonFiltering:
    def test_common_yaml_excluded_from_discovery(self, tmp_path):
        """YAML files in seeknal/common/ should not be discovered as nodes."""
        seeknal_dir = tmp_path / "seeknal"
        (seeknal_dir / "sources").mkdir(parents=True)
        (seeknal_dir / "common").mkdir(parents=True)

        # Regular source node
        (seeknal_dir / "sources" / "users.yml").write_text(
            yaml.dump({"kind": "source", "name": "users", "source": "csv", "table": "u.csv"})
        )
        # Common config file -- should NOT become a node
        (seeknal_dir / "common" / "sources.yml").write_text(
            yaml.dump({"sources": [{"id": "s", "ref": "source.s", "params": {"a": "1"}}]})
        )

        from seeknal.workflow.dag import DAGBuilder

        builder = DAGBuilder(project_path=tmp_path)
        yaml_files = builder._discover_yaml_files()
        file_names = [f.name for f in yaml_files]
        assert "users.yml" in file_names
        assert "sources.yml" not in file_names

    def test_common_config_loaded_during_build(self, tmp_path):
        """build() should populate _common_config from seeknal/common/."""
        seeknal_dir = tmp_path / "seeknal"
        (seeknal_dir / "sources").mkdir(parents=True)
        (seeknal_dir / "common").mkdir(parents=True)

        (seeknal_dir / "sources" / "users.yml").write_text(
            yaml.dump({"kind": "source", "name": "users", "source": "csv", "table": "u.csv"})
        )
        (seeknal_dir / "common" / "rules.yml").write_text(
            yaml.dump({"rules": [{"id": "active", "value": "status = 'active'"}]})
        )

        from seeknal.workflow.dag import DAGBuilder

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()
        assert builder._common_config is not None
        assert builder._common_config["rules.active"] == "status = 'active'"

    def test_transform_sql_resolved_during_build(self, tmp_path):
        """Transform SQL with {{ }} common refs should be resolved at build time."""
        seeknal_dir = tmp_path / "seeknal"
        (seeknal_dir / "transforms").mkdir(parents=True)
        (seeknal_dir / "common").mkdir(parents=True)

        (seeknal_dir / "common" / "rules.yml").write_text(
            yaml.dump({"rules": [{"id": "active", "value": "status = 'active'"}]})
        )
        (seeknal_dir / "transforms" / "filtered.yml").write_text(
            yaml.dump({
                "kind": "transform",
                "name": "filtered",
                "transform": "SELECT * FROM t WHERE {{ rules.active }}",
            })
        )

        from seeknal.workflow.dag import DAGBuilder

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()

        node = builder.get_node("transform.filtered")
        assert node is not None
        assert node.yaml_data["transform"] == "SELECT * FROM t WHERE status = 'active'"

    def test_build_without_common_dir(self, tmp_path):
        """Build should work fine when seeknal/common/ does not exist."""
        seeknal_dir = tmp_path / "seeknal"
        (seeknal_dir / "sources").mkdir(parents=True)
        (seeknal_dir / "sources" / "s.yml").write_text(
            yaml.dump({"kind": "source", "name": "s", "source": "csv", "table": "s.csv"})
        )

        from seeknal.workflow.dag import DAGBuilder

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()
        assert builder._common_config is None
        assert "source.s" in builder.nodes

"""Tests for semantic layer models, parsing, and validation."""
import pytest
import yaml

from seeknal.workflow.semantic.models import (
    AggregationType,
    Dimension,
    DimensionType,
    Entity,
    EntityGraph,
    EntityType,
    Measure,
    Metric,
    MetricQuery,
    MetricRegistry,
    MetricType,
    SemanticModel,
    parse_metric_yaml,
    parse_semantic_model_yaml,
)
from seeknal.dag.manifest import NodeType
from seeknal.dag.parser import ProjectParser


# ── Entity Tests ────────────────────────────────────────────────────────────


class TestEntity:
    def test_from_dict_primary(self):
        e = Entity.from_dict({"name": "order_id", "type": "primary"})
        assert e.name == "order_id"
        assert e.type == EntityType.PRIMARY

    def test_from_dict_foreign(self):
        e = Entity.from_dict({"name": "customer_id", "type": "foreign"})
        assert e.type == EntityType.FOREIGN

    def test_from_dict_default_type(self):
        e = Entity.from_dict({"name": "id"})
        assert e.type == EntityType.PRIMARY


# ── Dimension Tests ─────────────────────────────────────────────────────────


class TestDimension:
    def test_time_dimension(self):
        d = Dimension.from_dict({
            "name": "ordered_at",
            "type": "time",
            "expr": "ordered_at",
            "time_granularity": "day",
        })
        assert d.type == DimensionType.TIME
        assert d.time_granularity == "day"
        assert d.expr == "ordered_at"

    def test_categorical_dimension(self):
        d = Dimension.from_dict({"name": "region", "type": "categorical"})
        assert d.type == DimensionType.CATEGORICAL
        assert d.expr == "region"  # defaults to name

    def test_default_type(self):
        d = Dimension.from_dict({"name": "status"})
        assert d.type == DimensionType.CATEGORICAL


# ── Measure Tests ───────────────────────────────────────────────────────────


class TestMeasure:
    def test_sum_measure(self):
        m = Measure.from_dict({"name": "revenue", "expr": "amount", "agg": "sum"})
        assert m.agg == AggregationType.SUM
        assert m.expr == "amount"

    def test_count_distinct(self):
        m = Measure.from_dict({"name": "unique_users", "expr": "user_id", "agg": "count_distinct"})
        assert m.agg == AggregationType.COUNT_DISTINCT

    def test_avg_normalizes_to_average(self):
        m = Measure.from_dict({"name": "avg_val", "expr": "value", "agg": "avg"})
        assert m.agg == AggregationType.AVERAGE

    def test_default_expr(self):
        m = Measure.from_dict({"name": "total", "agg": "sum"})
        assert m.expr == "total"  # defaults to name


# ── SemanticModel Tests ─────────────────────────────────────────────────────


ORDERS_MODEL_DICT = {
    "kind": "semantic_model",
    "name": "orders",
    "description": "Order fact table",
    "model": "ref('transform.order_summary')",
    "default_time_dimension": "ordered_at",
    "entities": [
        {"name": "order_id", "type": "primary"},
        {"name": "customer_id", "type": "foreign"},
    ],
    "dimensions": [
        {"name": "ordered_at", "type": "time", "expr": "ordered_at", "time_granularity": "day"},
        {"name": "region", "type": "categorical"},
    ],
    "measures": [
        {"name": "revenue", "expr": "amount", "agg": "sum"},
        {"name": "order_count", "expr": "1", "agg": "sum"},
    ],
}


class TestSemanticModel:
    def test_from_dict(self):
        sm = SemanticModel.from_dict(ORDERS_MODEL_DICT)
        assert sm.name == "orders"
        assert sm.model_ref == "ref('transform.order_summary')"
        assert sm.default_time_dimension == "ordered_at"
        assert len(sm.entities) == 2
        assert len(sm.dimensions) == 2
        assert len(sm.measures) == 2

    def test_get_entity(self):
        sm = SemanticModel.from_dict(ORDERS_MODEL_DICT)
        assert sm.get_entity("order_id") is not None
        assert sm.get_entity("nonexistent") is None

    def test_get_primary_entity(self):
        sm = SemanticModel.from_dict(ORDERS_MODEL_DICT)
        primary = sm.get_primary_entity()
        assert primary is not None
        assert primary.name == "order_id"

    def test_get_measure(self):
        sm = SemanticModel.from_dict(ORDERS_MODEL_DICT)
        assert sm.get_measure("revenue") is not None
        assert sm.get_measure("nonexistent") is None

    def test_get_dimension(self):
        sm = SemanticModel.from_dict(ORDERS_MODEL_DICT)
        assert sm.get_dimension("ordered_at") is not None

    def test_validate_valid(self):
        sm = SemanticModel.from_dict(ORDERS_MODEL_DICT)
        errors = sm.validate()
        assert errors == []

    def test_validate_missing_model_ref(self):
        data = {**ORDERS_MODEL_DICT, "model": ""}
        sm = SemanticModel.from_dict(data)
        errors = sm.validate()
        assert any("missing 'model'" in e for e in errors)

    def test_validate_no_entities(self):
        data = {**ORDERS_MODEL_DICT, "entities": []}
        sm = SemanticModel.from_dict(data)
        errors = sm.validate()
        assert any("no entities" in e for e in errors)

    def test_validate_no_measures(self):
        data = {**ORDERS_MODEL_DICT, "measures": []}
        sm = SemanticModel.from_dict(data)
        errors = sm.validate()
        assert any("no measures" in e for e in errors)

    def test_validate_bad_default_time_dimension(self):
        data = {**ORDERS_MODEL_DICT, "default_time_dimension": "nonexistent"}
        sm = SemanticModel.from_dict(data)
        errors = sm.validate()
        assert any("not found in dimensions" in e for e in errors)

    def test_validate_non_time_default_time_dimension(self):
        data = {**ORDERS_MODEL_DICT, "default_time_dimension": "region"}
        sm = SemanticModel.from_dict(data)
        errors = sm.validate()
        assert any("not a time dimension" in e for e in errors)

    def test_validate_duplicate_entity_names(self):
        data = {
            **ORDERS_MODEL_DICT,
            "entities": [
                {"name": "order_id", "type": "primary"},
                {"name": "order_id", "type": "foreign"},
            ],
        }
        sm = SemanticModel.from_dict(data)
        errors = sm.validate()
        assert any("duplicate entity" in e for e in errors)


# ── Metric Tests ────────────────────────────────────────────────────────────


class TestMetric:
    def test_simple_metric(self):
        m = Metric.from_dict({
            "name": "total_revenue",
            "type": "simple",
            "measure": "revenue",
            "filter": "status != 'refunded'",
        })
        assert m.type == MetricType.SIMPLE
        assert m.measure == "revenue"
        assert m.filter == "status != 'refunded'"

    def test_ratio_metric(self):
        m = Metric.from_dict({
            "name": "revenue_per_customer",
            "type": "ratio",
            "numerator": "revenue",
            "denominator": "unique_customers",
        })
        assert m.type == MetricType.RATIO
        assert m.numerator == "revenue"
        assert m.denominator == "unique_customers"

    def test_cumulative_metric(self):
        m = Metric.from_dict({
            "name": "mtd_revenue",
            "type": "cumulative",
            "measure": "revenue",
            "grain_to_date": "month",
        })
        assert m.type == MetricType.CUMULATIVE
        assert m.grain_to_date == "month"

    def test_derived_metric(self):
        m = Metric.from_dict({
            "name": "revenue_growth",
            "type": "derived",
            "expr": "(current - previous) / previous",
            "inputs": [
                {"metric": "total_revenue", "alias": "current"},
                {"metric": "total_revenue", "alias": "previous", "offset": "1 month"},
            ],
        })
        assert m.type == MetricType.DERIVED
        assert len(m.inputs) == 2
        assert m.inputs[1].offset == "1 month"

    def test_validate_simple_missing_measure(self):
        m = Metric.from_dict({"name": "bad", "type": "simple"})
        errors = m.validate()
        assert any("missing 'measure'" in e for e in errors)

    def test_validate_ratio_missing_fields(self):
        m = Metric.from_dict({"name": "bad", "type": "ratio"})
        errors = m.validate()
        assert any("missing 'numerator'" in e for e in errors)
        assert any("missing 'denominator'" in e for e in errors)

    def test_validate_cumulative_missing_measure(self):
        m = Metric.from_dict({"name": "bad", "type": "cumulative"})
        errors = m.validate()
        assert any("missing 'measure'" in e for e in errors)

    def test_validate_derived_missing_fields(self):
        m = Metric.from_dict({"name": "bad", "type": "derived"})
        errors = m.validate()
        assert any("missing 'expr'" in e for e in errors)
        assert any("missing 'inputs'" in e for e in errors)

    def test_get_referenced_metrics(self):
        m = Metric.from_dict({
            "name": "growth",
            "type": "derived",
            "expr": "a - b",
            "inputs": [
                {"metric": "total_revenue", "alias": "a"},
                {"metric": "total_orders", "alias": "b"},
            ],
        })
        assert m.get_referenced_metrics() == ["total_revenue", "total_orders"]

    def test_get_referenced_measures_simple(self):
        m = Metric.from_dict({"name": "rev", "type": "simple", "measure": "revenue"})
        assert m.get_referenced_measures() == ["revenue"]

    def test_get_referenced_measures_ratio(self):
        m = Metric.from_dict({
            "name": "rpc",
            "type": "ratio",
            "numerator": "revenue",
            "denominator": "customers",
        })
        assert m.get_referenced_measures() == ["revenue", "customers"]


# ── MetricQuery Tests ───────────────────────────────────────────────────────


class TestMetricQuery:
    def test_from_dict(self):
        q = MetricQuery.from_dict({
            "metrics": ["total_revenue", "order_count"],
            "dimensions": ["region", "ordered_at__month"],
            "filters": ["ordered_at >= '2026-01-01'"],
            "order_by": ["-total_revenue"],
            "limit": 20,
        })
        assert q.metrics == ["total_revenue", "order_count"]
        assert q.limit == 20

    def test_defaults(self):
        q = MetricQuery.from_dict({"metrics": ["rev"]})
        assert q.dimensions == []
        assert q.filters == []
        assert q.limit is None


# ── EntityGraph Tests ───────────────────────────────────────────────────────


class TestEntityGraph:
    def _build_graph(self):
        graph = EntityGraph()
        orders = SemanticModel.from_dict(ORDERS_MODEL_DICT)
        customers = SemanticModel.from_dict({
            "name": "customers",
            "model": "ref('source.customers')",
            "entities": [{"name": "customer_id", "type": "primary"}],
            "dimensions": [{"name": "segment", "type": "categorical"}],
            "measures": [{"name": "customer_count", "expr": "1", "agg": "count"}],
        })
        graph.add_model(orders)
        graph.add_model(customers)
        return graph

    def test_add_and_get_model(self):
        graph = self._build_graph()
        assert graph.get_model("orders") is not None
        assert graph.get_model("customers") is not None
        assert graph.get_model("nonexistent") is None

    def test_get_models_for_entity(self):
        graph = self._build_graph()
        # customer_id is in both orders (foreign) and customers (primary)
        models = graph.get_models_for_entity("customer_id")
        names = [m[0] for m in models]
        assert "orders" in names
        assert "customers" in names

    def test_find_join_path(self):
        graph = self._build_graph()
        path = graph.find_join_path("orders", "customers")
        assert path is not None
        assert len(path) == 1
        assert path[0] == ("customer_id", "customers")

    def test_find_join_path_same_model(self):
        graph = self._build_graph()
        path = graph.find_join_path("orders", "orders")
        assert path == []

    def test_find_join_path_no_path(self):
        graph = EntityGraph()
        graph.add_model(SemanticModel.from_dict({
            "name": "isolated",
            "model": "ref('source.x')",
            "entities": [{"name": "x_id", "type": "primary"}],
            "measures": [{"name": "cnt", "expr": "1", "agg": "count"}],
        }))
        graph.add_model(SemanticModel.from_dict(ORDERS_MODEL_DICT))
        path = graph.find_join_path("isolated", "orders")
        assert path is None

    def test_get_joinable_models(self):
        graph = self._build_graph()
        joinable = graph.get_joinable_models("orders")
        assert "customers" in joinable

    def test_get_joinable_models_unknown(self):
        graph = self._build_graph()
        assert graph.get_joinable_models("nonexistent") == set()


# ── MetricRegistry Tests ───────────────────────────────────────────────────


class TestMetricRegistry:
    def _build_registry_and_graph(self):
        registry = MetricRegistry()
        graph = EntityGraph()
        graph.add_model(SemanticModel.from_dict(ORDERS_MODEL_DICT))

        registry.add(Metric.from_dict({
            "name": "total_revenue",
            "type": "simple",
            "measure": "revenue",
        }))
        registry.add(Metric.from_dict({
            "name": "total_orders",
            "type": "simple",
            "measure": "order_count",
        }))
        return registry, graph

    def test_add_and_get(self):
        registry, _ = self._build_registry_and_graph()
        assert registry.get("total_revenue") is not None
        assert registry.get("nonexistent") is None

    def test_validate_all_valid(self):
        registry, graph = self._build_registry_and_graph()
        errors = registry.validate_all(graph)
        assert errors == []

    def test_validate_missing_measure(self):
        registry = MetricRegistry()
        graph = EntityGraph()
        graph.add_model(SemanticModel.from_dict(ORDERS_MODEL_DICT))

        registry.add(Metric.from_dict({
            "name": "bad_metric",
            "type": "simple",
            "measure": "nonexistent_measure",
        }))
        errors = registry.validate_all(graph)
        assert any("nonexistent_measure" in e for e in errors)

    def test_validate_missing_metric_ref(self):
        registry = MetricRegistry()
        graph = EntityGraph()

        registry.add(Metric.from_dict({
            "name": "derived_bad",
            "type": "derived",
            "expr": "a + b",
            "inputs": [{"metric": "nonexistent", "alias": "a"}],
        }))
        errors = registry.validate_all(graph)
        assert any("nonexistent" in e for e in errors)

    def test_detect_no_cycle(self):
        registry, _ = self._build_registry_and_graph()
        has_cycle, _ = registry.detect_cycles()
        assert not has_cycle

    def test_detect_cycle(self):
        registry = MetricRegistry()
        registry.add(Metric.from_dict({
            "name": "a",
            "type": "derived",
            "expr": "b",
            "inputs": [{"metric": "b", "alias": "b"}],
        }))
        registry.add(Metric.from_dict({
            "name": "b",
            "type": "derived",
            "expr": "a",
            "inputs": [{"metric": "a", "alias": "a"}],
        }))
        has_cycle, cycle_path = registry.detect_cycles()
        assert has_cycle
        assert len(cycle_path) >= 2

    def test_resolve_order(self):
        registry, _ = self._build_registry_and_graph()
        registry.add(Metric.from_dict({
            "name": "derived_total",
            "type": "derived",
            "expr": "a + b",
            "inputs": [
                {"metric": "total_revenue", "alias": "a"},
                {"metric": "total_orders", "alias": "b"},
            ],
        }))
        order = registry.resolve_order()
        # derived_total must come after its dependencies
        assert order.index("total_revenue") < order.index("derived_total")
        assert order.index("total_orders") < order.index("derived_total")

    def test_resolve_order_cycle_raises(self):
        registry = MetricRegistry()
        registry.add(Metric.from_dict({
            "name": "a",
            "type": "derived",
            "expr": "b",
            "inputs": [{"metric": "b", "alias": "b"}],
        }))
        registry.add(Metric.from_dict({
            "name": "b",
            "type": "derived",
            "expr": "a",
            "inputs": [{"metric": "a", "alias": "a"}],
        }))
        with pytest.raises(ValueError, match="Circular"):
            registry.resolve_order()

    def test_find_measure_model(self):
        registry, graph = self._build_registry_and_graph()
        result = registry.find_measure_model("revenue", graph)
        assert result is not None
        assert result[0] == "orders"
        assert result[1].name == "revenue"

    def test_find_measure_model_not_found(self):
        registry, graph = self._build_registry_and_graph()
        assert registry.find_measure_model("nonexistent", graph) is None


# ── YAML Parsing Helper Tests ──────────────────────────────────────────────


class TestYamlParsingHelpers:
    def test_parse_semantic_model_yaml(self):
        sm = parse_semantic_model_yaml(ORDERS_MODEL_DICT)
        assert sm.name == "orders"

    def test_parse_metric_yaml(self):
        m = parse_metric_yaml({
            "name": "total_revenue",
            "type": "simple",
            "measure": "revenue",
        })
        assert m.name == "total_revenue"
        assert m.type == MetricType.SIMPLE


# ── ProjectParser Integration Tests ────────────────────────────────────────


class TestProjectParserSemantic:
    def test_parse_semantic_model_files(self, tmp_path):
        sm_dir = tmp_path / "seeknal" / "semantic_models"
        sm_dir.mkdir(parents=True)

        (sm_dir / "orders.yml").write_text(yaml.dump(ORDERS_MODEL_DICT))

        parser = ProjectParser("test", str(tmp_path))
        manifest = parser.parse()

        node = manifest.get_node("semantic_model.orders")
        assert node is not None
        assert node.node_type == NodeType.SEMANTIC_MODEL
        assert node.config["name"] == "orders"

    def test_parse_metric_files(self, tmp_path):
        metrics_dir = tmp_path / "seeknal" / "metrics"
        metrics_dir.mkdir(parents=True)

        metric_yaml = {
            "kind": "metric",
            "name": "total_revenue",
            "type": "simple",
            "measure": "revenue",
        }
        (metrics_dir / "revenue.yml").write_text(yaml.dump(metric_yaml))

        parser = ProjectParser("test", str(tmp_path))
        manifest = parser.parse()

        node = manifest.get_node("metric.total_revenue")
        assert node is not None
        assert node.node_type == NodeType.METRIC

    def test_parse_multi_document_metrics(self, tmp_path):
        metrics_dir = tmp_path / "seeknal" / "metrics"
        metrics_dir.mkdir(parents=True)

        content = """kind: metric
name: total_revenue
type: simple
measure: revenue
---
kind: metric
name: order_count
type: simple
measure: order_count
"""
        (metrics_dir / "metrics.yml").write_text(content)

        parser = ProjectParser("test", str(tmp_path))
        manifest = parser.parse()

        assert manifest.get_node("metric.total_revenue") is not None
        assert manifest.get_node("metric.order_count") is not None

    def test_skip_non_semantic_model(self, tmp_path):
        sm_dir = tmp_path / "seeknal" / "semantic_models"
        sm_dir.mkdir(parents=True)

        (sm_dir / "other.yml").write_text(yaml.dump({"kind": "source", "name": "x"}))

        parser = ProjectParser("test", str(tmp_path))
        manifest = parser.parse()

        assert manifest.get_node("semantic_model.x") is None

    def test_missing_name_error(self, tmp_path):
        sm_dir = tmp_path / "seeknal" / "semantic_models"
        sm_dir.mkdir(parents=True)

        (sm_dir / "bad.yml").write_text(yaml.dump({"kind": "semantic_model"}))

        parser = ProjectParser("test", str(tmp_path))
        parser.parse()

        errors = parser.get_errors()
        assert any("missing 'name'" in e for e in errors)

    def test_no_semantic_dirs_is_fine(self, tmp_path):
        parser = ProjectParser("test", str(tmp_path))
        manifest = parser.parse()
        assert len(manifest.nodes) == 0


# ── Compiler Tests ──────────────────────────────────────────────────────────

from seeknal.workflow.semantic.compiler import (
    MetricCompiler,
    resolve_dimension_expr,
    validate_filter,
)

CUSTOMERS_MODEL_DICT = {
    "name": "customers",
    "model": "ref('source.customers')",
    "entities": [{"name": "customer_id", "type": "primary"}],
    "dimensions": [{"name": "segment", "type": "categorical"}],
    "measures": [
        {"name": "customer_count", "expr": "1", "agg": "count"},
        {"name": "unique_customers", "expr": "customer_id", "agg": "count_distinct"},
    ],
}


def _build_compiler():
    orders = SemanticModel.from_dict(ORDERS_MODEL_DICT)
    customers = SemanticModel.from_dict(CUSTOMERS_MODEL_DICT)
    metrics = [
        Metric.from_dict({
            "name": "total_revenue",
            "type": "simple",
            "measure": "revenue",
        }),
        Metric.from_dict({
            "name": "total_orders",
            "type": "simple",
            "measure": "order_count",
        }),
        Metric.from_dict({
            "name": "revenue_per_customer",
            "type": "ratio",
            "numerator": "revenue",
            "denominator": "unique_customers",
        }),
        Metric.from_dict({
            "name": "mtd_revenue",
            "type": "cumulative",
            "measure": "revenue",
            "grain_to_date": "month",
        }),
        Metric.from_dict({
            "name": "total_plus_orders",
            "type": "derived",
            "expr": "(a + b)",
            "inputs": [
                {"metric": "total_revenue", "alias": "a"},
                {"metric": "total_orders", "alias": "b"},
            ],
        }),
    ]
    return MetricCompiler([orders, customers], metrics)


class TestValidateFilter:
    def test_valid_filter(self):
        assert validate_filter("status = 'active'") == "status = 'active'"

    def test_sql_injection_drop(self):
        with pytest.raises(ValueError, match="forbidden"):
            validate_filter("1; DROP TABLE users")

    def test_sql_injection_union(self):
        with pytest.raises(ValueError, match="forbidden"):
            validate_filter("1 UNION SELECT * FROM passwords")

    def test_sql_injection_comment(self):
        with pytest.raises(ValueError, match="forbidden"):
            validate_filter("1 -- comment")

    def test_sql_injection_semicolon(self):
        with pytest.raises(ValueError, match="forbidden"):
            validate_filter("x = 1; DELETE FROM users")


class TestResolveDimensionExpr:
    def test_plain_dimension(self):
        models = {"orders": SemanticModel.from_dict(ORDERS_MODEL_DICT)}
        assert resolve_dimension_expr("region", models) == "region"

    def test_time_grain_month(self):
        models = {"orders": SemanticModel.from_dict(ORDERS_MODEL_DICT)}
        result = resolve_dimension_expr("ordered_at__month", models)
        assert result == "date_trunc('month', ordered_at)"

    def test_time_grain_year(self):
        models = {"orders": SemanticModel.from_dict(ORDERS_MODEL_DICT)}
        result = resolve_dimension_expr("ordered_at__year", models)
        assert result == "date_trunc('year', ordered_at)"

    def test_unknown_grain_treated_as_dimension(self):
        models = {"orders": SemanticModel.from_dict(ORDERS_MODEL_DICT)}
        result = resolve_dimension_expr("region__something", models)
        # "something" is not a valid grain, so fallback
        assert result == "region__something"


class TestMetricCompilerSimple:
    def test_simple_metric_no_dimensions(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(metrics=["total_revenue"]))
        assert "SUM(amount)" in sql
        assert "total_revenue" in sql
        assert "FROM" in sql

    def test_simple_metric_with_dimension(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(
            metrics=["total_revenue"],
            dimensions=["region"],
        ))
        assert "SUM(amount) AS total_revenue" in sql
        assert "region" in sql
        assert "GROUP BY" in sql

    def test_simple_metric_with_time_grain(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(
            metrics=["total_revenue"],
            dimensions=["ordered_at__month"],
        ))
        assert "date_trunc('month', ordered_at)" in sql
        assert "GROUP BY" in sql


class TestMetricCompilerRatio:
    def test_ratio_metric(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(metrics=["revenue_per_customer"]))
        assert "SUM(amount)" in sql
        assert "NULLIF" in sql
        assert "COUNT(DISTINCT customer_id)" in sql

    def test_ratio_division_by_zero_protected(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(metrics=["revenue_per_customer"]))
        assert "NULLIF" in sql
        assert ", 0)" in sql


class TestMetricCompilerCumulative:
    def test_cumulative_with_grain(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(metrics=["mtd_revenue"]))
        assert "SUM(amount) OVER" in sql
        assert "PARTITION BY date_trunc('month', ordered_at)" in sql
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in sql


class TestMetricCompilerDerived:
    def test_derived_metric(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(metrics=["total_plus_orders"]))
        assert "SUM(amount)" in sql
        assert "SUM(1)" in sql
        assert "total_plus_orders" in sql


class TestMetricCompilerMultiModel:
    def test_multi_model_join(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(
            metrics=["total_revenue"],
            dimensions=["segment"],  # from customers model
        ))
        assert "JOIN" in sql
        assert "customer_id" in sql


class TestMetricCompilerFilters:
    def test_user_filter(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(
            metrics=["total_revenue"],
            filters=["region = 'US'"],
        ))
        assert "WHERE" in sql
        assert "region = 'US'" in sql

    def test_filter_injection_blocked(self):
        compiler = _build_compiler()
        with pytest.raises(ValueError, match="forbidden"):
            compiler.compile(MetricQuery(
                metrics=["total_revenue"],
                filters=["1; DROP TABLE users"],
            ))


class TestMetricCompilerOrderByLimit:
    def test_order_by_desc(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(
            metrics=["total_revenue"],
            dimensions=["region"],
            order_by=["-total_revenue"],
        ))
        assert "ORDER BY total_revenue DESC" in sql

    def test_order_by_asc(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(
            metrics=["total_revenue"],
            dimensions=["region"],
            order_by=["region"],
        ))
        assert "ORDER BY region ASC" in sql

    def test_limit(self):
        compiler = _build_compiler()
        sql = compiler.compile(MetricQuery(
            metrics=["total_revenue"],
            limit=10,
        ))
        assert "LIMIT 10" in sql


class TestMetricCompilerEdgeCases:
    def test_empty_metrics_raises(self):
        compiler = _build_compiler()
        with pytest.raises(ValueError, match="at least one metric"):
            compiler.compile(MetricQuery(metrics=[]))

    def test_unknown_metric_raises(self):
        compiler = _build_compiler()
        with pytest.raises(ValueError, match="Unknown metric"):
            compiler.compile(MetricQuery(metrics=["nonexistent"]))

"""
Semantic layer data models.

Defines the dataclasses for semantic models (entities, dimensions, measures)
and metrics (simple, ratio, cumulative, derived). Inspired by dbt MetricFlow.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ── Entity ──────────────────────────────────────────────────────────────────

class EntityType(Enum):
    """Entity relationship types."""
    PRIMARY = "primary"
    FOREIGN = "foreign"
    UNIQUE = "unique"


@dataclass(slots=True)
class Entity:
    """An entity column in a semantic model (join key)."""
    name: str
    type: EntityType

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Entity":
        return cls(
            name=data["name"],
            type=EntityType(data.get("type", "primary")),
        )


# ── Dimension ───────────────────────────────────────────────────────────────

class DimensionType(Enum):
    """Dimension types (Cube.js-compatible)."""
    TIME = "time"
    CATEGORICAL = "categorical"
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"


@dataclass(slots=True)
class Dimension:
    """A dimension in a semantic model."""
    name: str
    type: DimensionType
    expr: str = ""
    time_granularity: Optional[str] = None  # day, week, month, quarter, year
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Dimension":
        raw_type = data.get("type", "categorical")
        # Normalize: "string" is an alias for "categorical"
        if raw_type == "string":
            raw_type = "categorical"
        return cls(
            name=data["name"],
            type=DimensionType(raw_type),
            expr=data.get("expr", data["name"]),
            time_granularity=data.get("time_granularity"),
            description=data.get("description"),
        )


# ── Measure ─────────────────────────────────────────────────────────────────

class AggregationType(Enum):
    """Supported aggregation types."""
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVERAGE = "average"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


@dataclass(slots=True)
class Measure:
    """A measure in a semantic model."""
    name: str
    expr: str
    agg: AggregationType
    description: Optional[str] = None
    filters: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Measure":
        agg_value = data.get("agg", "sum")
        # Normalize "avg" to "average"
        if agg_value == "avg":
            agg_value = "average"
        # Parse filters: [{sql: "..."}, ...] or ["...", ...]
        raw_filters = data.get("filters", [])
        filters = []
        for f in raw_filters:
            if isinstance(f, dict):
                filters.append(f.get("sql", ""))
            else:
                filters.append(str(f))
        return cls(
            name=data["name"],
            expr=data.get("expr", data["name"]),
            agg=AggregationType(agg_value),
            description=data.get("description"),
            filters=[f for f in filters if f],
        )


# ── Joins ──────────────────────────────────────────────────────────────────

class JoinRelationship(Enum):
    """Join relationship types (Cube.js-compatible)."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"


@dataclass(slots=True)
class Join:
    """A join definition between semantic models."""
    name: str  # target model name
    relationship: JoinRelationship
    sql: str  # join condition, e.g. "{CUBE}.customer_id = {customers}.customer_id"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Join":
        return cls(
            name=data["name"],
            relationship=JoinRelationship(data.get("relationship", "many_to_one")),
            sql=data.get("sql", ""),
        )


# ── Semantic Model ──────────────────────────────────────────────────────────

@dataclass(slots=True)
class SemanticModel:
    """
    A semantic model maps a Seeknal node to business entities, dimensions,
    and measures. It provides the foundation for metric definitions.

    Advanced metrics (ratio, cumulative, derived) can be defined inline
    via the ``metrics`` field, eliminating the need for separate metric YAML files.
    """
    name: str
    model_ref: str  # Reference to a Seeknal node, e.g. "ref('transform.orders')"
    entities: list[Entity] = field(default_factory=list)
    dimensions: list[Dimension] = field(default_factory=list)
    measures: list[Measure] = field(default_factory=list)
    joins: list[Join] = field(default_factory=list)
    metrics: list["Metric"] = field(default_factory=list)
    description: Optional[str] = None
    default_time_dimension: Optional[str] = None

    def get_entity(self, name: str) -> Optional[Entity]:
        """Find an entity by name."""
        for e in self.entities:
            if e.name == name:
                return e
        return None

    def get_dimension(self, name: str) -> Optional[Dimension]:
        """Find a dimension by name."""
        for d in self.dimensions:
            if d.name == name:
                return d
        return None

    def get_measure(self, name: str) -> Optional[Measure]:
        """Find a measure by name."""
        for m in self.measures:
            if m.name == name:
                return m
        return None

    def get_primary_entity(self) -> Optional[Entity]:
        """Get the primary entity of this model."""
        for e in self.entities:
            if e.type == EntityType.PRIMARY:
                return e
        return None

    def get_join(self, target_name: str) -> Optional[Join]:
        """Find a join definition by target model name."""
        for j in self.joins:
            if j.name == target_name:
                return j
        return None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SemanticModel":
        return cls(
            name=data["name"],
            model_ref=data.get("model", ""),
            description=data.get("description"),
            default_time_dimension=data.get("default_time_dimension"),
            entities=[Entity.from_dict(e) for e in data.get("entities", [])],
            dimensions=[Dimension.from_dict(d) for d in data.get("dimensions", [])],
            measures=[Measure.from_dict(m) for m in data.get("measures", [])],
            joins=[Join.from_dict(j) for j in data.get("joins", [])],
            metrics=[Metric.from_dict(m) for m in data.get("metrics", [])],
        )

    def validate(self) -> list[str]:
        """Validate the semantic model. Returns a list of errors."""
        errors: list[str] = []
        if not self.name:
            errors.append("Semantic model missing 'name'")
        if not self.model_ref:
            errors.append(f"Semantic model '{self.name}' missing 'model' reference")
        if not self.entities:
            errors.append(f"Semantic model '{self.name}' has no entities")
        if not self.measures:
            errors.append(f"Semantic model '{self.name}' has no measures")

        # Check default_time_dimension references an actual time dimension
        if self.default_time_dimension:
            dim = self.get_dimension(self.default_time_dimension)
            if not dim:
                errors.append(
                    f"Semantic model '{self.name}': default_time_dimension "
                    f"'{self.default_time_dimension}' not found in dimensions"
                )
            elif dim.type != DimensionType.TIME:
                errors.append(
                    f"Semantic model '{self.name}': default_time_dimension "
                    f"'{self.default_time_dimension}' is not a time dimension"
                )

        # Check for duplicate names
        entity_names = [e.name for e in self.entities]
        if len(entity_names) != len(set(entity_names)):
            errors.append(f"Semantic model '{self.name}' has duplicate entity names")

        dim_names = [d.name for d in self.dimensions]
        if len(dim_names) != len(set(dim_names)):
            errors.append(f"Semantic model '{self.name}' has duplicate dimension names")

        measure_names = [m.name for m in self.measures]
        if len(measure_names) != len(set(measure_names)):
            errors.append(f"Semantic model '{self.name}' has duplicate measure names")

        # Validate joins
        for join in self.joins:
            if not join.sql:
                errors.append(
                    f"Semantic model '{self.name}': join to '{join.name}' missing 'sql' condition"
                )

        # Validate inline metrics
        metric_names = [m.name for m in self.metrics]
        if len(metric_names) != len(set(metric_names)):
            errors.append(f"Semantic model '{self.name}' has duplicate metric names")
        for m in self.metrics:
            errors.extend(m.validate())

        return errors


# ── Metric Types ────────────────────────────────────────────────────────────

class MetricType(Enum):
    """Types of metrics."""
    SIMPLE = "simple"
    RATIO = "ratio"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"


@dataclass(slots=True)
class MetricInput:
    """An input reference for derived metrics."""
    metric: str  # Name of the referenced metric
    alias: str = ""
    offset: Optional[str] = None  # e.g. "1 month"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MetricInput":
        return cls(
            metric=data["metric"],
            alias=data.get("alias", ""),
            offset=data.get("offset"),
        )


@dataclass(slots=True)
class Metric:
    """
    A metric definition. The type field determines which fields are relevant:
    - simple: measure, filter
    - ratio: numerator, denominator
    - cumulative: measure, grain_to_date
    - derived: expr, inputs
    """
    name: str
    type: MetricType
    description: Optional[str] = None
    filter: Optional[str] = None

    # simple / cumulative
    measure: Optional[str] = None

    # ratio
    numerator: Optional[str] = None
    denominator: Optional[str] = None

    # cumulative
    grain_to_date: Optional[str] = None  # month, quarter, year

    # derived
    expr: Optional[str] = None
    inputs: list[MetricInput] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Metric":
        return cls(
            name=data["name"],
            type=MetricType(data["type"]),
            description=data.get("description"),
            filter=data.get("filter"),
            measure=data.get("measure"),
            numerator=data.get("numerator"),
            denominator=data.get("denominator"),
            grain_to_date=data.get("grain_to_date"),
            expr=data.get("expr"),
            inputs=[MetricInput.from_dict(i) for i in data.get("inputs", [])],
        )

    def validate(self) -> list[str]:
        """Validate the metric definition. Returns a list of errors."""
        errors: list[str] = []
        if not self.name:
            errors.append("Metric missing 'name'")
        if self.type == MetricType.SIMPLE:
            if not self.measure:
                errors.append(f"Simple metric '{self.name}' missing 'measure'")
        elif self.type == MetricType.RATIO:
            if not self.numerator:
                errors.append(f"Ratio metric '{self.name}' missing 'numerator'")
            if not self.denominator:
                errors.append(f"Ratio metric '{self.name}' missing 'denominator'")
        elif self.type == MetricType.CUMULATIVE:
            if not self.measure:
                errors.append(f"Cumulative metric '{self.name}' missing 'measure'")
        elif self.type == MetricType.DERIVED:
            if not self.expr:
                errors.append(f"Derived metric '{self.name}' missing 'expr'")
            if not self.inputs:
                errors.append(f"Derived metric '{self.name}' missing 'inputs'")
        return errors

    def get_referenced_metrics(self) -> list[str]:
        """Get names of metrics this metric depends on (for derived metrics)."""
        if self.type == MetricType.DERIVED:
            return [inp.metric for inp in self.inputs]
        return []

    def get_referenced_measures(self) -> list[str]:
        """Get names of measures this metric references."""
        if self.type in (MetricType.SIMPLE, MetricType.CUMULATIVE) and self.measure:
            return [self.measure]
        if self.type == MetricType.RATIO:
            measures = []
            if self.numerator:
                measures.append(self.numerator)
            if self.denominator:
                measures.append(self.denominator)
            return measures
        return []


# ── Metric Query ────────────────────────────────────────────────────────────

@dataclass(slots=True)
class MetricQuery:
    """A query against the semantic layer."""
    metrics: list[str]
    dimensions: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    limit: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MetricQuery":
        return cls(
            metrics=data.get("metrics", []),
            dimensions=data.get("dimensions", []),
            filters=data.get("filters", []),
            order_by=data.get("order_by", []),
            limit=data.get("limit"),
        )


# ── Entity Graph ────────────────────────────────────────────────────────────

class EntityGraph:
    """
    Tracks entity relationships across semantic models for join planning.

    Builds a graph where entities (e.g. customer_id) connect semantic models
    that share the same entity, enabling automatic join resolution.
    """

    def __init__(self) -> None:
        # entity_name -> list of (semantic_model_name, entity_type)
        self._entity_models: dict[str, list[tuple[str, EntityType]]] = {}
        # semantic_model_name -> SemanticModel
        self._models: dict[str, SemanticModel] = {}

    def add_model(self, model: SemanticModel) -> None:
        """Register a semantic model and its entities."""
        self._models[model.name] = model
        for entity in model.entities:
            if entity.name not in self._entity_models:
                self._entity_models[entity.name] = []
            self._entity_models[entity.name].append((model.name, entity.type))

    def get_model(self, name: str) -> Optional[SemanticModel]:
        """Get a semantic model by name."""
        return self._models.get(name)

    def get_models_for_entity(self, entity_name: str) -> list[tuple[str, EntityType]]:
        """Get all models that reference an entity."""
        return self._entity_models.get(entity_name, [])

    def find_join_path(
        self, from_model: str, to_model: str
    ) -> Optional[list[tuple[str, str]]]:
        """
        Find a join path between two models via shared entities.

        Returns:
            List of (entity_name, intermediate_model) pairs forming the join path,
            or None if no path exists.
        """
        if from_model == to_model:
            return []

        # BFS for shortest join path
        visited: set[str] = {from_model}
        queue: list[tuple[str, list[tuple[str, str]]]] = [(from_model, [])]

        while queue:
            current, path = queue.pop(0)
            current_model = self._models.get(current)
            if not current_model:
                continue

            for entity in current_model.entities:
                for neighbor_model, _ in self._entity_models.get(entity.name, []):
                    if neighbor_model == to_model:
                        return path + [(entity.name, neighbor_model)]
                    if neighbor_model not in visited:
                        visited.add(neighbor_model)
                        queue.append(
                            (neighbor_model, path + [(entity.name, neighbor_model)])
                        )

        return None

    def get_joinable_models(self, model_name: str) -> set[str]:
        """Get all models that can be joined to the given model."""
        model = self._models.get(model_name)
        if not model:
            return set()

        joinable: set[str] = set()
        for entity in model.entities:
            for neighbor, _ in self._entity_models.get(entity.name, []):
                if neighbor != model_name:
                    joinable.add(neighbor)
        return joinable

    @property
    def models(self) -> dict[str, SemanticModel]:
        return self._models


# ── Metric Registry & Cycle Detection ───────────────────────────────────────

class MetricRegistry:
    """
    Registry of metrics with validation and dependency cycle detection.
    """

    def __init__(self) -> None:
        self._metrics: dict[str, Metric] = {}

    def add(self, metric: Metric) -> None:
        """Register a metric."""
        self._metrics[metric.name] = metric

    def get(self, name: str) -> Optional[Metric]:
        """Get a metric by name."""
        return self._metrics.get(name)

    @property
    def metrics(self) -> dict[str, Metric]:
        return self._metrics

    def detect_cycles(self) -> tuple[bool, list[str]]:
        """
        Detect circular dependencies among derived metrics.

        Returns:
            (has_cycle, cycle_path) where cycle_path lists metric names.
        """
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def dfs(name: str, path: list[str]) -> Optional[list[str]]:
            visited.add(name)
            rec_stack.add(name)
            path.append(name)

            metric = self._metrics.get(name)
            if metric:
                for ref_name in metric.get_referenced_metrics():
                    if ref_name not in visited:
                        result = dfs(ref_name, path)
                        if result is not None:
                            return result
                    elif ref_name in rec_stack:
                        # Found cycle
                        cycle_start = path.index(ref_name)
                        return path[cycle_start:] + [ref_name]

            path.pop()
            rec_stack.remove(name)
            return None

        for metric_name in self._metrics:
            if metric_name not in visited:
                result = dfs(metric_name, [])
                if result is not None:
                    return True, result

        return False, []

    def resolve_order(self) -> list[str]:
        """
        Return metrics in dependency order (topological sort).
        Metrics with no dependencies come first.

        Raises:
            ValueError: If there are circular dependencies.
        """
        has_cycle, cycle_path = self.detect_cycles()
        if has_cycle:
            raise ValueError(
                f"Circular metric dependency detected: {' -> '.join(cycle_path)}"
            )

        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            metric = self._metrics.get(name)
            if metric:
                for dep in metric.get_referenced_metrics():
                    visit(dep)
            order.append(name)

        for name in self._metrics:
            visit(name)

        return order

    def find_measure_model(
        self, measure_name: str, entity_graph: EntityGraph
    ) -> Optional[tuple[str, Measure]]:
        """Find which semantic model contains a given measure."""
        for model_name, model in entity_graph.models.items():
            measure = model.get_measure(measure_name)
            if measure:
                return model_name, measure
        return None

    def validate_all(self, entity_graph: EntityGraph) -> list[str]:
        """Validate all metrics against available measures and models."""
        errors: list[str] = []

        for name, metric in self._metrics.items():
            # Type-specific validation
            errors.extend(metric.validate())

            # Check measure references resolve
            for measure_name in metric.get_referenced_measures():
                if not self.find_measure_model(measure_name, entity_graph):
                    errors.append(
                        f"Metric '{name}' references measure '{measure_name}' "
                        f"which is not defined in any semantic model"
                    )

            # Check metric references resolve
            for ref_name in metric.get_referenced_metrics():
                if ref_name not in self._metrics:
                    errors.append(
                        f"Metric '{name}' references metric '{ref_name}' "
                        f"which is not defined"
                    )

        # Check for cycles
        has_cycle, cycle_path = self.detect_cycles()
        if has_cycle:
            errors.append(
                f"Circular metric dependency: {' -> '.join(cycle_path)}"
            )

        return errors


# ── YAML Parsing Helpers ────────────────────────────────────────────────────

def parse_semantic_model_yaml(data: dict[str, Any]) -> SemanticModel:
    """Parse a single semantic model from YAML dict."""
    return SemanticModel.from_dict(data)


def parse_metric_yaml(data: dict[str, Any]) -> Metric:
    """Parse a single metric from YAML dict."""
    return Metric.from_dict(data)

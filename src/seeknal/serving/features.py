"""SQL statement feature extraction for Seeknal Serving."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from sqlglot import exp

DDL_CLASSES = (
    exp.Create,
    exp.Drop,
    exp.AlterTable,
    exp.AlterColumn,
    exp.TruncateTable,
    exp.DropPartition,
    exp.AlterSet,
    exp.DDL,
)


@dataclass(frozen=True)
class QueryFeatures:
    """Structural features extracted from a parsed SQL statement."""

    kind: str
    tables: tuple[str, ...]
    has_aggregate: bool
    has_group_by: bool
    has_window: bool
    has_join: bool
    has_distinct: bool
    has_subquery: bool
    limit: int | None
    key_equalities: Mapping[str, int]


def _is_literal_or_param(node: exp.Expression) -> bool:
    """Check if an expression is a literal value or parameter placeholder."""
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(
        node, (exp.Literal, exp.Boolean, exp.Null, exp.Parameter, exp.Placeholder)
    ):
        return True
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return True
    if isinstance(node, exp.Cast) and _is_literal_or_param(node.this):
        return True
    return False


def _flatten_and(expr: exp.Expression) -> list[exp.Expression]:
    """Flatten nested top-level AND conjuncts."""
    while isinstance(expr, exp.Paren):
        expr = expr.this
    if isinstance(expr, exp.And):
        return _flatten_and(expr.this) + _flatten_and(expr.expression)
    return [expr]


def _has_subquery(statement: exp.Expression) -> bool:
    """Check if statement contains subqueries (excluding CTE definitions)."""
    if statement.find(exp.Subquery) or statement.find(exp.Exists):
        return True
    for node in statement.walk():
        if isinstance(node, exp.Select) and node is not statement:
            if not isinstance(node.parent, (exp.CTE, exp.Union)):
                return True
    return False


def _extract_kind(statement: exp.Expression) -> str:
    """Determine statement kind."""
    if isinstance(statement, (exp.Select, exp.Union)):
        return "select"
    if isinstance(statement, exp.Insert):
        return "insert"
    if isinstance(statement, exp.Update):
        return "update"
    if isinstance(statement, exp.Delete):
        return "delete"
    if isinstance(statement, exp.Merge):
        return "merge"
    if isinstance(statement, DDL_CLASSES):
        return "ddl"
    return "other"


def _extract_tables(statement: exp.Expression) -> tuple[str, ...]:
    """Extract lower-cased dotted table references in first-seen order, excluding CTEs."""
    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
    tables: list[str] = []
    for t in statement.find_all(exp.Table):
        # If bare reference matches a CTE, it's defined by statement itself
        if t.name.lower() in cte_names and not t.db and not t.catalog:
            continue
        parts: list[str] = []
        if t.catalog:
            parts.append(t.catalog)
        if t.db:
            parts.append(t.db)
        if t.name:
            parts.append(t.name)
        dotted = ".".join(parts).lower()
        if dotted and dotted not in tables:
            tables.append(dotted)
    return tuple(tables)


def _extract_limit(statement: exp.Expression) -> int | None:
    """Extract literal outermost LIMIT, if present."""
    limit_node = statement.args.get("limit")
    if limit_node and limit_node.expression:
        expr = limit_node.expression
        if isinstance(expr, exp.Literal) and not expr.is_string:
            try:
                return int(expr.this)
            except (ValueError, TypeError):
                return None
    return None


def _extract_key_equalities(statement: exp.Expression) -> dict[str, int]:
    """Extract constrained key equality columns and value counts in outermost WHERE."""
    where = statement.args.get("where")
    if where is None:
        return {}

    key_equalities: dict[str, int] = {}
    for conjunct in _flatten_and(where.this):
        while isinstance(conjunct, exp.Paren):
            conjunct = conjunct.this

        if isinstance(conjunct, exp.EQ):
            left = conjunct.this
            right = conjunct.expression
            if isinstance(left, exp.Column) and _is_literal_or_param(right):
                key_equalities[left.name.lower()] = 1
            elif isinstance(right, exp.Column) and _is_literal_or_param(left):
                key_equalities[right.name.lower()] = 1
        elif isinstance(conjunct, exp.In):
            left = conjunct.this
            if isinstance(left, exp.Column):
                if not conjunct.args.get("query") and conjunct.expressions:
                    if all(_is_literal_or_param(item) for item in conjunct.expressions):
                        key_equalities[left.name.lower()] = len(conjunct.expressions)

    return key_equalities


def extract_features(statement: exp.Expression) -> QueryFeatures:
    """Extract structural analysis features from a parsed SQL statement AST."""
    kind = _extract_kind(statement)
    tables = _extract_tables(statement)
    has_aggregate = bool(statement.find(exp.AggFunc) or statement.find(exp.Having))
    has_group_by = bool(statement.find(exp.Group))
    has_window = bool(statement.find(exp.Window))
    has_join = bool(statement.find(exp.Join))
    has_distinct = bool(statement.find(exp.Distinct))
    has_subquery = _has_subquery(statement)
    limit = _extract_limit(statement)
    key_equalities = _extract_key_equalities(statement)

    return QueryFeatures(
        kind=kind,
        tables=tables,
        has_aggregate=has_aggregate,
        has_group_by=has_group_by,
        has_window=has_window,
        has_join=has_join,
        has_distinct=has_distinct,
        has_subquery=has_subquery,
        limit=limit,
        key_equalities=key_equalities,
    )

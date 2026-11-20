"""SQL statement feature extraction for Seeknal Serving."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
import contextvars
from dataclasses import dataclass
import re
from typing import Any

from sqlglot import exp

PREFIX_LIKE_RE = re.compile(r"^([^%_\\]|\\[%_\\])+%$")

_ACTIVE_FEATURE_PARAMS: contextvars.ContextVar[tuple[Any, ...] | None] = (
    contextvars.ContextVar("seeknal_active_feature_params", default=None)
)


@contextmanager
def _bind_feature_params(params: Sequence[Any]) -> Iterator[None]:
    """Temporarily bind query parameters for AST feature/predicate extraction."""
    token = _ACTIVE_FEATURE_PARAMS.set(tuple(params))
    try:
        yield
    finally:
        _ACTIVE_FEATURE_PARAMS.reset(token)


def _resolve_active_params(
    statement: exp.Expression,
    params: Sequence[Any] | None = None,
) -> tuple[Any, ...]:
    if params is not None:
        return tuple(params)
    meta_params = statement.meta.get("bound_params") if hasattr(statement, "meta") else None
    if isinstance(meta_params, Sequence) and not isinstance(meta_params, (str, bytes)):
        return tuple(meta_params)
    ctx_params = _ACTIVE_FEATURE_PARAMS.get()
    if ctx_params is not None:
        return ctx_params
    return ()


def _is_valid_prefix_like_pattern(pattern: Any) -> bool:
    """Return True if *pattern* is a valid prefix LIKE string (no leading/interior wildcard)."""
    return type(pattern) is str and bool(PREFIX_LIKE_RE.fullmatch(pattern))


def _resolve_param_index(node: exp.Expression) -> int | None:
    """Return 1-based parameter index for an exp.Parameter or exp.Placeholder, or None."""
    if not isinstance(node, (exp.Parameter, exp.Placeholder)):
        return None
    raw = getattr(node, "name", None) or getattr(node, "this", None)
    if raw is None:
        return None
    raw_str = str(raw).lstrip("$")
    try:
        idx = int(raw_str)
    except (ValueError, TypeError):
        return None
    return idx if idx >= 1 else None

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
class PredicateFeature:
    """A sargable top-level WHERE conjunct extracted from a SQL statement."""

    column: str
    op: str
    value_count: int = 1
    table_qualifier: str | None = None


@dataclass(frozen=True)
class OrderByItem:
    """An item in the outermost ORDER BY clause."""

    column: str | None
    direction: str = "asc"
    table_qualifier: str | None = None
    is_bare_column: bool = True


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
    predicates: tuple[PredicateFeature, ...] = ()
    offset: int | None = None
    order_by: tuple[OrderByItem, ...] = ()


def _unwrap_paren(node: exp.Expression) -> exp.Expression:
    """Unwrap nested Parenthesis wrappers around an expression."""
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _is_bare_column(node: exp.Expression) -> bool:
    """Return True if *node* is a bare column reference (not Star, not CAST/Func)."""
    return isinstance(node, exp.Column) and not isinstance(node.this, exp.Star)


def _column_qualifier(col: exp.Column) -> str | None:
    """Return lower-cased dotted table qualifier of *col*, or None if unqualified."""
    parts = [p for p in (col.catalog, col.db, col.table) if p]
    return ".".join(parts).lower() if parts else None


def _is_literal_or_param(node: exp.Expression) -> bool:
    """Check if an expression is a non-null literal value or parameter placeholder."""
    node = _unwrap_paren(node)
    if isinstance(node, exp.Null):
        return False
    if isinstance(
        node, (exp.Literal, exp.Boolean, exp.Parameter, exp.Placeholder)
    ):
        return True
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        return True
    if isinstance(node, (exp.Cast, exp.TryCast)) and _is_literal_or_param(node.this):
        return True
    return False


def _flatten_and(expr: exp.Expression) -> list[exp.Expression]:
    """Flatten nested top-level AND conjuncts."""
    expr = _unwrap_paren(expr)
    if isinstance(expr, exp.And):
        return _flatten_and(expr.this) + _flatten_and(expr.expression)
    return [expr]


def _flatten_or(expr: exp.Expression) -> list[exp.Expression]:
    """Flatten nested OR disjuncts."""
    expr = _unwrap_paren(expr)
    if isinstance(expr, exp.Or):
        return _flatten_or(expr.this) + _flatten_or(expr.expression)
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


def _extract_offset(statement: exp.Expression) -> int | None:
    """Extract literal integer outermost OFFSET, if present."""
    offset_node = statement.args.get("offset")
    if offset_node and offset_node.expression:
        expr = _unwrap_paren(offset_node.expression)
        if isinstance(expr, exp.Literal) and not expr.is_string and expr.is_int:
            try:
                return int(expr.this)
            except (ValueError, TypeError):
                return None
        if (
            isinstance(expr, exp.Neg)
            and isinstance(expr.this, exp.Literal)
            and not expr.this.is_string
            and expr.this.is_int
        ):
            try:
                return -int(expr.this.this)
            except (ValueError, TypeError):
                return None
    return None


def _extract_order_by(statement: exp.Expression) -> tuple[OrderByItem, ...]:
    """Extract outermost ORDER BY items."""
    order_node = statement.args.get("order")
    if not order_node or not order_node.expressions:
        return ()

    items: list[OrderByItem] = []
    for item in order_node.expressions:
        if isinstance(item, exp.Ordered):
            target = _unwrap_paren(item.this)
            direction = "desc" if item.args.get("desc") else "asc"
        else:
            target = _unwrap_paren(item)
            direction = "asc"

        if _is_bare_column(target):
            assert isinstance(target, exp.Column)
            items.append(
                OrderByItem(
                    column=target.name.lower(),
                    direction=direction,
                    table_qualifier=_column_qualifier(target),
                    is_bare_column=True,
                )
            )
        else:
            items.append(
                OrderByItem(
                    column=None,
                    direction=direction,
                    table_qualifier=None,
                    is_bare_column=False,
                )
            )
    return tuple(items)


def _extract_single_eq_or_in(
    expr: exp.Expression,
) -> tuple[str, str | None, str, int] | None:
    """Extract (col_lower, qualifier, op, value_count) for a bare EQ or IN predicate."""
    expr = _unwrap_paren(expr)
    if isinstance(expr, exp.EQ):
        left = _unwrap_paren(expr.this)
        right = _unwrap_paren(expr.expression)
        if _is_bare_column(left) and _is_literal_or_param(right):
            assert isinstance(left, exp.Column)
            return (left.name.lower(), _column_qualifier(left), "eq", 1)
        if _is_bare_column(right) and _is_literal_or_param(left):
            assert isinstance(right, exp.Column)
            return (right.name.lower(), _column_qualifier(right), "eq", 1)
    elif isinstance(expr, exp.In):
        left = _unwrap_paren(expr.this)
        if (
            _is_bare_column(left)
            and not expr.args.get("query")
            and not expr.args.get("unnest")
            and not expr.args.get("field")
            and expr.expressions
            and all(_is_literal_or_param(item) for item in expr.expressions)
        ):
            assert isinstance(left, exp.Column)
            return (
                left.name.lower(),
                _column_qualifier(left),
                "in",
                len(expr.expressions),
            )
    return None


def _extract_predicates(
    statement: exp.Expression,
    *,
    params: Sequence[Any] | None = None,
) -> tuple[PredicateFeature, ...]:
    """Extract sargable top-level AND conjuncts from outermost WHERE per §4.2."""
    where = statement.args.get("where")
    if where is None or where.this is None:
        return ()

    resolved_params = _resolve_active_params(statement, params)
    preds: list[PredicateFeature] = []
    for raw_conjunct in _flatten_and(where.this):
        conjunct = _unwrap_paren(raw_conjunct)

        eq_or_in = _extract_single_eq_or_in(conjunct)
        if eq_or_in is not None:
            col_name, qual, op, vc = eq_or_in
            preds.append(
                PredicateFeature(
                    column=col_name,
                    op=op,
                    value_count=vc,
                    table_qualifier=qual,
                )
            )
            continue

        if isinstance(conjunct, (exp.GT, exp.GTE, exp.LT, exp.LTE)):
            left = _unwrap_paren(conjunct.this)
            right = _unwrap_paren(conjunct.expression)
            if _is_bare_column(left) and _is_literal_or_param(right):
                assert isinstance(left, exp.Column)
                preds.append(
                    PredicateFeature(
                        column=left.name.lower(),
                        op="range",
                        value_count=1,
                        table_qualifier=_column_qualifier(left),
                    )
                )
            elif _is_bare_column(right) and _is_literal_or_param(left):
                assert isinstance(right, exp.Column)
                preds.append(
                    PredicateFeature(
                        column=right.name.lower(),
                        op="range",
                        value_count=1,
                        table_qualifier=_column_qualifier(right),
                    )
                )
            continue

        if isinstance(conjunct, exp.Between):
            target = _unwrap_paren(conjunct.this)
            low = conjunct.args.get("low")
            high = conjunct.args.get("high")
            if (
                _is_bare_column(target)
                and low is not None
                and high is not None
                and _is_literal_or_param(low)
                and _is_literal_or_param(high)
            ):
                assert isinstance(target, exp.Column)
                preds.append(
                    PredicateFeature(
                        column=target.name.lower(),
                        op="between",
                        value_count=2,
                        table_qualifier=_column_qualifier(target),
                    )
                )
            continue

        if type(conjunct) is exp.Like:
            left = _unwrap_paren(conjunct.this)
            right = _unwrap_paren(conjunct.expression)
            if _is_bare_column(left):
                pat_str: str | None = None
                if isinstance(right, exp.Literal) and right.is_string:
                    pat_str = right.this
                elif isinstance(right, (exp.Parameter, exp.Placeholder)):
                    idx = _resolve_param_index(right)
                    if (
                        idx is not None
                        and 1 <= idx <= len(resolved_params)
                        and type(resolved_params[idx - 1]) is str
                    ):
                        pat_str = resolved_params[idx - 1]
                if pat_str is not None and _is_valid_prefix_like_pattern(pat_str):
                    assert isinstance(left, exp.Column)
                    preds.append(
                        PredicateFeature(
                            column=left.name.lower(),
                            op="prefix_like",
                            value_count=1,
                            table_qualifier=_column_qualifier(left),
                        )
                    )
            continue

        if isinstance(conjunct, exp.Or):
            branches = _flatten_or(conjunct)
            if len(branches) >= 2:
                branch_specs: list[tuple[str, str | None, str, int]] = []
                all_valid = True
                for br in branches:
                    spec = _extract_single_eq_or_in(br)
                    if spec is None:
                        all_valid = False
                        break
                    branch_specs.append(spec)
                if all_valid and branch_specs:
                    first_col, first_qual, _, _ = branch_specs[0]
                    if all(
                        c == first_col and q == first_qual
                        for c, q, _, _ in branch_specs[1:]
                    ):
                        total_vc = sum(vc for _, _, _, vc in branch_specs)
                        preds.append(
                            PredicateFeature(
                                column=first_col,
                                op="or_eq",
                                value_count=total_vc,
                                table_qualifier=first_qual,
                            )
                        )

    return tuple(preds)


def _extract_key_equalities(statement: exp.Expression) -> dict[str, int]:
    """Extract constrained key equality columns and value counts in outermost WHERE."""
    where = statement.args.get("where")
    if where is None or where.this is None:
        return {}

    key_equalities: dict[str, int] = {}
    for raw_conjunct in _flatten_and(where.this):
        spec = _extract_single_eq_or_in(raw_conjunct)
        if spec is not None:
            col_name, _qual, _op, vc = spec
            key_equalities[col_name] = vc

    return key_equalities


def extract_features(
    statement: exp.Expression,
    *,
    params: Sequence[Any] | None = None,
) -> QueryFeatures:
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
    predicates = _extract_predicates(statement, params=params)
    offset = _extract_offset(statement)
    order_by = _extract_order_by(statement)

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
        predicates=predicates,
        offset=offset,
        order_by=order_by,
    )

"""Table access pattern guardrails for Seeknal Serving (§4.2)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from sqlglot import exp

from seeknal.serving.enforce import EnforcementError
from seeknal.serving.features import (
    OrderByItem,
    PredicateFeature,
    _column_qualifier,
    _extract_order_by,
    _extract_predicates,
    _flatten_and,
    _is_bare_column,
    _unwrap_paren,
)
from seeknal.serving.placement import (
    Placement,
    Policy,
    TableAccess,
    TableEntry,
    is_postgres_wire,
)
from seeknal.serving.router import (
    RoutingDecision,
    _table_dotted_name,
    _table_leaf_name,
)

_PATTERN_PRIORITY = {"point": 3, "filter": 2, "scan": 1}
_SET_OP_CLASSES = (exp.Union, exp.Intersect, exp.Except)


class GuardrailError(EnforcementError):
    """Raised when a client query violates table access guardrails."""

    code: str = "filter_required"

    def __init__(
        self,
        message: str = "",
        *,
        code: str = "filter_required",
        guard: str = "reject",
        access_pattern: str | None = None,
        filter_columns: tuple[str, ...] = (),
        effective_max_rows: int | None = None,
        engine: str | None = None,
        engine_kind: str | None = None,
        rule: str | None = None,
    ) -> None:
        super().__init__(message, code=code)
        self.guard = guard
        self.access_pattern = access_pattern
        self.filter_columns = filter_columns
        self.effective_max_rows = effective_max_rows
        self.engine = engine
        self.engine_kind = engine_kind
        self.rule = rule


@dataclass(frozen=True)
class GuardrailResult:
    """Outcome of checking table access guardrails on a routed statement."""

    outcome: str
    access_pattern: str | None
    filter_columns: tuple[str, ...]
    effective_max_rows: int | None
    warnings: tuple[str, ...] = ()


@dataclass
class _RelationInstance:
    """A single FROM / JOIN relation instance within a SELECT scope."""

    idx: int
    table: TableEntry
    eff_access: TableAccess | None
    aliases: set[str]
    transparent: bool = True
    has_star: bool = True
    proj_map: dict[str, str | None] = field(default_factory=dict)
    predicates: list[PredicateFeature] = field(default_factory=list)
    satisfied: bool = False
    has_filter: bool = False

    def resolve_column(self, col_lower: str) -> tuple[bool, str | None]:
        """Resolve an outer column name to (exists_on_relation, bare_base_col_or_None)."""
        if not self.transparent:
            return (False, None)
        if col_lower in self.proj_map:
            mapped = self.proj_map[col_lower]
            return (True, mapped)
        if self.has_star:
            return (True, col_lower)
        return (False, None)


@dataclass(frozen=True)
class _EquiJoinEdge:
    """Directed equi-join propagation edge from src instance to dst instance."""

    src_idx: int
    src_col: str
    dst_idx: int
    dst_col: str


def effective_table_access(table: TableEntry) -> TableAccess | None:
    """Return explicit TableAccess or implicit PII TableAccess when untokenized."""
    if table.access is not None:
        return table.access
    if table.pii and not table.tokenized:
        if table.keys:
            return TableAccess(
                pattern="point",
                keys=table.keys,
                filterable=table.keys,
                sortable=table.keys,
                require_filter=True,
            )
        return TableAccess(
            pattern="filter",
            require_filter=True,
        )
    return None


def _record_guard_rejection(metrics: Any, code: str) -> None:
    if metrics is None:
        return
    fn = getattr(metrics, "record_guard_rejection", None) or getattr(
        metrics, "inc_guard_rejection", None
    )
    if callable(fn):
        fn(code)


def _record_guard_warning(metrics: Any, code: str) -> None:
    if metrics is None:
        return
    fn = getattr(metrics, "record_guard_warning", None) or getattr(
        metrics, "inc_guard_warning", None
    )
    if callable(fn):
        fn(code)


def _collect_cte_map(statement: exp.Expression) -> dict[str, exp.Expression]:
    """Collect CTE name -> inner query expression map."""
    cte_map: dict[str, exp.Expression] = {}
    with_clause = statement.args.get("with")
    if with_clause:
        for cte in with_clause.expressions:
            if isinstance(cte, exp.CTE) and cte.this is not None:
                cte_map[cte.alias_or_name.lower()] = cte.this
    return cte_map


def _collect_query_selects(node: exp.Expression) -> list[exp.Select]:
    """Collect top-level Select branches from a Select or Union/Intersect/Except."""
    node = _unwrap_paren(node)
    if isinstance(node, exp.Subquery) and node.this is not None:
        return _collect_query_selects(node.this)
    if isinstance(node, exp.Select):
        return [node]
    if isinstance(node, _SET_OP_CLASSES):
        left = _collect_query_selects(node.this) if node.this is not None else []
        right = (
            _collect_query_selects(node.expression)
            if node.expression is not None
            else []
        )
        return left + right
    return []


def _from_sources_of_select(sel: exp.Select) -> list[tuple[exp.Expression, str]]:
    """Return list of (relation_expr, join_side) for a Select's FROM and JOINs."""
    out: list[tuple[exp.Expression, str]] = []
    from_clause = sel.args.get("from")
    if from_clause is not None and from_clause.this is not None:
        out.append((from_clause.this, "FROM"))
    for j in sel.args.get("joins") or ():
        if isinstance(j, exp.Join) and j.this is not None:
            side = (j.side or "INNER").upper()
            out.append((j.this, side))
    return out


def _measure_from_depth_and_tables(
    rel_expr: exp.Expression,
    placement: Placement,
    cte_map: dict[str, exp.Expression],
    seen_ctes: frozenset[str] = frozenset(),
) -> tuple[int, list[TableEntry]]:
    """Return (subquery_nesting_depth, underlying_tables) for a FROM/JOIN source."""
    rel_expr = _unwrap_paren(rel_expr)
    if isinstance(rel_expr, exp.Table):
        if isinstance(rel_expr.this, exp.Func):
            return (0, [])
        t_name = rel_expr.name.lower()
        if t_name in cte_map and not rel_expr.db and not rel_expr.catalog:
            if t_name in seen_ctes:
                return (1, [])
            inner = cte_map[t_name]
            sub_depth, sub_tables = _query_max_depth_and_tables(
                inner, placement, cte_map, seen_ctes | {t_name}
            )
            return (1 + sub_depth, sub_tables)
        dotted = _table_dotted_name(rel_expr).lower()
        tbl = placement.find_table(dotted)
        return (0, [tbl] if tbl is not None else [])

    if isinstance(rel_expr, exp.Subquery) and rel_expr.this is not None:
        sub_depth, sub_tables = _query_max_depth_and_tables(
            rel_expr.this, placement, cte_map, seen_ctes
        )
        return (1 + sub_depth, sub_tables)

    return (0, [])


def _query_max_depth_and_tables(
    query_expr: exp.Expression,
    placement: Placement,
    cte_map: dict[str, exp.Expression],
    seen_ctes: frozenset[str],
) -> tuple[int, list[TableEntry]]:
    local_ctes = dict(cte_map)
    local_ctes.update(_collect_cte_map(query_expr))
    max_d = 0
    tables: list[TableEntry] = []
    for sel in _collect_query_selects(query_expr):
        local_sel_ctes = dict(local_ctes)
        local_sel_ctes.update(_collect_cte_map(sel))
        for src, _side in _from_sources_of_select(sel):
            d, tbls = _measure_from_depth_and_tables(
                src, placement, local_sel_ctes, seen_ctes
            )
            if d > max_d:
                max_d = d
            tables.extend(tbls)
    return (max_d, tables)


def is_plain_projection_derived_select(
    inner: exp.Expression,
    placement: Placement,
    cte_map: dict[str, exp.Expression],
) -> tuple[bool, TableEntry | None, bool, dict[str, str | None]]:
    """Check if *inner* is a 1-level plain-projection SELECT over a single base table.

    Returns ``(is_plain, base_table, has_star, proj_map)``.
    """
    inner = _unwrap_paren(inner)
    if not isinstance(inner, exp.Select):
        return (False, None, False, {})
    if (
        inner.args.get("joins")
        or inner.args.get("group")
        or inner.args.get("having")
        or inner.args.get("distinct")
        or inner.args.get("limit")
        or inner.args.get("offset")
        or inner.args.get("with")
    ):
        return (False, None, False, {})
    if (
        inner.find(exp.AggFunc)
        or inner.find(exp.Window)
        or inner.find(exp.Subquery)
        or inner.find(exp.Exists)
        or inner.find(_SET_OP_CLASSES)
    ):
        return (False, None, False, {})

    from_clause = inner.args.get("from")
    if from_clause is None or not isinstance(from_clause.this, exp.Table):
        return (False, None, False, {})

    base_tbl_node = from_clause.this
    if isinstance(base_tbl_node.this, exp.Func):
        return (False, None, False, {})
    if (
        base_tbl_node.name.lower() in cte_map
        and not base_tbl_node.db
        and not base_tbl_node.catalog
    ):
        return (False, None, False, {})

    base_entry = placement.find_table(_table_dotted_name(base_tbl_node).lower())
    if base_entry is None:
        return (False, None, False, {})

    base_aliases = set()
    if base_tbl_node.alias:
        base_aliases.add(base_tbl_node.alias.lower())
    else:
        base_aliases.add(_table_leaf_name(base_tbl_node).lower())
        base_aliases.add(_table_dotted_name(base_tbl_node).lower())
        base_aliases.add(base_entry.name.lower())

    has_star = False
    proj_map: dict[str, str | None] = {}
    for expr in inner.expressions:
        if isinstance(expr, exp.Star):
            has_star = True
        elif isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
            qual = _column_qualifier(expr)
            if qual is None or qual in base_aliases:
                has_star = True
        elif _is_bare_column(expr):
            assert isinstance(expr, exp.Column)
            qual = _column_qualifier(expr)
            if qual is None or qual in base_aliases:
                col_l = expr.name.lower()
                proj_map.setdefault(col_l, col_l)
            else:
                proj_map.setdefault(expr.name.lower(), None)
        elif isinstance(expr, exp.Alias):
            out_name = expr.alias.lower()
            target = _unwrap_paren(expr.this)
            if _is_bare_column(target):
                assert isinstance(target, exp.Column)
                qual = _column_qualifier(target)
                if qual is None or qual in base_aliases:
                    proj_map[out_name] = target.name.lower()
                else:
                    proj_map[out_name] = None
            else:
                proj_map[out_name] = None

    return (True, base_entry, has_star, proj_map)


def _match_instance_by_qualifier(
    qual: str,
    instances: list[_RelationInstance],
) -> _RelationInstance | None:
    qual_l = qual.lower()
    for inst in instances:
        if qual_l in inst.aliases:
            return inst
    return None


def _all_known_access_cols(inst: _RelationInstance) -> set[str]:
    cols = {k.lower() for k in inst.table.keys}
    acc = inst.eff_access
    if acc is not None:
        cols.update(c.lower() for c in acc.keys)
        cols.update(c.lower() for c in acc.filterable)
        cols.update(c.lower() for c in acc.range_filterable)
        cols.update(c.lower() for c in acc.prefix_filterable)
        cols.update(c.lower() for c in acc.sortable)
    return cols


def _attach_predicate(
    pred: PredicateFeature,
    instances: list[_RelationInstance],
) -> None:
    """Attach an outer sargable predicate to the matching relation instance(s)."""
    if not instances:
        return
    if pred.table_qualifier is not None:
        inst = _match_instance_by_qualifier(pred.table_qualifier, instances)
        if inst is not None:
            exists, base_col = inst.resolve_column(pred.column)
            if exists and base_col is not None:
                inst.predicates.append(
                    PredicateFeature(
                        column=base_col,
                        op=pred.op,
                        value_count=pred.value_count,
                        table_qualifier=None,
                    )
                )
        return

    if len(instances) == 1:
        inst = instances[0]
        exists, base_col = inst.resolve_column(pred.column)
        if exists and base_col is not None:
            inst.predicates.append(
                PredicateFeature(
                    column=base_col,
                    op=pred.op,
                    value_count=pred.value_count,
                    table_qualifier=None,
                )
            )
        return

    # Unqualified predicate across multiple joined instances:
    # first check explicit projection maps, else match instances whose access metadata declares it.
    explicit_matches: list[tuple[_RelationInstance, str]] = []
    star_matches: list[tuple[_RelationInstance, str]] = []
    for inst in instances:
        if not inst.transparent:
            continue
        if pred.column in inst.proj_map:
            mapped = inst.proj_map[pred.column]
            if mapped is not None:
                explicit_matches.append((inst, mapped))
        elif inst.has_star and pred.column in _all_known_access_cols(inst):
            star_matches.append((inst, pred.column))

    targets = explicit_matches if explicit_matches else star_matches
    for inst, base_col in targets:
        inst.predicates.append(
            PredicateFeature(
                column=base_col,
                op=pred.op,
                value_count=pred.value_count,
                table_qualifier=None,
            )
        )


def _evaluate_direct_filter(
    inst: _RelationInstance,
    policy: Policy,
) -> tuple[bool, bool, list[str]]:
    """Evaluate direct sargable predicates on *inst*.

    Returns ``(satisfied_require_filter, has_any_sargable_filter, matched_columns)``.
    """
    matched_cols: list[str] = []
    acc = inst.eff_access

    if acc is None:
        has_sargable = False
        for p in inst.predicates:
            if p.op in ("eq", "in", "or_eq") and p.value_count <= policy.max_in_list:
                has_sargable = True
                if p.column not in matched_cols:
                    matched_cols.append(p.column)
        return (True, has_sargable, matched_cols)

    keys_tuple = tuple(k.lower() for k in acc.keys)
    keys_set = set(keys_tuple)
    filterable_set = {c.lower() for c in acc.filterable}
    range_set = {c.lower() for c in acc.range_filterable}
    prefix_set = {c.lower() for c in acc.prefix_filterable}

    if not acc.require_filter:
        has_sargable = False
        eq_cols = keys_set | filterable_set
        for p in inst.predicates:
            if p.op in ("eq", "in", "or_eq") and p.value_count <= policy.max_in_list:
                if not eq_cols or p.column in (eq_cols | range_set | prefix_set):
                    has_sargable = True
                    if p.column not in matched_cols:
                        matched_cols.append(p.column)
            elif p.op in ("range", "between") and p.column in range_set:
                has_sargable = True
                if p.column not in matched_cols:
                    matched_cols.append(p.column)
            elif p.op == "prefix_like" and p.column in prefix_set:
                has_sargable = True
                if p.column not in matched_cols:
                    matched_cols.append(p.column)
        return (True, has_sargable, matched_cols)

    # Check primary/unique key equality/IN/OR constraint
    keys_satisfied = False
    if keys_tuple:
        key_min_vc: dict[str, int] = {}
        for p in inst.predicates:
            if (
                p.column in keys_set
                and p.op in ("eq", "in", "or_eq")
                and p.value_count <= policy.max_in_list
            ):
                prev = key_min_vc.get(p.column)
                if prev is None or p.value_count < prev:
                    key_min_vc[p.column] = p.value_count
        if all(k in key_min_vc for k in keys_tuple):
            prod = math.prod(key_min_vc[k] for k in keys_tuple)
            if prod <= policy.point_lookup_max_keys:
                keys_satisfied = True
                for k in keys_tuple:
                    if k not in matched_cols:
                        matched_cols.append(k)

    if acc.pattern == "point":
        # (a) Either all keys constrained <= point_lookup_max_keys,
        # OR a non-key column has a valid sargable filter.
        secondary_satisfied = False
        for p in inst.predicates:
            if p.column in keys_set:
                continue
            if (
                p.op in ("eq", "in", "or_eq")
                and p.value_count <= policy.max_in_list
                and p.column in (filterable_set | range_set | prefix_set)
            ):
                secondary_satisfied = True
                if p.column not in matched_cols:
                    matched_cols.append(p.column)
            elif p.op in ("range", "between") and p.column in range_set:
                secondary_satisfied = True
                if p.column not in matched_cols:
                    matched_cols.append(p.column)
            elif p.op == "prefix_like" and p.column in prefix_set:
                secondary_satisfied = True
                if p.column not in matched_cols:
                    matched_cols.append(p.column)

        ok = keys_satisfied or secondary_satisfied
        return (ok, ok, matched_cols)

    # (b) pattern != 'point'
    filter_satisfied = keys_satisfied
    for p in inst.predicates:
        if (
            p.op in ("eq", "in", "or_eq")
            and p.value_count <= policy.max_in_list
            and p.column in (filterable_set | range_set | prefix_set)
        ):
            filter_satisfied = True
            if p.column not in matched_cols:
                matched_cols.append(p.column)
        elif p.op in ("range", "between") and p.column in range_set:
            filter_satisfied = True
            if p.column not in matched_cols:
                matched_cols.append(p.column)
        elif p.op == "prefix_like" and p.column in prefix_set:
            filter_satisfied = True
            if p.column not in matched_cols:
                matched_cols.append(p.column)

    return (filter_satisfied, filter_satisfied, matched_cols)


def _extract_col_eq_pair(
    conjunct: exp.Expression,
    instances: list[_RelationInstance],
) -> tuple[_RelationInstance, str, _RelationInstance, str] | None:
    """Extract (inst1, base_col1, inst2, base_col2) for bare col1 = col2 across distinct instances."""
    conjunct = _unwrap_paren(conjunct)
    if not isinstance(conjunct, exp.EQ):
        return None
    left = _unwrap_paren(conjunct.this)
    right = _unwrap_paren(conjunct.expression)
    if not (_is_bare_column(left) and _is_bare_column(right)):
        return None
    assert isinstance(left, exp.Column) and isinstance(right, exp.Column)

    q_left = _column_qualifier(left)
    q_right = _column_qualifier(right)
    if q_left is None or q_right is None:
        return None

    inst_l = _match_instance_by_qualifier(q_left, instances)
    inst_r = _match_instance_by_qualifier(q_right, instances)
    if inst_l is None or inst_r is None or inst_l.idx == inst_r.idx:
        return None

    ok_l, base_l = inst_l.resolve_column(left.name.lower())
    ok_r, base_r = inst_r.resolve_column(right.name.lower())
    if not (ok_l and base_l and ok_r and base_r):
        return None

    return (inst_l, base_l, inst_r, base_r)


def _extract_equi_join_edges_and_join_preds(
    sel: exp.Select,
    instances: list[_RelationInstance],
    source_to_inst: dict[int, _RelationInstance],
) -> list[_EquiJoinEdge]:
    """Extract directed equi-join edges from JOIN ON / USING and top-level WHERE."""
    edges: list[_EquiJoinEdge] = []

    # 1. JOIN ON and USING
    seen_prior_insts: list[_RelationInstance] = []
    from_clause = sel.args.get("from")
    if from_clause is not None and from_clause.this is not None:
        first_inst = source_to_inst.get(id(from_clause.this))
        if first_inst is not None:
            seen_prior_insts.append(first_inst)

    for j in sel.args.get("joins") or ():
        if not isinstance(j, exp.Join) or j.this is None:
            continue
        right_inst = source_to_inst.get(id(j.this))
        side = (j.side or "INNER").upper()

        on_expr = j.args.get("on")
        if on_expr is not None:
            for conj in _flatten_and(on_expr):
                pair = _extract_col_eq_pair(conj, instances)
                if pair is not None:
                    inst_a, col_a, inst_b, col_b = pair
                    if side == "INNER":
                        edges.append(_EquiJoinEdge(inst_a.idx, col_a, inst_b.idx, col_b))
                        edges.append(_EquiJoinEdge(inst_b.idx, col_b, inst_a.idx, col_a))
                    elif side == "LEFT" and right_inst is not None:
                        if inst_b.idx == right_inst.idx:
                            edges.append(_EquiJoinEdge(inst_a.idx, col_a, inst_b.idx, col_b))
                        elif inst_a.idx == right_inst.idx:
                            edges.append(_EquiJoinEdge(inst_b.idx, col_b, inst_a.idx, col_a))
                    elif side == "RIGHT" and right_inst is not None:
                        if inst_a.idx == right_inst.idx:
                            edges.append(_EquiJoinEdge(inst_a.idx, col_a, inst_b.idx, col_b))
                        elif inst_b.idx == right_inst.idx:
                            edges.append(_EquiJoinEdge(inst_b.idx, col_b, inst_a.idx, col_a))
                elif side == "INNER":
                    # Inner join ON literal predicates are equivalent to top-level WHERE conjuncts
                    dummy_sel = exp.Select().where(conj, copy=False)
                    for p in _extract_predicates(dummy_sel):
                        _attach_predicate(p, instances)

        using_idents = j.args.get("using") or ()
        if using_idents and right_inst is not None:
            for ident in using_idents:
                col_u = ident.name.lower()
                ok_r, base_r = right_inst.resolve_column(col_u)
                if not (ok_r and base_r):
                    continue
                for left_inst in seen_prior_insts:
                    ok_l, base_l = left_inst.resolve_column(col_u)
                    if not (ok_l and base_l):
                        continue
                    if side == "INNER":
                        edges.append(
                            _EquiJoinEdge(left_inst.idx, base_l, right_inst.idx, base_r)
                        )
                        edges.append(
                            _EquiJoinEdge(right_inst.idx, base_r, left_inst.idx, base_l)
                        )
                    elif side == "LEFT":
                        edges.append(
                            _EquiJoinEdge(left_inst.idx, base_l, right_inst.idx, base_r)
                        )
                    elif side == "RIGHT":
                        edges.append(
                            _EquiJoinEdge(right_inst.idx, base_r, left_inst.idx, base_l)
                        )

        if right_inst is not None:
            seen_prior_insts.append(right_inst)

    # 2. Top-level WHERE bare column equalities (bidirectional)
    where = sel.args.get("where")
    if where is not None and where.this is not None:
        for conj in _flatten_and(where.this):
            pair = _extract_col_eq_pair(conj, instances)
            if pair is not None:
                inst_a, col_a, inst_b, col_b = pair
                edges.append(_EquiJoinEdge(inst_a.idx, col_a, inst_b.idx, col_b))
                edges.append(_EquiJoinEdge(inst_b.idx, col_b, inst_a.idx, col_a))

    return edges


def _select_alias_map(sel: exp.Select) -> dict[str, exp.Column | None]:
    """Map lower-case SELECT alias names to bare Column (or None if non-bare expression)."""
    alias_map: dict[str, exp.Column | None] = {}
    for expr in sel.expressions:
        if isinstance(expr, exp.Alias):
            target = _unwrap_paren(expr.this)
            if _is_bare_column(target):
                assert isinstance(target, exp.Column)
                alias_map[expr.alias.lower()] = target
            else:
                alias_map[expr.alias.lower()] = None
    return alias_map


def _check_order_by_for_select(
    order_items: tuple[OrderByItem, ...],
    sel: exp.Select | None,
    instances: list[_RelationInstance],
    has_group_by: bool,
) -> tuple[str, str] | None:
    """Validate ORDER BY items when any referenced instance has pattern in ('point', 'filter')."""
    if not order_items or has_group_by:
        return None

    guarded_insts = [
        inst
        for inst in instances
        if inst.eff_access is not None
        and inst.eff_access.pattern in ("point", "filter")
    ]
    if not guarded_insts:
        return None

    alias_map = _select_alias_map(sel) if sel is not None else {}

    for item in order_items:
        if not item.is_bare_column or item.column is None:
            tbl_names = ", ".join(inst.table.name for inst in guarded_insts)
            return (
                "sort_not_indexed",
                f"ORDER BY expression is not a bare sortable column on table(s): {tbl_names}",
            )

        col_name = item.column
        qual = item.table_qualifier

        # Resolve unqualified SELECT output alias if present
        if qual is None and col_name in alias_map:
            aliased_col = alias_map[col_name]
            if aliased_col is None:
                return (
                    "sort_not_indexed",
                    f"ORDER BY alias '{col_name}' resolves to a non-bare expression",
                )
            col_name = aliased_col.name.lower()
            qual = _column_qualifier(aliased_col)

        if qual is not None:
            inst = _match_instance_by_qualifier(qual, instances)
            if inst is None:
                return (
                    "sort_not_indexed",
                    f"ORDER BY column '{qual}.{col_name}' does not resolve to an indexed table",
                )
            exists, base_col = inst.resolve_column(col_name)
            if not exists or base_col is None:
                return (
                    "sort_not_indexed",
                    f"ORDER BY column '{qual}.{col_name}' is not a bare base column on '{inst.table.name}'",
                )
            if inst.eff_access is None or inst.eff_access.pattern not in (
                "point",
                "filter",
            ):
                return (
                    "sort_not_indexed",
                    f"ORDER BY column '{qual}.{col_name}' on '{inst.table.name}' cannot use an index on joined point/filter table",
                )
            allowed_sort = {c.lower() for c in inst.eff_access.sortable} | {
                c.lower() for c in inst.eff_access.keys
            }
            if base_col not in allowed_sort:
                return (
                    "sort_not_indexed",
                    f"ORDER BY column '{base_col}' is not in sortable columns for table '{inst.table.name}'",
                )
        else:
            if len(instances) == 1:
                inst = instances[0]
                exists, base_col = inst.resolve_column(col_name)
                if not exists or base_col is None or inst.eff_access is None:
                    return (
                        "sort_not_indexed",
                        f"ORDER BY column '{col_name}' is not a bare sortable column on table '{inst.table.name}'",
                    )
                allowed_sort = {c.lower() for c in inst.eff_access.sortable} | {
                    c.lower() for c in inst.eff_access.keys
                }
                if base_col not in allowed_sort:
                    return (
                        "sort_not_indexed",
                        f"ORDER BY column '{base_col}' is not in sortable columns for table '{inst.table.name}'",
                    )
            else:
                matched_sortable = False
                for inst in guarded_insts:
                    exists, base_col = inst.resolve_column(col_name)
                    if exists and base_col is not None and inst.eff_access is not None:
                        allowed_sort = {
                            c.lower() for c in inst.eff_access.sortable
                        } | {c.lower() for c in inst.eff_access.keys}
                        if base_col in allowed_sort:
                            matched_sortable = True
                            break
                if not matched_sortable:
                    tbl_names = ", ".join(inst.table.name for inst in guarded_insts)
                    return (
                        "sort_not_indexed",
                        f"ORDER BY column '{col_name}' is not in sortable columns on table(s): {tbl_names}",
                    )

    return None


def _check_select_scope(
    sel: exp.Select,
    placement: Placement,
    cte_map: dict[str, exp.Expression],
    *,
    outer_order_items: tuple[OrderByItem, ...] | None = None,
    has_outer_group_by: bool = False,
) -> tuple[list[tuple[str, str]], list[str], set[int]]:
    """Check one Select scope for derived depth, require_filter, and ORDER BY.

    Returns ``(violations, filter_columns, transparent_inner_select_ids)``.
    """
    violations: list[tuple[str, str]] = []
    filter_cols: list[str] = []
    transparent_inner_ids: set[int] = set()

    local_ctes = dict(cte_map)
    local_ctes.update(_collect_cte_map(sel))

    instances: list[_RelationInstance] = []
    source_to_inst: dict[int, _RelationInstance] = {}
    nested_non_plain_queries: list[exp.Expression] = []

    for raw_src, _side in _from_sources_of_select(sel):
        src = _unwrap_paren(raw_src)
        depth, underlying_tables = _measure_from_depth_and_tables(
            src, placement, local_ctes
        )

        if depth >= 2:
            for t in underlying_tables:
                eff = effective_table_access(t)
                if eff is not None and eff.require_filter:
                    violations.append(
                        (
                            "filter_required",
                            f"Subquery nesting depth {depth} in FROM is not permitted for table '{t.name}' requiring a filter",
                        )
                    )
            continue

        if depth == 0 and isinstance(src, exp.Table):
            if isinstance(src.this, exp.Func):
                continue
            dotted = _table_dotted_name(src).lower()
            t_entry = placement.find_table(dotted)
            if t_entry is None:
                continue
            aliases: set[str] = set()
            if src.alias:
                aliases.add(src.alias.lower())
            else:
                aliases.add(_table_leaf_name(src).lower())
                aliases.add(dotted)
                aliases.add(t_entry.name.lower())
            inst = _RelationInstance(
                idx=len(instances),
                table=t_entry,
                eff_access=effective_table_access(t_entry),
                aliases=aliases,
                transparent=True,
                has_star=True,
            )
            instances.append(inst)
            source_to_inst[id(raw_src)] = inst
            continue

        # depth == 1: either Subquery or 1-level CTE reference
        inner_query: exp.Expression | None = None
        outer_alias: str | None = None
        if isinstance(src, exp.Subquery) and src.this is not None:
            inner_query = src.this
            outer_alias = src.alias.lower() if src.alias else None
        elif isinstance(src, exp.Table) and src.name.lower() in local_ctes:
            cte_name = src.name.lower()
            inner_query = local_ctes[cte_name]
            outer_alias = src.alias.lower() if src.alias else cte_name

        if inner_query is not None:
            is_plain, base_tbl, has_star, proj_map = is_plain_projection_derived_select(
                inner_query, placement, local_ctes
            )
            if is_plain and base_tbl is not None:
                inner_sel = _unwrap_paren(inner_query)
                assert isinstance(inner_sel, exp.Select)
                transparent_inner_ids.add(id(inner_sel))
                aliases = {outer_alias} if outer_alias else set()
                inst = _RelationInstance(
                    idx=len(instances),
                    table=base_tbl,
                    eff_access=effective_table_access(base_tbl),
                    aliases=aliases,
                    transparent=True,
                    has_star=has_star,
                    proj_map=proj_map,
                    predicates=[
                        PredicateFeature(
                            column=p.column,
                            op=p.op,
                            value_count=p.value_count,
                            table_qualifier=None,
                        )
                        for p in _extract_predicates(inner_sel)
                    ],
                )
                instances.append(inst)
                source_to_inst[id(raw_src)] = inst

                # Also check inner ORDER BY if present on the derived table
                inner_order = _extract_order_by(inner_sel)
                if inner_order:
                    inner_ord_err = _check_order_by_for_select(
                        inner_order, inner_sel, [inst], False
                    )
                    if inner_ord_err is not None:
                        violations.append(inner_ord_err)
            else:
                nested_non_plain_queries.append(inner_query)
                # Represent non-transparent derived table with underlying tables so outer ORDER BY cannot assume indexed sort
                for t in underlying_tables:
                    aliases = {outer_alias} if outer_alias else set()
                    inst = _RelationInstance(
                        idx=len(instances),
                        table=t,
                        eff_access=effective_table_access(t),
                        aliases=aliases,
                        transparent=False,
                        has_star=False,
                        satisfied=True,  # checked inside nested_non_plain_queries
                    )
                    instances.append(inst)

    # Check non-plain 1-level subqueries in their own scope
    for nq in nested_non_plain_queries:
        for n_sel in _collect_query_selects(nq):
            transparent_inner_ids.add(id(n_sel))
            sub_v, sub_fc, sub_trans = _check_select_scope(
                n_sel, placement, local_ctes
            )
            violations.extend(sub_v)
            for c in sub_fc:
                if c not in filter_cols:
                    filter_cols.append(c)
            transparent_inner_ids.update(sub_trans)

    if violations:
        return (violations, filter_cols, transparent_inner_ids)

    # Attach outer sargable predicates
    for pred in _extract_predicates(sel):
        _attach_predicate(pred, instances)

    # Extract equi-join edges (and INNER JOIN ON sargable predicates)
    edges = _extract_equi_join_edges_and_join_preds(
        sel, instances, source_to_inst
    )

    # Evaluate direct filter satisfaction on each transparent instance
    for inst in instances:
        if not inst.transparent:
            continue
        sat, has_f, m_cols = _evaluate_direct_filter(inst, placement.policy)
        inst.satisfied = sat
        inst.has_filter = has_f
        for c in m_cols:
            if c not in filter_cols:
                filter_cols.append(c)

    # Propagate equi-joins from filtered instances to joined instances
    progress = True
    while progress:
        progress = False
        for edge in edges:
            src_inst = instances[edge.src_idx]
            dst_inst = instances[edge.dst_idx]
            if not src_inst.has_filter or not dst_inst.transparent:
                continue
            if dst_inst.satisfied and dst_inst.has_filter:
                continue

            acc_dst = dst_inst.eff_access
            if acc_dst is None:
                allowed_join_cols = {k.lower() for k in dst_inst.table.keys}
            else:
                allowed_join_cols = {c.lower() for c in acc_dst.filterable} | {
                    c.lower() for c in acc_dst.keys
                }

            if edge.dst_col in allowed_join_cols:
                dst_inst.satisfied = True
                dst_inst.has_filter = True
                if edge.dst_col not in filter_cols:
                    filter_cols.append(edge.dst_col)
                progress = True

    for inst in instances:
        if (
            inst.transparent
            and inst.eff_access is not None
            and inst.eff_access.require_filter
            and not inst.satisfied
        ):
            violations.append(
                (
                    "filter_required",
                    f"Table '{inst.table.name}' requires a selective sargable filter on an indexed column",
                )
            )

    if violations:
        return (violations, filter_cols, transparent_inner_ids)

    # Check ORDER BY on this Select (and any outer SetOperation ORDER BY passed in)
    sel_has_group_by = bool(sel.args.get("group")) or has_outer_group_by
    sel_order = _extract_order_by(sel)
    if sel_order:
        ord_err = _check_order_by_for_select(
            sel_order, sel, instances, sel_has_group_by
        )
        if ord_err is not None:
            violations.append(ord_err)

    if not violations and outer_order_items:
        ord_err = _check_order_by_for_select(
            outer_order_items, sel, instances, sel_has_group_by
        )
        if ord_err is not None:
            violations.append(ord_err)

    return (violations, filter_cols, transparent_inner_ids)


def _check_all_offsets(
    statement: exp.Expression,
    max_offset: int,
) -> tuple[str, str] | None:
    """Validate every OFFSET clause in *statement* against *max_offset*."""
    for offset_node in statement.find_all(exp.Offset):
        expr = (
            _unwrap_paren(offset_node.expression)
            if offset_node.expression is not None
            else None
        )
        if (
            expr is None
            or not isinstance(expr, exp.Literal)
            or expr.is_string
            or not expr.is_int
        ):
            return (
                "offset_too_large",
                f"OFFSET must be a non-negative integer literal <= {max_offset}",
            )
        try:
            val = int(expr.this)
        except (ValueError, TypeError):
            return (
                "offset_too_large",
                f"OFFSET must be a non-negative integer literal <= {max_offset}",
            )
        if val < 0 or val > max_offset:
            return (
                "offset_too_large",
                f"OFFSET {val} exceeds maximum permitted offset {max_offset}",
            )
    return None


def _compute_access_pattern(referenced_tables: list[TableEntry]) -> str | None:
    best_pat: str | None = None
    best_prio = 0
    for tbl in referenced_tables:
        eff = effective_table_access(tbl)
        if eff is not None:
            prio = _PATTERN_PRIORITY.get(eff.pattern, 0)
            if prio > best_prio:
                best_prio = prio
                best_pat = eff.pattern
    return best_pat


def check_guardrails(
    statement: exp.Expression,
    *,
    decision: RoutingDecision,
    placement: Placement,
    grant: Any = None,
    request_max_rows: int | None = None,
    metrics: Any = None,
) -> GuardrailResult:
    """Run table access guardrails after routing and before policy enforcement."""
    referenced_tables: list[TableEntry] = []
    for name in decision.features.tables:
        tbl = placement.find_table(name)
        if tbl is not None:
            referenced_tables.append(tbl)

    is_select_query = decision.features.kind == "select" or isinstance(
        statement, exp.Query
    )

    effective_max_rows: int | None = None
    if is_select_query:
        caps: list[int] = []
        if grant is not None and getattr(grant, "max_rows", None) is not None:
            caps.append(int(grant.max_rows))
        if request_max_rows is not None:
            caps.append(int(request_max_rows))
        for tbl in referenced_tables:
            if tbl.access is not None and tbl.access.max_rows is not None:
                caps.append(tbl.access.max_rows)
        if caps:
            effective_max_rows = min(caps)

    access_pattern = (
        _compute_access_pattern(referenced_tables) if is_select_query else None
    )

    target_engine = placement.engines.get(decision.engine)
    if (
        not is_select_query
        or target_engine is None
        or not is_postgres_wire(target_engine.kind)
    ):
        non_pg_filter_cols: tuple[str, ...] = ()
        if is_select_query and decision.features.predicates:
            seen_cols: list[str] = []
            for p in decision.features.predicates:
                if (
                    p.value_count <= placement.policy.max_in_list
                    and p.column not in seen_cols
                ):
                    seen_cols.append(p.column)
            non_pg_filter_cols = tuple(seen_cols)
        return GuardrailResult(
            outcome="pass",
            access_pattern=access_pattern,
            filter_columns=non_pg_filter_cols,
            effective_max_rows=effective_max_rows,
        )

    cte_map = _collect_cte_map(statement)
    violations: list[tuple[str, str]] = []
    filter_cols: list[str] = []
    checked_select_ids: set[int] = set()

    top_selects = _collect_query_selects(statement)
    outer_setop_order = (
        _extract_order_by(statement)
        if isinstance(statement, _SET_OP_CLASSES)
        else None
    )
    has_any_group_by = decision.features.has_group_by

    for sel in top_selects:
        checked_select_ids.add(id(sel))
        v_list, fc_list, trans_ids = _check_select_scope(
            sel,
            placement,
            cte_map,
            outer_order_items=outer_setop_order,
            has_outer_group_by=has_any_group_by,
        )
        violations.extend(v_list)
        for c in fc_list:
            if c not in filter_cols:
                filter_cols.append(c)
        checked_select_ids.update(trans_ids)

    # Also check any remaining subqueries (e.g. IN (SELECT ...) or EXISTS (SELECT ...) in WHERE)
    if not violations:
        cte_body_ids = {id(v) for v in cte_map.values()}
        for sub_sel in statement.find_all(exp.Select):
            if id(sub_sel) in checked_select_ids or id(sub_sel) in cte_body_ids:
                continue
            checked_select_ids.add(id(sub_sel))
            v_list, fc_list, trans_ids = _check_select_scope(
                sub_sel, placement, cte_map
            )
            violations.extend(v_list)
            for c in fc_list:
                if c not in filter_cols:
                    filter_cols.append(c)
            checked_select_ids.update(trans_ids)

    # Check OFFSET limits across statement
    if not violations:
        off_err = _check_all_offsets(statement, placement.policy.max_offset)
        if off_err is not None:
            violations.append(off_err)

    if violations:
        first_code, first_msg = violations[0]
        if placement.policy.guard_mode == "warn":
            _record_guard_warning(metrics, first_code)
            warn_msgs = tuple(f"{c}: {m}" for c, m in violations)
            return GuardrailResult(
                outcome="warn",
                access_pattern=access_pattern,
                filter_columns=tuple(filter_cols),
                effective_max_rows=effective_max_rows,
                warnings=warn_msgs,
            )
        _record_guard_rejection(metrics, first_code)
        raise GuardrailError(
            first_msg,
            code=first_code,
            guard="reject",
            access_pattern=access_pattern,
            filter_columns=tuple(filter_cols),
            effective_max_rows=effective_max_rows,
            engine=decision.engine,
            engine_kind=target_engine.kind if target_engine is not None else None,
            rule=decision.rule,
        )

    return GuardrailResult(
        outcome="pass",
        access_pattern=access_pattern,
        filter_columns=tuple(filter_cols),
        effective_max_rows=effective_max_rows,
    )

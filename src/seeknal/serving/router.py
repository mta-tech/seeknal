"""SQL statement router and query rewriter for Seeknal Serving."""

from __future__ import annotations

import math
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.parser import Parser
from sqlglot.tokens import TokenType

from seeknal.serving.features import QueryFeatures, extract_features
from seeknal.serving.placement import Engine, Placement, TableEntry

_orig_parse_version = Parser._parse_version
_orig_parse_table_alias = Parser._parse_table_alias
_orig_parse_table = Parser._parse_table


def _patched_parse_version(self: Parser) -> exp.Version | None:
    res = _orig_parse_version(self)
    if res is not None:
        return res
    idx = self._index
    if self._match_text_seq("AT") and self._match(TokenType.L_PAREN):
        if self._match_text_seq("VERSION") and self._match(TokenType.FARROW):
            expr = self._parse_bitwise()
            if self._match(TokenType.R_PAREN):
                v = self.expression(
                    exp.Version, this="VERSION", expression=expr, kind="AS OF"
                )
                if v is not None:
                    v.meta["duckdb_at_version"] = True
                return v
    self._retreat(idx)
    return None


def _patched_parse_table_alias(
    self: Parser, alias_tokens: Any = None
) -> exp.Expression | None:
    alias = _orig_parse_table_alias(self, alias_tokens=alias_tokens)
    if alias is not None:
        ver = self._parse_version()
        if ver is not None:
            alias.meta["post_alias_version"] = ver
    return alias


def _patched_parse_table(self: Parser, *args: Any, **kwargs: Any) -> exp.Expression | None:
    this = _orig_parse_table(self, *args, **kwargs)
    if isinstance(this, exp.Table):
        alias = this.args.get("alias")
        if alias is not None and alias.meta.get("post_alias_version") is not None:
            this.set("version", alias.meta.pop("post_alias_version"))
    return this


Parser._parse_version = _patched_parse_version
Parser._parse_table_alias = _patched_parse_table_alias
Parser._parse_table = _patched_parse_table


def _duckdb_version_sql(generator: Any, expression: exp.Version) -> str:
    if expression.meta.get("duckdb_at_version"):
        expr = generator.sql(expression, "expression")
        return f"AT (VERSION => {expr})"
    return generator.version_sql(expression)


def _duckdb_table_sql(generator: Any, expression: exp.Table, sep: str = " AS ") -> str:
    ver = expression.args.get("version")
    if ver is not None and ver.meta.get("duckdb_at_version"):
        expr_copy = expression.copy()
        expr_copy.set("version", None)
        joins = expr_copy.args.get("joins")
        laterals = expr_copy.args.get("laterals")
        expr_copy.set("joins", None)
        expr_copy.set("laterals", None)
        base_with_alias = generator.table_sql(expr_copy, sep=sep)
        ver_str = generator.sql(ver)
        joins_str = generator.indent(
            generator.expressions(expression, key="joins", sep="", flat=True),
            skip_first=True,
        )
        laterals_str = generator.expressions(expression, key="laterals", sep="")
        return f"{base_with_alias} {ver_str}{joins_str}{laterals_str}"
    return generator.table_sql(expression, sep=sep)


DuckDB.Generator.TRANSFORMS[exp.Version] = _duckdb_version_sql
DuckDB.Generator.TRANSFORMS[exp.Table] = _duckdb_table_sql


def _table_dotted_name(tbl: exp.Table) -> str:
    if tbl.parts:
        return ".".join(part.name for part in tbl.parts)
    parts: list[str] = []
    if tbl.catalog:
        parts.append(tbl.catalog)
    if tbl.db:
        parts.append(tbl.db)
    if tbl.name:
        parts.append(tbl.name)
    return ".".join(parts)


def _table_leaf_name(tbl: exp.Table) -> str:
    if tbl.parts:
        return tbl.parts[-1].name
    return tbl.name

HINT_PATTERN = re.compile(
    r"^\s*seeknal:engine\s*=\s*([a-zA-Z0-9_\-]+)\s*$", re.IGNORECASE
)


PII_CROSS_ENGINE_CODE = "pii_cross_engine"
_SAFE_PII_IDENT_RE = re.compile(r"^[\w][\w.-]{0,127}$", re.UNICODE)
_CANARY_RE = re.compile(r"(canary|sentinel|opaque)", re.IGNORECASE)


def _safe_router_ident(value: Any) -> str:
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    s = value.strip()
    if not s:
        return "<empty>"
    if "://" in s or "@" in s or _CANARY_RE.search(s) or not _SAFE_PII_IDENT_RE.fullmatch(s):
        return "<redacted>"
    return s


class RoutingError(Exception):
    """Raised when a SQL statement cannot be routed or served."""

    def __init__(self, message: str = "", *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class PiiCrossEngineError(RoutingError):
    """Typed routing error raised when a query violates PII engine isolation."""

    code: str = PII_CROSS_ENGINE_CODE

    def __init__(self, message: str) -> None:
        super().__init__(message, code=PII_CROSS_ENGINE_CODE)


def check_pii_query_mixing(
    referenced_tables: Sequence[TableEntry],
    *,
    hint: str | None = None,
    extra_pii_tables: Collection[str] | None = None,
) -> None:
    """Enforce rule E.3: PII tables cannot be mixed across engines or forced elsewhere via hint.

    Raises:
        PiiCrossEngineError: subclass of ``RoutingError`` with ``.code == 'pii_cross_engine'``
            before any engine is called.
    """
    extra_lower = (
        frozenset(str(p).lower() for p in extra_pii_tables)
        if extra_pii_tables
        else frozenset()
    )
    pii_tables = [
        t
        for t in referenced_tables
        if bool(getattr(t, "pii", False))
        or bool(getattr(t, "has_pii", False))
        or bool(getattr(t, "pii_columns", ()))
        or t.name.lower() in extra_lower
    ]
    if not pii_tables:
        return

    for pii_tbl in pii_tables:
        if len(pii_tbl.serves) != 1:
            raise PiiCrossEngineError(
                f"PII table '{_safe_router_ident(pii_tbl.name)}' must have exactly one serve, "
                f"got {len(pii_tbl.serves)}"
            )

    primary_pii = pii_tables[0]
    pii_engine = primary_pii.serves[0].engine

    for other_pii in pii_tables[1:]:
        other_eng = other_pii.serves[0].engine
        if other_eng != pii_engine:
            raise PiiCrossEngineError(
                f"PII query cannot combine PII table '{_safe_router_ident(primary_pii.name)}' "
                f"(engine '{_safe_router_ident(pii_engine)}') with PII table "
                f"'{_safe_router_ident(other_pii.name)}' (engine '{_safe_router_ident(other_eng)}')"
            )

    for tbl in referenced_tables:
        if not any(s.engine == pii_engine for s in tbl.serves):
            raise PiiCrossEngineError(
                f"PII table '{_safe_router_ident(primary_pii.name)}' is served on engine "
                f"'{_safe_router_ident(pii_engine)}', and cannot be queried together with table "
                f"'{_safe_router_ident(tbl.name)}' which is not served on '{_safe_router_ident(pii_engine)}'"
            )

    if hint is not None:
        clean_hint = hint.strip()
        if clean_hint != pii_engine:
            raise PiiCrossEngineError(
                f"PII table '{_safe_router_ident(primary_pii.name)}' is served only on engine "
                f"'{_safe_router_ident(pii_engine)}'; engine hint '{_safe_router_ident(clean_hint)}' is not permitted"
            )


@dataclass(frozen=True)
class RoutingDecision:
    """Routing outcome and rewritten SQL statement for a chosen engine."""

    engine: str
    sql: str
    rule: str
    reason: str
    features: QueryFeatures
    feasible: tuple[str, ...]
    rejected: Mapping[str, str]

    def explain(self) -> str:
        """Return a multi-line human summary of the routing decision."""
        lines = [
            f"Engine: {self.engine}",
            f"Rule: {self.rule}",
            f"Reason: {self.reason}",
        ]
        if self.rejected:
            lines.append("Rejected engines:")
            for eng, rej_reason in self.rejected.items():
                lines.append(f"  {eng}: {rej_reason}")
        return "\n".join(lines)


def _extract_hint_comment(statement: exp.Expression) -> str | None:
    """Extract engine hint from leading statement comment if present."""
    if statement.comments:
        first_comment = statement.comments[0]
        match = HINT_PATTERN.match(first_comment.strip())
        if match:
            return match.group(1).strip()
    return None


def _remove_hint_comment(statement: exp.Expression) -> None:
    """Remove leading hint comment from statement if present."""
    if statement.comments and HINT_PATTERN.match(statement.comments[0].strip()):
        statement.comments.pop(0)


def _select_engine_by_kinds(
    feasible: Sequence[str],
    kinds_order: Sequence[str],
    engines: Mapping[str, Engine],
) -> str | None:
    """Select the first feasible engine matching kind order."""
    for kind in kinds_order:
        for eng_name in feasible:
            if engines[eng_name].kind == kind:
                return eng_name
    return None


class Router:
    """Routes SQL statements to configured database and semantic engines."""

    def __init__(
        self,
        placement: Placement,
        *,
        pii_tables: Collection[str] = (),
    ) -> None:
        self.placement = placement
        placement_pii = getattr(placement, "pii_tables", ()) or ()
        self._pii_tables = frozenset(
            str(t).lower() for t in (*placement_pii, *pii_tables)
        )

    def _is_pii_table(
        self,
        table: TableEntry,
        extra_pii: Collection[str] | None = None,
    ) -> bool:
        if (
            bool(getattr(table, "pii", False))
            or bool(getattr(table, "has_pii", False))
            or bool(getattr(table, "pii_columns", ()))
        ):
            return True
        lower = table.name.lower()
        if lower in self._pii_tables:
            return True
        if extra_pii and any(str(p).lower() == lower for p in extra_pii):
            return True
        return False

    def _is_pii_cube(
        self,
        cube: Any,
        extra_pii: Collection[str] | None = None,
    ) -> bool:
        if bool(getattr(cube, "pii", False)):
            return True
        lower = cube.name.lower()
        if lower in self._pii_tables:
            return True
        if extra_pii and any(str(p).lower() == lower for p in extra_pii):
            return True
        return False

    def rewrite(self, statement: exp.Expression, engine: str) -> str:
        """Replace logical table references with that engine's physical names and render in engine dialect."""
        if engine not in self.placement.engines:
            raise RoutingError(f"Unknown engine '{engine}'")
        target_engine = self.placement.engines[engine]

        _remove_hint_comment(statement)

        # Rewrite table references
        cte_names = {
            cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)
        }
        for tbl in statement.find_all(exp.Table):
            if tbl.name.lower() in cte_names and not tbl.db and not tbl.catalog:
                continue
            dotted = _table_dotted_name(tbl).lower()
            if self.placement.find_cube(dotted) is not None:
                continue
            t_entry = self.placement.find_table(dotted)
            if t_entry is None:
                continue
            serve = next(
                (s for s in t_entry.serves if s.engine == engine), None
            )
            if serve is None:
                continue

            new_tbl = sqlglot.to_table(serve.physical_name, dialect=target_engine.dialect)
            if serve.snapshot_id is not None and target_engine.dialect == "duckdb":
                snap_expr = exp.Literal.number(int(serve.snapshot_id))
                ver_node = exp.Version(
                    this="VERSION",
                    expression=snap_expr,
                    kind="AS OF",
                )
                ver_node.meta["duckdb_at_version"] = True
                new_tbl.set("version", ver_node)

            orig_alias = tbl.alias
            alias_to_use = orig_alias if orig_alias else _table_leaf_name(tbl)
            new_tbl.set(
                "alias", exp.TableAlias(this=exp.to_identifier(alias_to_use))
            )
            tbl.replace(new_tbl)

        return statement.sql(dialect=target_engine.dialect)

    def route(
        self,
        sql: str,
        *,
        dialect: str = "postgres",
        hint: str | None = None,
        max_staleness_seconds: float | None = None,
    ) -> RoutingDecision:
        """Route and rewrite a SQL statement for the best execution engine."""
        return self.decide(
            sql,
            dialect=dialect,
            hint=hint,
            max_staleness_seconds=max_staleness_seconds,
        )

    def decide(
        self,
        sql: str,
        *,
        dialect: str = "postgres",
        hint: str | None = None,
        max_staleness_seconds: float | None = None,
        pii_tables: Collection[str] | None = None,
    ) -> RoutingDecision:
        """Route a SQL statement, picking the best engine and returning a RoutingDecision."""
        # 1. Parse statement
        try:
            statements = sqlglot.parse(sql, read=dialect)
        except Exception as exc:
            raise RoutingError(
                f"Failed to parse SQL statement: {exc}", code="invalid_sql"
            ) from exc

        valid_statements = [s for s in statements if s is not None]
        if not valid_statements:
            raise RoutingError("Empty SQL statement", code="invalid_sql")
        if len(valid_statements) > 1:
            raise RoutingError(
                f"Expected exactly 1 statement, got {len(valid_statements)}",
                code="invalid_sql",
            )

        statement = valid_statements[0]
        return self.decide_statement(
            statement,
            hint=hint,
            max_staleness_seconds=max_staleness_seconds,
            pii_tables=pii_tables,
        )

    def decide_statement(
        self,
        statement: exp.Expression,
        *,
        hint: str | None = None,
        max_staleness_seconds: float | None = None,
        pii_tables: Collection[str] | None = None,
    ) -> RoutingDecision:
        """Route a SQL statement AST, picking the best engine and returning a RoutingDecision."""
        # 2. Extract features
        features = extract_features(statement)
        if features.kind in ("ddl", "other"):
            raise RoutingError(
                f"Statement kind '{features.kind}' not served",
                code="invalid_sql",
            )

        # 3. Resolve referenced names
        referenced_cubes = []
        referenced_tables: list[TableEntry] = []
        for name in features.tables:
            cube = self.placement.find_cube(name)
            if cube is not None:
                referenced_cubes.append(cube)
            else:
                table = self.placement.find_table(name)
                if table is not None:
                    referenced_tables.append(table)
                else:
                    raise RoutingError(f"Unknown table or cube '{name}'")

        has_pii_table = any(
            self._is_pii_table(t, pii_tables) for t in referenced_tables
        )
        has_pii_cube = any(
            self._is_pii_cube(c, pii_tables) for c in referenced_cubes
        )
        has_pii = has_pii_table or has_pii_cube

        if referenced_cubes and referenced_tables:
            raise RoutingError(
                "Cannot mix cubes and tables in the same statement",
                code="pii_cross_engine" if has_pii else "query_spans_engines",
            )

        if len(referenced_cubes) > 1:
            cube_engines = {c.engine for c in referenced_cubes}
            if len(cube_engines) > 1:
                raise RoutingError(
                    f"Cubes on different engines referenced: {cube_engines}",
                    code="pii_cross_engine" if has_pii_cube else "query_spans_engines",
                )

        resolved_hint = hint
        if resolved_hint is None:
            resolved_hint = _extract_hint_comment(statement)

        if has_pii_cube and resolved_hint is not None:
            pii_cube = next(
                c for c in referenced_cubes if self._is_pii_cube(c, pii_tables)
            )
            clean_hint = resolved_hint.strip()
            if clean_hint != pii_cube.engine:
                raise PiiCrossEngineError(
                    f"PII cube '{_safe_router_ident(pii_cube.name)}' is served only on engine "
                    f"'{_safe_router_ident(pii_cube.engine)}'; engine hint '{_safe_router_ident(clean_hint)}' is not permitted"
                )

        pii_sole_engine: str | None = None
        if has_pii_table:
            merged_extra_pii = self._pii_tables | (
                frozenset(str(p).lower() for p in pii_tables)
                if pii_tables
                else frozenset()
            )
            check_pii_query_mixing(
                referenced_tables,
                hint=resolved_hint,
                extra_pii_tables=merged_extra_pii,
            )
            first_pii_tbl = next(
                t for t in referenced_tables if self._is_pii_table(t, pii_tables)
            )
            pii_sole_engine = first_pii_tbl.serves[0].engine

        # 4. Feasible engines
        feasible_list: list[str] = []
        rejected: dict[str, str] = {}
        stale_rejected_engines: set[str] = set()
        placement_engines = tuple(self.placement.engines.keys())

        if referenced_cubes:
            target_cube = referenced_cubes[0]
            for eng_name in placement_engines:
                if eng_name == target_cube.engine:
                    feasible_list.append(eng_name)
                else:
                    rejected[eng_name] = (
                        f"Engine '{eng_name}' does not host cube '{target_cube.name}'"
                    )
        else:
            for eng_name in placement_engines:
                if pii_sole_engine is not None and eng_name != pii_sole_engine:
                    rejected[eng_name] = (
                        f"Engine '{eng_name}' is not permitted for PII table (restricted to '{pii_sole_engine}')"
                    )
                    continue

                missing_tables: list[str] = []
                stale_tables: list[str] = []
                for table in referenced_tables:
                    serve = next(
                        (s for s in table.serves if s.engine == eng_name), None
                    )
                    if serve is None:
                        missing_tables.append(table.name)
                    elif max_staleness_seconds is not None:
                        if (
                            serve.max_lag_seconds is not None
                            and serve.max_lag_seconds > max_staleness_seconds
                        ):
                            stale_tables.append(
                                f"table '{table.name}' lag {serve.max_lag_seconds}s > max staleness {max_staleness_seconds}s"
                            )

                if missing_tables:
                    rejected[eng_name] = (
                        f"Engine '{eng_name}' does not serve: {', '.join(missing_tables)}"
                    )
                elif stale_tables:
                    stale_rejected_engines.add(eng_name)
                    rejected[eng_name] = (
                        f"Engine '{eng_name}' rejected due to staleness: {'; '.join(stale_tables)}"
                    )
                else:
                    feasible_list.append(eng_name)

        feasible: tuple[str, ...] = tuple(feasible_list)

        # 5. Hint
        if resolved_hint is not None:
            if resolved_hint not in self.placement.engines:
                raise RoutingError(f"Hint specifies unknown engine '{resolved_hint}'")
            if (
                features.kind in ("insert", "update", "delete", "merge")
                and self.placement.engines[resolved_hint].kind != "lakebase"
            ):
                raise RoutingError(
                    f"Writes go only to Lakebase; hinted engine '{resolved_hint}' is of kind '{self.placement.engines[resolved_hint].kind}'"
                )
            if resolved_hint not in feasible:
                rej_msg = rejected.get(resolved_hint, "infeasible")
                if resolved_hint in stale_rejected_engines:
                    hint_code = "data_stale"
                elif has_pii:
                    hint_code = "pii_cross_engine"
                else:
                    hint_code = "query_spans_engines"
                raise RoutingError(
                    f"Hinted engine '{resolved_hint}' is not feasible: {rej_msg}",
                    code=hint_code,
                )

        # 6. Pick engine and rule with rewrite retry loop
        current_feasible = list(feasible)

        while current_feasible:
            chosen_engine: str | None = None
            rule: str = ""
            reason: str = ""

            if resolved_hint is not None:
                chosen_engine = resolved_hint
                rule = "hint"
                reason = f"Routing forced to engine '{resolved_hint}' by hint"
            elif features.kind in ("insert", "update", "delete", "merge"):
                rule = "write"
                candidate = next(
                    (
                        e
                        for e in current_feasible
                        if self.placement.engines[e].kind == "lakebase"
                    ),
                    None,
                )
                if candidate is None:
                    raise RoutingError(
                        "No feasible engine of kind 'lakebase' available for write statement"
                    )
                chosen_engine = candidate
                reason = (
                    f"write statement of kind '{features.kind}' routed to lakebase"
                )
            elif referenced_cubes:
                rule = "semantic"
                chosen_engine = referenced_cubes[0].engine
                reason = f"semantic query over cube '{referenced_cubes[0].name}'"
            else:
                # Check point_lookup
                point_lookup_matched = False
                if (
                    features.kind == "select"
                    and len(referenced_tables) == 1
                    and len(referenced_tables[0].keys) > 0
                    and not features.has_aggregate
                    and not features.has_group_by
                    and not features.has_window
                    and not features.has_join
                    and not features.has_subquery
                ):
                    tbl = referenced_tables[0]
                    if all(k.lower() in features.key_equalities for k in tbl.keys):
                        key_prod = math.prod(
                            features.key_equalities[k.lower()] for k in tbl.keys
                        )
                        if key_prod <= self.placement.policy.point_lookup_max_keys:
                            point_lookup_matched = True

                if point_lookup_matched:
                    lakebase_eng = next(
                        (
                            e
                            for e in current_feasible
                            if self.placement.engines[e].kind == "lakebase"
                        ),
                        None,
                    )
                    if lakebase_eng is not None:
                        tbl = referenced_tables[0]
                        chosen_engine = lakebase_eng
                        rule = "point_lookup"
                        parts = []
                        for k in tbl.keys:
                            cnt = features.key_equalities[k.lower()]
                            val_word = "1 value" if cnt == 1 else f"{cnt} values"
                            parts.append(f"{k} by {val_word}")
                        reason = f"filters {', '.join(parts)} on table {tbl.name}"

                # If point_lookup not matched or no lakebase engine feasible, fall through
                if chosen_engine is None:
                    # Check analytics
                    is_analytics = (
                        features.has_aggregate
                        or features.has_group_by
                        or features.has_window
                        or features.has_join
                        or features.has_distinct
                        or any(
                            t.row_count is None
                            or t.row_count
                            >= self.placement.policy.analytics_min_rows
                            for t in referenced_tables
                        )
                    )

                    if is_analytics:
                        rule = "analytics"
                        reasons = []
                        if features.has_aggregate:
                            reasons.append("aggregation")
                        if features.has_group_by:
                            reasons.append("group by")
                        if features.has_window:
                            reasons.append("window function")
                        if features.has_join:
                            reasons.append("join")
                        if features.has_distinct:
                            reasons.append("distinct")
                        for t in referenced_tables:
                            if t.row_count is None:
                                reasons.append(f"unknown row count on {t.name}")
                            elif (
                                t.row_count
                                >= self.placement.policy.analytics_min_rows
                            ):
                                reasons.append(
                                    f"row count {t.row_count} >= {self.placement.policy.analytics_min_rows} on {t.name}"
                                )
                        reason = f"analytics query: {', '.join(reasons)}"

                        chosen_engine = _select_engine_by_kinds(
                            current_feasible,
                            self.placement.policy.analytics_order,
                            self.placement.engines,
                        )
                        if chosen_engine is None:
                            chosen_engine = _select_engine_by_kinds(
                                current_feasible,
                                self.placement.policy.small_scan_order,
                                self.placement.engines,
                            )
                        if chosen_engine is None and current_feasible:
                            chosen_engine = current_feasible[0]
                    else:
                        rule = "small_scan"
                        reason = f"scan over small table(s) (< {self.placement.policy.analytics_min_rows} rows)"
                        chosen_engine = _select_engine_by_kinds(
                            current_feasible,
                            self.placement.policy.small_scan_order,
                            self.placement.engines,
                        )
                        if chosen_engine is None:
                            chosen_engine = _select_engine_by_kinds(
                                current_feasible,
                                self.placement.policy.analytics_order,
                                self.placement.engines,
                            )
                        if chosen_engine is None and current_feasible:
                            chosen_engine = current_feasible[0]

            if chosen_engine is None:
                raise RoutingError("No feasible engine available for routing")

            # 7. Rewrite statement for chosen engine
            target_engine = self.placement.engines[chosen_engine]
            try:
                rendered_sql = self.rewrite(statement.copy(), chosen_engine)
                return RoutingDecision(
                    engine=chosen_engine,
                    sql=rendered_sql,
                    rule=rule,
                    reason=reason,
                    features=features,
                    feasible=feasible,
                    rejected=rejected,
                )
            except Exception as exc:
                rejected[chosen_engine] = (
                    f"Failed to render SQL in dialect '{target_engine.dialect}': {exc}"
                )
                current_feasible.remove(chosen_engine)
                if resolved_hint is not None and chosen_engine == resolved_hint:
                    raise RoutingError(
                        f"Hinted engine '{chosen_engine}' failed during SQL rendering: {exc}"
                    )

        if not feasible and stale_rejected_engines:
            infeasible_code = "data_stale"
        elif has_pii and len(referenced_tables) >= 2:
            infeasible_code = "pii_cross_engine"
        else:
            infeasible_code = "query_spans_engines"

        raise RoutingError(
            f"No feasible engine could serve the statement: {rejected}",
            code=infeasible_code,
        )

"""SQL statement router and query rewriter for Seeknal Serving."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Mapping, Sequence

import sqlglot
from sqlglot import exp

from seeknal.serving.features import QueryFeatures, extract_features
from seeknal.serving.placement import Placement, TableEntry

HINT_PATTERN = re.compile(
    r"^\s*seeknal:engine\s*=\s*([a-zA-Z0-9_\-]+)\s*$", re.IGNORECASE
)


class RoutingError(Exception):
    """Raised when a SQL statement cannot be routed or served."""


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
    engines: Mapping[str, any],
) -> str | None:
    """Select the first feasible engine matching kind order."""
    for kind in kinds_order:
        for eng_name in feasible:
            if engines[eng_name].kind == kind:
                return eng_name
    return None


class Router:
    """Routes SQL statements to configured database and semantic engines."""

    def __init__(self, placement: Placement) -> None:
        self.placement = placement

    def route(
        self,
        sql: str,
        *,
        dialect: str = "postgres",
        hint: str | None = None,
        max_staleness_seconds: float | None = None,
    ) -> RoutingDecision:
        """Route and rewrite a SQL statement for the best execution engine."""
        # 1. Parse statement
        try:
            statements = sqlglot.parse(sql, read=dialect)
        except Exception as exc:
            raise RoutingError(f"Failed to parse SQL statement: {exc}") from exc

        valid_statements = [s for s in statements if s is not None]
        if not valid_statements:
            raise RoutingError("Empty SQL statement")
        if len(valid_statements) > 1:
            raise RoutingError(
                f"Expected exactly 1 statement, got {len(valid_statements)}"
            )

        statement = valid_statements[0]

        # 2. Extract features
        features = extract_features(statement)
        if features.kind in ("ddl", "other"):
            raise RoutingError(f"Statement kind '{features.kind}' not served")

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

        if referenced_cubes and referenced_tables:
            raise RoutingError("Cannot mix cubes and tables in the same statement")

        if len(referenced_cubes) > 1:
            cube_engines = {c.engine for c in referenced_cubes}
            if len(cube_engines) > 1:
                raise RoutingError(
                    f"Cubes on different engines referenced: {cube_engines}"
                )

        # 4. Feasible engines
        feasible_list: list[str] = []
        rejected: dict[str, str] = {}
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
                    rejected[eng_name] = (
                        f"Engine '{eng_name}' rejected due to staleness: {'; '.join(stale_tables)}"
                    )
                else:
                    feasible_list.append(eng_name)

        feasible: tuple[str, ...] = tuple(feasible_list)

        # 5. Hint
        resolved_hint = hint
        if resolved_hint is None:
            resolved_hint = _extract_hint_comment(statement)

        if resolved_hint is not None:
            if resolved_hint not in self.placement.engines:
                raise RoutingError(f"Hint specifies unknown engine '{resolved_hint}'")
            if resolved_hint not in feasible:
                rej_msg = rejected.get(resolved_hint, "infeasible")
                raise RoutingError(
                    f"Hinted engine '{resolved_hint}' is not feasible: {rej_msg}"
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
            rewritten = statement.copy()
            _remove_hint_comment(rewritten)

            # Rewrite table references
            cte_names = {
                cte.alias_or_name.lower() for cte in rewritten.find_all(exp.CTE)
            }
            for tbl in rewritten.find_all(exp.Table):
                if tbl.name.lower() in cte_names and not tbl.db and not tbl.catalog:
                    continue
                parts = []
                if tbl.catalog:
                    parts.append(tbl.catalog)
                if tbl.db:
                    parts.append(tbl.db)
                if tbl.name:
                    parts.append(tbl.name)
                dotted = ".".join(parts).lower()
                if self.placement.find_cube(dotted) is not None:
                    continue
                t_entry = self.placement.find_table(dotted)
                if t_entry is None:
                    continue
                serve = next(
                    (s for s in t_entry.serves if s.engine == chosen_engine), None
                )
                if serve is None:
                    continue

                new_tbl = sqlglot.to_table(serve.physical_name)
                orig_alias = tbl.alias
                alias_to_use = orig_alias if orig_alias else tbl.name
                new_tbl.set(
                    "alias", exp.TableAlias(this=exp.to_identifier(alias_to_use))
                )
                tbl.replace(new_tbl)

            target_engine = self.placement.engines[chosen_engine]
            try:
                rendered_sql = rewritten.sql(dialect=target_engine.dialect)
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

        raise RoutingError(
            f"No feasible engine could serve the statement: {rejected}"
        )

"""Query policy enforcement for Seeknal Serving.

Enforces Catalog grants (table access, visible columns, column masks,
row filters, and row cap) on client SQL before engine execution.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

VALID_MASKS = frozenset({"redact", "null", "hash", "last4", "year"})
VALID_OPS = frozenset({"eq", "in", "gte", "lte"})
VALID_ACTIONS = frozenset({"select", "insert", "update", "delete"})
VALID_TARGET_KINDS = frozenset({"starrocks", "lakebase", "duckdb", "cube"})
VALID_TARGET_DIALECTS = frozenset({"postgres", "starrocks", "duckdb"})

ANONYMOUS_FUNCTION_ALLOWLIST: dict[str, set[str]] = {
    "cube": {"measure"},
    "starrocks": set(),
    "lakebase": set(),
    "duckdb": set(),
}

DISALLOWED_FUNCTION_PREFIXES: tuple[str, ...] = (
    "pg_",
    "read_",
    "lo_",
    "dblink",
    "glob",
    "sniff_",
    "query",
    "getenv",
    "current_setting",
    "set_config",
    "system",
    "load_file",
    "copy",
)

MASK_TEMPLATES: dict[str, dict[str, str]] = {
    "redact": {
        "postgres": "'****'",
        "starrocks": "'****'",
        "duckdb": "'****'",
    },
    "null": {
        "postgres": "CAST(NULL AS TEXT)",
        "starrocks": "CAST(NULL AS VARCHAR)",
        "duckdb": "CAST(NULL AS VARCHAR)",
    },
    "hash": {
        "postgres": "encode(sha256(convert_to(CAST(c AS TEXT), 'UTF8')), 'hex')",
        "starrocks": "sha2(CAST(c AS VARCHAR), 256)",
        "duckdb": "sha256(CAST(c AS VARCHAR))",
    },
    "last4": {
        "postgres": "'****' || right(CAST(c AS TEXT), 4)",
        "starrocks": "concat('****', right(CAST(c AS VARCHAR), 4))",
        "duckdb": "'****' || right(CAST(c AS VARCHAR), 4)",
    },
    "year": {
        "postgres": "CAST(date_trunc('year', c) AS DATE)",
        "starrocks": "CAST(date_trunc('year', c) AS DATE)",
        "duckdb": "CAST(date_trunc('year', c) AS DATE)",
    },
}


class EnforcementError(ValueError):
    """Raised when query enforcement rejects a statement."""


@dataclass(frozen=True)
class RowFilter:
    """A row-level filter predicate.

    `op` is one of `eq`, `in`, `gte`, `lte`.
    `eq`, `gte` and `lte` take exactly one value; `in` takes one or more.
    Values are `str`, `int`, `float`, `bool` or `None` (`None` only with `eq`, meaning `IS NULL`).
    """

    column: str
    op: str
    values: tuple[Any, ...]

    def __post_init__(self) -> None:
        op_lower = self.op.lower()
        if op_lower not in VALID_OPS:
            raise EnforcementError(f"unknown row filter op '{self.op}' for column '{self.column}'")

        if not isinstance(self.values, tuple):
            object.__setattr__(self, "values", tuple(self.values))

        if op_lower in ("eq", "gte", "lte"):
            if len(self.values) != 1:
                raise EnforcementError(
                    f"row filter op '{self.op}' on column '{self.column}' requires exactly 1 value, got {len(self.values)}"
                )
        elif op_lower == "in":
            if len(self.values) < 1:
                raise EnforcementError(
                    f"row filter op 'in' on column '{self.column}' requires at least 1 value, got 0"
                )

        for val in self.values:
            if val is None:
                if op_lower != "eq":
                    raise EnforcementError(
                        f"None value only permitted with 'eq' op, got '{self.op}' for column '{self.column}'"
                    )
            elif not isinstance(val, (str, int, float, bool)):
                raise EnforcementError(
                    f"invalid value type '{type(val).__name__}' in row filter for column '{self.column}'"
                )


@dataclass(frozen=True)
class TableGrant:
    """Grant specification for a single table."""

    table: str
    columns: tuple[str, ...]
    masks: Mapping[str, str]
    row_filters: tuple[RowFilter, ...] = ()
    actions: tuple[str, ...] = ("select",)

    def __post_init__(self) -> None:
        if not isinstance(self.columns, tuple):
            object.__setattr__(self, "columns", tuple(self.columns))
        if not isinstance(self.row_filters, tuple):
            rf_list = []
            for rf in self.row_filters:
                if isinstance(rf, dict):
                    rf_list.append(RowFilter(column=rf["column"], op=rf["op"], values=tuple(rf["values"])))
                else:
                    rf_list.append(rf)
            object.__setattr__(self, "row_filters", tuple(rf_list))
        if not isinstance(self.actions, tuple):
            object.__setattr__(self, "actions", tuple(self.actions))

        # Validate masks
        for col, method in self.masks.items():
            if method.lower() not in VALID_MASKS:
                raise EnforcementError(
                    f"unknown mask method '{method}' for column '{col}' in table '{self.table}'"
                )

        # Validate overlap between visible columns and masks
        visible_set = {c.lower() for c in self.columns}
        mask_set = {c.lower() for c in self.masks.keys()}
        overlap = visible_set & mask_set
        if overlap:
            col_name = next(iter(overlap))
            raise EnforcementError(
                f"column '{col_name}' is both visible and masked in table '{self.table}'"
            )

        # Validate actions
        for act in self.actions:
            if act.lower() not in VALID_ACTIONS:
                raise EnforcementError(
                    f"unknown action '{act}' in table '{self.table}'"
                )


@dataclass(frozen=True)
class Grant:
    """Decoded access grant issued by Catalog."""

    subject: str
    workspace: str
    audit_id: str
    sql_sha256: str
    max_rows: int
    expires_at: float
    tables: Mapping[str, TableGrant]

    def __post_init__(self) -> None:
        if self.max_rows <= 0:
            raise EnforcementError(f"max_rows must be positive, got {self.max_rows}")
        if not self.tables:
            raise EnforcementError("empty table list in grant")

    @classmethod
    def from_claims(cls, claims: Mapping[str, Any]) -> Grant:
        required_keys = ("sub", "ws", "jti", "sql_sha256", "max_rows", "exp", "tables")
        for key in required_keys:
            if key not in claims:
                raise EnforcementError(f"missing required claim '{key}' in grant claims")

        max_rows = claims["max_rows"]
        if not isinstance(max_rows, int) or max_rows <= 0:
            raise EnforcementError(f"max_rows must be positive integer, got {max_rows}")

        tables_raw = claims["tables"]
        if not tables_raw:
            raise EnforcementError("empty table list in grant claims")

        table_grants: dict[str, TableGrant] = {}
        for tbl_name, tbl_spec in tables_raw.items():
            cols = tuple(tbl_spec.get("columns", ()))
            masks = dict(tbl_spec.get("masks", {}))
            rf_raw = tbl_spec.get("row_filters", ())
            row_filters = tuple(
                RowFilter(column=rf["column"], op=rf["op"], values=tuple(rf["values"]))
                for rf in rf_raw
            )
            actions = tuple(tbl_spec.get("actions", ("select",)))
            table_grants[tbl_name] = TableGrant(
                table=tbl_name,
                columns=cols,
                masks=masks,
                row_filters=row_filters,
                actions=actions,
            )

        return cls(
            subject=claims["sub"],
            workspace=claims["ws"],
            audit_id=claims["jti"],
            sql_sha256=claims["sql_sha256"],
            max_rows=max_rows,
            expires_at=float(claims["exp"]),
            tables=table_grants,
        )


@dataclass(frozen=True)
class EnforcedQuery:
    """The result of enforcing policy on a SQL query."""

    statement: exp.Expression
    sql: str
    tables: tuple[str, ...]
    masked_columns: tuple[str, ...]
    row_limit: int | None
    redacted_sql: str
    cube_security_context: Mapping[str, Any] | None


def _build_row_filter_node(rf: RowFilter) -> exp.Expression:
    """Construct an AST predicate node for a RowFilter using sqlglot literals."""
    op = rf.op.lower()
    col = exp.to_column(rf.column)
    if op == "eq":
        val = rf.values[0]
        if val is None:
            return exp.Is(this=col, expression=exp.null())
        return exp.EQ(this=col, expression=exp.convert(val))
    if op == "in":
        return exp.In(this=col, expressions=[exp.convert(v) for v in rf.values])
    if op == "gte":
        return exp.GTE(this=col, expression=exp.convert(rf.values[0]))
    if op == "lte":
        return exp.LTE(this=col, expression=exp.convert(rf.values[0]))
    raise EnforcementError(f"unsupported row filter op: {rf.op}")


def _build_mask_expression(method: str, col_name: str, target_dialect: str) -> exp.Expression:
    """Build a mask expression for a column and method in the target dialect."""
    method_lower = method.lower()
    if method_lower not in MASK_TEMPLATES:
        raise EnforcementError(f"unknown mask method '{method}' for column '{col_name}'")
    dialect_templates = MASK_TEMPLATES[method_lower]
    if target_dialect not in dialect_templates:
        raise EnforcementError(f"unsupported target dialect '{target_dialect}'")

    template = dialect_templates[target_dialect]
    parsed = sqlglot.parse_one(template, read=target_dialect)
    for col_node in parsed.find_all(exp.Column):
        if col_node.name.lower() == "c":
            col_node.replace(exp.to_column(col_name))
    return parsed


def _table_full_name(table: exp.Table) -> str:
    """Return the dotted name of an exp.Table node."""
    if table.parts:
        return ".".join(part.name for part in table.parts)
    return table.name


def _get_tables_in_scope_chain(col: exp.Column, statement: exp.Expression, all_tables: set[str]) -> list[set[str]]:
    """Return list of table key sets in scope for a column, closest scope first."""
    scopes: list[set[str]] = []
    curr = col.parent
    while curr is not None:
        if isinstance(curr, exp.Select):
            tables: set[str] = set()
            f = curr.args.get("from")
            if f and isinstance(f.this, exp.Table):
                tables.add(_table_full_name(f.this).lower())
            for j in curr.args.get("joins") or []:
                if isinstance(j.this, exp.Table):
                    tables.add(_table_full_name(j.this).lower())
            if tables:
                scopes.append(tables)
        curr = curr.parent

    if isinstance(statement, (exp.Update, exp.Delete)):
        if isinstance(statement.this, exp.Table):
            scopes.append({_table_full_name(statement.this).lower()})

    if not scopes:
        scopes.append(set(all_tables))
    return scopes


def _find_statement_defined_column_names(statement: exp.Expression) -> set[str]:
    """Collect column and alias names introduced by CTEs, subqueries, and projections."""
    names: set[str] = set()

    for s in statement.find_all(exp.Select):
        for e in s.expressions:
            if isinstance(e, exp.Alias):
                names.add(e.alias.lower())

    for cte in statement.find_all(exp.CTE):
        table_alias = cte.args.get("alias")
        if table_alias and hasattr(table_alias, "columns") and table_alias.columns:
            for col in table_alias.columns:
                names.add(col.name.lower())
        if isinstance(cte.this, exp.Select):
            for e in cte.this.expressions:
                if isinstance(e, exp.Alias):
                    names.add(e.alias.lower())
                elif isinstance(e, exp.Column) and not isinstance(e.this, exp.Star):
                    names.add(e.name.lower())

    for sub in statement.find_all(exp.Subquery):
        if isinstance(sub.this, exp.Select):
            for e in sub.this.expressions:
                if isinstance(e, exp.Alias):
                    names.add(e.alias.lower())
                elif isinstance(e, exp.Column) and not isinstance(e.this, exp.Star):
                    names.add(e.name.lower())

    for val in statement.find_all(exp.Values):
        table_alias = val.args.get("alias")
        if table_alias and hasattr(table_alias, "columns") and table_alias.columns:
            for col in table_alias.columns:
                names.add(col.name.lower())

    return names


def _expand_stars(
    statement: exp.Expression,
    grant_tables_ci: Mapping[str, tuple[str, TableGrant]],
    schemas_ci: Mapping[str, tuple[str, list[str]]],
    cte_names: set[str],
) -> None:
    """Expand '*' and 't.*' projections only when they refer to granted tables."""
    selects = list(statement.find_all(exp.Select))
    # Process bottom-up (innermost selects first)
    for s in reversed(selects):
        from_clause = s.args.get("from")
        joins = s.args.get("joins") or []

        # Collect direct sources in this select
        sources: list[exp.Expression] = []
        if from_clause and from_clause.this:
            sources.append(from_clause.this)
        for j in joins:
            if j.this:
                sources.append(j.this)

        new_expressions: list[exp.Expression] = []
        for expr in s.expressions:
            if isinstance(expr, exp.Star):
                if not sources:
                    new_expressions.append(expr)
                    continue
                for src in sources:
                    is_granted_table = False
                    if isinstance(src, exp.Table):
                        tbl_key = _table_full_name(src).lower()
                        if tbl_key in grant_tables_ci and src.name.lower() not in cte_names:
                            is_granted_table = True
                    if is_granted_table:
                        tbl_key = _table_full_name(src).lower()
                        _, tg = grant_tables_ci[tbl_key]
                        sch_cols = schemas_ci[tbl_key][1]
                        granted_set = {c.lower() for c in tg.columns} | {m.lower() for m in tg.masks}
                        granted_in_order = [c for c in sch_cols if c.lower() in granted_set]
                        qualifier = src.alias or (src.name if len(sources) > 1 else None)
                        for c in granted_in_order:
                            new_expressions.append(exp.column(c, table=qualifier))
                    else:
                        # Non-table source (CTE, subquery, VALUES): leave as * or alias.*
                        if len(sources) > 1 and getattr(src, "alias", None):
                            new_expressions.append(exp.column(exp.Star(), table=src.alias))
                        else:
                            new_expressions.append(exp.Star())
            elif isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                qual = expr.table.lower() if expr.table else ""
                matched_src = None
                for src in sources:
                    alias = getattr(src, "alias", None) or getattr(src, "name", None)
                    if alias and alias.lower() == qual:
                        matched_src = src
                        break
                    if isinstance(src, exp.Table) and _table_full_name(src).lower() == qual:
                        matched_src = src
                        break
                is_granted_table = False
                if matched_src and isinstance(matched_src, exp.Table):
                    tbl_key = _table_full_name(matched_src).lower()
                    if tbl_key in grant_tables_ci and matched_src.name.lower() not in cte_names:
                        is_granted_table = True
                if is_granted_table:
                    tbl_key = _table_full_name(matched_src).lower()
                    _, tg = grant_tables_ci[tbl_key]
                    sch_cols = schemas_ci[tbl_key][1]
                    granted_set = {c.lower() for c in tg.columns} | {m.lower() for m in tg.masks}
                    granted_in_order = [c for c in sch_cols if c.lower() in granted_set]
                    for c in granted_in_order:
                        new_expressions.append(exp.column(c, table=expr.table))
                else:
                    # Non-table source (CTE, subquery, VALUES): leave as alias.*
                    new_expressions.append(expr)
            else:
                new_expressions.append(expr)

        s.set("expressions", new_expressions)


def enforce(
    sql: str,
    grant: Grant,
    *,
    schemas: Mapping[str, Sequence[str]],
    target_dialect: str,
    target_kind: str,
    dialect: str = "postgres",
    now: float | None = None,
) -> EnforcedQuery:
    """Enforce a data catalog grant on a SQL query."""
    # Step 1: Integrity
    computed_sha256 = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    if computed_sha256.lower() != grant.sql_sha256.lower():
        raise EnforcementError(
            f"SQL SHA256 mismatch: expected {grant.sql_sha256}, got {computed_sha256}"
        )

    check_now = time.time() if now is None else float(now)
    if check_now >= grant.expires_at:
        raise EnforcementError(
            f"grant expired at {grant.expires_at}, current time is {check_now}"
        )

    try:
        parsed_statements = sqlglot.parse(sql, read=dialect)
    except Exception as exc:
        raise EnforcementError(f"SQL parse error: {exc}") from exc

    if len(parsed_statements) != 1 or parsed_statements[0] is None:
        raise EnforcementError(
            f"expected exactly one statement, got {len(parsed_statements)}"
        )

    statement = parsed_statements[0]
    original_statement = statement.copy()

    # Target kind & dialect validation
    target_kind_lower = target_kind.lower()
    if target_kind_lower not in VALID_TARGET_KINDS:
        raise EnforcementError(f"unknown target_kind '{target_kind}'")

    target_dialect_lower = target_dialect.lower()
    if target_dialect_lower not in VALID_TARGET_DIALECTS:
        raise EnforcementError(f"unsupported target dialect '{target_dialect}'")

    # Validate function calls anywhere in the statement
    allowlist = ANONYMOUS_FUNCTION_ALLOWLIST.get(target_kind_lower, set())
    for f in statement.find_all(exp.Func):
        if isinstance(f, exp.Anonymous):
            fn_name = f.name
            if fn_name.lower() not in allowlist:
                raise EnforcementError(f"function '{fn_name}' is not permitted")
        else:
            sql_name = f.sql_name().lower()
            if isinstance(f, exp.ReadCSV) or sql_name.startswith(DISALLOWED_FUNCTION_PREFIXES):
                raise EnforcementError(f"function '{sql_name}' is not permitted")

    # Case-insensitive maps for grant.tables and schemas
    grant_tables_ci: dict[str, tuple[str, TableGrant]] = {
        k.lower(): (k, v) for k, v in grant.tables.items()
    }
    schemas_ci: dict[str, tuple[str, list[str]]] = {
        k.lower(): (k, list(v)) for k, v in schemas.items()
    }

    # CTE names defined by the statement itself
    cte_names: set[str] = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}

    # Step 2: Tables
    referenced_table_nodes: list[exp.Table] = []
    referenced_table_keys: set[str] = set()

    for tbl in statement.find_all(exp.Table):
        tbl_key = _table_full_name(tbl).lower()
        if tbl_key in cte_names:
            continue
        if tbl_key not in grant_tables_ci:
            raise EnforcementError(f"table '{_table_full_name(tbl)}' is not granted")
        if tbl_key not in schemas_ci:
            raise EnforcementError(f"table '{_table_full_name(tbl)}' is not in schemas")
        referenced_table_nodes.append(tbl)
        referenced_table_keys.add(tbl_key)

    # Step 3: Action
    target_table_ref: exp.Table | None = None
    if isinstance(statement, exp.Query):
        for tbl_key in referenced_table_keys:
            canonical_name, tg = grant_tables_ci[tbl_key]
            actions_set = {a.lower() for a in tg.actions}
            if "select" not in actions_set:
                raise EnforcementError(
                    f"action 'select' is not granted on table '{canonical_name}'"
                )
    elif isinstance(statement, exp.Insert):
        target = statement.this.this if isinstance(statement.this, exp.Schema) else statement.this
        if not isinstance(target, exp.Table):
            raise EnforcementError("INSERT target must be a table")
        target_table_ref = target
        target_key = _table_full_name(target).lower()
        if target_key not in grant_tables_ci:
            raise EnforcementError(f"table '{_table_full_name(target)}' is not granted")
        target_canonical, target_grant = grant_tables_ci[target_key]
        if "insert" not in {a.lower() for a in target_grant.actions}:
            raise EnforcementError(
                f"action 'insert' is not granted on table '{target_canonical}'"
            )
        for tbl_key in referenced_table_keys:
            if tbl_key == target_key:
                continue
            canonical_name, tg = grant_tables_ci[tbl_key]
            if "select" not in {a.lower() for a in tg.actions}:
                raise EnforcementError(
                    f"action 'select' is not granted on table '{canonical_name}'"
                )
    elif isinstance(statement, exp.Update):
        target = statement.this
        if not isinstance(target, exp.Table):
            raise EnforcementError("UPDATE target must be a table")
        target_table_ref = target
        target_key = _table_full_name(target).lower()
        if target_key not in grant_tables_ci:
            raise EnforcementError(f"table '{_table_full_name(target)}' is not granted")
        target_canonical, target_grant = grant_tables_ci[target_key]
        if "update" not in {a.lower() for a in target_grant.actions}:
            raise EnforcementError(
                f"action 'update' is not granted on table '{target_canonical}'"
            )
        for tbl_key in referenced_table_keys:
            if tbl_key == target_key:
                continue
            canonical_name, tg = grant_tables_ci[tbl_key]
            if "select" not in {a.lower() for a in tg.actions}:
                raise EnforcementError(
                    f"action 'select' is not granted on table '{canonical_name}'"
                )
    elif isinstance(statement, exp.Delete):
        target = statement.this
        if not isinstance(target, exp.Table):
            raise EnforcementError("DELETE target must be a table")
        target_table_ref = target
        target_key = _table_full_name(target).lower()
        if target_key not in grant_tables_ci:
            raise EnforcementError(f"table '{_table_full_name(target)}' is not granted")
        target_canonical, target_grant = grant_tables_ci[target_key]
        if "delete" not in {a.lower() for a in target_grant.actions}:
            raise EnforcementError(
                f"action 'delete' is not granted on table '{target_canonical}'"
            )
        for tbl_key in referenced_table_keys:
            if tbl_key == target_key:
                continue
            canonical_name, tg = grant_tables_ci[tbl_key]
            if "select" not in {a.lower() for a in tg.actions}:
                raise EnforcementError(
                    f"action 'select' is not granted on table '{canonical_name}'"
                )
    else:
        raise EnforcementError(
            f"statement kind '{type(statement).__name__}' is not permitted"
        )

    # Step 6 write validations (INSERT and UPDATE checks)
    if isinstance(statement, exp.Insert):
        target_key = _table_full_name(target_table_ref).lower()
        target_canonical, target_grant = grant_tables_ci[target_key]
        if target_grant.row_filters:
            raise EnforcementError(
                f"INSERT into table '{target_canonical}' with row filters is not permitted"
            )
        if isinstance(statement.this, exp.Schema):
            for col_ident in statement.this.expressions:
                c_name = col_ident.name.lower()
                if c_name in {m.lower() for m in target_grant.masks}:
                    raise EnforcementError(
                        f"INSERT naming masked column '{col_ident.name}' on table '{target_canonical}' is not permitted"
                    )
                if c_name not in {c.lower() for c in target_grant.columns}:
                    raise EnforcementError(
                        f"column '{col_ident.name}' is not granted for INSERT on table '{target_canonical}'"
                    )
        else:
            if target_grant.masks:
                raise EnforcementError(
                    f"INSERT into table '{target_canonical}' with masked columns is not permitted without explicit column list"
                )
            if set(c.lower() for c in target_grant.columns) != set(c.lower() for c in schemas_ci[target_key][1]):
                raise EnforcementError(
                    f"INSERT into table '{target_canonical}' with ungranted schema columns is not permitted without explicit column list"
                )

    if isinstance(statement, exp.Update):
        target_key = _table_full_name(target_table_ref).lower()
        target_canonical, target_grant = grant_tables_ci[target_key]
        for eq in statement.expressions:
            if isinstance(eq, exp.EQ):
                col_name = eq.this.name.lower()
                if col_name in {m.lower() for m in target_grant.masks}:
                    raise EnforcementError(
                        f"UPDATE cannot set masked column '{eq.this.name}' on table '{target_canonical}'"
                    )
                if col_name not in {c.lower() for c in target_grant.columns}:
                    raise EnforcementError(
                        f"UPDATE cannot set column '{eq.this.name}' outside granted columns on table '{target_canonical}'"
                    )

    # Step 4: Columns
    # First expand '*' and 't.*'
    _expand_stars(statement, grant_tables_ci, schemas_ci, cte_names)

    # Collect statement-defined names (CTEs, subqueries, values, projection aliases)
    statement_defined_names = _find_statement_defined_column_names(statement)
    subquery_aliases = {
        sub.alias.lower() for sub in statement.find_all(exp.Subquery) if sub.alias
    }
    values_aliases = {
        val.alias.lower() for val in statement.find_all(exp.Values) if val.alias
    }

    # Validate every column reference
    for col in statement.find_all(exp.Column):
        if isinstance(col.this, exp.Star):
            continue
        # Skip columns in INSERT schema declaration and UPDATE SET targets (already validated)
        if isinstance(statement, exp.Insert) and col.parent is statement.this:
            continue
        if isinstance(statement, exp.Update) and any(col is eq.this for eq in statement.expressions if isinstance(eq, exp.EQ)):
            continue

        col_name = col.name.lower()
        if col.table:
            # Qualified column reference
            qual = col.table.lower()
            full_qual = f"{col.db.lower()}.{qual}" if col.db else qual

            if qual in cte_names or qual in subquery_aliases or qual in values_aliases:
                # Allowed if defined or produced by CTE/subquery/values
                continue

            # Must match a referenced table or table alias
            matched_key: str | None = None
            for tbl in referenced_table_nodes:
                if tbl.alias and tbl.alias.lower() == qual:
                    matched_key = _table_full_name(tbl).lower()
                    break
                if not tbl.alias:
                    tbl_full = _table_full_name(tbl).lower()
                    if tbl.name.lower() == qual or tbl_full == full_qual or tbl_full == qual:
                        matched_key = tbl_full
                        break

            if matched_key is None:
                raise EnforcementError(
                    f"unknown table or alias qualifier '{col.table}' for column '{col.name}'"
                )

            canonical_tbl, table_grant = grant_tables_ci[matched_key]
            schema_cols = {c.lower(): c for c in schemas_ci[matched_key][1]}
            if col_name in schema_cols:
                granted_cols = {c.lower() for c in table_grant.columns} | {m.lower() for m in table_grant.masks}
                if col_name not in granted_cols:
                    raise EnforcementError(
                        f"column '{col.name}' in table '{canonical_tbl}' is neither visible nor masked"
                    )
            else:
                raise EnforcementError(
                    f"column '{col.name}' not found in table '{canonical_tbl}'"
                )
        else:
            # Unqualified column reference: resolve against schemas of tables in scope
            scopes = _get_tables_in_scope_chain(col, statement, referenced_table_keys or set(grant_tables_ci.keys()))
            found = False
            for scope_tables in scopes:
                matching_tables: list[str] = []
                for tbl_key in scope_tables:
                    if tbl_key in schemas_ci:
                        schema_cols_set = {c.lower() for c in schemas_ci[tbl_key][1]}
                        if col_name in schema_cols_set:
                            matching_tables.append(tbl_key)
                if len(matching_tables) > 1:
                    matched_names = [grant_tables_ci[k][0] for k in matching_tables]
                    raise EnforcementError(
                        f"ambiguous column reference '{col.name}' matches multiple tables: {matched_names}"
                    )
                if len(matching_tables) == 1:
                    tbl_key = matching_tables[0]
                    canonical_tbl, table_grant = grant_tables_ci[tbl_key]
                    granted_cols = {c.lower() for c in table_grant.columns} | {m.lower() for m in table_grant.masks}
                    if col_name not in granted_cols:
                        raise EnforcementError(
                            f"column '{col.name}' in table '{canonical_tbl}' is neither visible nor masked"
                        )
                    found = True
                    break

            if not found:
                # Not in any referenced schema in scope: check statement-defined names
                if col_name in statement_defined_names:
                    continue
                raise EnforcementError(
                    f"column '{col.name}' does not resolve to any granted column"
                )

    # Step 5: Secure-view rewrite (all kinds except cube)
    if target_kind_lower != "cube":
        for tbl in list(statement.find_all(exp.Table)):
            if tbl is target_table_ref:
                continue
            tbl_key = _table_full_name(tbl).lower()
            if tbl_key in cte_names:
                continue
            if tbl_key not in grant_tables_ci:
                continue

            canonical_tbl, table_grant = grant_tables_ci[tbl_key]
            schema_cols = schemas_ci[tbl_key][1]

            visible_cols_map = {c.lower(): c for c in table_grant.columns}
            masks_cols_map = {c.lower(): (c, m) for c, m in table_grant.masks.items()}

            inner_projections: list[exp.Expression] = []
            for col_name in schema_cols:
                col_lower = col_name.lower()
                if col_lower in visible_cols_map:
                    inner_projections.append(exp.to_column(col_name))
                elif col_lower in masks_cols_map:
                    orig_name, mask_method = masks_cols_map[col_lower]
                    mask_expr = _build_mask_expression(mask_method, orig_name, target_dialect_lower)
                    inner_projections.append(exp.alias_(mask_expr, orig_name))

            sub = exp.select(*inner_projections).from_(exp.to_table(canonical_tbl))
            if table_grant.row_filters:
                filter_nodes = [_build_row_filter_node(rf) for rf in table_grant.row_filters]
                sub = sub.where(exp.and_(*filter_nodes))

            derived_alias = tbl.alias or tbl.name
            derived_table = sub.subquery(alias=derived_alias)
            tbl.replace(derived_table)

        # Clear catalog/db qualifications on outer columns matching unaliased tables
        for col in statement.find_all(exp.Column):
            if col.db:
                col.set("catalog", None)
                col.set("db", None)

    # Step 6: Writes (AND-in row filters into WHERE clause for UPDATE/DELETE)
    if isinstance(statement, (exp.Update, exp.Delete)):
        target_key = _table_full_name(target_table_ref).lower()
        _, target_grant = grant_tables_ci[target_key]
        if target_grant.row_filters:
            filter_nodes = [_build_row_filter_node(rf) for rf in target_grant.row_filters]
            target_where = exp.and_(*filter_nodes)
            existing_where = statement.args.get("where")
            if existing_where is None:
                statement.set("where", exp.Where(this=target_where))
            else:
                existing_where.set("this", exp.and_(existing_where.this, target_where))

    # Step 7: Row cap
    row_limit: int | None = None
    if isinstance(statement, exp.Query):
        existing_limit = statement.args.get("limit")
        existing_val: int | None = None
        if existing_limit is not None and isinstance(existing_limit.expression, exp.Literal):
            try:
                existing_val = int(existing_limit.expression.this)
            except (ValueError, TypeError):
                existing_val = None
        cap = grant.max_rows + 1
        new_limit = min(existing_val, cap) if existing_val is not None else cap
        statement.set("limit", exp.Limit(expression=exp.Literal.number(new_limit)))
        row_limit = new_limit

    # Step 8: Cube security context
    cube_security_context: dict[str, Any] | None = None
    if target_kind_lower == "cube":
        cube_masks = {
            grant_tables_ci[t][0]: dict(grant_tables_ci[t][1].masks)
            for t in referenced_table_keys
            if grant_tables_ci[t][1].masks
        }
        cube_filters = {
            grant_tables_ci[t][0]: [
                {"column": rf.column, "op": rf.op, "values": list(rf.values)}
                for rf in grant_tables_ci[t][1].row_filters
            ]
            for t in referenced_table_keys
            if grant_tables_ci[t][1].row_filters
        }
        cube_security_context = {
            "subject": grant.subject,
            "masks": cube_masks,
            "row_filters": cube_filters,
        }

    # Step 11: redacted_sql
    redacted_stmt = original_statement.copy()
    for node in list(redacted_stmt.find_all((exp.Literal, exp.Boolean))):
        node.replace(exp.Placeholder())
    redacted_sql = redacted_stmt.sql(dialect=dialect)

    # Masked columns for referenced tables, sorted
    masked_cols_list: list[str] = []
    for tbl_key in referenced_table_keys:
        canonical_tbl, tg = grant_tables_ci[tbl_key]
        for col_name in tg.masks:
            masked_cols_list.append(f"{canonical_tbl}.{col_name}")
    masked_columns = tuple(sorted(masked_cols_list))

    # Referenced tables in canonical form, sorted
    canonical_tables = tuple(sorted({grant_tables_ci[k][0] for k in referenced_table_keys}))

    # Target SQL rendered in target_dialect
    rendered_sql = statement.sql(dialect=target_dialect_lower)

    return EnforcedQuery(
        statement=statement,
        sql=rendered_sql,
        tables=canonical_tables,
        masked_columns=masked_columns,
        row_limit=row_limit,
        redacted_sql=redacted_sql,
        cube_security_context=cube_security_context,
    )

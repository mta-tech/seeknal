"""Governed structured select compiler for POST /v1/select."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlglot import exp

from seeknal.serving.enforce import EnforcementError, Grant
from seeknal.serving.keyset import decode_cursor
from seeknal.serving.parameter_binding import (
    MAX_SAFE_INTEGER,
    MAX_STRING_UTF8_BYTES,
    MIN_SAFE_INTEGER,
)
from seeknal.serving.placement import Placement

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_PREFIX_LIKE_RE = re.compile(r"^([^%_\\]|\\[%_\\])+%$")

MAX_WHERE_DEPTH = 5
MAX_WHERE_NODES = 64
DEFAULT_MAX_IN_LIST = 1000

ALLOWED_SELECT_KEYS: frozenset[str] = frozenset(
    {
        "table",
        "columns",
        "where",
        "order_by",
        "limit",
        "after",
        "hint",
        "max_staleness_seconds",
        "timeout_s",
        "max_result_bytes",
    }
)

VALID_COMBINATORS: frozenset[str] = frozenset({"_and", "_or", "_not"})
VALID_COLUMN_OPS: frozenset[str] = frozenset(
    {
        "_eq",
        "_neq",
        "_in",
        "_nin",
        "_gt",
        "_gte",
        "_lt",
        "_lte",
        "_like",
        "_is_null",
    }
)
VALID_SORT_DIRECTIONS: frozenset[str] = frozenset({"asc", "desc"})


class SelectInvalidError(EnforcementError):
    """Raised when a structured POST /v1/select request payload is invalid."""

    code: str = "select_invalid"

    def __init__(
        self,
        message: str = "Invalid select request",
        *,
        code: str | None = None,
    ) -> None:
        super().__init__(message, code=code or "select_invalid")


@dataclass(frozen=True)
class CompiledSelect:
    """Validated and compiled structured select query."""

    table: str
    columns: tuple[str, ...]
    projected_columns: tuple[str, ...]
    extra_cursor_columns: tuple[str, ...]
    statement: exp.Select
    sql: str
    params: tuple[Any, ...]
    order_spec: tuple[tuple[str, str], ...]
    limit: int | None
    paged: bool
    hint: str | None = None
    max_staleness_seconds: float | None = None
    timeout_s: float | None = None
    max_result_bytes: int | None = None


@dataclass(frozen=True)
class _ValidatedColPred:
    column: str
    op: str
    value: Any


@dataclass(frozen=True)
class _ValidatedLogicPred:
    op: str  # "_and" | "_or" | "_not"
    children: tuple[Any, ...]


def _validate_select_scalar(val: Any) -> bool | int | str:
    """Validate a non-null scalar under seeknal-params-v1 rules without echoing values."""
    if val is None:
        raise SelectInvalidError("null value is not permitted; use _is_null")
    if isinstance(val, bool):
        return val
    if isinstance(val, float):
        raise SelectInvalidError("floating-point parameter values are not permitted")
    if type(val) is int:
        if val < MIN_SAFE_INTEGER or val > MAX_SAFE_INTEGER:
            raise SelectInvalidError("integer parameter is outside safe integer range")
        return 0 if val == 0 else int(val)
    if type(val) is str:
        try:
            utf8_bytes = val.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise SelectInvalidError(
                "string parameter contains invalid UTF-8 or unpaired surrogate"
            ) from exc
        if len(utf8_bytes) > MAX_STRING_UTF8_BYTES:
            raise SelectInvalidError("string parameter exceeds maximum UTF-8 length")
        return val
    raise SelectInvalidError(f"unsupported scalar type '{type(val).__name__}'")


class _WhereValidator:
    """Validates a Hasura-style where tree before any SQLGlot AST node is built."""

    def __init__(
        self,
        schema_cols_ci: Mapping[str, str],
        *,
        max_in_list: int,
    ) -> None:
        self.schema_cols_ci = schema_cols_ci
        self.max_in_list = max_in_list
        self.node_count = 0
        self.referenced_columns: list[str] = []

    def _bump_nodes(self) -> None:
        self.node_count += 1
        if self.node_count > MAX_WHERE_NODES:
            raise SelectInvalidError(
                f"where clause exceeds maximum of {MAX_WHERE_NODES} nodes"
            )

    def validate(self, cond: Any, *, depth: int = 1) -> Any:
        if depth > MAX_WHERE_DEPTH:
            raise SelectInvalidError(
                f"where clause exceeds maximum depth of {MAX_WHERE_DEPTH}"
            )
        if not isinstance(cond, dict) or not cond:
            raise SelectInvalidError("where condition must be a non-empty JSON object")

        conjuncts: list[Any] = []
        for raw_key, raw_sub in cond.items():
            if type(raw_key) is not str or not raw_key:
                raise SelectInvalidError("where condition key must be a non-empty string")

            if raw_key in ("_and", "_or"):
                self._bump_nodes()
                if not isinstance(raw_sub, list) or len(raw_sub) == 0:
                    raise SelectInvalidError(
                        f"'{raw_key}' must be a non-empty list of condition objects"
                    )
                children = tuple(
                    self.validate(child, depth=depth + 1) for child in raw_sub
                )
                conjuncts.append(_ValidatedLogicPred(op=raw_key, children=children))
            elif raw_key == "_not":
                self._bump_nodes()
                if not isinstance(raw_sub, dict) or not raw_sub:
                    raise SelectInvalidError(
                        "'_not' must map to a non-empty condition object"
                    )
                child = self.validate(raw_sub, depth=depth + 1)
                conjuncts.append(_ValidatedLogicPred(op="_not", children=(child,)))
            elif raw_key.startswith("_") and raw_key in VALID_COLUMN_OPS:
                raise SelectInvalidError(
                    f"column operator '{raw_key}' must appear inside a column object"
                )
            else:
                if not _IDENT_RE.fullmatch(raw_key):
                    raise SelectInvalidError("invalid column identifier in where clause")
                col_lower = raw_key.lower()
                if col_lower not in self.schema_cols_ci:
                    raise SelectInvalidError("unknown column in where clause")
                canon_col = self.schema_cols_ci[col_lower]
                self.referenced_columns.append(canon_col)

                if not isinstance(raw_sub, dict) or not raw_sub:
                    raise SelectInvalidError(
                        "column filter in where clause must be a non-empty operator object"
                    )

                for op_key, op_val in raw_sub.items():
                    if type(op_key) is not str or op_key not in VALID_COLUMN_OPS:
                        raise SelectInvalidError("unknown operator in where clause")
                    self._bump_nodes()
                    validated_val = self._validate_op_value(op_key, op_val)
                    conjuncts.append(
                        _ValidatedColPred(
                            column=canon_col,
                            op=op_key,
                            value=validated_val,
                        )
                    )

        if len(conjuncts) == 1:
            return conjuncts[0]
        return _ValidatedLogicPred(op="_and", children=tuple(conjuncts))

    def _validate_op_value(self, op_key: str, op_val: Any) -> Any:
        if op_key == "_is_null":
            if not isinstance(op_val, bool):
                raise SelectInvalidError("'_is_null' requires a boolean value")
            return op_val

        if op_key in ("_in", "_nin"):
            if not isinstance(op_val, list) or len(op_val) == 0:
                raise SelectInvalidError(f"'{op_key}' requires a non-empty list")
            if self.max_in_list > 0 and len(op_val) > self.max_in_list:
                raise SelectInvalidError(
                    f"'{op_key}' list exceeds maximum length of {self.max_in_list}"
                )
            return tuple(_validate_select_scalar(item) for item in op_val)

        if op_key == "_like":
            scalar = _validate_select_scalar(op_val)
            if type(scalar) is not str or not _PREFIX_LIKE_RE.fullmatch(scalar):
                raise SelectInvalidError(
                    "'_like' requires a non-empty prefix pattern ending with '%'"
                )
            return scalar

        return _validate_select_scalar(op_val)


def _build_col_node(canonical_col: str) -> exp.Column:
    return exp.Column(this=exp.Identifier(this=canonical_col, quoted=False))


def _build_table_node(canonical_table: str) -> exp.Table:
    parts = canonical_table.split(".")
    if len(parts) == 3:
        return exp.Table(
            this=exp.Identifier(this=parts[2], quoted=False),
            db=exp.Identifier(this=parts[1], quoted=False),
            catalog=exp.Identifier(this=parts[0], quoted=False),
        )
    if len(parts) == 2:
        return exp.Table(
            this=exp.Identifier(this=parts[1], quoted=False),
            db=exp.Identifier(this=parts[0], quoted=False),
        )
    return exp.Table(this=exp.Identifier(this=parts[0], quoted=False))


def _flatten_and_nodes(expr: exp.Expression) -> list[exp.Expression]:
    if isinstance(expr, exp.And):
        return _flatten_and_nodes(expr.this) + _flatten_and_nodes(expr.expression)
    return [expr]


def _left_and(exprs: Sequence[exp.Expression]) -> exp.Expression:
    flat: list[exp.Expression] = []
    for expr in exprs:
        for item in _flatten_and_nodes(expr):
            flat.append(exp.Paren(this=item) if isinstance(item, exp.Or) else item)
    acc = flat[0]
    for nxt in flat[1:]:
        acc = exp.And(this=acc, expression=nxt)
    return acc


def _build_where_ast(
    node: _ValidatedColPred | _ValidatedLogicPred,
    params_out: list[Any],
) -> exp.Expression:
    def _next_param(val: Any) -> exp.Parameter:
        params_out.append(val)
        return exp.Parameter(this=exp.var(str(len(params_out))))

    if isinstance(node, _ValidatedColPred):
        col = _build_col_node(node.column)
        op = node.op
        if op == "_eq":
            return exp.EQ(this=col, expression=_next_param(node.value))
        if op == "_neq":
            return exp.NEQ(this=col, expression=_next_param(node.value))
        if op == "_gt":
            return exp.GT(this=col, expression=_next_param(node.value))
        if op == "_gte":
            return exp.GTE(this=col, expression=_next_param(node.value))
        if op == "_lt":
            return exp.LT(this=col, expression=_next_param(node.value))
        if op == "_lte":
            return exp.LTE(this=col, expression=_next_param(node.value))
        if op == "_like":
            return exp.Like(this=col, expression=_next_param(node.value))
        if op == "_in":
            return exp.In(
                this=col,
                expressions=[_next_param(v) for v in node.value],
            )
        if op == "_nin":
            return exp.Not(
                this=exp.In(
                    this=col,
                    expressions=[_next_param(v) for v in node.value],
                )
            )
        if op == "_is_null":
            is_null_expr = exp.Is(this=col, expression=exp.Null())
            return is_null_expr if node.value else exp.Not(this=is_null_expr)
        raise SelectInvalidError("unsupported operator")

    if node.op == "_and":
        built = [_build_where_ast(ch, params_out) for ch in node.children]
        if len(built) == 1:
            return built[0]
        return _left_and(built)

    if node.op == "_or":
        built = []
        for ch in node.children:
            sub_expr = _build_where_ast(ch, params_out)
            if isinstance(sub_expr, exp.And):
                sub_expr = exp.Paren(this=sub_expr)
            built.append(sub_expr)
        if len(built) == 1:
            return built[0]
        acc = built[0]
        for nxt in built[1:]:
            acc = exp.Or(this=acc, expression=nxt)
        return acc

    if node.op == "_not":
        inner = _build_where_ast(node.children[0], params_out)
        if isinstance(inner, (exp.And, exp.Or, exp.Not)):
            return exp.Not(this=exp.Paren(this=inner))
        return exp.Not(this=inner)

    raise SelectInvalidError("unsupported logical combinator")


def compile_select_request(
    payload: Mapping[str, Any],
    *,
    placement: Placement,
    schemas: Mapping[str, Sequence[str]],
    default_catalog: str | None = None,
    grant: Grant | None = None,
    cursor_key: bytes | str | None = None,
    include_cursor_columns: bool = False,
) -> CompiledSelect:
    """Validate and compile a structured select JSON payload into a parameterized AST."""
    if not isinstance(payload, Mapping) or isinstance(
        payload, (str, bytes, bytearray)
    ):
        raise SelectInvalidError("select payload must be a JSON object")

    for k in payload.keys():
        if type(k) is not str or k not in ALLOWED_SELECT_KEYS:
            raise SelectInvalidError("unknown field in select request")

    # 1. Validate 'table' before any AST node is constructed
    if "table" not in payload or type(payload["table"]) is not str or not payload["table"]:
        raise SelectInvalidError("'table' must be a non-empty string")
    raw_table = payload["table"]
    segments = raw_table.split(".")
    if not (1 <= len(segments) <= 3) or not all(
        _IDENT_RE.fullmatch(seg) for seg in segments
    ):
        raise SelectInvalidError("invalid table identifier")

    if len(segments) == 2 and default_catalog is not None:
        if type(default_catalog) is not str or not _IDENT_RE.fullmatch(default_catalog):
            raise SelectInvalidError("invalid default catalog identifier")
        resolved_table_name = f"{default_catalog}.{raw_table}"
    else:
        resolved_table_name = raw_table

    table_entry = placement.find_table(resolved_table_name)
    if table_entry is None:
        raise SelectInvalidError("unknown table")

    canonical_table = table_entry.name
    canonical_parts = canonical_table.split(".")
    if not (1 <= len(canonical_parts) <= 3) or not all(
        _IDENT_RE.fullmatch(part) for part in canonical_parts
    ):
        raise SelectInvalidError("canonical table name is not a valid identifier")

    schemas_ci: dict[str, tuple[str, tuple[str, ...]]] = {
        str(k).lower(): (str(k), tuple(str(c) for c in cols))
        for k, cols in schemas.items()
    }
    if canonical_table.lower() not in schemas_ci:
        raise SelectInvalidError("table schema not found")

    _, schema_cols = schemas_ci[canonical_table.lower()]
    schema_cols_ci: dict[str, str] = {}
    for col_name in schema_cols:
        if _IDENT_RE.fullmatch(col_name):
            schema_cols_ci[col_name.lower()] = col_name

    # 2. Validate 'columns'
    if "columns" not in payload or not isinstance(payload["columns"], list) or len(payload["columns"]) == 0:
        raise SelectInvalidError("'columns' must be a non-empty list of column names")

    requested_columns: list[str] = []
    seen_req_cols_lower: set[str] = set()
    for raw_col in payload["columns"]:
        if type(raw_col) is not str or not _IDENT_RE.fullmatch(raw_col):
            raise SelectInvalidError("invalid column identifier in 'columns'")
        col_lower = raw_col.lower()
        if col_lower not in schema_cols_ci:
            raise SelectInvalidError("unknown column in 'columns'")
        if col_lower in seen_req_cols_lower:
            raise SelectInvalidError("duplicate column in 'columns'")
        seen_req_cols_lower.add(col_lower)
        requested_columns.append(schema_cols_ci[col_lower])

    # 3. Validate 'where' (optional)
    policy_max_in = getattr(placement.policy, "max_in_list", DEFAULT_MAX_IN_LIST)
    max_in_list = (
        int(policy_max_in)
        if isinstance(policy_max_in, int) and not isinstance(policy_max_in, bool)
        else DEFAULT_MAX_IN_LIST
    )

    validated_where: _ValidatedColPred | _ValidatedLogicPred | None = None
    where_columns: list[str] = []
    if "where" in payload:
        where_validator = _WhereValidator(schema_cols_ci, max_in_list=max_in_list)
        validated_where = where_validator.validate(payload["where"], depth=1)
        where_columns = where_validator.referenced_columns

    # 4. Validate 'order_by' (optional)
    user_order_spec: list[tuple[str, str]] = []
    seen_order_cols_lower: set[str] = set()
    order_direction: str | None = None
    if "order_by" in payload:
        raw_order_by = payload["order_by"]
        if not isinstance(raw_order_by, list):
            raise SelectInvalidError("'order_by' must be a list of single-key objects")
        for item in raw_order_by:
            if not isinstance(item, dict) or len(item) != 1:
                raise SelectInvalidError(
                    "each 'order_by' entry must be a single-key object"
                )
            (raw_ocol, raw_odir), = item.items()
            if type(raw_ocol) is not str or not _IDENT_RE.fullmatch(raw_ocol):
                raise SelectInvalidError("invalid column identifier in 'order_by'")
            ocol_lower = raw_ocol.lower()
            if ocol_lower not in schema_cols_ci:
                raise SelectInvalidError("unknown column in 'order_by'")
            if ocol_lower in seen_order_cols_lower:
                raise SelectInvalidError("duplicate column in 'order_by'")

            if type(raw_odir) is not str or raw_odir.lower() not in VALID_SORT_DIRECTIONS:
                raise SelectInvalidError("sort direction must be 'asc' or 'desc'")
            odir_lower = raw_odir.lower()
            if order_direction is None:
                order_direction = odir_lower
            elif odir_lower != order_direction:
                raise SelectInvalidError(
                    "all 'order_by' columns must share a single sort direction"
                )

            seen_order_cols_lower.add(ocol_lower)
            user_order_spec.append((schema_cols_ci[ocol_lower], odir_lower))

    # 5. Validate 'limit', 'after', and execution controls
    limit_val: int | None = None
    if "limit" in payload:
        raw_limit = payload["limit"]
        if (
            isinstance(raw_limit, bool)
            or type(raw_limit) is not int
            or raw_limit <= 0
            or raw_limit > MAX_SAFE_INTEGER
        ):
            raise SelectInvalidError("'limit' must be a positive safe integer")
        limit_val = raw_limit

    after_token: str | None = None
    if "after" in payload:
        raw_after = payload["after"]
        if type(raw_after) is not str or not raw_after:
            raise SelectInvalidError("'after' must be a non-empty cursor string")
        after_token = raw_after

    hint_val: str | None = None
    if "hint" in payload and payload["hint"] is not None:
        if type(payload["hint"]) is not str or not payload["hint"]:
            raise SelectInvalidError("'hint' must be a non-empty string")
        hint_val = payload["hint"]

    max_staleness_val: float | None = None
    if "max_staleness_seconds" in payload and payload["max_staleness_seconds"] is not None:
        raw_ms = payload["max_staleness_seconds"]
        if isinstance(raw_ms, bool) or not isinstance(raw_ms, (int, float)):
            raise SelectInvalidError("'max_staleness_seconds' must be a finite number")
        if math.isnan(raw_ms) or math.isinf(raw_ms):
            raise SelectInvalidError("'max_staleness_seconds' must be a finite number")
        max_staleness_val = float(raw_ms)

    timeout_val: float | None = None
    if "timeout_s" in payload and payload["timeout_s"] is not None:
        raw_ts = payload["timeout_s"]
        if isinstance(raw_ts, bool) or not isinstance(raw_ts, (int, float)):
            raise SelectInvalidError("'timeout_s' must be a positive finite number")
        if math.isnan(raw_ts) or math.isinf(raw_ts) or raw_ts <= 0:
            raise SelectInvalidError("'timeout_s' must be a positive finite number")
        timeout_val = float(raw_ts)

    max_result_bytes_val: int | None = None
    if "max_result_bytes" in payload and payload["max_result_bytes"] is not None:
        raw_mrb = payload["max_result_bytes"]
        if isinstance(raw_mrb, bool) or type(raw_mrb) is not int or raw_mrb <= 0:
            raise SelectInvalidError("'max_result_bytes' must be a positive integer")
        max_result_bytes_val = raw_mrb

    # 6. Check grant column permissions (masked/ungranted columns forbidden in where/order_by)
    visible_grant_cols_lower: set[str] | None = None
    if grant is not None:
        grant_tables_ci = {k.lower(): v for k, v in grant.tables.items()}
        table_grant = grant_tables_ci.get(canonical_table.lower())
        if table_grant is not None:
            visible_grant_cols_lower = {c.lower() for c in table_grant.columns}
            masked_grant_cols_lower = {m.lower() for m in table_grant.masks}
            granted_all_lower = visible_grant_cols_lower | masked_grant_cols_lower

            for col in where_columns:
                c_low = col.lower()
                if c_low in masked_grant_cols_lower or c_low not in visible_grant_cols_lower:
                    raise EnforcementError(
                        f"Masked or ungranted column '{col}' cannot be used in where or order_by",
                        code="access_denied",
                    )

            for col, _ in user_order_spec:
                c_low = col.lower()
                if c_low in masked_grant_cols_lower or c_low not in visible_grant_cols_lower:
                    raise EnforcementError(
                        f"Masked or ungranted column '{col}' cannot be used in where or order_by",
                        code="access_denied",
                    )

            for col in requested_columns:
                if col.lower() not in granted_all_lower:
                    raise EnforcementError(
                        f"column '{col}' in table '{canonical_table}' is neither visible nor masked",
                        code="access_denied",
                    )

    # 7. Append visible primary keys to effective_order_spec
    effective_direction = order_direction if order_direction is not None else "asc"
    effective_order_list = list(user_order_spec)
    for pk_col in table_entry.keys:
        pk_low = pk_col.lower()
        if pk_low not in schema_cols_ci or pk_low in seen_order_cols_lower:
            continue
        if visible_grant_cols_lower is not None and pk_low not in visible_grant_cols_lower:
            continue
        seen_order_cols_lower.add(pk_low)
        effective_order_list.append((schema_cols_ci[pk_low], effective_direction))

    effective_order_spec = tuple(effective_order_list)

    # 8. Decode keyset cursor if supplied (before building any AST node)
    cursor_values: tuple[Any, ...] | None = None
    if after_token is not None:
        cursor_values = decode_cursor(
            after_token,
            expected_table=canonical_table,
            expected_order_spec=effective_order_spec,
            secret_key=cursor_key,
        )

    # 9. Determine projected columns (optionally appending cursor sort columns)
    extra_cursor_cols: list[str] = []
    if include_cursor_columns and effective_order_spec:
        for ocol, _ in effective_order_spec:
            if ocol.lower() not in seen_req_cols_lower:
                seen_req_cols_lower.add(ocol.lower())
                extra_cursor_cols.append(ocol)

    projected_columns = tuple(requested_columns + extra_cursor_cols)

    # 10. Construct parameterized SQLGlot AST solely from allowlisted identifiers
    params_list: list[Any] = []
    where_expr: exp.Expression | None = None
    if validated_where is not None:
        where_expr = _build_where_ast(validated_where, params_list)

    if cursor_values is not None:
        def _next_cursor_param(val: Any) -> exp.Parameter:
            params_list.append(val)
            return exp.Parameter(this=exp.var(str(len(params_list))))

        if len(effective_order_spec) == 1:
            col_name, direction = effective_order_spec[0]
            lhs_single = _build_col_node(col_name)
            rhs_single = _next_cursor_param(cursor_values[0])
            keyset_expr: exp.Expression = (
                exp.GT(this=lhs_single, expression=rhs_single)
                if direction == "asc"
                else exp.LT(this=lhs_single, expression=rhs_single)
            )
        else:
            direction = effective_order_spec[0][1]
            lhs_tuple = exp.Tuple(
                expressions=[_build_col_node(c) for c, _ in effective_order_spec]
            )
            rhs_tuple = exp.Tuple(
                expressions=[_next_cursor_param(v) for v in cursor_values]
            )
            keyset_expr = (
                exp.GT(this=lhs_tuple, expression=rhs_tuple)
                if direction == "asc"
                else exp.LT(this=lhs_tuple, expression=rhs_tuple)
            )

        if where_expr is not None:
            where_expr = _left_and([where_expr, keyset_expr])
        else:
            where_expr = keyset_expr

    select_stmt = exp.select(
        *[_build_col_node(c) for c in projected_columns]
    ).from_(_build_table_node(canonical_table))

    if where_expr is not None:
        select_stmt = select_stmt.where(where_expr)

    if effective_order_spec:
        order_nodes = [
            exp.Ordered(
                this=_build_col_node(col_name),
                desc=(direction == "desc"),
                nulls_first=None,
            )
            for col_name, direction in effective_order_spec
        ]
        select_stmt = select_stmt.order_by(*order_nodes)

    rendered_postgres_sql = select_stmt.sql(dialect="postgres")

    return CompiledSelect(
        table=canonical_table,
        columns=tuple(requested_columns),
        projected_columns=projected_columns,
        extra_cursor_columns=tuple(extra_cursor_cols),
        statement=select_stmt,
        sql=rendered_postgres_sql,
        params=tuple(params_list),
        order_spec=effective_order_spec,
        limit=limit_val,
        paged=after_token is not None,
        hint=hint_val,
        max_staleness_seconds=max_staleness_val,
        timeout_s=timeout_val,
        max_result_bytes=max_result_bytes_val,
    )


__all__ = [
    "ALLOWED_SELECT_KEYS",
    "CompiledSelect",
    "DEFAULT_MAX_IN_LIST",
    "MAX_WHERE_DEPTH",
    "MAX_WHERE_NODES",
    "SelectInvalidError",
    "VALID_COLUMN_OPS",
    "VALID_COMBINATORS",
    "compile_select_request",
]

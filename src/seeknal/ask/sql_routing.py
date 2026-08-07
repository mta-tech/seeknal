"""Decide whether a SQL statement can run on a remote source instead of DuckDB.

Seeknal executes agent SQL through DuckDB, which may hold local views (parquet,
Iceberg) alongside ``ATTACH``-ed remote databases. DuckDB pushes down predicates
but not aggregation, so an aggregate over an attached table transfers the raw
columns and computes locally. When a statement touches exactly one remote source
and nothing local, sending it to that source is dramatically cheaper.

"Can we send it" is a structural question about the statement, so it is answered
from a parsed AST rather than by matching text. Two properties matter and text
matching gets both wrong:

- **What the statement references.** A name may be a remote table, a local view,
  or a CTE the statement defines for itself. Only the parser knows which.
- **What the statement means in the target dialect.** Engines differ on details
  such as NULL ordering — PostgreSQL sorts NULLs first under ``ORDER BY x DESC``
  while DuckDB sorts them last. Transpiling makes those defaults explicit so the
  remote engine returns what the local one would have.

Both come from ``sqlglot``, which Seeknal already depends on.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

import sqlglot
from sqlglot import exp

# Dialect of the SQL agents write. DuckDB is Seeknal's local execution engine,
# so its semantics are what a routed statement must reproduce.
LOCAL_DIALECT = "duckdb"


@dataclass(frozen=True)
class RoutingPlan:
    """Outcome of analysing one statement.

    ``namespace`` is None when the statement must stay on the local engine;
    ``reason`` then says why, so callers can record it instead of guessing.
    """

    namespace: Optional[str]
    sql: str
    reason: str

    @property
    def is_routable(self) -> bool:
        return self.namespace is not None


def _declined(reason: str) -> RoutingPlan:
    return RoutingPlan(namespace=None, sql="", reason=reason)


def _self_defined_names(statement: exp.Expression) -> set[str]:
    """Names the statement introduces itself, e.g. CTEs.

    A reference to one of these is resolved by the engine running the statement,
    not by a catalog, so it must not be mistaken for a local table.
    """
    return {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}


def _has_unnamed_projection(statement: exp.Expression) -> bool:
    """Whether any projection would be named by the engine rather than the query.

    An engine is free to invent a name for a projection the statement does not
    alias, and they disagree: ``SELECT COUNT(*)`` yields ``count_star()`` on
    DuckDB and ``count`` on PostgreSQL. Worse, PostgreSQL names by function so
    ``SELECT COUNT(*), COUNT(DISTINCT a)`` returns two columns both called
    ``count``. Values match either way, but headers are read by whatever
    consumes the result. Aliased projections and plain column references carry
    their own name and are safe.
    """
    select = statement.find(exp.Select)
    if select is None:
        return False
    return any(
        not isinstance(projection, (exp.Alias, exp.Column, exp.Star))
        for projection in select.expressions
    )


def probe_sql(sql: str, *, local_dialect: str = LOCAL_DIALECT) -> Optional[str]:
    """Render ``sql`` so the local engine reports its result columns cheaply.

    ``LIMIT 0`` makes the engine plan the statement and describe its output
    without reading rows. The limit is set on the parsed statement rather than
    appended as text, so a statement that already carries a ``LIMIT`` is
    rewritten instead of producing ``LIMIT 5 LIMIT 0``.
    """
    try:
        statement = sqlglot.parse_one(sql, dialect=local_dialect)
    except Exception:  # noqa: BLE001
        return None
    if statement is None:
        return None
    return statement.limit(0).sql(dialect=local_dialect)


def _apply_local_names(
    statement: exp.Expression,
    local_names: Sequence[str],
    max_identifier_bytes: Optional[int],
) -> bool:
    """Alias unnamed projections with the names the local engine gives them.

    Returns False when the names cannot be reproduced faithfully — a different
    number of projections, or a name the target cannot hold. Callers treat that
    as "do not route": a truncated header is the very difference this is meant
    to remove.
    """
    select = statement.find(exp.Select)
    if select is None or len(select.expressions) != len(local_names):
        return False

    for index, projection in enumerate(select.expressions):
        if isinstance(projection, (exp.Alias, exp.Column, exp.Star)):
            continue
        name = local_names[index]
        if max_identifier_bytes is not None and len(name.encode()) > max_identifier_bytes:
            return False
        select.expressions[index] = exp.alias_(
            projection.copy(), exp.Identifier(this=name, quoted=True)
        )
    return True


def _namespace_qualifier(table: exp.Table, candidates: set[str]) -> Optional[str]:
    """Return the namespace this table reference is qualified with, if any.

    A reference may name the namespace in either position depending on how many
    parts it has: ``ns.schema.table`` puts it in the catalog slot, ``ns.table``
    in the schema slot. The second form is only a namespace if it matches a
    configured one — otherwise it is an ordinary schema and the reference says
    nothing about which engine owns the table.
    """
    if table.catalog:
        return table.catalog
    if table.db and table.db in candidates:
        return table.db
    return None


def _clear_namespace_qualifier(table: exp.Table, namespace: str) -> None:
    """Drop the namespace from a reference; the remote engine has no such name."""
    if table.catalog == namespace:
        table.set("catalog", None)
    elif table.db == namespace:
        table.set("db", None)


def analyze(
    sql: str,
    candidate_namespaces: Iterable[str],
    *,
    target_dialect: str,
    local_dialect: str = LOCAL_DIALECT,
    local_column_names: Optional[Sequence[str]] = None,
    max_identifier_bytes: Optional[int] = None,
) -> RoutingPlan:
    """Return how (or whether) ``sql`` can be executed on a remote namespace.

    Routable only when every table reference is qualified with the *same*
    namespace and that namespace is one of ``candidate_namespaces``. Any
    unqualified reference could be a local view, so it forces local execution.

    The returned ``sql`` has the namespace qualifier removed — the remote engine
    knows nothing about it — and is rendered in ``target_dialect`` so dialect
    defaults survive the move.

    Projections the query does not name are declined, because the engines would
    name them differently. Pass ``local_column_names`` (see :func:`probe_sql`)
    to alias them with the local engine's own names instead, and
    ``max_identifier_bytes`` to decline rather than let the target truncate a
    name it cannot hold.
    """
    candidates = {str(ns) for ns in candidate_namespaces}
    if not candidates:
        return _declined("no remote namespace configured")

    try:
        statement = sqlglot.parse_one(sql, dialect=local_dialect)
    except Exception as exc:  # noqa: BLE001 — unparsable means "not our call"
        return _declined(f"unparsable: {type(exc).__name__}")
    if statement is None:
        return _declined("empty statement")

    local_names = _self_defined_names(statement)
    namespaces: set[str] = set()
    for table in statement.find_all(exp.Table):
        qualifier = _namespace_qualifier(table, candidates)
        if qualifier is not None:
            namespaces.add(qualifier)
        elif table.name.lower() not in local_names:
            return _declined(f"unqualified table reference: {table.name}")

    if not namespaces:
        return _declined("no qualified table reference")
    if len(namespaces) > 1:
        return _declined("spans multiple namespaces")
    (namespace,) = namespaces
    if namespace not in candidates:
        return _declined(f"namespace not routable: {namespace}")

    rewritten = statement.copy()
    if _has_unnamed_projection(rewritten):
        if local_column_names is None:
            return _declined("unaliased projection: local column names not supplied")
        if not _apply_local_names(rewritten, local_column_names, max_identifier_bytes):
            return _declined("unaliased projection: local names not reproducible")

    for table in rewritten.find_all(exp.Table):
        _clear_namespace_qualifier(table, namespace)

    try:
        target_sql = rewritten.sql(dialect=target_dialect)
    except Exception as exc:  # noqa: BLE001 — cannot express it there
        return _declined(f"not expressible in {target_dialect}: {type(exc).__name__}")

    return RoutingPlan(namespace=namespace, sql=target_sql, reason="")

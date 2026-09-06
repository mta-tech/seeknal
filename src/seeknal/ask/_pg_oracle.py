"""Direct psycopg2 oracle path for PG-only SQL — issue #64.

This module is intentionally narrow:

- It is imported only by ``seeknal.ask.testing.execute_expected_sql`` for
  the oracle/test path.
- It does NOT change agent execution. Agents continue to run through
  the DuckDB postgres_scanner. The rewrite at
  ``_rewrite_for_pg_pushdown`` makes that path work for EXTRACT(...)
  filters.

Behavior:

- ``detect_pg_only_namespace`` looks at every qualified table reference
  in the SQL and returns the single connected-PG namespace it touches.
  Any unqualified ``FROM`` / ``JOIN`` table reference forces a ``None``
  return so the caller falls back to DuckDB. Any mix of PG namespaces or
  any non-PG namespace also returns ``None``.
- ``resolve_pg_dsn`` builds a libpq URL string for ``psycopg2.connect``
  from (1) the env var named by ``source.dsn_env``, falling back to
  (2) ``connections[source.name]`` and (3) ``connections[source.namespace]``
  in the project profile.
- ``strip_namespace`` rewrites bounded ``<namespace>.<schema>.<table>``
  and ``<namespace>.<table>`` references to PG-native form.
- ``execute_via_psycopg2`` opens a read-only connection via
  ``contextlib.closing`` (NOT ``with psycopg2.connect(...)`` which
  manages the *transaction*), sets ``autocommit=True``, and calls
  ``conn.set_session(readonly=True)`` BEFORE creating a cursor. It
  records a distinct ``"execute_sql_pg_direct"`` timing event so
  observability can tell the engines apart.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import time
from typing import Any, Optional

import psycopg2

from seeknal.ask.agents.tools._context import record_timing_event
from seeknal.ask.testing import SqlOracleResult
from seeknal.connections.postgresql import (
    parse_postgresql_config,
    parse_postgresql_url,
)
from seeknal.sources.config import SourceConfig, SourceRegistry


def detect_pg_only_namespace(
    sql: str,
    registry: SourceRegistry,
) -> Optional[str]:
    """Return the single connected-PG namespace this SQL touches, or None.

    The SQL qualifies for the psycopg2 path only when:
    - every qualified table ref points at the SAME connected-PG namespace,
    - no unqualified ``FROM`` / ``JOIN`` table ref appears.

    A connected-PG source is one where ``source_kind == "connected"``,
    ``source_type == "database"``, ``connector in {"postgresql", "postgres"}``,
    and the source is read-only.

    The structural decision is delegated to :mod:`seeknal.ask.sql_routing`,
    which parses the statement instead of matching text — a name can be a
    remote table, a local view, or a CTE the statement defines, and only a
    parser can tell those apart.
    """
    from seeknal.ask.sql_routing import analyze

    pg_ns_set: set[str] = set()
    for source in registry.sources.values():
        if (
            source.source_kind == "connected"
            and source.source_type == "database"
            and source.connector in ("postgresql", "postgres")
            and source.is_read_only
        ):
            pg_ns_set.add(source.namespace)

    return analyze(sql, pg_ns_set, target_dialect="postgres").namespace


def resolve_pg_dsn(
    source: SourceConfig,
    profile_data: dict,
) -> str:
    """Resolve a libpq URL for ``psycopg2.connect``.

    Order:
    1. ``os.environ[source.dsn_env]`` if set+non-empty — use as-is (URL).
    2. Else ``profile_data["connections"][source.name]`` — build URL.
    3. Else ``profile_data["connections"][source.namespace]`` — build URL.
       (name wins over namespace when both exist.)
    4. Else raise ``RuntimeError`` mentioning ``source.dsn_env`` with an
       ``export ...`` hint.
    """
    if source.dsn_env:
        env_value = os.environ.get(source.dsn_env, "").strip()
        if env_value:
            # Validate format by parsing; we still return the URL string for
            # psycopg2.connect to consume directly.
            _ = parse_postgresql_url(env_value)
            return env_value

    connections = profile_data.get("connections") or {}
    raw_config = (
        connections.get(source.name)
        or connections.get(source.namespace)
    )
    if raw_config:
        pg_config = parse_postgresql_config(raw_config)
        # Build a libpq URL string from the parsed config.
        from urllib.parse import quote

        user = quote(pg_config.user, safe="")
        password = quote(pg_config.password, safe="") if pg_config.password else ""
        userinfo = f"{user}:{password}" if password else user
        return (
            f"postgresql://{userinfo}@{pg_config.host}:{pg_config.port}/"
            f"{pg_config.database}?sslmode={pg_config.sslmode}"
            f"&connect_timeout={pg_config.connect_timeout}"
        )

    env_name = source.dsn_env or f"<dsn_env not set for source '{source.name}'>"
    raise RuntimeError(
        f"PostgreSQL DSN unavailable for source '{source.name}'. "
        f"Set {env_name} or add a profile block under "
        f"connections.{source.name}. Example: "
        f"export {env_name}=postgresql://user:pass@host:5432/dbname"
    )


def strip_namespace(sql: str, namespace: str) -> str:
    """Rewrite ``<namespace>.<schema>.<table>`` → ``<schema>.<table>``
    and ``<namespace>.<table>`` → ``<table>`` (relying on PG search_path).

    Only the given namespace is stripped; other dotted references are left
    intact. ``\\b`` boundaries on both sides prevent partial matches.

    EDGE CASE: when ``namespace`` equals a real PG schema name (e.g.
    ``"public"``), ``strip_namespace("public.t", "public")`` → ``"t"`` —
    still valid via search_path.
    """
    # Strip 3-part first (so we don't accidentally turn a 3-part hit into
    # an over-stripped 2-part hit afterwards).
    pattern_3 = re.compile(rf'\b{re.escape(namespace)}\.')
    return pattern_3.sub("", sql)


def execute_via_psycopg2(dsn: str, sql: str) -> SqlOracleResult:
    """Open a read-only psycopg2 connection, run the SELECT, return rows.

    Lifecycle (issue #64 Architect A5 fix):
        with contextlib.closing(psycopg2.connect(dsn)) as conn:
            conn.autocommit = True
            conn.set_session(readonly=True)   # BEFORE cursor()
            with conn.cursor() as cur:
                cur.execute(sql)
                ...

    Rationale: ``with psycopg2.connect(...)`` manages the transaction,
    not the connection. We need ``contextlib.closing(...)`` for
    connection close on exception. ``autocommit=True`` avoids implicit
    transaction state for a single SELECT. ``set_session(readonly=True)``
    is set BEFORE any cursor is created.
    """
    stripped = (sql or "").lstrip()
    # Trim any leading SQL comments before the first token.
    while True:
        if stripped.startswith("--"):
            newline = stripped.find("\n")
            if newline < 0:
                stripped = ""
                break
            stripped = stripped[newline + 1 :].lstrip()
        elif stripped.startswith("/*"):
            end = stripped.find("*/")
            if end < 0:
                stripped = ""
                break
            stripped = stripped[end + 2 :].lstrip()
        else:
            break

    first_token = stripped.split(None, 1)[0].upper() if stripped else ""
    if first_token not in ("SELECT", "WITH"):
        return SqlOracleResult(
            error="Oracle psycopg2 path is SELECT-only (use SELECT or WITH)."
        )

    started = time.monotonic()
    try:
        with contextlib.closing(psycopg2.connect(dsn)) as conn:
            conn.autocommit = True
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute(sql)
                description = cur.description or []
                columns = [str(col[0]) for col in description]
                fetched = cur.fetchall() if description else []
        rows = [[_jsonable(cell) for cell in row] for row in fetched]
        return SqlOracleResult(columns=columns, rows=rows)
    except Exception as exc:  # noqa: BLE001 — surface psycopg2 errors
        return SqlOracleResult(error=str(exc))
    finally:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        try:
            record_timing_event("execute_sql_pg_direct", elapsed_ms)
        except Exception:  # noqa: BLE001 — observability never blocks
            pass


def _jsonable(value: Any) -> Any:
    """Minimal _jsonable mirror that doesn't import seeknal.ask.testing.

    Kept in sync with ``seeknal.ask.testing._jsonable`` (issue #64 AC-D4
    asserts byte-identical SqlOracleResult between psycopg2 and DuckDB
    paths).
    """
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


__all__ = [
    "detect_pg_only_namespace",
    "resolve_pg_dsn",
    "strip_namespace",
    "execute_via_psycopg2",
]

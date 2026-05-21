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


# Regexes for namespace detection (used in detect_pg_only_namespace).
# Quoted variants like "ns"."schema"."table" are also recognized.
_IDENT = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)'
_RE_3PART = re.compile(rf'\b({_IDENT})\.({_IDENT})\.({_IDENT})\b')
_RE_2PART = re.compile(rf'\b({_IDENT})\.({_IDENT})\b')
_RE_UNQUALIFIED = re.compile(
    rf'\b(?:FROM|JOIN)\s+({_IDENT})\b(?!\s*\.)',
    flags=re.IGNORECASE,
)


def _strip_ident_quotes(token: str) -> str:
    if token.startswith('"') and token.endswith('"'):
        return token[1:-1]
    return token


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
    """
    pg_ns_set: set[str] = set()
    for source in registry.sources.values():
        if (
            source.source_kind == "connected"
            and source.source_type == "database"
            and source.connector in ("postgresql", "postgres")
            and source.is_read_only
        ):
            pg_ns_set.add(source.namespace)

    if not pg_ns_set:
        return None

    # Collect leading-identifier set from qualified refs.
    # 3-part scan FIRST; replace those spans with whitespace so a follow-up
    # 2-part scan does not double-count the inner ``schema.table`` portion.
    leading: set[str] = set()
    masked = list(sql)
    for m in _RE_3PART.finditer(sql):
        leading.add(_strip_ident_quotes(m.group(1)))
        for idx in range(m.start(), m.end()):
            masked[idx] = " "
    masked_sql = "".join(masked)
    for m in _RE_2PART.finditer(masked_sql):
        leading.add(_strip_ident_quotes(m.group(1)))

    if not leading:
        return None

    # Any unqualified table ref forces a DuckDB fallback. This prevents
    # false-positive routing on SQL that mixes a qualified PG table and a
    # bare parquet view. Scan the masked SQL so the leading identifier of
    # a 3-part ref is not counted as an unqualified ``FROM`` ident.
    if _RE_UNQUALIFIED.search(masked_sql):
        return None

    # Every leading identifier must be the same single connected-PG namespace.
    if not leading.issubset(pg_ns_set):
        return None
    if len(leading) != 1:
        return None
    (ns,) = leading
    return ns


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

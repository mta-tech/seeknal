"""Execute SQL tool — runs read-only queries via the seeknal REPL.

Guards the LLM's context window by enforcing hard caps on row count,
column count, per-cell length, and total byte size. Whenever any cap
kicks in, an explicit ``Result truncated`` note is appended so the agent
knows to narrow its next query.
"""

from __future__ import annotations

import re
import hashlib

# Hard caps — not overridable from the agent. Tune here if needed.
_ROW_HARD_CAP = 500
_COLUMN_HARD_CAP = 50
_CELL_MAX_LEN = 200
_BYTE_BUDGET = 50 * 1024  # 50 KB of markdown table (roughly ~12k tokens)


def execute_sql(
    sql: str | None = None,
    limit: int = 100,
    query: str | None = None,
    refresh: bool = False,
    allow_sql_pair_drift: bool = False,
) -> str:
    """Execute a read-only SQL query against seeknal project data.

    Use this to query entities, feature groups, and intermediate tables.
    Only SELECT/WITH queries are allowed (read-only). Only query tables shown
    in list_tables output. Never reference file paths in SQL.
    Results are returned as a formatted table.

    Result safety guards (applied silently, flagged explicitly when tripped):
    - Row count is capped at 500 regardless of the ``limit`` argument. If
      the underlying query has more rows, the returned markdown shows the
      first 500 plus a note with the real total.
    - Column count is capped at 50. Extra columns are dropped and a
      note is appended.
    - Each cell value is truncated to 200 chars with an ellipsis.
    - If the assembled markdown exceeds 50 KB, trailing rows are dropped
      with a note. Narrow the query (WHERE / GROUP BY) to recover detail.

    DuckDB SQL dialect notes:
    - Do NOT include trailing semicolons in queries
    - Use CAST('2024-01-01' AS TIMESTAMP) for timestamp literals before INTERVAL arithmetic
    - Use CAST(COUNT(*) AS BIGINT) for aggregation counts
    - Use CAST(SUM(x) AS DOUBLE) for numeric aggregations
    - All non-aggregate SELECT columns must appear in GROUP BY
    - Access struct fields with dot notation: column_name.field_name
    - Use ILIKE for case-insensitive matching
    - In Python code: NEVER put # comments inside SQL strings — DuckDB does
      not recognize # as a comment. Use -- for SQL comments

    Tool errors include a JSON structure with 'category' and 'retryable' fields.
    For retryable errors, adjust your approach based on the 'hint'.
    For terminal errors, explain the limitation to the user.

    Args:
        sql: A DuckDB-compatible SELECT query. This is the preferred argument.
        query: Compatibility alias for ``sql``. Some local tool-call models
            naturally emit ``query=...``; accepting the alias keeps the tool
            thin and deterministic while reducing brittle schema failures.
        limit: Maximum rows to return (default 100). Capped at 500
            regardless of what is passed — use WHERE/GROUP BY to narrow
            beyond that, not a larger limit.
        refresh: Re-run the SQL even if the same normalized query already
            succeeded in this session. Defaults to False so repeated successful
            queries reuse the prior result instead of burning more tool calls.
        allow_sql_pair_drift: Compatibility escape hatch for SQL that differs
            from a SQL pair loaded earlier in the turn. Defaults to False.
            This is ignored after a successful authoritative SQL-pair result
            for an ordinary business question; the harness will stop and
            synthesize an answer instead of allowing query drift.
    """
    from seeknal.ask.agents.tools._context import (
        bump_authoritative_drift_attempt,
        get_authoritative_sql_pair_result,
        get_tool_context,
        get_successful_sql_cache,
        record_tool_result,
        repeated_failure_message,
        should_synthesize_after_authoritative_sql_pair,
        sql_pairs_checked,
    )
    from seeknal.ask.agents.tools.errors import (
        RETRYABLE_SYNTAX,
        TERMINAL_BOUNDED_EVIDENCE,
        format_tool_error,
    )

    ctx = get_tool_context()
    success_cache = get_successful_sql_cache(ctx)

    if sql is None:
        sql = query
    if sql is None or not str(sql).strip():
        return format_tool_error(
            RETRYABLE_SYNTAX,
            "Missing SQL query. Call execute_sql with sql='SELECT ...'.",
            hint=(
                "Use the `sql` argument for execute_sql. The `query` argument "
                "is accepted as a compatibility alias, but one of them must "
                "contain a read-only SELECT/WITH/DESCRIBE/SHOW statement."
            ),
        )

    # Strip trailing semicolons — LLMs often include them but DuckDB rejects them
    sql = str(sql).strip().rstrip(";").strip()
    sql, lint_notices = _repair_common_sql_before_execution(sql)

    if _should_require_sql_pair_lookup(ctx, sql, sql_pairs_checked(ctx)):
        return format_tool_error(
            RETRYABLE_SYNTAX,
            "Project SQL pairs exist and should be checked before ad-hoc SQL.",
            hint=(
                "Call list_sql_pairs(query='<business terms from the user question>') "
                "and execute a direct match with execute_sql_pair. If no relevant "
                "pair is found, then continue with execute_sql."
            ),
        )

    prior_failure = repeated_failure_message("execute_sql", {"sql": sql})
    if prior_failure:
        result = format_tool_error(
            RETRYABLE_SYNTAX,
            prior_failure,
            hint=(
                "Do not retry the same SQL. Use generated source context, "
                "list_tables/describe_table, or answer with caveats from "
                "already collected evidence."
            ),
        )
        record_tool_result("execute_sql", result, args={"sql": sql})
        return result

    cache_key = _sql_cache_key(sql)
    if not refresh and cache_key in success_cache:
        result = (
            "Reusing prior successful SQL result for the same normalized query. "
            "Pass refresh=True only when you intentionally need a fresh database read.\n\n"
            + success_cache[cache_key]
        )
        record_tool_result("execute_sql", result, args={"sql": sql})
        return result

    drift_notice = _sql_pair_drift_notice(ctx, sql)
    if (
        drift_notice
        and get_authoritative_sql_pair_result(ctx) is not None
        and should_synthesize_after_authoritative_sql_pair(ctx)
    ):
        if allow_sql_pair_drift:
            # Agent has explicitly overridden — let the query through with the
            # drift notice attached. The override is a meaningful escape hatch,
            # not a no-op.
            pass
        else:
            attempts = bump_authoritative_drift_attempt(ctx)
            if attempts >= 2:
                # Agent ignored the first RETRYABLE nudge — escalate to
                # TERMINAL so the harness stops looping.
                result = format_tool_error(
                    TERMINAL_BOUNDED_EVIDENCE,
                    "A matching SQL pair already executed successfully for this turn.",
                    hint=(
                        "Stop tool use and answer from the AUTHORITATIVE_RESULT. "
                        "If the user asked for a different filter or grain, pass "
                        "allow_sql_pair_drift=True on execute_sql to override."
                    ),
                )
                record_tool_result("execute_sql", result, args={"sql": sql})
                return result
            result = format_tool_error(
                RETRYABLE_SYNTAX,
                "SQL differs from an authoritative SQL pair already loaded this turn.",
                hint=(
                    "If the user really wants this exact alternate query, retry "
                    "with allow_sql_pair_drift=True. Otherwise answer from the "
                    "AUTHORITATIVE_RESULT. This is the only nudge; a second "
                    "attempt without the override will be terminal."
                ),
            )
            record_tool_result("execute_sql", result, args={"sql": sql})
            return result
    if (
        drift_notice
        and not allow_sql_pair_drift
        and getattr(ctx, "sql_pair_mode", "authoritative") != "advisory"
    ):
        result = format_tool_error(
            RETRYABLE_SYNTAX,
            "SQL differs from the SQL pair loaded earlier this turn.",
            hint=(
                "Use the execute_sql_pair result as authoritative. Only retry "
                "execute_sql with allow_sql_pair_drift=True if the user asked "
                "for a different filter/grain or the SQL pair failed."
            ),
        )
        record_tool_result("execute_sql", result, args={"sql": sql})
        return result

    # SQL validation is handled by the PRE_TOOL_USE hook (see hooks.py)

    # Clamp caller-supplied limit to the hard cap. Fetch one extra row so we
    # can detect "there are more rows beyond this page".
    try:
        asked = max(1, int(limit))
    except (TypeError, ValueError):
        asked = 100
    effective_limit = min(asked, _ROW_HARD_CAP)
    fetch_limit = effective_limit + 1

    try:
        columns, rows = _execute_oneshot_with_timeout(ctx, sql, limit=fetch_limit)
    except Exception as e:
        from seeknal.ask.agents.tools.errors import (
            classify_duckdb_error,
            format_tool_error,
            TERMINAL_TIMEOUT,
        )

        if isinstance(e, TimeoutError):
            result = format_tool_error(TERMINAL_TIMEOUT, str(e))
            record_tool_result("execute_sql", result, args={"sql": sql})
            return result

        repaired_sql = _repair_sql_from_duckdb_suggestion(sql, str(e))
        if repaired_sql and repaired_sql != sql:
            try:
                columns, rows = _execute_oneshot_with_timeout(
                    ctx,
                    repaired_sql,
                    limit=fetch_limit,
                )
                sql = repaired_sql
            except Exception as retry_error:
                result = format_tool_error(
                    TERMINAL_TIMEOUT
                    if isinstance(retry_error, TimeoutError)
                    else classify_duckdb_error(str(retry_error)),
                    str(retry_error),
                    hint=(
                        "A table-name suggestion was attempted but the query "
                        "still failed. Run list_tables and describe_table, then "
                        "retry with a fully qualified table name."
                    ),
                )
                record_tool_result("execute_sql", result, args={"sql": sql})
                return result
        else:
            result = format_tool_error(classify_duckdb_error(str(e)), str(e))
            record_tool_result("execute_sql", result, args={"sql": sql})
            return result

    if not columns:
        result = "Query executed successfully but returned no results."
        record_tool_result("execute_sql", result, args={"sql": sql})
        return result

    has_more_rows = len(rows) > effective_limit
    trimmed_rows = rows[:effective_limit]

    # Resolve the real total only when we actually hit the cap — saves the
    # round-trip for normal-sized results.
    total_rows: int | None = None
    if has_more_rows:
        try:
            total_rows = _count_total(ctx, sql)
        except Exception:
            total_rows = None

    result = _format_table(
        columns=columns,
        rows=trimmed_rows,
        total_rows=total_rows,
        has_more_rows=has_more_rows,
        requested_limit=asked,
    )
    if lint_notices:
        result += "\n\n" + "\n".join(f"ℹ SQL lint: {notice}" for notice in lint_notices)
    if drift_notice:
        result += "\n\n" + drift_notice
    success_cache[cache_key] = result
    record_tool_result("execute_sql", result, args={"sql": sql})
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# PostgreSQL EXTRACT-pushdown rewrite (issue #64)
# ---------------------------------------------------------------------------

# Single shared column-token regex usable for bare ident `col`, table-qualified
# `t.col`, schema-qualified `schema.t.col`, and quoted `"Col"` / `"t"."Col"`.
_PUSHDOWN_COL_PATTERN = (
    r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)'
    r'(?:\.(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)){0,2}'
)

_PUSHDOWN_NOTICE = "rewrote EXTRACT(...) to a pushdown-safe date range"


def _pushdown_mask_literals_and_comments(sql: str) -> tuple[str, list[tuple[str, str]]]:
    """Replace string literals and SQL comments with opaque placeholders.

    EXTRACT() text that appears inside a string literal or comment must never
    be rewritten. Masking those regions out before running the EXTRACT regex
    is the simplest robust approach. Placeholders are restored verbatim once
    the rewrite passes finish.
    """
    replacements: list[tuple[str, str]] = []

    def _store(text: str) -> str:
        token = f"\x00PD{len(replacements)}\x00"
        replacements.append((token, text))
        return token

    # Block comments first (non-greedy) — they can span newlines.
    sql = re.sub(
        r"/\*.*?\*/",
        lambda m: _store(m.group(0)),
        sql,
        flags=re.DOTALL,
    )
    # Then line comments — anything from -- to end-of-line.
    sql = re.sub(
        r"--[^\n]*",
        lambda m: _store(m.group(0)),
        sql,
    )
    # Single-quoted string literals with '' escape.
    sql = re.sub(
        r"'(?:''|[^'])*'",
        lambda m: _store(m.group(0)),
        sql,
    )
    return sql, replacements


def _pushdown_unmask(sql: str, replacements: list[tuple[str, str]]) -> str:
    for token, text in replacements:
        sql = sql.replace(token, text)
    return sql


def _pushdown_format_date(year: int, month: int = 1, day: int = 1) -> str:
    """Format a date literal preserving year boundary 9999 -> 10000."""
    return f"'{year}-{month:02d}-{day:02d}'"


def _pushdown_year_valid(year: int) -> bool:
    return 1 <= year <= 9999


def _rewrite_for_pg_pushdown(sql: str) -> tuple[str, list[str]]:
    """Rewrite EXTRACT(...) date-part filters to pushdown-safe date ranges.

    DuckDB's postgres_scanner does not push EXTRACT(...) past the
    COPY (SELECT ... TO STDOUT) boundary, so the filter runs locally
    after the full table is transferred. This helper rewrites the
    small, deterministic set of patterns documented in the spec to
    half-open date-range form, which postgres_scanner WILL push
    down. The same post-repair SQL is what the cache key and
    drift-detection layers see (see _sql_cache_key, _normalize_sql).

    Implementation notes:
    - 2-part stripping (pg_ns.t -> t) relies on PG search_path
      resolving the table; documented and tested.
    - Invalid date triples (Feb 29 in non-leap year) leave the SQL
      untouched via try/except ValueError — never crash the tool.
    - Boundary year = 9999 produces '10000-01-01' upper bound; this
      is correct ISO behavior and PostgreSQL accepts it.
    - Year = 0 or negative -> leave untouched (out of spec).
    """
    import datetime

    notices: list[str] = []
    masked, replacements = _pushdown_mask_literals_and_comments(sql)
    fired = False

    flags = re.IGNORECASE | re.DOTALL

    def extract_part(part: str) -> str:
        return (
            r"EXTRACT\s*\(\s*"
            + part
            + r"\s+FROM\s+("
            + _PUSHDOWN_COL_PATTERN
            + r")\s*\)"
        )
    # ------------------------------------------------------------------
    # Step 1: coupled-3 (DAY + MONTH + YEAR over same column)
    # ------------------------------------------------------------------
    day_re = extract_part(r"DAY")
    month_re = extract_part(r"MONTH")
    year_re = extract_part(r"YEAR")

    def _store_untouched(text: str) -> str:
        """Reserve a placeholder for matched-but-not-rewritten text.

        Prevents downstream step-2/step-3 patterns from rewriting a
        sub-expression of a triple whose date was invalid (e.g. Feb 29 in
        2023 — the MONTH+YEAR coupling must NOT take over once the triple
        was recognized as the user's intent).
        """
        token = f"\x00PD{len(replacements)}\x00"
        replacements.append((token, text))
        return token

    # Enumerate plausible orderings — DAY/MONTH/YEAR, and the reverse —
    # because regex backtracking across AND-separated triples is awkward.
    triple_orderings = [
        (day_re, month_re, year_re, "d", "m", "y"),
        (year_re, month_re, day_re, "y", "m", "d"),
    ]
    for first_re, second_re, third_re, k1, k2, k3 in triple_orderings:
        pattern = re.compile(
            first_re + r"\s*=\s*(\d+)\s+AND\s+"
            + second_re + r"\s*=\s*(\d+)\s+AND\s+"
            + third_re + r"\s*=\s*(-?\d+)",
            flags=flags,
        )

        def _triple_repl(match: re.Match) -> str:
            nonlocal fired
            col1 = match.group(1)
            val1 = int(match.group(2))
            col2 = match.group(3)
            val2 = int(match.group(4))
            col3 = match.group(5)
            val3 = int(match.group(6))
            if not (col1 == col2 == col3):
                return match.group(0)
            parts = {k1: val1, k2: val2, k3: val3}
            day = parts["d"]
            month = parts["m"]
            year = parts["y"]
            if not _pushdown_year_valid(year):
                return _store_untouched(match.group(0))
            try:
                start = datetime.date(year, month, day)
                end = start + datetime.timedelta(days=1)
            except ValueError:
                return _store_untouched(match.group(0))
            fired = True
            return (
                f"({col1} >= '{start.isoformat()}' "
                f"AND {col1} < '{end.isoformat()}')"
            )

        masked = pattern.sub(_triple_repl, masked)

    # ------------------------------------------------------------------
    # Step 2: coupled-2 (MONTH + YEAR; QUARTER + YEAR, both orderings)
    # ------------------------------------------------------------------
    def _emit_month_year(col: str, month: int, year: int) -> str:
        if month == 12:
            end_year = year + 1
            end_month = 1
        else:
            end_year = year
            end_month = month + 1
        start = _pushdown_format_date(year, month, 1)
        end = _pushdown_format_date(end_year, end_month, 1)
        return f"({col} >= {start} AND {col} < {end})"

    def _emit_quarter_year(col: str, quarter: int, year: int) -> str:
        start_month = 3 * (quarter - 1) + 1
        end_month = start_month + 3
        end_year = year
        if end_month > 12:
            end_month -= 12
            end_year += 1
        start = _pushdown_format_date(year, start_month, 1)
        end = _pushdown_format_date(end_year, end_month, 1)
        return f"({col} >= {start} AND {col} < {end})"

    couple_orderings = [
        # (first_part_regex, second_part_regex, emit_fn, first_is_year)
        (month_re, year_re, _emit_month_year, False),
        (year_re, month_re, _emit_month_year, True),
        (extract_part(r"QUARTER"), year_re, _emit_quarter_year, False),
        (year_re, extract_part(r"QUARTER"), _emit_quarter_year, True),
    ]

    for first_re, second_re, emit_fn, first_is_year in couple_orderings:
        pattern = re.compile(
            first_re + r"\s*=\s*(\d+)\s+AND\s+"
            + second_re + r"\s*=\s*(-?\d+)",
            flags=flags,
        )

        def _make_repl(emit_fn=emit_fn, first_is_year=first_is_year):
            def _couple_repl(match: re.Match) -> str:
                nonlocal fired
                col1 = match.group(1)
                val1 = int(match.group(2))
                col2 = match.group(3)
                val2 = int(match.group(4))
                if col1 != col2:
                    return match.group(0)
                if first_is_year:
                    year = val1
                    other = val2
                else:
                    other = val1
                    year = val2
                if not _pushdown_year_valid(year):
                    return match.group(0)
                fired = True
                return emit_fn(col1, other, year)

            return _couple_repl

        masked = pattern.sub(_make_repl(), masked)

    # ------------------------------------------------------------------
    # Step 3: standalone EXTRACT(YEAR FROM ...) patterns
    # ------------------------------------------------------------------
    # IN list
    in_pattern = re.compile(
        year_re + r"\s+IN\s*\(\s*((?:-?\d+\s*,\s*)*-?\d+)\s*\)",
        flags=flags,
    )

    def _in_repl(match: re.Match) -> str:
        nonlocal fired
        col = match.group(1)
        raw_years = match.group(2)
        years_list: list[int] = []
        seen: set[int] = set()
        for tok in raw_years.split(","):
            try:
                y = int(tok.strip())
            except ValueError:
                return match.group(0)
            if not _pushdown_year_valid(y):
                return match.group(0)
            if y in seen:
                continue
            seen.add(y)
            years_list.append(y)
        if not years_list:
            return match.group(0)
        fired = True
        ranges = [
            f"({col} >= {_pushdown_format_date(y)} "
            f"AND {col} < {_pushdown_format_date(y + 1)})"
            for y in years_list
        ]
        if len(ranges) == 1:
            return ranges[0]
        return "(" + " OR ".join(ranges) + ")"

    masked = in_pattern.sub(_in_repl, masked)

    # BETWEEN
    between_pattern = re.compile(
        year_re + r"\s+BETWEEN\s+(-?\d+)\s+AND\s+(-?\d+)",
        flags=flags,
    )

    def _between_repl(match: re.Match) -> str:
        nonlocal fired
        col = match.group(1)
        lo = int(match.group(2))
        hi = int(match.group(3))
        if lo > hi:
            return match.group(0)
        if not (_pushdown_year_valid(lo) and _pushdown_year_valid(hi)):
            return match.group(0)
        fired = True
        return (
            f"({col} >= {_pushdown_format_date(lo)} "
            f"AND {col} < {_pushdown_format_date(hi + 1)})"
        )

    masked = between_pattern.sub(_between_repl, masked)

    # != / <>
    neq_pattern = re.compile(
        year_re + r"\s*(?:!=|<>)\s*(-?\d+)",
        flags=flags,
    )

    def _neq_repl(match: re.Match) -> str:
        nonlocal fired
        col = match.group(1)
        year = int(match.group(2))
        if not _pushdown_year_valid(year):
            return match.group(0)
        fired = True
        return (
            f"({col} < {_pushdown_format_date(year)} "
            f"OR {col} >= {_pushdown_format_date(year + 1)})"
        )

    masked = neq_pattern.sub(_neq_repl, masked)

    # =
    eq_pattern = re.compile(
        year_re + r"\s*=\s*(-?\d+)",
        flags=flags,
    )

    def _eq_repl(match: re.Match) -> str:
        nonlocal fired
        col = match.group(1)
        year = int(match.group(2))
        if not _pushdown_year_valid(year):
            return match.group(0)
        fired = True
        return (
            f"({col} >= {_pushdown_format_date(year)} "
            f"AND {col} < {_pushdown_format_date(year + 1)})"
        )

    masked = eq_pattern.sub(_eq_repl, masked)

    # ------------------------------------------------------------------
    # Step 4: restore masked literals / comments
    # ------------------------------------------------------------------
    final_sql = _pushdown_unmask(masked, replacements)
    if fired:
        notices.append(_PUSHDOWN_NOTICE)
    return final_sql, notices


def _repair_common_sql_before_execution(sql: str) -> tuple[str, list[str]]:
    """Apply safe DuckDB-dialect repairs before executing agent SQL."""
    notices: list[str] = []

    # EXTRACT-pushdown rewrite runs FIRST so later repairs see the rewritten
    # date ranges instead of EXTRACT(...) shapes that postgres_scanner cannot
    # push down (issue #64).
    sql, pushdown_notices = _rewrite_for_pg_pushdown(sql)
    notices.extend(pushdown_notices)

    repaired = re.sub(
        r"\bILIKE\s*\(\s*([A-Za-z_][A-Za-z0-9_\\.\" ]*)\s*,\s*('(?:''|[^'])*')\s*\)",
        lambda m: f"{m.group(1).strip()} ILIKE {m.group(2)}",
        sql,
        flags=re.IGNORECASE,
    )
    if repaired != sql:
        notices.append("rewrote function-style ILIKE(...) to DuckDB infix ILIKE syntax.")
        sql = repaired

    # ORDER BY CAST(label AS INTEGER) often fails after the agent has already
    # labeled blanks/unknowns. TRY_CAST preserves numeric ordering while making
    # non-numeric labels sortable instead of crashing the query.
    order_by_pos = re.search(r"\bORDER\s+BY\b", sql, flags=re.IGNORECASE)
    if order_by_pos:
        prefix = sql[: order_by_pos.start()]
        order_clause = sql[order_by_pos.start():]
        repaired_order = re.sub(
            r"\bCAST\s*\(\s*([A-Za-z_][A-Za-z0-9_\\.]*)\s+AS\s+(INTEGER|INT|BIGINT)\s*\)",
            r"TRY_CAST(\1 AS \2)",
            order_clause,
            flags=re.IGNORECASE,
        )
        if repaired_order != order_clause:
            notices.append("rewrote ORDER BY CAST(... AS integer) to TRY_CAST to tolerate non-numeric labels.")
            sql = prefix + repaired_order

    return sql, notices


def _sql_cache_key(sql: str) -> str:
    normalized = _normalize_sql(sql)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _normalize_sql(sql: str) -> str:
    return " ".join(str(sql).strip().rstrip(";").lower().split())


def _sql_pair_drift_notice(ctx, sql: str) -> str | None:
    """Warn when executing SQL differs from a SQL pair loaded this turn.

    SQL pairs are project-owned examples. They are not mandatory for every
    query, so this notice is intentionally advisory and never blocks execution.
    It gives the model a chance to stop after the authoritative pair result
    instead of silently drifting into a plausible but different query.

    HIDDEN COUPLING (issue #64 R7 fix): Both sides are normalized THROUGH
    ``_rewrite_for_pg_pushdown`` before comparison. Without this, a SQL pair
    YAML containing ``EXTRACT(YEAR FROM ...) = N`` and an agent query using
    the same form would normalize differently (the agent's SQL passes
    through ``_repair_common_sql_before_execution`` first; the raw pair YAML
    stored in ``get_loaded_sql_pairs`` does not). That mismatch produced a
    spurious ``⚠ SQL pair drift`` warning on every EXTRACT pair invocation.
    """
    from seeknal.ask.agents.tools._context import get_loaded_sql_pairs

    loaded_pairs = get_loaded_sql_pairs(ctx)
    if not loaded_pairs:
        return None
    normalized = _normalize_sql(_rewrite_for_pg_pushdown(sql)[0])
    for pair_sql in loaded_pairs.values():
        pair_normalized = _normalize_sql(_rewrite_for_pg_pushdown(pair_sql)[0])
        if normalized == pair_normalized:
            return None
    names = ", ".join(sorted(loaded_pairs)[:3])
    if len(loaded_pairs) > 3:
        names += ", ..."
    return (
        "⚠ SQL pair drift: this SQL differs from the SQL pair loaded earlier "
        f"this turn ({names}). Prefer the SQL pair result as authoritative "
        "unless a tool error or user request requires a changed grain/filter."
    )


def _should_require_sql_pair_lookup(ctx, sql: str, checked: bool) -> bool:
    """Require SQL-pair lookup before ad-hoc business SQL when pairs exist.

    Only ``seeknal/sql_pairs/`` triggers the guard. ``context/sql_pairs/`` is
    historically a derived-context storage location (see ``CLAUDE.md``); using
    it as a guard trigger conflated curated cheatsheets with generated source
    context.
    """
    if checked or not (getattr(ctx, "current_question", None) or "").strip():
        return False
    if _looks_like_direct_sql_request(ctx.current_question):
        return False
    if _is_metadata_or_healthcheck_sql(sql):
        return False
    root = ctx.project_path.resolve() / "seeknal" / "sql_pairs"
    if not root.exists():
        return False
    return any(
        path.is_file() and path.suffix.lower() in {".yml", ".yaml"}
        for path in root.rglob("*")
    )


def _looks_like_direct_sql_request(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    return "execute_sql" in lowered or "run this sql" in lowered or "jalankan sql" in lowered


def _is_metadata_or_healthcheck_sql(sql: str) -> bool:
    normalized = _normalize_sql(sql)
    metadata_markers = (
        "information_schema",
        "pg_catalog",
        "show ",
        "describe ",
        "pragma ",
    )
    return any(marker in normalized for marker in metadata_markers)


def _repair_sql_from_duckdb_suggestion(sql: str, error: str) -> str | None:
    """Retry a missing-table query with DuckDB's fully-qualified suggestion."""
    suggestion_match = re.search(r'Did you mean "([A-Za-z_][A-Za-z0-9_.]*)"', error)
    missing_match = re.search(r"Table with name ([A-Za-z_][A-Za-z0-9_]*) does not exist", error)
    if not suggestion_match or not missing_match:
        return None

    missing = missing_match.group(1)
    suggested = suggestion_match.group(1)
    if suggested.split(".")[-1].lower() != missing.lower():
        return None
    # Replace only standalone unqualified relation tokens. This intentionally
    # avoids fuzzy SQL rewriting; it only accepts the fully-qualified table
    # name surfaced by DuckDB itself.
    return re.sub(rf"\b{re.escape(missing)}\b", suggested, sql, count=1)


def _count_total(ctx, sql: str) -> int | None:
    """Return the true row count of ``sql`` by wrapping it in COUNT(*).

    Returns None on any parse / execution failure so the caller can fall
    back to ``"many rows"`` without crashing the tool.
    """
    wrapped = f"SELECT CAST(COUNT(*) AS BIGINT) AS c FROM ({sql}) AS _seek_q"
    _cols, rows = _execute_oneshot_with_timeout(ctx, wrapped, limit=1)
    if rows and rows[0] and rows[0][0] is not None:
        try:
            return int(rows[0][0])
        except (TypeError, ValueError):
            return None
    return None


def _execute_oneshot_with_timeout(ctx, sql: str, limit: int | None = None):
    """Execute REPL SQL with the session db lock and optional hard timeout."""
    import concurrent.futures
    import time

    from seeknal.ask.agents.tools._context import record_timing_event

    timeout = int(getattr(ctx, "sql_timeout_seconds", 0) or 0)
    started = time.monotonic()
    with ctx.db_lock:
        if timeout <= 0:
            try:
                return ctx.repl.execute_oneshot(sql, limit=limit)
            finally:
                elapsed_ms = int((time.monotonic() - started) * 1000)
                record_timing_event("execute_sql", elapsed_ms)

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(ctx.repl.execute_oneshot, sql, limit)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            interrupt = getattr(getattr(ctx.repl, "conn", None), "interrupt", None)
            if callable(interrupt):
                try:
                    interrupt()
                except Exception:  # noqa: BLE001 - best-effort cancellation
                    pass
            future.cancel()
            raise TimeoutError(f"SQL execution timed out after {timeout} seconds") from exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            record_timing_event("execute_sql", elapsed_ms)


def _truncate_cell(value) -> tuple[str, bool]:
    """Stringify and cap cell content. Returns (text, was_truncated)."""
    if value is None:
        return "NULL", False
    text = str(value)
    if text.strip() == "":
        # Markdown tables and Rich rendering both collapse empty/whitespace-only
        # cells in ways that can visually shift columns. Surface the value as a
        # label so the agent and user can distinguish "blank string" from NULL
        # without losing column alignment.
        return "[blank]", False
    truncated = False
    if len(text) > _CELL_MAX_LEN:
        text = text[: _CELL_MAX_LEN - 1] + "…"
        truncated = True
    # Pipes and newlines break markdown rows.
    if "|" in text:
        text = text.replace("|", "\\|")
    if "\n" in text:
        text = text.replace("\n", " ")
    return text, truncated

def _format_table(
    columns: list[str],
    rows: list[tuple],
    total_rows: int | None,
    has_more_rows: bool,
    requested_limit: int,
) -> str:
    notices: list[str] = []

    # Column cap
    original_column_count = len(columns)
    columns_truncated = False
    if original_column_count > _COLUMN_HARD_CAP:
        columns = columns[:_COLUMN_HARD_CAP]
        columns_truncated = True
        notices.append(
            f"Result truncated: showing {_COLUMN_HARD_CAP} of "
            f"{original_column_count} columns. Use SELECT col1, col2, ... to "
            "narrow the projection."
        )

    # Build rows with cell-length truncation applied.
    any_cell_truncated = False
    body_lines: list[str] = []
    for row in rows:
        slice_row = row[: len(columns)] if columns_truncated else row
        cells = []
        for value in slice_row:
            text, was_truncated = _truncate_cell(value)
            if was_truncated:
                any_cell_truncated = True
            cells.append(text)
        body_lines.append("| " + " | ".join(cells) + " |")

    if any_cell_truncated:
        notices.append(
            f"Result truncated: long cell values capped at "
            f"{_CELL_MAX_LEN} chars. Query specific columns or use "
            "substring/length filters if you need full values."
        )

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    # Byte budget — drop rows from the end until the markdown fits.
    rows_dropped_for_budget = 0
    while body_lines:
        assembled = "\n".join([header, separator, *body_lines])
        if len(assembled.encode("utf-8")) <= _BYTE_BUDGET:
            break
        body_lines.pop()
        rows_dropped_for_budget += 1

    if rows_dropped_for_budget > 0:
        notices.append(
            f"Result truncated: output exceeded {_BYTE_BUDGET // 1024} KB; "
            f"dropped {rows_dropped_for_budget} trailing row(s). Narrow the "
            "query (WHERE / GROUP BY / aggregate) to see more."
        )

    shown_rows = len(body_lines)
    # The hard cap actually clamps only when the caller requested more than it.
    hard_cap_hit = requested_limit > _ROW_HARD_CAP
    if has_more_rows:
        if total_rows is not None:
            footer = f"({shown_rows:,} of {total_rows:,} rows shown)"
        else:
            footer = f"({shown_rows:,} of many rows shown)"
        # Only warn loudly when the hard cap is the reason we truncated; the
        # caller's own limit doing its job isn't "truncation", just pagination.
        if hard_cap_hit:
            if total_rows is not None:
                notices.append(
                    f"Result truncated: returned {shown_rows:,} of "
                    f"{total_rows:,} total rows (hard cap {_ROW_HARD_CAP}). "
                    "Use WHERE, GROUP BY, or LIMIT with pagination to see others."
                )
            else:
                notices.append(
                    f"Result truncated: returned {shown_rows:,} rows; the full "
                    "result set is larger (exact count unavailable). Use "
                    "WHERE or aggregation to narrow."
                )
    else:
        footer = f"({shown_rows:,} row{'s' if shown_rows != 1 else ''})"
        if hard_cap_hit:
            notices.append(
                f"Note: requested limit {requested_limit} was clamped to "
                f"{_ROW_HARD_CAP}; full result set fit within the cap."
            )

    result = "\n".join([header, separator, *body_lines, "", footer])
    if notices:
        result += "\n\n" + "\n".join(f"⚠ {n}" for n in notices)
    return result


__all__ = ["execute_sql"]

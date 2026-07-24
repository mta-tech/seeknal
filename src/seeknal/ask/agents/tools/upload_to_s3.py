"""upload_to_s3 — export query results OR computed data to a downloadable CSV.

General dual-mode tool (r4). The agent provides either a Raw SQL query (Mode 1)
or pre-computed rows (Mode 2) — the tool builds a CSV, uploads it to SeaweedFS
through iba-storage's v26.7.0 presigned-URL flow, and registers a
``pending_upload`` so the gateway streaming layer emits an ``upload_complete``
event (the frontend renders a Download button).

Two modes:
  - **Mode 1 (SQL):** ``upload_to_s3(filename, sql="SELECT ...")`` — tool
    executes the SQL via ``ctx.repl.execute_oneshot`` and uses the column names
    from the cursor as the CSV header. Use this when the data lives in a
    database (execute_sql results, ad-hoc queries).
  - **Mode 2 (data):** ``upload_to_s3(filename, data=[[...], ...],
    columns=["col1", "col2"])`` — tool builds the CSV directly from the
    provided rows and column names. Use this when the data is computed
    in-process and not SQL-queryable (forecast.points from run_forecast,
    future analytics tools, etc).

Both modes produce the same outcome: CSV bytes → presigned PUT → SeaweedFS →
``ctx.pending_upload`` → gateway emits ``upload_complete``.

``expires_at`` is sourced VERBATIM from the server response
(``download_url_expires_at``, a Unix int, via the W13-B update PR #154) and
converted to ISO 8601. The tool holds no local TTL constant.

Domain-neutral (``AGENTS.md``): the SQL or data is the agent's responsibility;
the tool contains no domain-specific strings.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# No storage URL/API key here: connectivity is resolved once at session init
# (agent.py) and injected via ToolContext.iba_storage_presign_url/iba_storage_api_key
# -- this module never reads os.environ for storage config. See _context.py's ToolContext.

# CSV exports are files, not chat-context tables — decoupled from both
# execute_sql's 500-row chat-display cap and ctx.request_limit (the
# pydantic-ai UsageLimits.request_limit model-request budget, NOT a row
# count; using it as a row limit silently truncated "full data" exports to
# ~100 rows). 5000 keeps exports well under typical SeaweedFS/browser CSV
# handling while covering the export use case (raw dumps larger than the
# chat table, smaller than a full warehouse scan).
_CSV_EXPORT_ROW_LIMIT = int(os.environ.get("SEEKNAL_CSV_EXPORT_ROW_LIMIT", "5000"))


def upload_to_s3(
    filename: str,
    sql: str = "",
    data: list[list[Any]] | None = None,
    columns: list[str] | None = None,
) -> str:
    """Export query result OR computed data to a downloadable CSV.

    Mode 1 (SQL):  ``upload_to_s3(filename, sql="SELECT ...")``
        Tool executes the SQL via ``ctx.repl.execute_oneshot`` and uses the
        column names from the cursor as the CSV header.

    Mode 2 (data): ``upload_to_s3(filename, data=[[...], ...], columns=[...])``
        Tool builds the CSV directly from the provided rows and column names.
        Use for computed results that are not SQL-queryable (e.g. forecast
        projection points from ``run_forecast``).

    The two arguments are mutually exclusive: provide ``sql=`` OR
    ``data=``+``columns=``, never both.

    Args:
        filename: Download filename, e.g. ``"registrations-2026.csv"`` or
            ``"forecast-2026-07.csv"``. The storage key is namespaced
            ``csv-exports/{uuid}/{filename}``; collisions are impossible.
        sql: SELECT to execute (Mode 1). Mutually exclusive with ``data=``.
            Column names become the CSV header.
        data: Pre-computed rows as a list of lists (Mode 2). Each inner list
            is one row; its length must match ``columns``.
        columns: Column names for the CSV header (Mode 2). Required with
            ``data=``; ignored in Mode 1.

    Returns:
        A confirmation string on success -- deliberately WITHOUT the raw
        download URL (the frontend renders its own Download button from the
        ``upload_complete`` event; a URL in the tool result invites the agent
        to paste it into the answer as a markdown link, duplicating and often
        breaking the dedicated UI widget). Do NOT include a raw link/URL in
        your answer text -- refer to the download in prose only (e.g. "the
        full data is available via the Download button below") if relevant.
        ``"No rows to export."`` if the SQL returned zero rows or ``data=[]``.
        On argument-mismatch: a ``## Kesalahan`` block describing the fix.
        On Mode 1 SQL execution failure: the same structured JSON error
        ``execute_sql`` returns (``format_tool_error``/``classify_duckdb_error``)
        -- one auto-retry is attempted first using DuckDB's own table-name
        suggestion. Returning the same shape lets the existing POST_TOOL_USE
        self-correction hook enrich it with retry hints, same as it already
        does for ``execute_sql`` failures.
        On storage-unreachable / PUT-failure: an error string describing the
        failure (the agent should surface it, not retry silently).
    """
    from seeknal.ask.agents.tools._context import (
        get_tool_context,
        record_tool_result,
        register_exported_dataset,
    )
    from seeknal.ask.agents.tools.errors import (
        TERMINAL_DEPENDENCY_UNAVAILABLE,
        format_tool_error,
    )

    ctx = get_tool_context()

    # ── STEP 1: resolve (columns, rows) from one of the two modes ─────────
    mode = _resolve_mode(sql=sql, data=data, columns=columns)
    if isinstance(mode, str):
        # Argument-mismatch error — surface as Kesalahan block, no tool_result
        # record (the tool never reached the network).
        return mode
    cols, rows, used_sql_mode = mode

    if not rows:
        record_tool_result(
            "upload_to_s3", "empty",
            args={"filename": filename, "mode": "sql" if used_sql_mode else "data"},
        )
        return "No rows to export."

    # Mode 1 (SQL, the agent's deliberate one-shot export per the project's
    # analyst skill) gets a filename derived from the user's question instead
    # of whatever the agent passed — deterministic and consistent regardless
    # of how carefully the agent named it. Mode 2
    # (forecast's self-upload, D7) is untouched: it already names files well
    # (`forecast-{table}-{timestamp}.csv`) and changing it risks that
    # already-verified-live behavior for no benefit.
    if used_sql_mode:
        filename = _derive_filename_from_question(getattr(ctx, "current_question", None), sql)

    # ── STEP 2: BUILD CSV (header from cols; no metadata rows mixed in) ────
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    # ── STEP 3: presigned URLs + server-computed expiry (W13-B update) ─────
    # Presign URL resolved once at session init and stored on ToolContext.
    # See _context.py::_resolve_storage_presign_url for the fallback chain.
    presign_url = ctx.iba_storage_presign_url
    if not presign_url:
        record_tool_result(
            "upload_to_s3",
            format_tool_error(
                TERMINAL_DEPENDENCY_UNAVAILABLE,
                "Storage belum dikonfigurasi -- set IBA_STORAGE_URL atau SEEKNAL_GATEWAY_URL.",
            ),
            args={"filename": filename, "mode": "sql" if used_sql_mode else "data"},
        )
        return "Storage belum dikonfigurasi -- set IBA_STORAGE_URL atau SEEKNAL_GATEWAY_URL."

    try:
        resp = httpx.post(
            presign_url,
            json={"filename": filename, "content_type": "text/csv"},
            headers={"X-API-Key": ctx.iba_storage_api_key},
            timeout=30.0,
        )
        resp.raise_for_status()
        urls = resp.json()
    except httpx.RequestError:
        record_tool_result(
            "upload_to_s3",
            format_tool_error(
                TERMINAL_DEPENDENCY_UNAVAILABLE,
                "Storage unavailable (timeout or connection failed).",
            ),
            args={"filename": filename, "mode": "sql" if used_sql_mode else "data"},
        )
        return "Storage unavailable (timeout or connection failed)."
    except httpx.HTTPStatusError as exc:
        record_tool_result(
            "upload_to_s3",
            format_tool_error(
                TERMINAL_DEPENDENCY_UNAVAILABLE,
                f"Failed to obtain presigned URL: HTTP {exc.response.status_code}.",
            ),
            args={"filename": filename, "mode": "sql" if used_sql_mode else "data"},
        )
        return f"Failed to obtain presigned URL: HTTP {exc.response.status_code}."

    # ── STEP 4: PUT the CSV to SeaweedFS via the presigned URL ────────────
    # Do this promptly — upload_url expires at urls["upload_url_expires_at"].
    try:
        put = httpx.put(
            urls["upload_url"],
            content=csv_bytes,
            headers={"Content-Type": "text/csv"},
            timeout=60.0,
        )
        put.raise_for_status()
    except httpx.HTTPError:
        record_tool_result(
            "upload_to_s3",
            format_tool_error(
                TERMINAL_DEPENDENCY_UNAVAILABLE,
                "CSV upload failed (SeaweedFS rejected the write).",
            ),
            args={"filename": filename, "mode": "sql" if used_sql_mode else "data"},
        )
        return "CSV upload failed (SeaweedFS rejected the write)."

    # ── STEP 5: register pending_upload — gateway loop emits upload_complete
    # expires_at is sourced VERBATIM from the server response
    # (download_url_expires_at, a Unix int) and converted to ISO 8601 because
    # the frontend types `expires_at: string`. No local `now + 8h` computation.
    try:
        expires_at = datetime.fromtimestamp(
            int(urls["download_url_expires_at"]), tz=timezone.utc
        ).isoformat()
    except (KeyError, TypeError, ValueError):
        expires_at = ""

    # download_url (2026-07-08): when reached via SEEKNAL_GATEWAY_URL, this is
    # no longer a raw SeaweedFS presigned URL -- iba-service's
    # /v6/internal/storage/presign proxy now overwrites it with a signed link
    # to its own /v1/downloads endpoint (see iba-service's internal_storage.py
    # + core/signing.py) so SeaweedFS's own address never reaches a browser.
    # This tool just passes whatever URL the presign response contains
    # through unmodified either way -- no change needed here for that fix.
    ctx.pending_upload = {
        "download_url": urls.get("download_url", ""),
        "file_name": filename,
        "file_size": len(csv_bytes),
        "expires_at": expires_at,
        "object_name": urls.get("object_name", ""),
    }

    # M9: publish the exported rows so visualize_chart can draw this exact
    # dataset. Matters most for Mode 2, whose rows a tool computed in-process
    # and no query can reproduce -- charting those from SQL would silently drop
    # the computed part. Best-effort: a registry failure must never fail an
    # upload that already succeeded.
    try:
        register_exported_dataset(filename, list(cols), rows, ctx=ctx)
    except Exception:
        logger.exception(
            "[upload_to_s3] dataset registration failed; upload unaffected"
        )

    record_tool_result(
        "upload_to_s3", urls.get("download_url", ""),
        args={"filename": filename, "mode": "sql" if used_sql_mode else "data"},
    )
    return (
        "Upload complete. A Download button for this file is now shown to "
        "the user automatically (valid 8h). Do NOT include a raw link/URL "
        "in your answer -- refer to it in prose only if relevant (e.g. "
        "\"the full data is available via the Download button below\")."
    )


def _derive_filename_from_sql(sql: str) -> str:
    """Best-effort CSV filename derived from the SQL query's table name.

    Fallback used by ``_derive_filename_from_question`` when the user's
    question is unavailable. The name only affects the user-facing Download
    label, not storage — the object key is namespaced under
    ``csv-exports/{uuid}/{filename}`` so collisions are impossible regardless.
    """
    table = "result"
    try:
        # Capture the full FROM target (e.g. "warehouse.public.some_table"),
        # then split on dots to get the bare table name. Word boundaries +
        # greedy [\w.]+ ensure we don't stop at the first segment.
        m = re.search(r"\bFROM\s+([\w\.]+)", sql, re.IGNORECASE)
        if m:
            table_full = m.group(1).strip("`\"'")
            table = table_full.split(".")[-1].lower()
            table = re.sub(r"^(t_|tbl_|table_|ft_)", "", table) or "result"
    except Exception:
        table = "result"
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    return f"{table}-{ts}.csv"


def _derive_filename_from_question(question: str | None, sql: str, max_len: int = 60) -> str:
    """CSV filename derived from the user's question, not just the SQL.

    Slugifies ``question`` (lowercase, non-alphanumerics collapsed to ``-``),
    truncates at a word boundary to ``max_len`` characters (never mid-word),
    and appends a timestamp — e.g. "What was the monthly trend last year?" ->
    ``what-was-the-monthly-trend-last-year-20260709-060344.csv``. Falls back
    to ``_derive_filename_from_sql`` when the question is empty/unavailable so
    Mode 1 always gets a reasonable name regardless of caller context.
    """
    if question and question.strip():
        slug = re.sub(r"[^a-z0-9]+", "-", question.strip().lower()).strip("-")
        slug = re.sub(r"-{2,}", "-", slug)
        if slug:
            if len(slug) > max_len:
                truncated = slug[:max_len]
                if "-" in truncated:
                    truncated = truncated.rsplit("-", 1)[0]
                slug = truncated or slug[:max_len]
            ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
            return f"{slug}-{ts}.csv"
    return _derive_filename_from_sql(sql)


def _resolve_mode(
    *,
    sql: str,
    data: list[list[Any]] | None,
    columns: list[str] | None,
) -> tuple[list[str], list[list[Any]], bool] | str:
    """Dispatch to SQL mode or data mode based on which arguments were provided.

    Returns ``(columns, rows, used_sql_mode)`` on success, or a ``## Kesalahan``
    markdown block string on argument-mismatch (caller returns it as-is).

    Validation rules:
      - ``sql=`` and ``data=`` are mutually exclusive (one or the other, not both)
      - Mode 2 requires BOTH ``data=`` and ``columns=``
      - Mode 2: every row length must equal ``len(columns)`` (else CSV would be
        jagged — reject up front with a clear error rather than producing a
        malformed CSV)
      - Empty SQL string is treated as "not provided" so the user can call
        ``upload_to_s3(filename, sql="")`` without surprising behavior
    """
    has_sql = bool(sql and sql.strip())
    has_data = data is not None
    has_columns = bool(columns)

    if has_sql and has_data:
        return (
            "## Kesalahan\n\n"
            "**upload_to_s3 argument conflict.**\n\n"
            "Provide either ``sql=`` (Mode 1) OR ``data=``+``columns=`` "
            "(Mode 2), not both. The two modes are mutually exclusive."
        )

    if has_sql:
        # Mode 1: execute SQL via the existing DuckDB ATTACH seam.
        #
        # Repair + row-limit + governance mirror execute_sql.py exactly so the
        # CSV always matches what the agent saw on screen (audit fix D1-D3,
        # 2026-07-09):
        #   D1 — without _repair_common_sql_before_execution, EXTRACT(YEAR
        #        FROM ...) filters are NOT pushed down to PostgreSQL and are
        #        evaluated locally in DuckDB with different cast/NULL
        #        semantics, silently producing different row counts than the
        #        execute_sql call the agent already ran for the same SQL.
        #   D2 — ctx.request_limit is pydantic-ai's UsageLimits.request_limit
        #        (a model-request budget, default 100), not a row count. Using
        #        it here silently truncated "full data" exports.
        #   D3 — execute_sql masks sensitive columns via Atlas governance;
        #        skipping that here would let a masked column leave
        #        ungoverned through the exported file.
        from seeknal.ask.agents.tools._context import get_tool_context
        from seeknal.ask.agents.tools.errors import classify_duckdb_error, format_tool_error
        from seeknal.ask.agents.tools.execute_sql import (
            _execute_oneshot_with_timeout,
            _repair_common_sql_before_execution,
            _repair_sql_from_duckdb_suggestion,
        )

        ctx = get_tool_context()
        # D1 follow-up (audit fix, 2026-07-09): execute_sql.py strips a
        # trailing semicolon BEFORE repair/execution; this branch didn't, so
        # agent SQL ending in ";" (LLMs add this routinely) survived into
        # _execute_oneshot_with_timeout, which wraps the query as
        # "SELECT * FROM (<sql>) AS _q LIMIT N" to apply the row cap -- the
        # leftover ";" then lands mid-statement ("...;) AS _q LIMIT 5000"),
        # a guaranteed DuckDB ParserException on every semicolon-terminated
        # query. Strip it here too, matching execute_sql.py exactly.
        sql = str(sql).strip().rstrip(";").strip()
        repaired_sql, _notices = _repair_common_sql_before_execution(sql)

        # Error handling reuses execute_sql.py's exact retry/classification
        # contract instead of inventing a parallel one (audit fix, 2026-07-09):
        # this call was previously unguarded, so any execution failure (not
        # just the semicolon case above) propagated as an unhandled exception
        # through the tool-calling machinery -- observed live to abort the
        # whole agent turn with no answer at all (SSE trace showed a bare
        # "error" event, no tool_result/answer; conversation history recorded
        # question/answer as null; worker log showed the turn completing with
        # only 3 events and status=error versus 22-68 for a normal turn).
        # Returning the SAME format_tool_error() JSON shape execute_sql.py
        # returns means the existing POST_TOOL_USE self-correction hook
        # (hooks.py, matcher now covers this tool too) enriches it with the
        # same retry hints the agent already knows how to act on -- table
        # suggestions, DuckDB syntax tips -- rather than a second,
        # upload_to_s3-specific self-correction path.
        try:
            cols, rows = _execute_oneshot_with_timeout(
                ctx, repaired_sql, limit=_CSV_EXPORT_ROW_LIMIT
            )
        except Exception as exc:
            # One auto-retry using DuckDB's own "Did you mean" suggestion,
            # same as execute_sql.py -- most missing-table typos self-resolve
            # here without costing the agent a turn.
            suggested_sql = _repair_sql_from_duckdb_suggestion(repaired_sql, str(exc))
            if suggested_sql and suggested_sql != repaired_sql:
                try:
                    cols, rows = _execute_oneshot_with_timeout(
                        ctx, suggested_sql, limit=_CSV_EXPORT_ROW_LIMIT
                    )
                    repaired_sql = suggested_sql
                except Exception as retry_exc:
                    return format_tool_error(classify_duckdb_error(str(retry_exc)), str(retry_exc))
            else:
                return format_tool_error(classify_duckdb_error(str(exc)), str(exc))
        rows = [list(r) for r in rows]

        from seeknal.integrations.atlas_client import AtlasPolicyDenied
        from seeknal.integrations.atlas_governance import (
            create_governance_gate_from_env,
            govern_query,
        )

        gov_gate = create_governance_gate_from_env()
        if gov_gate is not None:
            try:
                rows = govern_query(gov_gate, sql=repaired_sql, columns=list(cols), rows=rows)
            except AtlasPolicyDenied as denial:
                return f"Access denied by Atlas governance policy: {denial}"

        return list(cols), rows, True

    if has_data:
        # Mode 2: use provided rows + column names directly.
        if not has_columns:
            return (
                "## Kesalahan\n\n"
                "**upload_to_s3 Mode 2 missing column names.**\n\n"
                "When using ``data=``, you must also pass ``columns=`` "
                "(the CSV header). Example: "
                "``upload_to_s3(filename, data=[[...]], columns=['period','point'])``."
            )
        if len(columns) == 0:
            return (
                "## Kesalahan\n\n"
                "**upload_to_s3 Mode 2 empty columns list.**\n\n"
                "``columns=`` must contain at least one column name."
            )
        # Validate row shapes — reject jagged rows up front.
        n_cols = len(columns)
        for i, row in enumerate(data):
            if not isinstance(row, (list, tuple)):
                return (
                    f"## Kesalahan\n\n"
                    f"**upload_to_s3 Mode 2 row {i} is not a list.**\n\n"
                    f"Each row in ``data=`` must be a list/tuple. Got "
                    f"``{type(row).__name__}``."
                )
            if len(row) != n_cols:
                return (
                    f"## Kesalahan\n\n"
                    f"**upload_to_s3 Mode 2 row {i} has wrong length.**\n\n"
                    f"Expected ``{n_cols}`` cells (matching ``columns=``), "
                    f"got ``{len(row)}``. Jagged rows would produce a malformed "
                    f"CSV — fix the data or the columns list."
                )
        return list(columns), [list(r) for r in data], False

    # Neither mode satisfied.
    return (
        "## Kesalahan\n\n"
        "**upload_to_s3 missing data source.**\n\n"
        "Provide either:\n"
        "  - Mode 1 (SQL):  ``upload_to_s3(filename, sql='SELECT ...')``\n"
        "  - Mode 2 (data): ``upload_to_s3(filename, data=[[...]], columns=[...])``\n"
        "\n"
        "Use Mode 1 for database results (execute_sql outputs). Use Mode 2 for "
        "computed results not SQL-queryable (forecast.points, analytics)."
    )

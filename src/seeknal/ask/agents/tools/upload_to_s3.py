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
import os
from datetime import datetime, timezone
from typing import Any

import httpx

_IBA_STORAGE_URL = os.environ.get("IBA_STORAGE_URL", "")
# r6: if IBA_STORAGE_URL is not set, use SEEKNAL_GATEWAY_URL for proxy.
# Gateway route: /v6/internal/storage/presign → iba-storage presign endpoint.
if not _IBA_STORAGE_URL:
    _gw_s = os.environ.get("SEEKNAL_GATEWAY_URL", "")
    if _gw_s:
        _IBA_STORAGE_URL = _gw_s.rstrip("/").removesuffix("/v6") + "/v6/internal/storage/presign"
    else:
        _IBA_STORAGE_URL = "http://iba-storage:8000"
_IBA_STORAGE_API_KEY = os.environ.get("IBA_STORAGE_API_KEY", "")


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
        On storage-unreachable / PUT-failure: an error string describing the
        failure (the agent should surface it, not retry silently).
    """
    from seeknal.ask.agents.tools._context import get_tool_context, record_tool_result
    from seeknal.ask.agents.tools.execute_sql import _execute_oneshot_with_timeout

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

    # ── STEP 2: BUILD CSV (header from cols; no metadata rows mixed in) ────
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    # ── STEP 3: presigned URLs + server-computed expiry (W13-B update) ─────
    # NOTE: iba-storage mounts the internal router under ``/api`` (main.py:
    # ``app.include_router(internal_router, prefix="/api")``), so the full path
    # is ``/api/v1/internal/get-upload-url`` — not ``/v1/...``.
    # r6: if gateway URL is used, the path is already included.
    # If direct URL, append the presign path.
    if "/internal/storage/presign" in _IBA_STORAGE_URL:
        _presign_url = _IBA_STORAGE_URL
    else:
        _presign_url = f"{_IBA_STORAGE_URL}/api/v1/internal/get-upload-url"

    try:
        resp = httpx.post(
            _presign_url,
            json={"filename": filename, "content_type": "text/csv"},
            headers={"X-API-Key": _IBA_STORAGE_API_KEY},
            timeout=30.0,
        )
        resp.raise_for_status()
        urls = resp.json()
    except httpx.RequestError:
        record_tool_result(
            "upload_to_s3", "error",
            args={"filename": filename, "mode": "sql" if used_sql_mode else "data"},
        )
        return "Storage unavailable (timeout or connection failed)."
    except httpx.HTTPStatusError as exc:
        record_tool_result(
            "upload_to_s3", "error",
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
            "upload_to_s3", "error",
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

    ctx.pending_upload = {
        "download_url": urls.get("download_url", ""),
        "file_name": filename,
        "file_size": len(csv_bytes),
        "expires_at": expires_at,
        "object_name": urls.get("object_name", ""),
    }

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
        from seeknal.ask.agents.tools._context import get_tool_context
        from seeknal.ask.agents.tools.execute_sql import _execute_oneshot_with_timeout

        ctx = get_tool_context()
        cols, rows = _execute_oneshot_with_timeout(ctx, sql, limit=ctx.request_limit)
        return list(cols), [list(r) for r in rows], True

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

"""upload_to_s3 — export a query result to a downloadable CSV.

Generic SQL→CSV export via the IBA object store (SeaweedFS through iba-storage's
v26.7.0 presigned-URL flow). The agent provides Raw SQL only; the tool executes
it internally, serializes the result set to CSV, uploads it, and registers a
``pending_upload`` so the gateway streaming layer emits an ``upload_complete``
event (the frontend renders a Download button).

``expires_at`` is sourced VERBATIM from the server response
(``download_url_expires_at``, a Unix int, via the W13-B update PR #154) and
converted to ISO 8601. The tool holds no local TTL constant.

Domain-neutral (``AGENTS.md``): the SQL is the agent's responsibility; the tool
contains no domain-specific strings.
"""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timezone
from typing import Any

import httpx

_IBA_STORAGE_URL = os.environ.get("IBA_STORAGE_URL", "http://iba-storage:8000")
_IBA_STORAGE_API_KEY = os.environ.get("IBA_STORAGE_API_KEY", "")


def upload_to_s3(filename: str, sql: str) -> str:
    """Export a query result to a downloadable CSV via the IBA object store.

    Executes the caller-supplied SQL via ``ctx.repl.execute_oneshot``,
    serializes the result set to CSV, uploads it to SeaweedFS through the
    v26.7.0 presigned-URL flow (iba-storage), and registers the download link
    so the gateway can emit an ``upload_complete`` event (the frontend renders
    a Download button).

    The agent provides Raw SQL only — the tool does the execution internally.
    Column names become the CSV header.

    Args:
        filename: Download filename, e.g. ``"registrations-2026.csv"``. The
            storage key is namespaced ``csv-exports/{uuid}/{filename}``;
            collisions are impossible.
        sql: SELECT to execute and export. Column names become the CSV header.
            Keep the result set reasonable (the presigned PUT has an 8h TTL but
            very large uploads hurt latency).

    Returns:
        ``"Upload complete. Download (valid 8h): https://..."`` on success.
        On storage-unreachable / PUT-failure, an error string describing the
        failure (the agent should surface it, not retry silently).
    """
    from seeknal.ask.agents.tools._context import get_tool_context, record_tool_result
    from seeknal.ask.agents.tools.execute_sql import (
        _execute_oneshot_with_timeout,
        _repair_common_sql_before_execution,
    )

    ctx = get_tool_context()

    # STEP 0: PREPARE SQL — same pipeline as execute_sql (r2)
    sql = str(sql).strip().rstrip(";").strip()
    sql, _notices = _repair_common_sql_before_execution(sql)

    # STEP 1: EXECUTE the caller's SQL (tool does SQL internally — agent
    # provides Raw SQL only).
    columns, rows = _execute_oneshot_with_timeout(ctx, sql, limit=ctx.request_limit)

    # STEP 2: BUILD CSV (header from columns; no metadata rows mixed in).
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")
    if not rows:
        record_tool_result("upload_to_s3", "empty", args={"filename": filename})
        return "No rows to export."

    # STEP 3: presigned URLs + server-computed expiry (W13-B update, PR #154).
    # NOTE: iba-storage mounts the internal router under ``/api`` (main.py:
    # ``app.include_router(internal_router, prefix="/api")``), so the full path
    # is ``/api/v1/internal/get-upload-url`` — not ``/v1/...``.
    try:
        resp = httpx.post(
            f"{_IBA_STORAGE_URL}/api/v1/internal/get-upload-url",
            json={"filename": filename, "content_type": "text/csv"},
            headers={"X-API-Key": _IBA_STORAGE_API_KEY},
            timeout=30.0,
        )
        resp.raise_for_status()
        urls = resp.json()
    except httpx.RequestError:
        record_tool_result("upload_to_s3", "error", args={"filename": filename})
        return "Storage unavailable (timeout or connection failed)."
    except httpx.HTTPStatusError as exc:
        record_tool_result("upload_to_s3", "error", args={"filename": filename})
        return f"Failed to obtain presigned URL: HTTP {exc.response.status_code}."

    # STEP 4: PUT the CSV to SeaweedFS via the presigned URL (do this promptly
    # — the upload_url expires at urls["upload_url_expires_at"]).
    try:
        put = httpx.put(
            urls["upload_url"],
            content=csv_bytes,
            headers={"Content-Type": "text/csv"},
            timeout=60.0,
        )
        put.raise_for_status()
    except httpx.HTTPError:
        record_tool_result("upload_to_s3", "error", args={"filename": filename})
        return "CSV upload failed (SeaweedFS rejected the write)."

    # STEP 5: register pending_upload — gateway loop emits upload_complete.
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

    record_tool_result("upload_to_s3", urls.get("download_url", ""), args={"filename": filename})
    return f"Upload complete. Download (valid 8h): {urls.get('download_url', '')}"

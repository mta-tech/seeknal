"""Read tabular tool — parse a file or direct-download URL into a DuckDB preview.

Supports .xlsx, .csv, .tsv, and tabular .json files. For URLs, downloads
the file first via HTTP GET (no auth, no scraping). Stashes the resolved
source path on the ToolContext so write_ingested_table can pick it up.
"""

from __future__ import annotations

import ipaddress
import socket
import uuid
from pathlib import Path
from urllib.parse import urlparse

_MAX_FILE_BYTES = 200 * 1024 * 1024  # 200 MB
_SUPPORTED_SUFFIXES = {".csv", ".tsv", ".json", ".xlsx", ".parquet"}


def read_tabular(path_or_url: str, sample_rows: int = 20) -> str:
    """Read a tabular file or direct-download URL and return schema + sample rows.

    Supports .xlsx, .csv, .tsv, and tabular .json files. For URLs, downloads
    the file first via HTTP GET (no auth, no scraping).

    Returns a formatted summary: inferred column names, types, row count,
    and a sample preview table. Use this to inspect data before ingestion.

    Args:
        path_or_url: Absolute file path or direct-download HTTP(S) URL.
        sample_rows: Number of sample rows to include in preview (default 20).
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    try:
        sample_rows = max(1, min(int(sample_rows), 200))
    except (TypeError, ValueError):
        sample_rows = 20

    try:
        source_path = _resolve_source(path_or_url, ctx.project_path)
    except ValueError as exc:
        return f"Error: {exc}"

    suffix = source_path.suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        return (
            f"Error: unsupported file format '{suffix}'. "
            f"Supported: {', '.join(sorted(_SUPPORTED_SUFFIXES))}."
        )

    try:
        size = source_path.stat().st_size
    except OSError as exc:
        return f"Error: cannot access file: {exc}"
    if size > _MAX_FILE_BYTES:
        return (
            f"Error: file is {size / 1024 / 1024:.1f} MB, exceeds 200 MB limit."
        )

    # For xlsx, convert to parquet via pandas so DuckDB can read it uniformly.
    read_path = source_path
    if suffix == ".xlsx":
        try:
            read_path = _xlsx_to_parquet(source_path, ctx.project_path)
        except Exception as exc:
            return f"Error parsing xlsx: {exc}"

    try:
        schema_rows, row_count, preview_rows = _inspect(read_path, sample_rows)
    except Exception as exc:
        return f"Error reading '{source_path.name}': {exc}"

    # Stash the path downstream tools should read from. For xlsx inputs this
    # is the converted parquet under the staging dir; for everything else it
    # is the original path. Using the DuckDB-readable path means the agent
    # does not have to pass anything explicitly to write_ingested_table.
    ctx.last_read_staging_path = str(read_path)

    return _format_report(
        source_path=source_path,
        read_path=read_path,
        schema_rows=schema_rows,
        row_count=row_count,
        preview_rows=preview_rows,
    )


def _resolve_source(path_or_url: str, project_path: Path) -> Path:
    """Resolve an input string to a local Path.

    For URLs: download to target/ask_ingest/_staging/{uuid}/ and return that path.
    For file paths: validate existence and return the resolved Path.
    """
    parsed = urlparse(path_or_url)
    if parsed.scheme in {"http", "https"}:
        return _download(path_or_url, project_path)

    # File path. Accept absolute or relative.
    candidate = Path(path_or_url).expanduser()
    if not candidate.is_absolute():
        candidate = (project_path / candidate).resolve()
    else:
        candidate = candidate.resolve()

    if not candidate.exists():
        raise ValueError(f"file not found: {path_or_url}")
    if not candidate.is_file():
        raise ValueError(f"not a regular file: {path_or_url}")
    return candidate


def _reject_if_private(hostname: str) -> None:
    """Raise ValueError if hostname resolves to a loopback/private/link-local IP.

    Prevents SSRF to cloud metadata endpoints (169.254.169.254), localhost
    services, and RFC1918 internal networks.
    """
    if not hostname:
        raise ValueError("URL has no hostname")
    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as exc:
        raise ValueError(f"DNS lookup failed for {hostname}: {exc}")
    for info in infos:
        ip = info[4][0]
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            continue
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
            raise ValueError(
                f"URL host {hostname!r} resolves to a non-public address "
                f"({addr}); refusing to fetch."
            )


def _download(url: str, project_path: Path) -> Path:
    """Download a direct URL to the staging directory. Cap at 200 MB.

    Blocks SSRF to private/loopback/link-local networks (including cloud
    metadata endpoints like 169.254.169.254) and disables redirect-following
    so a public URL cannot redirect into an internal host.
    """
    import urllib.request

    parsed = urlparse(url)
    _reject_if_private(parsed.hostname or "")

    staging_root = project_path / "target" / "ask_ingest" / "_staging" / uuid.uuid4().hex[:12]
    staging_root.mkdir(parents=True, exist_ok=True)

    # Best-effort: pull a filename out of the URL path.
    suffix = Path(parsed.path).suffix.lower()
    base_name = Path(parsed.path).name or "download"
    if not suffix:
        base_name += ".csv"  # Last-resort default
    target = staging_root / base_name

    # No-redirect opener: any 3xx response raises rather than silently jumping
    # to a different host (which would bypass our private-IP check).
    class _DenyRedirect(urllib.request.HTTPRedirectHandler):
        def http_error_302(self, req, fp, code, msg, headers):  # noqa: D401
            raise ValueError(f"refusing to follow HTTP {code} redirect for {url}")
        http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302

    opener = urllib.request.build_opener(_DenyRedirect())
    req = urllib.request.Request(url, headers={"User-Agent": "seeknal-ask/1.0"})
    with opener.open(req, timeout=60) as resp:  # noqa: S310 — direct URL only, SSRF-guarded
        # Honour Content-Disposition if present for a cleaner suffix. Apply
        # Path(...).name to strip any traversal an attacker-controlled server
        # tries to smuggle in (e.g. filename="../../.env").
        disp = resp.headers.get("Content-Disposition", "")
        if "filename=" in disp:
            raw_fname = disp.split("filename=", 1)[1].strip().strip('"').strip("'")
            safe_fname = Path(raw_fname).name if raw_fname else ""
            if safe_fname:
                fname_suffix = Path(safe_fname).suffix.lower()
                if fname_suffix in _SUPPORTED_SUFFIXES:
                    target = staging_root / safe_fname
        total = 0
        with target.open("wb") as fh:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > _MAX_FILE_BYTES:
                    fh.write(b"")  # best-effort flush
                    target.unlink(missing_ok=True)
                    raise ValueError(
                        f"download exceeded 200 MB limit ({total / 1024 / 1024:.1f} MB)"
                    )
                fh.write(chunk)

    if not target.exists():
        raise ValueError("download failed: no bytes written")

    return target


def _xlsx_to_parquet(xlsx_path: Path, project_path: Path) -> Path:
    """Convert an xlsx file to a parquet in the staging directory."""
    import pandas as pd

    staging_root = project_path / "target" / "ask_ingest" / "_staging" / uuid.uuid4().hex[:12]
    staging_root.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(xlsx_path, engine="openpyxl")
    parquet_path = staging_root / f"{xlsx_path.stem}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def _inspect(path: Path, sample_rows: int) -> tuple[list[tuple[str, str, int]], int, list[tuple]]:
    """Return (schema_rows, row_count, preview_rows) for a file DuckDB can read."""
    import duckdb

    safe_path = str(path.resolve()).replace("'", "''")
    reader = _reader_expr(path, safe_path)

    con = duckdb.connect(":memory:")
    try:
        desc = con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
        schema_cols = [(row[0], row[1]) for row in desc]

        (row_count,) = con.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()

        # Per-column null counts (one aggregate query covers them all).
        null_exprs = ", ".join(
            f'SUM(CASE WHEN "{_quote(name)}" IS NULL THEN 1 ELSE 0 END) AS "nc_{idx}"'
            for idx, (name, _) in enumerate(schema_cols)
        )
        null_counts: list[int] = []
        if null_exprs:
            row = con.execute(f"SELECT {null_exprs} FROM {reader}").fetchone()
            null_counts = [int(v) if v is not None else 0 for v in row]

        preview = con.execute(
            f"SELECT * FROM {reader} LIMIT {int(sample_rows)}"
        ).fetchall()
    finally:
        con.close()

    schema_rows = [
        (name, ctype, null_counts[idx] if idx < len(null_counts) else 0)
        for idx, (name, ctype) in enumerate(schema_cols)
    ]
    return schema_rows, int(row_count), preview


def _reader_expr(path: Path, safe_path: str) -> str:
    """Return a DuckDB table expression for the given file path."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return f"read_csv_auto('{safe_path}')"
    if suffix == ".tsv":
        return f"read_csv_auto('{safe_path}', delim='\\t')"
    if suffix == ".json":
        return f"read_json_auto('{safe_path}')"
    if suffix == ".parquet":
        return f"read_parquet('{safe_path}')"
    raise ValueError(f"unsupported suffix: {suffix}")


def _quote(name: str) -> str:
    return name.replace('"', '""')


def _format_report(
    source_path: Path,
    read_path: Path,
    schema_rows: list[tuple[str, str, int]],
    row_count: int,
    preview_rows: list[tuple],
) -> str:
    lines: list[str] = []
    lines.append(f"## Preview: {source_path.name}")
    lines.append("")
    lines.append(f"- **Source path:** `{source_path}`")
    if read_path != source_path:
        lines.append(f"- **Parsed as parquet at:** `{read_path}`")
    lines.append(f"- **Rows:** {row_count:,}")
    lines.append(f"- **Columns:** {len(schema_rows)}")
    lines.append("")
    lines.append("### Schema")
    lines.append("| Column | Type | Nulls |")
    lines.append("| --- | --- | --- |")
    for name, ctype, nulls in schema_rows:
        lines.append(f"| `{name}` | {ctype} | {nulls} |")
    lines.append("")

    if preview_rows:
        header = "| " + " | ".join(name for name, _, _ in schema_rows) + " |"
        sep = "| " + " | ".join("---" for _ in schema_rows) + " |"
        body = []
        for row in preview_rows:
            cells = [str(v) if v is not None else "NULL" for v in row]
            body.append("| " + " | ".join(cells) + " |")
        lines.append("### Sample rows")
        lines.append(header)
        lines.append(sep)
        lines.extend(body)
        lines.append("")

    lines.append(
        "Next: call `write_ingested_table(source_path=..., table_name=..., "
        "business_key=..., mode='create')` after the user confirms the table "
        "name and business key."
    )
    return "\n".join(lines)

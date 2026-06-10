"""Deterministic ingestion service for the gateway one-shot channels.

The conversational `data-ingest` skill relies on interactive `ask_user`, which
does not work over the gateway's one-shot `/ask` (Telegram / web). This module
exposes the SAME create/append/dedup engine (`write_ingested_table`) behind two
deterministic endpoints — `propose` and `commit` — so an owner can upload a file,
see a structured proposal, and approve with one tap.

Pipeline: stage raw → **normalize headers** to valid snake_case identifiers (so a
column like ``No. Pesanan`` can be a business key) → classify a table name →
infer a business key → compute a create-vs-append plan (with dup/drift counts).
On commit, the staged normalized parquet is handed to the tested
`write_ingested_table(user_confirmed=True, ...)`, which writes
``target/ask_ingest/{table}.parquet`` + provenance and is auto-registered as the
`ingest_{table}` view on the next agent turn (REPL._register_ask_ingest).

Nothing here talks to the LLM — it is deterministic and reusable.
"""

from __future__ import annotations

import base64
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_COLUMN_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SUPPORTED_SUFFIXES = {".csv", ".tsv", ".json", ".xlsx", ".parquet"}
_ID_HINTS = (
    "order_id", "no_pesanan", "nomor_pesanan", "no_invoice", "nomor_invoice",
    "invoice", "no_resi", "pesanan", "order_sn", "order", "transaction", "trx",
    "sku", "id",
)
# Header signatures (normalized) that map an upload onto a canonical entity. The
# owner can always override the proposed table name before committing.
_MARKETPLACE_SIGNATURE = {"no_pesanan", "status_pesanan", "total_pembayaran"}


def _duck():
    """A fresh in-memory DuckDB with extension autoload disabled, so even a future
    SQL-injection regression cannot reach httpfs (no SSRF / network exfil). Core
    CSV/JSON/Parquet readers are statically linked and unaffected."""
    import duckdb

    con = duckdb.connect(":memory:")
    for pragma in (
        "SET autoinstall_known_extensions=false",
        "SET autoload_known_extensions=false",
    ):
        try:
            con.execute(pragma)
        except Exception:
            pass
    return con


# ---------------------------------------------------------------------------
# Header normalization
# ---------------------------------------------------------------------------


def normalize_header(name: str, used: set[str]) -> str:
    """Map an arbitrary column header to a unique snake_case SQL identifier."""
    s = re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"_{s}"
    base, i, out = s, 2, s
    while out in used:
        out = f"{base}_{i}"
        i += 1
    used.add(out)
    return out


def sanitize_table_name(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")
    s = re.sub(r"_(csv|tsv|json|xlsx|xls|parquet)$", "", s)
    if not s:
        s = "uploaded_data"
    if s[0].isdigit():
        s = f"data_{s}"
    return s[:48]


def _reader_expr(path: Path) -> str:
    safe = str(path.resolve()).replace("'", "''")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return f"read_csv_auto('{safe}', all_varchar=false)"
    if suffix == ".tsv":
        return f"read_csv_auto('{safe}', delim='\\t')"
    if suffix == ".json":
        return f"read_json_auto('{safe}')"
    if suffix == ".parquet":
        return f"read_parquet('{safe}')"
    raise ValueError(f"unsupported suffix: {suffix}")


def _xlsx_to_csv(xlsx_path: Path) -> Path:
    import pandas as pd

    df = pd.read_excel(xlsx_path, engine="openpyxl")
    out = xlsx_path.with_suffix(".csv")
    df.to_csv(out, index=False)
    return out


def normalize_to_parquet(raw_path: Path, out_path: Path) -> tuple[dict[str, str], list[str], int]:
    """Read raw → write a parquet with normalized headers. Returns (mapping, cols, rows)."""
    import duckdb

    if raw_path.suffix.lower() == ".xlsx":
        raw_path = _xlsx_to_csv(raw_path)

    con = _duck()
    try:
        reader = _reader_expr(raw_path)
        original = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()]
        if not original:
            raise ValueError("file has no columns")
        used: set[str] = set()
        mapping: dict[str, str] = {}
        selects: list[str] = []
        for col in original:
            clean = normalize_header(col, used)
            mapping[col] = clean
            safe_col = col.replace('"', '""')
            selects.append(f'"{safe_col}" AS "{clean}"')
        safe_out = str(out_path.resolve()).replace("'", "''")
        con.execute(
            f"COPY (SELECT {', '.join(selects)} FROM {reader}) TO '{safe_out}' (FORMAT PARQUET)"
        )
        (rows,) = con.execute(f"SELECT COUNT(*) FROM read_parquet('{safe_out}')").fetchone()
    finally:
        con.close()
    return mapping, list(mapping.values()), int(rows)


def infer_business_key(parquet_path: Path, columns: list[str]) -> str:
    """Pick a business key: a unique id-like column, else most-unique id-like, else widest."""
    import duckdb

    con = _duck()
    safe = str(parquet_path.resolve()).replace("'", "''")
    try:
        (total,) = con.execute(f"SELECT COUNT(*) FROM read_parquet('{safe}')").fetchone()
        cards: list[tuple[str, int]] = []
        for c in columns:
            (u,) = con.execute(
                f'SELECT COUNT(DISTINCT "{c}") FROM read_parquet(\'{safe}\')'
            ).fetchone()
            cards.append((c, int(u)))
    finally:
        con.close()

    unique_cols = [c for c, u in cards if total > 0 and u == total]
    for c in unique_cols:
        if any(h in c for h in _ID_HINTS):
            return c
    if unique_cols:
        return unique_cols[0]
    id_like = [(c, u) for c, u in cards if any(h in c for h in _ID_HINTS)]
    if id_like:
        return max(id_like, key=lambda x: x[1])[0]
    return max(cards, key=lambda x: x[1])[0] if cards else columns[0]


def classify_table_name(columns: list[str], filename: str) -> str:
    cset = set(columns)
    if len(_MARKETPLACE_SIGNATURE & cset) >= 2:
        return "marketplace"
    if {"nomor_invoice", "nomor_pesanan", "order_sn"} & cset:
        return "marketplace"
    return sanitize_table_name(Path(filename).stem)


def _compute_append_plan(new_parquet: Path, existing_parquet: Path, business_key: str) -> dict[str, Any]:
    import duckdb

    con = _duck()
    nn = str(new_parquet.resolve()).replace("'", "''")
    ee = str(existing_parquet.resolve()).replace("'", "''")
    try:
        new_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{nn}')").fetchall()]
        ex_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{ee}')").fetchall()]
        (new_rows,) = con.execute(f"SELECT COUNT(*) FROM read_parquet('{nn}')").fetchone()
        (ex_rows,) = con.execute(f"SELECT COUNT(*) FROM read_parquet('{ee}')").fetchone()
        bk = [c.strip() for c in business_key.split(",") if c.strip()]
        missing_bk = [c for c in bk if c not in new_cols or c not in ex_cols]
        dup = 0
        if not missing_bk:
            sel = ", ".join('"' + c.replace('"', '""') + '"' for c in bk)
            (dup,) = con.execute(
                f"SELECT COUNT(*) FROM (SELECT {sel} FROM read_parquet('{nn}') "
                f"INTERSECT SELECT {sel} FROM read_parquet('{ee}'))"
            ).fetchone()
    finally:
        con.close()
    new_rows, ex_rows, dup = int(new_rows), int(ex_rows), int(dup)
    return {
        "new_rows": new_rows,
        "existing_rows": ex_rows,
        "duplicate_count": dup,
        "new_unique": new_rows - dup,
        "missing_in_new": [c for c in ex_cols if c not in new_cols],
        "extra_in_new": [c for c in new_cols if c not in ex_cols],
        "missing_business_key": missing_bk,
    }


# ---------------------------------------------------------------------------
# Public: propose / commit
# ---------------------------------------------------------------------------


def _staging_dir(project_path: Path, token: str) -> Path:
    return project_path / "target" / "ask_ingest" / "_staging" / f"propose-{token}"


def _sweep_old_staging(project_path: Path, max_age_seconds: int = 3600) -> None:
    """Delete abandoned propose-* staging dirs (a proposal the owner never committed)."""
    import time

    base = project_path / "target" / "ask_ingest" / "_staging"
    if not base.exists():
        return
    now = time.time()
    for d in base.glob("propose-*"):
        try:
            if d.is_dir() and (now - d.stat().st_mtime) > max_age_seconds:
                shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass


def propose(
    project_path: Path,
    *,
    filename: str,
    data_base64: str | None = None,
    source_path: str | None = None,
    table_name: str | None = None,
    business_key: str | None = None,
) -> dict[str, Any]:
    """Stage + normalize an upload and return a create/append proposal (no write)."""
    project_path = Path(project_path)
    _sweep_old_staging(project_path)
    suffix = Path(filename or "upload.csv").suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        return {"ok": False, "error": f"unsupported file type '{suffix}'"}

    token = uuid.uuid4().hex[:12]
    staging = _staging_dir(project_path, token)
    staging.mkdir(parents=True, exist_ok=True)
    raw = staging / f"raw{suffix}"
    try:
        if data_base64:
            raw.write_bytes(base64.b64decode(data_base64))
        elif source_path:
            shutil.copy(Path(source_path).expanduser(), raw)
        else:
            return {"ok": False, "error": "no data_base64 or source_path provided"}

        norm = staging / "normalized.parquet"
        mapping, columns, row_count = normalize_to_parquet(raw, norm)
    except Exception as exc:  # noqa: BLE001 — surface parse errors to the owner
        shutil.rmtree(staging, ignore_errors=True)
        return {"ok": False, "error": f"could not read file: {exc}"}

    tbl = sanitize_table_name(table_name) if table_name else classify_table_name(columns, filename)
    bk = business_key.strip() if business_key else infer_business_key(norm, columns)

    # A business key must be normalized identifier(s) — it is interpolated into SQL
    # (quoted) downstream, so reject anything outside [a-zA-Z_][a-zA-Z0-9_]*.
    bk_cols = [c.strip() for c in bk.split(",") if c.strip()]
    if not bk_cols or not all(_COLUMN_NAME_RE.match(c) for c in bk_cols):
        shutil.rmtree(staging, ignore_errors=True)
        return {"ok": False, "error": f"invalid business key '{bk}'"}
    if any(c not in columns for c in bk_cols):
        shutil.rmtree(staging, ignore_errors=True)
        return {"ok": False, "error": f"business key not found in file: '{bk}'"}

    existing = project_path / "target" / "ask_ingest" / f"{tbl}.parquet"
    if existing.exists():
        mode = "append"
        plan = _compute_append_plan(norm, existing, bk)
    else:
        mode = "create"
        plan = {"new_rows": row_count, "new_unique": row_count, "duplicate_count": 0}

    return {
        "ok": True,
        "token": token,
        "table_name": tbl,
        "view_name": f"ingest_{tbl}",
        "mode": mode,
        "business_key": bk,
        "columns": columns,
        "column_mapping": mapping,
        "row_count": row_count,
        "plan": plan,
    }


def commit(
    project_path: Path,
    *,
    token: str,
    table_name: str,
    business_key: str,
    mode: str = "create",
    dedup_strategy: str = "skip",
) -> dict[str, Any]:
    """Persist the staged proposal via the tested write_ingested_table engine."""
    project_path = Path(project_path)
    if not token or not re.fullmatch(r"[a-f0-9]{6,32}", str(token)):
        return {"ok": False, "error": "invalid token"}
    table_name = sanitize_table_name(table_name)
    if not _TABLE_NAME_RE.match(table_name):
        return {"ok": False, "error": "invalid table_name"}
    bk_cols = [c.strip() for c in business_key.split(",") if c.strip()]
    if not bk_cols or not all(_COLUMN_NAME_RE.match(c) for c in bk_cols):
        return {"ok": False, "error": "invalid business key"}

    norm = _staging_dir(project_path, token) / "normalized.parquet"
    if not norm.exists():
        return {"ok": False, "error": "staged file expired — please upload again"}

    from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
    from seeknal.ask.agents.tools.write_ingested_table import write_ingested_table

    set_tool_context(ToolContext(repl=None, artifact_discovery=None, project_path=project_path))  # type: ignore[arg-type]
    result = write_ingested_table(
        source_path=str(norm),
        table_name=table_name,
        business_key=business_key,
        mode=mode if mode in {"create", "append"} else "create",
        user_confirmed=True,
        dedup_strategy=dedup_strategy if dedup_strategy in {"skip", "replace"} else "skip",
    )
    ok = not result.lstrip().startswith("Error")
    if ok:
        shutil.rmtree(norm.parent, ignore_errors=True)
    return {"ok": ok, "result": result, "view_name": f"ingest_{table_name}"}

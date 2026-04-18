"""Write ingested table tool — persist tabular data as Parquet + DuckDB view.

Self-defending: in append mode, runs deterministic drift/dedup SQL internally
and requires ``user_confirmed=True`` to actually write. When called with
``user_confirmed=False``, returns a markdown drift report without touching
the parquet on disk.

Emits a provenance JSON sidecar next to every successful write.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Literal

_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_COLUMN_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SUPPORTED_SUFFIXES = {".csv", ".tsv", ".json", ".xlsx", ".parquet"}


def write_ingested_table(
    source_path: str,
    table_name: str,
    business_key: str,
    mode: str = "create",
    user_confirmed: bool = False,
    dedup_strategy: Literal["skip", "replace"] = "skip",
) -> str:
    """Write tabular data to a persistent DuckDB table (Parquet on disk).

    Reads the source file, writes to {project}/target/ask_ingest/{table_name}.parquet,
    and registers it as a DuckDB view in the current session.

    For 'append' mode with user_confirmed=False: runs drift/dedup checks internally
    (deterministic SQL, not LLM) and returns a structured drift report WITHOUT writing.
    The agent must present this report to the user via ask_user, then call again with
    user_confirmed=True to proceed.

    For 'append' mode with user_confirmed=True: performs the write with dedup.

    dedup_strategy controls duplicate handling:
      - 'skip': keep existing rows (new rows with matching business keys are dropped).
      - 'replace': keep new rows (existing rows with matching keys are dropped).

    Also emits a provenance JSON sidecar at
    target/ask_ingest/{table_name}_provenance.json.

    Args:
        source_path: Path to the source file (as returned by read_tabular).
        table_name: Target table name (alphanumeric + underscores).
        business_key: Comma-separated column names forming the business key for dedup.
        mode: 'create' for new table, 'append' for append+dedup to existing.
        user_confirmed: Must be True for append mode to actually write. If False,
            returns drift report only.
        dedup_strategy: 'skip' (default) keeps existing rows, 'replace' keeps new rows.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.agents.tools._write_security import validate_write_path

    ctx = get_tool_context()

    if mode not in {"create", "append"}:
        return f"Error: mode must be 'create' or 'append', got '{mode}'."
    if dedup_strategy not in {"skip", "replace"}:
        return (
            f"Error: dedup_strategy must be 'skip' or 'replace', got '{dedup_strategy}'."
        )
    if not _TABLE_NAME_RE.match(table_name):
        return (
            f"Error: invalid table_name '{table_name}'. "
            "Must match [a-zA-Z_][a-zA-Z0-9_]*."
        )

    business_key_cols = [col.strip() for col in business_key.split(",") if col.strip()]
    if not business_key_cols:
        return "Error: business_key must name at least one column."
    for col in business_key_cols:
        if not _COLUMN_NAME_RE.match(col):
            return (
                f"Error: invalid business key column '{col}'. "
                "Must match [a-zA-Z_][a-zA-Z0-9_]*."
            )

    # Resolve source path: explicit arg wins; otherwise fall back to the
    # staged path stashed by read_tabular.
    resolved_source = _resolve_source_path(source_path, ctx)
    if resolved_source is None:
        return (
            "Error: source file not found. Pass an absolute path or call "
            "read_tabular first."
        )

    suffix = resolved_source.suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        return f"Error: unsupported source suffix '{suffix}'."

    # Validate output paths through the write security layer.
    rel_parquet = f"target/ask_ingest/{table_name}.parquet"
    rel_prov = f"target/ask_ingest/{table_name}_provenance.json"
    try:
        parquet_path = validate_write_path(rel_parquet, ctx.project_path)
        prov_path = validate_write_path(rel_prov, ctx.project_path)
    except ValueError as exc:
        return f"Error: {exc}"

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # Existing-table check.
    existing_present = parquet_path.exists()

    if mode == "create":
        if existing_present:
            return (
                f"Error: table '{table_name}' already exists at {parquet_path}. "
                "Use mode='append' to add new rows."
            )
        return _do_create(
            resolved_source=resolved_source,
            parquet_path=parquet_path,
            prov_path=prov_path,
            table_name=table_name,
            business_key_cols=business_key_cols,
            ctx=ctx,
        )

    # mode == 'append'
    if not existing_present:
        return (
            f"Error: no existing table '{table_name}' at {parquet_path}. "
            "Call with mode='create' for the first ingestion."
        )

    drift = _compute_drift(
        source_path=resolved_source,
        existing_parquet=parquet_path,
        business_key_cols=business_key_cols,
    )

    if not user_confirmed:
        return _format_drift_report(
            table_name=table_name,
            drift=drift,
            business_key_cols=business_key_cols,
            duplicate_count=drift["duplicate_count"],
        )

    return _do_append(
        resolved_source=resolved_source,
        parquet_path=parquet_path,
        prov_path=prov_path,
        table_name=table_name,
        business_key_cols=business_key_cols,
        dedup_strategy=dedup_strategy,
        drift=drift,
        ctx=ctx,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_source_path(source_path: str, ctx) -> Path | None:
    """Resolve an input path to a DuckDB-readable file.

    xlsx inputs are converted to a staging parquet on the fly (matching
    ``read_tabular``'s behavior) so the agent can call this tool directly
    on an .xlsx upload without first calling read_tabular.
    """
    candidate_raw = source_path or getattr(ctx, "last_read_staging_path", None) or ""
    if not candidate_raw:
        return None
    candidate = Path(candidate_raw).expanduser()
    if not candidate.is_absolute():
        candidate = (ctx.project_path / candidate).resolve()
    else:
        candidate = candidate.resolve()
    if not candidate.exists() or not candidate.is_file():
        return None

    if candidate.suffix.lower() == ".xlsx":
        try:
            candidate = _xlsx_to_parquet(candidate, ctx.project_path)
        except Exception:
            return None
        # Cache so subsequent calls (e.g. an append after a create) reuse the
        # converted parquet rather than re-running pandas each turn.
        try:
            ctx.last_read_staging_path = str(candidate)
        except Exception:
            pass

    return candidate


def _xlsx_to_parquet(xlsx_path: Path, project_path: Path) -> Path:
    """Convert an xlsx file to a parquet under target/ask_ingest/_staging/."""
    import pandas as pd

    staging_root = (
        project_path / "target" / "ask_ingest" / "_staging" / uuid.uuid4().hex[:12]
    )
    staging_root.mkdir(parents=True, exist_ok=True)
    df = pd.read_excel(xlsx_path, engine="openpyxl")
    parquet_path = staging_root / f"{xlsx_path.stem}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def _reader_expr(path: Path) -> str:
    safe = str(path.resolve()).replace("'", "''")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return f"read_csv_auto('{safe}')"
    if suffix == ".tsv":
        return f"read_csv_auto('{safe}', delim='\\t')"
    if suffix == ".json":
        return f"read_json_auto('{safe}')"
    if suffix == ".parquet":
        return f"read_parquet('{safe}')"
    if suffix == ".xlsx":
        raise ValueError("xlsx must be converted to parquet by read_tabular first")
    raise ValueError(f"unsupported suffix: {suffix}")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _now_iso() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat(timespec="seconds")


def _describe(con, reader: str) -> list[tuple[str, str]]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
    return [(r[0], r[1]) for r in rows]


def _register_view(ctx, table_name: str, parquet_path: Path) -> None:
    safe = str(parquet_path.resolve()).replace("'", "''")
    view_name = f"ingest_{table_name}"
    try:
        with ctx.db_lock:
            ctx.repl.conn.execute(
                f'CREATE OR REPLACE VIEW "{view_name}" AS '
                f"SELECT * FROM read_parquet('{safe}')"
            )
    except Exception:
        # Best-effort: view registration should not block a successful write.
        pass


def _write_provenance(
    prov_path: Path,
    source_path: Path,
    rows_before: int,
    rows_after: int,
    rows_inserted: int,
    rows_deduped: int,
    drift_decisions: list[dict],
) -> None:
    payload = {
        "source_file": str(source_path),
        "source_file_sha256": _sha256(source_path),
        "ingested_at": _now_iso(),
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_inserted": rows_inserted,
        "rows_deduped": rows_deduped,
        "drift_decisions": drift_decisions,
    }
    prov_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Create mode
# ---------------------------------------------------------------------------


def _do_create(
    resolved_source: Path,
    parquet_path: Path,
    prov_path: Path,
    table_name: str,
    business_key_cols: list[str],
    ctx,
) -> str:
    import duckdb

    reader = _reader_expr(resolved_source)
    safe_out = str(parquet_path.resolve()).replace("'", "''")

    con = duckdb.connect(":memory:")
    try:
        # Sanity check business key columns exist.
        schema = _describe(con, reader)
        col_names = {name for name, _ in schema}
        missing = [c for c in business_key_cols if c not in col_names]
        if missing:
            return (
                f"Error: business_key columns not present in source: "
                f"{', '.join(missing)}. Available columns: {', '.join(sorted(col_names))}."
            )

        (row_count,) = con.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()
        # fs_lock serialises the parquet + provenance write against other
        # filesystem-writing tools (draft_node, apply_draft, save_ingestion_skill).
        with ctx.fs_lock:
            con.execute(
                f"COPY (SELECT * FROM {reader}) TO '{safe_out}' (FORMAT PARQUET)"
            )
            _write_provenance(
                prov_path=prov_path,
                source_path=resolved_source,
                rows_before=0,
                rows_after=int(row_count),
                rows_inserted=int(row_count),
                rows_deduped=0,
                drift_decisions=[
                    {"kind": "create", "action": "new_table", "rows": int(row_count)}
                ],
            )
    except Exception as exc:
        return f"Error writing parquet: {exc}"
    finally:
        con.close()

    _register_view(ctx, table_name, parquet_path)

    return (
        f"Created table `ingest_{table_name}` at {parquet_path}.\n"
        f"- Rows inserted: {row_count:,}\n"
        f"- Business key: {', '.join(business_key_cols)}\n"
        f"- Provenance: {prov_path}\n\n"
        "You can now query it via `execute_sql('SELECT ... FROM "
        f"ingest_{table_name}')`."
    )


# ---------------------------------------------------------------------------
# Append mode — drift + dedup
# ---------------------------------------------------------------------------


def _compute_drift(
    source_path: Path,
    existing_parquet: Path,
    business_key_cols: list[str],
) -> dict:
    """Return a dict describing schema diff + duplicate-key count."""
    import duckdb

    new_reader = _reader_expr(source_path)
    safe_existing = str(existing_parquet.resolve()).replace("'", "''")
    existing_reader = f"read_parquet('{safe_existing}')"

    result: dict = {
        "schema_new": [],
        "schema_existing": [],
        "missing_in_new": [],  # cols in existing but not in new
        "extra_in_new": [],    # cols in new but not in existing
        "type_mismatches": [],  # list of (col, existing_type, new_type)
        "missing_bk_in_new": [],
        "missing_bk_in_existing": [],
        "duplicate_count": 0,
        "new_row_count": 0,
        "existing_row_count": 0,
    }

    con = duckdb.connect(":memory:")
    try:
        schema_new = _describe(con, new_reader)
        schema_existing = _describe(con, existing_reader)
        result["schema_new"] = schema_new
        result["schema_existing"] = schema_existing

        new_map = dict(schema_new)
        existing_map = dict(schema_existing)

        result["missing_in_new"] = [c for c in existing_map if c not in new_map]
        result["extra_in_new"] = [c for c in new_map if c not in existing_map]

        for col in new_map:
            if col in existing_map and new_map[col] != existing_map[col]:
                result["type_mismatches"].append((col, existing_map[col], new_map[col]))

        result["missing_bk_in_new"] = [c for c in business_key_cols if c not in new_map]
        result["missing_bk_in_existing"] = [
            c for c in business_key_cols if c not in existing_map
        ]

        (result["new_row_count"],) = con.execute(
            f"SELECT COUNT(*) FROM {new_reader}"
        ).fetchone()
        (result["existing_row_count"],) = con.execute(
            f"SELECT COUNT(*) FROM {existing_reader}"
        ).fetchone()

        if not result["missing_bk_in_new"] and not result["missing_bk_in_existing"]:
            bk_select = ", ".join(f'"{c}"' for c in business_key_cols)
            dup_sql = (
                f"SELECT COUNT(*) FROM ("
                f"SELECT {bk_select} FROM {new_reader} "
                f"INTERSECT "
                f"SELECT {bk_select} FROM {existing_reader}"
                f")"
            )
            (result["duplicate_count"],) = con.execute(dup_sql).fetchone()
    finally:
        con.close()

    return result


def _format_drift_report(
    table_name: str,
    drift: dict,
    business_key_cols: list[str],
    duplicate_count: int,
) -> str:
    lines: list[str] = []
    lines.append(f"## Drift report for `ingest_{table_name}` (dry run)")
    lines.append("")
    lines.append(
        f"- New file rows: **{drift['new_row_count']:,}**"
    )
    lines.append(
        f"- Existing table rows: **{drift['existing_row_count']:,}**"
    )
    lines.append(f"- Business key: {', '.join(business_key_cols)}")
    lines.append(f"- Duplicate business-key rows: **{duplicate_count:,}**")
    lines.append("")

    if drift["missing_in_new"]:
        lines.append(
            "### Columns missing from new file\n"
            + "\n".join(f"- `{c}`" for c in drift["missing_in_new"])
        )
        lines.append("")
    if drift["extra_in_new"]:
        lines.append(
            "### New columns not in existing table\n"
            + "\n".join(f"- `{c}`" for c in drift["extra_in_new"])
        )
        lines.append("")
    if drift["type_mismatches"]:
        lines.append("### Type mismatches")
        for col, existing_t, new_t in drift["type_mismatches"]:
            lines.append(f"- `{col}`: existing `{existing_t}` vs new `{new_t}`")
        lines.append("")
    if drift["missing_bk_in_new"]:
        lines.append(
            "### Business key columns missing from new file\n"
            + "\n".join(f"- `{c}`" for c in drift["missing_bk_in_new"])
        )
        lines.append("")
    if drift["missing_bk_in_existing"]:
        lines.append(
            "### Business key columns missing from existing table\n"
            + "\n".join(f"- `{c}`" for c in drift["missing_bk_in_existing"])
        )
        lines.append("")

    if duplicate_count > 0:
        lines.append(
            f"**Recommendation:** {duplicate_count:,} row(s) already exist. "
            "I suggest **skipping duplicates** (keep existing) unless the "
            "new file is a corrected version, in which case choose "
            "**replace duplicates**."
        )
    elif drift["type_mismatches"] or drift["missing_in_new"] or drift["extra_in_new"]:
        lines.append(
            "**Recommendation:** schema drifted. Confirm the diff above "
            "before appending."
        )
    else:
        lines.append(
            "**Recommendation:** no duplicates or drift detected. Safe to "
            "append with `user_confirmed=True, dedup_strategy='skip'`."
        )
    lines.append("")
    lines.append(
        "**No data was written.** Present this report to the user via "
        "`ask_user` with options like "
        "`Skip duplicates (keep existing)`, `Replace duplicates (keep new)`, "
        "`Skip this file`, `Type your own`. After the user decides, call "
        "`write_ingested_table(mode='append', user_confirmed=True, "
        "dedup_strategy='skip'|'replace')`."
    )
    return "\n".join(lines)


def _do_append(
    resolved_source: Path,
    parquet_path: Path,
    prov_path: Path,
    table_name: str,
    business_key_cols: list[str],
    dedup_strategy: str,
    drift: dict,
    ctx,
) -> str:
    import duckdb

    if drift["missing_bk_in_new"] or drift["missing_bk_in_existing"]:
        missing = drift["missing_bk_in_new"] + drift["missing_bk_in_existing"]
        return (
            "Error: business_key columns missing from schema; cannot dedup: "
            f"{', '.join(sorted(set(missing)))}."
        )

    new_reader = _reader_expr(resolved_source)
    safe_existing = str(parquet_path.resolve()).replace("'", "''")
    existing_reader = f"read_parquet('{safe_existing}')"

    # Temporary parquet for atomic swap. Same directory so rename is atomic.
    tmp_path = parquet_path.with_name(
        f"{parquet_path.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"
    )
    safe_tmp = str(tmp_path.resolve()).replace("'", "''")

    # Choose union columns = intersection of both schemas (preserve existing order).
    existing_cols = [name for name, _ in drift["schema_existing"]]
    new_cols = {name for name, _ in drift["schema_new"]}
    common_cols = [c for c in existing_cols if c in new_cols]
    if not common_cols:
        return "Error: no shared columns between existing table and new file."

    col_select = ", ".join(f'"{c}"' for c in common_cols)
    bk_select = ", ".join(f'"{c}"' for c in business_key_cols)

    # Build a de-duplicated set using a rowid-like ordering. We tag each source
    # so we can prefer one side on collisions.
    if dedup_strategy == "skip":
        # Existing wins on collision: give existing rows priority 0, new 1.
        priority_existing = 0
        priority_new = 1
    else:
        # Replace: new wins.
        priority_existing = 1
        priority_new = 0

    combined_sql = f"""
        WITH combined AS (
            SELECT {col_select}, {priority_existing} AS _seek_priority
            FROM {existing_reader}
            UNION ALL
            SELECT {col_select}, {priority_new} AS _seek_priority
            FROM {new_reader}
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {bk_select}
                ORDER BY _seek_priority
            ) AS _seek_rn
            FROM combined
        )
        SELECT {col_select}
        FROM ranked
        WHERE _seek_rn = 1
    """

    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY ({combined_sql}) TO '{safe_tmp}' (FORMAT PARQUET)"
        )
        (rows_after,) = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{safe_tmp}')"
        ).fetchone()
    except Exception as exc:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        con.close()
        return f"Error during dedup write: {exc}"
    finally:
        con.close()

    rows_before = int(drift["existing_row_count"])
    new_rows = int(drift["new_row_count"])
    duplicate_count = int(drift["duplicate_count"])

    if dedup_strategy == "skip":
        rows_inserted = new_rows - duplicate_count
        rows_deduped = duplicate_count
    else:  # replace
        rows_inserted = new_rows
        rows_deduped = duplicate_count

    # Atomic swap + provenance write, serialised via fs_lock.
    drift_decisions = [
        {
            "kind": "append",
            "dedup_strategy": dedup_strategy,
            "duplicate_count": duplicate_count,
            "rows_before": rows_before,
            "rows_after": int(rows_after),
        }
    ]
    if drift["type_mismatches"]:
        drift_decisions.append(
            {
                "kind": "type_mismatch",
                "mismatches": [
                    {"column": col, "existing": e, "new": n}
                    for col, e, n in drift["type_mismatches"]
                ],
            }
        )
    if drift["missing_in_new"]:
        drift_decisions.append(
            {"kind": "missing_in_new", "columns": drift["missing_in_new"]}
        )
    if drift["extra_in_new"]:
        drift_decisions.append(
            {"kind": "extra_in_new", "columns": drift["extra_in_new"]}
        )

    try:
        with ctx.fs_lock:
            tmp_path.replace(parquet_path)
            _write_provenance(
                prov_path=prov_path,
                source_path=resolved_source,
                rows_before=rows_before,
                rows_after=int(rows_after),
                rows_inserted=rows_inserted,
                rows_deduped=rows_deduped,
                drift_decisions=drift_decisions,
            )
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        return f"Error finalising parquet: {exc}"

    _register_view(ctx, table_name, parquet_path)

    return (
        f"Appended to `ingest_{table_name}` at {parquet_path}.\n"
        f"- Dedup strategy: {dedup_strategy}\n"
        f"- Rows before: {rows_before:,}\n"
        f"- Rows after: {rows_after:,}\n"
        f"- Rows inserted from new file: {rows_inserted:,}\n"
        f"- Rows deduped: {rows_deduped:,}\n"
        f"- Provenance: {prov_path}"
    )

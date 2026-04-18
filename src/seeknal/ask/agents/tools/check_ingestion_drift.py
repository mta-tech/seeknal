"""Check ingestion drift tool — compare new file against an existing table.

Returns a structured markdown report of schema differences (missing/extra/
changed columns), duplicate business key count, and row count comparison.
The report ends with an explicit recommendation the agent can relay to
the user via ``ask_user``.

The write tool also runs its own internal drift check; this tool exists so
the SKILL.md prose can request a user-presentable report as a first pass.
"""

from __future__ import annotations

import re
from pathlib import Path

_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_COLUMN_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SUPPORTED_SUFFIXES = {".csv", ".tsv", ".json", ".parquet"}


def check_ingestion_drift(
    source_path: str,
    table_name: str,
    business_key: str,
) -> str:
    """Compare a new file's schema and data against an existing ingested table.

    Returns a structured report of: schema differences (missing/extra/changed columns),
    duplicate business key count, and row count comparison. Use this before
    write_ingested_table in append mode to inform the user.

    Args:
        source_path: Path to the new source file.
        table_name: Existing ingested table name (without 'ingest_' prefix).
        business_key: Comma-separated business key columns.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    if not _TABLE_NAME_RE.match(table_name):
        return f"Error: invalid table_name '{table_name}'."

    business_key_cols = [c.strip() for c in business_key.split(",") if c.strip()]
    if not business_key_cols:
        return "Error: business_key must name at least one column."
    for col in business_key_cols:
        if not _COLUMN_NAME_RE.match(col):
            return f"Error: invalid business key column '{col}'."

    candidate = _resolve_source(source_path, ctx)
    if candidate is None:
        return (
            "Error: source file not found. Pass an absolute path or call "
            "read_tabular first."
        )

    suffix = candidate.suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        return (
            f"Error: unsupported source suffix '{suffix}'. "
            "Use read_tabular to stage an xlsx file into parquet first."
        )

    existing_parquet = ctx.project_path / "target" / "ask_ingest" / f"{table_name}.parquet"
    if not existing_parquet.exists():
        return (
            f"No existing table `ingest_{table_name}` found at {existing_parquet}.\n\n"
            "This is a first ingestion. Call `write_ingested_table(mode='create')` "
            "to create the table."
        )

    return _report(
        candidate=candidate,
        existing_parquet=existing_parquet,
        table_name=table_name,
        business_key_cols=business_key_cols,
    )


def _resolve_source(source_path: str, ctx) -> Path | None:
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
    return candidate


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
    raise ValueError(f"unsupported suffix: {suffix}")


def _report(
    candidate: Path,
    existing_parquet: Path,
    table_name: str,
    business_key_cols: list[str],
) -> str:
    import duckdb

    new_reader = _reader_expr(candidate)
    safe_existing = str(existing_parquet.resolve()).replace("'", "''")
    existing_reader = f"read_parquet('{safe_existing}')"

    # Own in-memory DuckDB — does not share state with the session REPL, so
    # there is no need to hold ctx.db_lock (which protects ctx.repl.conn).
    con = duckdb.connect(":memory:")
    try:
        schema_new = [
            (row[0], row[1])
            for row in con.execute(f"DESCRIBE SELECT * FROM {new_reader}").fetchall()
        ]
        schema_existing = [
            (row[0], row[1])
            for row in con.execute(
                f"DESCRIBE SELECT * FROM {existing_reader}"
            ).fetchall()
        ]
        (new_rows,) = con.execute(f"SELECT COUNT(*) FROM {new_reader}").fetchone()
        (existing_rows,) = con.execute(
            f"SELECT COUNT(*) FROM {existing_reader}"
        ).fetchone()

        new_map = dict(schema_new)
        existing_map = dict(schema_existing)
        missing_in_new = [c for c in existing_map if c not in new_map]
        extra_in_new = [c for c in new_map if c not in existing_map]
        type_mismatches = [
            (c, existing_map[c], new_map[c])
            for c in new_map
            if c in existing_map and new_map[c] != existing_map[c]
        ]
        missing_bk_new = [c for c in business_key_cols if c not in new_map]
        missing_bk_existing = [
            c for c in business_key_cols if c not in existing_map
        ]

        duplicate_count = 0
        if not missing_bk_new and not missing_bk_existing:
            bk_select = ", ".join(f'"{c}"' for c in business_key_cols)
            (duplicate_count,) = con.execute(
                f"SELECT COUNT(*) FROM ("
                f"SELECT {bk_select} FROM {new_reader} "
                f"INTERSECT "
                f"SELECT {bk_select} FROM {existing_reader})"
            ).fetchone()
    except Exception as exc:
        con.close()
        return f"Error comparing schemas: {exc}"
    con.close()

    lines: list[str] = []
    lines.append(f"## Drift check for `ingest_{table_name}`")
    lines.append("")
    lines.append(f"- New file rows: **{new_rows:,}**")
    lines.append(f"- Existing table rows: **{existing_rows:,}**")
    lines.append(f"- Business key: {', '.join(business_key_cols)}")
    lines.append(f"- Duplicate business-key rows: **{duplicate_count:,}**")
    lines.append("")

    if (
        not missing_in_new
        and not extra_in_new
        and not type_mismatches
        and not missing_bk_new
        and not missing_bk_existing
    ):
        lines.append("**Schema match.** All existing columns are present with matching types.")
    else:
        lines.append("### Schema differences")
        if missing_in_new:
            lines.append(
                "- Missing from new file: " + ", ".join(f"`{c}`" for c in missing_in_new)
            )
        if extra_in_new:
            lines.append(
                "- New columns not in existing table: "
                + ", ".join(f"`{c}`" for c in extra_in_new)
            )
        if type_mismatches:
            mismatch_str = "; ".join(
                f"`{col}` (was `{e}`, now `{n}`)" for col, e, n in type_mismatches
            )
            lines.append(f"- Type mismatches: {mismatch_str}")
        if missing_bk_new:
            lines.append(
                "- Business key missing in new file: "
                + ", ".join(f"`{c}`" for c in missing_bk_new)
            )
        if missing_bk_existing:
            lines.append(
                "- Business key missing in existing table: "
                + ", ".join(f"`{c}`" for c in missing_bk_existing)
            )
    lines.append("")

    if duplicate_count > 0:
        lines.append(
            f"**Recommendation:** {duplicate_count:,} row(s) already exist for the "
            "same business key. I suggest **skipping duplicates** (keep existing) "
            "unless the new file is a correction, in which case choose "
            "**replace duplicates**."
        )
    elif type_mismatches or missing_in_new or extra_in_new:
        lines.append(
            "**Recommendation:** schema drifted. Confirm the differences with the "
            "user before appending."
        )
    else:
        lines.append(
            "**Recommendation:** no duplicates or drift. Safe to append with "
            "`dedup_strategy='skip'`."
        )

    lines.append("")
    lines.append(
        "Next: present this report via `ask_user` with options like "
        "`Skip duplicates (keep existing)`, `Replace duplicates (keep new)`, "
        "`Skip this file`, `Type your own`. After the user confirms, call "
        "`write_ingested_table(mode='append', user_confirmed=True, "
        "dedup_strategy='skip'|'replace')`."
    )
    return "\n".join(lines)

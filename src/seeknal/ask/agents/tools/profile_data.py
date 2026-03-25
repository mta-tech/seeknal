"""Profile data tool — auto-profile CSV files for schema, quality, and relationships."""

from pathlib import Path

from langchain_core.tools import tool


@tool
def profile_data(file_path: str = "") -> str:
    """Profile data files in the project's data/ directory.

    Without arguments, lists all CSV files with row counts and column summaries.
    With a file_path, provides detailed profiling: types, nulls, unique counts,
    sample values, and potential join key candidates.

    Use this BEFORE building transforms to understand data quality and relationships.

    Args:
        file_path: Optional path to a specific CSV (e.g., 'data/customers.csv').
                   If empty, profiles all CSVs in data/.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    data_dir = ctx.project_path / "data"

    if not data_dir.exists():
        return "No data/ directory found in project."

    if file_path:
        return _profile_single(ctx.project_path / file_path)
    else:
        return _profile_all(data_dir)


def _profile_all(data_dir: Path) -> str:
    """List all CSVs with quick stats."""
    import duckdb

    csvs = sorted(data_dir.glob("*.csv"))
    if not csvs:
        return "No CSV files found in data/."

    con = duckdb.connect(":memory:")
    lines = ["## Data Files Overview\n"]

    for csv_path in csvs:
        try:
            safe_path = str(csv_path.resolve()).replace("'", "''")
            row_count = con.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{safe_path}')"
            ).fetchone()[0]
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_csv_auto('{safe_path}')"
            ).fetchall()

            col_summary = ", ".join(f"{c[0]} ({c[1]})" for c in cols)
            lines.append(
                f"**{csv_path.name}** — {row_count:,} rows, {len(cols)} columns\n"
                f"  Columns: {col_summary}\n"
            )
        except Exception as e:
            lines.append(f"**{csv_path.name}** — Error: {e}\n")

    # Suggest join keys with type mismatch detection
    lines.append("---\n**Potential join keys** (columns appearing in 2+ files):\n")
    col_type_map: dict[str, list[tuple[str, str]]] = {}  # col -> [(file, type), ...]
    for csv_path in csvs:
        try:
            safe_path = str(csv_path.resolve()).replace("'", "''")
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_csv_auto('{safe_path}')"
            ).fetchall()
            for col_name, col_type, *_ in cols:
                col_type_map.setdefault(col_name, []).append((csv_path.name, col_type))
        except Exception:
            pass

    shared = {k: v for k, v in col_type_map.items() if len(v) > 1}
    if shared:
        for col, file_types in sorted(shared.items()):
            files = [f for f, _ in file_types]
            types = set(t for _, t in file_types)
            line = f"  - `{col}` → {', '.join(files)}"
            if len(types) > 1:
                type_detail = ", ".join(f"{f}: {t}" for f, t in file_types)
                line += f" ⚠️ TYPE MISMATCH: {type_detail}"
            lines.append(line)
    else:
        lines.append("  (none detected)")

    con.close()
    return "\n".join(lines)


def _profile_single(csv_path: Path) -> str:
    """Detailed profile of a single CSV."""
    import duckdb

    if not csv_path.exists():
        return f"File not found: {csv_path}"

    con = duckdb.connect(":memory:")
    safe_path = str(csv_path.resolve()).replace("'", "''")

    try:
        # Basic stats
        row_count = con.execute(
            f"SELECT COUNT(*) FROM read_csv_auto('{safe_path}')"
        ).fetchone()[0]
        cols = con.execute(
            f"DESCRIBE SELECT * FROM read_csv_auto('{safe_path}')"
        ).fetchall()
    except Exception as e:
        con.close()
        return f"Error reading {csv_path.name}: {e}"

    lines = [f"## Profile: {csv_path.name}\n", f"**Rows:** {row_count:,}\n"]

    # Per-column profiling
    lines.append("| Column | Type | Nulls | Unique | Sample Values |")
    lines.append("| --- | --- | --- | --- | --- |")

    for col_name, col_type, *_ in cols:
        try:
            safe_col = col_name.replace('"', '""')
            stats = con.execute(f"""
                SELECT
                    COUNT(*) - COUNT("{safe_col}") AS null_count,
                    COUNT(DISTINCT "{safe_col}") AS unique_count
                FROM read_csv_auto('{safe_path}')
            """).fetchone()

            null_count = stats[0]
            unique_count = stats[1]
            null_pct = f"{null_count}/{row_count}" if null_count > 0 else "0"

            # Sample values (up to 5)
            samples = con.execute(f"""
                SELECT DISTINCT CAST("{safe_col}" AS VARCHAR)
                FROM read_csv_auto('{safe_path}')
                WHERE "{safe_col}" IS NOT NULL
                LIMIT 5
            """).fetchall()
            sample_str = ", ".join(str(s[0]) for s in samples)
            if len(sample_str) > 60:
                sample_str = sample_str[:60] + "..."

            lines.append(
                f"| {col_name} | {col_type} | {null_pct} | {unique_count} | {sample_str} |"
            )
        except Exception:
            lines.append(f"| {col_name} | {col_type} | ? | ? | ? |")

    con.close()
    return "\n".join(lines)

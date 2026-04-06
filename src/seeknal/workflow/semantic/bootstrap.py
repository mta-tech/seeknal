"""Semantic model bootstrap — profile data and generate draft semantic model YAML.

Scans data/ CSVs and target/intermediate/ parquets, uses DuckDB DESCRIBE to
classify columns, and writes draft SemanticModel YAML files to
seeknal/semantic_models/.
"""

import re
from pathlib import Path
from typing import Any

# Column name patterns for entity detection
_ID_KEY_PATTERN = re.compile(r".*(_id|_key)$", re.IGNORECASE)

# DuckDB type families
_TIMESTAMP_TYPES = {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "DATE"}
_NUMERIC_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "REAL",
    "INT", "INT2", "INT4", "INT8",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
}
_VARCHAR_TYPES = {"VARCHAR", "TEXT", "STRING", "ENUM", "BLOB"}

# High-cardinality threshold: columns above this ratio (unique/total) are
# treated as entity candidates when they also match _ID_KEY_PATTERN
_HIGH_CARDINALITY_RATIO = 0.5


def _discover_tables(project_path: Path) -> list[tuple[str, Path, str]]:
    """Discover data files to bootstrap.

    Returns:
        List of (table_name, file_path, model_ref) tuples.
    """
    tables: list[tuple[str, Path, str]] = []

    # data/*.csv
    data_dir = project_path / "data"
    if data_dir.exists():
        for csv_path in sorted(data_dir.glob("*.csv")):
            table_name = csv_path.stem
            model_ref = f"ref('source.{table_name}')"
            tables.append((table_name, csv_path, model_ref))

    # target/intermediate/*.parquet
    intermediate_dir = project_path / "target" / "intermediate"
    if intermediate_dir.exists():
        for pq_path in sorted(intermediate_dir.glob("*.parquet")):
            table_name = pq_path.stem
            model_ref = f"ref('transform.{table_name}')"
            tables.append((table_name, pq_path, model_ref))

    return tables


def _classify_column(
    col_name: str,
    col_type: str,
    unique_count: int,
    row_count: int,
) -> str:
    """Classify a column as entity, time_dimension, dimension, or measure.

    Returns one of: "entity", "time_dimension", "dimension", "measure".
    """
    upper_type = col_type.upper()

    # Normalize: remove precision/scale from DECIMAL(10,2) etc.
    base_type = re.sub(r"\(.*\)", "", upper_type).strip()

    # Time dimension: TIMESTAMP / DATE types
    if base_type in _TIMESTAMP_TYPES:
        return "time_dimension"

    # Entity: ID/key columns with high cardinality
    if _ID_KEY_PATTERN.match(col_name):
        cardinality_ratio = unique_count / max(row_count, 1)
        if cardinality_ratio >= _HIGH_CARDINALITY_RATIO:
            return "entity"
        # Low cardinality ID columns are categorical dimensions
        return "dimension"

    # Numeric: measure candidate
    if base_type in _NUMERIC_TYPES:
        return "measure"

    # Everything else: categorical dimension
    return "dimension"


def _profile_table(
    con: Any, table_name: str, file_path: Path
) -> tuple[list[dict], int]:
    """Profile a single table's columns.

    Returns:
        (column_info_list, row_count) where each column_info has
        name, type, unique_count, classification.
    """
    safe_path = str(file_path.resolve()).replace("'", "''")

    if file_path.suffix == ".csv":
        read_expr = f"read_csv_auto('{safe_path}')"
    else:
        read_expr = f"read_parquet('{safe_path}')"

    # Get row count
    row_count = con.execute(f"SELECT COUNT(*) FROM {read_expr}").fetchone()[0]

    # Describe columns
    cols = con.execute(f"DESCRIBE SELECT * FROM {read_expr}").fetchall()

    column_infos = []
    for col_name, col_type, *_ in cols:
        safe_col = col_name.replace('"', '""')
        try:
            unique_count = con.execute(
                f'SELECT COUNT(DISTINCT "{safe_col}") FROM {read_expr}'
            ).fetchone()[0]
        except Exception:
            unique_count = 0

        classification = _classify_column(col_name, col_type, unique_count, row_count)
        column_infos.append({
            "name": col_name,
            "type": col_type,
            "unique_count": unique_count,
            "classification": classification,
        })

    return column_infos, row_count


def _build_semantic_model_dict(
    table_name: str,
    model_ref: str,
    column_infos: list[dict],
) -> dict[str, Any]:
    """Build a SemanticModel dict from classified columns."""
    entities = []
    dimensions = []
    measures = []
    default_time_dimension = None

    for col in column_infos:
        name = col["name"]
        classification = col["classification"]

        if classification == "entity":
            entity_type = "primary" if len(entities) == 0 else "foreign"
            entities.append({"name": name, "type": entity_type})

        elif classification == "time_dimension":
            dimensions.append({
                "name": name,
                "type": "time",
                "expr": name,
                "time_granularity": "day",
            })
            if default_time_dimension is None:
                default_time_dimension = name

        elif classification == "dimension":
            dimensions.append({
                "name": name,
                "type": "categorical",
                "expr": name,
            })

        elif classification == "measure":
            measures.append({
                "name": f"total_{name}",
                "expr": name,
                "agg": "sum",
                "description": f"Sum of {name}",
            })

    model: dict[str, Any] = {
        "kind": "semantic_model",
        "name": table_name,
        "model": model_ref,
        "description": f"Auto-generated semantic model for {table_name}",
        "entities": entities,
        "dimensions": dimensions,
        "measures": measures,
    }

    if default_time_dimension:
        model["default_time_dimension"] = default_time_dimension

    return model


def _write_model_yaml(
    model_dict: dict[str, Any],
    output_dir: Path,
) -> Path:
    """Write a semantic model dict to YAML with auto-generated header."""
    import yaml

    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{model_dict['name']}.yml"

    yaml_content = yaml.safe_dump(model_dict, default_flow_style=False, sort_keys=False)
    content = (
        "# AUTO-GENERATED by seeknal semantic bootstrap\n"
        "# Review and adjust entities, dimensions, and measures as needed.\n"
        f"{yaml_content}"
    )
    file_path.write_text(content)
    return file_path


def bootstrap_semantic_models(
    project_path: Path,
    table_name: str = "",
) -> list[dict[str, Any]]:
    """Profile data files and generate draft semantic model YAML.

    Args:
        project_path: Path to the seeknal project root.
        table_name: If provided, bootstrap only this table. Otherwise all.

    Returns:
        List of generated semantic model dicts.
    """
    import duckdb

    tables = _discover_tables(project_path)

    if table_name:
        tables = [(n, p, r) for n, p, r in tables if n == table_name]
        if not tables:
            return []

    if not tables:
        return []

    con = duckdb.connect(":memory:")
    output_dir = project_path / "seeknal" / "semantic_models"
    generated: list[dict[str, Any]] = []

    try:
        for tbl_name, file_path, model_ref in tables:
            try:
                column_infos, row_count = _profile_table(con, tbl_name, file_path)
                model_dict = _build_semantic_model_dict(tbl_name, model_ref, column_infos)
                _write_model_yaml(model_dict, output_dir)
                generated.append(model_dict)
            except Exception:
                # Skip tables that fail to profile
                continue
    finally:
        con.close()

    return generated

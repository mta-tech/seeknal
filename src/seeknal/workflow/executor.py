"""
Executor for dry-run preview.

Handles execution of nodes for preview with sample data.
"""

import duckdb
from pathlib import Path
from typing import Any, Dict, Optional, Set
import sys
import time
import yaml

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from seeknal.cli.main import _echo_info, _echo_error, _echo_warning
from seeknal.utils.path_security import is_insecure_path
from tabulate import tabulate
import typer


# Allowed file extensions for source data
ALLOWED_SOURCE_EXTENSIONS: Set[str] = {'.csv', '.parquet', '.ipc', '.arrow'}


def validate_source_path(path: Path) -> Path:
    """Validate source file path meets security requirements.

    Args:
        path: Path to validate

    Returns:
        Resolved, validated Path object

    Raises:
        ValueError: If path fails validation
    """
    # Resolve symlinks to prevent symlink-based attacks
    try:
        resolved_path = path.resolve(strict=True)
    except FileNotFoundError:
        raise ValueError(f"Source file not found: {path}")

    # Check extension
    if resolved_path.suffix.lower() not in ALLOWED_SOURCE_EXTENSIONS:
        raise ValueError(
            f"Invalid source file type: {resolved_path.suffix}. "
            f"Allowed: {', '.join(ALLOWED_SOURCE_EXTENSIONS)}"
        )

    # Check for insecure path patterns (path traversal)
    if is_insecure_path(str(resolved_path)):
        raise ValueError(
            f"Insecure path detected: {resolved_path}. "
            f"Path traversal and symlink attacks are blocked."
        )

    return resolved_path


def execute_preview(
    yaml_data: Dict[str, Any],
    draft_path: Path,
    limit: int = 10,
    timeout: int = 30,
) -> Optional[Dict[str, Any]]:
    """Execute node for preview with sample data.

    Args:
        yaml_data: Parsed YAML data
        draft_path: Path to draft file (for context)
        limit: Row limit for preview
        timeout: Query timeout in seconds

    Returns:
        Result dict with execution info, or None if execution not supported

    Raises:
        typer.Exit: If execution fails
    """
    kind = yaml_data.get("kind")

    # Route to appropriate executor
    if kind == "source":
        return execute_source(yaml_data, limit, timeout)
    elif kind == "transform":
        return execute_transform(yaml_data, limit, timeout)
    elif kind == "feature_group":
        return execute_feature_group(yaml_data, limit, timeout)
    elif kind == "model":
        return execute_model(yaml_data, limit, timeout)
    elif kind == "aggregation":
        return execute_aggregation(yaml_data, limit, timeout)
    elif kind == "rule":
        return execute_rule(yaml_data, limit, timeout)
    elif kind == "exposure":
        return execute_exposure(yaml_data, limit, timeout)
    else:
        _echo_info(f"Preview not available for node type: {kind}")
        return None


def execute_source(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute source node for preview with actual data.

    For CSV sources, reads and displays sample data rows.
    For other sources, shows schema information.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout (unused in current implementation)

    Returns:
        Result dict with execution info
    """
    name = yaml_data.get("name", "unknown")
    source_type = yaml_data.get("source", "unknown")
    table = yaml_data.get("table", "unknown")

    import time
    start_time = time.time()

    # Handle CSV sources with actual data preview
    if source_type == "csv" and table.endswith(".csv"):
        try:
            # Convert to absolute path
            from pathlib import Path
            table_path = Path(table)
            if not table_path.is_absolute():
                table_path = Path.cwd() / table

            # Security: Validate path against security requirements
            validated_path = validate_source_path(table_path)

            # Use DuckDB to read CSV and display data
            con = duckdb.connect(":memory:")

            # Escape single quotes in path to prevent SQL injection
            # Note: DuckDB doesn't support true parameterization for file paths
            safe_path = str(validated_path).replace("'", "''")

            # Build query with escaped path
            query = f"SELECT * FROM read_csv_auto('{safe_path}') LIMIT {limit}"

            # Execute query
            result = con.execute(query)
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description]

            # Display data as table
            if rows:
                print(tabulate(rows, headers=columns, tablefmt="psql"))
            else:
                _echo_info(f"No data found in source: {name}")

            duration = time.time() - start_time
            return {
                "duration": duration,
                "row_count": len(rows),
                "schema_only": False,
            }

        except ValueError as e:
            # Security validation error
            _echo_warning(f"Security validation failed: {e}")
            _echo_info("Showing schema instead:")
        except Exception as e:
            _echo_warning(f"Could not preview data: {e}")
            _echo_info("Showing schema instead:")

    # Fallback to schema display
    columns = yaml_data.get("columns", {})

    if columns:
        # Format as table
        rows = [[col, desc] for col, desc in columns.items()]
        print(tabulate(rows, headers=["Column", "Description"], tablefmt="psql"))
    else:
        _echo_info(f"No columns defined for source: {name}")

    return {
        "duration": time.time() - start_time,
        "row_count": 0,
        "schema_only": True,
    }


def execute_transform(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute transform node for preview.

    Resolves upstream refs to actual data sources and executes the transform SQL.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout

    Returns:
        Result dict with execution info
    """
    import time
    start_time = time.time()

    name = yaml_data.get("name", "unknown")
    transform_sql = yaml_data.get("transform")

    if not transform_sql:
        _echo_error("No transform SQL defined")
        raise typer.Exit(1)

    # Get inputs (dependencies)
    inputs = yaml_data.get("inputs", [])

    # Create DuckDB connection for preview
    con = duckdb.connect(":memory:")

    # Track the first input to use for __THIS__ placeholder
    first_input_view = None

    # Resolve refs to actual data
    for input_ref in inputs:
        if isinstance(input_ref, dict) and "ref" in input_ref:
            ref = input_ref["ref"]

            # Parse ref (format: kind.name, e.g., "source.customers")
            if "." not in ref:
                _echo_warning(f"Invalid ref format: {ref}")
                continue

            kind, ref_name = ref.split(".", 1)

            # Use the first input as __THIS__
            if not first_input_view:
                first_input_view = f"{kind}_{ref_name}"

            # Handle source refs
            if kind == "source":
                # Find the source YAML file
                source_path = _find_source_file(ref_name)

                if source_path:
                    # Load source YAML to get table path
                    try:
                        with open(source_path, "r") as f:
                            source_data = yaml.safe_load(f)

                        source_type = source_data.get("source", "unknown")
                        table = source_data.get("table", "unknown")

                        if source_type == "csv" and table.endswith(".csv"):
                            # Resolve table path
                            table_path = Path(table)
                            if not table_path.is_absolute():
                                table_path = source_path.parent.parent.parent / table

                            # Validate and load the CSV
                            if table_path.exists():
                                safe_path = str(table_path.resolve()).replace("'", "''")
                                view_name = f"source_{ref_name}"

                                # Create view from CSV
                                con.execute(f"""
                                    CREATE OR REPLACE VIEW {view_name} AS
                                    SELECT * FROM read_csv_auto('{safe_path}')
                                """)

                                _echo_info(f"Loaded source '{ref_name}' from {table_path.name}")
                            else:
                                _echo_warning(f"Source file not found: {table_path}")
                        else:
                            _echo_warning(f"Unsupported source type: {source_type}")
                    except Exception as e:
                        _echo_warning(f"Could not load source '{ref_name}': {e}")
                else:
                    _echo_warning(f"Source file not found: {ref}")
            elif kind == "transform":
                # Try to load from intermediate storage (from previous run)
                project_path = Path.cwd()
                intermediate_path = project_path / "target" / "intermediate" / f"{kind}_{ref_name}.parquet"

                if intermediate_path.exists():
                    view_name = f"transform_{ref_name}"
                    con.execute(f"""
                        CREATE OR REPLACE VIEW {view_name} AS
                        SELECT * FROM read_parquet('{intermediate_path}')
                    """)
                    _echo_info(f"Loaded transform '{ref_name}' from intermediate storage")
                else:
                    _echo_warning(f"Transform '{ref_name}' not executed yet. Run 'seeknal run --nodes {ref_name}' first")
            else:
                _echo_warning(f"Unsupported kind: {kind}")

    # Replace __THIS__ placeholder with first input view
    if first_input_view:
        resolved_sql = transform_sql.replace("__THIS__", first_input_view)
    else:
        resolved_sql = transform_sql

    # Replace refs in SQL (source.customers -> source_customers)
    # (already handled by __THIS__ replacement, but keep for explicit refs)
    for input_ref in inputs:
        if isinstance(input_ref, dict) and "ref" in input_ref:
            ref = input_ref["ref"]
            if "." in ref:
                _, ref_name = ref.split(".", 1)
                # Replace kind.name with kind_name for DuckDB view names
                resolved_sql = resolved_sql.replace(f"source.{ref_name}", f"source_{ref_name}")
                resolved_sql = resolved_sql.replace(f"transform.{ref_name}", f"transform_{ref_name}")

    # Execute the transform SQL with limit
    try:
        # Add LIMIT to the query if not already present
        if "LIMIT" not in resolved_sql.upper() and "limit" not in resolved_sql:
            final_sql = f"SELECT * FROM ({resolved_sql}) AS subquery LIMIT {limit}"
        else:
            final_sql = resolved_sql

        # Execute query
        result = con.execute(final_sql)
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description]

        # Display data as table
        if rows:
            print(tabulate(rows, headers=columns, tablefmt="psql"))
        else:
            _echo_info(f"No data returned from transform: {name}")

        duration = time.time() - start_time
        return {
            "duration": duration,
            "row_count": len(rows),
            "preview_available": True,
        }

    except Exception as e:
        _echo_warning(f"Could not execute transform: {e}")
        _echo_info("Transform SQL:")
        print(transform_sql)

        return {
            "duration": time.time() - start_time,
            "row_count": 0,
            "preview_available": False,
        }


def _find_source_file(source_name: str) -> Optional[Path]:
    """Find source YAML file by name.

    Args:
        source_name: Name of the source (e.g., "customers")

    Returns:
        Path to source YAML file, or None if not found
    """
    # Search in seeknal/sources/ directory
    search_paths = [
        Path.cwd() / "seeknal" / "sources" / f"{source_name}.yml",
        Path.cwd() / "seeknal" / "sources" / f"{source_name}.yaml",
    ]

    for path in search_paths:
        if path.exists():
            return path

    return None


def execute_feature_group(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute feature group node for preview.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout

    Returns:
        Result dict with execution info
    """
    name = yaml_data.get("name", "unknown")
    features = yaml_data.get("features", {})

    # Show features as table
    if features:
        rows = []
        for feat_name, config in features.items():
            dtype = config.get("dtype", "unknown")
            desc = config.get("description", "")
            rows.append([feat_name, dtype, desc])

        print(tabulate(rows, headers=["Feature", "Type", "Description"], tablefmt="psql"))

    return {
        "duration": 0.1,
        "row_count": len(features),
        "feature_count": len(features),
    }


def execute_model(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute model node for preview.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout

    Returns:
        Result dict with execution info
    """
    name = yaml_data.get("name", "unknown")
    output_columns = yaml_data.get("output_columns", [])

    # Show model info
    _echo_info(f"Model: {name}")
    _echo_info(f"Output columns: {', '.join(output_columns)}")

    training = yaml_data.get("training", {})
    if training:
        algorithm = training.get("algorithm", "unknown")
        _echo_info(f"Algorithm: {algorithm}")

    return {
        "duration": 0.1,
        "row_count": 0,
        "model_info": True,
    }


def execute_aggregation(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute aggregation node for preview.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout

    Returns:
        Result dict with execution info
    """
    name = yaml_data.get("name", "unknown")
    features = yaml_data.get("features", [])

    # Show aggregation features
    _echo_info(f"Aggregation: {name}")
    _echo_info(f"Features: {len(features)}")

    for feat in features:
        feat_name = feat.get("name", "unknown")
        basic = feat.get("basic", [])
        rolling = feat.get("rolling", [])

        print(f"  - {feat_name}: basic={basic}, rolling={len(rolling)} windows")

    return {
        "duration": 0.1,
        "row_count": 0,
        "aggregation_info": True,
    }


def execute_rule(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute rule node for preview.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout

    Returns:
        Result dict with execution info
    """
    name = yaml_data.get("name", "unknown")
    rule = yaml_data.get("rule", {})

    # Show rule
    if isinstance(rule, dict):
        rule_value = rule.get("value", "undefined")
    else:
        rule_value = str(rule)

    _echo_info(f"Rule: {name}")
    print(f"  {rule_value}")

    return {
        "duration": 0.1,
        "row_count": 0,
        "rule_info": True,
    }


def execute_exposure(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute exposure node for preview.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout

    Returns:
        Result dict with execution info
    """
    name = yaml_data.get("name", "unknown")
    exposure_type = yaml_data.get("type", "unknown")
    url = yaml_data.get("url", "")

    # Show exposure info
    _echo_info(f"Exposure: {name}")
    _echo_info(f"Type: {exposure_type}")
    if url:
        _echo_info(f"URL: {url}")

    return {
        "duration": 0.1,
        "row_count": 0,
        "exposure_info": True,
    }

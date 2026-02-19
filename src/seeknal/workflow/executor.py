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
    Creates ``input_0``, ``input_1``, … views that match what the real pipeline
    executor provides, so the transform SQL can run unmodified.

    Data loading priority for each input:
    1. Intermediate parquet from a previous ``seeknal run`` (works for all source types)
    2. Original source file (CSV only — loaded directly from the file path in the source YAML)

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

    # Resolve each input ref and register as input_N views
    # This matches the naming convention used by the real transform executor
    loaded_inputs: list[str] = []

    for i, input_ref in enumerate(inputs):
        if not isinstance(input_ref, dict) or "ref" not in input_ref:
            _echo_warning(f"Input {i}: invalid format (expected dict with 'ref' key)")
            continue

        ref = input_ref["ref"]
        if "." not in ref:
            _echo_warning(f"Input {i}: invalid ref format '{ref}' (expected kind.name)")
            continue

        kind, ref_name = ref.split(".", 1)
        input_view = f"input_{i}"
        project_path = Path.cwd()

        # Priority 1: Load from intermediate parquet (works for ALL source types)
        intermediate_path = project_path / "target" / "intermediate" / f"{kind}_{ref_name}.parquet"
        if intermediate_path.exists():
            try:
                safe_path = str(intermediate_path.resolve()).replace("'", "''")
                con.execute(f"CREATE OR REPLACE VIEW {input_view} AS SELECT * FROM read_parquet('{safe_path}')")
                _echo_info(f"Loaded {input_view} <- {kind}.{ref_name} (from previous run)")
                loaded_inputs.append(input_view)
                continue
            except Exception as e:
                _echo_warning(f"Failed to load intermediate for {ref}: {e}")

        # Priority 2: Load original source data (CSV only)
        if kind == "source":
            source_path = _find_source_file(ref_name)
            if source_path:
                try:
                    with open(source_path, "r") as f:
                        source_data = yaml.safe_load(f)

                    source_type = source_data.get("source", "unknown")
                    table = source_data.get("table", "unknown")

                    if source_type == "csv" and table.endswith(".csv"):
                        table_path = Path(table)
                        if not table_path.is_absolute():
                            table_path = source_path.parent.parent.parent / table

                        if table_path.exists():
                            safe_path = str(table_path.resolve()).replace("'", "''")
                            con.execute(f"CREATE OR REPLACE VIEW {input_view} AS SELECT * FROM read_csv_auto('{safe_path}')")
                            _echo_info(f"Loaded {input_view} <- source.{ref_name} (from {table_path.name})")
                            loaded_inputs.append(input_view)
                            continue
                        else:
                            _echo_warning(f"Source file not found: {table_path}")
                    else:
                        _echo_warning(
                            f"Cannot preview {source_type} source '{ref_name}' directly. "
                            f"Run 'seeknal run' first, then dry-run will use the intermediate output."
                        )
                except Exception as e:
                    _echo_warning(f"Could not load source '{ref_name}': {e}")
            else:
                _echo_warning(f"Source YAML not found for: {ref_name}")
        elif kind == "transform":
            _echo_warning(
                f"Transform '{ref_name}' has no intermediate output. "
                f"Run 'seeknal run --nodes transform.{ref_name}' first."
            )
        else:
            _echo_warning(f"Unsupported input kind: {kind}")

    # Also register __THIS__ as alias for input_0 (single-input backward compat)
    if loaded_inputs:
        try:
            con.execute(f"CREATE OR REPLACE VIEW __THIS__ AS SELECT * FROM input_0")
        except Exception:
            pass

    # Resolve {{ }} common config expressions before ref() resolution
    common_dir = Path.cwd() / "seeknal" / "common"
    if common_dir.is_dir():
        from seeknal.workflow.common.loader import load_common_config
        from seeknal.workflow.parameters.resolver import ParameterResolver

        common_config = load_common_config(common_dir)
        if common_config:
            resolver = ParameterResolver(common_config=common_config)
            transform_sql = resolver.resolve_string(transform_sql)

    # Resolve named ref() syntax: ref('source.sales') -> input_0
    resolved_sql = _resolve_named_refs(transform_sql, inputs)

    # Execute the transform SQL with limit
    try:
        if "LIMIT" not in resolved_sql.upper() and "limit" not in resolved_sql:
            final_sql = f"SELECT * FROM ({resolved_sql}) AS subquery LIMIT {limit}"
        else:
            final_sql = resolved_sql

        result = con.execute(final_sql)
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description]

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


import re as _re

# Patterns for ref() resolution (module-level to avoid recompilation)
_REF_PATTERN = _re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""")
_REF_NAME_PATTERN = _re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.\-]*$")


def _resolve_named_refs(sql: str, inputs: list) -> str:
    """Resolve named ref() syntax to positional input_N placeholders.

    Replaces ``ref('source.sales')`` with ``input_0`` based on the
    order of inputs. Falls through to original SQL if no ref() calls
    or no list-style inputs.

    Args:
        sql: SQL string with potential ref() calls
        inputs: List of input dicts with 'ref' keys

    Returns:
        SQL with ref() calls replaced by input_N identifiers
    """
    if not inputs or not isinstance(inputs, list):
        return sql

    # Build lookup: {"source.sales": "input_0", ...}
    ref_lookup: Dict[str, str] = {}
    for i, item in enumerate(inputs):
        if isinstance(item, dict):
            ref_value = item.get("ref", "")
        elif isinstance(item, str):
            ref_value = item
        else:
            continue
        if ref_value:
            ref_lookup[ref_value] = f"input_{i}"

    if not ref_lookup:
        return sql

    def replace_ref(match):
        ref_name = match.group(1)
        # Validate ref argument
        if not _REF_NAME_PATTERN.match(ref_name):
            _echo_warning(f"Invalid ref() argument: '{ref_name}'")
            return match.group(0)  # Return unchanged
        if ref_name not in ref_lookup:
            _echo_warning(
                f"Unknown ref('{ref_name}'). "
                f"Available: {', '.join(sorted(ref_lookup.keys()))}"
            )
            return match.group(0)  # Return unchanged
        return ref_lookup[ref_name]

    return _REF_PATTERN.sub(replace_ref, sql)


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

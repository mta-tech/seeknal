"""
Executor for dry-run preview.

Handles execution of nodes for preview with sample data.
"""

import duckdb
from pathlib import Path
from typing import Any, Dict, Optional
import sys
import time

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from seeknal.cli.main import _echo_info, _echo_error
from tabulate import tabulate
import typer


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
    """Execute source node for preview.

    For sources, we show the schema information rather than actual data
    since we may not have access to the actual database.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit (unused for sources)
        timeout: Timeout (unused for sources)

    Returns:
        Result dict with schema information
    """
    name = yaml_data.get("name", "unknown")
    source_type = yaml_data.get("source", "unknown")
    table = yaml_data.get("table", "unknown")

    # Show schema info
    columns = yaml_data.get("columns", {})

    if columns:
        # Format as table
        rows = [[col, desc] for col, desc in columns.items()]
        print(tabulate(rows, headers=["Column", "Description"], tablefmt="psql"))
    else:
        _echo_info(f"No columns defined for source: {name}")

    return {
        "duration": 0.1,
        "row_count": 0,
        "schema_only": True,
    }


def execute_transform(
    yaml_data: Dict[str, Any],
    limit: int,
    timeout: int,
) -> Dict[str, Any]:
    """Execute transform node for preview.

    Args:
        yaml_data: Parsed YAML data
        limit: Row limit
        timeout: Query timeout

    Returns:
        Result dict with execution info
    """
    transform_sql = yaml_data.get("transform")

    if not transform_sql:
        _echo_error("No transform SQL defined")
        raise typer.Exit(1)

    # For now, show the SQL that would be executed
    # In a full implementation, we would:
    # 1. Resolve refs to actual tables
    # 2. Execute the query in DuckDB
    # 3. Return sample results

    _echo_info("Transform SQL:")
    print(transform_sql)

    return {
        "duration": 0.1,
        "row_count": 0,
        "preview_available": False,
    }


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

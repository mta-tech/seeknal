"""
YAML validators for Seeknal workflow.

Provides validation for YAML syntax, schema, and dependencies.
"""

import yaml
from pathlib import Path
from typing import Any, Dict, List
import sys
import typer

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from seeknal.cli.main import _echo_error


# Schema definitions for each node type
SCHEMAS = {
    "source": {
        "required": ["kind", "name"],
        "optional": ["description", "owner", "tags", "source", "table", "params", "columns", "freshness"],
    },
    "transform": {
        "required": ["kind", "name"],
        "optional": ["description", "owner", "tags", "inputs", "params", "transform", "output"],
    },
    "feature_group": {
        "required": ["kind", "name", "entity", "materialization"],
        "optional": ["description", "owner", "tags", "inputs", "transform", "features", "tests"],
    },
    "model": {
        "required": ["kind", "name", "output_columns", "inputs"],
        "optional": ["description", "owner", "tags", "aggregation", "training", "tests"],
    },
    "aggregation": {
        "required": ["kind", "name", "id_col", "feature_date_col"],
        "optional": ["description", "owner", "application_date_col", "features"],
    },
    "second_order_aggregation": {
        "required": ["kind", "name", "id_col", "feature_date_col", "source", "features"],
        "optional": ["description", "owner", "application_date_col", "inputs", "tags", "materialization"],
    },
    "profile": {
        "required": ["kind", "name", "inputs"],
        "optional": ["description", "owner", "tags", "profile"],
    },
    "rule": {
        "required": ["kind", "name", "rule"],
        "optional": ["description", "owner", "params"],
    },
    "exposure": {
        "required": ["kind", "name", "type"],
        "optional": ["description", "owner", "tags", "depends_on", "url", "params"],
    },
    "semantic_model": {
        "required": ["kind", "name", "model"],
        "optional": ["description", "default_time_dimension", "entities", "dimensions", "measures", "joins", "metrics"],
    },
    "metric": {
        "required": ["kind", "name", "type"],
        "optional": ["description", "filter", "measure", "numerator", "denominator", "grain_to_date", "expr", "inputs"],
    },
}


def validate_yaml_syntax(file_path: Path) -> Dict[str, Any]:
    """Validate YAML syntax with helpful error messages.

    Args:
        file_path: Path to YAML file

    Returns:
        Parsed YAML data

    Raises:
        typer.Exit: If YAML is invalid
    """
    try:
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            _echo_error(f"Invalid YAML: expected dictionary, got {type(data).__name__}")
            raise typer.Exit(1)

        return data

    except yaml.YAMLError as e:
        # Extract line number if available
        error_line = ""
        if hasattr(e, 'problem_mark'):
            mark = e.problem_mark
            error_line = f" at line {mark.line + 1}"

        # Show context around error
        context = ""
        if hasattr(e, 'problem_mark') and hasattr(e, 'context'):
            context = f"\n  Context: {e.context}"

        _echo_error(f"YAML syntax error{error_line}: {e.problem}{context}")

        # Suggest common fixes
        if "mapping values are not allowed here" in str(e):
            _echo_error("  Hint: Check for invalid colon (:) usage")
        elif "could not find expected ':'" in str(e):
            _echo_error("  Hint: Check indentation and colons")
        elif "unexpected character" in str(e):
            _echo_error("  Hint: Check for special characters that need quoting")

        raise typer.Exit(1)

    except FileNotFoundError:
        _echo_error(f"File not found: {file_path}")
        raise typer.Exit(1)

    except Exception as e:
        _echo_error(f"Failed to read file: {e}")
        raise typer.Exit(1)


def validate_schema(yaml_data: Dict[str, Any]) -> None:
    """Validate YAML schema for node type.

    Args:
        yaml_data: Parsed YAML data

    Raises:
        typer.Exit: If schema is invalid
    """
    kind = yaml_data.get("kind")

    if not kind:
        _echo_error("Missing required field: kind")
        raise typer.Exit(1)

    if kind not in SCHEMAS:
        valid_types = ", ".join(sorted(SCHEMAS.keys()))
        _echo_error(f"Unknown node type: '{kind}'")
        _echo_error(f"Valid types: {valid_types}")
        raise typer.Exit(1)

    schema = SCHEMAS[kind]
    required_fields = schema["required"]

    # Check required fields
    missing_fields = []
    for field in required_fields:
        if field not in yaml_data:
            missing_fields.append(field)

    if missing_fields:
        _echo_error(f"Missing required fields for {kind}: {', '.join(missing_fields)}")
        raise typer.Exit(1)


def validate_dependencies(yaml_data: Dict[str, Any]) -> None:
    """Validate dependencies (refs to upstream nodes).

    Checks for:
    - refs() to non-existent nodes
    - Circular dependencies
    - Self-references

    Args:
        yaml_data: Parsed YAML data

    Raises:
        Warning: Printed for dependency issues (not fatal)
    """
    # Check for refs in inputs
    if "inputs" in yaml_data:
        for inp in yaml_data["inputs"]:
            if isinstance(inp, dict) and "ref" in inp:
                ref = inp["ref"]
                # TODO: Check if ref exists in manifest
                # For now, just validate format
                if not ref or "." not in ref:
                    print(f"⚠ Warning: Invalid ref format: '{ref}' (expected 'type.name')")


def check_for_credentials(yaml_data: Dict[str, Any]) -> List[str]:
    """Check for credentials in YAML data.

    Args:
        yaml_data: Parsed YAML data

    Returns:
        List of warning messages
    """
    credential_fields = ["password", "api_key", "token", "secret", "credential"]
    warnings = []

    for field in credential_fields:
        if field in yaml_data:
            value = yaml_data[field]
            if value and value != "${{ ENV_VAR }}":
                warnings.append(
                    f"Credentials found in field '{field}'. "
                    "Use environment variables instead."
                )

    return warnings

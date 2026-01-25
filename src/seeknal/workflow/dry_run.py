"""
Dry-run command for Seeknal workflow.

Validates YAML and previews execution.
"""

import typer
from pathlib import Path
from typing import Optional
import sys
import yaml

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from seeknal.cli.main import _echo_success, _echo_error, _echo_warning, _echo_info
from seeknal.workflow.validators import validate_yaml_syntax, validate_schema, validate_dependencies
from seeknal.workflow.executor import execute_preview

# Default options
DEFAULT_LIMIT = 10
DEFAULT_TIMEOUT = 30


def validate_draft_file(file_path: str) -> Path:
    """Validate draft file exists and is readable.

    Args:
        file_path: Path to draft YAML file

    Returns:
        Path object

    Raises:
        typer.Exit: If file doesn't exist or isn't readable
    """
    path = Path(file_path)

    if not path.exists():
        _echo_error(f"File not found: {file_path}")
        raise typer.Exit(1)

    if not path.is_file():
        _echo_error(f"Not a file: {file_path}")
        raise typer.Exit(1)

    return path


def dry_run_command(
    file_path: str = typer.Argument(..., help="Path to draft YAML file"),
    limit: int = typer.Option(DEFAULT_LIMIT, "--limit", "-l", help="Row limit for preview (default: 10)"),
    timeout: int = typer.Option(DEFAULT_TIMEOUT, "--timeout", "-t", help="Query timeout in seconds (default: 30)"),
    schema_only: bool = typer.Option(False, "--schema-only", "-s", help="Validate schema only, skip execution"),
):
    """Validate YAML and preview execution.

    Performs comprehensive validation:
    1. YAML syntax validation with line numbers
    2. Schema validation (required fields)
    3. Dependency validation (refs to upstream nodes)
    4. Preview execution with sample data

    Examples:
        # Validate and preview
        $ seeknal dry-run draft_feature_group_user_behavior.yml

        # Preview with 5 rows
        $ seeknal dry-run draft_feature_group_user_behavior.yml --limit 5

        # Schema validation only (no execution)
        $ seeknal dry-run draft_source_postgres.yml --schema-only

        # Longer timeout for slow queries
        $ seeknal dry-run draft_transform.yml --timeout 60
    """
    # Validate file exists
    draft_path = validate_draft_file(file_path)

    _echo_info("Validating YAML...")

    # Step 1: YAML syntax validation
    try:
        yaml_data = validate_yaml_syntax(draft_path)
        _echo_success("YAML syntax valid")
    except typer.Exit:
        raise
    except Exception as e:
        _echo_error(f"YAML validation failed: {e}")
        raise typer.Exit(1)

    # Step 2: Schema validation
    try:
        validate_schema(yaml_data)
        _echo_success("Schema validation passed")
    except typer.Exit:
        raise
    except Exception as e:
        _echo_error(f"Schema validation failed: {e}")
        raise typer.Exit(1)

    # Step 3: Dependency validation
    try:
        validate_dependencies(yaml_data)
        _echo_success("Dependency check passed")
    except Exception as e:
        # Dependency issues are warnings, not errors
        _echo_warning(f"Dependency warning: {e}")

    # Step 4: Preview execution (unless schema-only)
    if not schema_only:
        _echo_info(f"Executing preview (limit {limit} rows)...")

        try:
            result = execute_preview(yaml_data, draft_path, limit, timeout)

            if result:
                _echo_success(f"Preview completed in {result.get('duration', 0):.1f}s")
                _echo_info(f"Run 'seeknal apply {file_path}' to apply")
            else:
                _echo_warning("No preview available for this node type")
                _echo_info(f"Run 'seeknal apply {file_path}' to apply")
        except typer.Exit:
            raise
        except Exception as e:
            _echo_error(f"Execution failed: {e}")
            raise typer.Exit(1)
    else:
        _echo_info("Schema-only mode: execution skipped")
        _echo_info(f"Run 'seeknal apply {file_path}' to apply")


if __name__ == "__main__":
    typer.run(dry_run_command)

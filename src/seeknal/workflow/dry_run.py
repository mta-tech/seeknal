"""
Dry-run command for Seeknal workflow.

Validates YAML and Python pipeline files and previews execution.
"""

import typer
from pathlib import Path
from typing import Optional
import sys
import yaml
import ast

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from seeknal.cli.main import _echo_success, _echo_error, _echo_warning, _echo_info
from seeknal.workflow.validators import validate_yaml_syntax, validate_schema, validate_dependencies
from seeknal.workflow.executor import execute_preview

# Default options
DEFAULT_LIMIT = 10
DEFAULT_TIMEOUT = 30


def is_python_file(file_path: Path) -> bool:
    """Check if file is a Python pipeline file.

    Args:
        file_path: Path to check

    Returns:
        True if .py file
    """
    return file_path.suffix == ".py"


def validate_python_syntax(file_path: Path) -> dict:
    """Validate Python syntax and extract metadata.

    Args:
        file_path: Path to Python file

    Returns:
        Dictionary with metadata (name, kind, description, dependencies)

    Raises:
        ValueError: If syntax is invalid or required elements missing
    """
    try:
        content = file_path.read_text()
        tree = ast.parse(content, filename=str(file_path))
    except SyntaxError as e:
        raise ValueError(f"Python syntax error at line {e.lineno}: {e.msg}")
    except Exception as e:
        raise ValueError(f"Failed to read Python file: {e}")

    # Extract PEP 723 metadata
    pep723_metadata = extract_pep723_metadata(content)

    # Check for required dependencies
    dependencies = pep723_metadata.get("dependencies", [])
    required_deps = ["pandas", "duckdb"]
    missing_deps = [dep for dep in required_deps if dep not in dependencies]
    if missing_deps:
        _echo_warning(f"Missing recommended dependencies: {', '.join(missing_deps)}")

    # Find transform/source/feature_group decorators
    transform_info = extract_decorators(tree)

    if not transform_info:
        raise ValueError(
            "No pipeline decorator found. Expected @source, @transform, or @feature_group decorator"
        )

    return {
        "name": transform_info.get("name"),
        "kind": transform_info.get("kind", "transform"),
        "description": transform_info.get("description", ""),
        "dependencies": dependencies,
        "pep723": pep723_metadata
    }


def extract_pep723_metadata(content: str) -> dict:
    """Extract PEP 723 inline script metadata.

    Args:
        content: Python file content

    Returns:
        Dictionary with PEP 723 metadata
    """
    lines = content.split("\n")
    metadata = {}
    in_header = False
    in_dependencies = False
    deps_buffer = []

    for line in lines:
        if line.strip() == "# /// script":
            in_header = True
            continue
        if in_header:
            if line.strip() == "# ///":
                # End of header, process any accumulated deps
                if deps_buffer:
                    import re
                    deps_str = "\n".join(deps_buffer)
                    # Extract package names from quotes
                    deps = []
                    for match in re.finditer(r'"([^"]+)"', deps_str):
                        deps.append(match.group(1))
                    if deps:
                        metadata["dependencies"] = deps
                break

            # Parse dependencies (multi-line)
            if line.strip().startswith("# dependencies"):
                in_dependencies = True
                # Check if list starts on same line
                if "[" in line:
                    deps_buffer.append(line)
                continue

            if in_dependencies:
                deps_buffer.append(line)
                # Check if list ends on this line
                if "]" in line:
                    in_dependencies = False
                continue

            # Parse other fields
            elif "requires-python" in line:
                version_match = line.find('"')
                if version_match > 0:
                    try:
                        version = line[version_match:].split('"')[1]
                        metadata["requires_python"] = version
                    except:
                        pass

    return metadata


def extract_decorators(tree: ast.AST) -> dict:
    """Extract pipeline decorator information.

    Args:
        tree: AST tree

    Returns:
        Dictionary with decorator info (name, kind, description)
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Check decorators
            for decorator in node.decorator_list:
                # Handle @decorator(args) or @decorator
                decorator_name = None
                decorator_args = {}

                if isinstance(decorator, ast.Call):
                    # @transform(name="foo")
                    if isinstance(decorator.func, ast.Name):
                        decorator_name = decorator.func.id
                        # Extract keyword arguments
                        for keyword in decorator.keywords:
                            if isinstance(keyword.value, ast.Constant):
                                decorator_args[keyword.arg] = keyword.value.value
                elif isinstance(decorator, ast.Name):
                    # @transform
                    decorator_name = decorator.id

                # Check if it's a pipeline decorator
                if decorator_name in ["source", "transform", "feature_group"]:
                    docstring = ast.get_docstring(node)
                    return {
                        "kind": decorator_name,
                        "name": decorator_args.get("name", node.name),
                        "description": decorator_args.get("description", docstring or ""),
                        "function_name": node.name
                    }

    return {}


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
    file_path: str = typer.Argument(..., help="Path to YAML or Python pipeline file"),
    limit: int = typer.Option(DEFAULT_LIMIT, "--limit", "-l", help="Row limit for preview (default: 10)"),
    timeout: int = typer.Option(DEFAULT_TIMEOUT, "--timeout", "-t", help="Query timeout in seconds (default: 30)"),
    schema_only: bool = typer.Option(False, "--schema-only", "-s", help="Validate schema only, skip execution"),
):
    """Validate YAML/Python and preview execution.

    Performs comprehensive validation:
    - YAML files: syntax, schema, dependencies, preview
    - Python files: PEP 723 metadata, decorators, imports, syntax

    Examples:
        # Validate YAML file
        $ seeknal dry-run draft_feature_group_user_behavior.yml

        # Validate Python file
        $ seeknal dry-run seeknal/pipelines/enriched_sales.py

        # Preview with 5 rows
        $ seeknal dry-run draft_transform.yml --limit 5

        # Schema validation only (no execution)
        $ seeknal dry-run draft_source.yml --schema-only
    """
    # Validate file exists
    draft_path = validate_draft_file(file_path)

    # Detect file type
    is_py = is_python_file(draft_path)

    if is_py:
        dry_run_python(draft_path, file_path, limit, timeout, schema_only)
    else:
        dry_run_yaml(draft_path, file_path, limit, timeout, schema_only)


def dry_run_python(file_path: Path, file_path_str: str, limit: int, timeout: int, schema_only: bool):
    """Dry-run for Python pipeline files.

    Args:
        file_path: Path object
        file_path_str: Original file path string
        limit: Row limit for preview
        timeout: Query timeout
        schema_only: Skip execution
    """
    _echo_info("Validating Python pipeline...")

    # Step 1: Python syntax validation
    try:
        metadata = validate_python_syntax(file_path)
        _echo_success("Python syntax valid")
    except ValueError as e:
        _echo_error(f"Python validation failed: {e}")
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Validation error: {e}")
        raise typer.Exit(1)

    # Step 2: Show metadata
    _echo_info(f"Node name: {metadata.get('name')}")
    _echo_info(f"Node kind: {metadata.get('kind')}")
    if metadata.get('description'):
        _echo_info(f"Description: {metadata['description']}")

    deps = metadata.get('dependencies', [])
    if deps:
        _echo_info(f"Dependencies: {', '.join(deps)}")

    requires_python = metadata.get('pep723', {}).get('requires_python')
    if requires_python:
        _echo_info(f"Requires Python: {requires_python}")

    # Step 3: Check for ctx.ref() calls
    try:
        content = file_path.read_text()
        if 'ctx.ref(' in content:
            import re
            refs = re.findall(r'ctx\.ref\(["\']([^"\']+)["\']\)', content)
            if refs:
                _echo_info(f"References: {', '.join(refs)}")
                for ref in refs:
                    # Check if it's a valid node reference (kind.name)
                    if '.' in ref:
                        parts = ref.split('.')
                        if len(parts) == 2:
                            _echo_info(f"  - {ref}: ✓ Valid reference format")
                        else:
                            _echo_warning(f"  - {ref}: Unexpected format")
                    else:
                        _echo_warning(f"  - {ref}: Should be qualified (e.g., 'source.raw_data')")
    except Exception as e:
        _echo_warning(f"Could not analyze references: {e}")

    # Step 4: Preview execution (unless schema-only)
    if not schema_only:
        _echo_info(f"Executing preview (limit {limit} rows)...")

        try:
            import time
            import importlib.util
            from pathlib import Path as PathLib

            # Get project paths
            project_path = PathLib.cwd()
            target_path = project_path / "target"

            # Import PipelineContext
            sys.path.insert(0, str(PathLib(__file__).parent.parent.parent))
            from seeknal.pipeline.context import PipelineContext

            # Create execution context
            ctx = PipelineContext(
                project_path=project_path,
                target_dir=target_path,
                config={}
            )

            # Import and execute the pipeline function
            spec = importlib.util.spec_from_file_location("pipeline_module", str(file_path))
            module = importlib.util.module_from_spec(spec)

            # Monkey-patch to limit output
            original_limit = limit

            # Execute the function
            spec.loader.exec_module(module)
            func = getattr(module, metadata.get('name'))

            start_time = time.time()
            result = func(ctx)
            duration = time.time() - start_time

            # Display preview
            if result is not None:
                try:
                    import pandas as pd
                    if isinstance(result, pd.DataFrame):
                        # Show first N rows
                        preview_df = result.head(limit)

                        # Pretty print table
                        _echo_info(f"Preview ({len(result)} total rows, showing {limit}):")
                        print(preview_df.to_string(index=False))
                    else:
                        _echo_info(f"Result type: {type(result).__name__}")
                        _echo_info(f"Result: {result}")
                except ImportError:
                    # pandas not available, just show basic info
                    _echo_info(f"Result type: {type(result).__name__}")
                    _echo_info(f"Result: {str(result)[:200]}")

            ctx.close()
            _echo_success(f"Preview completed in {duration:.1f}s")

        except ImportError as e:
            _echo_warning(f"Preview skipped (missing dependency): {e}")
            _echo_info(f"Install missing dependencies and try again")
        except FileNotFoundError as e:
            _echo_warning(f"Preview skipped (upstream data not found): {e}")
            _echo_info(f"Ensure upstream nodes are executed first, or use 'seeknal run' for full pipeline")
        except Exception as e:
            _echo_warning(f"Preview failed: {e}")
            _echo_info(f"Use 'seeknal run --nodes {metadata.get('name')}' to execute in full pipeline")
    else:
        _echo_info("Schema-only mode: execution skipped")
        _echo_info(f"Use 'seeknal run --nodes {metadata.get('name')}' to execute")


def dry_run_yaml(file_path: Path, file_path_str: str, limit: int, timeout: int, schema_only: bool):
    """Dry-run for YAML pipeline files.

    Args:
        file_path: Path object
        file_path_str: Original file path string
        limit: Row limit for preview
        timeout: Query timeout
        schema_only: Skip execution
    """
    _echo_info("Validating YAML...")

    # Step 1: YAML syntax validation
    try:
        yaml_data = validate_yaml_syntax(file_path)
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
            result = execute_preview(yaml_data, file_path, limit, timeout)

            if result:
                _echo_success(f"Preview completed in {result.get('duration', 0):.1f}s")
                _echo_info(f"Run 'seeknal apply {file_path_str}' to apply")
            else:
                _echo_warning("No preview available for this node type")
                _echo_info(f"Run 'seeknal apply {file_path_str}' to apply")
        except typer.Exit:
            raise
        except Exception as e:
            _echo_error(f"Execution failed: {e}")
            raise typer.Exit(1)
    else:
        _echo_info("Schema-only mode: execution skipped")
        _echo_info(f"Run 'seeknal apply {file_path_str}' to apply")


if __name__ == "__main__":
    typer.run(dry_run_command)

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
            "No pipeline decorator found. Expected @source, @transform, @feature_group, or @second_order_aggregation decorator"
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
                if decorator_name in ["source", "transform", "feature_group", "second_order_aggregation"]:
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


def _resolve_source_path(table: str, project_path: Path) -> Path:
    """Resolve a source table path against project root.

    Args:
        table: Table path (relative or absolute)
        project_path: Project root path

    Returns:
        Resolved absolute Path
    """
    table_path = Path(table)
    if not table_path.is_absolute():
        table_path = project_path / table_path
    return table_path


def _load_source_data(node_meta: dict, project_path: Path):
    """Load data from a source node's configuration for preview.

    Supports all Seeknal source types:
    - File-based: csv, parquet, json, jsonl, hive
    - Database: sqlite, postgresql/postgres, starrocks, iceberg

    Args:
        node_meta: Node metadata from @source decorator
        project_path: Project root path for resolving relative paths

    Returns:
        pandas DataFrame or None if loading fails
    """
    source_type = node_meta.get("source", "").lower()
    table = node_meta.get("table", "")
    params = node_meta.get("params", {})

    if not table and source_type not in ("postgresql", "postgres", "starrocks", "iceberg"):
        return None

    # File-based sources
    if source_type in ("csv", "parquet", "json", "jsonl", "hive"):
        return _load_file_source(source_type, table, params, project_path)

    # SQLite
    if source_type == "sqlite":
        return _load_sqlite_source(table, params, project_path)

    # PostgreSQL
    if source_type in ("postgresql", "postgres"):
        return _load_postgresql_source(table, params, project_path)

    # StarRocks
    if source_type == "starrocks":
        return _load_starrocks_source(table, params)

    # Iceberg
    if source_type == "iceberg":
        return _load_iceberg_source(table, params)

    _echo_warning(f"Source type '{source_type}' not supported for dry-run preview")
    return None


def _load_file_source(source_type: str, table: str, params: dict, project_path: Path):
    """Load file-based source data using DuckDB."""
    import duckdb

    table_path = _resolve_source_path(table, project_path)
    if not table_path.exists():
        _echo_warning(f"Source file not found: {table_path}")
        return None

    abs_path = str(table_path.resolve())
    con = duckdb.connect()

    try:
        if source_type == "csv":
            delimiter = params.get("delimiter", "")
            if delimiter:
                result = con.execute(
                    f"SELECT * FROM read_csv('{abs_path}', delim='{delimiter}')"
                )
            else:
                result = con.execute(f"SELECT * FROM read_csv_auto('{abs_path}')")

        elif source_type == "parquet":
            result = con.execute(f"SELECT * FROM read_parquet('{abs_path}')")

        elif source_type in ("json", "jsonl"):
            json_format = params.get("format", "auto")
            if source_type == "jsonl" or json_format == "newline":
                result = con.execute(
                    f"SELECT * FROM read_json_auto('{abs_path}', format='newline_delimited')"
                )
            elif json_format == "array":
                result = con.execute(
                    f"SELECT * FROM read_json_auto('{abs_path}', format='array')"
                )
            else:
                result = con.execute(f"SELECT * FROM read_json_auto('{abs_path}')")

        elif source_type == "hive":
            # Hive: detect file type from extension
            ext = table_path.suffix.lower()
            if ext in (".parquet", ".pq"):
                result = con.execute(f"SELECT * FROM read_parquet('{abs_path}')")
            elif ext == ".csv":
                result = con.execute(f"SELECT * FROM read_csv_auto('{abs_path}')")
            else:
                _echo_warning(f"Cannot detect file format for hive source: {ext}")
                con.close()
                return None
        else:
            con.close()
            return None

        df = result.df()
        con.close()
        return df

    except Exception as e:
        con.close()
        _echo_warning(f"Could not load {source_type} source: {e}")
        return None


def _load_sqlite_source(table: str, params: dict, project_path: Path):
    """Load SQLite source data using DuckDB."""
    import duckdb

    db_path = params.get("path", table)
    query_table = table

    # If table looks like a file path, treat it as db_path
    if table.endswith((".db", ".sqlite", ".sqlite3")):
        db_path = table
        query_table = params.get("table", "main")

    resolved_path = _resolve_source_path(db_path, project_path)
    if not resolved_path.exists():
        _echo_warning(f"SQLite database not found: {resolved_path}")
        return None

    con = duckdb.connect()
    try:
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute(f"ATTACH '{resolved_path}' AS sqlite_db (TYPE sqlite)")
        schema = params.get("schema", "main")
        full_table = f"sqlite_db.{schema}.{query_table}" if schema != "main" else f"sqlite_db.{query_table}"
        df = con.execute(f"SELECT * FROM {full_table}").df()
        con.close()
        return df
    except Exception as e:
        con.close()
        _echo_warning(f"Could not load SQLite source: {e}")
        return None


def _load_postgresql_source(table: str, params: dict, project_path: Path):
    """Load PostgreSQL source data using DuckDB postgres extension."""
    import duckdb

    connection_name = params.get("connection", "")
    query = params.get("query", "")

    # Resolve connection config from profiles or inline params
    conn_params = dict(params)
    if connection_name:
        try:
            from seeknal.workflow.materialization.profile_loader import ProfileLoader  # ty: ignore[unresolved-import]
            loader = ProfileLoader()
            profile = loader.load_connection_profile(connection_name)
            # Profile as base, inline params override
            for key, val in conn_params.items():
                profile[key] = val
            conn_params = profile
        except Exception as e:
            _echo_warning(f"Could not load connection profile '{connection_name}': {e}")

    host = conn_params.get("host", "localhost")
    port = conn_params.get("port", 5432)
    user = conn_params.get("user", "")
    password = conn_params.get("password", "")
    database = conn_params.get("database", "")

    if not database:
        _echo_warning("PostgreSQL: no database configured — skipping preview")
        return None

    dsn = f"host={host} port={port} dbname={database}"
    if user:
        dsn += f" user={user}"
    if password:
        dsn += f" password={password}"
    dsn += " connect_timeout=5"

    con = duckdb.connect()
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{dsn}' AS pg_db (TYPE postgres, READ_ONLY)")

        if query:
            df = con.execute(f"SELECT * FROM postgres_query('pg_db', '{query}')").df()
        elif table:
            df = con.execute(f"SELECT * FROM pg_db.{table}").df()
        else:
            con.close()
            _echo_warning("PostgreSQL: no table or query specified")
            return None

        con.close()
        return df
    except Exception as e:
        con.close()
        _echo_warning(f"Could not load PostgreSQL source: {e}")
        _echo_info("Check connection config or use 'seeknal run' for full execution")
        return None


def _load_starrocks_source(table: str, params: dict):
    """Load StarRocks source data using pymysql."""
    try:
        import pymysql
        import pandas as pd
    except ImportError:
        _echo_warning("StarRocks preview requires pymysql: pip install pymysql")
        return None

    host = params.get("host", "localhost")
    port = int(params.get("port", 9030))
    user = params.get("user", "root")
    password = params.get("password", "")
    database = params.get("database", "")
    query = params.get("query", "")

    if not database:
        _echo_warning("StarRocks: no database configured — skipping preview")
        return None

    try:
        conn = pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=database, connect_timeout=5
        )
        sql = query if query else f"SELECT * FROM {table}"
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        _echo_warning(f"Could not load StarRocks source: {e}")
        _echo_info("Check connection config or use 'seeknal run' for full execution")
        return None


def _load_iceberg_source(table: str, params: dict):
    """Load Iceberg source data using DuckDB iceberg extension."""
    import duckdb
    import os

    catalog_uri = params.get("catalog_uri", os.environ.get("LAKEKEEPER_URL", ""))
    warehouse = params.get("warehouse", os.environ.get("LAKEKEEPER_WAREHOUSE", ""))

    if not catalog_uri:
        _echo_warning("Iceberg: no catalog_uri configured — skipping preview")
        _echo_info("Set LAKEKEEPER_URL env var or add catalog_uri to params")
        return None

    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg; LOAD iceberg;")

        # Build ATTACH command with REST catalog
        attach_opts = f"TYPE ICEBERG, ENDPOINT '{catalog_uri}'"
        if warehouse:
            attach_opts += f", WAREHOUSE '{warehouse}'"

        # Add S3/OAuth2 config from environment
        for env_key, duck_key in [
            ("AWS_ENDPOINT_URL", "ENDPOINT"),
            ("AWS_REGION", "REGION"),
            ("AWS_ACCESS_KEY_ID", "KEY_ID"),
            ("AWS_SECRET_ACCESS_KEY", "SECRET"),
        ]:
            val = os.environ.get(env_key, "")
            if val:
                attach_opts += f", {duck_key} '{val}'"

        con.execute(f"ATTACH '' AS iceberg_cat ({attach_opts})")
        df = con.execute(f"SELECT * FROM iceberg_cat.{table}").df()
        con.close()
        return df
    except Exception as e:
        con.close()
        _echo_warning(f"Could not load Iceberg source: {e}")
        _echo_info("Check Iceberg/Lakekeeper config or use 'seeknal run' for full execution")
        return None


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

            # Execute the module to register decorators
            spec.loader.exec_module(module)

            # Use Python function name (not decorator name=) for module lookup
            func_name = metadata.get('function_name') or metadata.get('name')
            func = getattr(module, func_name)

            start_time = time.time()
            result = func(ctx)
            duration = time.time() - start_time

            # For source nodes with pass body, load data from source config
            if result is None and metadata.get('kind') == 'source':
                node_meta = getattr(func, '_seeknal_node', None)
                if node_meta:
                    result = _load_source_data(node_meta, project_path)

            # Display preview
            if result is not None:
                import pandas as pd

                # Convert non-DataFrame results to pandas for display
                result_df = None
                if isinstance(result, pd.DataFrame):
                    result_df = result
                else:
                    # Try pyarrow Table
                    try:
                        import pyarrow as pa
                        if isinstance(result, pa.Table):
                            result_df = result.to_pandas()
                    except ImportError:
                        pass

                    # Try DuckDB relation
                    if result_df is None:
                        try:
                            if hasattr(result, 'df'):
                                result_df = result.df()
                        except Exception:
                            pass

                if result_df is not None:
                    preview_df = result_df.head(limit)
                    _echo_info(f"Preview ({len(result_df)} total rows, showing {min(limit, len(result_df))}):")
                    typer.echo(preview_df.to_string(index=False))
                else:
                    _echo_info(f"Result type: {type(result).__name__}")
                    _echo_info(f"Result: {str(result)[:200]}")
            else:
                _echo_warning("No preview data available")
                _echo_info("Use 'seeknal run' for full execution")

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

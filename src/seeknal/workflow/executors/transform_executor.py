"""
Transform node executor for Seeknal workflow execution.

This module implements the TransformExecutor class, which executes:
- YAML transforms with SQL (using DuckDB)
- Python transforms with decorated functions (using PipelineContext)

It handles:
- Resolving input refs to actual table/view names or DataFrames
- Executing Python functions and writing results to intermediate storage
- Executing multi-statement SQL (separated by semicolons)
- Supporting CTEs and complex queries
- Returning row counts and execution timing
- Error handling for both SQL and Python execution
- Iceberg materialization for both YAML and Python transforms

Key Features:
- For YAML: Parses the `transform` field, resolves inputs, executes SQL
- For Python: Imports the module, executes the function, writes DataFrame to intermediate storage
- Intermediate storage enables downstream nodes and exposures to read Python outputs
- Iceberg materialization: Creates DuckDB views for Python outputs, enabling materialization to Iceberg
- Returns ExecutorResult with metrics (row count, output path, etc.)
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from seeknal.cli.main import _echo_error, _echo_info, _echo_warning
from seeknal.dag.manifest import Manifest, Node, NodeType

from .base import (
    BaseExecutor,
    ExecutionContext,
    ExecutionStatus,
    ExecutorExecutionError,
    ExecutorResult,
    ExecutorValidationError,
    register_executor,
)

# Iceberg materialization support
from seeknal.workflow.materialization.yaml_integration import materialize_node_if_enabled

logger = logging.getLogger(__name__)


@register_executor(NodeType.TRANSFORM)
class TransformExecutor(BaseExecutor):
    """
    Executor for TRANSFORM nodes.

    Executes both YAML and Python transforms:
    
    **YAML Transforms:**
    - SQL transformations using DuckDB
    - Multi-statement SQL (separated by semicolons)
    - CTEs (WITH clauses)
    - Input ref resolution (e.g., `inputs: ref: source.users`)
    - CREATE TABLE AS, CREATE VIEW AS, and SELECT statements

    **Python Transforms:**
    - Imports Python module from seeknal/pipelines/
    - Executes decorated function (@transform, @source, @feature_group)
    - Passes PipelineContext with ctx.ref() support
    - Writes output DataFrame to intermediate storage
    - Creates DuckDB view for Iceberg materialization
    - Enables downstream nodes and exposures to read Python outputs

    **Iceberg Materialization:**
    - Automatically materializes to Iceberg when `materialization.enabled: true`
    - Works for both YAML and Python transforms
    - Creates DuckDB views (`transform.{name}`) for materialization
    - Supports append and overwrite modes

    Example:
        >>> executor = TransformExecutor(node, context)
        >>> result = executor.run()
        >>> print(f"Processed {result.row_count} rows in {result.duration_seconds:.2f}s")
    """

    @property
    def node_type(self) -> NodeType:
        """Return the NodeType this executor handles."""
        return NodeType.TRANSFORM

    def validate(self) -> None:
        """
        Validate the transform node configuration.

        Checks:
        - For YAML: `transform` field is present and contains SQL
        - For Python: file exists and has correct structure
        - `inputs` references are valid (if present)

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        # Python transforms don't have a `transform` field - skip SQL validation
        if self._is_python_transform():
            # Basic validation: check file exists
            from pathlib import Path
            file_path = Path(self.node.file_path)
            if not file_path.exists():
                raise ExecutorValidationError(
                    self.node.id,
                    f"Python file not found: {file_path}"
                )
            return

        # Check for transform SQL (YAML transforms)
        transform_sql = self.node.config.get("transform", "")

        if not transform_sql or not isinstance(transform_sql, str):
            raise ExecutorValidationError(
                self.node.id,
                "Missing or invalid 'transform' field. Must be a non-empty string."
            )

        # Check for empty SQL (after stripping whitespace and comments)
        cleaned_sql = self._clean_sql(transform_sql)
        if not cleaned_sql.strip():
            raise ExecutorValidationError(
                self.node.id,
                "'transform' field is empty or contains only comments."
            )

        # Validate input refs (if present)
        inputs = self.node.config.get("inputs", {})
        if inputs:
            # Support both list format (dbt-style) and dict format
            if isinstance(inputs, list):
                # List format: [{"ref": "source.customers"}, ...]
                for item in inputs:
                    if not isinstance(item, dict):
                        raise ExecutorValidationError(
                            self.node.id,
                            f"Input item must be a dictionary, got {type(item).__name__}"
                        )
                    if "ref" not in item:
                        raise ExecutorValidationError(
                            self.node.id,
                            "Input item missing 'ref' field"
                        )
                    if not isinstance(item["ref"], str):
                        raise ExecutorValidationError(
                            self.node.id,
                            f"Input ref must be a string, got {type(item['ref']).__name__}"
                        )
            elif isinstance(inputs, dict):
                # Dict format: {"users": "ref:source.users", ...}
                for key, value in inputs.items():
                    if not isinstance(value, str):
                        raise ExecutorValidationError(
                            self.node.id,
                            f"Input '{key}' must be a string reference, got {type(value).__name__}"
                        )
            else:
                raise ExecutorValidationError(
                    self.node.id,
                    f"'inputs' must be a list or dictionary, got {type(inputs).__name__}"
                )

    def pre_execute(self) -> None:
        """
        Setup before transform execution.

        Creates output directory, prepares DuckDB connection, and loads
        cached upstream views that may have been lost from in-memory DuckDB.
        """
        conn = self.context.get_duckdb_connection()

        # Create output directory for this node
        output_path = self.context.get_output_path(self.node)
        output_path.mkdir(parents=True, exist_ok=True)

        # Load cached upstream source/transform views into DuckDB
        # This fixes the "Table does not exist" bug when source nodes are skipped
        self._load_cached_upstream_views(conn)

    def _load_cached_upstream_views(self, conn) -> None:
        """Load cached Parquet files as DuckDB views for upstream dependencies."""
        if not hasattr(self.context, '_manifest') or self.context._manifest is None:
            # No manifest available - try to load from input refs
            self._load_views_from_inputs(conn)
            return

        manifest = self.context._manifest
        upstream_ids = manifest.get_upstream_nodes(self.node.id)
        for up_id in upstream_ids:
            up_node = manifest.get_node(up_id)
            if up_node is None:
                continue
            self._ensure_view_loaded(conn, up_node)

    def _load_views_from_inputs(self, conn) -> None:
        """Load cached views based on input refs in the node config."""
        inputs = self.node.config.get("inputs", [])
        if not inputs:
            return

        for inp in inputs:
            if isinstance(inp, dict):
                ref = inp.get("ref", "")
            elif isinstance(inp, str):
                ref = inp
            else:
                continue

            if not ref:
                continue

            # Check if view exists in DuckDB
            try:
                conn.execute(f"SELECT 1 FROM {ref} LIMIT 0")
            except Exception:
                # View doesn't exist - try to load from cache
                parts = ref.split(".")
                if len(parts) == 2:
                    kind, name = parts
                    cache_path = self.context.target_path / "cache" / kind / f"{name}.parquet"
                    if cache_path.exists():
                        try:
                            conn.execute(
                                f"CREATE OR REPLACE VIEW \"{ref}\" AS "
                                f"SELECT * FROM read_parquet('{cache_path}')"
                            )
                            logger.info(f"Loaded cached view: {ref} from {cache_path}")
                        except Exception as e:
                            logger.warning(f"Failed to load cache for {ref}: {e}")

    def _ensure_view_loaded(self, conn, node: Node) -> None:
        """Ensure a node's view is loaded in DuckDB, from cache if needed."""
        view_name = f"{node.node_type.value}.{node.name}"
        try:
            conn.execute(f"SELECT 1 FROM \"{view_name}\" LIMIT 0")
        except Exception:
            # View missing - load from cache
            cache_path = self.context.get_cache_path(node)
            if cache_path.exists():
                try:
                    conn.execute(
                        f"CREATE OR REPLACE VIEW \"{view_name}\" AS "
                        f"SELECT * FROM read_parquet('{cache_path}')"
                    )
                    logger.info(f"Loaded cached view: {view_name} from {cache_path}")
                except Exception as e:
                    logger.warning(f"Failed to load cache for {view_name}: {e}")
                    # Cache corrupted — delete it so the node re-executes
                    try:
                        cache_path.unlink()
                        logger.info(f"Deleted corrupted cache: {cache_path}")
                    except Exception:
                        pass

    def execute(self) -> ExecutorResult:
        """
        Execute the transform node.

        **For Python transforms** (file_path ends with .py):
        1. Import the Python module from seeknal/pipelines/
        2. Create PipelineContext with ctx.ref() support
        3. Execute the decorated function (e.g., @transform(name="my_transform"))
        4. Write the returned DataFrame to intermediate storage
        5. Return ExecutorResult with row count and output path

        **For YAML transforms** (file_path ends with .yml):
        1. Get transform SQL from node config
        2. Resolve input refs to actual table names
        3. Execute multi-statement SQL in DuckDB
        4. Extract row count from last SELECT statement
        5. Return ExecutorResult with metrics

        Returns:
            ExecutorResult with executionResult with execution outcome

        Raises:
            ExecutorExecutionError: If execution fails
        """
        start_time = time.time()

        # Handle dry-run mode
        if self.context.dry_run:
            return self._execute_dry_run()

        # Check if this is a Python-based transform
        if self._is_python_transform():
            return self._execute_python_transform()

        # Get transform SQL for YAML-based transforms
        transform_sql = self.node.config.get("transform", "")

        try:
            # Get DuckDB connection
            con = self.context.get_duckdb_connection()

            # Resolve {{ }} common config expressions (e.g., {{ rules.active }})
            # This must happen BEFORE ref() resolution
            if self.context.common_config:
                from seeknal.workflow.parameters.resolver import ParameterResolver
                param_resolver = ParameterResolver(
                    common_config=self.context.common_config,
                )
                transform_sql = param_resolver.resolve_string(transform_sql)

            # Resolve named ref() syntax (e.g., ref('source.sales') -> input_0)
            resolved_sql = self._resolve_named_refs(transform_sql)

            # Resolve input refs
            resolved_sql = self._resolve_input_refs(resolved_sql, con)

            # Execute multi-statement SQL
            row_count, statements_executed = self._execute_sql(
                resolved_sql,
                con,
            )
            
            # Create view for materialization (if there's a SELECT result)
            # View name format: transform.{node_name}
            view_name = f"transform.{self.node.name}"
            try:
                # Check if there's a SELECT statement that produced results
                # Convert row_count to int for comparison (handles string returns from DuckDB)
                if isinstance(row_count, str):
                    try:
                        row_count = int(row_count)
                    except ValueError:
                        row_count = -1  # Invalid count
                if row_count >= 0:  # Has results
                    # Create a view from the last SELECT by wrapping it
                    # The last SELECT statement should have created a result set
                    # We create a view by re-executing the last SELECT with CREATE VIEW
                    last_select = self._get_last_select_statement(resolved_sql)
                    if last_select:
                        # Create schema if it doesn't exist
                        con.execute("CREATE SCHEMA IF NOT EXISTS transform")
                        
                        # Create view from the SELECT
                        create_view_sql = f"CREATE OR REPLACE VIEW {view_name} AS {last_select}"
                        con.execute(create_view_sql)
                        
                        if self.context.verbose:
                            _echo_info(f"Created view '{view_name}' for materialization")
            except Exception as e:
                # View creation is not critical for transform execution
                # Log warning but don't fail
                import warnings
                warnings.warn(f"Failed to create view '{view_name}': {e}")

            # Calculate duration
            duration = time.time() - start_time

            # Get output path (if materialized)
            output_path = self._get_output_path_if_materialized(resolved_sql)
            
            # Write to intermediate storage (for Python ctx.ref() compatibility)
            intermediate_path = self.context.target_path / "intermediate" / f"{self.node.id.replace('.', '_')}.parquet"
            if row_count > 0:
                try:
                    intermediate_path.parent.mkdir(parents=True, exist_ok=True)
                    # Write the transform view result to parquet
                    con.execute(f"COPY (SELECT * FROM transform.{self.node.name}) TO '{intermediate_path}' (FORMAT PARQUET)")
                    if self.context.verbose:
                        _echo_info(f"Wrote transform output to {intermediate_path}")
                except Exception as e:
                    # Intermediate storage is not critical
                    import warnings
                    warnings.warn(f"Failed to write to intermediate storage: {str(e)}")

            # Return success result
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=row_count,
                output_path=output_path,
                metadata={
                    "engine": "duckdb",
                    "statements_executed": statements_executed,
                    "sql_length": len(transform_sql),
                },
            )

        except ExecutorExecutionError:
            # Re-raise executor errors as-is
            raise
        except Exception as e:
            # Wrap other exceptions in ExecutorExecutionError
            raise ExecutorExecutionError(
                self.node.id,
                f"Transform execution failed: {str(e)}",
                e,
            ) from e

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Cleanup after transform execution.

        Adds additional metadata to the result and handles Iceberg materialization
        if enabled in the node configuration.

        Args:
            result: Result from execute()

        Returns:
            Enhanced ExecutorResult
        """
        # Add executor version
        result.metadata["executor_version"] = "1.0.0"

        # Add SQL preview (first 100 chars)
        transform_sql = self.node.config.get("transform", "")
        result.metadata["sql_preview"] = transform_sql[:100] + (
            "..." if len(transform_sql) > 100 else ""
        )

        # Handle Iceberg materialization if enabled
        if result.status == ExecutionStatus.SUCCESS and not result.is_dry_run:
            try:
                # Get the context's DuckDB connection (has the view)
                con = self.context.get_duckdb_connection()
                mat_result = materialize_node_if_enabled(
                    self.node,
                    source_con=con,
                    enabled_override=getattr(self.context, 'materialize_enabled', None)
                )
                if mat_result:
                    # Materialization was enabled and succeeded
                    result.metadata["materialization"] = {
                        "enabled": True,
                        "success": mat_result.get("success", False),
                        "table": mat_result.get("table"),
                        "row_count": mat_result.get("row_count"),
                        "mode": mat_result.get("mode"),
                        "iceberg_table": mat_result.get("iceberg_table"),
                    }
                    logger.info(
                        f"Materialized node '{self.node.id}' to Iceberg table "
                        f"'{mat_result.get('table')}' ({mat_result.get('row_count')} rows)"
                    )
            except Exception as e:
                # Log materialization error but don't fail the executor
                # Materialization is optional/augmentation, not core functionality
                logger.warning(
                    f"Failed to materialize node '{self.node.id}' to Iceberg: {e}"
                )
                result.metadata["materialization"] = {
                    "enabled": True,
                    "success": False,
                    "error": str(e),
                }

        return result

    # --- Helper Methods ---

    def _execute_dry_run(self) -> ExecutorResult:
        """
        Handle dry-run execution.

        Validates SQL syntax without actually executing.

        Returns:
            ExecutorResult with dry_run=True
        """
        transform_sql = self.node.config.get("transform", "")

        # Resolve named ref() syntax (validates ref arguments even in dry-run)
        resolved_sql = self._resolve_named_refs(transform_sql)

        # Basic syntax validation (check for balanced parentheses)
        if not self._validate_sql_syntax(resolved_sql):
            raise ExecutorValidationError(
                self.node.id,
                "SQL syntax validation failed: unbalanced parentheses or quotes"
            )

        # Count statements
        statements = self._split_sql_statements(resolved_sql)

        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=0.0,
            row_count=0,
            metadata={
                "engine": "duckdb",
                "statements_count": len(statements),
                "dry_run": True,
            },
            is_dry_run=True,
        )

    # Regex for ref('source.sales') or ref("source.sales")
    _REF_PATTERN = re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""")
    # Validation pattern for ref arguments (safe SQL identifiers with dots/hyphens)
    _REF_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.\-]*$")

    def _resolve_named_refs(self, sql: str) -> str:
        """
        Resolve named ref() syntax to positional input_N placeholders.

        Replaces ``ref('source.sales')`` with ``input_0`` based on the
        order of inputs defined in the node config.

        Args:
            sql: SQL string with potential ref() calls

        Returns:
            SQL string with ref() calls replaced by input_N identifiers

        Raises:
            ExecutorExecutionError: If a ref() argument is invalid or unknown
        """
        inputs = self.node.config.get("inputs", [])
        if not inputs or not isinstance(inputs, list):
            return sql

        # Build lookup: {"source.sales": "input_0", "source.products": "input_1"}
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

        # Find all ref() calls in the SQL
        def replace_ref(match: re.Match) -> str:
            ref_name = match.group(1)

            # Validate ref argument to prevent SQL injection
            if not self._REF_NAME_PATTERN.match(ref_name):
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Invalid ref() argument: '{ref_name}'. "
                    f"Must match pattern: {self._REF_NAME_PATTERN.pattern}"
                )

            if ref_name not in ref_lookup:
                available = ", ".join(sorted(ref_lookup.keys()))
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unknown ref('{ref_name}'). "
                    f"Available inputs: {available}"
                )

            return ref_lookup[ref_name]

        return self._REF_PATTERN.sub(replace_ref, sql)

    def _resolve_input_refs(
        self,
        sql: str,
        con: Any,
    ) -> str:
        """
        Resolve input refs to actual table/view names.

        Input refs are specified in YAML as:
        ```yaml
        inputs:
          users: ref:source.users
          orders: ref:source.orders
        ```

        This method replaces refs in SQL with actual table names.
        The refs can be used in SQL as: `{{users}}`, `{{orders}}`

        Args:
            sql: SQL string with potential ref placeholders
            con: DuckDB connection

        Returns:
            SQL string with refs resolved to table names

        Raises:
            ExecutorExecutionError: If ref resolution fails
        """
        inputs = self.node.config.get("inputs", {})

        if not inputs:
            # No refs to resolve
            return sql

        # Get manifest from context (if available)
        # For now, we'll assume refs are node IDs in the manifest
        # In the future, we might want to track actual table names

        resolved_sql = sql

        # Handle both list and dict formats
        if isinstance(inputs, list):
            # List format (dbt-style): [{"ref": "source.customers"}, ...]
            # For single input, replace __THIS__ with the actual table
            if len(inputs) == 1:
                ref_value = inputs[0].get("ref", "")
                if ref_value:
                    # Extract ref value (format: "ref:node.id" or just "node.id")
                    if ref_value.startswith("ref:"):
                        node_ref = ref_value[4:]  # Remove "ref:" prefix
                    else:
                        node_ref = ref_value
                    
                    # Check if this is a Python node output
                    # Python nodes write to intermediate/ with underscores replacing dots
                    intermediate_path = self.context.target_path / "intermediate" / f"{node_ref.replace('.', '_')}.parquet"
                    
                    if intermediate_path.exists():
                        # Load Python node output and register as DuckDB view
                        try:
                            con.execute(f"CREATE OR REPLACE VIEW __THIS__ AS SELECT * FROM read_parquet('{intermediate_path}')")
                            con.execute(f"CREATE OR REPLACE VIEW input_0 AS SELECT * FROM read_parquet('{intermediate_path}')")
                            if self.context.verbose:
                                _echo_info(f"Loaded Python node output as input_0")
                        except Exception as e:
                            raise ExecutorExecutionError(
                                self.node.id,
                                f"Failed to load Python node output from {intermediate_path}: {str(e)}",
                                e
                            ) from e
                    else:
                        # YAML source - should already be registered in DuckDB
                        # Generate table name from node ref
                        table_name = node_ref.replace(".", "_").replace("-", "_")
                        # Create alias views for both __THIS__ and input_0
                        try:
                            con.execute(f"CREATE OR REPLACE VIEW __THIS__ AS SELECT * FROM {table_name}")
                            con.execute(f"CREATE OR REPLACE VIEW input_0 AS SELECT * FROM {table_name}")
                            if self.context.verbose:
                                _echo_info(f"Created input_0 view from {table_name}")
                        except Exception as e:
                            # Table might already exist with schema prefix
                            if "." in node_ref:
                                schema, name = node_ref.split(".", 1)
                                try:
                                    con.execute(f"CREATE OR REPLACE VIEW __THIS__ AS SELECT * FROM {schema}.{name}")
                                    con.execute(f"CREATE OR REPLACE VIEW input_0 AS SELECT * FROM {schema}.{name}")
                                    if self.context.verbose:
                                        _echo_info(f"Created input_0 view from {schema}.{name}")
                                except Exception as e2:
                                    raise ExecutorExecutionError(
                                        self.node.id,
                                        f"Failed to create input_0 view from {node_ref}: {str(e2)}",
                                        e2
                                    ) from e2
                            else:
                                raise ExecutorExecutionError(
                                    self.node.id,
                                    f"Failed to create input_0 view from {node_ref}: {str(e)}",
                                    e
                                ) from e
            else:
                # Multiple inputs - validate and register each
                for i, item in enumerate(inputs):
                    ref_value = item.get("ref", "")
                    if ref_value:
                        # Extract ref value
                        if ref_value.startswith("ref:"):
                            node_ref = ref_value[4:]
                        else:
                            node_ref = ref_value
                        
                        # Check if Python node output exists
                        intermediate_path = self.context.target_path / "intermediate" / f"{node_ref.replace('.', '_')}.parquet"
                        
                        if intermediate_path.exists():
                            # Load and register Python node output
                            view_name = f"input_{i}"
                            try:
                                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{intermediate_path}')")
                                if self.context.verbose:
                                    _echo_info(f"Loaded Python node output as {view_name}")
                            except Exception as e:
                                raise ExecutorExecutionError(
                                    self.node.id,
                                    f"Failed to load Python node output from {intermediate_path}: {str(e)}",
                                    e
                                ) from e
        elif isinstance(inputs, dict):
            # Dict format: {"users": "ref:source.users", ...}
            # Resolve each input ref
            for input_name, ref_value in inputs.items():
                # Extract ref value (format: "ref:node.id" or just "node.id")
                if isinstance(ref_value, str):
                    if ref_value.startswith("ref:"):
                        node_ref = ref_value[4:]  # Remove "ref:" prefix
                    else:
                        node_ref = ref_value

                    # Generate table name from node ref
                    # Replace dots with underscores for valid SQL identifier
                    table_name = node_ref.replace(".", "_").replace("-", "_")

                    # Replace {{input_name}} placeholders in SQL
                    # Support both {{input_name}} and $input_name syntax
                    placeholder_pattern = rf"{{\s*{re.escape(input_name)}\s*}}"
                    dollar_pattern = rf"\${re.escape(input_name)}\b"

                    resolved_sql = re.sub(placeholder_pattern, table_name, resolved_sql)
                    resolved_sql = re.sub(dollar_pattern, table_name, resolved_sql)

        if self.context.verbose:
            _echo_info(f"Resolved SQL for {self.node.name}")
            if resolved_sql != sql:
                _echo_info("  Input refs were resolved")

        return resolved_sql

    def _execute_sql(
        self,
        sql: str,
        con: Any,
    ) -> Tuple[int, int]:
        """
        Execute multi-statement SQL in DuckDB.

        Splits SQL by semicolons and executes each statement.
        Returns row count from the last SELECT statement.

        Args:
            sql: SQL string (may contain multiple statements)
            con: DuckDB connection

        Returns:
            Tuple of (row_count, statements_executed)

        Raises:
            ExecutorExecutionError: If SQL execution fails
        """
        # Split SQL into statements
        statements = self._split_sql_statements(sql)

        if not statements:
            raise ExecutorExecutionError(
                self.node.id,
                "No valid SQL statements found"
            )

        row_count = 0
        last_select_result = None

        for i, statement in enumerate(statements, 1):
            try:
                # Skip empty statements
                if not statement.strip():
                    continue

                # Execute statement
                if self.context.verbose:
                    _echo_info(f"  Executing statement {i}/{len(statements)}")

                result = con.execute(statement)

                # Check if this is a SELECT statement
                is_select = self._is_select_statement(statement)

                if is_select:
                    # Fetch row count from SELECT
                    # For performance, we can use COUNT(*) or fetch all
                    try:
                        # Always fetch as DataFrame to get accurate row count
                        # This is simpler and more reliable than trying to detect aggregate queries
                        df = result.df()
                        row_count = len(df)
                        last_select_result = df
                    except Exception as e:
                        # If row count extraction fails, estimate
                        _echo_warning(f"Could not extract row count: {e}")
                        row_count = -1  # Unknown count

                elif self.context.verbose:
                    _echo_info(f"    Statement {i} executed successfully")

            except Exception as e:
                # Provide helpful error message
                statement_preview = statement[:100] + (
                    "..." if len(statement) > 100 else ""
                )
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Statement {i} failed: {str(e)}\n  Statement: {statement_preview}",
                    e,
                ) from e

        return row_count, len(statements)

    def _split_sql_statements(self, sql: str) -> List[str]:
        """
        Split SQL into individual statements.

        Handles:
        - Semicolons inside string literals
        - Semicolons inside comments
        - CREATE FUNCTION/PROCEDURE with nested blocks

        Args:
            sql: SQL string to split

        Returns:
            List of SQL statements
        """
        # Simple approach: split by semicolon, filtering empty strings
        # For more robust parsing, consider using a proper SQL parser

        statements = []
        current_statement = []
        in_string = False
        in_comment = False
        string_char = None
        prev_char = ""
        paren_depth = 0

        for char in sql:
            # Track string literals
            if char in ("'", '"') and prev_char != "\\":
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            # Track comments (single-line -- and multi-line /* */)
            if not in_string:
                if char == "-" and prev_char == "-":
                    in_comment = True
                elif char == "*" and prev_char == "/":
                    in_comment = True
                elif char == "/" and prev_char == "*":
                    in_comment = False
                elif char == "\n" and in_comment:
                    in_comment = False

            # Track parentheses for CREATE FUNCTION blocks
            if not in_string and not in_comment:
                if char == "(":
                    paren_depth += 1
                elif char == ")":
                    paren_depth = max(0, paren_depth - 1)

            # Split on semicolon (outside strings/comments/blocks)
            if (
                char == ";"
                and not in_string
                and not in_comment
                and paren_depth == 0
            ):
                statement = "".join(current_statement).strip()
                if statement:
                    statements.append(statement)
                current_statement = []
            else:
                current_statement.append(char)

            prev_char = char

        # Add final statement (if no trailing semicolon)
        final_statement = "".join(current_statement).strip()
        if final_statement:
            statements.append(final_statement)

        return statements

    def _is_select_statement(self, statement: str) -> bool:
        """
        Check if a statement is a SELECT (returns rows).

        Returns True for:
        - SELECT ...
        - WITH ... SELECT ... (CTE)

        Returns False for:
        - CREATE TABLE/VIEW/INDEX/SCHEMA
        - INSERT/UPDATE/DELETE
        - DROP/TRUNCATE/ALTER

        Args:
            statement: SQL statement to check

        Returns:
            True if statement returns rows
        """
        # Remove leading whitespace
        statement = statement.strip()

        # Check for CTE (WITH clause)
        if statement.upper().startswith("WITH"):
            # Find the main SELECT after the CTE
            # This is simplified - proper parsing would need a SQL parser
            return "SELECT" in statement.upper()

        # Check for SELECT
        if statement.upper().startswith("SELECT"):
            return True

        # Not a SELECT
        return False

    def _get_last_select_statement(self, sql: str) -> Optional[str]:
        """
        Extract the last SELECT statement from SQL.
        
        This is used to create a view from the transform results.
        Handles CTEs (WITH clauses) and regular SELECT statements.
        
        Args:
            sql: SQL string (may contain multiple statements)
            
        Returns:
            The last SELECT statement as a string, or None if not found
        """
        # Split SQL into statements
        statements = self._split_sql_statements(sql)
        
        # Find the last statement that returns data (SELECT or WITH...SELECT)
        for statement in reversed(statements):
            if self._is_select_statement(statement):
                return statement
        
        return None

    def _get_output_path_if_materialized(self, sql: str) -> Optional[Any]:
        """
        Get output path if SQL creates a table/view.

        Checks for CREATE TABLE AS and CREATE VIEW AS statements.

        Args:
            sql: SQL string to check

        Returns:
            Path object if materialized, None otherwise
        """
        # Check for CREATE TABLE/VIEW
        sql_upper = sql.upper()

        # Pattern: CREATE [OR REPLACE] TABLE/VIEW table_name AS
        match = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW)\s+(\w+)",
            sql_upper,
            re.IGNORECASE
        )

        if match:
            # Extract table/view name
            object_type = match.group(1)
            object_name = match.group(2)

            # Determine output path
            if object_type == "TABLE":
                output_dir = self.context.target_path / "tables"
            else:  # VIEW
                output_dir = self.context.target_path / "views"

            output_dir.mkdir(parents=True, exist_ok=True)

            # Return path (note: actual materialization depends on DuckDB config)
            return output_dir / f"{object_name}.parquet"

        return None

    def _clean_sql(self, sql: str) -> str:
        """
        Remove SQL comments and extra whitespace.

        Args:
            sql: SQL string to clean

        Returns:
            Cleaned SQL string
        """
        # Remove single-line comments (-- ...)
        lines = sql.split("\n")
        cleaned_lines = []
        for line in lines:
            # Find -- comment (but not inside strings)
            comment_pos = -1
            in_string = False
            string_char = None
            for i, char in enumerate(line):
                if char in ("'", '"') and (i == 0 or line[i-1] != "\\"):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                elif not in_string and char == "-" and i < len(line) - 1 and line[i+1] == "-":
                    comment_pos = i
                    break

            if comment_pos >= 0:
                line = line[:comment_pos]

            cleaned_lines.append(line)

        # Remove multi-line comments /* ... */
        sql = "\n".join(cleaned_lines)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        # Remove extra whitespace
        sql = "\n".join(
            line.strip() for line in sql.split("\n") if line.strip()
        )

        return sql

    def _validate_sql_syntax(self, sql: str) -> bool:
        """
        Basic SQL syntax validation.

        Checks for:
        - Balanced parentheses
        - Balanced quotes (single and double)

        Args:
            sql: SQL string to validate

        Returns:
            True if syntax appears valid, False otherwise
        """
        paren_count = 0
        single_quote_count = 0
        double_quote_count = 0

        for i, char in enumerate(sql):
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1
            elif char == "'" and (i == 0 or sql[i-1] != "\\"):
                single_quote_count += 1
            elif char == '"' and (i == 0 or sql[i-1] != "\\"):
                double_quote_count += 1

        # Check balanced
        if paren_count != 0:
            return False
        if single_quote_count % 2 != 0:
            return False
        if double_quote_count % 2 != 0:
            return False

        return True

    def _is_python_transform(self) -> bool:
        """
        Check if this transform node is from a Python file.

        Returns:
            True if the node's file path ends with .py, False otherwise
        """
        return self.node.file_path.endswith('.py')

    def _execute_python_transform(self) -> ExecutorResult:
        """
        Execute a Python-based transform node.

        Process:
        1. Import the Python module
        2. Execute the decorated function
        3. Write the returned DataFrame to intermediate storage
        4. Return ExecutorResult with metrics

        Returns:
            ExecutorResult with execution outcome

        Raises:
            ExecutorExecutionError: If Python execution fails
        """
        import importlib.util
        import sys
        from pathlib import Path as PathLib

        start_time = time.time()

        try:
            # Get the function name from node config
            function_name = self.node.name

            # Import the Python module
            file_path = PathLib(self.node.file_path)
            spec = importlib.util.spec_from_file_location(
                f"transform_{function_name}",
                str(file_path)
            )
            module = importlib.util.module_from_spec(spec)

            # Execute the module to load the function
            spec.loader.exec_module(module)

            # Get the function
            func = getattr(module, function_name)

            # Create PipelineContext for the function
            project_path = PathLib.cwd()
            target_path = project_path / "target"

            # Import PipelineContext
            sys.path.insert(0, str(PathLib(__file__).parent.parent.parent))
            from seeknal.pipeline.context import PipelineContext

            ctx = PipelineContext(
                project_path=project_path,
                target_dir=target_path,
                config={}
            )

            # Execute the function
            result = func(ctx)

            # Convert result to DataFrame if needed
            if result is None:
                raise ExecutorExecutionError(
                    self.node.id,
                    "Python function returned None. Expected a DataFrame."
                )

            # Ensure we have a DataFrame
            try:
                import pandas as pd
                if isinstance(result, pd.DataFrame):
                    df = result
                elif hasattr(result, 'df'):
                    df = result.df()
                else:
                    # Try to convert dict/list to DataFrame
                    df = pd.DataFrame(result)
            except ImportError:
                raise ExecutorExecutionError(
                    self.node.id,
                    "pandas is required for Python transforms. Install with: pip install pandas"
                )

            # Write to intermediate storage
            intermediate_path = target_path / "intermediate" / f"{self.node.id.replace('.', '_')}.parquet"
            intermediate_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                df.to_parquet(intermediate_path, index=False)
                if self.context.verbose:
                    _echo_info(f"Wrote Python output to {intermediate_path}")
            except Exception as e:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Failed to write output to intermediate storage: {e}"
                )

            # Create DuckDB view for Iceberg materialization
            # This enables post_execute() to materialize Python outputs to Iceberg
            try:
                con = self.context.get_duckdb_connection()
                view_name = f"transform.{self.node.name}"
                
                # Create schema if it doesn't exist
                con.execute("CREATE SCHEMA IF NOT EXISTS transform")
                
                # Create view from the Parquet file
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{intermediate_path}')")
                
                if self.context.verbose:
                    _echo_info(f"Created DuckDB view '{view_name}' for Iceberg materialization")
            except Exception as e:
                # View creation is not critical for Python execution
                # Log warning but don't fail
                import warnings
                warnings.warn(f"Failed to create DuckDB view for materialization: {e}")

            # Calculate duration
            duration = time.time() - start_time

            # Return success result
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=len(df),
                output_path=intermediate_path,
                metadata={
                    "engine": "python",
                    "intermediate_path": str(intermediate_path),
                    "columns": list(df.columns),
                }
            )

        except ExecutorExecutionError:
            raise
        except ImportError as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Missing dependency: {e}",
                e
            ) from e
        except AttributeError as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Function '{function_name}' not found in {file_path.name}: {e}",
                e
            ) from e
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Python execution failed: {str(e)}",
                e
            ) from e
        return True

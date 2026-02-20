"""
SOURCE node executor for Seeknal workflow execution.

This module implements the SourceExecutor class, which handles loading data
from various sources (CSV, database, Parquet, JSON) for workflow execution.

Design Decisions:
- Uses DuckDB for all data loading (efficient for CSV/Parquet/JSON)
- Supports both file paths and database connections
- Handles both absolute and relative paths
- Returns row counts and execution timing
- Validates inputs and handles errors gracefully
- Supports dry-run mode for preview without loading data

Key Components:
- SourceExecutor: Main executor class for SOURCE nodes
- Support for CSV, Parquet, JSON files
- Support for database connections (SQLite, PostgreSQL via DuckDB)
- Path resolution and validation
- Error handling with informative messages
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

from seeknal.dag.manifest import Node, NodeType  # ty: ignore[unresolved-import]
from seeknal.validation import validate_file_path  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.base import (  # ty: ignore[unresolved-import]
    BaseExecutor,
    ExecutionContext,
    ExecutionStatus,
    ExecutorResult,
    ExecutorExecutionError,
    ExecutorValidationError,
    register_executor,
)

# Materialization support
from seeknal.workflow.materialization.yaml_integration import materialize_node_if_enabled  # ty: ignore[unresolved-import]
from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher  # ty: ignore[unresolved-import]

logger = logging.getLogger(__name__)


@register_executor(NodeType.SOURCE)
class SourceExecutor(BaseExecutor):
    """
    Executor for SOURCE nodes.

    Loads data from various sources (CSV, database, Parquet, JSON)
    and returns execution results with row counts and timing.

    Supported source types:
    - CSV files (.csv)
    - Parquet files (.parquet)
    - JSON files (.json, .jsonl)
    - SQLite databases (.db, .sqlite, .sqlite3)
    - PostgreSQL databases (via ATTACH in DuckDB)
    - Hive tables (external databases)

    The executor reads the node configuration and uses DuckDB to load
    the data efficiently. It handles both absolute and relative paths,
    validates inputs, and provides informative error messages.

    Attributes:
        node: The SOURCE node to execute
        context: The execution context (paths, databases, config, etc.)

    Example:
        >>> executor = SourceExecutor(node, context)
        >>> result = executor.run()
        >>> if result.is_success():
        ...     print(f"Loaded {result.row_count} rows in {result.duration_seconds:.2f}s")
    """

    @property
    def node_type(self) -> NodeType:
        """Return the node type this executor handles."""
        return NodeType.SOURCE

    def validate(self) -> None:
        """
        Validate the SOURCE node configuration.

        Checks that:
        - Required fields are present (source, table)
        - Source type is supported
        - File path is valid (if file-based source)
        - Database configuration is valid (if database source)

        Raises:
            ExecutorValidationError: If node configuration is invalid
        """
        config = self.node.config

        # Check required fields
        if "source" not in config:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required field 'source' in node configuration"
            )

        source_type = config["source"]

        # PostgreSQL sources allow query: in params as alternative to table:
        has_table = bool(config.get("table"))
        params = config.get("params", {})
        has_query = isinstance(params, dict) and "query" in params

        if source_type in ("postgresql", "postgres"):
            if has_table and has_query:
                raise ExecutorValidationError(
                    self.node.id,
                    "Cannot specify both 'table' and 'query' — they are mutually exclusive"
                )
            if not has_table and not has_query:
                raise ExecutorValidationError(
                    self.node.id,
                    "PostgreSQL source requires either 'table' or 'query' in params"
                )
            if has_query:
                self._validate_pushdown_query(params["query"])
        elif not has_table:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required field 'table' in node configuration"
            )

        table = config.get("table", "")

        # Validate source type
        supported_sources = [
            "csv", "parquet", "json", "jsonl",
            "sqlite", "postgresql", "postgres", "hive",
            "starrocks", "iceberg",
            "bigquery", "snowflake", "redshift"
        ]

        if source_type not in supported_sources:
            raise ExecutorValidationError(
                self.node.id,
                f"Unsupported source type '{source_type}'. "
                f"Supported types: {', '.join(supported_sources)}"
            )

        # Validate file path for file-based sources (skip for remote sources)
        if source_type in ["csv", "parquet", "json", "jsonl", "sqlite"]:
            try:
                validate_file_path(table)
            except Exception as e:
                raise ExecutorValidationError(
                    self.node.id,
                    f"Invalid file path '{table}': {str(e)}"
                ) from e

        # Validate database configuration for database sources
        if source_type in ["postgresql", "postgres", "starrocks"]:
            params = config.get("params", {})

            if not isinstance(params, dict):
                raise ExecutorValidationError(
                    self.node.id,
                    "Invalid 'params' field: must be a dictionary"
                )

            # Check for required database parameters
            # (host, port, database, user, password can be in params or env vars)
            if not any(k in params for k in ["host", "database", "profile"]):
                # Will be loaded from environment or default
                pass

    def pre_execute(self) -> None:
        """
        Perform setup before SOURCE node execution.

        Creates the output directory for the node's results.
        """
        output_path = self.context.get_output_path(self.node)
        output_path.mkdir(parents=True, exist_ok=True)

    def execute(self) -> ExecutorResult:
        """
        Execute the SOURCE node and load data.

        This method:
        1. Determines the source type and table/path
        2. Uses DuckDB to load the data
        3. Counts the rows loaded
        4. Optionally writes results to cache
        5. Returns ExecutorResult with timing and row count

        For dry-run mode, it validates the configuration but doesn't
        actually load the data.

        Returns:
            ExecutorResult with execution outcome

        Raises:
            ExecutorExecutionError: If data loading fails
        """
        start_time = time.time()

        # Handle dry-run mode
        if self.context.dry_run:
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=0.0,
                row_count=0,
                is_dry_run=True,
                metadata={
                    "source": self.node.config.get("source"),
                    "table": self.node.config.get("table"),
                    "dry_run": True
                }
            )

        config = self.node.config
        source_type = config["source"]
        table = config.get("table", "")
        params = config.get("params", {})

        try:
            # Get DuckDB connection
            con = self.context.get_duckdb_connection()

            # Load data based on source type
            if source_type == "csv":
                row_count = self._load_csv(con, table, params)
            elif source_type == "parquet":
                row_count = self._load_parquet(con, table, params)
            elif source_type == "json" or source_type == "jsonl":
                row_count = self._load_json(con, table, params)
            elif source_type in ["sqlite", "postgresql", "postgres"]:
                row_count = self._load_database(con, source_type, table, params)
            elif source_type == "starrocks":
                row_count = self._load_starrocks(con, table, params)
            elif source_type == "hive":
                row_count = self._load_hive(con, table, params)
            elif source_type == "iceberg":
                row_count = self._load_iceberg(con, table, params)
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unsupported source type: {source_type}"
                )

            duration = time.time() - start_time

            # Cache the result if enabled (for YAML pipeline reuse)
            cache_path = self.context.get_cache_path(self.node)
            # Also write to intermediate/ directory (for Python ctx.ref() compatibility)
            intermediate_path = self.context.target_path / "intermediate" / f"{self.node.id.replace('.', '_')}.parquet"
            
            if cache_path or intermediate_path:
                try:
                    # Create cache directories if they don't exist
                    if cache_path:
                        cache_path.parent.mkdir(parents=True, exist_ok=True)
                    if intermediate_path:
                        intermediate_path.parent.mkdir(parents=True, exist_ok=True)

                    # Get schema-qualified view name for caching
                    node_id = self.node.id
                    if "." in node_id:
                        schema, view_name = node_id.split(".", 1)
                        qualified_view = f"{schema}.{view_name}"
                    else:
                        qualified_view = "loaded_data"
                    
                    # Write to parquet cache (for YAML pipeline reuse)
                    if cache_path:
                        con.execute(f"COPY (SELECT * FROM {qualified_view}) TO '{cache_path}' (FORMAT PARQUET)")
                    
                    # Write to intermediate directory (for Python ctx.ref() compatibility)
                    if intermediate_path:
                        con.execute(f"COPY (SELECT * FROM {qualified_view}) TO '{intermediate_path}' (FORMAT PARQUET)")
                except Exception as e:
                    # Cache failure is not critical
                    if self.context.verbose:
                        import warnings
                        warnings.warn(f"Failed to cache result: {str(e)}")

            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=row_count,
                output_path=self.context.get_output_path(self.node),
                metadata={
                    "source": source_type,
                    "table": table,
                    "params": params,
                    "cached": cache_path.exists() if cache_path else False,
                    "intermediate_path": str(intermediate_path) if intermediate_path else None,
                }
            )

        except ExecutorExecutionError:
            raise
        except FileNotFoundError as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Source file not found: {str(e)}",
                e
            ) from e
        except PermissionError as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Permission denied accessing source: {str(e)}",
                e
            ) from e
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load data from {source_type}: {str(e)}",
                e
            ) from e

    def _resolve_path(self, path: str) -> str:
        """
        Resolve a file path to absolute path.

        Handles both absolute and relative paths.
        Relative paths are resolved relative to the current working directory.

        Args:
            path: File path to resolve

        Returns:
            Absolute path as string
        """
        path_obj = Path(path)

        if not path_obj.is_absolute():
            # Resolve relative to current working directory
            path_obj = Path.cwd() / path_obj

        return str(path_obj.resolve())

    def _load_csv(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from CSV file using DuckDB.

        Args:
            con: DuckDB connection
            table: CSV file path
            params: Additional parameters (delimiter, header, etc.)

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        abs_path = self._resolve_path(table)

        # Create schema and view names from node qualified name
        # Node ID is like "source.customers" -> schema="source", view="customers"
        node_id = self.node.id
        if "." in node_id:
            schema, view_name = node_id.split(".", 1)
        else:
            schema, view_name = "source", node_id

        # Create schema if it doesn't exist
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Build DuckDB read query with options
        # Use read_csv_auto for auto-detection, or read_csv with custom options
        if params:
            read_options = self._build_csv_options(params)
            if read_options:
                # Use read_csv when custom options are provided
                query = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM read_csv('{abs_path}'{read_options})"
            else:
                # No valid options, use auto-detection
                query = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM read_csv_auto('{abs_path}')"
        else:
            # Use auto-detection when no params
            query = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM read_csv_auto('{abs_path}')"

        try:
            # Create view for the CSV data
            con.execute(query)

            # Count rows
            count_result = con.execute(f"SELECT COUNT(*) FROM {schema}.{view_name}").fetchone()
            row_count = count_result[0] if count_result else 0

            return row_count

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load CSV from '{abs_path}': {str(e)}",
                e
            ) from e

    def _build_csv_options(self, params: Dict[str, Any]) -> str:
        """
        Build DuckDB CSV read options from parameters.

        Args:
            params: Dictionary of CSV parameters

        Returns:
            DuckDB options string (e.g., ", delim = ',', header = true")
        """
        options = []

        # Common CSV options (using DuckDB parameter names)
        # Note: DuckDB uses 'delim' not 'delimiter', and 'sep' is an alias
        if "delimiter" in params:
            delimiter = params["delimiter"]
            # Escape single quotes in delimiter
            delimiter_escaped = delimiter.replace("'", "''")
            options.append(f", delim = '{delimiter_escaped}'")
        elif "delim" in params:
            delim = params["delim"]
            delim_escaped = delim.replace("'", "''")
            options.append(f", delim = '{delim_escaped}'")

        if "header" in params:
            header = params["header"]
            # Convert boolean to DuckDB format
            header_val = "true" if header else "false"
            options.append(f", header = {header_val}")

        if "skip" in params:
            skip = params["skip"]
            options.append(f", skip = {skip}")

        return "".join(options)

    def _load_parquet(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from Parquet file using DuckDB.

        Args:
            con: DuckDB connection
            table: Parquet file path
            params: Additional parameters

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        abs_path = self._resolve_path(table)

        # Create schema and view names from node qualified name
        node_id = self.node.id
        if "." in node_id:
            schema, view_name = node_id.split(".", 1)
        else:
            schema, view_name = "source", node_id

        # Create schema if it doesn't exist
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        try:
            # Create view for the Parquet data
            query = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM read_parquet('{abs_path}')"
            con.execute(query)

            # Count rows
            count_result = con.execute(f"SELECT COUNT(*) FROM {schema}.{view_name}").fetchone()
            row_count = count_result[0] if count_result else 0

            return row_count

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load Parquet from '{abs_path}': {str(e)}",
                e
            ) from e

    def _load_json(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from JSON file using DuckDB.

        Args:
            con: DuckDB connection
            table: JSON file path
            params: Additional parameters

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        abs_path = self._resolve_path(table)

        # Create schema and view names from node qualified name
        node_id = self.node.id
        if "." in node_id:
            schema, view_name = node_id.split(".", 1)
        else:
            schema, view_name = "source", node_id

        # Create schema if it doesn't exist
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Determine JSON format
        json_format = params.get("format", "auto")  # auto, array, newline

        try:
            # Build read_json query with format option
            if json_format == "newline":
                query = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM read_json_auto('{abs_path}', format='newline_delimited')"
            elif json_format == "array":
                query = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM read_json_auto('{abs_path}', format='array')"
            else:  # auto
                query = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM read_json_auto('{abs_path}')"

            con.execute(query)

            # Count rows
            count_result = con.execute(f"SELECT COUNT(*) FROM {schema}.{view_name}").fetchone()
            row_count = count_result[0] if count_result else 0

            return row_count

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load JSON from '{abs_path}': {str(e)}",
                e
            ) from e

    def _load_database(
        self,
        con: Any,
        source_type: str,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from database using DuckDB.

        Supports:
        - SQLite databases (ATTACH)
        - PostgreSQL databases (ATTACH with postgres extension)

        Args:
            con: DuckDB connection
            source_type: Type of database (sqlite, postgresql, postgres)
            table: Table name (or file path for SQLite)
            params: Connection parameters

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        try:
            if source_type == "sqlite":
                return self._load_sqlite(con, table, params)
            elif source_type in ["postgresql", "postgres"]:
                return self._load_postgresql(con, table, params)
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unsupported database type: {source_type}"
                )

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load from {source_type} database: {str(e)}",
                e
            ) from e

    def _load_sqlite(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from SQLite database using DuckDB.

        Args:
            con: DuckDB connection
            table: Table name in SQLite database
            params: Connection parameters (path, schema)

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        # Get SQLite database path
        db_path = params.get("path", table)

        # If table looks like a file path, use it as db_path
        if table.endswith((".db", ".sqlite", ".sqlite3")):
            db_path = table
            # Use table from params or default to main table
            table = params.get("table", "main")

        abs_path = self._resolve_path(db_path)
        schema = params.get("schema", "main")

        try:
            # Attach SQLite database
            con.execute(f"ATTACH '{abs_path}' AS {schema}_db (TYPE sqlite)")

            # Create view from SQLite table
            full_table_name = f"{schema}_db.{table}" if schema != "main" else f"{schema}_db.{table}"
            query = f"CREATE OR REPLACE VIEW loaded_data AS SELECT * FROM {full_table_name}"
            con.execute(query)

            # Count rows
            count_result = con.execute("SELECT COUNT(*) FROM loaded_data").fetchone()
            row_count = count_result[0] if count_result else 0

            return row_count

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load from SQLite database '{abs_path}', table '{table}': {str(e)}",
                e
            ) from e

    # DDL/DML keywords that must NOT appear at the start of pushdown queries
    _FORBIDDEN_SQL_PREFIXES = (
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
        "TRUNCATE", "GRANT", "REVOKE", "COPY", "VACUUM", "ANALYZE",
    )

    def _validate_pushdown_query(self, query: str) -> None:
        """Validate a pushdown SQL query for safety.

        Rules:
        - Must start with SELECT or WITH (case-insensitive)
        - Must not start with DDL/DML keywords

        Args:
            query: Raw SQL string from params

        Raises:
            ExecutorValidationError: If query is unsafe
        """
        stripped = query.strip()
        if not stripped:
            raise ExecutorValidationError(
                self.node.id,
                "Pushdown query is empty"
            )

        first_word = stripped.split()[0].upper()

        if first_word in self._FORBIDDEN_SQL_PREFIXES:
            raise ExecutorValidationError(
                self.node.id,
                f"Pushdown query must be a read-only SELECT/WITH statement, "
                f"got '{first_word}' (DDL/DML not allowed)"
            )

        if first_word not in ("SELECT", "WITH"):
            raise ExecutorValidationError(
                self.node.id,
                f"Pushdown query must start with SELECT or WITH, got '{first_word}'"
            )

    def _resolve_postgresql_config(self, params: Dict[str, Any]) -> Any:
        """Resolve PostgreSQL config from profile connection or inline params.

        If ``params`` contains a ``connection:`` key, loads the named profile
        and merges inline params as overrides. Otherwise uses inline params
        directly.

        Args:
            params: Source node params dict

        Returns:
            PostgreSQLConfig instance
        """
        from seeknal.connections.postgresql import parse_postgresql_config  # ty: ignore[unresolved-import]

        connection_name = params.get("connection")
        if connection_name:
            from seeknal.workflow.materialization.profile_loader import ProfileLoader  # ty: ignore[unresolved-import]

            profile_path = getattr(self.context, 'profile_path', None)
            loader = ProfileLoader(profile_path=profile_path)
            profile = loader.load_connection_profile(connection_name)
            # Inline params override profile defaults
            for key in ("host", "port", "database", "user", "password", "schema",
                        "sslmode", "connect_timeout"):
                if key in params:
                    profile[key] = params[key]
            config = parse_postgresql_config(profile)
            loader.clear_connection_credentials(profile)
        else:
            config = parse_postgresql_config(params)

        return config

    def _load_postgresql(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from PostgreSQL database using DuckDB.

        Supports two connection modes:
        1. **Profile-based**: ``params: { connection: my_pg }`` resolves from
           ``profiles.yml`` connections section. Inline params override profile.
        2. **Inline**: ``params: { host, port, database, user, password }``
           (existing behavior, still works).

        Requires the postgres extension to be loaded.

        Args:
            con: DuckDB connection
            table: Table name in PostgreSQL database
            params: Connection parameters or connection profile reference

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        from seeknal.connections.postgresql import mask_password  # ty: ignore[unresolved-import]

        config = self._resolve_postgresql_config(params)

        # Create schema-qualified view name from node ID (consistent with CSV loader)
        node_id = self.node.id
        if "." in node_id:
            duckdb_schema, view_name = node_id.split(".", 1)
        else:
            duckdb_schema, view_name = "source", node_id

        conn_str = config.to_libpq_string()

        try:
            # Load postgres extension
            try:
                con.execute("LOAD postgres")
            except Exception:
                con.execute("INSTALL postgres; LOAD postgres;")

            # Create DuckDB schema for the view
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {duckdb_schema}")

            # Attach PostgreSQL database (use unique alias to avoid conflicts)
            pg_alias = f"pg_{view_name}"
            try:
                con.execute(f"""
                ATTACH '{conn_str}'
                AS {pg_alias} (TYPE postgres)
                """)
            except Exception as e:
                # May already be attached from a previous source
                if "already exists" not in str(e):
                    raise

            qualified_view = f"{duckdb_schema}.{view_name}"

            # Use postgres_query() for pushdown queries, or table scan
            pushdown_query = params.get("query")
            if pushdown_query:
                # Pushdown: run arbitrary SELECT on the remote PostgreSQL
                escaped_query = pushdown_query.replace("'", "''")
                con.execute(
                    f"CREATE OR REPLACE VIEW {qualified_view} AS "
                    f"SELECT * FROM postgres_query('{pg_alias}', '{escaped_query}')"
                )
            else:
                # Table scan: SELECT * FROM attached table
                # If table already contains schema (e.g., "public.customers"),
                # use it directly; otherwise prepend config.schema
                if "." in table:
                    full_table_name = f"{pg_alias}.{table}"
                else:
                    full_table_name = f"{pg_alias}.{config.schema}.{table}"
                con.execute(
                    f"CREATE OR REPLACE VIEW {qualified_view} AS "
                    f"SELECT * FROM {full_table_name}"
                )

            # Count rows
            count_result = con.execute(f"SELECT COUNT(*) FROM {qualified_view}").fetchone()
            row_count = count_result[0] if count_result else 0

            return row_count

        except ExecutorExecutionError:
            raise
        except Exception as e:
            safe_msg = mask_password(str(e))
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load from PostgreSQL '{config.host}:{config.port}/{config.database}', "
                f"table '{table}': {safe_msg}",
                e
            ) from e

    def _load_starrocks(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from StarRocks into DuckDB view via pymysql.

        Queries StarRocks via MySQL protocol, fetches results,
        and creates a DuckDB view from the data.

        Args:
            con: DuckDB connection
            table: Table name in StarRocks
            params: Connection parameters (host, port, user, password, database, profile, query)

        Returns:
            Number of rows loaded
        """
        try:
            from seeknal.connections.starrocks import create_starrocks_connection  # ty: ignore[unresolved-import]
        except ImportError:
            raise ExecutorExecutionError(
                self.node.id,
                "pymysql is required for StarRocks sources. Install with: pip install pymysql"
            )

        # Build connection config from params
        conn_config = {
            "host": params.get("host", "localhost"),
            "port": params.get("port", 9030),
            "user": params.get("user", "root"),
            "password": params.get("password", ""),
            "database": params.get("database", ""),
        }

        # Create schema and view names from node qualified name
        node_id = self.node.id
        if "." in node_id:
            schema, view_name = node_id.split(".", 1)
        else:
            schema, view_name = "source", node_id

        try:
            sr_conn = create_starrocks_connection(conn_config)
            cursor = sr_conn.cursor()

            # Use custom query or default SELECT *
            query = params.get("query", f"SELECT * FROM {table}")
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            sr_conn.close()

            if not rows:
                # Create empty view with correct schema
                col_defs = ", ".join(f'NULL AS "{col}"' for col in columns)
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                con.execute(
                    f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT {col_defs} WHERE FALSE"
                )
                return 0

            # Create DuckDB view from fetched data
            import pandas as pd  # ty: ignore[unresolved-import]
            df = pd.DataFrame(rows, columns=columns)

            # Register as DuckDB table then create view
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            con.register(f"_sr_temp_{view_name}", df)
            con.execute(
                f"CREATE OR REPLACE VIEW {schema}.{view_name} AS SELECT * FROM _sr_temp_{view_name}"
            )

            return len(rows)

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load from StarRocks table '{table}': {str(e)}",
                e
            ) from e

    def _load_iceberg(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from an Iceberg table via Lakekeeper REST catalog.

        Uses DuckDB's iceberg extension to ATTACH a Lakekeeper catalog
        and create a view from the specified Iceberg table.

        The table name must be a 3-part format: catalog.namespace.table
        (e.g., atlas.my_namespace.my_table).

        Connection details can be provided via params or environment variables:
        - catalog_uri: Lakekeeper URL (env: LAKEKEEPER_URL)
        - warehouse: Warehouse name (env: LAKEKEEPER_WAREHOUSE, default: seeknal-warehouse)

        S3 and OAuth2 credentials are always read from environment variables:
        - AWS_ENDPOINT_URL, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        - KEYCLOAK_TOKEN_URL, KEYCLOAK_CLIENT_ID, KEYCLOAK_CLIENT_SECRET

        Args:
            con: DuckDB connection
            table: Fully qualified Iceberg table name (catalog.namespace.table)
            params: Optional connection parameters (catalog_uri, warehouse)

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        import os
        import json
        import urllib.request

        # Validate 3-part table name
        parts = table.split(".")
        if len(parts) != 3:
            raise ExecutorExecutionError(
                self.node.id,
                f"Iceberg table must be 3-part format 'catalog.namespace.table', "
                f"got '{table}' ({len(parts)} parts)"
            )

        catalog_alias, namespace, table_name = parts

        # Get connection config from params or env vars
        catalog_uri = params.get(
            "catalog_uri",
            os.getenv("LAKEKEEPER_URL", "")
        )
        if not catalog_uri:
            raise ExecutorExecutionError(
                self.node.id,
                "Iceberg source requires 'catalog_uri' in params or "
                "LAKEKEEPER_URL environment variable"
            )

        warehouse = params.get(
            "warehouse",
            os.getenv("LAKEKEEPER_WAREHOUSE", "seeknal-warehouse")
        )

        # S3 credentials from env
        minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "")
        s3_region = os.getenv("AWS_REGION", "us-east-1")
        s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
        s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")

        # OAuth2 credentials from env
        token_url = os.getenv("KEYCLOAK_TOKEN_URL", "")
        client_id = os.getenv("KEYCLOAK_CLIENT_ID", "")
        client_secret = os.getenv("KEYCLOAK_CLIENT_SECRET", "")

        # Create schema and view names from node qualified name
        node_id = self.node.id
        if "." in node_id:
            schema, view_name = node_id.split(".", 1)
        else:
            schema, view_name = "source", node_id

        try:
            # Install and load required extensions
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
            con.execute("INSTALL iceberg")
            con.execute("LOAD iceberg")

            # Configure S3 if endpoint is provided
            if minio_endpoint:
                # Strip protocol for endpoint
                endpoint = minio_endpoint.replace("http://", "").replace("https://", "")
                con.execute(f"SET s3_region = '{s3_region}'")
                con.execute(f"SET s3_endpoint = '{endpoint}'")
                con.execute("SET s3_url_style = 'path'")
                con.execute("SET s3_use_ssl = false")
                con.execute(f"SET s3_access_key_id = '{s3_access_key}'")
                con.execute(f"SET s3_secret_access_key = '{s3_secret_key}'")

            # Get OAuth2 token
            if token_url and client_id and client_secret:
                token_data = (
                    f"grant_type=client_credentials"
                    f"&client_id={client_id}"
                    f"&client_secret={client_secret}"
                ).encode()
                req = urllib.request.Request(token_url, data=token_data)
                token_response = json.loads(
                    urllib.request.urlopen(req).read()
                )
                token = token_response["access_token"]
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    "Iceberg source requires OAuth2 credentials: "
                    "KEYCLOAK_TOKEN_URL, KEYCLOAK_CLIENT_ID, KEYCLOAK_CLIENT_SECRET"
                )

            # Build catalog URL
            base_url = catalog_uri.rstrip("/")
            if "/catalog" not in base_url:
                catalog_url = f"{base_url}/catalog"
            else:
                catalog_url = base_url

            # Attach Lakekeeper catalog (skip if already attached)
            try:
                con.execute(f"""
                    ATTACH '{warehouse}' AS {catalog_alias} (
                        TYPE ICEBERG,
                        ENDPOINT '{catalog_url}',
                        AUTHORIZATION_TYPE 'oauth2',
                        TOKEN '{token}'
                    )
                """)
            except Exception as attach_err:
                # Catalog may already be attached from a previous source
                if "already exists" not in str(attach_err).lower():
                    raise

            # Create local schema and view from the Iceberg table
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            con.execute(
                f"CREATE OR REPLACE VIEW {schema}.{view_name} AS "
                f"SELECT * FROM {catalog_alias}.{namespace}.{table_name}"
            )

            # Count rows
            count_result = con.execute(
                f"SELECT COUNT(*) FROM {schema}.{view_name}"
            ).fetchone()
            row_count = count_result[0] if count_result else 0

            return row_count

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load from Iceberg table '{table}': {str(e)}",
                e
            ) from e

    def _load_hive(
        self,
        con: Any,
        table: str,
        params: Dict[str, Any]
    ) -> int:
        """
        Load data from Hive table using DuckDB.

        This is a placeholder for future Hive integration.
        Currently, it treats the table as a file path.

        Args:
            con: DuckDB connection
            table: Hive table name or path
            params: Additional parameters

        Returns:
            Number of rows loaded

        Raises:
            ExecutorExecutionError: If loading fails
        """
        # For now, treat as file path
        # Future implementation can use Hive ATTACH
        abs_path = self._resolve_path(table)

        try:
            # Try to detect file type and load
            if abs_path.endswith(".parquet"):
                return self._load_parquet(con, abs_path, params)
            elif abs_path.endswith(".csv"):
                return self._load_csv(con, abs_path, params)
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unsupported file format for Hive table: {abs_path}"
                )

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to load from Hive table '{table}': {str(e)}",
                e
            ) from e

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Perform cleanup after SOURCE node execution.

        Adds additional metadata to the result and handles Iceberg materialization
        if enabled in the node configuration.

        Args:
            result: The result from execute()

        Returns:
            Modified ExecutorResult with additional metadata
        """
        # Add executor version
        result.metadata["executor_version"] = "1.0.0"

        # Add source type if not present
        if "source" not in result.metadata:
            result.metadata["source"] = self.node.config.get("source", "unknown")

        # Handle materialization if enabled
        if result.status == ExecutionStatus.SUCCESS and not result.is_dry_run:
            # Check for multi-target materializations (new dispatcher path)
            mat_targets = self.node.config.get("materializations", [])
            if mat_targets and self.context.materialize_enabled is not False:
                try:
                    con = self.context.get_duckdb_connection()
                    view_name = f"{self.node.node_type.value}.{self.node.name}"
                    profile_path = getattr(self.context, 'profile_path', None)
                    from seeknal.workflow.materialization.profile_loader import ProfileLoader  # ty: ignore[unresolved-import]
                    loader = ProfileLoader(profile_path=profile_path) if profile_path else None
                    dispatcher = MaterializationDispatcher(profile_loader=loader)
                    dispatch_result = dispatcher.dispatch(
                        con=con,
                        view_name=view_name,
                        targets=mat_targets,
                        node_id=self.node.id,
                    )
                    result.metadata["materialization"] = {
                        "enabled": True,
                        "success": dispatch_result.all_succeeded,
                        "total": dispatch_result.total,
                        "succeeded": dispatch_result.succeeded,
                        "failed": dispatch_result.failed,
                        "results": dispatch_result.results,
                    }
                    if dispatch_result.all_succeeded:
                        logger.info(
                            f"Materialized node '{self.node.id}' to "
                            f"{dispatch_result.succeeded}/{dispatch_result.total} targets"
                        )
                    else:
                        logger.warning(
                            f"Materialized node '{self.node.id}': "
                            f"{dispatch_result.succeeded}/{dispatch_result.total} succeeded"
                        )
                except Exception as e:
                    logger.warning(
                        f"Failed to materialize node '{self.node.id}': {e}"
                    )
                    result.metadata["materialization"] = {
                        "enabled": True,
                        "success": False,
                        "error": str(e),
                    }
            elif not mat_targets:
                # Fallback: legacy Iceberg-only path (singular materialization config)
                try:
                    con = self.context.get_duckdb_connection()
                    mat_result = materialize_node_if_enabled(
                        self.node,
                        source_con=con,
                        enabled_override=self.context.materialize_enabled
                    )
                    if mat_result:
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
                    logger.warning(
                        f"Failed to materialize node '{self.node.id}' to Iceberg: {e}"
                    )
                    result.metadata["materialization"] = {
                        "enabled": True,
                        "success": False,
                        "error": str(e),
                    }

        return result

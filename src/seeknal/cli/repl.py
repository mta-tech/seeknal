"""Minimal SQL REPL for seeknal using DuckDB.

Uses DuckDB scanner extensions to query postgres, mysql, sqlite databases
and native DuckDB for parquet/csv files.
"""
from __future__ import annotations

import os
import re
import readline
from pathlib import Path
from typing import Optional

import duckdb
from tabulate import tabulate

from seeknal.utils.path_security import is_insecure_path
from seeknal.db_utils import sanitize_error_message


# SQL operation allowlist for security
ALLOWED_SQL_PATTERNS = [
    r'^\s*SELECT\s+',           # SELECT queries
    r'^\s*WITH\s+',             # CTEs (Common Table Expressions)
    r'^\s*EXPLAIN\s+',          # Query explain plans
    r'^\s*DESCRIBE\s+',         # Table descriptions
    r'^\s*SHOW\s+',             # Show commands
    r'^\s*PRAGMA\s+',           # SQLite/DuckDB pragma commands
    r'^\s*DESC\s+',             # Short for DESCRIBE
]

# Dangerous SQL keywords that are NOT allowed in REPL
DANGEROUS_SQL_KEYWORDS = [
    'DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE',
    'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE', 'EXECUTE',
    'CALL', 'COPY', 'IMPORT', 'EXPORT'
]


def validate_sql(sql: str) -> tuple[bool, str]:
    """Validate SQL query against security rules.

    Args:
        sql: The SQL query to validate

    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if query passes validation
        - error_message: Empty string if valid, error description if invalid
    """
    # Remove leading/trailing whitespace for checking
    sql_stripped = sql.strip()

    # Check for empty query
    if not sql_stripped:
        return True, ""

    # Check for dangerous keywords (case-insensitive)
    sql_upper = sql_stripped.upper()
    for keyword in DANGEROUS_SQL_KEYWORDS:
        # Use word boundary to avoid false positives (e.g., "column_name" shouldn't match "UPDATE")
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_upper):
            return False, (
                f"Dangerous keyword '{keyword}' not allowed in REPL. "
                f"REPL is read-only for data exploration."
            )

    # Check against allowlist - query must start with allowed pattern
    is_allowed = any(re.match(pattern, sql_stripped, re.IGNORECASE) for pattern in ALLOWED_SQL_PATTERNS)
    if not is_allowed:
        return False, (
            "Only SELECT and read-only queries are allowed in REPL. "
            "Allowed: SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, PRAGMA."
        )

    return True, ""


# Scanner extensions to load
EXTENSIONS = ["postgres", "mysql", "sqlite"]


class REPL:
    """Simple SQL REPL using DuckDB as the single query engine."""

    def __init__(
        self,
        project_path: Optional[Path] = None,
        profile_path: Optional[Path] = None,
        env_name: Optional[str] = None,
        skip_history: bool = False,
    ) -> None:
        self.conn = duckdb.connect(":memory:")
        self.attached: set[str] = set()
        self._starrocks_connections: dict = {}
        self._active_starrocks: Optional[str] = None
        self.project_path = project_path
        self.profile_path = profile_path
        self.env_name = env_name
        self._registered_parquets: int = 0
        self._registered_pg: int = 0
        self._registered_iceberg: int = 0
        self._node_count: int = 0
        self._last_run: Optional[str] = None
        self._load_extensions()
        if not skip_history:
            self._setup_history()
        if self.project_path:
            self._load_run_state()
            self._auto_register_project()

    def _load_extensions(self) -> None:
        """Install and load DuckDB scanner extensions."""
        for ext in EXTENSIONS:
            try:
                self.conn.execute(f"INSTALL {ext}")
                self.conn.execute(f"LOAD {ext}")
            except Exception:
                pass  # Extension may already be installed or unavailable

    def _setup_history(self) -> None:
        """Configure readline history."""
        history_dir = Path.home() / ".seeknal"
        history_dir.mkdir(parents=True, exist_ok=True)
        self._history_path = history_dir / ".repl_history"

        try:
            if self._history_path.exists():
                readline.read_history_file(str(self._history_path))
            readline.set_history_length(1000)
        except Exception:
            pass

    def _load_run_state(self) -> None:
        """Load run state to get node count and last run timestamp.

        When env_name is set, tries the environment's run_state first,
        falls back to production state.
        """
        if self.env_name:
            env_state = self.project_path / "target" / "environments" / self.env_name / "run_state.json"
            if env_state.exists():
                try:
                    import json
                    with open(env_state) as f:
                        data = json.load(f)
                    self._node_count = len(data.get("nodes", {}))
                    self._last_run = data.get("last_run")
                    return
                except Exception:
                    pass
        state_path = self.project_path / "target" / "run_state.json"
        if not state_path.exists():
            return
        try:
            import json
            with open(state_path) as f:
                data = json.load(f)
            self._node_count = len(data.get("nodes", {}))
            self._last_run = data.get("last_run")
        except Exception:
            pass  # Best-effort

    def _auto_register_project(self) -> None:
        """Auto-register queryable data from the project context.

        Three phases, each best-effort (failures emit warnings):
        1. Register intermediate parquets from target/intermediate/ and target/cache/ as views
        2. Attach PostgreSQL connections read-only via DuckDB ATTACH
        3. Attach Iceberg catalogs via DuckDB Iceberg extension
        """
        import warnings

        # Phase 1: Register intermediate parquets
        try:
            self._register_parquets()
        except Exception as e:
            warnings.warn(f"REPL: Failed to register parquets: {e}")

        # Phase 1b: Register consolidated entity parquets
        try:
            self._register_consolidated_entities()
        except Exception as e:
            warnings.warn(f"REPL: Failed to register consolidated entities: {e}")

        # Phase 2: Attach PostgreSQL connections
        try:
            self._register_postgresql_connections()
        except Exception as e:
            warnings.warn(f"REPL: Failed to attach PostgreSQL connections: {e}")

        # Phase 3: Attach Iceberg catalogs
        try:
            self._register_iceberg_catalogs()
        except Exception as e:
            warnings.warn(f"REPL: Failed to attach Iceberg catalogs: {e}")

    def _register_parquets(self) -> None:
        """Phase 1: Register intermediate parquets as DuckDB views.

        Scans both target/intermediate/ (primary, written by all executors)
        and target/cache/ (legacy) for parquet files.

        When env_name is set, scans the environment's intermediate/ first
        (changed nodes), then falls back to production intermediate/ and
        cache/ for unchanged nodes. Env outputs take priority.

        Files in target/intermediate/ use naming convention
        {kind}_{name}.parquet (e.g. transform_orders_cleaned.parquet).
        The kind prefix is kept to avoid collisions between nodes of
        different types with the same name (e.g. source_products vs
        transform_products).
        """
        registered_names: set[str] = set()

        # If in an environment, scan env intermediate first (changed nodes)
        if self.env_name:
            env_intermediate = (
                self.project_path / "target" / "environments"
                / self.env_name / "intermediate"
            )
            if env_intermediate.exists():
                self._scan_parquet_dir(env_intermediate, registered_names)

        # Primary: target/intermediate/ (all executor outputs go here)
        intermediate_dir = self.project_path / "target" / "intermediate"
        if intermediate_dir.exists():
            self._scan_parquet_dir(intermediate_dir, registered_names)

        # Legacy: target/cache/ (backward compat — files have no kind prefix)
        cache_dir = self.project_path / "target" / "cache"
        if cache_dir.exists():
            self._scan_parquet_dir(cache_dir, registered_names)

    def _scan_parquet_dir(
        self, directory: Path, registered_names: set
    ) -> None:
        """Register parquet files from a directory as DuckDB views.

        View names use the file stem directly (e.g. transform_orders_cleaned.parquet
        becomes view ``transform_orders_cleaned``). The kind prefix is kept so that
        nodes of different types with the same name don't collide.

        Args:
            directory: Directory to scan for .parquet files.
            registered_names: Already-registered view names (skipped, updated in place).
        """
        for parquet_file in directory.rglob("*.parquet"):
            view_name = parquet_file.stem
            if view_name in registered_names:
                continue
            safe_path = str(parquet_file.resolve()).replace("'", "''")
            try:
                self.conn.execute(
                    f"CREATE VIEW \"{view_name}\" AS "
                    f"SELECT * FROM read_parquet('{safe_path}')"
                )
                self._registered_parquets += 1
                registered_names.add(view_name)
            except Exception:
                pass  # Skip files that can't be read

    def _register_consolidated_entities(self) -> None:
        """Phase 1b: Register consolidated entity parquets as DuckDB views.

        Scans target/feature_store/{entity}/features.parquet and registers
        each as a view named 'entity_{name}' (e.g. 'entity_customer').
        """
        feature_store_dir = self.project_path / "target" / "feature_store"
        if not feature_store_dir or not feature_store_dir.exists():
            return

        for entity_dir in feature_store_dir.iterdir():
            if not entity_dir.is_dir():
                continue
            parquet_path = entity_dir / "features.parquet"
            if not parquet_path.exists():
                continue

            view_name = f"entity_{entity_dir.name}"
            safe_path = str(parquet_path.resolve()).replace("'", "''")
            try:
                self.conn.execute(
                    f'CREATE VIEW "{view_name}" AS '
                    f"SELECT * FROM read_parquet('{safe_path}')"
                )
                self._registered_parquets += 1
            except Exception:
                pass  # Skip files that can't be read

    def _register_postgresql_connections(self) -> None:
        """Phase 2: Attach PostgreSQL connections read-only via DuckDB ATTACH."""
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        loader = ProfileLoader(profile_path=self.profile_path)
        profile_data = loader._load_profile_data()
        connections = profile_data.get("connections", {})

        for name, config in connections.items():
            conn_type = config.get("type", "")
            if conn_type not in ("postgresql", "postgres"):
                continue

            try:
                from seeknal.workflow.materialization.profile_loader import (
                    interpolate_env_vars_in_dict,
                )

                resolved = interpolate_env_vars_in_dict(config)
                host = resolved.get("host", "localhost")
                port = resolved.get("port", 5432)
                user = resolved.get("user", "")
                password = resolved.get("password", "")
                database = resolved.get("database", "")

                conn_str = (
                    f"host={host} port={port} dbname={database} "
                    f"user={user} password={password} "
                    f"connect_timeout=5"
                )
                self.conn.execute(
                    f"ATTACH '{conn_str}' AS \"{name}\" (TYPE postgres, READ_ONLY)"
                )
                self.attached.add(name)
                self._registered_pg += 1
            except Exception:
                pass  # Best-effort: skip connections that fail

    def _register_iceberg_catalogs(self) -> None:
        """Phase 3: Attach Iceberg catalogs via DuckDB Iceberg extension."""
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        loader = ProfileLoader(profile_path=self.profile_path)
        profile_data = loader._load_profile_data()
        mat_config = profile_data.get("materialization", {})
        catalog_data = mat_config.get("catalog", {})

        if not catalog_data or catalog_data.get("type", "rest") != "rest":
            return

        from seeknal.workflow.materialization.profile_loader import (
            interpolate_env_vars_in_dict,
        )

        resolved = interpolate_env_vars_in_dict(catalog_data)
        uri = resolved.get("uri", "")
        warehouse = resolved.get("warehouse", "")

        if not uri:
            return

        from seeknal.workflow.materialization.operations import DuckDBIcebergExtension

        DuckDBIcebergExtension.load_extension(self.conn)
        DuckDBIcebergExtension.configure_s3(self.conn)

        # Get OAuth2 token if configured
        bearer_token = None
        try:
            bearer_token = DuckDBIcebergExtension.get_oauth2_token()
        except Exception:
            pass  # No OAuth2 configured, try without

        DuckDBIcebergExtension.attach_rest_catalog(
            con=self.conn,
            catalog_name="iceberg",
            uri=uri,
            warehouse_path=warehouse,
            bearer_token=bearer_token,
        )
        self.attached.add("iceberg")
        self._registered_iceberg += 1

    def execute_oneshot(
        self, sql: str, limit: Optional[int] = None
    ) -> tuple[list[str], list[tuple]]:
        """Execute a single SQL query and return structured results.

        Used by --exec mode for non-interactive one-shot execution.

        Args:
            sql: SQL query string, or "-" to read from stdin.
            limit: Optional row limit to apply.

        Returns:
            Tuple of (column_names, rows).

        Raises:
            ValueError: On validation failure, empty input, or multi-statement SQL.
        """
        import sys

        # Read from stdin if sql is "-"
        if sql == "-":
            if sys.stdin.isatty():
                print(
                    "Reading SQL from stdin (end with Ctrl+D)...",
                    file=sys.stderr,
                )
            sql = sys.stdin.read().strip()
            if not sql:
                raise ValueError("No SQL provided on stdin")

        # Reject multi-statement queries
        stripped = sql.strip().rstrip(";")
        if ";" in stripped:
            raise ValueError(
                "Only single SQL statements are supported with --exec. "
                "Found multiple statements separated by ';'."
            )

        # Validate SQL (same read-only allowlist as interactive REPL)
        is_valid, error_msg = validate_sql(sql)
        if not is_valid:
            raise ValueError(f"Query rejected: {error_msg}")

        # Apply --limit by wrapping query
        if limit is not None:
            sql = f"SELECT * FROM ({sql}) AS _q LIMIT {int(limit)}"

        result = self.conn.execute(sql)
        if not result.description:
            return [], []

        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return columns, rows

    def _save_history(self) -> None:
        """Save readline history."""
        try:
            readline.write_history_file(str(self._history_path))
        except Exception:
            pass

    def run(self) -> None:
        """Main REPL loop."""
        print("Seeknal SQL REPL (DuckDB + StarRocks)")
        print("Commands: .connect <url>, .tables, .schema <table>, .quit")
        print("Example: .connect postgres://user:pass@host/db")
        print("         .connect starrocks://user:pass@host:9030/db")
        if self.project_path:
            # Build project header with node count and last run
            header = f"Project: {self.project_path.name}"
            if self.env_name:
                header += f" [env: {self.env_name}]"
            meta_parts = []
            if self._node_count:
                meta_parts.append(f"{self._node_count} nodes")
            if self._last_run:
                # Show date portion only (e.g., "2026-02-21 10:38")
                last_run_display = self._last_run[:16].replace("T", " ")
                meta_parts.append(f"last run: {last_run_display}")
            if meta_parts:
                header += f" ({', '.join(meta_parts)})"
            print(header)

            # Show registration details
            parts = []
            if self._registered_parquets:
                parts.append(f"  Intermediate outputs: {self._registered_parquets} tables registered")
            if self._registered_pg:
                parts.append(f"  PostgreSQL: {self._registered_pg} connection(s) attached (read-only)")
            if self._registered_iceberg:
                parts.append(f"  Iceberg: {self._registered_iceberg} catalog(s) attached")
            if parts:
                for part in parts:
                    print(part)
            elif not self._node_count:
                print("  (no data registered)")
        print()

        try:
            while True:
                try:
                    if self._active_starrocks:
                        prompt = f"seeknal[{self._active_starrocks}]> "
                    elif self.env_name:
                        prompt = f"seeknal[{self.env_name}]> "
                    else:
                        prompt = "seeknal> "
                    line = input(prompt).strip()
                    if not line:
                        continue

                    if line == ".quit" or line == ".exit":
                        break
                    elif line == ".help":
                        self._show_help()
                    elif line == ".sources":
                        self._show_sources()
                    elif line.startswith(".connect "):
                        self._connect(line[9:].strip())
                    elif line == ".tables":
                        self._show_tables()
                    elif line.startswith(".schema "):
                        self._show_schema(line[8:].strip())
                    elif line == ".duckdb":
                        self._active_starrocks = None
                        print("Switched to DuckDB mode.")
                    elif line.startswith("."):
                        print(f"Unknown command: {line.split()[0]}. Type .help")
                    else:
                        self._execute(line)

                except KeyboardInterrupt:
                    print()  # Newline after ^C
                    continue

        except EOFError:
            pass
        finally:
            self._save_history()
            print("Goodbye!")

    def _show_help(self) -> None:
        """Show available commands."""
        print(
            """
Commands:
  .sources          List saved data sources
  .connect <name>   Connect to a saved source by name
  .connect <url>    Connect to a database URL directly
  .tables           List tables in connected databases
  .schema <table>   Show table schema
  .duckdb           Switch back to DuckDB query mode
  .quit             Exit the REPL

Query Security:
  REPL is READ-ONLY for data exploration.
  Allowed: SELECT, WITH, EXPLAIN, DESCRIBE, SHOW, PRAGMA
  Blocked: DROP, DELETE, UPDATE, INSERT, CREATE, ALTER, etc.

Connection examples:
  .connect mydb                                    (saved source)
  .connect postgres://user:pass@localhost/mydb    (direct URL)
  .connect starrocks://user:pass@host:9030/db     (StarRocks)
  .connect $DATABASE_URL                          (env variable)
  .connect /path/to/data.parquet                  (file)

Manage sources:
  seeknal source add <name> --url <connection_url>
  seeknal source list
  seeknal source remove <name>
"""
        )

    def _show_sources(self) -> None:
        """List saved data sources."""
        try:
            from seeknal.request import ReplSourceRequest

            sources = ReplSourceRequest.select_all()

            if not sources:
                print("No saved sources. Add with: seeknal source add <name> --url <url>")
                return

            print(f"{'NAME':<20} {'TYPE':<12} {'URL'}")
            print("-" * 60)
            for s in sources:
                print(f"{s.name:<20} {s.source_type:<12} {s.masked_url}")
        except Exception as e:
            print(f"Error listing sources: {sanitize_error_message(str(e))}")

    def _resolve_source(self, name_or_url: str) -> str:
        """Resolve a source name or URL to a connection URL.

        If name_or_url looks like a saved source name (no :// or file extension),
        look it up and decrypt credentials. Otherwise return as-is.
        """
        # Check if it's a URL or file path
        if "://" in name_or_url or name_or_url.endswith((".parquet", ".csv")):
            return os.path.expandvars(name_or_url)

        # Try to load as saved source
        try:
            from seeknal.request import ReplSourceRequest
            from seeknal.utils.encryption import decrypt_value

            source = ReplSourceRequest.select_by_name(name_or_url)
            if source:
                return self._reconstruct_url(source)
        except Exception:
            pass

        # Not found - maybe it's an env var or typo
        return os.path.expandvars(name_or_url)

    def _reconstruct_url(self, source) -> str:
        """Reconstruct full URL from source record with decrypted password."""
        from seeknal.utils.encryption import decrypt_value

        if source.source_type in ("parquet", "csv"):
            return source.masked_url

        password = ""
        if source.encrypted_credentials:
            password = decrypt_value(source.encrypted_credentials)

        # Rebuild URL
        scheme_map = {"postgres": "postgresql", "mysql": "mysql", "sqlite": "sqlite"}
        scheme = scheme_map.get(source.source_type, source.source_type)

        if source.source_type == "sqlite":
            return f"sqlite:///{source.database}"

        netloc = ""
        if source.username:
            netloc = source.username
            if password:
                netloc += f":{password}"
            netloc += "@"
        if source.host:
            netloc += source.host
            if source.port:
                netloc += f":{source.port}"

        path = f"/{source.database}" if source.database else ""

        return f"{scheme}://{netloc}{path}"

    def _connect(self, name_or_url: str) -> None:
        """Connect to a data source by name or URL."""
        # Resolve saved source name to URL
        url = self._resolve_source(name_or_url)

        # Generate alias - use source name if available, else db0, db1...
        if "://" not in name_or_url and not name_or_url.endswith((".parquet", ".csv")):
            alias = name_or_url  # Use source name as alias
        else:
            alias = f"db{len(self.attached)}"

        try:
            if url.startswith(("postgres://", "postgresql://")):
                self._attach_postgres(alias, url)
            elif url.startswith("mysql://"):
                self._attach_mysql(alias, url)
            elif url.startswith("sqlite://"):
                self._attach_sqlite(alias, url)
            elif url.startswith("starrocks://"):
                self._attach_starrocks(alias, url)
                return  # StarRocks has its own success message
            elif url.endswith(".parquet"):
                self._attach_parquet(alias, url)
            elif url.endswith(".csv"):
                self._attach_csv(alias, url)
            else:
                print(f"Unknown source or URL format: {name_or_url}")
                print("Use a saved source name, URL (postgres://...), or file path")
                return

            self.attached.add(alias)
            print(f"Connected as '{alias}'. Query with: SELECT * FROM {alias}.<table>")

        except Exception as e:
            # Sanitize to avoid credential leakage
            print(f"Connection failed: {sanitize_error_message(str(e))}")

    def _attach_postgres(self, alias: str, url: str) -> None:
        """Attach PostgreSQL database."""
        from urllib.parse import urlparse

        parsed = urlparse(url)

        conn_str = (
            f"host={parsed.hostname or 'localhost'} "
            f"port={parsed.port or 5432} "
            f"dbname={parsed.path.lstrip('/')} "
            f"user={parsed.username or ''} "
            f"password={parsed.password or ''}"
        )
        self.conn.execute(f"ATTACH '{conn_str}' AS {alias} (TYPE postgres)")

    def _attach_mysql(self, alias: str, url: str) -> None:
        """Attach MySQL database."""
        from urllib.parse import urlparse

        parsed = urlparse(url)

        conn_str = (
            f"host={parsed.hostname or 'localhost'} "
            f"port={parsed.port or 3306} "
            f"database={parsed.path.lstrip('/')} "
            f"user={parsed.username or ''} "
            f"password={parsed.password or ''}"
        )
        self.conn.execute(f"ATTACH '{conn_str}' AS {alias} (TYPE mysql)")

    def _attach_sqlite(self, alias: str, url: str) -> None:
        """Attach SQLite database."""
        # sqlite:///path/to/file.db -> /path/to/file.db
        path = url.replace("sqlite://", "")
        if path and not path.startswith(":memory:"):
            # Resolve symlinks FIRST to prevent symlink-based attacks
            resolved_path = Path(path).resolve(strict=True)

            # Check the RESOLVED path for security (not the original path)
            if is_insecure_path(str(resolved_path)):
                raise RuntimeError(f"Insecure path: {resolved_path}")

            # Use resolved path
            path = str(resolved_path)

        self.conn.execute(f"ATTACH '{path}' AS {alias} (TYPE sqlite)")

    def _attach_parquet(self, alias: str, path: str) -> None:
        """Attach parquet file as a view."""
        # Resolve symlinks FIRST to prevent symlink-based attacks
        resolved_path = Path(path).resolve(strict=True)

        # Check the RESOLVED path for security (not the original path)
        if is_insecure_path(str(resolved_path)):
            raise RuntimeError(f"Insecure path: {resolved_path}")

        # Verify it's actually a parquet file
        if resolved_path.suffix.lower() != '.parquet':
            raise RuntimeError(f"Invalid file type: {resolved_path.suffix}. Expected .parquet")

        # Escape single quotes in path
        safe_path = str(resolved_path).replace("'", "''")
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}")
        self.conn.execute(
            f"CREATE VIEW {alias}.data AS SELECT * FROM read_parquet('{safe_path}')"
        )

    def _attach_csv(self, alias: str, path: str) -> None:
        """Attach CSV file as a view."""
        # Resolve symlinks FIRST to prevent symlink-based attacks
        resolved_path = Path(path).resolve(strict=True)

        # Check the RESOLVED path for security (not the original path)
        if is_insecure_path(str(resolved_path)):
            raise RuntimeError(f"Insecure path: {resolved_path}")

        # Verify it's actually a CSV file
        if resolved_path.suffix.lower() != '.csv':
            raise RuntimeError(f"Invalid file type: {resolved_path.suffix}. Expected .csv")

        # Escape single quotes in path
        safe_path = str(resolved_path).replace("'", "''")
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}")
        self.conn.execute(
            f"CREATE VIEW {alias}.data AS SELECT * FROM read_csv_auto('{safe_path}')"
        )

    def _attach_starrocks(self, alias: str, url: str) -> None:
        """Connect to StarRocks via pymysql."""
        try:
            from seeknal.connections.starrocks import create_starrocks_connection_from_url
        except ImportError:
            print("pymysql is required for StarRocks. Install with: pip install pymysql")
            return

        try:
            conn = create_starrocks_connection_from_url(url)
            self._starrocks_connections[alias] = conn
            self._active_starrocks = alias
            self.attached.add(alias)

            # Get version for confirmation
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            print(f"Connected to StarRocks ({version}) as '{alias}'.")
            print("Queries will route to StarRocks. Use .duckdb to switch back.")
        except Exception as e:
            print(f"StarRocks connection failed: {sanitize_error_message(str(e))}")

    def _show_tables(self) -> None:
        """List tables in all attached databases and auto-registered views."""
        has_content = False

        # Show auto-registered parquet views (in default catalog)
        if self._registered_parquets > 0:
            try:
                result = self.conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_catalog = 'memory' AND table_schema = 'main' "
                    "AND table_type = 'VIEW' ORDER BY table_name"
                ).fetchall()
                if result:
                    has_content = True
                    for (view_name,) in result:
                        print(f"  {view_name}")
            except Exception:
                pass

        if not self.attached and not has_content:
            print("No databases connected. Use .connect <url> first.")
            return

        for alias in sorted(self.attached):
            # StarRocks connections
            if alias in self._starrocks_connections:
                try:
                    sr_conn = self._starrocks_connections[alias]
                    cursor = sr_conn.cursor()
                    cursor.execute("SHOW TABLES")
                    for row in cursor.fetchall():
                        print(f"{alias}.{row[0]}")
                    cursor.close()
                except Exception as e:
                    print(f"Error listing StarRocks tables for {alias}: {sanitize_error_message(str(e))}")
                continue

            # DuckDB-attached databases
            try:
                result = self.conn.execute(
                    f"SELECT table_schema, table_name FROM information_schema.tables "
                    f"WHERE table_catalog = '{alias}'"
                ).fetchall()
                for schema, table in result:
                    print(f"{alias}.{schema}.{table}")
            except Exception:
                # For file-based sources
                try:
                    result = self.conn.execute(f"SHOW TABLES FROM {alias}").fetchall()
                    for row in result:
                        print(f"{alias}.{row[0]}")
                except Exception:
                    pass

    def _show_schema(self, table: str) -> None:
        """Show schema for a table."""
        if not table:
            print("Usage: .schema <table_name>")
            return

        # Check if table belongs to a StarRocks alias
        parts = table.split(".", 1)
        if len(parts) == 2 and parts[0] in self._starrocks_connections:
            alias, tbl = parts
            try:
                sr_conn = self._starrocks_connections[alias]
                cursor = sr_conn.cursor()
                cursor.execute(f"DESCRIBE {tbl}")
                result = cursor.fetchall()
                cursor.close()
                print(
                    tabulate(
                        result, headers=["Field", "Type", "Null", "Key", "Default", "Extra"]
                    )
                )
            except Exception as e:
                print(f"Error: {sanitize_error_message(str(e))}")
            return

        # If active StarRocks and no alias prefix, try StarRocks first
        if self._active_starrocks and self._active_starrocks in self._starrocks_connections:
            try:
                sr_conn = self._starrocks_connections[self._active_starrocks]
                cursor = sr_conn.cursor()
                cursor.execute(f"DESCRIBE {table}")
                result = cursor.fetchall()
                cursor.close()
                print(
                    tabulate(
                        result, headers=["Field", "Type", "Null", "Key", "Default", "Extra"]
                    )
                )
                return
            except Exception:
                pass  # Fall through to DuckDB

        try:
            result = self.conn.execute(f"DESCRIBE {table}").fetchall()
            print(
                tabulate(
                    result, headers=["Column", "Type", "Null", "Key", "Default", "Extra"]
                )
            )
        except Exception as e:
            print(f"Error: {sanitize_error_message(str(e))}")

    def _execute(self, sql: str) -> None:
        """Execute SQL query and display results."""
        # Security validation: prevent SQL injection and dangerous operations
        is_valid, error_msg = validate_sql(sql)
        if not is_valid:
            print(f"Query rejected: {error_msg}")
            return

        # Route to StarRocks if active
        if self._active_starrocks and self._active_starrocks in self._starrocks_connections:
            self._execute_starrocks(sql)
            return

        try:
            result = self.conn.execute(sql)

            if not result.description:
                print("Query executed successfully.")
                return

            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            # Limit display
            MAX_ROWS = 100
            display_rows = rows[:MAX_ROWS]

            print(tabulate(display_rows, headers=columns, tablefmt="simple"))

            if len(rows) > MAX_ROWS:
                print(
                    f"\n({MAX_ROWS} of {len(rows)} rows displayed. Use LIMIT for more control.)"
                )
            else:
                print(f"\n({len(rows)} rows)")

        except Exception as e:
            print(f"Error: {sanitize_error_message(str(e))}")

    def _execute_starrocks(self, sql: str) -> None:
        """Execute SQL on the active StarRocks connection."""
        sr_conn = self._starrocks_connections[self._active_starrocks]
        try:
            cursor = sr_conn.cursor()
            cursor.execute(sql)

            if not cursor.description:
                print("Query executed successfully.")
                cursor.close()
                return

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            MAX_ROWS = 100
            display_rows = rows[:MAX_ROWS]

            print(tabulate(display_rows, headers=columns, tablefmt="simple"))

            if len(rows) > MAX_ROWS:
                print(
                    f"\n({MAX_ROWS} of {len(rows)} rows displayed. Use LIMIT for more control.)"
                )
            else:
                print(f"\n({len(rows)} rows)")

        except Exception as e:
            print(f"StarRocks error: {sanitize_error_message(str(e))}")


def run_repl(
    project_path: Optional[Path] = None,
    profile_path: Optional[Path] = None,
    env_name: Optional[str] = None,
) -> None:
    """Entry point for REPL."""
    repl = REPL(project_path=project_path, profile_path=profile_path, env_name=env_name)
    repl.run()

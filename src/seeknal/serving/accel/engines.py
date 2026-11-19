"""Database engine implementations for Seeknal Serving DuckDB acceleration."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional, Protocol, Union

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore[assignment]

from seeknal.serving.accel.settings import (
    DEFAULT_EXPECTED_DUCKDB_VERSION,
    get_expected_duckdb_version,
    get_extension_dir,
    get_memory_limit,
    get_threads,
    validate_memory_limit,
    validate_threads,
)

DUCKDB_AVAILABLE = duckdb is not None
PINNED_DUCKDB_VERSION = DEFAULT_EXPECTED_DUCKDB_VERSION


def get_installed_duckdb_version() -> str:
    """Return the installed duckdb.__version__ or 'unknown'."""
    if duckdb is not None:
        return str(getattr(duckdb, "__version__", "unknown"))
    return "unknown"


def check_duckdb_version(
    allow_mismatch: bool = False,
    expected_version: Optional[str] = None,
) -> None:
    """Refuse to run if duckdb.__version__ differs from expected version, unless overridden."""
    if duckdb is None:
        raise RuntimeError("duckdb is not installed")
    exp = expected_version or get_expected_duckdb_version()
    current_version = getattr(duckdb, "__version__", None)
    if current_version != exp:
        msg = (
            f"duckdb version {current_version!r} does not match expected version {exp!r}; "
            f"Seeknal Serving requires duckdb {exp}. Use --allow-version-mismatch to override."
        )
        if not allow_mismatch:
            raise RuntimeError(msg)
        from seeknal.serving.accel.loader import log_event

        log_event(
            "version_mismatch_warning",
            level="WARNING",
            current_version=str(current_version),
            expected_version=exp,
        )


class SourceProtocol(Protocol):
    def get_row_count(self, table_or_view: str) -> int:
        ...


class TargetProtocol(Protocol):
    def execute(self, stmt: str) -> Any:
        ...

    def get_row_count(self, table_name: str) -> int:
        ...

    def close(self) -> None:
        ...


class DuckDBTarget:
    """Target DuckDB database connection."""

    def __init__(
        self,
        path: Union[str, Path],
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        attach_pg: bool = True,
        attach_sql: Optional[str] = None,
        allow_version_mismatch: bool = False,
        expected_version: Optional[str] = None,
        extension_dir: Optional[str] = None,
        setup_fn: Optional[Callable[[Any], None]] = None,
        stmt_rewriter: Optional[Callable[[str], str]] = None,
    ) -> None:
        if duckdb is None:
            raise RuntimeError("duckdb is not installed")
        check_duckdb_version(
            allow_mismatch=allow_version_mismatch,
            expected_version=expected_version,
        )
        self.path = str(path)
        self.executed_statements: list[str] = []
        self._stmt_rewriter = stmt_rewriter
        self.conn = duckdb.connect(self.path)

        try:
            mem = validate_memory_limit(memory_limit or get_memory_limit())
            thr = validate_threads(threads if threads is not None else get_threads())
            self.conn.execute(f"SET memory_limit = '{mem}'")
            self.conn.execute(f"SET threads = {thr}")

            if setup_fn is not None:
                setup_fn(self.conn)
            else:
                ext_dir = extension_dir or get_extension_dir()
                if ext_dir:
                    self.conn.execute(f"SET extension_directory = '{ext_dir}'")
                    self.conn.execute("SET autoinstall_known_extensions = false")
                    self.conn.execute("SET autoload_known_extensions = false")
                    try:
                        self.conn.execute("LOAD postgres")
                    except Exception as exc:
                        curr_v = getattr(duckdb, "__version__", "unknown")
                        raise RuntimeError(
                            f"Postgres extension is not pre-staged in {ext_dir} for DuckDB version {curr_v}: {exc}"
                        ) from exc

                if attach_sql:
                    self.conn.execute(attach_sql)
                elif attach_pg:
                    # ATTACH Postgres using standard libpq env vars; no secrets in SQL
                    self.conn.execute("ATTACH '' AS pg (TYPE postgres, READ_ONLY)")
        except Exception:
            try:
                self.conn.close()
            except Exception:
                pass
            raise

    def execute(self, stmt: str) -> Any:
        self.executed_statements.append(stmt)
        effective_stmt = (
            self._stmt_rewriter(stmt) if self._stmt_rewriter is not None else stmt
        )
        return self.conn.execute(effective_stmt)

    def get_row_count(self, table_name: str) -> int:
        res = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(res[0]) if res else 0

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class LakebaseSource:
    """Lakebase Postgres source engine accessed via DuckDB postgres scanner."""

    source_type = "postgres"

    def __init__(
        self,
        conn: Any = None,
        allow_version_mismatch: bool = False,
        expected_version: Optional[str] = None,
        extension_dir: Optional[str] = None,
    ) -> None:
        self._conn = conn
        self.allow_version_mismatch = allow_version_mismatch
        self.expected_version = expected_version
        self.extension_dir = extension_dir

    def get_row_count(
        self,
        table_or_view: str,
        snapshot_id: Optional[int] = None,
    ) -> int:
        _ = snapshot_id
        view = table_or_view.replace("rpt_base.", "")
        if self._conn is None:
            if duckdb is None:
                raise RuntimeError("duckdb is not installed")
            check_duckdb_version(
                allow_mismatch=self.allow_version_mismatch,
                expected_version=self.expected_version,
            )
            self._conn = duckdb.connect(":memory:")

            ext_dir = self.extension_dir or get_extension_dir()
            if ext_dir:
                self._conn.execute(f"SET extension_directory = '{ext_dir}'")
                self._conn.execute("SET autoinstall_known_extensions = false")
                self._conn.execute("SET autoload_known_extensions = false")
                try:
                    self._conn.execute("LOAD postgres")
                except Exception as exc:
                    curr_v = getattr(duckdb, "__version__", "unknown")
                    raise RuntimeError(
                        f"Postgres extension is not pre-staged in {ext_dir} for DuckDB version {curr_v}: {exc}"
                    ) from exc

            self._conn.execute("ATTACH '' AS pg (TYPE postgres, READ_ONLY)")
        res = self._conn.execute(f"SELECT COUNT(*) FROM pg.rpt_base.{view}").fetchone()
        return int(res[0]) if res else 0

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass


def create_target_factory(
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
    attach_pg: bool = True,
    attach_sql: Optional[str] = None,
    allow_version_mismatch: bool = False,
    expected_version: Optional[str] = None,
    extension_dir: Optional[str] = None,
    setup_fn: Optional[Callable[[Any], None]] = None,
    stmt_rewriter: Optional[Callable[[str], str]] = None,
) -> Callable[[Union[str, Path]], DuckDBTarget]:
    """Return a factory callable that instantiates DuckDBTarget."""

    def factory(target_path: Union[str, Path]) -> DuckDBTarget:
        return DuckDBTarget(
            path=target_path,
            memory_limit=memory_limit,
            threads=threads,
            attach_pg=attach_pg,
            attach_sql=attach_sql,
            allow_version_mismatch=allow_version_mismatch,
            expected_version=expected_version,
            extension_dir=extension_dir,
            setup_fn=setup_fn,
            stmt_rewriter=stmt_rewriter,
        )

    return factory

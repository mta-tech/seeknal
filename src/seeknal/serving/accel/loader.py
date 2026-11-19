"""Load engine that caches Lakebase Postgres or Lakekeeper Iceberg tables into DuckDB."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timezone
import fcntl
import inspect
import json
import os
from pathlib import Path
import re
import sys
import threading
from typing import Any, Optional, Union
import uuid

from seeknal.serving.accel.settings import (
    DEFAULT_PII_MODE,
    LIBPQ_ENV_VARS,
    LOADER_VERSION,
    validate_pii_mode,
)
from seeknal.serving.accel.tables import (
    TableSpec,
    validate_duckdb_type,
    validate_identifier,
    validate_snapshot_id,
)

SCRUB_PATTERNS = [
    re.compile(r"(host|user|port|dbname|password)\s*=\s*[^\s,;]+", re.IGNORECASE),
    re.compile(r"://([^:@\s]+):([^@\s]+)@", re.IGNORECASE),
]

IP_V4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
IP_V6_RE = re.compile(
    r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b|"
    r"\b(?:[0-9a-fA-F]{1,4}:){1,7}:|"
    r"::(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}\b"
)

LOADER_FILE_PATTERN = re.compile(r"^mart-\d{8}T\d{6}Z(?:-[0-9a-fA-F]{8})?\.duckdb$")

_SECRET_LOCK = threading.Lock()
_REGISTERED_SECRET_ENV_VARS: set[str] = set()
_REGISTERED_SECRET_VALUES: set[str] = set()


class AccelerationError(Exception):
    """Base error for acceleration operations."""


class BadInputError(AccelerationError):
    """Raised on invalid inputs or configuration (exit 2)."""


class LoadError(AccelerationError):
    """Raised on load or execution failure (exit 3)."""


class RowCountMismatchError(LoadError):
    """Raised when source and target table row counts do not match."""


def register_secret_env_vars(*var_names: str) -> None:
    """Register environment variable names whose values must be scrubbed from logs/errors."""
    with _SECRET_LOCK:
        for name in var_names:
            if isinstance(name, str) and name.strip():
                _REGISTERED_SECRET_ENV_VARS.add(name.strip())


def register_secret_values(*values: str) -> None:
    """Register resolved secret values so they are always scrubbed from logs/errors."""
    with _SECRET_LOCK:
        for val in values:
            if isinstance(val, str) and len(val) > 1:
                _REGISTERED_SECRET_VALUES.add(val)


def clear_registered_secrets() -> None:
    """Clear dynamically registered secret env var names and values."""
    with _SECRET_LOCK:
        _REGISTERED_SECRET_ENV_VARS.clear()
        _REGISTERED_SECRET_VALUES.clear()


def scrub_text(text: str, *, extra_secrets: Optional[Iterable[str]] = None) -> str:
    """Scrub database/catalog secrets, libpq settings, and IP addresses from text."""
    if not isinstance(text, str):
        text = str(text)

    with _SECRET_LOCK:
        env_vars = tuple(LIBPQ_ENV_VARS) + tuple(sorted(_REGISTERED_SECRET_ENV_VARS))
        secret_vals = set(_REGISTERED_SECRET_VALUES)

    for var in env_vars:
        val = os.environ.get(var)
        if val and len(val) > 1:
            secret_vals.add(val)

    if extra_secrets is not None:
        for val in extra_secrets:
            if isinstance(val, str) and len(val) > 1:
                secret_vals.add(val)

    # Scrub longest values first to avoid partial substring residue
    for val in sorted(secret_vals, key=len, reverse=True):
        if val in text:
            text = text.replace(val, "[REDACTED]")

    for pat in SCRUB_PATTERNS:
        text = pat.sub(r"\1=[REDACTED]", text)
    text = IP_V4_RE.sub("[REDACTED_IP]", text)
    text = IP_V6_RE.sub("[REDACTED_IP]", text)
    return text


def log_event(step: str, level: str = "INFO", **kwargs: Any) -> None:
    """Emit a single-line JSON log entry to stderr with UTC timestamp, scrubbing secrets."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    safe_data: dict[str, Any] = {"step": step, "level": level, "timestamp": now_utc}

    for k, v in kwargs.items():
        if any(
            secret_term in k.lower()
            for secret_term in ("pass", "secret", "token", "cred", "key_id")
        ):
            continue
        if k in ("error", "message", "detail", "error_message", "err", "exception"):
            safe_data[k] = scrub_text(str(v))
        else:
            safe_data[k] = str(v)

    line = json.dumps(safe_data)
    sys.stderr.write(line + "\n")
    sys.stderr.flush()


def _split_target_table_name(spec: TableSpec) -> tuple[str, str]:
    """Validate and split target `spec.name` into `(schema_name, table_name)`."""
    if "." in spec.name:
        parts = spec.name.split(".")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid target table name {spec.name!r}; expected '<schema>.<table>'"
            )
        schema_name, table_name = parts
        validate_identifier(schema_name)
        validate_identifier(table_name)
        return schema_name, table_name
    validate_identifier(spec.view)
    return "rpt", spec.view


def build_select_columns_clause(spec: TableSpec, pii_mode: str = DEFAULT_PII_MODE) -> str:
    """Build validated SELECT column projection list according to `pii_mode`."""
    mode = validate_pii_mode(pii_mode)
    col_parts: list[str] = []
    for col in spec.columns:
        validate_identifier(col.name)
        validate_duckdb_type(col.duckdb_type, col.name)
        if mode == "exclude" and (col.is_nulled or col.is_pii):
            continue
        if col.is_nulled:
            col_parts.append(f"CAST(NULL AS {col.duckdb_type}) AS {col.name}")
        else:
            col_parts.append(col.name)

    if not col_parts:
        raise ValueError(
            f"Table {spec.name!r} has no remaining columns to select under pii_mode={mode!r}"
        )
    return ", ".join(col_parts)


def plan_sql(
    specs: Sequence[TableSpec],
    pii_mode: str = DEFAULT_PII_MODE,
    source_type: Optional[str] = None,
) -> list[str]:
    """Return deterministic DuckDB statements for caching tables, validating types and PII rules."""
    mode = validate_pii_mode(pii_mode)
    if source_type == "iceberg":
        from seeknal.serving.accel.iceberg_source import plan_iceberg_load_sql

        return plan_iceberg_load_sql(specs, pii_mode=mode)

    statements: list[str] = []
    seen_schemas: set[str] = set()

    if not specs:
        return ["CREATE SCHEMA rpt"]

    for spec in specs:
        validate_identifier(spec.view)
        schema_name, table_name = _split_target_table_name(spec)
        if schema_name not in seen_schemas:
            statements.append(f"CREATE SCHEMA {schema_name}")
            seen_schemas.add(schema_name)

        cols_clause = build_select_columns_clause(spec, pii_mode=mode)

        if spec.source == f"rpt_base.{spec.view}" or spec.source.startswith("rpt_base."):
            source_view = spec.source.split(".", 1)[1]
            validate_identifier(source_view)
            from_clause = f"pg.rpt_base.{source_view}"
        elif spec.source.startswith("pg."):
            pg_parts = spec.source.split(".")
            if len(pg_parts) != 3:
                raise ValueError(f"Invalid Postgres source reference {spec.source!r}")
            for p in pg_parts:
                validate_identifier(p)
            from_clause = spec.source
        else:
            from seeknal.serving.accel.iceberg_source import format_iceberg_from_clause

            from_clause = format_iceberg_from_clause(
                spec.source,
                snapshot_id=spec.snapshot_id,
            )

        stmt = f"CREATE TABLE {schema_name}.{table_name} AS SELECT {cols_clause} FROM {from_clause}"
        statements.append(stmt)

    return statements


def _prune_older_files(cache_dir: Path, keep: int) -> int:
    """Delete older mart-*.duckdb files beyond keep matching loader pattern, never the linked one."""
    symlink_path = cache_dir / "mart.duckdb"
    linked_target: Optional[Path] = None
    if symlink_path.is_symlink():
        try:
            rel_or_abs = os.readlink(symlink_path)
            linked_target = (cache_dir / rel_or_abs).resolve()
        except OSError:
            pass

    candidates = [
        f
        for f in cache_dir.glob("mart-*.duckdb")
        if f.is_file() and not f.is_symlink() and LOADER_FILE_PATTERN.fullmatch(f.name)
    ]

    for f in list(candidates):
        if linked_target is not None and f.resolve() == linked_target:
            continue
        wal_file = f.with_name(f"{f.name}.wal")
        if wal_file.exists():
            try:
                wal_file.unlink()
            except OSError:
                pass
            try:
                f.unlink()
            except OSError:
                pass
            candidates.remove(f)

    for wal in cache_dir.glob("mart-*.duckdb.wal"):
        base_db_name = wal.name[:-4]
        if LOADER_FILE_PATTERN.fullmatch(base_db_name):
            base_db = cache_dir / base_db_name
            if not base_db.exists():
                try:
                    wal.unlink()
                except OSError:
                    pass

    candidates.sort(key=lambda p: p.name, reverse=True)

    pruned = 0
    cutoff = max(0, keep)
    to_delete = candidates[cutoff:]
    for f in to_delete:
        try:
            if linked_target is not None and f.resolve() == linked_target:
                continue
            wal_file = f.with_name(f"{f.name}.wal")
            if wal_file.exists():
                try:
                    wal_file.unlink()
                except OSError:
                    pass
            f.unlink()
            pruned += 1
        except OSError:
            pass

    return pruned


def _invoke_get_row_count(
    source: Any,
    table_ref: str,
    snapshot_id: Optional[int],
) -> int:
    """Invoke `source.get_row_count` passing `snapshot_id` when supported by its signature."""
    fn = source.get_row_count
    if snapshot_id is not None:
        validate_snapshot_id(snapshot_id, context=f"Row count check for '{table_ref}'")
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        accepts_snapshot = any(
            p.name == "snapshot_id"
            or p.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
            for p in params
        )
    except (TypeError, ValueError):
        accepts_snapshot = False

    if accepts_snapshot:
        return int(fn(table_ref, snapshot_id=snapshot_id))
    return int(fn(table_ref))


def _query_source_row_count(source: Any, spec: TableSpec) -> int:
    """Query source row count for `spec`, falling back to `spec.view` only if supported."""
    try:
        return _invoke_get_row_count(source, spec.source, spec.snapshot_id)
    except Exception as primary_exc:
        if getattr(source, "source_type", None) == "iceberg":
            raise
        try:
            return _invoke_get_row_count(source, spec.view, spec.snapshot_id)
        except Exception:
            raise primary_exc


def run_load(
    specs: Sequence[TableSpec],
    source: Any,
    target_factory: Callable[[Path], Any],
    cache_dir: Union[str, Path],
    clock: Any = None,
    keep: int = 3,
    as_of_date: Optional[str] = None,
    duckdb_version: Optional[str] = None,
    pii_mode: str = DEFAULT_PII_MODE,
    source_type: Optional[str] = None,
) -> Path:
    """Run the load-verify-swap sequence to produce a new cached DuckDB file."""
    mode = validate_pii_mode(pii_mode)
    effective_source_type = source_type or getattr(source, "source_type", None)

    cache_path = Path(cache_dir).resolve()
    cache_dir_existed = cache_path.exists()
    cache_path.mkdir(parents=True, exist_ok=True)
    if not cache_dir_existed:
        try:
            os.chmod(cache_path, 0o755)
        except OSError:
            pass

    lock_file_path = cache_path / ".loader.lock"
    lock_file = open(lock_file_path, "a", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError) as lock_exc:
        lock_file.close()
        log_event(
            "lock_failed",
            level="ERROR",
            error_type=type(lock_exc).__name__,
            message="Another loader process is currently running; lock held on .loader.lock",
        )
        raise LoadError(
            "Another loader process is currently running; lock held on .loader.lock"
        ) from lock_exc

    created_file: Optional[Path] = None
    target = None

    try:
        symlink_path = cache_path / "mart.duckdb"
        if symlink_path.exists() and not symlink_path.is_symlink():
            raise LoadError(
                f"Pre-existing regular file or directory at {symlink_path}; refusing load without modifying anything"
            )

        if callable(clock):
            now = clock()
        elif clock is not None:
            now = clock
        else:
            now = datetime.now(timezone.utc)

        if isinstance(now, (int, float)):
            now = datetime.fromtimestamp(now, tz=timezone.utc)
        elif isinstance(now, datetime):
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            else:
                now = now.astimezone(timezone.utc)
        else:
            now = datetime.now(timezone.utc)

        timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")
        suffix = uuid.uuid4().hex[:8]
        file_name = f"mart-{timestamp_str}-{suffix}.duckdb"
        new_file_path = cache_path / file_name

        if new_file_path.exists():
            raise LoadError(f"Target file already exists: {new_file_path}")

        date_str = as_of_date if as_of_date else now.strftime("%Y-%m-%d")

        effective_duckdb_version = duckdb_version
        if not effective_duckdb_version:
            try:
                from seeknal.serving.accel.engines import get_installed_duckdb_version

                effective_duckdb_version = get_installed_duckdb_version()
            except ImportError:
                effective_duckdb_version = "unknown"

        log_event("load_start", file=file_name, target=str(new_file_path))

        plan = plan_sql(
            specs,
            pii_mode=mode,
            source_type=effective_source_type,
        )
        log_event("plan_generated", statement_count=len(plan))

        created_file = new_file_path
        target = target_factory(new_file_path)

        if new_file_path.exists():
            try:
                os.chmod(new_file_path, 0o644)
            except OSError:
                pass
        log_event("target_initialized", path=str(new_file_path))

        for stmt in plan:
            target.execute(stmt)
        log_event("plan_executed")

        target.execute("CHECKPOINT")
        log_event("checkpoint_executed")

        include_snapshots = (
            effective_source_type == "iceberg"
            or hasattr(source, "get_snapshot_id")
            or any(s.snapshot_id is not None for s in specs)
        )
        row_counts: dict[str, int] = {}
        snapshots: dict[str, Optional[int]] = {}

        for spec in specs:
            target_count = int(target.get_row_count(spec.name))
            source_count = _query_source_row_count(source, spec)

            if target_count != source_count:
                raise RowCountMismatchError(
                    f"Row count mismatch for {spec.name}: source={source_count}, target={target_count}"
                )
            row_counts[spec.name] = target_count

            if include_snapshots:
                if spec.snapshot_id is not None:
                    snapshots[spec.name] = spec.snapshot_id
                elif hasattr(source, "get_snapshot_id"):
                    snapshots[spec.name] = source.get_snapshot_id(spec)
                else:
                    snapshots[spec.name] = None

        log_event("row_counts_verified", tables=len(specs))

        if hasattr(target, "close"):
            try:
                target.close()
            except Exception:
                pass
            target = None

        tmp_symlink = cache_path / f".mart.duckdb.tmp.{uuid.uuid4().hex}"
        try:
            os.symlink(file_name, tmp_symlink)
            os.replace(tmp_symlink, symlink_path)
        finally:
            if tmp_symlink.is_symlink() or tmp_symlink.exists():
                try:
                    tmp_symlink.unlink(missing_ok=True)
                except OSError:
                    pass

        created_file = None
        log_event("symlink_updated", symlink="mart.duckdb", points_to=file_name)

        freshness_payload: dict[str, Any] = {
            "file_name": file_name,
            "tables": row_counts,
            "as_of_date": date_str,
            "loaded_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "loader_version": LOADER_VERSION,
            "duckdb_version": effective_duckdb_version,
        }
        if include_snapshots:
            freshness_payload["snapshots"] = snapshots

        tmp_freshness = cache_path / f".freshness.json.tmp.{uuid.uuid4().hex}"
        try:
            with open(tmp_freshness, "w", encoding="utf-8") as f:
                json.dump(freshness_payload, f, indent=2)
            os.chmod(tmp_freshness, 0o644)
            os.replace(tmp_freshness, cache_path / "freshness.json")
            os.chmod(cache_path / "freshness.json", 0o644)
        except Exception as fresh_exc:
            log_event(
                "freshness_write_failed_after_swap",
                level="ERROR",
                message=f"Symlink mart.duckdb updated to {file_name}, but freshness.json failed: {fresh_exc}",
            )
            raise LoadError(
                f"Symlink mart.duckdb updated to {file_name}, but freshness.json failed: {scrub_text(str(fresh_exc))}"
            ) from fresh_exc
        finally:
            if tmp_freshness.exists():
                try:
                    tmp_freshness.unlink(missing_ok=True)
                except OSError:
                    pass
        log_event("freshness_written", path="freshness.json")

        if new_file_path.exists():
            try:
                os.chmod(new_file_path, 0o644)
            except OSError:
                pass

        pruned_count = _prune_older_files(cache_path, keep=keep)
        log_event("old_files_pruned", pruned=pruned_count, keep=keep)

        log_event("load_success", file=file_name)
        return new_file_path

    except Exception as exc:
        if target is not None and hasattr(target, "close"):
            try:
                target.close()
            except Exception:
                pass

        if created_file is not None:
            if created_file.exists():
                try:
                    created_file.unlink()
                except OSError:
                    pass
            wal_file = created_file.with_name(f"{created_file.name}.wal")
            if wal_file.exists():
                try:
                    wal_file.unlink()
                except OSError:
                    pass

        log_event(
            "load_failed",
            level="ERROR",
            error_type=type(exc).__name__,
            message=scrub_text(str(exc)),
        )
        if isinstance(exc, AccelerationError):
            raise
        raise LoadError(f"Load failed: {scrub_text(str(exc))}") from exc

    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_file.close()

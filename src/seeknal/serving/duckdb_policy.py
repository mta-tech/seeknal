"""Locked-down DuckDB connection policy and probe conformance tool."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import uuid
from collections.abc import Sequence
from typing import Any

import duckdb


def _validate_allowed_directory(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ValueError(f"Allowed directory must be a non-empty string, got {path!r}")
    if not os.path.isabs(path):
        raise ValueError(f"Allowed directory must be an absolute path, got {path!r}")
    if not path.endswith("/"):
        raise ValueError(f"Allowed directory must end with '/', got {path!r}")
    if path != "/" and os.path.normpath(path) + "/" != path:
        raise ValueError(f"Allowed directory must be a normalized path, got {path!r}")
    return path


def harden_connection(
    conn: Any,
    *,
    allowed_directories: Sequence[str] = (),
    memory_limit: str | None = None,
    threads: int | None = None,
    temp_directory: str | None = None,
    max_temp_directory_size: str | None = None,
) -> None:
    """Harden a DuckDB connection in strict order."""
    validated_dirs: list[str] = []
    # 1. sets allowed_directories (when non-empty; absolute, normalized paths, each must end with /, otherwise ValueError)
    if allowed_directories:
        validated_dirs = [_validate_allowed_directory(d) for d in allowed_directories]
        conn.execute("SET allowed_directories = ?", [validated_dirs])

    # 1b. temp_directory and max_temp_directory_size
    if temp_directory is not None:
        if not isinstance(temp_directory, str) or not temp_directory.strip():
            raise ValueError(f"temp_directory must be a non-empty string, got {temp_directory!r}")
        if not os.path.isabs(temp_directory):
            raise ValueError(f"temp_directory must be an absolute path, got {temp_directory!r}")
        if not os.path.exists(temp_directory):
            raise ValueError(f"temp_directory '{temp_directory}' does not exist")
        if not os.path.isdir(temp_directory):
            raise ValueError(f"temp_directory '{temp_directory}' is not a directory")
        if not os.access(temp_directory, os.W_OK):
            raise ValueError(f"temp_directory '{temp_directory}' is not writable")

        temp_real = os.path.realpath(temp_directory)
        for d in validated_dirs:
            d_real = os.path.realpath(d)
            if temp_real == d_real or temp_real.startswith(d_real + "/"):
                raise ValueError(
                    f"temp_directory '{temp_directory}' must not be inside allowed directory '{d}'"
                )
        conn.execute("SET temp_directory = ?", [temp_directory])
        if max_temp_directory_size is not None:
            conn.execute("SET max_temp_directory_size = ?", [str(max_temp_directory_size)])
    else:
        conn.execute("SET max_temp_directory_size = '0B'")
        conn.execute("SET temp_directory = ''")

    # 2. sets enable_external_access = false
    conn.execute("SET enable_external_access = false")

    # 3. sets autoinstall_known_extensions = false and autoload_known_extensions = false
    conn.execute("SET autoinstall_known_extensions = false")
    conn.execute("SET autoload_known_extensions = false")

    # 4. sets the optional memory_limit and threads
    if memory_limit is not None:
        conn.execute("SET memory_limit = ?", [str(memory_limit)])
    if threads is not None:
        conn.execute("SET threads = ?", [int(threads)])

    # 5. last, sets lock_configuration = true
    conn.execute("SET lock_configuration = true")


def connect_hardened(
    path: str,
    *,
    read_only: bool = True,
    allowed_directories: Sequence[str] = (),
    memory_limit: str | None = None,
    threads: int | None = None,
    temp_directory: str | None = None,
    max_temp_directory_size: str | None = None,
) -> Any:
    """Open and harden a DuckDB connection, never falling back to unhardened."""
    if temp_directory is not None and path != ":memory:" and not path.startswith(":memory:"):
        temp_real = os.path.realpath(temp_directory)
        db_real = os.path.realpath(path)
        db_dir_real = os.path.dirname(db_real)
        if temp_real == db_dir_real or temp_real.startswith(db_dir_real + "/"):
            raise ValueError(
                f"temp_directory '{temp_directory}' must not be inside data directory '{db_dir_real}'"
            )

    conn = duckdb.connect(path, read_only=read_only)
    try:
        harden_connection(
            conn,
            allowed_directories=allowed_directories,
            memory_limit=memory_limit,
            threads=threads,
            temp_directory=temp_directory,
            max_temp_directory_size=max_temp_directory_size,
        )
    except duckdb.Error as exc:
        if "the configuration has been locked" in str(exc) or "locked" in str(exc):
            p = probe(conn)
            if all(p.values()):
                return conn
        try:
            conn.close()
        except Exception:
            pass
        raise
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    return conn


class DuckDBInstance:
    """A hardened DuckDB database instance for a resolved database file."""

    def __init__(
        self,
        holder: DuckDBHolder,
        key: tuple[int, int],
        configured_path: str,
        resolved_path: str,
        stat_identity: tuple[int, int, int] | None,
        connection: Any,
        params: tuple,
    ) -> None:
        self.holder = holder
        self.key = key
        self.configured_path = configured_path
        self.resolved_path = resolved_path
        self.stat_identity = stat_identity
        self.connection = connection
        self.params = params
        self.in_flight = 0
        self.is_retired = False
        self.is_closed = False
        self.last_used_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire_cursor(self) -> Any:
        with self.lock:
            if self.is_closed:
                raise RuntimeError("DuckDB instance is closed")
            self.in_flight += 1
            self.last_used_at = time.monotonic()
            raw_cursor = self.connection.cursor()
            return DuckDBCursorWrapper(raw_cursor, self)

    def release_cursor(self, cursor: Any) -> None:
        close_exc: Exception | None = None
        try:
            cursor.close()
        except Exception as exc:
            close_exc = exc
        with self.lock:
            self.in_flight -= 1
            self.last_used_at = time.monotonic()
        self.holder._on_cursor_released(self)
        if close_exc is not None:
            raise close_exc

    def close_instance(self) -> None:
        with self.lock:
            if not self.is_closed:
                self.is_closed = True
                try:
                    self.connection.close()
                except Exception:
                    pass


class DuckDBCursorWrapper:
    """Wrapper around a DuckDB cursor that tracks in-flight queries on its DuckDBInstance."""

    def __init__(self, cursor: Any, instance: DuckDBInstance) -> None:
        self._cursor = cursor
        self._instance = instance
        self._closed = False
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                cur = self._cursor
                self._cursor = None
                self._instance.release_cursor(cur)

    def __getattr__(self, name: str) -> Any:
        if self._cursor is None:
            raise RuntimeError("Cursor is closed")
        return getattr(self._cursor, name)


class DuckDBHolder:
    """Process-wide, thread-safe holder keeping one hardened connection per resolved database file."""

    _instance: DuckDBHolder | None = None
    _global_lock = threading.Lock()

    def __init__(self, *, max_idle_seconds: float = 300.0) -> None:
        # Key: (st_dev, st_ino) -> DuckDBInstance (including retired-but-still-open instances)
        self._instances_by_dev_ino: dict[tuple[int, int], DuckDBInstance] = {}
        # Key: configured_path -> (st_dev, st_ino)
        self._configured_to_dev_ino: dict[str, tuple[int, int]] = {}
        self._closed = False
        self._lock = threading.Lock()
        self._max_idle_seconds = max_idle_seconds
        self._reaper_lock = threading.Lock()
        self._reaper_thread: threading.Thread | None = None
        self._stop_reaper = threading.Event()

    @classmethod
    def get_holder(cls) -> DuckDBHolder:
        with cls._global_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset(cls) -> None:
        with cls._global_lock:
            if cls._instance is not None:
                cls._instance.close_all()
                cls._instance = None

    def _ensure_reaper_started(self) -> None:
        if self._max_idle_seconds <= 0:
            return
        with self._reaper_lock:
            if self._reaper_thread is None and not self._closed:
                self._stop_reaper.clear()
                self._reaper_thread = threading.Thread(
                    target=self._reaper_loop,
                    daemon=True,
                    name="duckdb-holder-reaper",
                )
                self._reaper_thread.start()

    def _reaper_loop(self) -> None:
        interval = max(0.05, min(self._max_idle_seconds / 2.0, 10.0))
        while not self._stop_reaper.wait(timeout=interval):
            if self._closed:
                break
            now = time.monotonic()
            to_close: list[DuckDBInstance] = []
            with self._lock:
                for key, inst in list(self._instances_by_dev_ino.items()):
                    with inst.lock:
                        idle_expired = (
                            inst.in_flight <= 0
                            and (now - inst.last_used_at) > self._max_idle_seconds
                        )
                        retired_drained = (
                            inst.is_retired
                            and inst.in_flight <= 0
                            and not self._is_current(key)
                        )
                    if idle_expired or retired_drained:
                        self._instances_by_dev_ino.pop(key, None)
                        for cfg_path, cfg_key in list(self._configured_to_dev_ino.items()):
                            if cfg_key == key:
                                self._configured_to_dev_ino.pop(cfg_path, None)
                        to_close.append(inst)
            for inst in to_close:
                inst.close_instance()

    def _is_current(self, key: tuple[int, int]) -> bool:
        return any(k == key for k in self._configured_to_dev_ino.values())

    def _on_cursor_released(self, inst: DuckDBInstance) -> None:
        with self._lock:
            with inst.lock:
                should_close = (
                    inst.is_retired
                    and inst.in_flight <= 0
                    and not self._is_current(inst.key)
                )
            if should_close:
                inst.close_instance()
                self._instances_by_dev_ino.pop(inst.key, None)

    def invalidate_path(self, path: str) -> None:
        """Drop invalidated instance (e.g. after FatalException) so next query reopens."""
        with self._lock:
            key = self._configured_to_dev_ino.pop(path, None)
            if key is not None:
                inst = self._instances_by_dev_ino.pop(key, None)
                if inst is not None:
                    inst.close_instance()

    def get_cursor(
        self,
        path: str,
        *,
        allowed_directories: Sequence[str] = (),
        memory_limit: str | None = None,
        threads: int | None = None,
        temp_directory: str | None = None,
        max_temp_directory_size: str | None = None,
    ) -> Any:
        with self._lock:
            if self._closed:
                raise RuntimeError("DuckDBHolder is closed")
            self._ensure_reaper_started()

            is_memory = path == ":memory:" or path.startswith(":memory:")

            if is_memory:
                key = (0, id(path))
                resolved = path
                stat_id = None
            else:
                st = None
                resolved = None
                # Handle transient filesystem race during atomic symlink replace (macOS/POSIX)
                for _ in range(25):
                    try:
                        st = os.stat(path)
                        resolved = os.path.realpath(path)
                        if os.path.exists(resolved):
                            break
                    except (FileNotFoundError, OSError):
                        pass
                    time.sleep(0.002)

                if st is None or resolved is None:
                    # Fallback to current instance if currently open and not closed
                    curr_key = self._configured_to_dev_ino.get(path)
                    if curr_key is not None:
                        curr_inst = self._instances_by_dev_ino.get(curr_key)
                        if curr_inst is not None and not curr_inst.is_closed:
                            return curr_inst.acquire_cursor()
                    raise FileNotFoundError("DuckDB database file not found")

                stat_id = (st.st_dev, st.st_ino, st.st_mtime_ns)
                key = (st.st_dev, st.st_ino)

            params = (
                tuple(sorted(allowed_directories)),
                memory_limit,
                threads,
                temp_directory,
                max_temp_directory_size,
            )

            # Check if this configured_path was pointing to a different file
            old_key = self._configured_to_dev_ino.get(path)
            if old_key is not None and old_key != key:
                # Update configured mapping to new key first
                self._configured_to_dev_ino[path] = key
                old_inst = self._instances_by_dev_ino.get(old_key)
                if old_inst is not None:
                    with old_inst.lock:
                        old_inst.is_retired = True
                        should_close = (
                            old_inst.in_flight <= 0 and not self._is_current(old_key)
                        )
                    if should_close:
                        old_inst.close_instance()
                        self._instances_by_dev_ino.pop(old_key, None)
            else:
                self._configured_to_dev_ino[path] = key

            # Check if an instance already exists for this (device, inode)
            existing = self._instances_by_dev_ino.get(key)
            if existing is not None and not existing.is_closed:
                # Refuse if parameters differ
                if existing.params != params:
                    raise ValueError(
                        "DuckDB database file is already open with different hardening parameters"
                    )
                # Check for unsupported in-place rewrite of active file
                if (
                    stat_id is not None
                    and existing.stat_identity is not None
                    and stat_id[2] != existing.stat_identity[2]
                ):
                    raise RuntimeError(
                        "In-place rewrite of active DuckDB file is not supported; use atomic symlink swaps"
                    )

                # REUSE existing instance! Unretire it since it is now current for `path`
                with existing.lock:
                    existing.is_retired = False
                return existing.acquire_cursor()

            # Open and harden a new instance
            conn = connect_hardened(
                resolved if not is_memory else path,
                read_only=True,
                allowed_directories=allowed_directories,
                memory_limit=memory_limit,
                threads=threads,
                temp_directory=temp_directory,
                max_temp_directory_size=max_temp_directory_size,
            )
            probe_results = probe(conn)
            if not all(probe_results.values()):
                try:
                    conn.close()
                except Exception:
                    pass
                raise RuntimeError("DuckDB probe conformance check failed")

            inst = DuckDBInstance(
                holder=self,
                key=key,
                configured_path=path,
                resolved_path=resolved,
                stat_identity=stat_id,
                connection=conn,
                params=params,
            )
            self._instances_by_dev_ino[key] = inst
            return inst.acquire_cursor()

    def close_all(self) -> None:
        self._stop_reaper.set()
        with self._lock:
            self._closed = True
            for inst in list(self._instances_by_dev_ino.values()):
                inst.close_instance()
            self._instances_by_dev_ino.clear()
            self._configured_to_dev_ino.clear()


def probe(conn: Any) -> dict[str, bool]:
    """Run conformance checks against a connection and return {check_name: passed}."""
    checks: dict[str, bool] = {}

    # 1. external_access_disabled: SELECT current_setting('enable_external_access') is false.
    try:
        row = conn.execute("SELECT current_setting('enable_external_access')").fetchone()
        val = row[0] if row else None
        checks["external_access_disabled"] = bool(val is False or str(val).lower() == "false")
    except Exception:
        checks["external_access_disabled"] = False

    # 2. config_locked: SET threads = 1 is refused.
    try:
        conn.execute("SET threads = 1")
        checks["config_locked"] = False
    except duckdb.Error:
        checks["config_locked"] = True
    except Exception:
        checks["config_locked"] = False

    # Retrieve allowed directories if configured
    try:
        row = conn.execute("SELECT current_setting('allowed_directories')").fetchone()
        allowed = row[0] if row else []
    except Exception:
        allowed = []
    if not isinstance(allowed, list):
        allowed = []

    # 3. read_text_outside_refused: SELECT * FROM read_text('/etc/hostname') is refused (any path outside the allowed directories).
    read_path = "/etc/hostname"
    if any(read_path.startswith(d) for d in allowed):
        for candidate in ["/etc/shadow", "/root/outside.txt", "/proc/version"]:
            if not any(candidate.startswith(d) for d in allowed):
                read_path = candidate
                break
    try:
        conn.execute(f"SELECT * FROM read_text('{read_path}')")
        checks["read_text_outside_refused"] = False
    except duckdb.Error:
        checks["read_text_outside_refused"] = True
    except Exception:
        checks["read_text_outside_refused"] = False

    # 4. copy_to_refused: COPY (SELECT 1) TO '/tmp/<random>.csv' is refused.
    copy_dir = "/tmp"
    if any(copy_dir.startswith(d.rstrip("/")) for d in allowed):
        for candidate in ["/var/tmp", "/dev/shm", "/opt"]:
            if not any(candidate.startswith(d.rstrip("/")) for d in allowed):
                copy_dir = candidate
                break
    copy_path = f"{copy_dir}/probe_{uuid.uuid4().hex}.csv"
    try:
        conn.execute(f"COPY (SELECT 1) TO '{copy_path}'")
        checks["copy_to_refused"] = False
        if os.path.exists(copy_path):
            try:
                os.remove(copy_path)
            except Exception:
                pass
    except duckdb.Error:
        checks["copy_to_refused"] = True
    except Exception:
        checks["copy_to_refused"] = False

    # 5. attach_refused: ATTACH ':memory:' AS x or an attach of an outside file is refused.
    attach_refused = False
    try:
        conn.execute("ATTACH ':memory:' AS probe_mem_check")
    except duckdb.Error:
        attach_refused = True

    if not attach_refused:
        attach_dir = "/tmp"
        if any(attach_dir.startswith(d.rstrip("/")) for d in allowed):
            for candidate in ["/var/tmp", "/dev/shm", "/opt"]:
                if not any(candidate.startswith(d.rstrip("/")) for d in allowed):
                    attach_dir = candidate
                    break
        outside_file = f"{attach_dir}/probe_outside_{uuid.uuid4().hex}.duckdb"
        try:
            conn.execute(f"ATTACH '{outside_file}' AS probe_outside_check")
        except duckdb.Error:
            attach_refused = True
        else:
            try:
                conn.execute("DETACH probe_outside_check")
            except Exception:
                pass
            if os.path.exists(outside_file):
                try:
                    os.remove(outside_file)
                except Exception:
                    pass
    checks["attach_refused"] = attach_refused

    # 6. install_refused: INSTALL httpfs is refused.
    try:
        conn.execute("INSTALL httpfs")
        checks["install_refused"] = False
    except duckdb.Error:
        checks["install_refused"] = True
    except Exception:
        checks["install_refused"] = False

    # 7. load_refused: LOAD httpfs is refused.
    try:
        conn.execute("LOAD httpfs")
        checks["load_refused"] = False
    except duckdb.Error:
        checks["load_refused"] = True
    except Exception:
        checks["load_refused"] = False

    # 8. select_ok: SELECT 42 returns 42.
    try:
        res = conn.execute("SELECT 42").fetchone()
        checks["select_ok"] = bool(res is not None and len(res) == 1 and res[0] == 42)
    except Exception:
        checks["select_ok"] = False

    return checks


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="DuckDB hardening policy enforcement and probe tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    probe_parser = subparsers.add_parser("probe", help="Run conformance checks against a DuckDB database")
    probe_parser.add_argument("--path", required=True, help="Path to DuckDB database file")
    probe_parser.add_argument("--allowed-dir", action="append", default=[], help="Allowed directory path (can be specified multiple times)")

    args = parser.parse_args(argv)

    if args.command == "probe":
        allowed_dirs = []
        for d in args.allowed_dir:
            if isinstance(d, list):
                allowed_dirs.extend(d)
            else:
                allowed_dirs.append(d)

        conn = connect_hardened(args.path, allowed_directories=allowed_dirs)
        try:
            results = probe(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        print(json.dumps(results, indent=2))
        return 0 if all(results.values()) else 1

    return 1


if __name__ == "__main__":
    sys.exit(main())

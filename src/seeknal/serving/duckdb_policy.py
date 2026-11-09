"""Locked-down DuckDB connection policy and probe conformance tool."""

from __future__ import annotations

import argparse
import json
import os
import sys
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
) -> None:
    """Harden a DuckDB connection in strict order."""
    # 1. sets allowed_directories (when non-empty; absolute, normalized paths, each must end with /, otherwise ValueError)
    if allowed_directories:
        validated_dirs = [_validate_allowed_directory(d) for d in allowed_directories]
        conn.execute("SET allowed_directories = ?", [validated_dirs])

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
) -> Any:
    """Open and harden a DuckDB connection, never falling back to unhardened."""
    conn = duckdb.connect(path, read_only=read_only)
    try:
        harden_connection(
            conn,
            allowed_directories=allowed_directories,
            memory_limit=memory_limit,
            threads=threads,
        )
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    return conn


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

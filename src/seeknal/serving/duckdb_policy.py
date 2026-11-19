"""Locked-down DuckDB connection policy and probe conformance tool."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import duckdb

_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_EXT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_S3_PREFIX_RE = re.compile(r"^s3://[A-Za-z0-9._-]+/.+/$")


class IcebergDirectHardeningError(RuntimeError):
    """Secret-safe error raised when an iceberg_direct hardening step fails."""

    def __init__(self, step_name: str) -> None:
        self.step_name = step_name
        super().__init__(f"Iceberg direct hardening failed at step '{step_name}'")


@dataclass(frozen=True)
class Step:
    """A single pure-planned hardening statement for iceberg_direct."""

    phase: str
    name: str
    sql: str
    redacted_sql: str | None = None
    capability: str | None = None
    capability_value: bool | None = None

    @property
    def safe_sql(self) -> str:
        return self.redacted_sql if self.redacted_sql is not None else self.sql


def _quote_sql_string(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected string for SQL literal quoting, got {type(value).__name__}")
    if "\x00" in value:
        raise ValueError("Null byte not permitted in SQL string literal")
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _validate_catalog_alias(alias: str) -> str:
    if not isinstance(alias, str) or not _ALIAS_RE.fullmatch(alias):
        raise ValueError("Catalog alias must match ^[a-z][a-z0-9_]{0,31}$")
    if alias in ("system", "temp", "memory", "main"):
        raise ValueError(f"Catalog alias '{alias}' is reserved")
    return alias


def _normalize_dir_with_slash(path: str, label: str = "directory") -> str:
    if not isinstance(path, str) or not path.strip():
        raise ValueError(f"{label} must be a non-empty string")
    p = path.strip()
    if not os.path.isabs(p):
        raise ValueError(f"{label} must be an absolute path")
    norm = os.path.normpath(p)
    if p.rstrip("/") != norm.rstrip("/"):
        raise ValueError(f"{label} must be a normalized path")
    return "/" if norm == "/" else norm.rstrip("/") + "/"


def _resolve_secret_from_cfg(
    var_name: str,
    cfg: Any,
    env: Mapping[str, str] | None = None,
) -> str:
    if not isinstance(var_name, str) or not var_name.strip():
        raise ValueError("Missing secret environment variable name")
    name = var_name.strip()
    secrets_env = getattr(cfg, "secrets_env", None)
    if isinstance(cfg, Mapping) and secrets_env is None:
        secrets_env = cfg.get("secrets_env")
    if isinstance(secrets_env, Mapping) and name in secrets_env:
        val = secrets_env[name]
        if isinstance(val, str) and val:
            return val
    proc_env = env if env is not None else os.environ
    if name in proc_env and proc_env[name]:
        return str(proc_env[name])
    raise ValueError(f"Missing required environment variable '{name}'")


def _cfg_attr(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def plan_iceberg_direct(
    cfg: Any,
    *,
    env: Mapping[str, str] | None = None,
    s3_prefix_confinement_supported: bool = True,
    standin_catalogs: Mapping[str, str] | None = None,
) -> list[Step]:
    """Build the ordered hardening statements for iceberg_direct as a pure function."""
    ext_dir_raw = _cfg_attr(cfg, "extension_directory")
    ext_dir_slash = _normalize_dir_with_slash(ext_dir_raw, "extension_directory")
    ext_dir_clean = ext_dir_slash.rstrip("/") or "/"

    temp_dir_raw = _cfg_attr(cfg, "temp_directory")
    temp_dir_slash: str | None = None
    temp_dir_clean: str | None = None
    if temp_dir_raw is not None:
        temp_dir_slash = _normalize_dir_with_slash(temp_dir_raw, "temp_directory")
        temp_dir_clean = temp_dir_slash.rstrip("/") or "/"
        if (
            temp_dir_slash == ext_dir_slash
            or temp_dir_slash.startswith(ext_dir_slash)
            or ext_dir_slash.startswith(temp_dir_slash)
        ):
            raise ValueError("temp_directory and extension_directory must not overlap")

    s3_prefixes_raw = _cfg_attr(cfg, "allowed_s3_prefixes", ()) or ()
    if not isinstance(s3_prefixes_raw, (list, tuple)) or not s3_prefixes_raw:
        raise ValueError("allowed_s3_prefixes must be a non-empty sequence")
    s3_prefixes: list[str] = []
    for pfx in s3_prefixes_raw:
        if not isinstance(pfx, str) or not _S3_PREFIX_RE.fullmatch(pfx) or ".." in pfx or "//" in pfx[len("s3://"):]:
            raise ValueError("Invalid S3 prefix in allowed_s3_prefixes")
        s3_prefixes.append(pfx)

    extensions_raw = _cfg_attr(cfg, "extensions", ()) or ()
    if not isinstance(extensions_raw, (list, tuple)):
        raise ValueError("extensions must be a sequence")
    extensions: list[str] = []
    for ext in extensions_raw:
        if not isinstance(ext, str) or not _EXT_NAME_RE.fullmatch(ext):
            raise ValueError(f"Invalid extension name: {ext!r}")
        extensions.append(ext)

    catalogs_raw = _cfg_attr(cfg, "iceberg_catalogs", {}) or {}
    if not isinstance(catalogs_raw, Mapping) or not catalogs_raw:
        raise ValueError("iceberg_catalogs must be a non-empty mapping")

    steps: list[Step] = []

    # (a) allowed_directories = extension dir + temp dir (+ S3 prefixes when supported)
    allowed_dirs: list[str] = [ext_dir_slash]
    if temp_dir_slash is not None:
        allowed_dirs.append(temp_dir_slash)
    if s3_prefix_confinement_supported:
        allowed_dirs.extend(s3_prefixes)

    dirs_literal = "[" + ", ".join(_quote_sql_string(d) for d in allowed_dirs) + "]"
    steps.append(
        Step(
            phase="a",
            name="a_allowed_directories",
            sql=f"SET allowed_directories = {dirs_literal}",
            capability="s3_prefix_confinement",
            capability_value=bool(s3_prefix_confinement_supported),
        )
    )
    steps.append(
        Step(
            phase="a",
            name="a_allowed_paths",
            sql="SET allowed_paths = []",
        )
    )

    # (b) autoinstall/autoload off, extension_directory set, LOAD each allowlisted extension (never INSTALL)
    steps.append(
        Step(
            phase="b",
            name="b_autoinstall_off",
            sql="SET autoinstall_known_extensions = false",
        )
    )
    steps.append(
        Step(
            phase="b",
            name="b_autoload_off",
            sql="SET autoload_known_extensions = false",
        )
    )
    steps.append(
        Step(
            phase="b",
            name="b_allow_persistent_secrets_off",
            sql="SET allow_persistent_secrets = false",
        )
    )
    steps.append(
        Step(
            phase="b",
            name="b_extension_directory",
            sql=f"SET extension_directory = {_quote_sql_string(ext_dir_clean)}",
        )
    )
    steps.append(
        Step(
            phase="b",
            name="b_disable_extension_repository",
            sql="SET custom_extension_repository = '/nonexistent_extension_repository'",
        )
    )
    for ext in extensions:
        steps.append(
            Step(
                phase="b",
                name=f"b_load_extension_{ext}",
                sql=f"LOAD {ext}",
            )
        )

    # (c) CREATE TEMPORARY SECRET for S3 and for the catalog OAuth2 client (never persistent)
    # and (d) ATTACH '<warehouse>' AS <alias> (TYPE ICEBERG, ENDPOINT ..., READ_ONLY ...) per catalog
    attach_steps: list[Step] = []
    for raw_alias, cat in catalogs_raw.items():
        alias = _validate_catalog_alias(str(raw_alias))
        endpoint = str(_cfg_attr(cat, "endpoint", "")).strip()
        warehouse = str(_cfg_attr(cat, "warehouse", "")).strip()
        token_uri = str(_cfg_attr(cat, "token_uri", "")).strip()
        scope = _cfg_attr(cat, "scope")
        access_delegation = str(_cfg_attr(cat, "access_delegation", "vended")).strip()
        if access_delegation not in ("vended", "none"):
            raise ValueError(f"Invalid access_delegation for catalog '{alias}'")

        cid_env = str(_cfg_attr(cat, "client_id_env", "")).strip()
        csec_env = str(_cfg_attr(cat, "client_secret_env", "")).strip()
        client_id_val = _resolve_secret_from_cfg(cid_env, cfg, env)
        client_secret_val = _resolve_secret_from_cfg(csec_env, cfg, env)

        s3_cfg = _cfg_attr(cat, "s3")
        if s3_cfg is None:
            raise ValueError(f"Missing s3 configuration for catalog '{alias}'")
        s3_endpoint = str(_cfg_attr(s3_cfg, "endpoint", "")).strip()
        s3_region = str(_cfg_attr(s3_cfg, "region", "")).strip()
        s3_url_style = str(_cfg_attr(s3_cfg, "url_style", "path")).strip()
        if s3_url_style not in ("path", "vhost"):
            raise ValueError(f"Invalid S3 url_style for catalog '{alias}'")
        s3_use_ssl = bool(_cfg_attr(s3_cfg, "use_ssl", True))
        s3_key_env = str(_cfg_attr(s3_cfg, "key_id_env", "")).strip()
        s3_sec_env = str(_cfg_attr(s3_cfg, "secret_env", "")).strip()
        s3_key_val = _resolve_secret_from_cfg(s3_key_env, cfg, env)
        s3_sec_val = _resolve_secret_from_cfg(s3_sec_env, cfg, env)

        use_ssl_sql = "true" if s3_use_ssl else "false"
        scope_list = ", ".join(_quote_sql_string(p) for p in s3_prefixes)

        if standin_catalogs is not None and alias in standin_catalogs:
            # Offline stand-in secret using built-in TYPE HTTP so secret manager mechanics execute without httpfs/iceberg
            s3_sql = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{'X-S3-Key': {_quote_sql_string(s3_key_val)}, 'X-S3-Secret': {_quote_sql_string(s3_sec_val)}}}"
                f")"
            )
            s3_redacted = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{'X-S3-Key': '<redacted>', 'X-S3-Secret': '<redacted>'}}"
                f")"
            )
            oauth_sql = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{'X-Client-Id': {_quote_sql_string(client_id_val)}, 'X-Client-Secret': {_quote_sql_string(client_secret_val)}}}"
                f")"
            )
            oauth_redacted = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{'X-Client-Id': '<redacted>', 'X-Client-Secret': '<redacted>'}}"
                f")"
            )
        else:
            s3_sql = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE S3, "
                f"KEY_ID {_quote_sql_string(s3_key_val)}, "
                f"SECRET {_quote_sql_string(s3_sec_val)}, "
                f"ENDPOINT {_quote_sql_string(s3_endpoint)}, "
                f"REGION {_quote_sql_string(s3_region)}, "
                f"URL_STYLE {_quote_sql_string(s3_url_style)}, "
                f"USE_SSL {use_ssl_sql}, "
                f"SCOPE [{scope_list}]"
                f")"
            )
            s3_redacted = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE S3, "
                f"KEY_ID '<redacted>', "
                f"SECRET '<redacted>', "
                f"ENDPOINT {_quote_sql_string(s3_endpoint)}, "
                f"REGION {_quote_sql_string(s3_region)}, "
                f"URL_STYLE {_quote_sql_string(s3_url_style)}, "
                f"USE_SSL {use_ssl_sql}, "
                f"SCOPE [{scope_list}]"
                f")"
            )

            scope_clause = (
                f", OAUTH2_SCOPE {_quote_sql_string(str(scope).strip())}"
                if scope
                else ""
            )
            oauth_sql = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE ICEBERG, "
                f"CLIENT_ID {_quote_sql_string(client_id_val)}, "
                f"CLIENT_SECRET {_quote_sql_string(client_secret_val)}, "
                f"OAUTH2_SERVER_URI {_quote_sql_string(token_uri)}"
                f"{scope_clause}"
                f")"
            )
            oauth_redacted = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE ICEBERG, "
                f"CLIENT_ID '<redacted>', "
                f"CLIENT_SECRET '<redacted>', "
                f"OAUTH2_SERVER_URI {_quote_sql_string(token_uri)}"
                f"{scope_clause}"
                f")"
            )

        steps.append(
            Step(
                phase="c",
                name=f"c_secret_s3_{alias}",
                sql=s3_sql,
                redacted_sql=s3_redacted,
            )
        )
        steps.append(
            Step(
                phase="c",
                name=f"c_secret_oauth_{alias}",
                sql=oauth_sql,
                redacted_sql=oauth_redacted,
            )
        )

        if standin_catalogs is not None and alias in standin_catalogs:
            standin_path = standin_catalogs[alias]
            attach_sql = f"ATTACH {_quote_sql_string(standin_path)} AS {alias} (READ_ONLY)"
        else:
            del_mode = "vended-credentials" if access_delegation == "vended" else "none"
            attach_sql = (
                f"ATTACH {_quote_sql_string(warehouse)} AS {alias} ("
                f"TYPE ICEBERG, "
                f"ENDPOINT {_quote_sql_string(endpoint)}, "
                f"SECRET iceberg_{alias}, "
                f"ACCESS_DELEGATION_MODE {_quote_sql_string(del_mode)}, "
                f"READ_ONLY"
                f")"
            )
        attach_steps.append(
            Step(
                phase="d",
                name=f"d_attach_{alias}",
                sql=attach_sql,
            )
        )

    steps.extend(attach_steps)

    # (e) SET enable_external_access=false ONLY when external_access_after_attach: off (default off)
    ext_after = str(_cfg_attr(cfg, "external_access_after_attach", "off")).strip().lower()
    if ext_after not in ("off", "on"):
        raise ValueError("external_access_after_attach must be 'off' or 'on'")
    if ext_after == "off":
        steps.append(
            Step(
                phase="e",
                name="e_disable_external_access",
                sql="SET enable_external_access = false",
            )
        )

    # (f) memory_limit, threads, temp settings
    mem_limit = _cfg_attr(cfg, "memory_limit")
    if mem_limit is not None:
        steps.append(
            Step(
                phase="f",
                name="f_memory_limit",
                sql=f"SET memory_limit = {_quote_sql_string(str(mem_limit).strip())}",
            )
        )
    threads = _cfg_attr(cfg, "threads")
    if threads is not None:
        if isinstance(threads, bool) or not isinstance(threads, int) or threads <= 0:
            raise ValueError("threads must be a positive integer")
        steps.append(
            Step(
                phase="f",
                name="f_threads",
                sql=f"SET threads = {int(threads)}",
            )
        )

    max_temp_sz = _cfg_attr(cfg, "max_temp_directory_size")
    if temp_dir_clean is not None:
        steps.append(
            Step(
                phase="f",
                name="f_temp_directory",
                sql=f"SET temp_directory = {_quote_sql_string(temp_dir_clean)}",
            )
        )
        if max_temp_sz is not None:
            steps.append(
                Step(
                    phase="f",
                    name="f_max_temp_directory_size",
                    sql=f"SET max_temp_directory_size = {_quote_sql_string(str(max_temp_sz).strip())}",
                )
            )
    else:
        steps.append(
            Step(
                phase="f",
                name="f_max_temp_directory_size",
                sql="SET max_temp_directory_size = '0B'",
            )
        )
        steps.append(
            Step(
                phase="f",
                name="f_temp_directory",
                sql="SET temp_directory = ''",
            )
        )

    # (g) lock_configuration=true
    steps.append(
        Step(
            phase="g",
            name="g_lock_configuration",
            sql="SET lock_configuration = true",
        )
    )

    return steps


def harden_iceberg_direct(
    conn: Any,
    cfg: Any,
    *,
    env: Mapping[str, str] | None = None,
    standin_catalogs: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Harden an in-memory DuckDB connection for the iceberg_direct profile in strict order.

    Closes the connection and raises a secret-safe IcebergDirectHardeningError naming only
    the failed step on any failure.
    """
    current_step = "plan_iceberg_direct"
    capabilities: dict[str, Any] = {
        "s3_prefix_confinement": False,
        "external_access_after_attach": str(
            _cfg_attr(cfg, "external_access_after_attach", "off")
        ).strip().lower(),
    }
    try:
        temp_dir_raw = _cfg_attr(cfg, "temp_directory")
        if temp_dir_raw is not None:
            current_step = "f_temp_directory"
            td = str(temp_dir_raw).strip()
            if not os.path.isabs(td) or not os.path.exists(td) or not os.path.isdir(td) or not os.access(td, os.W_OK):
                raise ValueError("Invalid or non-writable temp_directory")

        steps = plan_iceberg_direct(
            cfg,
            env=env,
            s3_prefix_confinement_supported=True,
            standin_catalogs=standin_catalogs,
        )

        # Notice: on DuckDB 2.0.0, `SET temp_directory` is rejected by DuckDB after
        # `SET enable_external_access = false` with:
        # `PermissionException: Modifying the temp_directory has been disabled by configuration`.
        # When `temp_directory` is set before step (e) or at connection init, we still verify
        # or apply `temp_directory` before `enable_external_access = false` if not already set,
        # and handle step `f_temp_directory` cleanly.
        for step in steps:
            current_step = step.name
            if step.name == "a_allowed_directories":
                try:
                    conn.execute(step.sql)
                    capabilities["s3_prefix_confinement"] = True
                except duckdb.Error:
                    # Fallback if DuckDB version rejects s3:// in allowed_directories
                    fallback_steps = plan_iceberg_direct(
                        cfg,
                        env=env,
                        s3_prefix_confinement_supported=False,
                        standin_catalogs=standin_catalogs,
                    )
                    fb_step = next(s for s in fallback_steps if s.name == "a_allowed_directories")
                    conn.execute(fb_step.sql)
                    capabilities["s3_prefix_confinement"] = False
                continue

            if step.name == "e_disable_external_access":
                # Apply temp_directory right before disabling external access so DuckDB 2.0.0
                # does not block modifying temp_directory in step (f).
                for f_step in steps:
                    if f_step.name == "f_temp_directory":
                        current_step = "f_temp_directory"
                        conn.execute(f_step.sql)
                current_step = step.name
                conn.execute(step.sql)
                continue

            if step.name == "f_temp_directory" and capabilities["external_access_after_attach"] == "off":
                # Already applied immediately prior to e_disable_external_access; verify setting matches.
                expected_td = ""
                if temp_dir_raw is not None:
                    expected_td = (_normalize_dir_with_slash(temp_dir_raw, "temp_directory").rstrip("/") or "/")
                row = conn.execute("SELECT current_setting('temp_directory')").fetchone()
                actual_td = str(row[0]) if row and row[0] is not None else ""
                if actual_td != expected_td:
                    conn.execute(step.sql)
                continue

            conn.execute(step.sql)
    except IcebergDirectHardeningError:
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
        raise IcebergDirectHardeningError(current_step) from None

    try:
        setattr(conn, "_iceberg_direct_capabilities", capabilities)
    except Exception:
        pass
    return capabilities


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
    """A hardened DuckDB database instance for a resolved database file or iceberg_direct generation."""

    def __init__(
        self,
        holder: DuckDBHolder,
        key: tuple[int, int],
        configured_path: str,
        resolved_path: str,
        stat_identity: tuple[int, int, int] | None,
        connection: Any,
        params: tuple,
        *,
        created_at: float | None = None,
        recycle_seconds: float | None = None,
        profile: str = "local_file",
    ) -> None:
        self.holder = holder
        self.key = key
        self.configured_path = configured_path
        self.resolved_path = resolved_path
        self.stat_identity = stat_identity
        self.connection = connection
        self.params = params
        self.profile = profile
        self.recycle_seconds = recycle_seconds
        now = created_at if created_at is not None else holder._clock()
        self.created_at = now
        self.in_flight = 0
        self.is_retired = False
        self.is_closed = False
        self.last_used_at = now
        self.lock = threading.Lock()

    def acquire_cursor(self) -> Any:
        with self.lock:
            if self.is_closed:
                raise RuntimeError("DuckDB instance is closed")
            self.in_flight += 1
            self.last_used_at = self.holder._clock()
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
            self.last_used_at = self.holder._clock()
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


def _make_iceberg_locked_cursor(raw_cursor: Any, allowed_aliases: Sequence[str]) -> Any:
    """Wrap a DuckDB cursor for iceberg_direct to enforce post-lock refusal of DETACH and CREATE SECRET."""
    return _IcebergLockedCursor(raw_cursor, allowed_aliases)


_POST_LOCK_FORBIDDEN_SQL_RE = re.compile(
    r"^\s*(?:DETACH\b|(?:FORCE\s+)?INSTALL\b|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|PERSISTENT\s+)?SECRET\b|DROP\s+(?:TEMPORARY\s+|PERSISTENT\s+)?SECRET\b)",
    re.IGNORECASE,
)


class _IcebergLockedCursor:
    """Locked cursor proxy for iceberg_direct that refuses DETACH and CREATE/DROP SECRET after lock_configuration."""

    def __init__(self, raw_cursor: Any, allowed_aliases: Sequence[str] = ()) -> None:
        self._raw = raw_cursor
        self._allowed_aliases = tuple(allowed_aliases)

    def execute(self, query: str, parameters: Any = None) -> Any:
        if isinstance(query, str) and _POST_LOCK_FORBIDDEN_SQL_RE.match(query):
            raise duckdb.PermissionException(
                "Permission Error: Catalog DETACH and SECRET modifications are disabled after lock_configuration"
            )
        if parameters is not None:
            res = self._raw.execute(query, parameters)
        else:
            res = self._raw.execute(query)
        return self if res is self._raw else res

    def cursor(self) -> _IcebergLockedCursor:
        return _IcebergLockedCursor(self._raw.cursor(), self._allowed_aliases)

    def close(self) -> None:
        self._raw.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


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
    """Process-wide, thread-safe holder keeping one hardened connection per resolved database file or iceberg_direct generation."""

    _instance: DuckDBHolder | None = None
    _global_lock = threading.Lock()

    def __init__(
        self,
        *,
        max_idle_seconds: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        # Key: (st_dev, st_ino) -> DuckDBInstance (including retired-but-still-open instances)
        self._instances_by_dev_ino: dict[tuple[int, int], DuckDBInstance] = {}
        # Key: configured_path -> (st_dev, st_ino)
        self._configured_to_dev_ino: dict[str, tuple[int, int]] = {}
        self._generation_seq = 0
        self._clock = clock
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

    def _open_iceberg_direct_connection(
        self,
        cfg: Any,
        *,
        env: Mapping[str, str] | None = None,
        standin_catalogs: Mapping[str, str] | None = None,
    ) -> Any:
        raw_conn = duckdb.connect(":memory:")
        try:
            capabilities = harden_iceberg_direct(
                raw_conn,
                cfg,
                env=env,
                standin_catalogs=standin_catalogs,
            )
            catalogs_raw = _cfg_attr(cfg, "iceberg_catalogs", {}) or {}
            allowed_aliases = tuple(sorted(str(a) for a in catalogs_raw.keys()))
            locked_conn = _IcebergLockedCursor(raw_conn, allowed_aliases)
            try:
                setattr(locked_conn, "_iceberg_direct_capabilities", capabilities)
            except Exception:
                pass
            probe_results = probe_iceberg_direct(locked_conn, cfg)
            if not probe_iceberg_direct_ok(probe_results, cfg):
                try:
                    raw_conn.close()
                except Exception:
                    pass
                raise RuntimeError("DuckDB probe conformance check failed")
            return locked_conn
        except Exception:
            try:
                raw_conn.close()
            except Exception:
                pass
            raise

    def get_iceberg_cursor(
        self,
        cfg: Any,
        *,
        config_id: str | None = None,
        env: Mapping[str, str] | None = None,
        standin_catalogs: Mapping[str, str] | None = None,
        probe_on_reuse: bool = False,
    ) -> Any:
        """Acquire a cursor from the current iceberg_direct generation, recycling on expiry or probe failure."""
        with self._lock:
            if self._closed:
                raise RuntimeError("DuckDBHolder is closed")
            self._ensure_reaper_started()

            cfg_key = config_id or f"iceberg_direct:{id(cfg)}"
            recycle_raw = _cfg_attr(cfg, "recycle_seconds")
            recycle_seconds = float(recycle_raw) if recycle_raw is not None else None
            now = self._clock()

            curr_key = self._configured_to_dev_ino.get(cfg_key)
            curr_inst = self._instances_by_dev_ino.get(curr_key) if curr_key is not None else None

            needs_recycle = False
            if curr_inst is None or curr_inst.is_closed:
                needs_recycle = True
            else:
                if (
                    recycle_seconds is not None
                    and recycle_seconds > 0
                    and (now - curr_inst.created_at) >= recycle_seconds
                ):
                    needs_recycle = True
                elif probe_on_reuse:
                    try:
                        pr = probe_iceberg_direct(curr_inst.connection, cfg)
                        if not probe_iceberg_direct_ok(pr, cfg):
                            needs_recycle = True
                    except Exception:
                        needs_recycle = True

            if not needs_recycle and curr_inst is not None:
                with curr_inst.lock:
                    curr_inst.is_retired = False
                return curr_inst.acquire_cursor()

            # Build and probe new generation before retiring the old one
            new_conn = self._open_iceberg_direct_connection(
                cfg,
                env=env,
                standin_catalogs=standin_catalogs,
            )
            self._generation_seq += 1
            new_key = (-1, self._generation_seq)

            if curr_key is not None and curr_key != new_key:
                self._configured_to_dev_ino[cfg_key] = new_key
                if curr_inst is not None:
                    with curr_inst.lock:
                        curr_inst.is_retired = True
                        should_close = (
                            curr_inst.in_flight <= 0 and not self._is_current(curr_key)
                        )
                    if should_close:
                        curr_inst.close_instance()
                        self._instances_by_dev_ino.pop(curr_key, None)
            else:
                self._configured_to_dev_ino[cfg_key] = new_key

            new_inst = DuckDBInstance(
                holder=self,
                key=new_key,
                configured_path=cfg_key,
                resolved_path=":memory:",
                stat_identity=None,
                connection=new_conn,
                params=("iceberg_direct", cfg_key),
                created_at=now,
                recycle_seconds=recycle_seconds,
                profile="iceberg_direct",
            )
            self._instances_by_dev_ino[new_key] = new_inst
            return new_inst.acquire_cursor()

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


def probe_iceberg_direct(conn: Any, cfg: Any = None) -> dict[str, Any]:
    """Run conformance checks for the iceberg_direct profile and return report dict."""
    checks: dict[str, Any] = {}

    catalogs_raw = _cfg_attr(cfg, "iceberg_catalogs", {}) if cfg is not None else {}
    allowlisted_aliases = sorted(str(a) for a in (catalogs_raw or {}).keys())
    ext_after = (
        str(_cfg_attr(cfg, "external_access_after_attach", "off")).strip().lower()
        if cfg is not None
        else "off"
    )
    caps = getattr(conn, "_iceberg_direct_capabilities", None) or {}
    s3_confinement_cap = caps.get("s3_prefix_confinement", True)

    # 1. external_access_disabled (or report external_access_after_attach="on")
    try:
        row = conn.execute("SELECT current_setting('enable_external_access')").fetchone()
        val = row[0] if row else None
        is_disabled = bool(val is False or str(val).lower() == "false")
        checks["external_access_disabled"] = is_disabled
    except Exception:
        checks["external_access_disabled"] = False
    if ext_after == "on":
        checks["external_access_after_attach"] = "on"

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

    local_allowed = [d for d in allowed if isinstance(d, str) and not d.startswith("s3://")]
    s3_allowed = [d for d in allowed if isinstance(d, str) and d.startswith("s3://")]

    # 3. read_text_outside_refused
    read_path = "/etc/hostname"
    if any(read_path.startswith(d) for d in local_allowed):
        for candidate in ["/etc/shadow", "/root/outside.txt", "/proc/version"]:
            if not any(candidate.startswith(d) for d in local_allowed):
                read_path = candidate
                break
    try:
        conn.execute(f"SELECT * FROM read_text('{read_path}')")
        checks["read_text_outside_refused"] = False
    except duckdb.Error:
        checks["read_text_outside_refused"] = True
    except Exception:
        checks["read_text_outside_refused"] = False

    # 4. copy_to_refused
    copy_dir = "/tmp"
    if any(copy_dir.startswith(d.rstrip("/")) for d in local_allowed):
        for candidate in ["/var/tmp", "/dev/shm", "/opt"]:
            if not any(candidate.startswith(d.rstrip("/")) for d in local_allowed):
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

    # 5. install_refused
    try:
        conn.execute("INSTALL httpfs")
        checks["install_refused"] = False
    except duckdb.Error:
        checks["install_refused"] = True
    except Exception:
        checks["install_refused"] = False

    # 6. new_attach_refused: attaching an outside database file after lock is refused
    attach_dir = "/tmp"
    if any(attach_dir.startswith(d.rstrip("/")) for d in local_allowed):
        for candidate in ["/var/tmp", "/dev/shm", "/opt"]:
            if not any(candidate.startswith(d.rstrip("/")) for d in local_allowed):
                attach_dir = candidate
                break
    outside_file = f"{attach_dir}/probe_outside_{uuid.uuid4().hex}.duckdb"
    try:
        conn.execute(f"ATTACH '{outside_file}' AS probe_outside_check")
        checks["new_attach_refused"] = False
        try:
            conn.execute("DETACH probe_outside_check")
        except Exception:
            pass
        if os.path.exists(outside_file):
            try:
                os.remove(outside_file)
            except Exception:
                pass
    except duckdb.Error:
        checks["new_attach_refused"] = True
    except Exception:
        checks["new_attach_refused"] = False

    # 7. detach_refused: detaching an attached catalog (or system/memory) after lock is refused
    target_detach = allowlisted_aliases[0] if allowlisted_aliases else "memory"
    try:
        conn.execute(f"DETACH {target_detach}")
        checks["detach_refused"] = False
    except duckdb.Error:
        checks["detach_refused"] = True
    except Exception:
        checks["detach_refused"] = False

    # 8. create_secret_refused: creating a persistent or temporary secret after lock is refused
    persistent_refused = False
    try:
        conn.execute("CREATE PERSISTENT SECRET probe_pers_sec (TYPE HTTP)")
    except duckdb.Error:
        persistent_refused = True
    except Exception:
        persistent_refused = False

    temp_secret_refused = False
    try:
        conn.execute("CREATE TEMPORARY SECRET probe_tmp_sec (TYPE HTTP)")
    except duckdb.Error:
        temp_secret_refused = True
    except Exception:
        temp_secret_refused = False

    checks["create_secret_refused"] = bool(persistent_refused and temp_secret_refused)

    # 9. catalog_set_exact: duckdb_databases() = allowlisted aliases + system/temp/memory only
    try:
        rows = conn.execute("SELECT database_name FROM duckdb_databases()").fetchall()
        actual_dbs = {str(r[0]) for r in rows}
        expected_base = {"system", "temp", "memory"}
        expected_aliases = set(allowlisted_aliases)
        checks["catalog_set_exact"] = bool(
            expected_aliases.issubset(actual_dbs)
            and actual_dbs.issubset(expected_base | expected_aliases)
        )
    except Exception:
        checks["catalog_set_exact"] = False

    # 10. load_unlisted_refused: loading an unlisted external extension is refused
    unlisted_candidates = ["fts", "spatial", "inet", "tpch", "avro", "excel"]
    configured_exts = set(_cfg_attr(cfg, "extensions", ()) or ()) if cfg is not None else set()
    probe_ext = next((e for e in unlisted_candidates if e not in configured_exts), "fts")
    try:
        conn.execute(f"LOAD {probe_ext}")
        checks["load_unlisted_refused"] = False
    except duckdb.Error:
        checks["load_unlisted_refused"] = True
    except Exception:
        checks["load_unlisted_refused"] = False

    # 11. secrets_temporary_only: every secret in duckdb_secrets() has persistent == False
    try:
        sec_rows = conn.execute("SELECT name, persistent FROM duckdb_secrets()").fetchall()
        checks["secrets_temporary_only"] = bool(
            all(r[1] is False or str(r[1]).lower() == "false" for r in sec_rows)
        )
    except Exception:
        checks["secrets_temporary_only"] = False

    # 12. writes_refused on each attached catalog
    if allowlisted_aliases:
        writes_ok = True
        for alias in allowlisted_aliases:
            try:
                conn.execute(f"CREATE TABLE {alias}.main.__probe_write_check (id INT)")
                writes_ok = False
                try:
                    conn.execute(f"DROP TABLE {alias}.main.__probe_write_check")
                except Exception:
                    pass
            except duckdb.Error:
                pass
            except Exception:
                writes_ok = False
        checks["writes_refused"] = writes_ok
    else:
        checks["writes_refused"] = False

    # 13. s3_outside_prefix_refused (or reported unsupported)
    if not s3_confinement_cap or not s3_allowed:
        checks["s3_outside_prefix_refused"] = "unsupported"
    else:
        outside_s3 = f"s3://__probe_forbidden_bucket_{uuid.uuid4().hex[:8]}__/probe.parquet"
        try:
            conn.execute(f"SELECT * FROM read_parquet('{outside_s3}')")
            checks["s3_outside_prefix_refused"] = False
        except duckdb.PermissionException:
            checks["s3_outside_prefix_refused"] = True
        except duckdb.Error as exc:
            msg = str(exc)
            if "file system operations are disabled by configuration" in msg or "Permission Error" in msg:
                checks["s3_outside_prefix_refused"] = True
            else:
                checks["s3_outside_prefix_refused"] = False
        except Exception:
            checks["s3_outside_prefix_refused"] = False

    # 14. select_ok: SELECT 42 returns 42
    try:
        res = conn.execute("SELECT 42").fetchone()
        checks["select_ok"] = bool(res is not None and len(res) == 1 and res[0] == 42)
    except Exception:
        checks["select_ok"] = False

    return checks


def probe_iceberg_direct_ok(results: Mapping[str, Any], cfg: Any = None) -> bool:
    """Return True if all probe_iceberg_direct checks pass (fail closed on any failure)."""
    ext_after = (
        str(_cfg_attr(cfg, "external_access_after_attach", "off")).strip().lower()
        if cfg is not None
        else "off"
    )
    required_true = [
        "config_locked",
        "read_text_outside_refused",
        "copy_to_refused",
        "install_refused",
        "new_attach_refused",
        "detach_refused",
        "create_secret_refused",
        "catalog_set_exact",
        "load_unlisted_refused",
        "secrets_temporary_only",
        "writes_refused",
        "select_ok",
    ]
    for k in required_true:
        if results.get(k) is not True:
            return False

    if ext_after == "on":
        if results.get("external_access_after_attach") != "on":
            return False
    else:
        if results.get("external_access_disabled") is not True:
            return False

    s3_res = results.get("s3_outside_prefix_refused")
    if s3_res is not True and s3_res != "unsupported":
        return False

    return True


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

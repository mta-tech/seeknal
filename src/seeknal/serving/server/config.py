"""Server configuration model and validation for Seeknal Serving."""

from __future__ import annotations

import base64
import inspect
import json
import logging
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import re
from typing import Any

import yaml
from yaml.constructor import ConstructorError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.audit import (
    AuditConfig,
    AuditWriteError,
    verify_audit_config_offline,
)
from seeknal.serving.compilation_cache import (
    DEFAULT_COMPILATION_CACHE_MAX_ENTRIES,
    validate_compilation_cache_max_entries,
)
from seeknal.serving.discovery.config import DiscoveryConfig
from seeknal.serving.errors import register_config_secrets
from seeknal.serving.grant_token import _load_public_key
from seeknal.serving.ops import OpsConfig
from seeknal.serving.placement import Placement, PlacementError, UniqueKeySafeLoader
from seeknal.serving.postgres_session import (
    PostgresAccountConfig,
    PostgresFreshnessConfig,
    PostgresSessionConfig,
    PostgresTLSConfig,
    VALID_POSTGRES_MODES,
    VALID_SSLMODES,
)

try:
    from seeknal.serving.lakebase_endpoint import (
        effective_storage_class,
        parse_lakebase_block,
    )
except ImportError:
    from seeknal.serving.postgres_session import (
        effective_storage_class,
        parse_lakebase_block,
    )

logger = logging.getLogger(__name__)

VALID_WORKLOADS: frozenset[str] = frozenset(
    {"interactive", "bi", "report", "ask", "job"}
)
VALID_STORAGE_CLASSES: frozenset[str] = frozenset({"local", "object_storage"})
_FORBIDDEN_LITERAL_SECRET_KEYS: frozenset[str] = frozenset(
    {
        "dsn",
        "password",
        "passwd",
        "secret",
        "token",
        "api_token",
        "uri",
        "url",
        "connection_string",
    }
)
_QUALIFIED_SYNC_TABLE_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,62}$"
)


class ServerConfigError(Exception):
    """Raised when server configuration is invalid or missing."""


class AuditStartupError(ServerConfigError, AuditWriteError):
    """Raised when a fail-closed audit sink fails startup validation."""


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")
_SIMPLE_LOWER_ENUM_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_ICEBERG_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_S3_PREFIX_RE = re.compile(r"^s3://[A-Za-z0-9._-]+/.+/$")
_ALLOWED_ICEBERG_EXTENSIONS = frozenset({"iceberg", "httpfs", "avro"})
_SUSPICIOUS_TOKEN_RE = re.compile(
    r"(://|@|secret|password|passwd|token|bearer|private|canary|sentinel|opaque|dsn|credential)",
    re.IGNORECASE,
)


def _safe_label(value: Any, fallback: str = "<redacted>") -> str:
    """Return a safe diagnostic label for an identifier, redacting URI/secret-like strings."""
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    s = value.strip()
    if not s:
        return "<empty>"
    if _SUSPICIOUS_TOKEN_RE.search(s) or not _SAFE_IDENTIFIER_RE.match(s):
        return fallback
    return s


def _safe_enum_token(value: Any) -> str:
    """Return a strictly lowercase simple enum token or '<invalid>'."""
    if not isinstance(value, str):
        return "<invalid>"
    s = value.strip()
    if not s or _SUSPICIOUS_TOKEN_RE.search(s):
        return "<invalid>"
    if not _SIMPLE_LOWER_ENUM_RE.fullmatch(s):
        return "<invalid>"
    return s


def check_extension_files_on_disk(
    ext_dir: str | None,
    extensions: Sequence[str],
    *,
    eng_name: str = "duckdb",
    only_when_populated: bool = False,
) -> None:
    """Verify required DuckDB extension files exist on disk under extension_directory."""
    if not ext_dir or not isinstance(ext_dir, str):
        return
    if not os.path.isdir(ext_dir):
        if only_when_populated:
            return
        raise ServerConfigError(
            f"DuckDB iceberg_direct connection '{_safe_label(eng_name)}' "
            f"extension_directory '{ext_dir}' does not exist"
        )

    try:
        top_entries = os.listdir(ext_dir)
    except OSError:
        return

    if only_when_populated and not top_entries:
        return

    found_ext_files: set[str] = set()
    missing_avro_in_iceberg_dir = False
    for _root, _dirs, files in os.walk(ext_dir):
        file_set = set(files)
        for fname in file_set:
            if fname.endswith(".duckdb_extension"):
                found_ext_files.add(fname)
        if (
            "avro" in extensions
            and "iceberg.duckdb_extension" in file_set
            and "avro.duckdb_extension" not in file_set
        ):
            missing_avro_in_iceberg_dir = True

    if "avro" in extensions and (
        "avro.duckdb_extension" not in found_ext_files or missing_avro_in_iceberg_dir
    ):
        raise ServerConfigError(
            f"DuckDB iceberg_direct connection '{_safe_label(eng_name)}' is missing required "
            f"'avro.duckdb_extension' in extension_directory '{ext_dir}'"
        )

    if not only_when_populated:
        for ext in extensions:
            ext_fname = f"{ext}.duckdb_extension"
            if ext_fname not in found_ext_files:
                raise ServerConfigError(
                    f"DuckDB iceberg_direct connection '{_safe_label(eng_name)}' is missing required "
                    f"'{ext_fname}' in extension_directory '{ext_dir}'"
                )


def _format_yaml_error(exc: yaml.YAMLError, role: str, path: str | os.PathLike) -> str:
    """Format a PyYAML error with file path and line/column, never raw file snippets."""
    mark = getattr(exc, "problem_mark", None)
    if isinstance(exc, ConstructorError) and (
        hasattr(exc, "duplicate_key")
        or "duplicate YAML key" in str(getattr(exc, "problem", ""))
    ):
        raw_dup = getattr(exc, "duplicate_key", None)
        if raw_dup is None:
            m = re.search(
                r"duplicate YAML key '(.*)'", str(getattr(exc, "problem", ""))
            )
            raw_dup = m.group(1) if m else ""
        safe_key = _safe_label(str(raw_dup))
        if mark is not None:
            return (
                f"Invalid YAML in {role} '{path}' at line {mark.line + 1}, "
                f"column {mark.column + 1}: duplicate YAML key '{safe_key}' — remove duplicate key"
            )
        return (
            f"Invalid YAML in {role} '{path}': duplicate YAML key '{safe_key}' "
            f"— remove duplicate key"
        )
    if mark is not None:
        return (
            f"Invalid YAML in {role} '{path}' at line {mark.line + 1}, "
            f"column {mark.column + 1} — fix YAML syntax"
        )
    return f"Invalid YAML in {role} '{path}' — fix YAML syntax"


def _format_json_error(exc: json.JSONDecodeError, role: str, path: str | os.PathLike) -> str:
    """Format a JSONDecodeError with file path and line/column, never raw file snippets."""
    return (
        f"Invalid JSON in {role} '{path}' at line {exc.lineno}, "
        f"column {exc.colno} ({exc.msg}) — fix JSON syntax"
    )


@dataclass(frozen=True)
class PreflightResult:
    """Validated offline runtime-wiring artifacts ready for server assembly."""

    mode: str
    placement: Placement | None = None
    schemas: dict[str, list[str]] | None = None


def _b64url_decode(s: str) -> bytes:
    rem = len(s) % 4
    padded = s + ("=" * ((4 - rem) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def parse_jwks(data_or_str: dict | list | str) -> dict[str, str]:
    """Parse JWKS into a mapping of kid to PEM-encoded EC P-256 public key string."""
    if isinstance(data_or_str, str):
        try:
            raw = json.loads(data_or_str)
        except json.JSONDecodeError as exc:
            raise ServerConfigError(
                f"Invalid JSON in JWKS at line {exc.lineno}, column {exc.colno} ({exc.msg})"
            ) from exc
        except Exception as exc:
            raise ServerConfigError("Invalid JSON in JWKS") from exc
    else:
        raw = data_or_str

    if isinstance(raw, dict):
        if "keys" in raw:
            keys_list = raw["keys"]
        elif "kty" in raw:
            keys_list = [raw]
        else:
            raise ServerConfigError("JWKS must contain a 'keys' array")
    elif isinstance(raw, list):
        keys_list = raw
    else:
        raise ServerConfigError(
            f"JWKS must be an object or list, got {type(raw).__name__}"
        )

    if not isinstance(keys_list, list):
        raise ServerConfigError("JWKS 'keys' must be a list")
    if not keys_list:
        raise ServerConfigError("JWKS keys list must not be empty")

    seen_kids: set[str] = set()
    result: dict[str, str] = {}

    for i, key in enumerate(keys_list):
        if not isinstance(key, dict):
            raise ServerConfigError(f"JWK entry {i} must be an object")

        kid = key.get("kid")
        if not kid or not isinstance(kid, str) or not kid.strip():
            raise ServerConfigError(f"JWK entry {i} missing required 'kid'")
        kid_str = kid.strip()
        if kid_str in seen_kids:
            raise ServerConfigError(f"Duplicate kid '{kid_str}' in JWKS")
        seen_kids.add(kid_str)

        if "d" in key:
            raise ServerConfigError(f"JWK '{kid_str}' contains private key member 'd'")

        kty = key.get("kty")
        if kty != "EC":
            raise ServerConfigError(
                f"JWK '{kid_str}' has unsupported kty '{kty}', expected 'EC'"
            )

        crv = key.get("crv")
        if crv != "P-256":
            raise ServerConfigError(
                f"JWK '{kid_str}' has unsupported curve '{crv}', expected 'P-256'"
            )

        x_b64 = key.get("x")
        y_b64 = key.get("y")
        if (
            not x_b64
            or not isinstance(x_b64, str)
            or not y_b64
            or not isinstance(y_b64, str)
        ):
            raise ServerConfigError(
                f"JWK '{kid_str}' missing required 'x' or 'y' coordinate"
            )

        try:
            x_bytes = _b64url_decode(x_b64)
            y_bytes = _b64url_decode(y_b64)
        except Exception as exc:
            raise ServerConfigError(
                f"JWK '{_safe_label(kid_str)}' has invalid base64url coordinate"
            ) from exc

        if len(x_bytes) != 32 or len(y_bytes) != 32:
            raise ServerConfigError(
                f"JWK '{_safe_label(kid_str)}' coordinates must each be 32 bytes for P-256"
            )

        try:
            x_int = int.from_bytes(x_bytes, "big")
            y_int = int.from_bytes(y_bytes, "big")
            pub_numbers = ec.EllipticCurvePublicNumbers(x_int, y_int, ec.SECP256R1())
            pub_key = pub_numbers.public_key()
            pem_bytes = pub_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            result[kid_str] = pem_bytes.decode("ascii")
        except Exception as exc:
            raise ServerConfigError(
                f"Failed to load EC public key for kid '{_safe_label(kid_str)}'"
            ) from exc

    if not result:
        raise ServerConfigError("JWKS resulted in empty key set")
    return result


def check_file_not_group_world_readable(
    path: str, description: str = "Secret file"
) -> None:
    """Refuse files that are group- or world-readable (mode has any of 0o044)."""
    if not os.path.exists(path):
        return
    st = os.stat(path)
    if (st.st_mode & 0o044) != 0:
        raise ServerConfigError(f"{description} '{path}' is group- or world-readable")


def read_secret_file(path: str) -> str:
    """Read secret from file, verifying permissions and stripping one trailing newline."""
    if not os.path.exists(path):
        raise ServerConfigError(f"Secret file not found: '{path}'")
    st = os.stat(path)
    if (st.st_mode & 0o044) != 0:
        raise ServerConfigError(f"Secret file '{path}' is group- or world-readable")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    if content.endswith("\r\n"):
        resolved = content[:-2]
    elif content.endswith("\n"):
        resolved = content[:-1]
    else:
        resolved = content
    register_config_secrets(resolved)
    return resolved


def check_and_parse_secrets_env_file(path: str) -> dict[str, str]:
    """Parse secrets_env_file without shell expansion, requiring mode 0600 or stricter."""
    if not os.path.exists(path):
        raise ServerConfigError(f"secrets_env_file not found: '{path}'")
    st = os.stat(path)
    if (st.st_mode & 0o077) != 0:
        raise ServerConfigError(
            f"secrets_env_file '{path}' must be mode 0600 or stricter"
        )

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    result: dict[str, str] = {}
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if (v.startswith('"') and v.endswith('"')) or (
            v.startswith("'") and v.endswith("'")
        ):
            v = v[1:-1]
        result[k] = v
    register_config_secrets(*result.values())
    return result


def _validate_optional_storage_class(
    raw_val: Any,
    context: str,
    *,
    forbid_local: bool = False,
) -> str | None:
    if raw_val is None:
        return None
    if not isinstance(raw_val, str) or raw_val.strip() not in VALID_STORAGE_CLASSES:
        raise ServerConfigError(
            f"{context} 'storage_class' must be 'local' or 'object_storage', "
            f"got '{_safe_enum_token(raw_val)}'"
        )
    cleaned = raw_val.strip()
    if forbid_local and cleaned == "local":
        raise ServerConfigError(
            f"{context} cannot set storage_class='local' (reads object storage)"
        )
    return cleaned


def _validate_dsn_env_name(raw_val: Any, context: str) -> str:
    if (
        not isinstance(raw_val, str)
        or not raw_val.strip()
        or not _ENV_VAR_NAME_RE.fullmatch(raw_val.strip())
    ):
        raise ServerConfigError(
            f"{context} 'dsn_env' must be a valid environment variable name "
            f"matching ^[A-Za-z_][A-Za-z0-9_]{{0,127}}$"
        )
    return raw_val.strip()


def _validate_dsn_file_path(raw_val: Any, desc: str) -> str:
    if (
        not isinstance(raw_val, str)
        or not raw_val.strip()
        or "://" in raw_val
        or "@" in raw_val
        or any(ord(ch) < 32 for ch in raw_val)
    ):
        raise ServerConfigError(
            f"{desc} must be a non-empty filesystem path (never a literal DSN)"
        )
    path_str = raw_val.strip()
    check_file_not_group_world_readable(path_str, desc)
    return path_str


def _check_no_literal_secrets(mapping_obj: Mapping[str, Any], context: str) -> None:
    for raw_k in mapping_obj.keys():
        k_low = str(raw_k).strip().lower()
        if k_low in _FORBIDDEN_LITERAL_SECRET_KEYS:
            raise ServerConfigError(
                f"{context} forbids literal secret key '{k_low}'; "
                f"reference environment variables or secret files instead"
            )


def _parse_postgres_session_block(
    raw_session: Any, context: str
) -> PostgresSessionConfig:
    if not isinstance(raw_session, Mapping):
        raise ServerConfigError(f"{context} 'session' must be a mapping")
    _check_no_literal_secrets(raw_session, f"{context} 'session'")
    allowed_session = {
        "statement_timeout_ms",
        "lock_timeout_ms",
        "idle_in_transaction_session_timeout_ms",
        "search_path",
        "application_name",
        "startup_options",
        "connection_limit",
    }
    unknown_session = set(raw_session.keys()) - allowed_session
    if unknown_session:
        raise ServerConfigError(
            f"Unknown key(s) in {context} 'session': "
            f"{sorted(_safe_label(str(k)) for k in unknown_session)}"
        )

    def _pos_int(field_k: str, default_v: int) -> int:
        v = raw_session.get(field_k, default_v)
        if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
            raise ServerConfigError(
                f"{context} 'session.{field_k}' must be a positive integer"
            )
        return v

    stmt_ms = _pos_int("statement_timeout_ms", 30000)
    lock_ms = _pos_int("lock_timeout_ms", 2000)
    idle_ms = _pos_int("idle_in_transaction_session_timeout_ms", 10000)

    sp_raw = raw_session.get("search_path", "pg_catalog")
    if not isinstance(sp_raw, str) or not sp_raw.strip():
        raise ServerConfigError(
            f"{context} 'session.search_path' must be a non-empty string"
        )

    app_raw = raw_session.get("application_name", "seeknal-serving")
    if (
        not isinstance(app_raw, str)
        or not app_raw.strip()
        or any(ord(ch) < 32 for ch in app_raw)
        or "://" in app_raw
        or "@" in app_raw
    ):
        raise ServerConfigError(
            f"{context} 'session.application_name' must be a safe non-empty string"
        )

    startup_opts = raw_session.get("startup_options", True)
    if not isinstance(startup_opts, bool):
        raise ServerConfigError(
            f"{context} 'session.startup_options' must be a boolean"
        )

    conn_limit_raw = raw_session.get("connection_limit")
    conn_limit: int | None = None
    if conn_limit_raw is not None:
        if (
            isinstance(conn_limit_raw, bool)
            or not isinstance(conn_limit_raw, int)
            or conn_limit_raw <= 0
        ):
            raise ServerConfigError(
                f"{context} 'session.connection_limit' must be a positive integer or null"
            )
        conn_limit = conn_limit_raw

    try:
        return PostgresSessionConfig(
            statement_timeout_ms=stmt_ms,
            lock_timeout_ms=lock_ms,
            idle_in_transaction_session_timeout_ms=idle_ms,
            search_path=sp_raw.strip(),
            application_name=app_raw.strip(),
            startup_options=startup_opts,
            connection_limit=conn_limit,
        )
    except ValueError as exc:
        raise ServerConfigError(f"Invalid {context} 'session': {exc}") from exc


def _parse_postgres_tls_block(raw_tls: Any, context: str) -> PostgresTLSConfig:
    if not isinstance(raw_tls, Mapping):
        raise ServerConfigError(f"{context} 'tls' must be a mapping")
    _check_no_literal_secrets(raw_tls, f"{context} 'tls'")
    allowed_tls = {
        "sslmode",
        "sslrootcert_file",
        "sslcert_file",
        "sslkey_file",
        "allow_insecure_tls",
    }
    unknown_tls = set(raw_tls.keys()) - allowed_tls
    if unknown_tls:
        raise ServerConfigError(
            f"Unknown key(s) in {context} 'tls': "
            f"{sorted(_safe_label(str(k)) for k in unknown_tls)}"
        )

    sslmode_raw = raw_tls.get("sslmode", "verify-full")
    if not isinstance(sslmode_raw, str) or sslmode_raw.strip() not in VALID_SSLMODES:
        raise ServerConfigError(
            f"{context} 'tls.sslmode' must be one of {sorted(VALID_SSLMODES)}"
        )
    sslmode_clean = sslmode_raw.strip()

    allow_insecure_tls = raw_tls.get("allow_insecure_tls", False)
    if not isinstance(allow_insecure_tls, bool):
        raise ServerConfigError(
            f"{context} 'tls.allow_insecure_tls' must be a boolean"
        )

    file_vals: dict[str, str | None] = {}
    for fk in ("sslrootcert_file", "sslcert_file", "sslkey_file"):
        fv = raw_tls.get(fk)
        if fv is None:
            file_vals[fk] = None
        else:
            if (
                not isinstance(fv, str)
                or not fv.strip()
                or "://" in fv
                or any(ord(ch) < 32 for ch in fv)
            ):
                raise ServerConfigError(
                    f"{context} 'tls.{fk}' must be a non-empty file path string"
                )
            file_vals[fk] = fv.strip()
            if fk == "sslkey_file":
                check_file_not_group_world_readable(
                    file_vals[fk],  # type: ignore[arg-type]
                    f"{context} tls.sslkey_file",
                )

    return PostgresTLSConfig(
        sslmode=sslmode_clean,
        sslrootcert_file=file_vals["sslrootcert_file"],
        sslcert_file=file_vals["sslcert_file"],
        sslkey_file=file_vals["sslkey_file"],
        allow_insecure_tls=allow_insecure_tls,
    )


def _parse_postgres_freshness_block(
    raw_fr: Any, context: str, *, default_sync_table: str | None = None
) -> PostgresFreshnessConfig:
    if not isinstance(raw_fr, Mapping):
        raise ServerConfigError(f"{context} 'freshness' must be a mapping")
    _check_no_literal_secrets(raw_fr, f"{context} 'freshness'")
    unknown_fr = set(raw_fr.keys()) - {"sync_state_table", "freshness_sql"}
    if unknown_fr:
        raise ServerConfigError(
            f"Unknown key(s) in {context} 'freshness': "
            f"{sorted(_safe_label(str(k)) for k in unknown_fr)}"
        )
    sst_raw = raw_fr.get("sync_state_table", default_sync_table)
    sst_clean: str | None = None
    if sst_raw is not None:
        if not isinstance(sst_raw, str) or not _QUALIFIED_SYNC_TABLE_RE.fullmatch(
            sst_raw.strip()
        ):
            raise ServerConfigError(
                f"{context} 'freshness.sync_state_table' must be schema-qualified '<schema>.<table>'"
            )
        sst_clean = sst_raw.strip()

    fsql_raw = raw_fr.get("freshness_sql")
    fsql_clean: str | None = None
    if fsql_raw is not None:
        if not isinstance(fsql_raw, str) or not fsql_raw.strip():
            raise ServerConfigError(
                f"{context} 'freshness.freshness_sql' must be a non-empty SQL string or null"
            )
        fsql_clean = fsql_raw.strip()

    return PostgresFreshnessConfig(
        sync_state_table=sst_clean,
        freshness_sql=fsql_clean,
    )


def _invoke_parse_lakebase_block(raw_lb: Any, safe_eng: str) -> Any:
    if not isinstance(raw_lb, Mapping):
        raise ServerConfigError(
            f"Lakebase connection '{safe_eng}' field 'lakebase' must be a mapping"
        )
    _check_no_literal_secrets(raw_lb, f"Lakebase connection '{safe_eng}' 'lakebase'")
    if "storage_class" in raw_lb:
        raise ServerConfigError(
            f"Lakebase connection '{safe_eng}' cannot set 'storage_class' inside 'lakebase'"
        )
    cs_sub = raw_lb.get("cold_start")
    if isinstance(cs_sub, Mapping):
        _check_no_literal_secrets(
            cs_sub, f"Lakebase connection '{safe_eng}' 'lakebase.cold_start'"
        )

    try:
        sig = inspect.signature(parse_lakebase_block)
        params = sig.parameters
        has_varkw = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
        kw: dict[str, Any] = {}
        if "eng_name" in params or has_varkw:
            kw["eng_name"] = safe_eng
        if "error_cls" in params or has_varkw:
            kw["error_cls"] = ServerConfigError
        return parse_lakebase_block(raw_lb, **kw)
    except ServerConfigError:
        raise
    except Exception as exc:
        msg = str(exc)
        if _SUSPICIOUS_TOKEN_RE.search(msg):
            msg = "invalid configuration value"
        raise ServerConfigError(
            f"Invalid 'lakebase' block in connection '{safe_eng}': {msg}"
        ) from exc


def _parse_postgres_wire_connection(
    eng_name_str: str,
    eng_spec: Mapping[str, Any],
    *,
    kind: str,
) -> PostgresConnectionConfig | LakebaseConnectionConfig:
    safe_eng = _safe_label(eng_name_str)
    kind_title = "Postgres" if kind == "postgres" else "Lakebase"
    ctx = f"{kind_title} connection '{safe_eng}'"

    _check_no_literal_secrets(eng_spec, ctx)

    allowed_keys = {
        "kind",
        "mode",
        "dsn_env",
        "dsn_file",
        "accounts",
        "pool_max",
        "pool_min",
        "connect_timeout_seconds",
        "max_idle_seconds",
        "max_lifetime_seconds",
        "acquire_timeout_seconds",
        "session",
        "tls",
        "role_guard",
        "storage_class",
        "freshness",
    }
    if kind == "lakebase":
        allowed_keys.add("lakebase")

    unknown = set(eng_spec.keys()) - allowed_keys
    if unknown:
        raise ServerConfigError(
            f"Unknown key(s) in {kind} connection '{safe_eng}': "
            f"{sorted(_safe_label(str(k)) for k in unknown)}"
        )

    has_lb_block = kind == "lakebase" and "lakebase" in eng_spec
    lakebase_cfg: Any = None
    if has_lb_block:
        if "storage_class" in eng_spec:
            raise ServerConfigError(
                f"Lakebase connection '{safe_eng}' cannot set 'storage_class' (fixed to 'object_storage')"
            )
        lakebase_cfg = _invoke_parse_lakebase_block(eng_spec["lakebase"], safe_eng)

    # Credential validation: exactly one of dsn_env XOR dsn_file XOR accounts
    has_dsn_env = "dsn_env" in eng_spec
    has_dsn_file = "dsn_file" in eng_spec
    has_accounts = "accounts" in eng_spec
    cred_count = int(has_dsn_env) + int(has_dsn_file) + int(has_accounts)
    if cred_count > 1:
        raise ServerConfigError(
            f"{ctx} must specify exactly one of 'dsn_env', 'dsn_file', or 'accounts'"
        )
    if cred_count == 0:
        raise ServerConfigError(
            f"{ctx} missing required 'dsn_env' (specify exactly one of 'dsn_env', 'dsn_file', or 'accounts')"
        )

    dsn_env_clean = ""
    dsn_file_clean: str | None = None
    accounts_clean: dict[str, PostgresAccountConfig] = {}

    if has_dsn_env:
        dsn_env_clean = _validate_dsn_env_name(eng_spec["dsn_env"], ctx)
    elif has_dsn_file:
        dsn_file_clean = _validate_dsn_file_path(
            eng_spec["dsn_file"], f"{ctx} dsn_file"
        )
    else:
        accounts_raw = eng_spec["accounts"]
        if not isinstance(accounts_raw, Mapping) or not accounts_raw:
            raise ServerConfigError(
                f"{ctx} must have a non-empty 'accounts' mapping"
            )
        invalid_accs = set(accounts_raw.keys()) - VALID_WORKLOADS
        if invalid_accs:
            raise ServerConfigError(
                f"{ctx} contains invalid account(s): "
                f"{sorted(_safe_label(str(a)) for a in invalid_accs)}"
            )
        if "interactive" not in accounts_raw:
            raise ServerConfigError(
                f"{ctx} accounts must contain at least 'interactive'"
            )
        for acc_name, acc_spec in accounts_raw.items():
            if not isinstance(acc_spec, Mapping):
                raise ServerConfigError(
                    f"{ctx} account '{_safe_label(acc_name)}' must be a mapping"
                )
            acc_ctx = f"{ctx} account '{_safe_label(acc_name)}'"
            _check_no_literal_secrets(acc_spec, acc_ctx)
            unknown_acc = set(acc_spec.keys()) - {"dsn_env", "dsn_file"}
            if unknown_acc:
                raise ServerConfigError(
                    f"Unknown key(s) in {acc_ctx}: "
                    f"{sorted(_safe_label(str(k)) for k in unknown_acc)}"
                )
            acc_has_env = "dsn_env" in acc_spec
            acc_has_file = "dsn_file" in acc_spec
            if acc_has_env and acc_has_file:
                raise ServerConfigError(
                    f"{acc_ctx} cannot specify both 'dsn_env' and 'dsn_file'"
                )
            if not acc_has_env and not acc_has_file:
                raise ServerConfigError(
                    f"{acc_ctx} must specify either 'dsn_env' or 'dsn_file'"
                )
            if acc_has_env:
                acc_env_val = _validate_dsn_env_name(acc_spec["dsn_env"], acc_ctx)
                accounts_clean[acc_name] = PostgresAccountConfig(dsn_env=acc_env_val)
            else:
                acc_file_val = _validate_dsn_file_path(
                    acc_spec["dsn_file"], f"{acc_ctx} dsn_file"
                )
                accounts_clean[acc_name] = PostgresAccountConfig(
                    dsn_file=acc_file_val
                )

    # Pool bounds and timeouts
    pool_max = eng_spec.get("pool_max", 8)
    if (
        isinstance(pool_max, bool)
        or not isinstance(pool_max, int)
        or pool_max < 1
        or pool_max > 64
    ):
        raise ServerConfigError(
            f"{ctx} 'pool_max' must be an integer between 1 and 64"
        )

    pool_min = eng_spec.get("pool_min", 0)
    if (
        isinstance(pool_min, bool)
        or not isinstance(pool_min, int)
        or pool_min < 0
        or pool_min > pool_max
    ):
        raise ServerConfigError(
            f"{ctx} 'pool_min' must be an integer between 0 and pool_max ({pool_max})"
        )

    def _pos_timeout(field_k: str, default_v: float) -> float:
        raw_v = eng_spec.get(field_k, default_v)
        if isinstance(raw_v, (bool, str)) or not isinstance(raw_v, (int, float)):
            raise ServerConfigError(
                f"{ctx} '{field_k}' must be a positive number"
            )
        fv = float(raw_v)
        if fv <= 0 or math.isnan(fv) or math.isinf(fv):
            raise ServerConfigError(
                f"{ctx} '{field_k}' must be a positive number"
            )
        return fv

    ct_f = _pos_timeout("connect_timeout_seconds", 10.0)
    mi_f = _pos_timeout("max_idle_seconds", 300.0)
    ml_f = _pos_timeout("max_lifetime_seconds", 1800.0)
    at_f = _pos_timeout("acquire_timeout_seconds", 5.0)

    # role_guard
    role_guard = eng_spec.get("role_guard", True)
    if not isinstance(role_guard, bool):
        raise ServerConfigError(f"{ctx} 'role_guard' must be a boolean")
    if kind == "lakebase" and role_guard is False:
        raise ServerConfigError(
            f"Lakebase connection '{safe_eng}' cannot disable 'role_guard'"
        )

    # session
    session_cfg: PostgresSessionConfig | None = None
    if "session" in eng_spec and eng_spec["session"] is not None:
        session_cfg = _parse_postgres_session_block(eng_spec["session"], ctx)

    # tls
    tls_cfg: PostgresTLSConfig | None = None
    if "tls" in eng_spec and eng_spec["tls"] is not None:
        tls_cfg = _parse_postgres_tls_block(eng_spec["tls"], ctx)

    if kind == "lakebase" and lakebase_cfg is not None:
        if tls_cfg is not None and tls_cfg.allow_insecure_tls:
            raise ServerConfigError(
                f"Lakebase connection '{safe_eng}' forbids 'tls.allow_insecure_tls'; "
                f"sslmode='verify-full' is mandatory"
            )
        if (
            tls_cfg is None
            or tls_cfg.sslmode != "verify-full"
            or not tls_cfg.sslrootcert_file
        ):
            raise ServerConfigError(
                f"Lakebase connection '{safe_eng}' requires 'tls' with "
                f"sslmode='verify-full' and a non-empty 'sslrootcert_file'"
            )
    elif tls_cfg is not None and tls_cfg.sslmode != "verify-full":
        if not tls_cfg.allow_insecure_tls:
            raise ServerConfigError(
                f"{ctx} 'tls.sslmode' ('{tls_cfg.sslmode}') is weaker than "
                f"'verify-full'; use 'verify-full' or explicitly set "
                f"'tls.allow_insecure_tls: true'"
            )

    # mode
    if kind == "postgres":
        mode_raw = eng_spec.get("mode", "read_only")
        if not isinstance(mode_raw, str) or mode_raw.strip() not in VALID_POSTGRES_MODES:
            raise ServerConfigError(
                f"{ctx} 'mode' must be 'read_only' or 'read_write', "
                f"got '{_safe_enum_token(mode_raw)}'"
            )
        mode_clean = mode_raw.strip()
    else:
        if lakebase_cfg is not None:
            ep_mode = getattr(lakebase_cfg, "endpoint_type", "read_write")
            if "mode" in eng_spec and eng_spec["mode"] is not None:
                m_raw = eng_spec["mode"]
                if (
                    not isinstance(m_raw, str)
                    or m_raw.strip() not in VALID_POSTGRES_MODES
                ):
                    raise ServerConfigError(
                        f"{ctx} 'mode' must be 'read_only' or 'read_write'"
                    )
                if m_raw.strip() != ep_mode:
                    raise ServerConfigError(
                        f"{ctx} 'mode' ('{m_raw.strip()}') conflicts with "
                        f"'lakebase.endpoint_type' ('{ep_mode}')"
                    )
            mode_clean = ep_mode
        else:
            mode_raw = eng_spec.get("mode", "read_write")
            if (
                not isinstance(mode_raw, str)
                or mode_raw.strip() not in VALID_POSTGRES_MODES
            ):
                raise ServerConfigError(
                    f"{ctx} 'mode' must be 'read_only' or 'read_write', "
                    f"got '{_safe_enum_token(mode_raw)}'"
                )
            mode_clean = mode_raw.strip()

    # storage_class
    storage_class_clean: str | None = None
    if lakebase_cfg is None and "storage_class" in eng_spec:
        storage_class_clean = _validate_optional_storage_class(
            eng_spec.get("storage_class"), ctx
        )

    # freshness
    freshness_cfg: Any = None
    if "freshness" in eng_spec and eng_spec["freshness"] is not None:
        default_st = (
            "public._lakebase_sync_state" if kind == "lakebase" else None
        )
        freshness_cfg = _parse_postgres_freshness_block(
            eng_spec["freshness"], ctx, default_sync_table=default_st
        )
    elif lakebase_cfg is not None:
        freshness_cfg = getattr(lakebase_cfg, "freshness", None)

    # Emit warnings after validation succeeds
    if tls_cfg is not None and tls_cfg.sslmode != "verify-full" and tls_cfg.allow_insecure_tls:
        logger.warning(
            "connection '%s' uses insecure TLS sslmode='%s' via tls.allow_insecure_tls: true",
            safe_eng,
            tls_cfg.sslmode,
        )
    if kind == "postgres" and role_guard is False:
        logger.warning(
            "connection '%s' has role_guard: false; superuser and BYPASSRLS checks are disabled",
            safe_eng,
        )
    if kind == "lakebase" and lakebase_cfg is None:
        logger.warning(
            "connection '%s' uses kind: lakebase without a lakebase: endpoint block; "
            "if this is a plain PostgreSQL database relabel it kind: postgres "
            "(mode: read_write keeps today's write behaviour), otherwise add the lakebase: block",
            safe_eng,
        )
    elif kind == "lakebase" and mode_clean == "read_only":
        logger.warning(
            "connection '%s': read_only Lakebase endpoints serve a frozen snapshot",
            safe_eng,
        )

    if kind == "postgres":
        return PostgresConnectionConfig(
            kind="postgres",
            mode=mode_clean,
            dsn_env=dsn_env_clean,
            dsn_file=dsn_file_clean,
            accounts=accounts_clean,
            pool_max=pool_max,
            pool_min=pool_min,
            connect_timeout_seconds=ct_f,
            max_idle_seconds=mi_f,
            max_lifetime_seconds=ml_f,
            acquire_timeout_seconds=at_f,
            session=session_cfg,
            tls=tls_cfg,
            role_guard=role_guard,
            storage_class=storage_class_clean,
            freshness=freshness_cfg,
        )

    return LakebaseConnectionConfig(
        kind="lakebase",
        dsn_env=dsn_env_clean,
        pool_max=pool_max,
        pool_min=pool_min,
        connect_timeout_seconds=ct_f,
        max_idle_seconds=mi_f,
        max_lifetime_seconds=ml_f,
        acquire_timeout_seconds=at_f,
        dsn_file=dsn_file_clean,
        accounts=accounts_clean,
        mode=mode_clean,
        session=session_cfg,
        tls=tls_cfg,
        role_guard=role_guard,
        storage_class=storage_class_clean,
        lakebase=lakebase_cfg,
        freshness=freshness_cfg,
    )


@dataclass(frozen=True)
class ListenConfig:
    host: str = "0.0.0.0"
    port: int = 8765


@dataclass(frozen=True)
class TLSConfig:
    cert_file: str
    key_file: str
    client_ca_file: str | None = None
    require_client_cert: bool = True


@dataclass(frozen=True)
class GrantKeysConfig:
    jwks_file: str | None = None
    pem_files: dict[str, str] | None = None


def load_grant_keys(grant_keys: GrantKeysConfig | Mapping) -> dict[str, str]:
    """Load grant keys from JWKS file or PEM files mapping."""
    if isinstance(grant_keys, GrantKeysConfig):
        cfg = grant_keys
    elif isinstance(grant_keys, Mapping):
        jwks = grant_keys.get("jwks_file")
        pems = grant_keys.get("pem_files")
        cfg = GrantKeysConfig(jwks_file=jwks, pem_files=pems)
    else:
        raise ServerConfigError(
            f"Invalid grant_keys configuration: expected mapping, got {type(grant_keys).__name__}"
        )

    if cfg.jwks_file:
        if not os.path.exists(cfg.jwks_file):
            raise ServerConfigError(f"JWKS file '{cfg.jwks_file}' not found")
        with open(cfg.jwks_file, "r", encoding="utf-8") as f:
            return parse_jwks(f.read())
    elif cfg.pem_files:
        result: dict[str, str] = {}
        for kid, path in cfg.pem_files.items():
            if not os.path.exists(path):
                raise ServerConfigError(
                    f"PEM file for kid '{_safe_label(kid)}' not found: '{path}'"
                )
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            try:
                _load_public_key(content, kid)
            except Exception as exc:
                raise ServerConfigError(
                    f"Invalid PEM public key for kid '{_safe_label(kid)}' in '{path}'"
                ) from exc
            result[kid] = content
        if not result:
            raise ServerConfigError("pem_files must not be empty")
        return result
    else:
        raise ServerConfigError("grant_keys has neither jwks_file nor pem_files")


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    format: str = "json"
    file: str | None = None


@dataclass(frozen=True)
class StaticPlacementConfig:
    placement_file: str
    schemas_file: str


@dataclass(frozen=True)
class PlacementConfig:
    discovery: dict | None = None
    static: StaticPlacementConfig | None = None
    discovery_config: DiscoveryConfig | None = None

    @property
    def is_static(self) -> bool:
        return self.static is not None


@dataclass(frozen=True)
class StarRocksAccountConfig:
    url_env: str | None = None
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password_env: str | None = None
    password_file: str | None = None
    database: str | None = None


@dataclass(frozen=True)
class StarRocksConnectionConfig:
    kind: str = "starrocks"
    accounts: dict[str, StarRocksAccountConfig] = field(default_factory=dict)
    storage_class: str | None = None


@dataclass(frozen=True)
class PostgresConnectionConfig:
    kind: str = "postgres"
    mode: str = "read_only"
    dsn_env: str = ""
    dsn_file: str | None = None
    accounts: dict[str, PostgresAccountConfig] = field(default_factory=dict)
    pool_max: int = 8
    pool_min: int = 0
    connect_timeout_seconds: float = 10.0
    max_idle_seconds: float = 300.0
    max_lifetime_seconds: float = 1800.0
    acquire_timeout_seconds: float = 5.0
    session: PostgresSessionConfig | None = None
    tls: PostgresTLSConfig | None = None
    role_guard: bool = True
    storage_class: str | None = None
    freshness: PostgresFreshnessConfig | None = None

    @property
    def read_only(self) -> bool:
        return self.mode == "read_only"


@dataclass(frozen=True)
class LakebaseConnectionConfig:
    kind: str = "lakebase"
    dsn_env: str = ""
    pool_max: int = 8
    pool_min: int = 0
    connect_timeout_seconds: float = 10.0
    max_idle_seconds: float = 300.0
    max_lifetime_seconds: float = 1800.0
    acquire_timeout_seconds: float = 5.0
    dsn_file: str | None = None
    accounts: dict[str, PostgresAccountConfig] = field(default_factory=dict)
    mode: str = "read_write"
    session: PostgresSessionConfig | None = None
    tls: PostgresTLSConfig | None = None
    role_guard: bool = True
    storage_class: str | None = None
    lakebase: Any = None
    freshness: Any = None

    @property
    def is_legacy(self) -> bool:
        return self.lakebase is None

    @property
    def read_only(self) -> bool:
        return self.mode == "read_only"


@dataclass(frozen=True)
class CubeConnectionConfig:
    kind: str = "cube"
    host: str = ""
    port: int = 15432
    dbname: str = ""
    sql_user: str = ""
    secret_env: str | None = None
    connect_timeout_seconds: float = 10.0
    storage_class: str | None = None


@dataclass(frozen=True)
class IcebergS3Config:
    """S3 credentials and endpoint configuration for an Iceberg catalog."""

    endpoint: str
    region: str
    key_id_env: str
    secret_env: str
    url_style: str = "path"
    use_ssl: bool = True


@dataclass(frozen=True)
class IcebergCatalogConfig:
    """Lakekeeper REST catalog configuration for DuckDB iceberg_direct profile."""

    alias: str
    endpoint: str
    warehouse: str
    token_uri: str
    client_id_env: str
    client_secret_env: str
    access_delegation: str
    s3: IcebergS3Config
    scope: str | None = None
    token_lifetime_seconds: float | None = None


@dataclass(frozen=True)
class DuckDBConnectionConfig:
    kind: str = "duckdb"
    profile: str = "local_file"
    path: str = ""
    allowed_directories: tuple[str, ...] = ()
    memory_limit: str | None = None
    threads: int | None = None
    temp_directory: str | None = None
    max_temp_directory_size: str | None = None
    max_concurrent_cursors: int = 32
    extension_directory: str | None = None
    extensions: tuple[str, ...] = ()
    iceberg_catalogs: Mapping[str, IcebergCatalogConfig] = field(default_factory=dict)
    allowed_s3_prefixes: tuple[str, ...] = ()
    recycle_seconds: float | None = None
    token_lifetime_seconds: float | None = None
    external_access_after_attach: str = "off"
    storage_class: str | None = None
    secrets_env: Mapping[str, str] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class LimitsConfig:
    timeout_seconds: float = 30.0
    max_body_bytes: int = 1_000_000
    max_result_bytes: int = 67_108_864


@dataclass(frozen=True)
class ServerConfig:
    """Parsed and validated Seeknal Serving configuration."""

    listen: ListenConfig
    grant_keys: GrantKeysConfig
    placement: PlacementConfig
    connections: dict[str, Any]
    tls: TLSConfig | None = None
    secrets_env_file: str | None = None
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    require_grant_typ: bool = True
    default_catalog: str | None = None
    cube_mode: str = "refuse"
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    secrets_env: dict[str, str] = field(default_factory=dict, repr=False)
    grant_max_lifetime_seconds: float = 60.0
    replay_cache_max_entries: int = 100_000
    compilation_cache_max_entries: int = DEFAULT_COMPILATION_CACHE_MAX_ENTRIES
    audit: AuditConfig | None = None
    ops: OpsConfig | None = None
    cursor_key_env: str | None = None

    def __post_init__(self) -> None:
        validate_compilation_cache_max_entries(
            self.compilation_cache_max_entries,
            error_cls=ServerConfigError,
        )
        if self.audit is not None and not isinstance(self.audit, AuditConfig):
            raise ServerConfigError("'audit' must be an AuditConfig instance or None")
        if self.ops is not None and not isinstance(self.ops, OpsConfig):
            raise ServerConfigError("'ops' must be an OpsConfig instance or None")
        if self.cursor_key_env is not None and (
            not isinstance(self.cursor_key_env, str)
            or not _ENV_VAR_NAME_RE.fullmatch(self.cursor_key_env)
        ):
            raise ServerConfigError(
                "'cursor_key_env' must be None or a valid environment variable name"
            )
        if self.secrets_env:
            register_config_secrets(*self.secrets_env.values())
        for conn in self.connections.values():
            if isinstance(conn, DuckDBConnectionConfig) and conn.secrets_env:
                register_config_secrets(*conn.secrets_env.values())

    def _register_resolved_secrets(
        self, env: Mapping[str, str] | None = None
    ) -> None:
        """Register any currently available referenced env and secret-file values for log scrubbing."""
        proc_env = env if env is not None else os.environ
        if self.secrets_env:
            register_config_secrets(*self.secrets_env.values())
        for var_name in self.get_referenced_env_vars():
            if var_name in self.secrets_env:
                register_config_secrets(self.secrets_env[var_name])
            elif var_name in proc_env:
                register_config_secrets(proc_env[var_name])

        candidate_files: list[str] = []
        for conn in self.connections.values():
            if isinstance(conn, StarRocksConnectionConfig):
                for acc in conn.accounts.values():
                    if acc.password_file:
                        candidate_files.append(acc.password_file)
            elif isinstance(conn, (PostgresConnectionConfig, LakebaseConnectionConfig)):
                if conn.dsn_file:
                    candidate_files.append(conn.dsn_file)
                for acc in conn.accounts.values():
                    if acc.dsn_file:
                        candidate_files.append(acc.dsn_file)
        if self.placement.discovery_config is not None:
            disc = self.placement.discovery_config
            if (
                disc.lakekeeper
                and disc.lakekeeper.oauth
                and disc.lakekeeper.oauth.client_secret_file
            ):
                candidate_files.append(disc.lakekeeper.oauth.client_secret_file)
            for eng in disc.engines.values():
                if eng.api_secret_file:
                    candidate_files.append(eng.api_secret_file)

        for sf_path in candidate_files:
            if os.path.isfile(sf_path):
                try:
                    st = os.stat(sf_path)
                    if (st.st_mode & 0o044) == 0:
                        read_secret_file(sf_path)
                except OSError:
                    pass

    def load_grant_keys(self) -> dict[str, str]:
        """Load and return parsed PEM public keys."""
        return load_grant_keys(self.grant_keys)

    def get_env(self, var_name: str, env: Mapping[str, str] | None = None) -> str:
        """Resolve an environment variable, consulting secrets_env before the process env."""
        if var_name in self.secrets_env:
            val = self.secrets_env[var_name]
            register_config_secrets(val)
            return val
        process_env = env if env is not None else os.environ
        if var_name in process_env:
            val = process_env[var_name]
            register_config_secrets(val)
            return val
        raise ServerConfigError(f"Missing required environment variable '{var_name}'")

    def get_referenced_env_vars(self) -> list[str]:
        """Collect names of all environment variables referenced in this configuration."""
        referenced: list[str] = []

        # Connections
        for conn in self.connections.values():
            if isinstance(conn, StarRocksConnectionConfig):
                for acc in conn.accounts.values():
                    if acc.url_env:
                        referenced.append(acc.url_env)
                    if acc.password_env:
                        referenced.append(acc.password_env)
            elif isinstance(conn, PostgresConnectionConfig):
                if conn.dsn_env:
                    referenced.append(conn.dsn_env)
                for acc in conn.accounts.values():
                    if acc.dsn_env:
                        referenced.append(acc.dsn_env)
            elif isinstance(conn, LakebaseConnectionConfig):
                if conn.dsn_env:
                    referenced.append(conn.dsn_env)
                for acc in conn.accounts.values():
                    if acc.dsn_env:
                        referenced.append(acc.dsn_env)
                if conn.lakebase is not None:
                    cs = getattr(conn.lakebase, "cold_start", None)
                    api_tok_env = (
                        getattr(cs, "api_token_env", None) if cs is not None else None
                    )
                    if api_tok_env:
                        referenced.append(api_tok_env)
            elif isinstance(conn, CubeConnectionConfig):
                if conn.secret_env:
                    referenced.append(conn.secret_env)
            elif isinstance(conn, DuckDBConnectionConfig):
                if conn.profile == "iceberg_direct":
                    for cat in conn.iceberg_catalogs.values():
                        if cat.client_id_env:
                            referenced.append(cat.client_id_env)
                        if cat.client_secret_env:
                            referenced.append(cat.client_secret_env)
                        if cat.s3.key_id_env:
                            referenced.append(cat.s3.key_id_env)
                        if cat.s3.secret_env:
                            referenced.append(cat.s3.secret_env)

        # Discovery config
        if self.placement.discovery_config is not None:
            disc = self.placement.discovery_config
            if disc.lakekeeper:
                if disc.lakekeeper.token_env:
                    referenced.append(disc.lakekeeper.token_env)
                if disc.lakekeeper.oauth and disc.lakekeeper.oauth.client_secret_env:
                    referenced.append(disc.lakekeeper.oauth.client_secret_env)
            for eng in disc.engines.values():
                if eng.token_env:
                    referenced.append(eng.token_env)
                if eng.api_secret_env:
                    referenced.append(eng.api_secret_env)

        # Ops config
        if self.ops is not None and self.ops.auth_token_env:
            referenced.append(self.ops.auth_token_env)

        # Cursor signing key env
        if self.cursor_key_env:
            referenced.append(self.cursor_key_env)

        return referenced

    def check_secret_files(self) -> None:
        """Verify that all referenced secret files exist and have safe permissions."""
        secret_files: list[tuple[str, str]] = []

        # Connections
        for eng_name, conn in self.connections.items():
            safe_eng = _safe_label(eng_name)
            if isinstance(conn, StarRocksConnectionConfig):
                for acc_name, acc in conn.accounts.items():
                    if acc.password_file:
                        secret_files.append(
                            (
                                acc.password_file,
                                f"StarRocks account '{_safe_label(acc_name)}' password_file",
                            )
                        )
            elif isinstance(conn, PostgresConnectionConfig):
                if conn.dsn_file:
                    secret_files.append(
                        (
                            conn.dsn_file,
                            f"Postgres connection '{safe_eng}' dsn_file",
                        )
                    )
                for acc_name, acc in conn.accounts.items():
                    if acc.dsn_file:
                        secret_files.append(
                            (
                                acc.dsn_file,
                                f"Postgres connection '{safe_eng}' account '{_safe_label(acc_name)}' dsn_file",
                            )
                        )
            elif isinstance(conn, LakebaseConnectionConfig):
                if conn.dsn_file:
                    secret_files.append(
                        (
                            conn.dsn_file,
                            f"Lakebase connection '{safe_eng}' dsn_file",
                        )
                    )
                for acc_name, acc in conn.accounts.items():
                    if acc.dsn_file:
                        secret_files.append(
                            (
                                acc.dsn_file,
                                f"Lakebase connection '{safe_eng}' account '{_safe_label(acc_name)}' dsn_file",
                            )
                        )

        # Discovery config
        if self.placement.discovery_config is not None:
            disc = self.placement.discovery_config
            if (
                disc.lakekeeper
                and disc.lakekeeper.oauth
                and disc.lakekeeper.oauth.client_secret_file
            ):
                secret_files.append(
                    (
                        disc.lakekeeper.oauth.client_secret_file,
                        "Lakekeeper oauth client_secret_file",
                    )
                )
            for eng in disc.engines.values():
                if eng.api_secret_file:
                    secret_files.append(
                        (
                            eng.api_secret_file,
                            f"Cube engine '{eng.name}' api_secret_file",
                        )
                    )

        for path, desc in secret_files:
            if not os.path.exists(path):
                raise ServerConfigError(f"{desc} not found: '{path}'")
            st = os.stat(path)
            if (st.st_mode & 0o044) != 0:
                raise ServerConfigError(
                    f"{desc} '{path}' is group- or world-readable"
                )
            read_secret_file(path)

    def check_env(
        self,
        env: Mapping[str, str] | None = None,
        *,
        cursor_key: bytes | str | None = None,
    ) -> None:
        """Verify that every referenced environment variable exists and secret files are safe."""
        for var in self.get_referenced_env_vars():
            if self.cursor_key_env is not None and var == self.cursor_key_env:
                from seeknal.serving.startup_validation import (
                    resolve_startup_cursor_key,
                )

                resolve_startup_cursor_key(
                    self,
                    cursor_key=cursor_key,
                    env=env,
                )
            else:
                self.get_env(var, env)
        self.check_secret_files()

    def validate_runtime_wiring(
        self,
        *,
        env: Mapping[str, str] | None = None,
        check_cursor_key: bool = False,
        cursor_key: bytes | str | None = None,
    ) -> PreflightResult:
        """Offline preflight validation of env, static placement/schemas, and engine-connection wiring.

        Returns a PreflightResult so callers (such as build_server) can reuse parsed
        static Placement and schemas without re-reading files.
        """
        self.check_env(env=env, cursor_key=cursor_key)
        if self.audit is not None:
            verify_audit_config_offline(self.audit, error_cls=AuditStartupError)

        from seeknal.serving.startup_validation import validate_startup

        report = validate_startup(
            self,
            env=env,
            check_grant_keys=False,
            check_cursor_key=check_cursor_key,
            cursor_key=cursor_key,
        )
        return PreflightResult(
            mode=report.mode,
            placement=report.placement,
            schemas=report.schemas,
        )

    @classmethod
    def from_dict(
        cls,
        d: Mapping,
        *,
        load_keys: bool = True,
        env: Mapping[str, str] | None = None,
    ) -> ServerConfig:
        """Construct and validate ServerConfig from a dictionary."""
        if not isinstance(d, Mapping):
            raise ServerConfigError(
                f"Configuration must be a mapping, got {type(d).__name__}"
            )

        # 1. Unknown top-level keys
        allowed_top = {
            "listen",
            "tls",
            "grant_keys",
            "secrets_env_file",
            "logging",
            "require_grant_typ",
            "default_catalog",
            "cube_mode",
            "placement",
            "connections",
            "limits",
            "grant_max_lifetime_seconds",
            "replay_cache_max_entries",
            "compilation_cache_max_entries",
            "audit",
            "ops",
            "cursor_key_env",
        }
        unknown_top = set(d.keys()) - allowed_top
        if unknown_top:
            raise ServerConfigError(
                f"Unknown top-level configuration key(s): {sorted(_safe_label(str(k)) for k in unknown_top)}"
            )

        # 2. listen
        listen_raw = d.get("listen")
        if listen_raw is None:
            listen_cfg = ListenConfig()
        else:
            if not isinstance(listen_raw, Mapping):
                raise ServerConfigError("'listen' must be a mapping")
            unknown_listen = set(listen_raw.keys()) - {"host", "port"}
            if unknown_listen:
                raise ServerConfigError(
                    f"Unknown key(s) in 'listen': {sorted(unknown_listen)}"
                )
            host = listen_raw.get("host", "0.0.0.0")
            if not isinstance(host, str) or not host.strip():
                raise ServerConfigError("'listen.host' must be a non-empty string")
            port = listen_raw.get("port", 8765)
            if (
                isinstance(port, bool)
                or not isinstance(port, int)
                or port < 1
                or port > 65535
            ):
                port_desc = port if isinstance(port, int) and not isinstance(port, bool) else type(port).__name__
                raise ServerConfigError(
                    f"'listen.port' must be an integer between 1 and 65535, got {port_desc}"
                )
            listen_cfg = ListenConfig(host=host.strip(), port=port)

        # 3. tls
        tls_raw = d.get("tls")
        if tls_raw is None:
            tls_cfg = None
        else:
            if not isinstance(tls_raw, Mapping):
                raise ServerConfigError("'tls' must be a mapping")
            unknown_tls = set(tls_raw.keys()) - {
                "cert_file",
                "key_file",
                "client_ca_file",
                "require_client_cert",
            }
            if unknown_tls:
                raise ServerConfigError(
                    f"Unknown key(s) in 'tls': {sorted(unknown_tls)}"
                )
            cert_file = tls_raw.get("cert_file")
            if not cert_file or not isinstance(cert_file, str) or not cert_file.strip():
                raise ServerConfigError("'tls.cert_file' must be a non-empty string")
            key_file = tls_raw.get("key_file")
            if not key_file or not isinstance(key_file, str) or not key_file.strip():
                raise ServerConfigError("'tls.key_file' must be a non-empty string")
            client_ca_file = tls_raw.get("client_ca_file")
            if client_ca_file is not None and (
                not isinstance(client_ca_file, str) or not client_ca_file.strip()
            ):
                raise ServerConfigError(
                    "'tls.client_ca_file' must be a non-empty string"
                )
            client_ca_str = client_ca_file.strip() if client_ca_file else None

            default_req = True if client_ca_str else False
            require_client_cert = tls_raw.get("require_client_cert", default_req)
            if not isinstance(require_client_cert, bool):
                raise ServerConfigError("'tls.require_client_cert' must be a boolean")

            tls_cfg = TLSConfig(
                cert_file=cert_file.strip(),
                key_file=key_file.strip(),
                client_ca_file=client_ca_str,
                require_client_cert=require_client_cert,
            )

        # 4. grant_keys
        if "grant_keys" not in d:
            raise ServerConfigError("Missing required section 'grant_keys'")
        gk_raw = d["grant_keys"]
        if not isinstance(gk_raw, Mapping):
            raise ServerConfigError("'grant_keys' must be a mapping")
        unknown_gk = set(gk_raw.keys()) - {"jwks_file", "pem_files"}
        if unknown_gk:
            raise ServerConfigError(
                f"Unknown key(s) in 'grant_keys': {sorted(unknown_gk)}"
            )
        has_jwks = "jwks_file" in gk_raw
        has_pem = "pem_files" in gk_raw
        if has_jwks and has_pem:
            raise ServerConfigError(
                "'grant_keys' must specify exactly one of 'jwks_file' or 'pem_files', got both"
            )
        if not has_jwks and not has_pem:
            raise ServerConfigError(
                "'grant_keys' must specify exactly one of 'jwks_file' or 'pem_files', got neither"
            )

        jwks_file = None
        pem_files = None
        if has_jwks:
            jwks_val = gk_raw["jwks_file"]
            if not isinstance(jwks_val, str) or not jwks_val.strip():
                raise ServerConfigError(
                    "'grant_keys.jwks_file' must be a non-empty string"
                )
            jwks_file = jwks_val.strip()
        else:
            pem_val = gk_raw["pem_files"]
            if not isinstance(pem_val, Mapping) or not pem_val:
                raise ServerConfigError(
                    "'grant_keys.pem_files' must be a non-empty mapping of kid to file path"
                )
            pem_files = {}
            for kid, p in pem_val.items():
                if not isinstance(kid, str) or not kid.strip():
                    raise ServerConfigError(
                        "kid in 'grant_keys.pem_files' must be a non-empty string"
                    )
                if not isinstance(p, str) or not p.strip():
                    raise ServerConfigError(
                        f"Path for kid '{kid}' in 'grant_keys.pem_files' must be a non-empty string"
                    )
                pem_files[kid.strip()] = p.strip()

        grant_keys_cfg = GrantKeysConfig(jwks_file=jwks_file, pem_files=pem_files)

        # 5. secrets_env_file
        secrets_env_file = d.get("secrets_env_file")
        secrets_env: dict[str, str] = {}
        if secrets_env_file is not None:
            if not isinstance(secrets_env_file, str) or not secrets_env_file.strip():
                raise ServerConfigError("'secrets_env_file' must be a non-empty string")
            secrets_env_file = secrets_env_file.strip()
            secrets_env = check_and_parse_secrets_env_file(secrets_env_file)

        # 6. logging
        log_raw = d.get("logging")
        if log_raw is None:
            logging_cfg = LoggingConfig()
        else:
            if not isinstance(log_raw, Mapping):
                raise ServerConfigError("'logging' must be a mapping")
            unknown_log = set(log_raw.keys()) - {"level", "format", "file"}
            if unknown_log:
                raise ServerConfigError(
                    f"Unknown key(s) in 'logging': {sorted(unknown_log)}"
                )
            level = str(log_raw.get("level", "INFO")).upper()
            if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
                raise ServerConfigError(f"Invalid log level '{_safe_label(level)}'")
            fmt = str(log_raw.get("format", "json")).lower()
            if fmt not in {"json", "text"}:
                raise ServerConfigError(
                    f"Invalid log format '{_safe_label(fmt)}', must be 'json' or 'text'"
                )
            f_val = log_raw.get("file")
            if f_val is not None and not isinstance(f_val, str):
                raise ServerConfigError("'logging.file' must be a string or null")
            logging_cfg = LoggingConfig(level=level, format=fmt, file=f_val)

        # 7. require_grant_typ
        require_grant_typ = d.get("require_grant_typ", True)
        if not isinstance(require_grant_typ, bool):
            raise ServerConfigError("'require_grant_typ' must be a boolean")

        # 8. default_catalog
        default_catalog = d.get("default_catalog", "lakekeeper")
        if default_catalog is not None and not isinstance(default_catalog, str):
            raise ServerConfigError("'default_catalog' must be a string")

        # 9. cube_mode
        cube_mode = d.get("cube_mode", "refuse")
        if cube_mode not in ("refuse", "jwt"):
            raise ServerConfigError(
                f"Invalid cube_mode '{_safe_label(cube_mode)}', must be 'refuse' or 'jwt'"
            )

        # 10. placement
        if "placement" not in d:
            raise ServerConfigError("Missing required section 'placement'")
        placement_raw = d["placement"]
        if not isinstance(placement_raw, Mapping):
            raise ServerConfigError("'placement' must be a mapping")
        unknown_pl = set(placement_raw.keys()) - {"discovery", "static"}
        if unknown_pl:
            raise ServerConfigError(
                f"Unknown key(s) in 'placement': {sorted(unknown_pl)}"
            )
        has_disc = "discovery" in placement_raw
        has_stat = "static" in placement_raw
        if has_disc and has_stat:
            raise ServerConfigError(
                "'placement' must specify exactly one of 'discovery' or 'static', got both"
            )
        if not has_disc and not has_stat:
            raise ServerConfigError(
                "'placement' must specify exactly one of 'discovery' or 'static', got neither"
            )

        disc_cfg = None
        stat_cfg = None
        parsed_disc_cfg = None

        if has_disc:
            disc_data = placement_raw["discovery"]
            if not isinstance(disc_data, Mapping):
                raise ServerConfigError("'placement.discovery' must be a mapping")
            try:
                parsed_disc_cfg = DiscoveryConfig.from_dict(disc_data)
            except Exception as exc:
                raise ServerConfigError(
                    f"Invalid 'placement.discovery' config: {exc}"
                ) from exc
            disc_cfg = dict(disc_data)
        else:
            stat_data = placement_raw["static"]
            if not isinstance(stat_data, Mapping):
                raise ServerConfigError("'placement.static' must be a mapping")
            unknown_stat = set(stat_data.keys()) - {"placement_file", "schemas_file"}
            if unknown_stat:
                raise ServerConfigError(
                    f"Unknown key(s) in 'placement.static': {sorted(unknown_stat)}"
                )
            pf = stat_data.get("placement_file")
            sf = stat_data.get("schemas_file")
            if not pf or not isinstance(pf, str) or not pf.strip():
                raise ServerConfigError(
                    "'placement.static.placement_file' must be a non-empty string"
                )
            if not sf or not isinstance(sf, str) or not sf.strip():
                raise ServerConfigError(
                    "'placement.static.schemas_file' must be a non-empty string"
                )
            stat_cfg = StaticPlacementConfig(
                placement_file=pf.strip(), schemas_file=sf.strip()
            )

        placement_cfg = PlacementConfig(
            discovery=disc_cfg,
            static=stat_cfg,
            discovery_config=parsed_disc_cfg,
        )

        # 11. connections
        if "connections" not in d:
            raise ServerConfigError("Missing required section 'connections'")
        conn_raw = d["connections"]
        if not isinstance(conn_raw, Mapping) or not conn_raw:
            raise ServerConfigError("'connections' must be a non-empty mapping")

        connections: dict[str, Any] = {}
        for eng_name, eng_spec in conn_raw.items():
            if not isinstance(eng_name, str) or not eng_name.strip():
                raise ServerConfigError(
                    "Connection engine name must be a non-empty string"
                )
            eng_name_str = eng_name.strip()
            if not isinstance(eng_spec, Mapping):
                raise ServerConfigError(
                    f"Connection spec for '{eng_name_str}' must be a mapping"
                )

            kind = eng_spec.get("kind")
            if not kind or not isinstance(kind, str) or not kind.strip():
                raise ServerConfigError(
                    f"Connection '{_safe_label(eng_name_str)}' missing required field 'kind'"
                )
            kind = kind.strip()
            if kind not in ("starrocks", "postgres", "lakebase", "cube", "duckdb"):
                raise ServerConfigError(
                    f"Unsupported connection kind '{_safe_enum_token(kind)}' for engine '{_safe_label(eng_name_str)}'"
                )

            if kind == "starrocks":
                unknown_sr = set(eng_spec.keys()) - {"kind", "accounts", "storage_class"}
                if unknown_sr:
                    raise ServerConfigError(
                        f"Unknown key(s) in starrocks connection '{_safe_label(eng_name_str)}': "
                        f"{sorted(_safe_label(str(k)) for k in unknown_sr)}"
                    )
                sr_storage_class = _validate_optional_storage_class(
                    eng_spec.get("storage_class"),
                    f"StarRocks connection '{_safe_label(eng_name_str)}'",
                )
                accounts_raw = eng_spec.get("accounts")
                if not isinstance(accounts_raw, Mapping) or not accounts_raw:
                    raise ServerConfigError(
                        f"StarRocks connection '{eng_name_str}' must have a non-empty 'accounts' mapping"
                    )
                if "interactive" not in accounts_raw:
                    raise ServerConfigError(
                        f"StarRocks connection '{eng_name_str}' accounts must contain at least 'interactive'"
                    )
                invalid_accs = set(accounts_raw.keys()) - {
                    "interactive",
                    "bi",
                    "report",
                    "ask",
                    "job",
                }
                if invalid_accs:
                    raise ServerConfigError(
                        f"StarRocks connection '{eng_name_str}' contains invalid account(s): {sorted(invalid_accs)}"
                    )

                accounts: dict[str, StarRocksAccountConfig] = {}
                for acc_name, acc_spec in accounts_raw.items():
                    if not isinstance(acc_spec, Mapping):
                        raise ServerConfigError(
                            f"StarRocks account '{acc_name}' must be a mapping"
                        )
                    if "url_env" in acc_spec:
                        unknown_acc = set(acc_spec.keys()) - {"url_env"}
                        if unknown_acc:
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' cannot mix 'url_env' with other fields: {sorted(unknown_acc)}"
                            )
                        url_env = acc_spec["url_env"]
                        if not isinstance(url_env, str) or not url_env.strip():
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' 'url_env' must be a non-empty string"
                            )
                        accounts[acc_name] = StarRocksAccountConfig(
                            url_env=url_env.strip()
                        )
                    else:
                        unknown_acc = set(acc_spec.keys()) - {
                            "host",
                            "port",
                            "user",
                            "password_env",
                            "password_file",
                            "database",
                        }
                        if unknown_acc:
                            raise ServerConfigError(
                                f"Unknown key(s) in StarRocks account '{acc_name}': {sorted(unknown_acc)}"
                            )
                        host = acc_spec.get("host")
                        if not host or not isinstance(host, str) or not host.strip():
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' missing required 'host'"
                            )
                        port = acc_spec.get("port")
                        if (
                            isinstance(port, bool)
                            or not isinstance(port, int)
                            or port < 1
                            or port > 65535
                        ):
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' 'port' must be an integer between 1 and 65535"
                            )
                        user = acc_spec.get("user")
                        if not user or not isinstance(user, str) or not user.strip():
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' missing required 'user'"
                            )
                        database = acc_spec.get("database")
                        if database is not None and not isinstance(database, str):
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' 'database' must be a string"
                            )

                        has_penv = "password_env" in acc_spec
                        has_pfile = "password_file" in acc_spec
                        if has_penv and has_pfile:
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' cannot have both 'password_env' and 'password_file'"
                            )
                        if not has_penv and not has_pfile:
                            raise ServerConfigError(
                                f"StarRocks account '{acc_name}' must specify either 'password_env' or 'password_file'"
                            )

                        penv_val = None
                        pfile_val = None
                        if has_penv:
                            penv = acc_spec["password_env"]
                            if not isinstance(penv, str) or not penv.strip():
                                raise ServerConfigError(
                                    f"StarRocks account '{acc_name}' 'password_env' must be a non-empty string"
                                )
                            penv_val = penv.strip()
                        else:
                            pfile = acc_spec["password_file"]
                            if not isinstance(pfile, str) or not pfile.strip():
                                raise ServerConfigError(
                                    f"StarRocks account '{acc_name}' 'password_file' must be a non-empty string"
                                )
                            pfile_val = pfile.strip()
                            check_file_not_group_world_readable(
                                pfile_val,
                                f"StarRocks account '{acc_name}' password_file",
                            )

                        accounts[acc_name] = StarRocksAccountConfig(
                            host=host.strip(),
                            port=port,
                            user=user.strip(),
                            password_env=penv_val,
                            password_file=pfile_val,
                            database=database.strip() if database else None,
                        )
                connections[eng_name_str] = StarRocksConnectionConfig(
                    accounts=accounts,
                    storage_class=sr_storage_class,
                )

            elif kind in ("postgres", "lakebase"):
                connections[eng_name_str] = _parse_postgres_wire_connection(
                    eng_name_str, eng_spec, kind=kind
                )

            elif kind == "cube":
                unknown_cube = set(eng_spec.keys()) - {
                    "kind",
                    "host",
                    "port",
                    "dbname",
                    "sql_user",
                    "secret_env",
                    "connect_timeout_seconds",
                    "storage_class",
                }
                if unknown_cube:
                    raise ServerConfigError(
                        f"Unknown key(s) in cube connection '{_safe_label(eng_name_str)}': "
                        f"{sorted(_safe_label(str(k)) for k in unknown_cube)}"
                    )
                cube_storage_class = _validate_optional_storage_class(
                    eng_spec.get("storage_class"),
                    f"Cube connection '{_safe_label(eng_name_str)}'",
                )
                host = eng_spec.get("host")
                if not host or not isinstance(host, str) or not host.strip():
                    raise ServerConfigError(
                        f"Cube connection '{eng_name_str}' missing required 'host'"
                    )
                port = eng_spec.get("port")
                if (
                    isinstance(port, bool)
                    or not isinstance(port, int)
                    or port < 1
                    or port > 65535
                ):
                    raise ServerConfigError(
                        f"Cube connection '{eng_name_str}' 'port' must be an integer between 1 and 65535"
                    )
                dbname = eng_spec.get("dbname")
                if not dbname or not isinstance(dbname, str) or not dbname.strip():
                    raise ServerConfigError(
                        f"Cube connection '{eng_name_str}' missing required 'dbname'"
                    )
                sql_user = eng_spec.get("sql_user")
                if (
                    not sql_user
                    or not isinstance(sql_user, str)
                    or not sql_user.strip()
                ):
                    raise ServerConfigError(
                        f"Cube connection '{eng_name_str}' missing required 'sql_user'"
                    )

                timeout = eng_spec.get("connect_timeout_seconds", 10.0)
                if isinstance(timeout, (bool, str)) or not isinstance(timeout, (int, float)):
                    raise ServerConfigError(
                        f"Cube connection '{eng_name_str}' 'connect_timeout_seconds' must be a positive number"
                    )
                try:
                    timeout_float = float(timeout)
                    if (
                        timeout_float <= 0
                        or math.isnan(timeout_float)
                        or math.isinf(timeout_float)
                    ):
                        raise ValueError()
                except (TypeError, ValueError) as exc:
                    raise ServerConfigError(
                        f"Cube connection '{eng_name_str}' 'connect_timeout_seconds' must be a positive number"
                    ) from exc

                secret_env = eng_spec.get("secret_env")
                if cube_mode == "jwt":
                    if (
                        not secret_env
                        or not isinstance(secret_env, str)
                        or not secret_env.strip()
                    ):
                        raise ServerConfigError(
                            f"Cube connection '{eng_name_str}' requires 'secret_env' when cube_mode is 'jwt'"
                        )
                    secret_env_str = secret_env.strip()
                else:
                    if secret_env is not None:
                        raise ServerConfigError(
                            f"Cube connection '{eng_name_str}' forbids 'secret_env' when cube_mode is 'refuse'"
                        )
                    secret_env_str = None

                connections[eng_name_str] = CubeConnectionConfig(
                    host=host.strip(),
                    port=port,
                    dbname=dbname.strip(),
                    sql_user=sql_user.strip(),
                    secret_env=secret_env_str,
                    connect_timeout_seconds=timeout_float,
                    storage_class=cube_storage_class,
                )

            elif kind == "duckdb":
                profile_raw = eng_spec.get("profile", "local_file")
                if not isinstance(profile_raw, str) or not profile_raw.strip():
                    raise ServerConfigError(
                        f"DuckDB connection '{eng_name_str}' 'profile' must be a non-empty string"
                    )
                profile_str = profile_raw.strip()
                if profile_str not in ("local_file", "iceberg_direct"):
                    raise ServerConfigError(
                        f"DuckDB connection '{eng_name_str}' has unsupported profile "
                        f"'{_safe_label(profile_str)}', expected 'local_file' or 'iceberg_direct'"
                    )

                for raw_key in eng_spec.keys():
                    key_lower = str(raw_key).lower()
                    if (
                        "postgres" in key_lower
                        or "lakebase" in key_lower
                        or key_lower in ("attach", "pg", "postgresql")
                    ):
                        raise ServerConfigError(
                            f"DuckDB connection '{eng_name_str}' forbids Postgres/attach key "
                            f"'{_safe_label(str(raw_key))}'"
                        )

                mem_limit = eng_spec.get("memory_limit")
                if mem_limit is not None:
                    if not isinstance(mem_limit, str) or not mem_limit.strip():
                        raise ServerConfigError(
                            f"DuckDB connection '{eng_name_str}' 'memory_limit' must be a non-empty string"
                        )
                    mem_limit = mem_limit.strip()

                threads = eng_spec.get("threads")
                if threads is not None:
                    if isinstance(threads, bool) or not isinstance(threads, int) or threads <= 0:
                        raise ServerConfigError(
                            f"DuckDB connection '{eng_name_str}' 'threads' must be a positive integer"
                        )

                max_temp_sz = eng_spec.get("max_temp_directory_size")
                if max_temp_sz is not None:
                    if not isinstance(max_temp_sz, (str, int)) or isinstance(max_temp_sz, bool) or (
                        isinstance(max_temp_sz, str) and not max_temp_sz.strip()
                    ):
                        raise ServerConfigError(
                            f"DuckDB connection '{eng_name_str}' 'max_temp_directory_size' must be a non-empty string or integer"
                        )
                    max_temp_sz = str(max_temp_sz).strip()

                max_concurrent_cursors = eng_spec.get("max_concurrent_cursors", 32)
                if (
                    isinstance(max_concurrent_cursors, bool)
                    or not isinstance(max_concurrent_cursors, int)
                    or max_concurrent_cursors <= 0
                ):
                    raise ServerConfigError(
                        f"DuckDB connection '{eng_name_str}' 'max_concurrent_cursors' must be a positive integer"
                    )

                if profile_str == "local_file":
                    unknown_duck = set(eng_spec.keys()) - {
                        "kind",
                        "profile",
                        "path",
                        "allowed_directories",
                        "memory_limit",
                        "threads",
                        "temp_directory",
                        "max_temp_directory_size",
                        "max_concurrent_cursors",
                        "storage_class",
                    }
                    if unknown_duck:
                        raise ServerConfigError(
                            f"Unknown key(s) in duckdb connection '{_safe_label(eng_name_str)}': "
                            f"{sorted(_safe_label(str(k)) for k in unknown_duck)}"
                        )
                    duck_storage_class = _validate_optional_storage_class(
                        eng_spec.get("storage_class"),
                        f"DuckDB connection '{_safe_label(eng_name_str)}'",
                    )
                    path = eng_spec.get("path")
                    if not path or not isinstance(path, str) or not path.strip():
                        raise ServerConfigError(
                            f"DuckDB connection '{eng_name_str}' missing required 'path'"
                        )

                    raw_allowed = eng_spec.get("allowed_directories", ())
                    if not isinstance(raw_allowed, (list, tuple)):
                        raise ServerConfigError(
                            f"DuckDB connection '{eng_name_str}' 'allowed_directories' must be a list of strings"
                        )
                    validated_allowed: list[str] = []
                    for ad in raw_allowed:
                        if not isinstance(ad, str):
                            raise ServerConfigError(
                                f"DuckDB connection '{eng_name_str}' 'allowed_directories' entries must be strings"
                            )
                        if not (os.path.isabs(ad) and ad.endswith("/") and (ad == "/" or os.path.normpath(ad) + "/" == ad)):
                            raise ServerConfigError(
                                f"DuckDB connection '{eng_name_str}' allowed directory '{ad}' must be an absolute, normalized path ending with '/'"
                            )
                        validated_allowed.append(ad)

                    temp_dir = eng_spec.get("temp_directory")
                    if temp_dir is not None:
                        if not isinstance(temp_dir, str) or not temp_dir.strip():
                            raise ServerConfigError(
                                f"DuckDB connection '{eng_name_str}' 'temp_directory' must be a non-empty string"
                            )
                        temp_dir = temp_dir.strip()
                        if not os.path.isabs(temp_dir):
                            raise ServerConfigError(
                                f"DuckDB connection '{eng_name_str}' 'temp_directory' must be an absolute path"
                            )
                        if not os.path.exists(temp_dir) or not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK):
                            raise ServerConfigError(
                                f"DuckDB connection '{eng_name_str}' 'temp_directory' '{temp_dir}' must be an existing writable directory"
                            )

                        temp_real = os.path.realpath(temp_dir)
                        if path != ":memory:" and not path.startswith(":memory:"):
                            db_real = os.path.realpath(path)
                            db_dir_real = os.path.dirname(db_real)
                            if temp_real == db_dir_real or temp_real.startswith(db_dir_real + "/"):
                                raise ServerConfigError(
                                    f"DuckDB connection '{eng_name_str}' 'temp_directory' must not be inside data directory '{db_dir_real}'"
                                )

                        for ad in validated_allowed:
                            ad_real = os.path.realpath(ad)
                            if temp_real == ad_real or temp_real.startswith(ad_real + "/"):
                                raise ServerConfigError(
                                    f"DuckDB connection '{eng_name_str}' 'temp_directory' must not be inside allowed_directory '{ad}'"
                                )

                    connections[eng_name_str] = DuckDBConnectionConfig(
                        profile="local_file",
                        path=path.strip(),
                        allowed_directories=tuple(validated_allowed),
                        memory_limit=mem_limit,
                        threads=threads,
                        temp_directory=temp_dir,
                        max_temp_directory_size=max_temp_sz,
                        max_concurrent_cursors=max_concurrent_cursors,
                        storage_class=duck_storage_class,
                    )
                else:
                    if "path" in eng_spec:
                        raise ServerConfigError(
                            f"DuckDB connection '{eng_name_str}' with profile 'iceberg_direct' "
                            f"uses an in-memory database and forbids 'path'"
                        )
                    unknown_iceberg_duck = set(eng_spec.keys()) - {
                        "kind",
                        "profile",
                        "extension_directory",
                        "extensions",
                        "iceberg_catalogs",
                        "allowed_s3_prefixes",
                        "recycle_seconds",
                        "token_lifetime_seconds",
                        "external_access_after_attach",
                        "temp_directory",
                        "max_temp_directory_size",
                        "memory_limit",
                        "threads",
                        "max_concurrent_cursors",
                        "storage_class",
                    }
                    if unknown_iceberg_duck:
                        raise ServerConfigError(
                            f"Unknown key(s) in duckdb iceberg_direct connection '{eng_name_str}': "
                            f"{sorted(_safe_label(str(k)) for k in unknown_iceberg_duck)}"
                        )
                    iceberg_duck_storage_class = _validate_optional_storage_class(
                        eng_spec.get("storage_class"),
                        f"DuckDB iceberg_direct connection '{_safe_label(eng_name_str)}'",
                        forbid_local=True,
                    )

                    ext_dir = eng_spec.get("extension_directory")
                    if not ext_dir or not isinstance(ext_dir, str) or not ext_dir.strip():
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' missing required 'extension_directory'"
                        )
                    ext_dir = ext_dir.strip()
                    if not os.path.isabs(ext_dir):
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' 'extension_directory' must be an absolute path"
                        )
                    ext_dir_norm = os.path.normpath(ext_dir)
                    if ext_dir.rstrip("/") != ext_dir_norm.rstrip("/"):
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' 'extension_directory' must be a normalized path"
                        )

                    raw_exts = eng_spec.get("extensions")
                    if not isinstance(raw_exts, (list, tuple)) or not raw_exts:
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' requires non-empty 'extensions' list"
                        )
                    validated_exts: list[str] = []
                    for ext_item in raw_exts:
                        if not isinstance(ext_item, str) or not ext_item.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'extensions' entries must be non-empty strings"
                            )
                        ext_clean = ext_item.strip().lower()
                        if ext_clean not in _ALLOWED_ICEBERG_EXTENSIONS:
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' extension "
                                f"'{_safe_label(ext_clean)}' is not in allowlist {{'avro', 'httpfs', 'iceberg'}}"
                            )
                        if ext_clean not in validated_exts:
                            validated_exts.append(ext_clean)

                    if "iceberg" in validated_exts and "avro" not in validated_exts:
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' requires 'avro' in 'extensions' when 'iceberg' is configured"
                        )
                    if "avro" in validated_exts and "iceberg" in validated_exts:
                        if validated_exts.index("avro") > validated_exts.index("iceberg"):
                            validated_exts.remove("avro")
                            validated_exts.insert(validated_exts.index("iceberg"), "avro")

                    check_extension_files_on_disk(
                        ext_dir,
                        validated_exts,
                        eng_name=eng_name_str,
                        only_when_populated=True,
                    )

                    raw_prefixes = eng_spec.get("allowed_s3_prefixes")
                    if not isinstance(raw_prefixes, (list, tuple)) or not raw_prefixes:
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' requires non-empty 'allowed_s3_prefixes' list"
                        )
                    validated_prefixes: list[str] = []
                    for pfx in raw_prefixes:
                        if not isinstance(pfx, str) or not pfx.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'allowed_s3_prefixes' entries must be non-empty strings"
                            )
                        pfx_str = pfx.strip()
                        if (
                            not _S3_PREFIX_RE.match(pfx_str)
                            or ".." in pfx_str
                            or "//" in pfx_str[len("s3://"):]
                        ):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' invalid S3 prefix "
                                f"'{_safe_label(pfx_str)}', expected 's3://<bucket>/<prefix>/'"
                            )
                        validated_prefixes.append(pfx_str)

                    temp_dir = eng_spec.get("temp_directory")
                    if temp_dir is not None:
                        if not isinstance(temp_dir, str) or not temp_dir.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'temp_directory' must be a non-empty string"
                            )
                        temp_dir = temp_dir.strip()
                        if not os.path.isabs(temp_dir):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'temp_directory' must be an absolute path"
                            )
                        temp_dir_norm = os.path.normpath(temp_dir)
                        if temp_dir.rstrip("/") != temp_dir_norm.rstrip("/"):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'temp_directory' must be a normalized path"
                            )
                        if os.path.exists(temp_dir) and (
                            not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK)
                        ):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'temp_directory' '{temp_dir}' must be a writable directory"
                            )
                        ext_real = os.path.realpath(ext_dir)
                        temp_real = os.path.realpath(temp_dir)
                        if (
                            temp_real == ext_real
                            or temp_real.startswith(ext_real.rstrip("/") + "/")
                            or ext_real.startswith(temp_real.rstrip("/") + "/")
                        ):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'temp_directory' and 'extension_directory' must not overlap"
                            )

                    raw_recycle = eng_spec.get("recycle_seconds")
                    if raw_recycle is None or isinstance(raw_recycle, (bool, str)) or not isinstance(raw_recycle, (int, float)):
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' 'recycle_seconds' must be a positive number"
                        )
                    recycle_f = float(raw_recycle)
                    if recycle_f <= 0 or math.isnan(recycle_f) or math.isinf(recycle_f):
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' 'recycle_seconds' must be > 0"
                        )

                    conn_tok_life: float | None = None
                    if "token_lifetime_seconds" in eng_spec and eng_spec["token_lifetime_seconds"] is not None:
                        raw_tl = eng_spec["token_lifetime_seconds"]
                        if isinstance(raw_tl, (bool, str)) or not isinstance(raw_tl, (int, float)):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'token_lifetime_seconds' must be a positive number"
                            )
                        conn_tok_life = float(raw_tl)
                        if conn_tok_life <= 0 or math.isnan(conn_tok_life) or math.isinf(conn_tok_life):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'token_lifetime_seconds' must be > 0"
                            )
                        if recycle_f >= conn_tok_life:
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' 'recycle_seconds' ({recycle_f}) "
                                f"must be strictly below 'token_lifetime_seconds' ({conn_tok_life})"
                            )

                    raw_ext_access = eng_spec.get("external_access_after_attach", "off")
                    if raw_ext_access is False:
                        ext_access_str = "off"
                    elif raw_ext_access is True:
                        ext_access_str = "on"
                    elif isinstance(raw_ext_access, str) and raw_ext_access.strip().lower() in ("off", "on"):
                        ext_access_str = raw_ext_access.strip().lower()
                    else:
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' 'external_access_after_attach' "
                            f"must be 'off' or 'on'"
                        )

                    cats_raw = eng_spec.get("iceberg_catalogs")
                    if not isinstance(cats_raw, Mapping) or not cats_raw:
                        raise ServerConfigError(
                            f"DuckDB iceberg_direct connection '{eng_name_str}' requires non-empty 'iceberg_catalogs' mapping"
                        )

                    parsed_catalogs: dict[str, IcebergCatalogConfig] = {}
                    for cat_alias, cat_spec in cats_raw.items():
                        if not isinstance(cat_alias, str) or not _ICEBERG_ALIAS_RE.fullmatch(cat_alias):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' catalog alias "
                                f"'{_safe_label(cat_alias)}' must match ^[a-z][a-z0-9_]{{0,31}}$"
                            )
                        if cat_alias in ("system", "temp", "memory", "main"):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' catalog alias "
                                f"'{cat_alias}' is reserved"
                            )
                        if not isinstance(cat_spec, Mapping):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct connection '{eng_name_str}' catalog '{cat_alias}' must be a mapping"
                            )
                        for raw_ck in cat_spec.keys():
                            ck_lower = str(raw_ck).lower()
                            if "postgres" in ck_lower or ck_lower in ("client_id", "client_secret", "token", "secret", "password"):
                                raise ServerConfigError(
                                    f"DuckDB iceberg_direct catalog '{cat_alias}' forbids literal secret or Postgres key "
                                    f"'{_safe_label(str(raw_ck))}'; use *_env environment variable names only"
                                )
                        unknown_cat = set(cat_spec.keys()) - {
                            "endpoint",
                            "warehouse",
                            "token_uri",
                            "client_id_env",
                            "client_secret_env",
                            "scope",
                            "access_delegation",
                            "token_lifetime_seconds",
                            "s3",
                        }
                        if unknown_cat:
                            raise ServerConfigError(
                                f"Unknown key(s) in iceberg_catalogs.{cat_alias}: "
                                f"{sorted(_safe_label(str(k)) for k in unknown_cat)}"
                            )

                        endpoint = cat_spec.get("endpoint")
                        if not endpoint or not isinstance(endpoint, str) or not endpoint.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' missing required 'endpoint'"
                            )
                        endpoint = endpoint.strip()
                        if not (endpoint.startswith("http://") or endpoint.startswith("https://")):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' 'endpoint' must start with http:// or https://"
                            )

                        warehouse = cat_spec.get("warehouse")
                        if not warehouse or not isinstance(warehouse, str) or not warehouse.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' missing required 'warehouse'"
                            )
                        warehouse = warehouse.strip()

                        token_uri = cat_spec.get("token_uri")
                        if not token_uri or not isinstance(token_uri, str) or not token_uri.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' missing required 'token_uri'"
                            )
                        token_uri = token_uri.strip()
                        if not (token_uri.startswith("http://") or token_uri.startswith("https://")):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' 'token_uri' must start with http:// or https://"
                            )

                        cid_env = cat_spec.get("client_id_env")
                        if not cid_env or not isinstance(cid_env, str) or not _ENV_VAR_NAME_RE.fullmatch(cid_env.strip()):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' 'client_id_env' must be a valid environment variable name"
                            )
                        cid_env = cid_env.strip()

                        csec_env = cat_spec.get("client_secret_env")
                        if not csec_env or not isinstance(csec_env, str) or not _ENV_VAR_NAME_RE.fullmatch(csec_env.strip()):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' 'client_secret_env' must be a valid environment variable name"
                            )
                        csec_env = csec_env.strip()

                        scope_val = cat_spec.get("scope")
                        if scope_val is not None:
                            if not isinstance(scope_val, str) or not scope_val.strip():
                                raise ServerConfigError(
                                    f"DuckDB iceberg_direct catalog '{cat_alias}' 'scope' must be a non-empty string"
                                )
                            scope_val = scope_val.strip()

                        access_del = cat_spec.get("access_delegation")
                        if not isinstance(access_del, str) or access_del.strip() not in ("vended", "none"):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' 'access_delegation' must be 'vended' or 'none'"
                            )
                        access_del = access_del.strip()

                        cat_tok_life: float | None = None
                        if "token_lifetime_seconds" in cat_spec and cat_spec["token_lifetime_seconds"] is not None:
                            raw_ctl = cat_spec["token_lifetime_seconds"]
                            if isinstance(raw_ctl, (bool, str)) or not isinstance(raw_ctl, (int, float)):
                                raise ServerConfigError(
                                    f"DuckDB iceberg_direct catalog '{cat_alias}' 'token_lifetime_seconds' must be a positive number"
                                )
                            cat_tok_life = float(raw_ctl)
                            if cat_tok_life <= 0 or math.isnan(cat_tok_life) or math.isinf(cat_tok_life):
                                raise ServerConfigError(
                                    f"DuckDB iceberg_direct catalog '{cat_alias}' 'token_lifetime_seconds' must be > 0"
                                )
                            if recycle_f >= cat_tok_life:
                                raise ServerConfigError(
                                    f"DuckDB iceberg_direct connection '{eng_name_str}' 'recycle_seconds' ({recycle_f}) "
                                    f"must be strictly below catalog '{cat_alias}' 'token_lifetime_seconds' ({cat_tok_life})"
                                )

                        s3_raw = cat_spec.get("s3")
                        if not isinstance(s3_raw, Mapping):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}' missing required 's3' mapping"
                            )
                        for raw_sk in s3_raw.keys():
                            sk_lower = str(raw_sk).lower()
                            if sk_lower in ("key_id", "secret", "access_key", "secret_key", "password", "token"):
                                raise ServerConfigError(
                                    f"DuckDB iceberg_direct catalog '{cat_alias}.s3' forbids literal secret key "
                                    f"'{_safe_label(str(raw_sk))}'; use 'key_id_env' and 'secret_env' only"
                                )
                        unknown_s3 = set(s3_raw.keys()) - {
                            "endpoint",
                            "region",
                            "url_style",
                            "key_id_env",
                            "secret_env",
                            "use_ssl",
                        }
                        if unknown_s3:
                            raise ServerConfigError(
                                f"Unknown key(s) in iceberg_catalogs.{cat_alias}.s3: "
                                f"{sorted(_safe_label(str(k)) for k in unknown_s3)}"
                            )

                        s3_endpoint = s3_raw.get("endpoint")
                        if not s3_endpoint or not isinstance(s3_endpoint, str) or not s3_endpoint.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}.s3' missing required 'endpoint'"
                            )
                        s3_endpoint = s3_endpoint.strip()

                        s3_region = s3_raw.get("region")
                        if not s3_region or not isinstance(s3_region, str) or not s3_region.strip():
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}.s3' missing required 'region'"
                            )
                        s3_region = s3_region.strip()

                        s3_url_style = s3_raw.get("url_style", "path")
                        if not isinstance(s3_url_style, str) or s3_url_style.strip() not in ("path", "vhost"):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}.s3' 'url_style' must be 'path' or 'vhost'"
                            )
                        s3_url_style = s3_url_style.strip()

                        s3_key_env = s3_raw.get("key_id_env")
                        if not s3_key_env or not isinstance(s3_key_env, str) or not _ENV_VAR_NAME_RE.fullmatch(s3_key_env.strip()):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}.s3' 'key_id_env' must be a valid environment variable name"
                            )
                        s3_key_env = s3_key_env.strip()

                        s3_sec_env = s3_raw.get("secret_env")
                        if not s3_sec_env or not isinstance(s3_sec_env, str) or not _ENV_VAR_NAME_RE.fullmatch(s3_sec_env.strip()):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}.s3' 'secret_env' must be a valid environment variable name"
                            )
                        s3_sec_env = s3_sec_env.strip()

                        s3_use_ssl = s3_raw.get("use_ssl")
                        if not isinstance(s3_use_ssl, bool):
                            raise ServerConfigError(
                                f"DuckDB iceberg_direct catalog '{cat_alias}.s3' 'use_ssl' is required and must be a boolean"
                            )

                        parsed_catalogs[cat_alias] = IcebergCatalogConfig(
                            alias=cat_alias,
                            endpoint=endpoint,
                            warehouse=warehouse,
                            token_uri=token_uri,
                            client_id_env=cid_env,
                            client_secret_env=csec_env,
                            scope=scope_val,
                            access_delegation=access_del,
                            token_lifetime_seconds=cat_tok_life,
                            s3=IcebergS3Config(
                                endpoint=s3_endpoint,
                                region=s3_region,
                                url_style=s3_url_style,
                                key_id_env=s3_key_env,
                                secret_env=s3_sec_env,
                                use_ssl=s3_use_ssl,
                            ),
                        )

                    referenced_duck_vars: set[str] = set()
                    for cat_obj in parsed_catalogs.values():
                        referenced_duck_vars.add(cat_obj.client_id_env)
                        referenced_duck_vars.add(cat_obj.client_secret_env)
                        referenced_duck_vars.add(cat_obj.s3.key_id_env)
                        referenced_duck_vars.add(cat_obj.s3.secret_env)
                    scoped_secrets_env = {
                        k: v for k, v in secrets_env.items() if k in referenced_duck_vars
                    }

                    connections[eng_name_str] = DuckDBConnectionConfig(
                        profile="iceberg_direct",
                        path=":memory:",
                        extension_directory=ext_dir,
                        extensions=tuple(validated_exts),
                        iceberg_catalogs=parsed_catalogs,
                        allowed_s3_prefixes=tuple(validated_prefixes),
                        recycle_seconds=recycle_f,
                        token_lifetime_seconds=conn_tok_life,
                        external_access_after_attach=ext_access_str,
                        memory_limit=mem_limit,
                        threads=threads,
                        temp_directory=temp_dir,
                        max_temp_directory_size=max_temp_sz,
                        max_concurrent_cursors=max_concurrent_cursors,
                        storage_class=iceberg_duck_storage_class,
                        secrets_env=scoped_secrets_env,
                    )

        # 12. limits
        lim_raw = d.get("limits")
        if lim_raw is None:
            limits_cfg = LimitsConfig()
        else:
            if not isinstance(lim_raw, Mapping):
                raise ServerConfigError("'limits' must be a mapping")
            unknown_lim = set(lim_raw.keys()) - {
                "timeout_seconds",
                "max_body_bytes",
                "max_result_bytes",
            }
            if unknown_lim:
                raise ServerConfigError(
                    f"Unknown key(s) in 'limits': {sorted(unknown_lim)}"
                )
            to_val = lim_raw.get("timeout_seconds", 30.0)
            if isinstance(to_val, (bool, str)) or not isinstance(to_val, (int, float)):
                raise ServerConfigError(
                    "'limits.timeout_seconds' must be a positive number"
                )
            try:
                to_f = float(to_val)
                if to_f <= 0 or math.isnan(to_f) or math.isinf(to_f):
                    raise ValueError()
            except (TypeError, ValueError) as exc:
                raise ServerConfigError(
                    "'limits.timeout_seconds' must be a positive number"
                ) from exc

            mb_val = lim_raw.get("max_body_bytes", 1_000_000)
            if isinstance(mb_val, bool) or not isinstance(mb_val, int) or mb_val <= 0:
                raise ServerConfigError(
                    "'limits.max_body_bytes' must be a positive integer"
                )

            mr_val = lim_raw.get("max_result_bytes", 67_108_864)
            if isinstance(mr_val, bool) or not isinstance(mr_val, int) or mr_val <= 0:
                raise ServerConfigError(
                    "'limits.max_result_bytes' must be a positive integer"
                )

            limits_cfg = LimitsConfig(
                timeout_seconds=to_f,
                max_body_bytes=mb_val,
                max_result_bytes=mr_val,
            )

        # 13. grant_max_lifetime_seconds
        grant_max_lifetime_raw = d.get("grant_max_lifetime_seconds", 60.0)
        if isinstance(grant_max_lifetime_raw, bool) or not isinstance(
            grant_max_lifetime_raw, (int, float)
        ):
            raise ServerConfigError(
                "'grant_max_lifetime_seconds' must be a finite number"
            )
        try:
            grant_max_lifetime = float(grant_max_lifetime_raw)
        except (ValueError, TypeError) as exc:
            raise ServerConfigError(
                "'grant_max_lifetime_seconds' must be a finite number"
            ) from exc
        if (
            math.isnan(grant_max_lifetime)
            or math.isinf(grant_max_lifetime)
            or grant_max_lifetime <= 0
            or grant_max_lifetime > 300
        ):
            raise ServerConfigError(
                f"'grant_max_lifetime_seconds' must be a finite number with 0 < x <= 300, got {grant_max_lifetime}"
            )

        # 14. replay_cache_max_entries
        replay_cache_max_raw = d.get("replay_cache_max_entries", 100_000)
        if (
            isinstance(replay_cache_max_raw, bool)
            or not isinstance(replay_cache_max_raw, int)
            or replay_cache_max_raw <= 0
        ):
            raise ServerConfigError(
                "'replay_cache_max_entries' must be a positive integer"
            )
        replay_cache_max_entries = replay_cache_max_raw

        # 15. compilation_cache_max_entries
        compilation_cache_max_raw = d.get(
            "compilation_cache_max_entries", DEFAULT_COMPILATION_CACHE_MAX_ENTRIES
        )
        compilation_cache_max_entries = validate_compilation_cache_max_entries(
            compilation_cache_max_raw,
            error_cls=ServerConfigError,
        )

        # 16. audit
        audit_raw = d.get("audit")
        if audit_raw is None:
            audit_cfg = None
        else:
            audit_cfg = AuditConfig.from_dict(audit_raw, error_cls=ServerConfigError)

        # 17. ops
        ops_raw = d.get("ops")
        if ops_raw is None:
            ops_cfg = None
        else:
            ops_cfg = OpsConfig.from_dict(ops_raw, error_cls=ServerConfigError)

        # 18. cursor_key_env
        cursor_key_env_raw = d.get("cursor_key_env")
        cursor_key_env: str | None = None
        if cursor_key_env_raw is not None:
            if (
                not isinstance(cursor_key_env_raw, str)
                or not cursor_key_env_raw.strip()
                or not _ENV_VAR_NAME_RE.fullmatch(cursor_key_env_raw.strip())
            ):
                raise ServerConfigError(
                    "'cursor_key_env' must be a valid environment variable name "
                    "matching ^[A-Za-z_][A-Za-z0-9_]{0,127}$"
                )
            cursor_key_env = cursor_key_env_raw.strip()

        server_config = cls(
            listen=listen_cfg,
            tls=tls_cfg,
            grant_keys=grant_keys_cfg,
            secrets_env_file=secrets_env_file,
            logging=logging_cfg,
            require_grant_typ=require_grant_typ,
            default_catalog=default_catalog,
            cube_mode=cube_mode,
            placement=placement_cfg,
            connections=connections,
            limits=limits_cfg,
            secrets_env=secrets_env,
            grant_max_lifetime_seconds=grant_max_lifetime,
            replay_cache_max_entries=replay_cache_max_entries,
            compilation_cache_max_entries=compilation_cache_max_entries,
            audit=audit_cfg,
            ops=ops_cfg,
            cursor_key_env=cursor_key_env,
        )

        server_config._register_resolved_secrets(env=env)

        if load_keys:
            server_config.load_grant_keys()

        return server_config

    @classmethod
    def from_yaml(
        cls,
        path: str | os.PathLike,
        *,
        load_keys: bool = True,
        env: Mapping[str, str] | None = None,
    ) -> ServerConfig:
        """Construct and validate ServerConfig from a YAML file."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.load(f, Loader=UniqueKeySafeLoader)
        except FileNotFoundError as exc:
            raise ServerConfigError(f"Config file not found: '{path}'") from exc
        except yaml.YAMLError as exc:
            raise ServerConfigError(_format_yaml_error(exc, "config file", path)) from exc
        except OSError as exc:
            raise ServerConfigError(
                f"Failed to read config file '{path}': {exc.strerror or type(exc).__name__}"
            ) from exc

        if not isinstance(data, Mapping):
            raise ServerConfigError(
                f"Config file '{path}' must contain a mapping, got {type(data).__name__}"
            )
        return cls.from_dict(data, load_keys=load_keys, env=env)


def _load_static_placement(path: str | os.PathLike) -> Placement:
    """Load and validate a static placement YAML file with secret-safe error reporting."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.load(f, Loader=UniqueKeySafeLoader)
    except FileNotFoundError as exc:
        raise ServerConfigError(
            f"Static placement file not found: '{path}' — verify 'placement.static.placement_file'"
        ) from exc
    except yaml.YAMLError as exc:
        raise ServerConfigError(
            _format_yaml_error(exc, "static placement file", path)
        ) from exc
    except OSError as exc:
        raise ServerConfigError(
            f"Failed to read static placement file '{path}': {exc.strerror or type(exc).__name__}"
        ) from exc

    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise ServerConfigError(
            f"Static placement file '{path}' must contain a YAML mapping at root, "
            f"got {type(raw).__name__}"
        )

    for section in ("engines", "tables", "cubes", "policy"):
        sec_val = raw.get(section)
        if sec_val is not None and not isinstance(sec_val, Mapping):
            raise ServerConfigError(
                f"Static placement file '{path}' section '{section}' must be a mapping, "
                f"got {type(sec_val).__name__}"
            )

    for section in ("engines", "tables", "cubes"):
        sec_val = raw.get(section) or {}
        for entry_name, entry_spec in sec_val.items():
            if not isinstance(entry_spec, Mapping):
                raise ServerConfigError(
                    f"Static placement file '{path}' entry '{section}.{_safe_label(entry_name)}' "
                    f"must be a mapping, got {type(entry_spec).__name__}"
                )
            if section == "tables":
                serve_val = entry_spec.get("serve")
                if serve_val is not None and not isinstance(serve_val, Mapping):
                    raise ServerConfigError(
                        f"Static placement file '{path}' field "
                        f"'tables.{_safe_label(entry_name)}.serve' must be a mapping, "
                        f"got {type(serve_val).__name__}"
                    )

    try:
        return Placement.from_dict(raw)
    except (PlacementError, TypeError, ValueError, AttributeError, OverflowError) as exc:
        raise ServerConfigError(
            f"Invalid static placement in '{path}' — verify 'engines', 'tables', 'cubes', "
            f"and 'policy' definitions"
        ) from exc


class _DuplicateJsonKeyError(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    obj: dict[str, Any] = {}
    for k, v in pairs:
        if k in seen:
            raise _DuplicateJsonKeyError(k)
        seen.add(k)
        obj[k] = v
    return obj


def _load_and_validate_static_schemas(
    path: str | os.PathLike,
    placement: Placement,
) -> dict[str, list[str]]:
    """Load and validate static schemas JSON against static Placement tables and cubes."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError as exc:
        raise ServerConfigError(
            f"Static schemas file not found: '{path}' — verify 'placement.static.schemas_file'"
        ) from exc
    except OSError as exc:
        raise ServerConfigError(
            f"Failed to read static schemas file '{path}': {exc.strerror or type(exc).__name__}"
        ) from exc

    try:
        raw = json.loads(content, object_pairs_hook=_reject_duplicate_json_keys)
    except _DuplicateJsonKeyError as exc:
        raise ServerConfigError(
            f"Static schemas file '{path}' contains duplicate JSON key "
            f"'{_safe_label(exc.key)}' — remove duplicate key entry"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ServerConfigError(
            _format_json_error(exc, "static schemas file", path)
        ) from exc

    if not isinstance(raw, dict):
        raise ServerConfigError(
            f"Static schemas file '{path}' root must be a JSON object mapping logical "
            f"table/cube names to column lists, got {type(raw).__name__}"
        )

    # Check non-empty keys and case-insensitive key collisions in schemas
    schemas_by_casefold: dict[str, str] = {}
    validated_schemas: dict[str, list[str]] = {}

    for key, cols in raw.items():
        if not isinstance(key, str) or not key.strip():
            raise ServerConfigError(
                f"Static schemas file '{path}' contains an empty or non-string table/cube key "
                f"— all schema keys must be non-empty logical names"
            )
        folded_key = key.casefold()
        if folded_key in schemas_by_casefold:
            prev_key = schemas_by_casefold[folded_key]
            raise ServerConfigError(
                f"Static schemas file '{path}' has case-insensitive key collision between "
                f"'{_safe_label(prev_key)}' and '{_safe_label(key)}' — keep only one canonical key"
            )
        schemas_by_casefold[folded_key] = key

        safe_tbl = _safe_label(key)
        if not isinstance(cols, list):
            raise ServerConfigError(
                f"Static schemas file '{path}' entry for '{safe_tbl}' must be a JSON list "
                f"of column names, got {type(cols).__name__}"
            )

        seen_cols_casefold: dict[str, str] = {}
        seen_cols_lower: dict[str, str] = {}
        clean_cols: list[str] = []
        for idx, col in enumerate(cols):
            if not isinstance(col, str) or not col.strip():
                raise ServerConfigError(
                    f"Static schemas file '{path}' entry '{safe_tbl}' has invalid column at "
                    f"index {idx} — every column name must be a non-empty string"
                )
            col_folded = col.casefold()
            col_lower = col.lower()
            if col_folded in seen_cols_casefold or col_lower in seen_cols_lower:
                raise ServerConfigError(
                    f"Static schemas file '{path}' entry '{safe_tbl}' has duplicate column "
                    f"at index {idx} — column names must be unique case-insensitively"
                )
            seen_cols_casefold[col_folded] = col
            seen_cols_lower[col_lower] = col
            clean_cols.append(col)

        validated_schemas[key] = clean_cols

    # Expected logical names from placement tables + cubes (rejecting ambiguous casefold/lower collisions)
    expected_by_casefold: dict[str, str] = {}
    expected_by_lower: dict[str, str] = {}
    for kind_label, names_iter in (
        ("table", placement.tables.keys()),
        ("cube", placement.cubes.keys()),
    ):
        for logical_name in names_iter:
            folded = logical_name.casefold()
            lowered = logical_name.lower()
            if folded in expected_by_casefold or lowered in expected_by_lower:
                prev_name = (
                    expected_by_casefold.get(folded)
                    or expected_by_lower.get(lowered, "")
                )
                raise ServerConfigError(
                    f"Static placement defines ambiguous {kind_label} name "
                    f"'{_safe_label(logical_name)}' that collides under casefold with "
                    f"'{_safe_label(prev_name)}' — use distinct logical names"
                )
            expected_by_casefold[folded] = logical_name
            expected_by_lower[lowered] = logical_name

    missing_folded = sorted(set(expected_by_casefold.keys()) - set(schemas_by_casefold.keys()))
    if missing_folded:
        missing_names = [_safe_label(expected_by_casefold[k]) for k in missing_folded]
        raise ServerConfigError(
            f"Static schemas file '{path}' is missing schema entry for placement "
            f"table/cube: {', '.join(repr(n) for n in missing_names)} — add matching column list(s) "
            f"to '{path}' or remove the table/cube from placement"
        )

    extra_folded = sorted(set(schemas_by_casefold.keys()) - set(expected_by_casefold.keys()))
    if extra_folded:
        extra_names = [_safe_label(schemas_by_casefold[k]) for k in extra_folded]
        raise ServerConfigError(
            f"Static schemas file '{path}' contains stale/extra schema key(s) not present in "
            f"placement tables or cubes: {', '.join(repr(n) for n in extra_names)} — remove stale "
            f"schema key(s) from '{path}' or declare them in placement"
        )

    # Require actual runtime lower-normalized schema-key set to equal lower-normalized
    # placement tables+cubes key set so runtime enforcer/Placement .lower() lookup and
    # preflight .casefold() check never diverge on Unicode edge cases (e.g. 'straße' vs 'STRASSE').
    schemas_lower_set = {k.lower() for k in validated_schemas.keys()}
    expected_lower_set = set(expected_by_lower.keys())
    if schemas_lower_set != expected_lower_set:
        for folded, pl_name in expected_by_casefold.items():
            sch_name = schemas_by_casefold[folded]
            if pl_name.lower() != sch_name.lower():
                raise ServerConfigError(
                    f"Static schemas file '{path}' key '{_safe_label(sch_name)}' matches placement "
                    f"name '{_safe_label(pl_name)}' by casefold but differs under lowercase — "
                    f"use matching casing in '{path}'"
                )
        raise ServerConfigError(
            f"Static schemas file '{path}' lowercase key coverage does not match "
            f"placement tables and cubes — align schema key casing with placement"
        )

    return validated_schemas

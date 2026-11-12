"""Server configuration model and validation for Seeknal Serving."""

from __future__ import annotations

import base64
import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from typing import Any

import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.compilation_cache import (
    DEFAULT_COMPILATION_CACHE_MAX_ENTRIES,
    validate_compilation_cache_max_entries,
)
from seeknal.serving.discovery.config import DiscoveryConfig
from seeknal.serving.grant_token import _load_public_key
from seeknal.serving.placement import Placement, PlacementError


class ServerConfigError(Exception):
    """Raised when server configuration is invalid or missing."""


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")
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


def _format_yaml_error(exc: yaml.YAMLError, role: str, path: str | os.PathLike) -> str:
    """Format a PyYAML error with file path and line/column, never raw file snippets."""
    mark = getattr(exc, "problem_mark", None)
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
        return content[:-2]
    elif content.endswith("\n"):
        return content[:-1]
    return content


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
    return result


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


@dataclass(frozen=True)
class LakebaseConnectionConfig:
    kind: str = "lakebase"
    dsn_env: str = ""


@dataclass(frozen=True)
class CubeConnectionConfig:
    kind: str = "cube"
    host: str = ""
    port: int = 15432
    dbname: str = ""
    sql_user: str = ""
    secret_env: str | None = None
    connect_timeout_seconds: float = 10.0


@dataclass(frozen=True)
class DuckDBConnectionConfig:
    kind: str = "duckdb"
    path: str = ""
    allowed_directories: tuple[str, ...] = ()
    memory_limit: str | None = None
    threads: int | None = None


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
    secrets_env: dict[str, str] = field(default_factory=dict)
    grant_max_lifetime_seconds: float = 60.0
    replay_cache_max_entries: int = 100_000
    compilation_cache_max_entries: int = DEFAULT_COMPILATION_CACHE_MAX_ENTRIES

    def __post_init__(self) -> None:
        validate_compilation_cache_max_entries(
            self.compilation_cache_max_entries,
            error_cls=ServerConfigError,
        )

    def load_grant_keys(self) -> dict[str, str]:
        """Load and return parsed PEM public keys."""
        return load_grant_keys(self.grant_keys)

    def get_env(self, var_name: str, env: Mapping[str, str] | None = None) -> str:
        """Resolve an environment variable, consulting secrets_env before the process env."""
        if var_name in self.secrets_env:
            return self.secrets_env[var_name]
        process_env = env if env is not None else os.environ
        if var_name in process_env:
            return process_env[var_name]
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
            elif isinstance(conn, LakebaseConnectionConfig):
                if conn.dsn_env:
                    referenced.append(conn.dsn_env)
            elif isinstance(conn, CubeConnectionConfig):
                if conn.secret_env:
                    referenced.append(conn.secret_env)

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

        return referenced

    def check_secret_files(self) -> None:
        """Verify that all referenced secret files exist and have safe permissions."""
        secret_files: list[tuple[str, str]] = []

        # Connections
        for eng_name, conn in self.connections.items():
            if isinstance(conn, StarRocksConnectionConfig):
                for acc_name, acc in conn.accounts.items():
                    if acc.password_file:
                        secret_files.append(
                            (
                                acc.password_file,
                                f"StarRocks account '{acc_name}' password_file",
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

    def check_env(self, env: Mapping[str, str] | None = None) -> None:
        """Verify that every referenced environment variable exists and secret files are safe."""
        for var in self.get_referenced_env_vars():
            self.get_env(var, env)
        self.check_secret_files()

    def validate_runtime_wiring(
        self,
        *,
        env: Mapping[str, str] | None = None,
    ) -> PreflightResult:
        """Offline preflight validation of env, static placement/schemas, and engine-connection wiring.

        Returns a PreflightResult so callers (such as build_server) can reuse parsed
        static Placement and schemas without re-reading files.
        """
        self.check_env(env=env)

        if self.placement.is_static:
            static_cfg = self.placement.static
            assert static_cfg is not None
            placement = _load_static_placement(static_cfg.placement_file)

            # Every declared placement engine must have an exact same-name connection with matching kind
            for eng_name, eng in placement.engines.items():
                safe_eng = _safe_label(eng_name)
                if eng_name not in self.connections:
                    raise ServerConfigError(
                        f"Static placement file '{static_cfg.placement_file}' declares engine "
                        f"'{safe_eng}' (kind '{eng.kind}') in 'placement.engines', but no matching "
                        f"entry 'connections.{safe_eng}' is configured — add 'connections.{safe_eng}' "
                        f"with kind '{eng.kind}' or remove the unused engine from placement"
                    )
                conn = self.connections[eng_name]
                conn_kind = getattr(conn, "kind", None)
                if conn_kind != eng.kind:
                    raise ServerConfigError(
                        f"Engine '{safe_eng}' kind mismatch between static placement "
                        f"('{static_cfg.placement_file}', kind '{eng.kind}') and "
                        f"'connections.{safe_eng}' (kind '{conn_kind}') — align both to '{eng.kind}'"
                    )

            schemas = _load_and_validate_static_schemas(
                static_cfg.schemas_file,
                placement,
            )
            return PreflightResult(
                mode="static",
                placement=placement,
                schemas=schemas,
            )

        disc_cfg = self.placement.discovery_config
        assert disc_cfg is not None

        for eng_name, eng_cfg in disc_cfg.engines.items():
            safe_eng = _safe_label(eng_name)
            if eng_name not in self.connections:
                raise ServerConfigError(
                    f"Discovery engine 'placement.discovery.engines.{safe_eng}' "
                    f"(kind '{eng_cfg.kind}') has no matching entry 'connections.{safe_eng}' — "
                    f"add 'connections.{safe_eng}' with kind '{eng_cfg.kind}' or remove the "
                    f"engine from 'placement.discovery.engines'"
                )
            conn = self.connections[eng_name]
            conn_kind = getattr(conn, "kind", None)
            if conn_kind != eng_cfg.kind:
                raise ServerConfigError(
                    f"Discovery engine 'placement.discovery.engines.{safe_eng}' has kind "
                    f"'{eng_cfg.kind}', but 'connections.{safe_eng}' has kind '{conn_kind}' — "
                    f"align both to '{eng_cfg.kind}'"
                )

        if disc_cfg.lakebase is not None:
            lb_eng = disc_cfg.lakebase.engine
            safe_lb = _safe_label(lb_eng)
            if lb_eng not in self.connections:
                raise ServerConfigError(
                    f"Discovery 'placement.discovery.lakebase.engine' references '{safe_lb}', "
                    f"but 'connections.{safe_lb}' is not configured — add a lakebase connection "
                    f"named '{safe_lb}'"
                )
            lb_conn = self.connections[lb_eng]
            lb_kind = getattr(lb_conn, "kind", None)
            if lb_kind != "lakebase":
                raise ServerConfigError(
                    f"Discovery 'placement.discovery.lakebase.engine' ('{safe_lb}') requires "
                    f"'connections.{safe_lb}' to have kind 'lakebase', got '{lb_kind}'"
                )

        return PreflightResult(mode="discovery", placement=None, schemas=None)

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
        }
        unknown_top = set(d.keys()) - allowed_top
        if unknown_top:
            raise ServerConfigError(
                f"Unknown top-level configuration key(s): {sorted(unknown_top)}"
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
                    f"Connection '{eng_name_str}' missing required field 'kind'"
                )
            kind = kind.strip()
            if kind not in ("starrocks", "lakebase", "cube", "duckdb"):
                raise ServerConfigError(
                    f"Unsupported connection kind '{_safe_label(kind)}' for engine '{_safe_label(eng_name_str)}'"
                )

            if kind == "starrocks":
                unknown_sr = set(eng_spec.keys()) - {"kind", "accounts"}
                if unknown_sr:
                    raise ServerConfigError(
                        f"Unknown key(s) in starrocks connection '{eng_name_str}': {sorted(unknown_sr)}"
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
                connections[eng_name_str] = StarRocksConnectionConfig(accounts=accounts)

            elif kind == "lakebase":
                unknown_lb = set(eng_spec.keys()) - {"kind", "dsn_env"}
                if unknown_lb:
                    raise ServerConfigError(
                        f"Unknown key(s) in lakebase connection '{eng_name_str}': {sorted(unknown_lb)}"
                    )
                dsn_env = eng_spec.get("dsn_env")
                if not dsn_env or not isinstance(dsn_env, str) or not dsn_env.strip():
                    raise ServerConfigError(
                        f"Lakebase connection '{eng_name_str}' missing required 'dsn_env'"
                    )
                connections[eng_name_str] = LakebaseConnectionConfig(
                    dsn_env=dsn_env.strip()
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
                }
                if unknown_cube:
                    raise ServerConfigError(
                        f"Unknown key(s) in cube connection '{eng_name_str}': {sorted(unknown_cube)}"
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
                )

            elif kind == "duckdb":
                unknown_duck = set(eng_spec.keys()) - {
                    "kind",
                    "path",
                    "allowed_directories",
                    "memory_limit",
                    "threads",
                }
                if unknown_duck:
                    raise ServerConfigError(
                        f"Unknown key(s) in duckdb connection '{eng_name_str}': {sorted(unknown_duck)}"
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

                connections[eng_name_str] = DuckDBConnectionConfig(
                    path=path.strip(),
                    allowed_directories=tuple(validated_allowed),
                    memory_limit=mem_limit,
                    threads=threads,
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
        )

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
                data = yaml.safe_load(f)
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
            raw = yaml.safe_load(f)
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

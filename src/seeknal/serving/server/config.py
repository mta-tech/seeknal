"""Server configuration model and validation for Seeknal Serving."""

from __future__ import annotations

import base64
import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.discovery.config import DiscoveryConfig
from seeknal.serving.grant_token import _load_public_key


class ServerConfigError(Exception):
    """Raised when server configuration is invalid or missing."""


def _b64url_decode(s: str) -> bytes:
    rem = len(s) % 4
    padded = s + ("=" * ((4 - rem) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def parse_jwks(data_or_str: dict | list | str) -> dict[str, str]:
    """Parse JWKS into a mapping of kid to PEM-encoded EC P-256 public key string."""
    if isinstance(data_or_str, str):
        try:
            raw = json.loads(data_or_str)
        except Exception as exc:
            raise ServerConfigError(f"Invalid JSON in JWKS: {exc}") from exc
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
                f"JWK '{kid_str}' invalid base64url coordinate: {exc}"
            ) from exc

        if len(x_bytes) != 32 or len(y_bytes) != 32:
            raise ServerConfigError(
                f"JWK '{kid_str}' coordinates must each be 32 bytes for P-256"
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
                f"Failed to load EC public key for kid '{kid_str}': {exc}"
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
        raise ServerConfigError(f"Invalid grant_keys configuration: {grant_keys}")

    if cfg.jwks_file:
        if not os.path.exists(cfg.jwks_file):
            raise ServerConfigError(f"JWKS file '{cfg.jwks_file}' not found")
        with open(cfg.jwks_file, "r", encoding="utf-8") as f:
            return parse_jwks(f.read())
    elif cfg.pem_files:
        result: dict[str, str] = {}
        for kid, path in cfg.pem_files.items():
            if not os.path.exists(path):
                raise ServerConfigError(f"PEM file for kid '{kid}' not found: '{path}'")
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            try:
                _load_public_key(content, kid)
            except Exception as exc:
                raise ServerConfigError(
                    f"Invalid PEM public key for kid '{kid}': {exc}"
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
                raise ServerConfigError(
                    f"'listen.port' must be an integer between 1 and 65535, got {port}"
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
                raise ServerConfigError(f"Invalid log level '{level}'")
            fmt = str(log_raw.get("format", "json")).lower()
            if fmt not in {"json", "text"}:
                raise ServerConfigError(
                    f"Invalid log format '{fmt}', must be 'json' or 'text'"
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
                f"Invalid cube_mode '{cube_mode}', must be 'refuse' or 'jwt'"
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
                    f"Unsupported connection kind '{kind}' for engine '{eng_name_str}'"
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
                unknown_duck = set(eng_spec.keys()) - {"kind", "path"}
                if unknown_duck:
                    raise ServerConfigError(
                        f"Unknown key(s) in duckdb connection '{eng_name_str}': {sorted(unknown_duck)}"
                    )
                path = eng_spec.get("path")
                if not path or not isinstance(path, str) or not path.strip():
                    raise ServerConfigError(
                        f"DuckDB connection '{eng_name_str}' missing required 'path'"
                    )
                connections[eng_name_str] = DuckDBConnectionConfig(path=path.strip())

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
            raise ServerConfigError(f"Invalid YAML in config file: {exc}") from exc
        except Exception as exc:
            raise ServerConfigError(f"Failed to read config file: {exc}") from exc

        if not isinstance(data, Mapping):
            raise ServerConfigError(
                f"Config file must contain a mapping, got {type(data).__name__}"
            )
        return cls.from_dict(data, load_keys=load_keys, env=env)

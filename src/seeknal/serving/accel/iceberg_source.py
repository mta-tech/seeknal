"""Read-only Lakekeeper Iceberg REST catalog source for Seeknal Serving accel."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
from typing import Any, Callable, Optional, Union

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore[assignment]

from seeknal.serving.accel.engines import (
    DuckDBTarget,
    check_duckdb_version,
    create_target_factory,
)
from seeknal.serving.accel.loader import (
    BadInputError,
    LoadError,
    _split_target_table_name,
    build_select_columns_clause,
    register_secret_env_vars,
    register_secret_values,
    scrub_text,
)
from seeknal.serving.accel.settings import (
    DEFAULT_PII_MODE,
    get_extension_dir,
    validate_memory_limit,
    validate_pii_mode,
    validate_threads,
)
from seeknal.serving.accel.tables import (
    DEFAULT_LIVE_VIEWS,
    ColumnSpec,
    TableSpec,
    _load_json_data,
    build_table_specs,
    map_type,
    resolve_column_pii,
    validate_duckdb_type,
    validate_identifier,
    validate_snapshot_id,
)

_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_EXT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_S3_PREFIX_RE = re.compile(r"^s3://[A-Za-z0-9._-]+/.+/$")
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")
_SUSPICIOUS_TOKEN_RE = re.compile(
    r"(://|@|secret|password|passwd|token|bearer|private|canary|sentinel|opaque|dsn|credential)",
    re.IGNORECASE,
)
_UNQUOTED_SEG_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")
_STANDIN_AT_VERSION_RE = re.compile(
    r"\b([a-z][a-z0-9_]{0,31})((?:\.(?:[a-zA-Z_][a-zA-Z0-9_]*|\"[^\"]+\")){2,})\s+AT\s*\(\s*VERSION\s*=>\s*(\d+)\s*\)"
)

_RESERVED_ALIASES = frozenset({"system", "temp", "memory", "main"})
_ALLOWED_ICEBERG_EXTENSIONS = frozenset({"httpfs", "avro", "iceberg"})
_DEFAULT_EXTENSIONS = ("httpfs", "avro", "iceberg")
_DEFAULT_ICEBERG_EXTENSIONS = _DEFAULT_EXTENSIONS
_STANDIN_BUILTIN_EXTENSIONS = ("json", "parquet")


def _normalize_iceberg_extensions(exts: Iterable[str]) -> tuple[str, ...]:
    """Deduplicate extensions and ensure 'avro' is always loaded BEFORE 'iceberg'."""
    deduped: list[str] = []
    for ext in exts:
        if ext not in deduped:
            deduped.append(ext)
    if "iceberg" in deduped:
        if "avro" not in deduped:
            ice_idx = deduped.index("iceberg")
            deduped.insert(ice_idx, "avro")
        else:
            avro_idx = deduped.index("avro")
            ice_idx = deduped.index("iceberg")
            if avro_idx > ice_idx:
                deduped.pop(avro_idx)
                ice_idx = deduped.index("iceberg")
                deduped.insert(ice_idx, "avro")
    return tuple(deduped)

_FORBIDDEN_CATALOG_SECRET_KEYS = frozenset(
    {
        "client_id",
        "client_secret",
        "token",
        "secret",
        "password",
        "passwd",
        "bearer",
        "credential",
        "credentials",
    }
)
_FORBIDDEN_S3_SECRET_KEYS = frozenset(
    {
        "key_id",
        "secret",
        "access_key",
        "secret_key",
        "aws_access_key_id",
        "aws_secret_access_key",
        "password",
        "passwd",
        "token",
        "session_token",
    }
)


def _safe_label(value: Any, fallback: str = "<redacted>") -> str:
    """Return a secret-safe diagnostic label."""
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    s = value.strip()
    if not s:
        return "<empty>"
    if _SUSPICIOUS_TOKEN_RE.search(s) or not _SAFE_LABEL_RE.fullmatch(s):
        return fallback
    return s


def quote_sql_string(value: str) -> str:
    """Safely quote a SQL string literal using doubled single quotes."""
    if not isinstance(value, str):
        raise BadInputError(
            f"Expected string for SQL literal quoting, got {type(value).__name__}"
        )
    if "\x00" in value:
        raise BadInputError("Null byte not permitted in SQL string literal")
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def validate_catalog_alias(alias: str) -> str:
    """Validate Iceberg catalog alias per iceberg_direct rules."""
    if not isinstance(alias, str) or not _ALIAS_RE.fullmatch(alias):
        raise BadInputError(
            f"Catalog alias '{_safe_label(alias)}' must match ^[a-z][a-z0-9_]{{0,31}}$"
        )
    if alias in _RESERVED_ALIASES:
        raise BadInputError(f"Catalog alias '{alias}' is reserved")
    return alias


def validate_env_var_name(var_name: Any, field_label: str) -> str:
    """Validate that a secret reference is an environment variable name, never a literal."""
    if not isinstance(var_name, str) or not var_name.strip():
        raise BadInputError(
            f"'{field_label}' must be a valid environment variable name"
        )
    cleaned = var_name.strip()
    if not _ENV_VAR_NAME_RE.fullmatch(cleaned):
        raise BadInputError(
            f"'{field_label}' must be a valid environment variable name"
        )
    return cleaned


def normalize_extension_directory(path: Any) -> str:
    """Validate extension_directory is a non-empty, absolute, normalized path."""
    if not isinstance(path, (str, Path)):
        raise BadInputError("extension_directory must be a non-empty absolute path")
    raw = str(path).strip()
    if not raw:
        raise BadInputError("extension_directory must be a non-empty absolute path")
    if any(c in raw for c in ("\x00", "\n", "\r")):
        raise BadInputError(
            "extension_directory must not contain control or null characters"
        )
    if not os.path.isabs(raw):
        raise BadInputError("extension_directory must be an absolute path")
    norm = os.path.normpath(raw)
    if raw.rstrip("/") != norm.rstrip("/"):
        raise BadInputError("extension_directory must be a normalized path")
    return "/" if norm == "/" else norm.rstrip("/")


def _check_staged_avro_for_iceberg(
    ext_dir: Optional[str],
    extensions: Iterable[str],
) -> None:
    """Fail early if `iceberg.duckdb_extension` is staged in `ext_dir` without `avro.duckdb_extension`."""
    if not ext_dir or "iceberg" not in extensions:
        return
    root = Path(ext_dir)
    if not root.is_dir():
        return
    for iceberg_ext_file in sorted(root.rglob("iceberg.duckdb_extension")):
        if iceberg_ext_file.exists():
            avro_sibling = iceberg_ext_file.parent / "avro.duckdb_extension"
            if not avro_sibling.exists():
                raise BadInputError(
                    f"avro.duckdb_extension is missing from extension_directory '{ext_dir}' "
                    f"(found '{iceberg_ext_file}' without sibling 'avro.duckdb_extension')"
                )


def check_and_parse_secrets_env_file(path: Union[str, Path]) -> dict[str, str]:
    """Parse secrets_env_file without shell expansion, requiring mode 0600 or stricter."""
    p = str(path).strip()
    if not os.path.exists(p):
        raise BadInputError(f"secrets_env_file not found: '{p}'")
    st = os.stat(p)
    if (st.st_mode & 0o077) != 0:
        raise BadInputError(
            f"secrets_env_file '{p}' must be mode 0600 or stricter"
        )

    with open(p, "r", encoding="utf-8") as f:
        content = f.read()

    result: dict[str, str] = {}
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        k, v = stripped.split("=", 1)
        k = k.strip()
        v = v.strip()
        if (v.startswith('"') and v.endswith('"')) or (
            v.startswith("'") and v.endswith("'")
        ):
            v = v[1:-1]
        if k:
            result[k] = v
    return result


def _split_qualified_segments(ref: str) -> list[str]:
    """Split a dot-qualified SQL reference supporting double-quoted identifier segments."""
    if not isinstance(ref, str) or not ref.strip():
        raise BadInputError("Empty Iceberg table reference")
    s = ref.strip()
    if any(c in s for c in ("\x00", "\n", "\r", ";")):
        raise BadInputError("Invalid characters in Iceberg table reference")

    segments: list[str] = []
    cur: list[str] = []
    in_quotes = False
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if in_quotes:
            if ch == '"':
                if i + 1 < n and s[i + 1] == '"':
                    cur.append('""')
                    i += 2
                    continue
                in_quotes = False
                cur.append('"')
                i += 1
                continue
            cur.append(ch)
            i += 1
        else:
            if ch == '"':
                if cur:
                    raise BadInputError(
                        f"Malformed quoted identifier in Iceberg table reference '{_safe_label(s)}'"
                    )
                in_quotes = True
                cur.append('"')
                i += 1
            elif ch == ".":
                seg = "".join(cur)
                if not seg:
                    raise BadInputError(
                        f"Empty identifier segment in Iceberg table reference '{_safe_label(s)}'"
                    )
                segments.append(seg)
                cur = []
                i += 1
            else:
                cur.append(ch)
                i += 1

    if in_quotes:
        raise BadInputError(
            f"Unterminated double quote in Iceberg table reference '{_safe_label(s)}'"
        )
    last_seg = "".join(cur)
    if not last_seg:
        raise BadInputError(
            f"Trailing dot in Iceberg table reference '{_safe_label(s)}'"
        )
    segments.append(last_seg)
    return segments


def validate_iceberg_table_ref(
    ref: str,
    *,
    allowed_aliases: Optional[Collection[str]] = None,
) -> tuple[str, str]:
    """Validate `<alias>.<namespace>[.<sub_ns>...].<table>` and return `(alias, canonical_ref)`."""
    segments = _split_qualified_segments(ref)
    if len(segments) < 3:
        raise BadInputError(
            f"Iceberg table reference '{_safe_label(ref)}' must have at least 3 parts: '<alias>.<namespace>.<table>'"
        )

    raw_first = segments[0]
    if raw_first.startswith('"') and raw_first.endswith('"'):
        unquoted_alias = raw_first[1:-1].replace('""', '"')
        alias = validate_catalog_alias(unquoted_alias)
    else:
        alias = validate_catalog_alias(raw_first)

    if allowed_aliases is not None and alias not in allowed_aliases:
        raise BadInputError(
            f"Iceberg table reference uses unknown catalog alias '{alias}'; expected one of {sorted(allowed_aliases)}"
        )

    validated_rest: list[str] = []
    for seg in segments[1:]:
        if seg.startswith('"') and seg.endswith('"'):
            inner = seg[1:-1]
            if not inner:
                raise BadInputError("Quoted identifier segment must not be empty")
            validated_rest.append(seg)
        else:
            if not _UNQUOTED_SEG_RE.fullmatch(seg):
                raise BadInputError(
                    f"Invalid identifier segment '{_safe_label(seg)}' in Iceberg table reference"
                )
            validated_rest.append(seg)

    first_rendered = raw_first if raw_first.startswith('"') else alias
    canonical = ".".join([first_rendered, *validated_rest])
    return alias, canonical


def format_iceberg_from_clause(
    source_ref: str,
    *,
    snapshot_id: Optional[int] = None,
    allowed_aliases: Optional[Collection[str]] = None,
) -> str:
    """Format validated Iceberg `FROM` target with optional `AT (VERSION => <snapshot_id>)`."""
    _, canonical_ref = validate_iceberg_table_ref(
        source_ref,
        allowed_aliases=allowed_aliases,
    )
    if snapshot_id is not None:
        snap = validate_snapshot_id(
            snapshot_id,
            context=f"Iceberg source '{canonical_ref}'",
        )
        return f"{canonical_ref} AT (VERSION => {snap})"
    return canonical_ref


@dataclass(frozen=True)
class IcebergSetupStep:
    """A single pure-planned DuckDB configuration statement for IcebergSource."""

    phase: str
    name: str
    sql: str = field(repr=False)
    redacted_sql: Optional[str] = None

    @property
    def safe_sql(self) -> str:
        return self.redacted_sql if self.redacted_sql is not None else self.sql


@dataclass(frozen=True)
class IcebergS3Spec:
    """S3 endpoint and credential env-var configuration for an Iceberg catalog."""

    endpoint: str
    region: str
    key_id_env: str
    secret_env: str
    url_style: str = "path"
    use_ssl: bool = True


@dataclass(frozen=True)
class IcebergCatalogSpec:
    """Lakekeeper Iceberg REST catalog configuration."""

    alias: str
    endpoint: str
    warehouse: str
    token_uri: str
    client_id_env: str
    client_secret_env: str
    s3: IcebergS3Spec
    access_delegation: str = "vended"
    scope: Optional[str] = None
    allow_insecure_http: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.allow_insecure_http, bool):
            raise BadInputError(
                f"Catalog '{self.alias}' 'allow_insecure_http' must be a boolean"
            )
        if self.endpoint.startswith("http://") and not self.allow_insecure_http:
            raise BadInputError(
                f"Catalog '{self.alias}' 'endpoint' uses insecure 'http://'; "
                "use 'https://' or set 'allow_insecure_http: true'"
            )
        if self.token_uri.startswith("http://") and not self.allow_insecure_http:
            raise BadInputError(
                f"Catalog '{self.alias}' 'token_uri' uses insecure 'http://'; "
                "use 'https://' or set 'allow_insecure_http: true'"
            )


AccelIcebergCatalogConfig = IcebergCatalogSpec


@dataclass(frozen=True)
class IcebergTableEntry:
    """Optional inline or override table entry in Iceberg source config."""

    view: str
    source: Optional[str] = None
    target_name: Optional[str] = None
    snapshot_id: Optional[int] = None
    keys: tuple[str, ...] = ()
    columns: Optional[tuple[dict[str, Any], ...]] = None
    live: bool = False


@dataclass(frozen=True)
class IcebergSourceConfig:
    """Validated Iceberg source configuration for accel."""

    iceberg_catalogs: Mapping[str, IcebergCatalogSpec]
    extension_directory: Optional[str] = None
    extensions: tuple[str, ...] = _DEFAULT_ICEBERG_EXTENSIONS
    allowed_s3_prefixes: tuple[str, ...] = ()
    secrets_env_file: Optional[str] = None
    secrets_env: Mapping[str, str] = field(default_factory=dict, repr=False)
    memory_limit: Optional[str] = None
    threads: Optional[int] = None
    pii_mode: Optional[str] = None
    default_catalog: Optional[str] = None
    default_namespace: Optional[str] = None
    target_schema: str = "rpt"
    tables: tuple[IcebergTableEntry, ...] = ()
    pii_columns: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized_exts = _normalize_iceberg_extensions(self.extensions)
        if tuple(self.extensions) != normalized_exts:
            object.__setattr__(self, "extensions", normalized_exts)

    def referenced_env_vars(self) -> tuple[str, ...]:
        vars_list: list[str] = []
        for cat in self.iceberg_catalogs.values():
            for v in (
                cat.client_id_env,
                cat.client_secret_env,
                cat.s3.key_id_env,
                cat.s3.secret_env,
            ):
                if v not in vars_list:
                    vars_list.append(v)
        return tuple(vars_list)

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
        *,
        extension_dir_override: Optional[str] = None,
    ) -> IcebergSourceConfig:
        if not isinstance(raw, Mapping):
            raise BadInputError(
                f"Iceberg source configuration must be a mapping, got {type(raw).__name__}"
            )

        for top_key in raw.keys():
            kl = str(top_key).lower()
            if (
                "postgres" in kl
                or "lakebase" in kl
                or kl in ("pg", "postgresql", "attach")
                or kl in _FORBIDDEN_CATALOG_SECRET_KEYS
                or kl in _FORBIDDEN_S3_SECRET_KEYS
            ):
                raise BadInputError(
                    f"Iceberg source configuration forbids key '{_safe_label(str(top_key))}'"
                )

        allowed_top = {
            "secrets_env_file",
            "extension_directory",
            "extensions",
            "allowed_s3_prefixes",
            "iceberg_catalogs",
            "catalogs",
            "memory_limit",
            "threads",
            "pii_mode",
            "default_catalog",
            "default_namespace",
            "target_schema",
            "tables",
            "pii_columns",
        }
        unknown_top = set(raw.keys()) - allowed_top
        if unknown_top:
            raise BadInputError(
                f"Unknown key(s) in Iceberg source configuration: "
                f"{sorted(_safe_label(str(k)) for k in unknown_top)}"
            )

        if "iceberg_catalogs" in raw and "catalogs" in raw:
            raise BadInputError(
                "Specify either 'iceberg_catalogs' or 'catalogs', not both"
            )

        secrets_env_file: Optional[str] = None
        parsed_secrets_env: dict[str, str] = {}
        raw_sec_file = raw.get("secrets_env_file")
        if raw_sec_file is not None:
            if not isinstance(raw_sec_file, str) or not raw_sec_file.strip():
                raise BadInputError("'secrets_env_file' must be a non-empty string")
            secrets_env_file = raw_sec_file.strip()
            parsed_secrets_env = check_and_parse_secrets_env_file(secrets_env_file)

        ext_dir_val: Optional[str] = None
        eff_ext_dir = (
            extension_dir_override
            if extension_dir_override is not None
            else raw.get("extension_directory")
        )
        if eff_ext_dir is not None:
            ext_dir_val = normalize_extension_directory(eff_ext_dir)

        raw_exts = raw.get("extensions")
        if raw_exts is None:
            validated_exts = _DEFAULT_ICEBERG_EXTENSIONS
        else:
            if not isinstance(raw_exts, (list, tuple)) or not raw_exts:
                raise BadInputError("'extensions' must be a non-empty list")
            ext_items: list[str] = []
            for item in raw_exts:
                if not isinstance(item, str) or not item.strip():
                    raise BadInputError("'extensions' entries must be non-empty strings")
                ext_clean = item.strip().lower()
                if ext_clean not in _ALLOWED_ICEBERG_EXTENSIONS:
                    raise BadInputError(
                        f"Extension '{_safe_label(ext_clean)}' is not in allowlist {{'httpfs', 'avro', 'iceberg'}}"
                    )
                if ext_clean not in ext_items:
                    ext_items.append(ext_clean)
            validated_exts = _normalize_iceberg_extensions(ext_items)

        if ext_dir_val is not None:
            _check_staged_avro_for_iceberg(ext_dir_val, validated_exts)

        raw_prefixes = raw.get("allowed_s3_prefixes")
        validated_prefixes: list[str] = []
        if raw_prefixes is not None:
            if not isinstance(raw_prefixes, (list, tuple)):
                raise BadInputError("'allowed_s3_prefixes' must be a list of s3:// URIs")
            for pfx in raw_prefixes:
                if not isinstance(pfx, str) or not pfx.strip():
                    raise BadInputError(
                        "'allowed_s3_prefixes' entries must be non-empty strings"
                    )
                pfx_str = pfx.strip()
                if (
                    not _S3_PREFIX_RE.fullmatch(pfx_str)
                    or ".." in pfx_str
                    or "//" in pfx_str[len("s3://") :]
                ):
                    raise BadInputError(
                        f"Invalid S3 prefix '{_safe_label(pfx_str)}'; expected 's3://<bucket>/<prefix>/'"
                    )
                validated_prefixes.append(pfx_str)

        cats_raw = raw.get("iceberg_catalogs")
        if cats_raw is None:
            cats_raw = raw.get("catalogs")
        if not isinstance(cats_raw, Mapping) or not cats_raw:
            raise BadInputError("Requires non-empty 'iceberg_catalogs' mapping")

        parsed_catalogs: dict[str, IcebergCatalogSpec] = {}
        for raw_alias, cat_spec in cats_raw.items():
            alias = validate_catalog_alias(str(raw_alias))
            if not isinstance(cat_spec, Mapping):
                raise BadInputError(f"Catalog '{alias}' must be a mapping")

            for raw_ck in cat_spec.keys():
                ck_lower = str(raw_ck).lower()
                if (
                    "postgres" in ck_lower
                    or "lakebase" in ck_lower
                    or ck_lower in _FORBIDDEN_CATALOG_SECRET_KEYS
                ):
                    raise BadInputError(
                        f"Catalog '{alias}' forbids literal secret or Postgres key "
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
                "allow_insecure_http",
                "s3",
            }
            if unknown_cat:
                raise BadInputError(
                    f"Unknown key(s) in iceberg_catalogs.{alias}: "
                    f"{sorted(_safe_label(str(k)) for k in unknown_cat)}"
                )

            allow_insecure_http = cat_spec.get("allow_insecure_http", False)
            if not isinstance(allow_insecure_http, bool):
                raise BadInputError(
                    f"Catalog '{alias}' 'allow_insecure_http' must be a boolean"
                )

            endpoint = cat_spec.get("endpoint")
            if not isinstance(endpoint, str) or not endpoint.strip():
                raise BadInputError(f"Catalog '{alias}' missing required 'endpoint'")
            endpoint = endpoint.strip()
            if not (
                endpoint.startswith("http://") or endpoint.startswith("https://")
            ):
                raise BadInputError(
                    f"Catalog '{alias}' 'endpoint' must start with http:// or https://"
                )
            if endpoint.startswith("http://") and not allow_insecure_http:
                raise BadInputError(
                    f"Catalog '{alias}' 'endpoint' uses insecure 'http://'; "
                    "use 'https://' or set 'allow_insecure_http: true'"
                )

            warehouse = cat_spec.get("warehouse")
            if not isinstance(warehouse, str) or not warehouse.strip():
                raise BadInputError(f"Catalog '{alias}' missing required 'warehouse'")
            warehouse = warehouse.strip()

            token_uri = cat_spec.get("token_uri")
            if not isinstance(token_uri, str) or not token_uri.strip():
                raise BadInputError(f"Catalog '{alias}' missing required 'token_uri'")
            token_uri = token_uri.strip()
            if not (
                token_uri.startswith("http://") or token_uri.startswith("https://")
            ):
                raise BadInputError(
                    f"Catalog '{alias}' 'token_uri' must start with http:// or https://"
                )
            if token_uri.startswith("http://") and not allow_insecure_http:
                raise BadInputError(
                    f"Catalog '{alias}' 'token_uri' uses insecure 'http://'; "
                    "use 'https://' or set 'allow_insecure_http: true'"
                )

            cid_env = validate_env_var_name(
                cat_spec.get("client_id_env"),
                f"iceberg_catalogs.{alias}.client_id_env",
            )
            csec_env = validate_env_var_name(
                cat_spec.get("client_secret_env"),
                f"iceberg_catalogs.{alias}.client_secret_env",
            )

            scope_val = cat_spec.get("scope")
            if scope_val is not None:
                if not isinstance(scope_val, str) or not scope_val.strip():
                    raise BadInputError(
                        f"Catalog '{alias}' 'scope' must be a non-empty string"
                    )
                scope_val = scope_val.strip()

            access_del_raw = cat_spec.get("access_delegation", "vended")
            if not isinstance(access_del_raw, str) or access_del_raw.strip() not in (
                "vended",
                "none",
            ):
                raise BadInputError(
                    f"Catalog '{alias}' 'access_delegation' must be 'vended' or 'none'"
                )
            access_del = access_del_raw.strip()

            s3_raw = cat_spec.get("s3")
            if not isinstance(s3_raw, Mapping):
                raise BadInputError(f"Catalog '{alias}' missing required 's3' mapping")
            for raw_sk in s3_raw.keys():
                sk_lower = str(raw_sk).lower()
                if sk_lower in _FORBIDDEN_S3_SECRET_KEYS:
                    raise BadInputError(
                        f"Catalog '{alias}.s3' forbids literal secret key "
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
                raise BadInputError(
                    f"Unknown key(s) in iceberg_catalogs.{alias}.s3: "
                    f"{sorted(_safe_label(str(k)) for k in unknown_s3)}"
                )

            s3_endpoint = s3_raw.get("endpoint")
            if not isinstance(s3_endpoint, str) or not s3_endpoint.strip():
                raise BadInputError(
                    f"Catalog '{alias}.s3' missing required 'endpoint'"
                )
            s3_endpoint = s3_endpoint.strip()

            s3_region = s3_raw.get("region")
            if not isinstance(s3_region, str) or not s3_region.strip():
                raise BadInputError(f"Catalog '{alias}.s3' missing required 'region'")
            s3_region = s3_region.strip()

            s3_url_style = s3_raw.get("url_style", "path")
            if not isinstance(s3_url_style, str) or s3_url_style.strip() not in (
                "path",
                "vhost",
            ):
                raise BadInputError(
                    f"Catalog '{alias}.s3' 'url_style' must be 'path' or 'vhost'"
                )
            s3_url_style = s3_url_style.strip()

            s3_key_env = validate_env_var_name(
                s3_raw.get("key_id_env"),
                f"iceberg_catalogs.{alias}.s3.key_id_env",
            )
            s3_sec_env = validate_env_var_name(
                s3_raw.get("secret_env"),
                f"iceberg_catalogs.{alias}.s3.secret_env",
            )

            s3_use_ssl = s3_raw.get("use_ssl", True)
            if not isinstance(s3_use_ssl, bool):
                raise BadInputError(
                    f"Catalog '{alias}.s3' 'use_ssl' must be a boolean"
                )

            parsed_catalogs[alias] = IcebergCatalogSpec(
                alias=alias,
                endpoint=endpoint,
                warehouse=warehouse,
                token_uri=token_uri,
                client_id_env=cid_env,
                client_secret_env=csec_env,
                scope=scope_val,
                access_delegation=access_del,
                allow_insecure_http=allow_insecure_http,
                s3=IcebergS3Spec(
                    endpoint=s3_endpoint,
                    region=s3_region,
                    url_style=s3_url_style,
                    key_id_env=s3_key_env,
                    secret_env=s3_sec_env,
                    use_ssl=s3_use_ssl,
                ),
            )

        mem_limit: Optional[str] = None
        if raw.get("memory_limit") is not None:
            try:
                mem_limit = validate_memory_limit(str(raw["memory_limit"]))
            except ValueError as exc:
                raise BadInputError(str(exc)) from exc

        threads: Optional[int] = None
        if raw.get("threads") is not None:
            try:
                threads = validate_threads(raw["threads"])
            except ValueError as exc:
                raise BadInputError(str(exc)) from exc

        cfg_pii_mode: Optional[str] = None
        if "pii_mode" in raw and raw["pii_mode"] is not None:
            try:
                cfg_pii_mode = validate_pii_mode(raw["pii_mode"])
            except ValueError as exc:
                raise BadInputError(str(exc)) from exc

        target_schema = raw.get("target_schema", "rpt")
        try:
            validate_identifier(target_schema)
        except ValueError as exc:
            raise BadInputError(str(exc)) from exc

        default_catalog = raw.get("default_catalog")
        if default_catalog is not None:
            default_catalog = validate_catalog_alias(str(default_catalog))
            if default_catalog not in parsed_catalogs:
                raise BadInputError(
                    f"'default_catalog' '{default_catalog}' is not defined in 'iceberg_catalogs'"
                )

        default_namespace = raw.get("default_namespace")
        if default_namespace is not None:
            if not isinstance(default_namespace, str) or not default_namespace.strip():
                raise BadInputError("'default_namespace' must be a non-empty string")
            default_namespace = default_namespace.strip()

        parsed_tables = _parse_tables_section(
            raw.get("tables"),
            allowed_aliases=set(parsed_catalogs.keys()),
        )

        pii_cols_raw = raw.get("pii_columns") or {}
        if not isinstance(pii_cols_raw, Mapping):
            raise BadInputError("'pii_columns' must be a mapping")

        referenced_vars: set[str] = set()
        for c in parsed_catalogs.values():
            referenced_vars.update(
                (
                    c.client_id_env,
                    c.client_secret_env,
                    c.s3.key_id_env,
                    c.s3.secret_env,
                )
            )
        register_secret_env_vars(*referenced_vars)

        scoped_secrets_env = {
            k: v for k, v in parsed_secrets_env.items() if k in referenced_vars
        }
        if scoped_secrets_env:
            register_secret_values(*scoped_secrets_env.values())

        return cls(
            iceberg_catalogs=parsed_catalogs,
            extension_directory=ext_dir_val,
            extensions=validated_exts,
            allowed_s3_prefixes=tuple(validated_prefixes),
            secrets_env_file=secrets_env_file,
            secrets_env=scoped_secrets_env,
            memory_limit=mem_limit,
            threads=threads,
            pii_mode=cfg_pii_mode,
            default_catalog=default_catalog,
            default_namespace=default_namespace,
            target_schema=target_schema,
            tables=tuple(parsed_tables),
            pii_columns=dict(pii_cols_raw),
        )


def _parse_tables_section(
    raw_tables: Any,
    *,
    allowed_aliases: Collection[str],
) -> list[IcebergTableEntry]:
    if raw_tables is None:
        return []

    items: list[tuple[str, Mapping[str, Any]]] = []
    if isinstance(raw_tables, Mapping):
        for view_key, item_val in raw_tables.items():
            if isinstance(item_val, str):
                items.append((str(view_key), {"source": item_val}))
            elif isinstance(item_val, Mapping):
                items.append((str(view_key), item_val))
            else:
                raise BadInputError(
                    f"Invalid table entry for '{_safe_label(str(view_key))}'"
                )
    elif isinstance(raw_tables, (list, tuple)):
        for idx, entry in enumerate(raw_tables):
            if not isinstance(entry, Mapping):
                raise BadInputError(f"tables[{idx}] must be a mapping")
            view_name = entry.get("view") or entry.get("name")
            if not isinstance(view_name, str) or not view_name.strip():
                raise BadInputError(f"tables[{idx}] missing required 'view'")
            items.append((view_name.strip(), entry))
    else:
        raise BadInputError("'tables' must be a list or mapping")

    parsed: list[IcebergTableEntry] = []
    for view_name, entry in items:
        try:
            validate_identifier(view_name)
        except ValueError as exc:
            raise BadInputError(str(exc)) from exc

        unknown_entry = set(entry.keys()) - {
            "view",
            "name",
            "source",
            "target",
            "snapshot_id",
            "keys",
            "columns",
            "live",
        }
        if unknown_entry:
            raise BadInputError(
                f"Unknown key(s) in table '{view_name}': "
                f"{sorted(_safe_label(str(k)) for k in unknown_entry)}"
            )

        src = entry.get("source")
        if src is not None:
            if not isinstance(src, str) or not src.strip():
                raise BadInputError(
                    f"Table '{view_name}' 'source' must be a non-empty string"
                )
            _, src = validate_iceberg_table_ref(
                src.strip(),
                allowed_aliases=allowed_aliases,
            )

        target_name = entry.get("target")
        if target_name is not None:
            if not isinstance(target_name, str) or "." not in target_name:
                raise BadInputError(
                    f"Table '{view_name}' 'target' must have format '<schema>.<table>'"
                )
            t_parts = target_name.split(".")
            if len(t_parts) != 2:
                raise BadInputError(
                    f"Table '{view_name}' 'target' must have format '<schema>.<table>'"
                )
            try:
                validate_identifier(t_parts[0])
                validate_identifier(t_parts[1])
            except ValueError as exc:
                raise BadInputError(str(exc)) from exc

        snap_id: Optional[int] = None
        if entry.get("snapshot_id") is not None:
            try:
                snap_id = validate_snapshot_id(
                    entry["snapshot_id"],
                    context=f"Table '{view_name}'",
                )
            except ValueError as exc:
                raise BadInputError(str(exc)) from exc

        keys_raw = entry.get("keys") or ()
        if not isinstance(keys_raw, (list, tuple)):
            raise BadInputError(f"Table '{view_name}' 'keys' must be a list")
        keys_list: list[str] = []
        for k in keys_raw:
            try:
                validate_identifier(str(k))
            except ValueError as exc:
                raise BadInputError(str(exc)) from exc
            keys_list.append(str(k))

        cols_tuple: Optional[tuple[dict[str, Any], ...]] = None
        if "columns" in entry and entry["columns"] is not None:
            raw_cols = entry["columns"]
            if not isinstance(raw_cols, (list, tuple)) or not raw_cols:
                raise BadInputError(
                    f"Table '{view_name}' 'columns' must be a non-empty list"
                )
            cols_list: list[dict[str, Any]] = []
            for c in raw_cols:
                if not isinstance(c, Mapping):
                    raise BadInputError(
                        f"Column entry in table '{view_name}' must be a mapping"
                    )
                cols_list.append(dict(c))
            cols_tuple = tuple(cols_list)

        live_flag = entry.get("live", False)
        if not isinstance(live_flag, bool):
            raise BadInputError(f"Table '{view_name}' 'live' must be a boolean")

        parsed.append(
            IcebergTableEntry(
                view=view_name,
                source=src,
                target_name=target_name,
                snapshot_id=snap_id,
                keys=tuple(keys_list),
                columns=cols_tuple,
                live=live_flag,
            )
        )
    return parsed


def load_iceberg_source_config(
    path_or_data: Union[str, Path, Mapping[str, Any]],
    *,
    extension_dir: Optional[str] = None,
) -> IcebergSourceConfig:
    """Load and validate Iceberg source configuration from JSON/YAML file or mapping."""
    if isinstance(path_or_data, Mapping):
        return IcebergSourceConfig.from_dict(
            path_or_data,
            extension_dir_override=extension_dir,
        )

    p = Path(path_or_data)
    if not p.exists():
        raise BadInputError(f"Iceberg source config file not found: '{p}'")

    suffix = p.suffix.lower()
    with open(p, "r", encoding="utf-8") as f:
        text = f.read()

    if suffix in (".yaml", ".yml"):
        import yaml

        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            mark = getattr(exc, "problem_mark", None)
            if mark is not None:
                raise BadInputError(
                    f"Invalid YAML in Iceberg source config '{p}' at line {mark.line + 1}, column {mark.column + 1}"
                ) from exc
            raise BadInputError(
                f"Invalid YAML in Iceberg source config '{p}'"
            ) from exc
    else:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise BadInputError(
                f"Invalid JSON in Iceberg source config '{p}' at line {exc.lineno}, column {exc.colno}"
            ) from exc

    if not isinstance(data, Mapping):
        raise BadInputError(
            f"Iceberg source config '{p}' must contain a top-level mapping"
        )
    return IcebergSourceConfig.from_dict(
        data,
        extension_dir_override=extension_dir,
    )


def _cfg_attr(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _resolve_secret(
    var_name: str,
    cfg: Any,
    env: Optional[Mapping[str, str]] = None,
) -> str:
    """Resolve a secret strictly from secrets_env or environment variables."""
    cleaned = validate_env_var_name(var_name, "secret_env_var")
    register_secret_env_vars(cleaned)

    secrets_env = _cfg_attr(cfg, "secrets_env")
    if isinstance(secrets_env, Mapping) and cleaned in secrets_env:
        val = secrets_env[cleaned]
        if isinstance(val, str) and val:
            register_secret_values(val)
            return val

    proc_env = env if env is not None else os.environ
    if cleaned in proc_env and proc_env[cleaned]:
        val = str(proc_env[cleaned])
        register_secret_values(val)
        return val

    raise BadInputError(f"Missing required environment variable '{cleaned}'")


def _coerce_config(
    cfg: Union[IcebergSourceConfig, Mapping[str, Any]],
    *,
    extension_dir: Optional[str] = None,
) -> IcebergSourceConfig:
    if isinstance(cfg, IcebergSourceConfig):
        if extension_dir is not None:
            norm_ext = normalize_extension_directory(extension_dir)
            _check_staged_avro_for_iceberg(norm_ext, cfg.extensions)
            if norm_ext != cfg.extension_directory:
                return IcebergSourceConfig(
                    iceberg_catalogs=cfg.iceberg_catalogs,
                    extension_directory=norm_ext,
                    extensions=cfg.extensions,
                    allowed_s3_prefixes=cfg.allowed_s3_prefixes,
                    secrets_env_file=cfg.secrets_env_file,
                    secrets_env=cfg.secrets_env,
                    memory_limit=cfg.memory_limit,
                    threads=cfg.threads,
                    pii_mode=cfg.pii_mode,
                    default_catalog=cfg.default_catalog,
                    default_namespace=cfg.default_namespace,
                    target_schema=cfg.target_schema,
                    tables=cfg.tables,
                    pii_columns=cfg.pii_columns,
                )
        return cfg
    return IcebergSourceConfig.from_dict(cfg, extension_dir_override=extension_dir)


def plan_iceberg_source_setup(
    cfg: Union[IcebergSourceConfig, Mapping[str, Any]],
    *,
    extension_dir: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    standin_catalogs: Optional[Mapping[str, str]] = None,
    standin_extensions: Optional[Sequence[str]] = None,
) -> list[IcebergSetupStep]:
    """Build the strict-ordered DuckDB setup statements before querying an Iceberg catalog.

    Order:
      1. SET autoinstall_known_extensions = false
      2. SET autoload_known_extensions = false
      3. SET allow_persistent_secrets = false
      4. SET extension_directory = '<extension_dir>'
      5. SET custom_extension_repository = '/nonexistent_extension_repository'
      6. LOAD httpfs / LOAD avro / LOAD iceberg (avro ALWAYS before iceberg; NEVER INSTALL)
      7. CREATE TEMPORARY SECRET s3_<alias> ... and iceberg_<alias> ...
      8. ATTACH '<warehouse>' AS <alias> (TYPE ICEBERG, ..., READ_ONLY)
    """
    parsed = _coerce_config(cfg, extension_dir=extension_dir)

    raw_ext_dir = (
        extension_dir
        or parsed.extension_directory
        or get_extension_dir()
    )
    if not raw_ext_dir:
        raise BadInputError(
            "Iceberg source requires 'extension_directory' (via config, --extension-dir, "
            "ACCEL_EXTENSION_DIR, or SEEKNAL_EXTENSION_DIRECTORY)"
        )
    ext_dir_clean = normalize_extension_directory(raw_ext_dir)

    all_standin = bool(
        standin_catalogs is not None
        and parsed.iceberg_catalogs
        and all(alias in standin_catalogs for alias in parsed.iceberg_catalogs.keys())
    )
    if standin_extensions is not None:
        raw_standin_exts: list[str] = []
        for ext in standin_extensions:
            if not isinstance(ext, str) or not _EXT_NAME_RE.fullmatch(ext):
                raise BadInputError(f"Invalid extension name '{_safe_label(ext)}'")
            raw_standin_exts.append(ext)
        exts_to_load = list(_normalize_iceberg_extensions(raw_standin_exts))
    elif all_standin and tuple(parsed.extensions) == _DEFAULT_ICEBERG_EXTENSIONS:
        exts_to_load = list(_STANDIN_BUILTIN_EXTENSIONS)
    else:
        exts_to_load = list(_normalize_iceberg_extensions(parsed.extensions))

    _check_staged_avro_for_iceberg(ext_dir_clean, exts_to_load)

    steps: list[IcebergSetupStep] = [
        IcebergSetupStep(
            phase="extensions",
            name="set_autoinstall_off",
            sql="SET autoinstall_known_extensions = false",
        ),
        IcebergSetupStep(
            phase="extensions",
            name="set_autoload_off",
            sql="SET autoload_known_extensions = false",
        ),
        IcebergSetupStep(
            phase="extensions",
            name="set_allow_persistent_secrets_off",
            sql="SET allow_persistent_secrets = false",
        ),
        IcebergSetupStep(
            phase="extensions",
            name="set_extension_directory",
            sql=f"SET extension_directory = {quote_sql_string(ext_dir_clean)}",
        ),
        IcebergSetupStep(
            phase="extensions",
            name="set_custom_extension_repository",
            sql="SET custom_extension_repository = '/nonexistent_extension_repository'",
        ),
    ]

    for ext in exts_to_load:
        steps.append(
            IcebergSetupStep(
                phase="load",
                name=f"load_extension_{ext}",
                sql=f"LOAD {ext}",
            )
        )

    attach_steps: list[IcebergSetupStep] = []
    for raw_alias, cat in parsed.iceberg_catalogs.items():
        alias = validate_catalog_alias(raw_alias)
        client_id_val = _resolve_secret(cat.client_id_env, parsed, env)
        client_secret_val = _resolve_secret(cat.client_secret_env, parsed, env)
        s3_key_val = _resolve_secret(cat.s3.key_id_env, parsed, env)
        s3_sec_val = _resolve_secret(cat.s3.secret_env, parsed, env)

        use_ssl_sql = "true" if cat.s3.use_ssl else "false"
        scope_clause_s3 = ""
        if parsed.allowed_s3_prefixes:
            scope_items = ", ".join(
                quote_sql_string(p) for p in parsed.allowed_s3_prefixes
            )
            scope_clause_s3 = f", SCOPE [{scope_items}]"

        if standin_catalogs is not None and alias in standin_catalogs:
            s3_sql = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{"
                f"'X-S3-Key': {quote_sql_string(s3_key_val)}, "
                f"'X-S3-Secret': {quote_sql_string(s3_sec_val)}"
                f"}})"
            )
            s3_redacted = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{"
                f"'X-S3-Key': '<redacted>', "
                f"'X-S3-Secret': '<redacted>'"
                f"}})"
            )
            oauth_sql = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{"
                f"'X-Client-Id': {quote_sql_string(client_id_val)}, "
                f"'X-Client-Secret': {quote_sql_string(client_secret_val)}"
                f"}})"
            )
            oauth_redacted = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE HTTP, "
                f"EXTRA_HTTP_HEADERS MAP {{"
                f"'X-Client-Id': '<redacted>', "
                f"'X-Client-Secret': '<redacted>'"
                f"}})"
            )
        else:
            s3_sql = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE S3, "
                f"KEY_ID {quote_sql_string(s3_key_val)}, "
                f"SECRET {quote_sql_string(s3_sec_val)}, "
                f"ENDPOINT {quote_sql_string(cat.s3.endpoint)}, "
                f"REGION {quote_sql_string(cat.s3.region)}, "
                f"URL_STYLE {quote_sql_string(cat.s3.url_style)}, "
                f"USE_SSL {use_ssl_sql}"
                f"{scope_clause_s3}"
                f")"
            )
            s3_redacted = (
                f"CREATE TEMPORARY SECRET s3_{alias} ("
                f"TYPE S3, "
                f"KEY_ID '<redacted>', "
                f"SECRET '<redacted>', "
                f"ENDPOINT {quote_sql_string(cat.s3.endpoint)}, "
                f"REGION {quote_sql_string(cat.s3.region)}, "
                f"URL_STYLE {quote_sql_string(cat.s3.url_style)}, "
                f"USE_SSL {use_ssl_sql}"
                f"{scope_clause_s3}"
                f")"
            )

            oauth_scope_sql = (
                f", OAUTH2_SCOPE {quote_sql_string(cat.scope)}"
                if cat.scope
                else ""
            )
            oauth_sql = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE ICEBERG, "
                f"CLIENT_ID {quote_sql_string(client_id_val)}, "
                f"CLIENT_SECRET {quote_sql_string(client_secret_val)}, "
                f"OAUTH2_SERVER_URI {quote_sql_string(cat.token_uri)}"
                f"{oauth_scope_sql}"
                f")"
            )
            oauth_redacted = (
                f"CREATE TEMPORARY SECRET iceberg_{alias} ("
                f"TYPE ICEBERG, "
                f"CLIENT_ID '<redacted>', "
                f"CLIENT_SECRET '<redacted>', "
                f"OAUTH2_SERVER_URI {quote_sql_string(cat.token_uri)}"
                f"{oauth_scope_sql}"
                f")"
            )

        steps.append(
            IcebergSetupStep(
                phase="secrets",
                name=f"create_secret_s3_{alias}",
                sql=s3_sql,
                redacted_sql=s3_redacted,
            )
        )
        steps.append(
            IcebergSetupStep(
                phase="secrets",
                name=f"create_secret_iceberg_{alias}",
                sql=oauth_sql,
                redacted_sql=oauth_redacted,
            )
        )

        if standin_catalogs is not None and alias in standin_catalogs:
            standin_path = str(standin_catalogs[alias])
            attach_sql = (
                f"ATTACH {quote_sql_string(standin_path)} AS {alias} (READ_ONLY)"
            )
        else:
            del_mode = (
                "vended-credentials"
                if cat.access_delegation == "vended"
                else "none"
            )
            attach_sql = (
                f"ATTACH {quote_sql_string(cat.warehouse)} AS {alias} ("
                f"TYPE ICEBERG, "
                f"ENDPOINT {quote_sql_string(cat.endpoint)}, "
                f"SECRET iceberg_{alias}, "
                f"ACCESS_DELEGATION_MODE {quote_sql_string(del_mode)}, "
                f"READ_ONLY"
                f")"
            )

        attach_steps.append(
            IcebergSetupStep(
                phase="attach",
                name=f"attach_{alias}",
                sql=attach_sql,
            )
        )

    steps.extend(attach_steps)
    return steps


def plan_iceberg_load_sql(
    specs: Sequence[TableSpec],
    *,
    pii_mode: str = DEFAULT_PII_MODE,
    allowed_aliases: Optional[Collection[str]] = None,
) -> list[str]:
    """Return deterministic DuckDB DDL/CTAS statements for loading from Iceberg."""
    mode = validate_pii_mode(pii_mode)
    if not specs:
        return ["CREATE SCHEMA rpt"]

    statements: list[str] = []
    seen_schemas: set[str] = set()

    for spec in specs:
        validate_identifier(spec.view)
        schema_name, table_name = _split_target_table_name(spec)
        if schema_name not in seen_schemas:
            statements.append(f"CREATE SCHEMA {schema_name}")
            seen_schemas.add(schema_name)

        cols_clause = build_select_columns_clause(spec, pii_mode=mode)
        from_clause = format_iceberg_from_clause(
            spec.source,
            snapshot_id=spec.snapshot_id,
            allowed_aliases=allowed_aliases,
        )
        statements.append(
            f"CREATE TABLE {schema_name}.{table_name} AS SELECT {cols_clause} FROM {from_clause}"
        )

    return statements


def plan_iceberg_count_sql(
    table_or_spec: Union[str, TableSpec],
    *,
    snapshot_id: Optional[int] = None,
    allowed_aliases: Optional[Collection[str]] = None,
) -> str:
    """Return deterministic `SELECT COUNT(*)` verification SQL for an Iceberg table."""
    if isinstance(table_or_spec, TableSpec):
        source_ref = table_or_spec.source
        eff_snapshot = (
            snapshot_id if snapshot_id is not None else table_or_spec.snapshot_id
        )
    else:
        source_ref = table_or_spec
        eff_snapshot = snapshot_id

    from_clause = format_iceberg_from_clause(
        source_ref,
        snapshot_id=eff_snapshot,
        allowed_aliases=allowed_aliases,
    )
    return f"SELECT COUNT(*) FROM {from_clause}"


def plan_iceberg_snapshot_sql(
    table_or_spec: Union[str, TableSpec],
    *,
    allowed_aliases: Optional[Collection[str]] = None,
) -> str:
    """Return deterministic SQL query to resolve an Iceberg table's latest snapshot_id."""
    source_ref = (
        table_or_spec.source
        if isinstance(table_or_spec, TableSpec)
        else table_or_spec
    )
    _, canonical_ref = validate_iceberg_table_ref(
        source_ref,
        allowed_aliases=allowed_aliases,
    )
    return (
        f"SELECT snapshot_id FROM iceberg_snapshots({canonical_ref}) "
        "ORDER BY timestamp_ms DESC LIMIT 1"
    )


def execute_iceberg_source_setup(
    conn: Any,
    cfg: Union[IcebergSourceConfig, Mapping[str, Any]],
    *,
    extension_dir: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    standin_catalogs: Optional[Mapping[str, str]] = None,
    standin_extensions: Optional[Sequence[str]] = None,
) -> list[IcebergSetupStep]:
    """Execute `plan_iceberg_source_setup` on `conn` in strict order with secret-safe error handling."""
    steps = plan_iceberg_source_setup(
        cfg,
        extension_dir=extension_dir,
        env=env,
        standin_catalogs=standin_catalogs,
        standin_extensions=standin_extensions,
    )
    executed: list[IcebergSetupStep] = []
    for step in steps:
        try:
            conn.execute(step.sql)
            executed.append(step)
        except Exception as exc:
            safe_err = scrub_text(str(exc))
            raise LoadError(
                f"Iceberg setup failed at step '{step.name}' ({step.safe_sql}): {safe_err}"
            ) from None
    return executed


def _standin_snapshot_table_exists(conn: Any, alias: str, rest_path: str, snap_id: str) -> bool:
    """Check whether `<alias><rest_path>__snap_<snap_id>` exists in a stand-in DuckDB catalog."""
    candidate = f"{alias}{rest_path}__snap_{snap_id}"
    try:
        conn.execute(f"SELECT 1 FROM {candidate} LIMIT 0")
        return True
    except Exception:
        return False


def _make_standin_rewriter(
    conn: Any,
    standin_catalogs: Optional[Mapping[str, str]],
) -> Optional[Callable[[str], str]]:
    """Create a statement rewriter ONLY when offline standin_catalogs are used.

    In production (`standin_catalogs is None`), returns `None` so DuckDB executes
    `AT (VERSION => <snapshot_id>)` natively against the Iceberg catalog.
    In offline stand-in mode, resolves `<alias>.<ns>.<table> AT (VERSION => <id>)`
    to `<alias>.<ns>.<table>__snap_<id>` if present in the stand-in DB, or `<alias>.<ns>.<table>`.
    """
    if not standin_catalogs:
        return None

    def rewrite(stmt: str) -> str:
        def _repl(match: re.Match[str]) -> str:
            alias = match.group(1)
            rest_path = match.group(2)
            snap_id = match.group(3)
            if alias not in standin_catalogs:
                return match.group(0)
            if _standin_snapshot_table_exists(conn, alias, rest_path, snap_id):
                return f"{alias}{rest_path}__snap_{snap_id}"
            return f"{alias}{rest_path}"

        return _STANDIN_AT_VERSION_RE.sub(_repl, stmt)

    return rewrite


def build_iceberg_table_specs(
    config: Union[IcebergSourceConfig, Mapping[str, Any]],
    *,
    columns: Optional[Union[dict, list, str, Path]] = None,
    pii_policy: Optional[Union[dict, str, Path]] = None,
    registry: Optional[Union[dict, list, str, Path]] = None,
    live_views: Optional[Iterable[str]] = DEFAULT_LIVE_VIEWS,
    allow_non_hashed_pii: bool = False,
    pii_mode: Optional[str] = None,
    i_understand_pii_leaves_unmasked: bool = False,
) -> list[TableSpec]:
    """Build `TableSpec` list for Iceberg source from either external metadata or inline `tables`."""
    if allow_non_hashed_pii and not i_understand_pii_leaves_unmasked:
        raise BadInputError(
            "--allow-non-hashed-pii requires --i-understand-pii-leaves-unmasked "
            "(i_understand_pii_leaves_unmasked=True)"
        )
    cfg = _coerce_config(config)
    effective_mode = validate_pii_mode(
        pii_mode if pii_mode is not None else (cfg.pii_mode or DEFAULT_PII_MODE)
    )
    allowed_aliases = set(cfg.iceberg_catalogs.keys())

    table_entry_by_view = {t.view: t for t in cfg.tables}
    live_set = set(live_views) if live_views is not None else set(DEFAULT_LIVE_VIEWS)
    for t_entry in cfg.tables:
        if t_entry.live:
            live_set.add(t_entry.view)

    def _resolve_iceberg_source_for_view(
        view_name: str, existing_source: Optional[str] = None
    ) -> str:
        entry = table_entry_by_view.get(view_name)
        if entry is not None and entry.source:
            _, canonical = validate_iceberg_table_ref(
                entry.source, allowed_aliases=allowed_aliases
            )
            return canonical
        if existing_source and not existing_source.startswith("rpt_base."):
            _, canonical = validate_iceberg_table_ref(
                existing_source, allowed_aliases=allowed_aliases
            )
            return canonical
        if cfg.default_namespace:
            cat_alias = cfg.default_catalog
            if cat_alias is None:
                if len(cfg.iceberg_catalogs) == 1:
                    cat_alias = next(iter(cfg.iceberg_catalogs.keys()))
                else:
                    raise BadInputError(
                        f"Multiple catalogs configured; specify 'default_catalog' or explicit 'source' for table '{view_name}'"
                    )
            candidate = f"{cat_alias}.{cfg.default_namespace}.{view_name}"
            _, canonical = validate_iceberg_table_ref(
                candidate, allowed_aliases=allowed_aliases
            )
            return canonical
        raise BadInputError(
            f"Table '{view_name}' has no Iceberg source mapping; specify 'tables' entry with 'source' or 'default_namespace'"
        )

    if columns is not None:
        if registry is None:
            raise BadInputError("'--registry' is required when '--columns' is provided")
        eff_pii: Any = pii_policy if pii_policy is not None else cfg.pii_columns
        snapshot_pins = {
            t.view: t.snapshot_id
            for t in cfg.tables
            if t.snapshot_id is not None
        }
        source_overrides = {
            t.view: t.source for t in cfg.tables if t.source is not None
        }
        base_specs = build_table_specs(
            columns=columns,
            pii_policy=eff_pii,
            registry=registry,
            live_views=live_set,
            allow_non_hashed_pii=allow_non_hashed_pii,
            pii_mode=effective_mode,
            snapshot_pins=snapshot_pins,
            source_overrides=source_overrides,
            target_schema=cfg.target_schema,
            i_understand_pii_leaves_unmasked=i_understand_pii_leaves_unmasked,
        )
        resolved_specs: list[TableSpec] = []
        for s in base_specs:
            ice_source = _resolve_iceberg_source_for_view(s.view, s.source)
            t_entry = table_entry_by_view.get(s.view)
            target_name = (
                t_entry.target_name
                if (t_entry is not None and t_entry.target_name)
                else s.name
            )
            resolved_specs.append(
                TableSpec(
                    name=target_name,
                    source=ice_source,
                    view=s.view,
                    columns=s.columns,
                    keys=s.keys,
                    snapshot_id=s.snapshot_id,
                )
            )
        return resolved_specs

    # Inline tables mode from source-config
    if not cfg.tables:
        raise BadInputError(
            "Iceberg source requires either '--columns'/'--registry' or inline 'tables' in '--source-config'"
        )

    pii_data: Mapping[str, Any] = cfg.pii_columns
    if pii_policy is not None:
        loaded = _load_json_data(pii_policy)
        if isinstance(loaded, Mapping):
            pii_data = loaded

    specs: list[TableSpec] = []
    for entry in cfg.tables:
        if entry.view in live_set or entry.live:
            continue
        if not entry.columns:
            raise BadInputError(
                f"Inline table '{entry.view}' in '--source-config' requires 'columns' when '--columns' is not passed"
            )
        ice_source = _resolve_iceberg_source_for_view(entry.view, entry.source)

        col_specs: list[ColumnSpec] = []
        for col in entry.columns:
            col_name = col.get("name")
            if not col_name:
                raise BadInputError(
                    f"Column in table '{entry.view}' missing 'name'"
                )
            try:
                validate_identifier(col_name)
                raw_type = col.get("type")
                if not raw_type:
                    raise ValueError(
                        f"Column {col_name!r} in view {entry.view!r} missing 'type'"
                    )
                duckdb_type = map_type(col_name, raw_type)
                validate_duckdb_type(duckdb_type, col_name)
                is_pii, is_nulled, is_excluded = resolve_column_pii(
                    col,
                    col_name=col_name,
                    view_name=entry.view,
                    pii_data=pii_data,
                    pii_mode=effective_mode,
                    allow_non_hashed_pii=allow_non_hashed_pii,
                    i_understand_pii_leaves_unmasked=i_understand_pii_leaves_unmasked,
                )
            except ValueError as exc:
                raise BadInputError(str(exc)) from exc

            if is_excluded:
                continue
            col_specs.append(
                ColumnSpec(
                    name=col_name,
                    duckdb_type=duckdb_type,
                    is_nulled=is_nulled,
                    is_pii=is_pii,
                )
            )

        if not col_specs:
            raise BadInputError(
                f"Table '{entry.view}' has no remaining columns after applying pii_mode={effective_mode!r}"
            )

        target_name = (
            entry.target_name
            if entry.target_name
            else f"{cfg.target_schema}.{entry.view}"
        )
        specs.append(
            TableSpec(
                name=target_name,
                source=ice_source,
                view=entry.view,
                columns=tuple(col_specs),
                keys=entry.keys,
                snapshot_id=entry.snapshot_id,
            )
        )

    return specs


class IcebergSource:
    """Read-only Lakekeeper Iceberg REST catalog source accessed via DuckDB."""

    source_type = "iceberg"

    def __init__(
        self,
        config: Union[IcebergSourceConfig, Mapping[str, Any], str, Path],
        *,
        conn: Any = None,
        allow_version_mismatch: bool = False,
        expected_version: Optional[str] = None,
        extension_dir: Optional[str] = None,
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
        env: Optional[Mapping[str, str]] = None,
        standin_catalogs: Optional[Mapping[str, str]] = None,
        standin_extensions: Optional[Sequence[str]] = None,
        standin_snapshots: Optional[Mapping[str, Optional[int]]] = None,
        snapshot_resolver: Optional[Callable[[str], Optional[int]]] = None,
    ) -> None:
        if isinstance(config, (str, Path)):
            self.config = load_iceberg_source_config(
                config, extension_dir=extension_dir
            )
        else:
            self.config = _coerce_config(config, extension_dir=extension_dir)

        self._conn = conn
        self.allow_version_mismatch = allow_version_mismatch
        self.expected_version = expected_version
        self.extension_dir = extension_dir or self.config.extension_directory
        self.memory_limit = memory_limit or self.config.memory_limit
        self.threads = threads if threads is not None else self.config.threads
        self._env = env
        if self._env is not None:
            for var_name in self.config.referenced_env_vars():
                val = self._env.get(var_name)
                if isinstance(val, str) and val:
                    register_secret_values(val)
        self.standin_catalogs = standin_catalogs
        self.standin_extensions = standin_extensions
        self.standin_snapshots: dict[str, Optional[int]] = (
            dict(standin_snapshots) if standin_snapshots is not None else {}
        )
        self._snapshot_resolver = snapshot_resolver
        self.executed_setup_steps: list[IcebergSetupStep] = []
        self.executed_snapshot_sqls: list[str] = []
        self.executed_count_sqls: list[str] = []

    @property
    def allowed_aliases(self) -> frozenset[str]:
        return frozenset(self.config.iceberg_catalogs.keys())

    def _ensure_conn(self) -> Any:
        if self._conn is not None:
            return self._conn
        if duckdb is None:
            raise RuntimeError("duckdb is not installed")
        check_duckdb_version(
            allow_mismatch=self.allow_version_mismatch,
            expected_version=self.expected_version,
        )
        conn = duckdb.connect(":memory:")
        try:
            if self.memory_limit:
                mem = validate_memory_limit(self.memory_limit)
                conn.execute(f"SET memory_limit = '{mem}'")
            if self.threads is not None:
                thr = validate_threads(self.threads)
                conn.execute(f"SET threads = {thr}")

            self.executed_setup_steps = execute_iceberg_source_setup(
                conn,
                self.config,
                extension_dir=self.extension_dir,
                env=self._env,
                standin_catalogs=self.standin_catalogs,
                standin_extensions=self.standin_extensions,
            )
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise
        self._conn = conn
        return conn

    def get_row_count(
        self,
        table_or_view: str,
        snapshot_id: Optional[int] = None,
    ) -> int:
        count_sql = plan_iceberg_count_sql(
            table_or_view,
            snapshot_id=snapshot_id,
            allowed_aliases=self.allowed_aliases,
        )
        self.executed_count_sqls.append(count_sql)
        conn = self._ensure_conn()
        rewriter = _make_standin_rewriter(conn, self.standin_catalogs)
        effective_sql = rewriter(count_sql) if rewriter is not None else count_sql
        try:
            res = conn.execute(effective_sql).fetchone()
        except Exception as exc:
            raise LoadError(
                f"Failed to query Iceberg row count ({count_sql}): {scrub_text(str(exc))}"
            ) from None
        return int(res[0]) if res else 0

    def _validate_resolved_snapshot(self, raw_val: Any, context_label: str) -> int:
        if raw_val is None:
            raise LoadError(
                f"Iceberg snapshot resolution returned None for '{context_label}'"
            )
        try:
            return validate_snapshot_id(
                raw_val,
                context=f"Resolved snapshot for '{context_label}'",
            )
        except ValueError as exc:
            raise LoadError(scrub_text(str(exc))) from exc

    def resolve_snapshot_id(
        self,
        table_or_spec: Union[str, TableSpec],
    ) -> int:
        """Resolve pinned or current Iceberg snapshot_id; raises LoadError on error/null."""
        if isinstance(table_or_spec, TableSpec):
            if table_or_spec.snapshot_id is not None:
                return self._validate_resolved_snapshot(
                    table_or_spec.snapshot_id,
                    table_or_spec.source,
                )
            source_ref = table_or_spec.source
            view_name = table_or_spec.view
            target_name = table_or_spec.name
        else:
            source_ref = table_or_spec
            view_name = table_or_spec
            target_name = table_or_spec

        alias, canonical_ref = validate_iceberg_table_ref(
            source_ref,
            allowed_aliases=self.allowed_aliases,
        )
        segments = _split_qualified_segments(canonical_ref)
        rest_ref = ".".join(segments[1:])
        leaf_table = segments[-1]

        if self._snapshot_resolver is not None:
            try:
                resolved = self._snapshot_resolver(canonical_ref)
            except LoadError:
                raise
            except Exception as exc:
                raise LoadError(
                    f"Failed to resolve Iceberg snapshot for '{canonical_ref}': {scrub_text(str(exc))}"
                ) from None
            return self._validate_resolved_snapshot(resolved, canonical_ref)

        for candidate_key in (
            canonical_ref,
            source_ref,
            rest_ref,
            target_name,
            view_name,
            leaf_table,
        ):
            if candidate_key in self.standin_snapshots:
                return self._validate_resolved_snapshot(
                    self.standin_snapshots[candidate_key],
                    canonical_ref,
                )

        conn = self._ensure_conn()

        if self.standin_catalogs is not None and alias in self.standin_catalogs:
            candidates_sql = ", ".join(
                quote_sql_string(k)
                for k in dict.fromkeys(
                    (canonical_ref, rest_ref, target_name, view_name, leaf_table)
                )
            )
            standin_sql = (
                f"SELECT snapshot_id FROM {alias}._iceberg_snapshots "
                f"WHERE table_name IN ({candidates_sql}) "
                "ORDER BY snapshot_id DESC LIMIT 1"
            )
            self.executed_snapshot_sqls.append(standin_sql)
            try:
                row = conn.execute(standin_sql).fetchone()
            except Exception as exc:
                raise LoadError(
                    f"Failed to resolve stand-in Iceberg snapshot for '{canonical_ref}': {scrub_text(str(exc))}"
                ) from None
            if not row or row[0] is None:
                raise LoadError(
                    f"No snapshot found in {alias}._iceberg_snapshots for '{canonical_ref}'"
                )
            return self._validate_resolved_snapshot(row[0], canonical_ref)

        snap_sql = plan_iceberg_snapshot_sql(
            canonical_ref,
            allowed_aliases=self.allowed_aliases,
        )
        self.executed_snapshot_sqls.append(snap_sql)
        try:
            row = conn.execute(snap_sql).fetchone()
        except Exception as exc:
            raise LoadError(
                f"Failed to resolve Iceberg snapshot ({snap_sql}): {scrub_text(str(exc))}"
            ) from None
        if not row or row[0] is None:
            raise LoadError(
                f"Iceberg snapshot query returned no snapshot for '{canonical_ref}'"
            )
        return self._validate_resolved_snapshot(row[0], canonical_ref)

    def get_snapshot_id(
        self,
        table_or_spec: Union[str, TableSpec],
    ) -> int:
        """Resolve and return positive 64-bit snapshot_id; never swallows errors or returns None."""
        return self.resolve_snapshot_id(table_or_spec)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


def create_iceberg_target_factory(
    config: Union[IcebergSourceConfig, Mapping[str, Any], str, Path],
    *,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
    allow_version_mismatch: bool = False,
    expected_version: Optional[str] = None,
    extension_dir: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    standin_catalogs: Optional[Mapping[str, str]] = None,
    standin_extensions: Optional[Sequence[str]] = None,
    setup_steps_out: Optional[list[IcebergSetupStep]] = None,
) -> Callable[[Union[str, Path]], DuckDBTarget]:
    """Return a target factory that configures DuckDB for read-only Iceberg access before CTAS."""
    if isinstance(config, (str, Path)):
        parsed_cfg = load_iceberg_source_config(config, extension_dir=extension_dir)
    else:
        parsed_cfg = _coerce_config(config, extension_dir=extension_dir)

    eff_ext_dir = extension_dir or parsed_cfg.extension_directory
    eff_mem = memory_limit or parsed_cfg.memory_limit
    eff_threads = threads if threads is not None else parsed_cfg.threads

    def _setup(conn: Any) -> None:
        steps = execute_iceberg_source_setup(
            conn,
            parsed_cfg,
            extension_dir=eff_ext_dir,
            env=env,
            standin_catalogs=standin_catalogs,
            standin_extensions=standin_extensions,
        )
        if setup_steps_out is not None:
            setup_steps_out.extend(steps)

    def factory(target_path: Union[str, Path]) -> DuckDBTarget:
        base_factory = create_target_factory(
            memory_limit=eff_mem,
            threads=eff_threads,
            attach_pg=False,
            allow_version_mismatch=allow_version_mismatch,
            expected_version=expected_version,
            extension_dir=eff_ext_dir,
            setup_fn=_setup,
        )
        target = base_factory(target_path)
        if standin_catalogs:
            target._stmt_rewriter = _make_standin_rewriter(
                target.conn, standin_catalogs
            )
        return target

    return factory

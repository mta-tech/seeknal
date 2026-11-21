"""Configuration model and validation for Seeknal Serving discovery."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import yaml
from yaml.constructor import ConstructorError

from seeknal.serving.placement import UniqueKeySafeLoader

_ICEBERG_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_RESERVED_ICEBERG_ALIASES = frozenset({"main", "system", "temp", "memory"})
_SIMPLE_LOWER_ENUM_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_LOGICAL_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PG_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}\Z")
_SYNC_STATE_TABLE_RE = re.compile(
    r"^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?\Z"
)
VALID_POSTGRES_SOURCE_ACCOUNTS = frozenset(
    {"interactive", "bi", "report", "ask", "job"}
)
VALID_GUARD_MODES = frozenset({"enforce", "warn"})
VALID_ACCESS_PATTERNS = frozenset({"point", "filter", "scan"})
VALID_ACCESS_KEYS = frozenset(
    {
        "pattern",
        "keys",
        "filterable",
        "range_filterable",
        "prefix_filterable",
        "sortable",
        "require_filter",
        "max_rows",
    }
)
_SAFE_DISC_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
_SUSPICIOUS_DISC_RE = re.compile(
    r"(://|@|secret|password|passwd|token|bearer|private|canary|sentinel|opaque|dsn|credential)",
    re.IGNORECASE,
)


def _safe_enum_token(value: Any) -> str:
    """Return a strictly lowercase simple enum token or '<invalid>'."""
    if not isinstance(value, str):
        return "<invalid>"
    s = value.strip()
    if not s or _SUSPICIOUS_DISC_RE.search(s):
        return "<invalid>"
    if not _SIMPLE_LOWER_ENUM_RE.fullmatch(s):
        return "<invalid>"
    return s


def _safe_disc_label(value: Any, fallback: str = "<redacted>") -> str:
    """Return a safe identifier label for discovery error messages."""
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    s = value.strip()
    if not s:
        return "<empty>"
    if _SUSPICIOUS_DISC_RE.search(s) or not _SAFE_DISC_IDENT_RE.fullmatch(s):
        return fallback
    return s


def _validate_discovery_logical_table_name(
    raw_name: Any,
    *,
    default_catalog: str,
    field_label: str,
    allowed_catalogs: frozenset[str] | None = None,
) -> str:
    """Validate a 3-part unquoted logical table name '<catalog>.<namespace>.<table>'."""
    if not isinstance(raw_name, str) or not raw_name:
        raise DiscoveryConfigError(
            f"'{field_label}' must contain only non-empty 3-part table name strings"
        )
    if '"' in raw_name or "'" in raw_name or any(ch.isspace() for ch in raw_name):
        raise DiscoveryConfigError(
            f"Invalid table name in '{field_label}': quotes and whitespace are not permitted "
            f"(expected unquoted '{_safe_disc_label(default_catalog)}.<namespace>.<table>')"
        )
    parts = raw_name.split(".")
    if len(parts) != 3 or not all(_LOGICAL_SEGMENT_RE.fullmatch(p) for p in parts):
        raise DiscoveryConfigError(
            f"Invalid table name '{_safe_disc_label(raw_name)}' in '{field_label}': "
            f"expected 3-part '<default_catalog>.<namespace>.<table>' with valid identifier segments"
        )
    valid_cats = (
        {c.lower() for c in allowed_catalogs}
        if allowed_catalogs is not None
        else {default_catalog.lower()}
    )
    if parts[0].lower() not in valid_cats:
        raise DiscoveryConfigError(
            f"Table '{_safe_disc_label(raw_name)}' in '{field_label}' has catalog prefix "
            f"'{_safe_disc_label(parts[0])}', expected default_catalog '{_safe_disc_label(default_catalog)}'"
        )
    return raw_name


def _validate_pg_ident(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not _PG_IDENT_RE.match(value):
        raise DiscoveryConfigError(
            f"Invalid identifier '{_safe_disc_label(value)}' in {label}: "
            "must match '^[a-z_][a-z0-9_]{0,62}$'"
        )
    return value


def _validate_access_override(
    raw_access: Any,
    *,
    table_label: str,
) -> dict[str, Any]:
    if not isinstance(raw_access, Mapping):
        raise DiscoveryConfigError(
            f"Table override '{table_label}' 'access' must be a mapping"
        )
    unknown = set(raw_access.keys()) - VALID_ACCESS_KEYS
    if unknown:
        raise DiscoveryConfigError(
            f"Unknown key(s) in '{table_label}.access': {sorted(str(k) for k in unknown)}"
        )
    cleaned: dict[str, Any] = {}
    if "pattern" in raw_access and raw_access["pattern"] is not None:
        pat = raw_access["pattern"]
        if not isinstance(pat, str) or pat not in VALID_ACCESS_PATTERNS:
            raise DiscoveryConfigError(
                f"Invalid pattern '{_safe_enum_token(pat)}' in '{table_label}.access': "
                "expected 'point', 'filter', or 'scan'"
            )
        cleaned["pattern"] = pat

    for col_list_field in (
        "keys",
        "filterable",
        "range_filterable",
        "prefix_filterable",
        "sortable",
    ):
        if col_list_field in raw_access and raw_access[col_list_field] is not None:
            raw_seq = raw_access[col_list_field]
            if not isinstance(raw_seq, (list, tuple)):
                raise DiscoveryConfigError(
                    f"'{table_label}.access.{col_list_field}' must be a list or tuple of column names"
                )
            cols: list[str] = []
            for col_item in raw_seq:
                cols.append(
                    _validate_pg_ident(
                        col_item,
                        label=f"'{table_label}.access.{col_list_field}'",
                    )
                )
            cleaned[col_list_field] = cols

    if "require_filter" in raw_access and raw_access["require_filter"] is not None:
        rf = raw_access["require_filter"]
        if not isinstance(rf, bool):
            raise DiscoveryConfigError(
                f"'{table_label}.access.require_filter' must be a boolean"
            )
        cleaned["require_filter"] = rf

    if "max_rows" in raw_access:
        mr = raw_access["max_rows"]
        if mr is None:
            cleaned["max_rows"] = None
        elif isinstance(mr, bool) or not isinstance(mr, int) or mr <= 0:
            raise DiscoveryConfigError(
                f"'{table_label}.access.max_rows' must be a positive integer"
            )
        else:
            cleaned["max_rows"] = mr

    return cleaned


def _validate_discovery_alias(alias: Any, eng_name: str) -> str:
    if not isinstance(alias, str) or not _ICEBERG_ALIAS_RE.fullmatch(alias):
        raise DiscoveryConfigError(
            f"Engine '{eng_name}' alias must match ^[a-z][a-z0-9_]{{0,31}}$"
        )
    if alias in _RESERVED_ICEBERG_ALIASES:
        raise DiscoveryConfigError(
            f"Engine '{eng_name}' alias '{alias}' is reserved"
        )
    return alias


class DiscoveryError(Exception):
    """Base exception for all discovery failures."""


class DiscoveryConfigError(DiscoveryError):
    """Raised when discovery configuration is invalid."""


@dataclass(frozen=True)
class LakekeeperOAuthConfig:
    """Configuration for Lakekeeper OAuth2 client credentials."""

    token_url: str
    client_id: str
    client_secret_env: str | None = None
    client_secret_file: str | None = None
    scope: str | None = None
    audience: str | None = None


@dataclass(frozen=True)
class LakekeeperConfig:
    """Configuration for Iceberg REST catalog (Lakekeeper)."""

    url: str
    warehouse: str
    token_env: str | None = None
    namespaces: tuple[str, ...] | None = None
    oauth: LakekeeperOAuthConfig | None = None


@dataclass(frozen=True)
class EngineConfig:
    """Engine configuration within discovery."""

    name: str
    kind: str
    dialect: str
    catalog: str | None = None
    url: str | None = None
    token_env: str | None = None
    api_secret_env: str | None = None
    api_secret_file: str | None = None
    api_claims: Mapping[str, Any] | None = None
    profile: str | None = None
    alias: str | None = None
    pin_current_snapshot: bool = False


@dataclass(frozen=True)
class SyncedTableConfig:
    """Synced table mapping for Lakebase."""

    target: str
    primary_key: tuple[str, ...] = ()
    pii: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.pii, bool):
            raise DiscoveryConfigError(
                f"Synced table target '{self.target}' 'pii' must be a boolean"
            )


@dataclass(frozen=True)
class LakebaseConfig:
    """Lakebase replication and serving configuration."""

    engine: str
    synced_tables: Mapping[str, SyncedTableConfig] = field(default_factory=dict)


@dataclass(frozen=True)
class PostgresFreshnessConfig:
    """Freshness marker configuration for a Postgres or Lakebase source."""

    sync_state_table: str | None = None
    freshness_sql: str | None = None
    sync_state_explicit: bool = field(default=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.sync_state_table is not None:
            if not isinstance(
                self.sync_state_table, str
            ) or not _SYNC_STATE_TABLE_RE.match(self.sync_state_table):
                raise DiscoveryConfigError(
                    f"Invalid sync_state_table '{_safe_disc_label(self.sync_state_table)}': "
                    "must match '^[a-z_][a-z0-9_]{0,62}(\\.[a-z_][a-z0-9_]{0,62})?$'"
                )
        if self.freshness_sql is not None:
            if not isinstance(self.freshness_sql, str) or not self.freshness_sql.strip():
                raise DiscoveryConfigError(
                    "'freshness_sql' must be a non-empty SQL string when set"
                )
            object.__setattr__(self, "freshness_sql", self.freshness_sql.strip())


@dataclass(frozen=True)
class PostgresSchemaConfig:
    """Allowlist and classification settings for a physical PostgreSQL schema."""

    include: tuple[str, ...] = ("*",)
    exclude: tuple[str, ...] = ()
    pii: bool = False
    tokenized: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.include, (list, tuple)) or any(
            not isinstance(p, str) or not p.strip() for p in self.include
        ):
            raise DiscoveryConfigError(
                "Schema 'include' must be a sequence of non-empty glob strings"
            )
        if not isinstance(self.exclude, (list, tuple)) or any(
            not isinstance(p, str) or not p.strip() for p in self.exclude
        ):
            raise DiscoveryConfigError(
                "Schema 'exclude' must be a sequence of non-empty glob strings"
            )
        if not isinstance(self.pii, bool):
            raise DiscoveryConfigError("Schema 'pii' must be a boolean")
        if not isinstance(self.tokenized, bool):
            raise DiscoveryConfigError("Schema 'tokenized' must be a boolean")
        object.__setattr__(self, "include", tuple(self.include))
        object.__setattr__(self, "exclude", tuple(self.exclude))


@dataclass(frozen=True)
class PostgresTableOverrideConfig:
    """Per-table classification and access override for an introspected table."""

    pii: bool | None = None
    tokenized: bool | None = None
    access: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.pii is not None and not isinstance(self.pii, bool):
            raise DiscoveryConfigError("Table override 'pii' must be a boolean or None")
        if self.tokenized is not None and not isinstance(self.tokenized, bool):
            raise DiscoveryConfigError(
                "Table override 'tokenized' must be a boolean or None"
            )
        if self.access is not None:
            validated = _validate_access_override(
                self.access, table_label="<table_override>"
            )
            object.__setattr__(self, "access", validated)


@dataclass(frozen=True)
class PostgresSourceConfig:
    """pg_catalog introspection configuration for a postgres or lakebase engine."""

    engine: str
    schemas: Mapping[str, PostgresSchemaConfig] = field(default_factory=dict)
    account: str = "interactive"
    logical_catalog: str = ""
    schema_map: Mapping[str, str] = field(default_factory=dict)
    freshness: PostgresFreshnessConfig | None = None
    tables: Mapping[str, PostgresTableOverrideConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.engine, str) or not self.engine.strip():
            raise DiscoveryConfigError("PostgresSourceConfig 'engine' must be non-empty")
        if self.account not in VALID_POSTGRES_SOURCE_ACCOUNTS:
            raise DiscoveryConfigError(
                f"Postgres source '{_safe_disc_label(self.engine)}' invalid account "
                f"'{_safe_enum_token(self.account)}': expected one of {sorted(VALID_POSTGRES_SOURCE_ACCOUNTS)}"
            )
        eff_cat = self.logical_catalog if self.logical_catalog else self.engine
        _validate_pg_ident(
            eff_cat,
            label=f"postgres_sources.{_safe_disc_label(self.engine)}.logical_catalog",
        )
        object.__setattr__(self, "logical_catalog", eff_cat)

        if not isinstance(self.schemas, Mapping) or not self.schemas:
            raise DiscoveryConfigError(
                f"postgres_sources.{_safe_disc_label(self.engine)}.schemas must be a non-empty mapping"
            )
        for phys_sch, sch_cfg in self.schemas.items():
            _validate_pg_ident(
                phys_sch,
                label=f"postgres_sources.{_safe_disc_label(self.engine)}.schemas",
            )
            if not isinstance(sch_cfg, PostgresSchemaConfig):
                raise DiscoveryConfigError(
                    f"Invalid schema config for '{_safe_disc_label(phys_sch)}'"
                )

        if not isinstance(self.schema_map, Mapping):
            raise DiscoveryConfigError(
                f"postgres_sources.{_safe_disc_label(self.engine)}.schema_map must be a mapping"
            )
        for phys_s, log_s in self.schema_map.items():
            _validate_pg_ident(
                phys_s,
                label=f"postgres_sources.{_safe_disc_label(self.engine)}.schema_map",
            )
            _validate_pg_ident(
                log_s,
                label=f"postgres_sources.{_safe_disc_label(self.engine)}.schema_map",
            )

        if not isinstance(self.tables, Mapping):
            raise DiscoveryConfigError(
                f"postgres_sources.{_safe_disc_label(self.engine)}.tables must be a mapping"
            )
        for tbl_key, ov_cfg in self.tables.items():
            if not isinstance(tbl_key, str) or '"' in tbl_key or "'" in tbl_key:
                raise DiscoveryConfigError(
                    f"Invalid table key in postgres_sources.{_safe_disc_label(self.engine)}.tables"
                )
            parts = tbl_key.split(".")
            if len(parts) != 3 or not all(_PG_IDENT_RE.match(p) for p in parts):
                raise DiscoveryConfigError(
                    f"Invalid table override key '{_safe_disc_label(tbl_key)}' in "
                    f"postgres_sources.{_safe_disc_label(self.engine)}.tables: "
                    f"expected 3-part '{eff_cat}.<schema>.<table>' matching '^[a-z_][a-z0-9_]{{0,62}}$'"
                )
            if parts[0] != eff_cat:
                raise DiscoveryConfigError(
                    f"Table override '{_safe_disc_label(tbl_key)}' in "
                    f"postgres_sources.{_safe_disc_label(self.engine)}.tables must start with "
                    f"logical_catalog '{eff_cat}'"
                )
            if not isinstance(ov_cfg, PostgresTableOverrideConfig):
                raise DiscoveryConfigError(
                    f"Invalid override config for '{_safe_disc_label(tbl_key)}'"
                )


@dataclass(frozen=True)
class DiscoveryConfig:
    """Top-level discovery configuration."""

    default_catalog: str
    refresh_interval_seconds: float = 60.0
    timeout_seconds: float = 10.0
    lakekeeper: LakekeeperConfig | None = None
    engines: Mapping[str, EngineConfig] = field(default_factory=dict)
    lakebase: LakebaseConfig | None = None
    postgres_sources: Mapping[str, PostgresSourceConfig] = field(default_factory=dict)
    pii_tables: tuple[str, ...] = ()
    pii_engines: tuple[str, ...] = ()
    pii_object_storage_engines: tuple[str, ...] = ()
    require_filter_min_rows: int = 100_000
    max_in_list: int = 1_000
    max_offset: int = 10_000
    guard_mode: str = "enforce"
    _explicit_policy_keys: frozenset[str] = field(
        default_factory=frozenset, repr=False, compare=False
    )

    @classmethod
    def from_dict(cls, data: Mapping) -> DiscoveryConfig:
        """Construct and validate DiscoveryConfig from a raw mapping."""
        if not isinstance(data, Mapping):
            raise DiscoveryConfigError(
                f"Config data must be a mapping, got {type(data).__name__}"
            )

        # 1. default_catalog
        default_catalog = data.get("default_catalog")
        if (
            not default_catalog
            or not isinstance(default_catalog, str)
            or not default_catalog.strip()
        ):
            raise DiscoveryConfigError(
                "Missing or invalid required field 'default_catalog'"
            )
        default_catalog = default_catalog.strip()

        # 2. refresh_interval_seconds
        raw_interval = data.get("refresh_interval_seconds", 60.0)
        try:
            refresh_interval = float(raw_interval)
            if (
                refresh_interval <= 0
                or math.isnan(refresh_interval)
                or math.isinf(refresh_interval)
            ):
                raise ValueError()
        except (TypeError, ValueError) as exc:
            raise DiscoveryConfigError(
                f"Invalid 'refresh_interval_seconds': {raw_interval}"
            ) from exc

        # 3. timeout_seconds
        raw_timeout = data.get("timeout_seconds", data.get("timeout", 10.0))
        try:
            timeout_seconds = float(raw_timeout)
            if (
                timeout_seconds <= 0
                or math.isnan(timeout_seconds)
                or math.isinf(timeout_seconds)
            ):
                raise ValueError()
        except (TypeError, ValueError) as exc:
            raise DiscoveryConfigError(
                f"Invalid 'timeout_seconds': {raw_timeout}"
            ) from exc

        # 4. engines
        engines_raw = data.get("engines")
        if engines_raw is not None and not isinstance(engines_raw, Mapping):
            raise DiscoveryConfigError("'engines' must be a mapping")

        engines_dict: dict[str, EngineConfig] = {}
        if engines_raw:
            starrocks_count = 0
            cube_count = 0

            for eng_name, eng_data in engines_raw.items():
                if not isinstance(eng_name, str) or not eng_name.strip():
                    raise DiscoveryConfigError("Engine name must be a non-empty string")
                eng_name_str = eng_name.strip()
                if not isinstance(eng_data, Mapping):
                    raise DiscoveryConfigError(
                        f"Engine spec for '{eng_name_str}' must be a mapping"
                    )

                if "storage_class" in eng_data:
                    safe_eng = _safe_disc_label(eng_name_str)
                    raise DiscoveryConfigError(
                        f"Engine '{safe_eng}' cannot declare 'storage_class' in "
                        f"placement.discovery.engines; declare 'storage_class' on connections.{safe_eng} instead"
                    )

                kind = eng_data.get("kind")
                if not isinstance(kind, str) or not kind.strip():
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' missing required field 'kind'"
                    )
                kind_str = kind.strip()
                if kind_str not in (
                    "starrocks",
                    "cube",
                    "lakebase",
                    "duckdb",
                    "postgres",
                ):
                    raise DiscoveryConfigError(
                        f"Unknown engine kind '{_safe_enum_token(kind_str)}' for engine '{_safe_disc_label(eng_name_str)}'"
                    )

                dialect = eng_data.get("dialect")
                if not isinstance(dialect, str) or not dialect.strip():
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' missing required field 'dialect'"
                    )
                dialect_str = dialect.strip()

                catalog = eng_data.get("catalog")
                if catalog is not None and (
                    not isinstance(catalog, str) or not catalog.strip()
                ):
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' has invalid 'catalog'"
                    )
                catalog_str = catalog.strip() if catalog else None

                url = eng_data.get("url")
                if url is not None and (not isinstance(url, str) or not url.strip()):
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' has invalid 'url'"
                    )
                url_str = url.strip().rstrip("/") if url else None

                token_env = eng_data.get("token_env")
                if token_env is not None and (
                    not isinstance(token_env, str) or not token_env.strip()
                ):
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' has invalid 'token_env'"
                    )
                token_env_str = token_env.strip() if token_env else None

                api_secret_env = eng_data.get("api_secret_env")
                if api_secret_env is not None and (
                    not isinstance(api_secret_env, str) or not api_secret_env.strip()
                ):
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' has invalid 'api_secret_env'"
                    )
                api_secret_env_str = api_secret_env.strip() if api_secret_env else None

                api_secret_file = eng_data.get("api_secret_file")
                if api_secret_file is not None and (
                    not isinstance(api_secret_file, str) or not api_secret_file.strip()
                ):
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' has invalid 'api_secret_file'"
                    )
                api_secret_file_str = api_secret_file.strip() if api_secret_file else None

                api_claims = eng_data.get("api_claims")
                if api_claims is not None and not isinstance(api_claims, Mapping):
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' 'api_claims' must be a mapping"
                    )

                if token_env_str and (api_secret_env_str or api_secret_file_str):
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' cannot have both 'token_env' and 'api_secret_env'/'api_secret_file'"
                    )
                if api_secret_env_str and api_secret_file_str:
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' cannot have both 'api_secret_env' and 'api_secret_file'"
                    )
                if api_secret_file_str and os.path.exists(api_secret_file_str):
                    st = os.stat(api_secret_file_str)
                    if (st.st_mode & 0o044) != 0:
                        raise DiscoveryConfigError(
                            f"Engine '{eng_name_str}' api_secret_file '{api_secret_file_str}' is group- or world-readable"
                        )

                if kind_str != "cube" and (
                    api_secret_env_str or api_secret_file_str or api_claims is not None
                ):
                    raise DiscoveryConfigError(
                        f"Engine '{_safe_disc_label(eng_name_str)}' of kind '{_safe_enum_token(kind_str)}' cannot specify 'api_secret_env'/'api_secret_file'/'api_claims'"
                    )

                if kind_str == "starrocks":
                    starrocks_count += 1
                    if catalog_str is None:
                        catalog_str = default_catalog
                elif kind_str == "cube":
                    cube_count += 1
                    if not url_str:
                        raise DiscoveryConfigError(
                            f"Cube engine '{eng_name_str}' requires 'url'"
                        )

                profile_raw = eng_data.get("profile")
                profile_str: str | None = None
                alias_str: str | None = None
                pin_snap = False

                if profile_raw is not None:
                    if kind_str != "duckdb":
                        raise DiscoveryConfigError(
                            f"Engine '{_safe_disc_label(eng_name_str)}' of kind '{_safe_enum_token(kind_str)}' cannot specify 'profile'"
                        )
                    if not isinstance(profile_raw, str) or profile_raw.strip() not in ("local_file", "iceberg_direct"):
                        raise DiscoveryConfigError(
                            f"DuckDB engine '{eng_name_str}' has invalid 'profile', expected 'local_file' or 'iceberg_direct'"
                        )
                    profile_str = profile_raw.strip()
                elif kind_str == "duckdb":
                    profile_str = "local_file"

                alias_raw = eng_data.get("alias")
                if alias_raw is not None:
                    if not isinstance(alias_raw, str):
                        raise DiscoveryConfigError(
                            f"Engine '{eng_name_str}' has invalid 'alias'"
                        )
                    alias_str = _validate_discovery_alias(alias_raw.strip(), eng_name_str)

                if kind_str == "duckdb" and profile_str == "iceberg_direct":
                    if alias_str is None:
                        alias_str = _validate_discovery_alias(
                            catalog_str or default_catalog, eng_name_str
                        )
                    if catalog_str is None:
                        catalog_str = alias_str
                    pin_raw = eng_data.get("pin_current_snapshot", False)
                    if not isinstance(pin_raw, bool):
                        raise DiscoveryConfigError(
                            f"Engine '{eng_name_str}' 'pin_current_snapshot' must be a boolean"
                        )
                    pin_snap = pin_raw

                engines_dict[eng_name_str] = EngineConfig(
                    name=eng_name_str,
                    kind=kind_str,
                    dialect=dialect_str,
                    catalog=catalog_str,
                    url=url_str,
                    token_env=token_env_str,
                    api_secret_env=api_secret_env_str,
                    api_secret_file=api_secret_file_str,
                    api_claims=api_claims,
                    profile=profile_str,
                    alias=alias_str,
                    pin_current_snapshot=pin_snap,
                )

            if starrocks_count > 1:
                raise DiscoveryConfigError(
                    f"Only one StarRocks engine is allowed, found {starrocks_count}"
                )
            if cube_count > 1:
                raise DiscoveryConfigError(
                    f"At most one Cube engine is allowed, found {cube_count}"
                )

        # 5. lakekeeper
        lk_raw = data.get("lakekeeper")
        lakekeeper_cfg: LakekeeperConfig | None = None
        if lk_raw is not None:
            if not isinstance(lk_raw, Mapping):
                raise DiscoveryConfigError("'lakekeeper' must be a mapping")
            url = lk_raw.get("url")
            if not isinstance(url, str) or not url.strip():
                raise DiscoveryConfigError("Lakekeeper config requires non-empty 'url'")
            warehouse = lk_raw.get("warehouse")
            if not isinstance(warehouse, str) or not warehouse.strip():
                raise DiscoveryConfigError(
                    "Lakekeeper config requires non-empty 'warehouse'"
                )
            token_env = lk_raw.get("token_env")
            if token_env is not None and (
                not isinstance(token_env, str) or not token_env.strip()
            ):
                raise DiscoveryConfigError(
                    "Lakekeeper 'token_env' must be a non-empty string"
                )
            token_env_str = token_env.strip() if token_env else None

            oauth_raw = lk_raw.get("oauth")
            oauth_cfg: LakekeeperOAuthConfig | None = None
            if oauth_raw is not None:
                if not isinstance(oauth_raw, Mapping):
                    raise DiscoveryConfigError("Lakekeeper 'oauth' must be a mapping")
                if token_env_str:
                    raise DiscoveryConfigError(
                        "Lakekeeper cannot have both 'token_env' and 'oauth'"
                    )
                allowed_oauth = {
                    "token_url",
                    "client_id",
                    "client_secret_env",
                    "client_secret_file",
                    "scope",
                    "audience",
                }
                unknown_oauth = set(oauth_raw.keys()) - allowed_oauth
                if unknown_oauth:
                    raise DiscoveryConfigError(
                        f"Unknown key(s) in lakekeeper 'oauth': {sorted(unknown_oauth)}"
                    )

                token_url = oauth_raw.get("token_url")
                if (
                    not token_url
                    or not isinstance(token_url, str)
                    or not token_url.strip()
                ):
                    raise DiscoveryConfigError(
                        "Lakekeeper oauth requires non-empty 'token_url'"
                    )

                client_id = oauth_raw.get("client_id")
                if (
                    not client_id
                    or not isinstance(client_id, str)
                    or not client_id.strip()
                ):
                    raise DiscoveryConfigError(
                        "Lakekeeper oauth requires non-empty 'client_id'"
                    )

                has_cs_env = (
                    "client_secret_env" in oauth_raw
                    and oauth_raw["client_secret_env"] is not None
                )
                has_cs_file = (
                    "client_secret_file" in oauth_raw
                    and oauth_raw["client_secret_file"] is not None
                )
                if has_cs_env and has_cs_file:
                    raise DiscoveryConfigError(
                        "Lakekeeper oauth cannot have both 'client_secret_env' and 'client_secret_file'"
                    )
                if not has_cs_env and not has_cs_file:
                    raise DiscoveryConfigError(
                        "Lakekeeper oauth must specify either 'client_secret_env' or 'client_secret_file'"
                    )

                cs_env_str = None
                cs_file_str = None
                if has_cs_env:
                    cs_env = oauth_raw["client_secret_env"]
                    if not isinstance(cs_env, str) or not cs_env.strip():
                        raise DiscoveryConfigError(
                            "Lakekeeper oauth 'client_secret_env' must be a non-empty string"
                        )
                    cs_env_str = cs_env.strip()
                else:
                    cs_file = oauth_raw["client_secret_file"]
                    if not isinstance(cs_file, str) or not cs_file.strip():
                        raise DiscoveryConfigError(
                            "Lakekeeper oauth 'client_secret_file' must be a non-empty string"
                        )
                    cs_file_str = cs_file.strip()
                    if os.path.exists(cs_file_str):
                        st = os.stat(cs_file_str)
                        if (st.st_mode & 0o044) != 0:
                            raise DiscoveryConfigError(
                                f"Lakekeeper client_secret_file '{cs_file_str}' is group- or world-readable"
                            )

                scope = oauth_raw.get("scope")
                if scope is not None and (
                    not isinstance(scope, str) or not scope.strip()
                ):
                    raise DiscoveryConfigError(
                        "Lakekeeper oauth 'scope' must be a non-empty string"
                    )
                scope_str = scope.strip() if scope else None

                audience = oauth_raw.get("audience")
                if audience is not None and (
                    not isinstance(audience, str) or not audience.strip()
                ):
                    raise DiscoveryConfigError(
                        "Lakekeeper oauth 'audience' must be a non-empty string"
                    )
                audience_str = audience.strip() if audience else None

                oauth_cfg = LakekeeperOAuthConfig(
                    token_url=token_url.strip(),
                    client_id=client_id.strip(),
                    client_secret_env=cs_env_str,
                    client_secret_file=cs_file_str,
                    scope=scope_str,
                    audience=audience_str,
                )

            ns_raw = lk_raw.get("namespaces")
            namespaces: tuple[str, ...] | None = None
            if ns_raw is not None:
                if not isinstance(ns_raw, (list, tuple, set)):
                    raise DiscoveryConfigError(
                        "Lakekeeper 'namespaces' must be a list of strings"
                    )
                ns_list = []
                for ns in ns_raw:
                    if not isinstance(ns, str) or not ns.strip():
                        raise DiscoveryConfigError(
                            "Lakekeeper namespace must be a non-empty string"
                        )
                    ns_list.append(ns.strip())
                namespaces = tuple(ns_list)

            lakekeeper_cfg = LakekeeperConfig(
                url=url.strip().rstrip("/"),
                warehouse=warehouse.strip(),
                token_env=token_env_str,
                namespaces=namespaces,
                oauth=oauth_cfg,
            )

        # 6. lakebase
        lb_raw = data.get("lakebase")
        lakebase_cfg: LakebaseConfig | None = None
        if lb_raw is not None:
            if not isinstance(lb_raw, Mapping):
                raise DiscoveryConfigError("'lakebase' must be a mapping")
            lb_eng = lb_raw.get("engine")
            if not isinstance(lb_eng, str) or not lb_eng.strip():
                raise DiscoveryConfigError("Lakebase config requires non-empty 'engine'")
            lb_eng_str = lb_eng.strip()
            if lb_eng_str not in engines_dict:
                raise DiscoveryConfigError(
                    f"Lakebase engine '{lb_eng_str}' not defined in 'engines'"
                )
            if engines_dict[lb_eng_str].kind != "lakebase":
                raise DiscoveryConfigError(
                    f"Lakebase engine '{lb_eng_str}' must have kind 'lakebase'"
                )

            synced_raw = lb_raw.get("synced_tables")
            synced_tables: dict[str, SyncedTableConfig] = {}
            seen_synced_lower: dict[str, str] = {}
            if synced_raw is not None:
                if not isinstance(synced_raw, Mapping):
                    raise DiscoveryConfigError(
                        "Lakebase 'synced_tables' must be a mapping"
                    )
                for tbl_name, tbl_spec in synced_raw.items():
                    tbl_name_str = _validate_discovery_logical_table_name(
                        tbl_name,
                        default_catalog=default_catalog,
                        field_label="lakebase.synced_tables",
                    )
                    tbl_lower = tbl_name_str.lower()
                    if tbl_lower in seen_synced_lower:
                        raise DiscoveryConfigError(
                            f"Duplicate case-insensitive synced table '{_safe_disc_label(tbl_name_str)}' in 'lakebase.synced_tables'"
                        )
                    seen_synced_lower[tbl_lower] = tbl_name_str
                    if not isinstance(tbl_spec, Mapping):
                        raise DiscoveryConfigError(
                            f"Synced table spec for '{tbl_name_str}' must be a mapping"
                        )
                    target = tbl_spec.get("target")
                    if not isinstance(target, str) or not target.strip():
                        raise DiscoveryConfigError(
                            f"Synced table '{tbl_name_str}' requires non-empty 'target'"
                        )
                    pk_raw = tbl_spec.get("primary_key", ())
                    if isinstance(pk_raw, str):
                        pk = (pk_raw.strip(),)
                    elif isinstance(pk_raw, (list, tuple)):
                        pk = tuple(str(x).strip() for x in pk_raw if str(x).strip())
                    else:
                        raise DiscoveryConfigError(
                            f"Synced table '{tbl_name_str}' 'primary_key' must be a sequence of strings"
                        )
                    pii_raw = tbl_spec.get("pii", False)
                    if not isinstance(pii_raw, bool):
                        raise DiscoveryConfigError(
                            f"Synced table '{tbl_name_str}' 'pii' must be a boolean"
                        )
                    synced_tables[tbl_name_str] = SyncedTableConfig(
                        target=target.strip(),
                        primary_key=pk,
                        pii=pii_raw,
                    )
            lakebase_cfg = LakebaseConfig(
                engine=lb_eng_str,
                synced_tables=synced_tables,
            )

        # 6b. postgres_sources
        pg_sources_raw = data.get("postgres_sources")
        postgres_sources_dict: dict[str, PostgresSourceConfig] = {}
        if pg_sources_raw is not None:
            if not isinstance(pg_sources_raw, Mapping):
                raise DiscoveryConfigError("'postgres_sources' must be a mapping")
            allowed_src_keys = {
                "account",
                "logical_catalog",
                "schema_map",
                "freshness",
                "schemas",
                "tables",
            }
            for src_eng_raw, src_spec in pg_sources_raw.items():
                if not isinstance(src_eng_raw, str) or not src_eng_raw.strip():
                    raise DiscoveryConfigError(
                        "Engine key in 'postgres_sources' must be a non-empty string"
                    )
                src_eng = src_eng_raw.strip()
                safe_src_eng = _safe_disc_label(src_eng)
                if src_eng not in engines_dict:
                    raise DiscoveryConfigError(
                        f"postgres_sources engine '{safe_src_eng}' is not defined in 'engines'"
                    )
                src_kind = engines_dict[src_eng].kind
                if src_kind not in ("postgres", "lakebase"):
                    raise DiscoveryConfigError(
                        f"postgres_sources engine '{safe_src_eng}' must have kind 'postgres' or 'lakebase', "
                        f"got '{_safe_enum_token(src_kind)}'"
                    )
                if not isinstance(src_spec, Mapping):
                    raise DiscoveryConfigError(
                        f"postgres_sources.{safe_src_eng} must be a mapping"
                    )
                unknown_src = set(src_spec.keys()) - allowed_src_keys
                if unknown_src:
                    raise DiscoveryConfigError(
                        f"Unknown key(s) in postgres_sources.{safe_src_eng}: "
                        f"{sorted(str(k) for k in unknown_src)}"
                    )

                # account
                acc_raw = src_spec.get("account", "interactive")
                if (
                    not isinstance(acc_raw, str)
                    or acc_raw not in VALID_POSTGRES_SOURCE_ACCOUNTS
                ):
                    raise DiscoveryConfigError(
                        f"postgres_sources.{safe_src_eng}.account must be one of "
                        f"{sorted(VALID_POSTGRES_SOURCE_ACCOUNTS)}, got '{_safe_enum_token(acc_raw)}'"
                    )

                # logical_catalog
                raw_log_cat = src_spec.get("logical_catalog")
                if raw_log_cat is None:
                    log_cat = _validate_pg_ident(
                        src_eng,
                        label=f"postgres_sources.{safe_src_eng}.logical_catalog",
                    )
                else:
                    log_cat = _validate_pg_ident(
                        raw_log_cat,
                        label=f"postgres_sources.{safe_src_eng}.logical_catalog",
                    )

                # schema_map
                raw_smap = src_spec.get("schema_map", {})
                if not isinstance(raw_smap, Mapping):
                    raise DiscoveryConfigError(
                        f"postgres_sources.{safe_src_eng}.schema_map must be a mapping"
                    )
                schema_map: dict[str, str] = {}
                for phys_s, log_s in raw_smap.items():
                    ps = _validate_pg_ident(
                        phys_s,
                        label=f"postgres_sources.{safe_src_eng}.schema_map",
                    )
                    ls = _validate_pg_ident(
                        log_s,
                        label=f"postgres_sources.{safe_src_eng}.schema_map",
                    )
                    schema_map[ps] = ls

                # freshness
                default_sync_tbl = (
                    "public._lakebase_sync_state" if src_kind == "lakebase" else None
                )
                if "freshness" not in src_spec:
                    freshness_cfg: PostgresFreshnessConfig | None = (
                        PostgresFreshnessConfig(
                            sync_state_table=default_sync_tbl,
                            freshness_sql=None,
                            sync_state_explicit=False,
                        )
                    )
                elif src_spec["freshness"] is None:
                    freshness_cfg = PostgresFreshnessConfig(
                        sync_state_table=None,
                        freshness_sql=None,
                        sync_state_explicit=True,
                    )
                else:
                    fr_raw = src_spec["freshness"]
                    if not isinstance(fr_raw, Mapping):
                        raise DiscoveryConfigError(
                            f"postgres_sources.{safe_src_eng}.freshness must be a mapping"
                        )
                    unknown_fr = set(fr_raw.keys()) - {
                        "sync_state_table",
                        "freshness_sql",
                    }
                    if unknown_fr:
                        raise DiscoveryConfigError(
                            f"Unknown key(s) in postgres_sources.{safe_src_eng}.freshness: "
                            f"{sorted(str(k) for k in unknown_fr)}"
                        )
                    if "sync_state_table" in fr_raw:
                        sync_tbl = fr_raw["sync_state_table"]
                        sync_explicit = True
                    else:
                        sync_tbl = default_sync_tbl
                        sync_explicit = False
                    fr_sql = fr_raw.get("freshness_sql")
                    freshness_cfg = PostgresFreshnessConfig(
                        sync_state_table=sync_tbl,
                        freshness_sql=fr_sql,
                        sync_state_explicit=sync_explicit,
                    )

                # schemas
                raw_schemas = src_spec.get("schemas")
                if not isinstance(raw_schemas, Mapping) or not raw_schemas:
                    raise DiscoveryConfigError(
                        f"postgres_sources.{safe_src_eng}.schemas must be a non-empty mapping"
                    )
                allowed_sch_keys = {"include", "exclude", "pii", "tokenized"}
                schemas_dict: dict[str, PostgresSchemaConfig] = {}
                for phys_sch_raw, sch_spec in raw_schemas.items():
                    phys_sch = _validate_pg_ident(
                        phys_sch_raw,
                        label=f"postgres_sources.{safe_src_eng}.schemas",
                    )
                    if not isinstance(sch_spec, Mapping):
                        raise DiscoveryConfigError(
                            f"postgres_sources.{safe_src_eng}.schemas.{phys_sch} must be a mapping"
                        )
                    unknown_sch = set(sch_spec.keys()) - allowed_sch_keys
                    if unknown_sch:
                        raise DiscoveryConfigError(
                            f"Unknown key(s) in postgres_sources.{safe_src_eng}.schemas.{phys_sch}: "
                            f"{sorted(str(k) for k in unknown_sch)}"
                        )
                    inc_raw = sch_spec.get("include", ("*",))
                    exc_raw = sch_spec.get("exclude", ())
                    pii_flag = sch_spec.get("pii", False)
                    tok_flag = sch_spec.get("tokenized", False)
                    schemas_dict[phys_sch] = PostgresSchemaConfig(
                        include=inc_raw,
                        exclude=exc_raw,
                        pii=pii_flag,
                        tokenized=tok_flag,
                    )

                # tables overrides
                raw_tables = src_spec.get("tables", {})
                if not isinstance(raw_tables, Mapping):
                    raise DiscoveryConfigError(
                        f"postgres_sources.{safe_src_eng}.tables must be a mapping"
                    )
                allowed_ov_keys = {"pii", "tokenized", "access"}
                tables_dict: dict[str, PostgresTableOverrideConfig] = {}
                seen_ov_lower: dict[str, str] = {}
                for tbl_key, ov_spec in raw_tables.items():
                    if (
                        not isinstance(tbl_key, str)
                        or '"' in tbl_key
                        or "'" in tbl_key
                        or any(ch.isspace() for ch in tbl_key)
                    ):
                        raise DiscoveryConfigError(
                            f"Invalid table key in postgres_sources.{safe_src_eng}.tables"
                        )
                    parts = tbl_key.split(".")
                    if len(parts) != 3 or not all(
                        _PG_IDENT_RE.match(p) for p in parts
                    ):
                        raise DiscoveryConfigError(
                            f"Invalid table override key '{_safe_disc_label(tbl_key)}' in "
                            f"postgres_sources.{safe_src_eng}.tables: expected 3-part "
                            f"'{log_cat}.<schema>.<table>' matching '^[a-z_][a-z0-9_]{{0,62}}$'"
                        )
                    if parts[0] != log_cat:
                        raise DiscoveryConfigError(
                            f"Table override '{_safe_disc_label(tbl_key)}' in "
                            f"postgres_sources.{safe_src_eng}.tables has catalog prefix "
                            f"'{parts[0]}', expected logical_catalog '{log_cat}'"
                        )
                    tl_lower = tbl_key.lower()
                    if tl_lower in seen_ov_lower:
                        raise DiscoveryConfigError(
                            f"Duplicate table override '{_safe_disc_label(tbl_key)}' in "
                            f"postgres_sources.{safe_src_eng}.tables"
                        )
                    seen_ov_lower[tl_lower] = tbl_key
                    if not isinstance(ov_spec, Mapping):
                        raise DiscoveryConfigError(
                            f"Table override '{tbl_key}' in postgres_sources.{safe_src_eng}.tables must be a mapping"
                        )
                    unknown_ov = set(ov_spec.keys()) - allowed_ov_keys
                    if unknown_ov:
                        raise DiscoveryConfigError(
                            f"Unknown key(s) in postgres_sources.{safe_src_eng}.tables['{tbl_key}']: "
                            f"{sorted(str(k) for k in unknown_ov)}"
                        )
                    ov_acc = (
                        _validate_access_override(ov_spec["access"], table_label=tbl_key)
                        if "access" in ov_spec and ov_spec["access"] is not None
                        else None
                    )
                    tables_dict[tbl_key] = PostgresTableOverrideConfig(
                        pii=ov_spec.get("pii"),
                        tokenized=ov_spec.get("tokenized"),
                        access=ov_acc,
                    )

                postgres_sources_dict[src_eng] = PostgresSourceConfig(
                    engine=src_eng,
                    account=acc_raw,
                    logical_catalog=log_cat,
                    schema_map=schema_map,
                    freshness=freshness_cfg,
                    schemas=schemas_dict,
                    tables=tables_dict,
                )

        # 7. pii_tables and policy keys
        pii_tables_raw = data.get("pii_tables", ())
        if not isinstance(pii_tables_raw, (list, tuple)):
            raise DiscoveryConfigError(
                "'pii_tables' must be a list or tuple of non-empty table name strings"
            )
        allowed_pii_catalogs = frozenset(
            {default_catalog}
            | {src.logical_catalog for src in postgres_sources_dict.values()}
        )
        pii_tables_list: list[str] = []
        for pt in pii_tables_raw:
            pii_tables_list.append(
                _validate_discovery_logical_table_name(
                    pt,
                    default_catalog=default_catalog,
                    field_label="pii_tables",
                    allowed_catalogs=allowed_pii_catalogs,
                )
            )
        pii_tables = tuple(pii_tables_list)

        if lakekeeper_cfg is None:
            if pii_tables and not postgres_sources_dict:
                raise DiscoveryConfigError(
                    "'pii_tables' cannot be configured when 'lakekeeper' is not configured"
                )
            if lakebase_cfg is not None and any(
                st.pii for st in lakebase_cfg.synced_tables.values()
            ):
                raise DiscoveryConfigError(
                    "Synced tables with 'pii: true' cannot be configured when 'lakekeeper' is not configured"
                )

        def _parse_engine_names(seq: Any, label: str) -> tuple[str, ...]:
            if not isinstance(seq, (list, tuple)):
                raise DiscoveryConfigError(
                    f"'{label}' must be a list or tuple of non-empty engine name strings"
                )
            names: list[str] = []
            for item in seq:
                if not isinstance(item, str) or not item.strip():
                    raise DiscoveryConfigError(
                        f"'{label}' must contain only non-empty engine name strings"
                    )
                names.append(item.strip())
            return tuple(names)

        def _parse_int_policy(
            val: Any,
            *,
            label: str,
            min_value: int,
        ) -> int:
            if isinstance(val, bool) or not isinstance(val, int) or val < min_value:
                raise DiscoveryConfigError(
                    f"Invalid '{label}': must be an integer >= {min_value}"
                )
            return val

        policy_raw = data.get("policy")
        explicit_policy_keys: set[str] = set()
        if policy_raw is not None:
            if not isinstance(policy_raw, Mapping):
                raise DiscoveryConfigError("'policy' must be a mapping")
            allowed_policy_keys = {
                "require_filter_min_rows",
                "max_in_list",
                "max_offset",
                "guard_mode",
                "pii_engines",
                "pii_object_storage_engines",
                "point_lookup_max_keys",
                "analytics_min_rows",
                "small_scan_order",
                "analytics_order",
            }
            unknown_pol = set(policy_raw.keys()) - allowed_policy_keys
            if unknown_pol:
                raise DiscoveryConfigError(
                    f"Unknown key(s) in 'policy': {sorted(str(k) for k in unknown_pol)}"
                )
            explicit_policy_keys.update(str(k) for k in policy_raw.keys())

        # pii_engines (top-level and/or policy.pii_engines)
        has_top_pe = "pii_engines" in data
        has_pol_pe = isinstance(policy_raw, Mapping) and "pii_engines" in policy_raw
        top_pe = (
            _parse_engine_names(data["pii_engines"], "pii_engines")
            if has_top_pe
            else ()
        )
        pol_pe = (
            _parse_engine_names(policy_raw["pii_engines"], "policy.pii_engines")
            if has_pol_pe and isinstance(policy_raw, Mapping)
            else ()
        )
        if has_top_pe and has_pol_pe and top_pe != pol_pe:
            raise DiscoveryConfigError(
                "Conflicting 'pii_engines' and 'policy.pii_engines' in discovery config"
            )
        pii_engines = top_pe if has_top_pe else pol_pe
        if has_top_pe:
            explicit_policy_keys.add("pii_engines")

        # pii_object_storage_engines (top-level and/or policy.pii_object_storage_engines)
        has_top_pose = "pii_object_storage_engines" in data
        has_pol_pose = (
            isinstance(policy_raw, Mapping)
            and "pii_object_storage_engines" in policy_raw
        )
        top_pose = (
            _parse_engine_names(
                data["pii_object_storage_engines"], "pii_object_storage_engines"
            )
            if has_top_pose
            else ()
        )
        pol_pose = (
            _parse_engine_names(
                policy_raw["pii_object_storage_engines"],
                "policy.pii_object_storage_engines",
            )
            if has_pol_pose and isinstance(policy_raw, Mapping)
            else ()
        )
        if has_top_pose and has_pol_pose and top_pose != pol_pose:
            raise DiscoveryConfigError(
                "Conflicting 'pii_object_storage_engines' and 'policy.pii_object_storage_engines' in discovery config"
            )
        pii_object_storage_engines = top_pose if has_top_pose else pol_pose
        if has_top_pose:
            explicit_policy_keys.add("pii_object_storage_engines")
        if engines_dict:
            for pose_eng in pii_object_storage_engines:
                if pose_eng not in engines_dict:
                    raise DiscoveryConfigError(
                        f"Policy 'pii_object_storage_engines' references undefined engine '{_safe_disc_label(pose_eng)}'"
                    )

        def _resolve_scalar_policy(key: str, default: Any) -> Any:
            in_top = key in data
            in_pol = isinstance(policy_raw, Mapping) and key in policy_raw
            if in_top:
                explicit_policy_keys.add(key)
            if in_top and in_pol and isinstance(policy_raw, Mapping):
                if data[key] != policy_raw[key]:
                    raise DiscoveryConfigError(
                        f"Conflicting '{key}' and 'policy.{key}' in discovery config"
                    )
                return policy_raw[key]
            if in_pol and isinstance(policy_raw, Mapping):
                return policy_raw[key]
            if in_top:
                return data[key]
            return default

        require_filter_min_rows = _parse_int_policy(
            _resolve_scalar_policy("require_filter_min_rows", 100_000),
            label="policy.require_filter_min_rows",
            min_value=0,
        )
        max_in_list = _parse_int_policy(
            _resolve_scalar_policy("max_in_list", 1_000),
            label="policy.max_in_list",
            min_value=1,
        )
        max_offset = _parse_int_policy(
            _resolve_scalar_policy("max_offset", 10_000),
            label="policy.max_offset",
            min_value=0,
        )
        raw_guard_mode = _resolve_scalar_policy("guard_mode", "enforce")
        if not isinstance(raw_guard_mode, str) or raw_guard_mode not in VALID_GUARD_MODES:
            raise DiscoveryConfigError(
                f"Invalid 'policy.guard_mode': '{_safe_enum_token(raw_guard_mode)}', expected 'enforce' or 'warn'"
            )

        return cls(
            default_catalog=default_catalog,
            refresh_interval_seconds=refresh_interval,
            timeout_seconds=timeout_seconds,
            lakekeeper=lakekeeper_cfg,
            engines=engines_dict,
            lakebase=lakebase_cfg,
            postgres_sources=postgres_sources_dict,
            pii_tables=pii_tables,
            pii_engines=pii_engines,
            pii_object_storage_engines=pii_object_storage_engines,
            require_filter_min_rows=require_filter_min_rows,
            max_in_list=max_in_list,
            max_offset=max_offset,
            guard_mode=raw_guard_mode,
            _explicit_policy_keys=frozenset(explicit_policy_keys),
        )

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> DiscoveryConfig:
        """Construct DiscoveryConfig from a YAML file."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.load(f, Loader=UniqueKeySafeLoader)
        except FileNotFoundError as exc:
            raise DiscoveryConfigError(f"Config file not found: {path}") from exc
        except ConstructorError as exc:
            mark = getattr(exc, "problem_mark", None)
            loc = f" at line {mark.line + 1}, column {mark.column + 1}" if mark is not None else ""
            if hasattr(exc, "duplicate_key") or "duplicate YAML key" in str(getattr(exc, "problem", "")):
                dup_key = getattr(exc, "duplicate_key", None)
                safe_dup = _safe_disc_label(str(dup_key)) if dup_key is not None else "<redacted>"
                raise DiscoveryConfigError(
                    f"Invalid YAML in config file '{path}'{loc}: duplicate YAML key '{safe_dup}'"
                ) from exc
            raise DiscoveryConfigError(f"Invalid YAML in config file '{path}'{loc}") from exc
        except yaml.YAMLError as exc:
            mark = getattr(exc, "problem_mark", None)
            loc = f" at line {mark.line + 1}, column {mark.column + 1}" if mark is not None else ""
            raise DiscoveryConfigError(f"Invalid YAML in config file '{path}'{loc}") from exc
        except Exception as exc:
            raise DiscoveryConfigError(f"Failed to read config file: {path}") from exc

        if not isinstance(data, Mapping):
            raise DiscoveryConfigError(
                f"Config file must contain a mapping, got {type(data).__name__}"
            )
        if "placement" in data and isinstance(data["placement"], Mapping):
            pl_map = data["placement"]
            if "discovery" not in pl_map or not isinstance(
                pl_map["discovery"], Mapping
            ):
                raise DiscoveryConfigError(
                    f"Config file '{path}' contains 'placement' without a 'discovery' mapping"
                )
            disc_map = dict(pl_map["discovery"])
            if "default_catalog" not in disc_map and "default_catalog" in data:
                disc_map["default_catalog"] = data["default_catalog"]
            return cls.from_dict(disc_map)
        return cls.from_dict(data)

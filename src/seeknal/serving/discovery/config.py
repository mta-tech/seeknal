"""Configuration model and validation for Seeknal Serving discovery."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

import yaml


class DiscoveryError(Exception):
    """Base exception for all discovery failures."""


class DiscoveryConfigError(DiscoveryError):
    """Raised when discovery configuration is invalid."""


@dataclass(frozen=True)
class LakekeeperConfig:
    """Configuration for Iceberg REST catalog (Lakekeeper)."""

    url: str
    warehouse: str
    token_env: str | None = None
    namespaces: tuple[str, ...] | None = None


@dataclass(frozen=True)
class EngineConfig:
    """Engine configuration within discovery."""

    name: str
    kind: str
    dialect: str
    catalog: str | None = None
    url: str | None = None
    token_env: str | None = None


@dataclass(frozen=True)
class SyncedTableConfig:
    """Synced table mapping for Lakebase."""

    target: str
    primary_key: tuple[str, ...] = ()


@dataclass(frozen=True)
class LakebaseConfig:
    """Lakebase replication and serving configuration."""

    engine: str
    synced_tables: Mapping[str, SyncedTableConfig] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveryConfig:
    """Top-level discovery configuration."""

    default_catalog: str
    refresh_interval_seconds: float = 60.0
    timeout_seconds: float = 10.0
    lakekeeper: LakekeeperConfig | None = None
    engines: Mapping[str, EngineConfig] = field(default_factory=dict)
    lakebase: LakebaseConfig | None = None

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

                kind = eng_data.get("kind")
                if not isinstance(kind, str) or not kind.strip():
                    raise DiscoveryConfigError(
                        f"Engine '{eng_name_str}' missing required field 'kind'"
                    )
                kind_str = kind.strip()
                if kind_str not in ("starrocks", "cube", "lakebase", "duckdb"):
                    raise DiscoveryConfigError(
                        f"Unknown engine kind '{kind_str}' for engine '{eng_name_str}'"
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

                engines_dict[eng_name_str] = EngineConfig(
                    name=eng_name_str,
                    kind=kind_str,
                    dialect=dialect_str,
                    catalog=catalog_str,
                    url=url_str,
                    token_env=token_env_str,
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
                token_env=token_env.strip() if token_env else None,
                namespaces=namespaces,
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
            if synced_raw is not None:
                if not isinstance(synced_raw, Mapping):
                    raise DiscoveryConfigError(
                        "Lakebase 'synced_tables' must be a mapping"
                    )
                for tbl_name, tbl_spec in synced_raw.items():
                    if not isinstance(tbl_name, str) or not tbl_name.strip():
                        raise DiscoveryConfigError(
                            "Synced table name must be a non-empty string"
                        )
                    tbl_name_str = tbl_name.strip()
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
                    synced_tables[tbl_name_str] = SyncedTableConfig(
                        target=target.strip(),
                        primary_key=pk,
                    )
            lakebase_cfg = LakebaseConfig(
                engine=lb_eng_str,
                synced_tables=synced_tables,
            )

        return cls(
            default_catalog=default_catalog,
            refresh_interval_seconds=refresh_interval,
            timeout_seconds=timeout_seconds,
            lakekeeper=lakekeeper_cfg,
            engines=engines_dict,
            lakebase=lakebase_cfg,
        )

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> DiscoveryConfig:
        """Construct DiscoveryConfig from a YAML file."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError as exc:
            raise DiscoveryConfigError(f"Config file not found: {path}") from exc
        except yaml.YAMLError as exc:
            raise DiscoveryConfigError(f"Invalid YAML in config file: {exc}") from exc
        except Exception as exc:
            raise DiscoveryConfigError(f"Failed to read config file: {exc}") from exc

        if not isinstance(data, Mapping):
            raise DiscoveryConfigError(
                f"Config file must contain a mapping, got {type(data).__name__}"
            )
        return cls.from_dict(data)

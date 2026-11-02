"""Catalog discovery builder for Seeknal Serving."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    EngineConfig,
)
from seeknal.serving.discovery.cube import CubeMeta, CubeSource, _sanitize
from seeknal.serving.discovery.lakebase import LakebaseSyncSource, SyncState
from seeknal.serving.discovery.lakekeeper import IcebergTable, LakekeeperSource
from seeknal.serving.placement import Placement


@dataclass(frozen=True)
class DiscoveryResult:
    """Result of catalog discovery containing placement, schemas, and diagnostics."""

    placement: dict
    schemas: dict[str, list[str]]
    warnings: tuple[str, ...]
    discovered_at: datetime


def discover(
    config: DiscoveryConfig | Mapping | str | os.PathLike,
    *,
    http_client: httpx.Client,
    lakebase_connect: Callable[[], Any] | None = None,
    env: Mapping[str, str] = os.environ,
    now: datetime | None = None,
) -> DiscoveryResult:
    """Discover tables, schemas, and cubes to construct Placement and schemas."""
    # 1. Resolve config
    if isinstance(config, (str, os.PathLike)):
        cfg = DiscoveryConfig.from_yaml(config)
    elif isinstance(config, Mapping) and not isinstance(config, DiscoveryConfig):
        cfg = DiscoveryConfig.from_dict(config)
    elif isinstance(config, DiscoveryConfig):
        cfg = config
    else:
        raise TypeError(f"Unsupported config type: {type(config).__name__}")

    # 2. Resolve discovery timestamp
    if now is None:
        now_dt = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now_dt = now.replace(tzinfo=timezone.utc)
    else:
        now_dt = now.astimezone(timezone.utc)

    warnings: list[str] = []

    # 3. Discover Iceberg tables (Lakekeeper)
    iceberg_tables: list[IcebergTable] = []
    if cfg.lakekeeper is not None:
        lk_source = LakekeeperSource(
            cfg.lakekeeper,
            http_client,
            env=env,
            timeout=cfg.timeout_seconds,
        )
        iceberg_tables = lk_source.discover()
        warnings.extend(lk_source.warnings)

    # 4. Read Lakebase sync state
    lakebase_state: dict[str, SyncState] = {}
    if cfg.lakebase is not None and lakebase_connect is not None:
        lb_source = LakebaseSyncSource(lakebase_connect)
        lakebase_state = lb_source.read_state()

    # 5. Discover Cubes
    cube_engine_name: str | None = None
    cube_metas: list[CubeMeta] = []
    for eng_name, eng in cfg.engines.items():
        if eng.kind == "cube":
            cube_engine_name = eng_name
            break

    if cube_engine_name is not None:
        try:
            cube_source = CubeSource(
                cfg.engines[cube_engine_name],
                http_client,
                env=env,
                timeout=cfg.timeout_seconds,
            )
            cube_metas = cube_source.discover()
            warnings.extend(cube_source.warnings)
        except Exception as exc:
            cube_metas = []
            eng = cfg.engines[cube_engine_name]
            tok = env.get(eng.token_env) if eng.token_env else None
            sec = None
            if eng.api_secret_env:
                sec = env.get(eng.api_secret_env)
            elif eng.api_secret_file and os.path.exists(eng.api_secret_file):
                try:
                    with open(eng.api_secret_file, "r", encoding="utf-8") as f:
                        sec = f.read().strip()
                except Exception:
                    pass
            clean_err = _sanitize(str(exc), tok, sec)
            warnings.append(
                f"Cube metadata was unavailable ({clean_err}); cubes were omitted from placement and schemas"
            )

    # 6. Locate StarRocks engine
    starrocks_engine: EngineConfig | None = None
    for eng_name, eng in cfg.engines.items():
        if eng.kind == "starrocks":
            starrocks_engine = eng
            break

    # 7. Build engines placement dict (kind and dialect only)
    engines_placement: dict[str, dict[str, str]] = {}
    for eng_name, eng in cfg.engines.items():
        engines_placement[eng_name] = {
            "kind": eng.kind,
            "dialect": eng.dialect,
        }

    # 8. Build tables placement and schemas
    tables_placement: dict[str, Any] = {}
    schemas_placement: dict[str, list[str]] = {}
    iceberg_logical_names: set[str] = set()

    for it in iceberg_tables:
        logical = f"{cfg.default_catalog}.{it.namespace}.{it.name}"
        iceberg_logical_names.add(logical)

        # Determine keys
        if it.keys:
            keys = list(it.keys)
        elif (
            cfg.lakebase is not None
            and logical in cfg.lakebase.synced_tables
            and cfg.lakebase.synced_tables[logical].primary_key
        ):
            keys = list(cfg.lakebase.synced_tables[logical].primary_key)
        else:
            keys = []

        # Determine serve
        serve: dict[str, Any] = {}

        # StarRocks serve
        if starrocks_engine is not None:
            sr_catalog = starrocks_engine.catalog or cfg.default_catalog
            serve[starrocks_engine.name] = {
                "name": f"{sr_catalog}.{it.namespace}.{it.name}"
            }

        # Lakebase serve
        if cfg.lakebase is not None and logical in cfg.lakebase.synced_tables:
            synced_cfg = cfg.lakebase.synced_tables[logical]
            target = synced_cfg.target
            if target in lakebase_state:
                sync_state = lakebase_state[target]
                if (
                    sync_state.snapshot_id is not None
                    and it.current_snapshot_id is not None
                    and sync_state.snapshot_id == it.current_snapshot_id
                ):
                    observed_lag = 0.0
                else:
                    diff = (now_dt - sync_state.synced_at).total_seconds()
                    observed_lag = max(0.0, diff)

                max_lag = observed_lag + cfg.refresh_interval_seconds
                serve[cfg.lakebase.engine] = {
                    "name": target,
                    "max_lag_seconds": max_lag,
                }
            else:
                warnings.append(
                    f"Synced table '{logical}' has no state row in _lakebase_sync_state for target '{target}'"
                )

        if serve:
            tables_placement[logical] = {
                "keys": keys,
                "row_count": it.row_count,
                "serve": serve,
            }
            schemas_placement[logical] = list(it.columns)
        else:
            warnings.append(
                f"Table '{logical}' has no serve and was omitted from placement"
            )

    # Check for synced tables missing from Iceberg
    if cfg.lakebase is not None:
        for synced_logical in cfg.lakebase.synced_tables:
            if synced_logical not in iceberg_logical_names:
                warnings.append(
                    f"Synced table '{synced_logical}' not found in discovered Iceberg tables"
                )

    # 9. Build cubes placement dict and schemas
    cubes_placement: dict[str, Any] = {}
    if cube_engine_name is not None:
        for cm in cube_metas:
            cubes_placement[cm.name] = {
                "engine": cube_engine_name,
                "measures": list(cm.measures),
                "dimensions": list(cm.dimensions),
                "mask_variants": {k: list(v) for k, v in cm.mask_variants.items()},
            }
            schemas_placement[cm.name] = list(cm.dimensions) + list(cm.measures)

    # 10. Assemble and validate Placement
    placement_dict = {
        "engines": engines_placement,
        "tables": tables_placement,
        "cubes": cubes_placement,
    }

    # Validate by instantiating Placement
    Placement.from_dict(placement_dict)

    return DiscoveryResult(
        placement=placement_dict,
        schemas=schemas_placement,
        warnings=tuple(warnings),
        discovered_at=now_dt,
    )

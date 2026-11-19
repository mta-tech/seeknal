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
    DiscoveryError,
    EngineConfig,
    _safe_disc_label,
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

    # 6. Locate StarRocks and DuckDB iceberg_direct engines
    starrocks_engine: EngineConfig | None = None
    iceberg_direct_engines: list[EngineConfig] = []
    for eng_name, eng in cfg.engines.items():
        if eng.kind == "starrocks":
            starrocks_engine = eng
            break
    for eng_name, eng in cfg.engines.items():
        if eng.kind == "duckdb" and eng.profile == "iceberg_direct":
            iceberg_direct_engines.append(eng)

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
    iceberg_logical_names = {
        f"{cfg.default_catalog}.{it.namespace}.{it.name}" for it in iceberg_tables
    }
    iceberg_lower = {n.lower(): n for n in iceberg_logical_names}

    # Fail closed if any configured PII table entry matches no discovered Iceberg table
    unmatched_pii: list[str] = []
    seen_unmatched_lower: set[str] = set()
    for pt in cfg.pii_tables:
        if pt.lower() not in iceberg_lower and pt.lower() not in seen_unmatched_lower:
            unmatched_pii.append(_safe_disc_label(pt))
            seen_unmatched_lower.add(pt.lower())
    if cfg.lakebase is not None:
        for logical_name, st_cfg in cfg.lakebase.synced_tables.items():
            if (
                st_cfg.pii is True
                and logical_name.lower() not in iceberg_lower
                and logical_name.lower() not in seen_unmatched_lower
            ):
                unmatched_pii.append(_safe_disc_label(logical_name))
                seen_unmatched_lower.add(logical_name.lower())
    if unmatched_pii:
        unmatched_fmt = ", ".join(f"'{u}'" for u in unmatched_pii)
        raise DiscoveryError(
            f"Configured PII table(s) matched no discovered Iceberg table: {unmatched_fmt}"
        )

    pii_tables_lower = {t.lower() for t in cfg.pii_tables}
    synced_by_lower = (
        {k.lower(): v for k, v in cfg.lakebase.synced_tables.items()}
        if cfg.lakebase is not None
        else {}
    )
    synced_pii_lower = {
        k_lower for k_lower, st_cfg in synced_by_lower.items() if st_cfg.pii is True
    }
    pii_engines_set = set(cfg.pii_engines)

    for it in iceberg_tables:
        logical = f"{cfg.default_catalog}.{it.namespace}.{it.name}"
        logical_lower = logical.lower()
        is_pii = (
            bool(getattr(it, "pii", False))
            or logical_lower in pii_tables_lower
            or logical_lower in synced_pii_lower
        )

        # Determine keys
        synced_entry = synced_by_lower.get(logical_lower)
        if it.keys:
            keys = list(it.keys)
        elif synced_entry is not None and synced_entry.primary_key:
            keys = list(synced_entry.primary_key)
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

        # DuckDB iceberg_direct serve(s)
        for id_eng in iceberg_direct_engines:
            id_alias = id_eng.alias or id_eng.catalog or cfg.default_catalog
            if it.snapshot_ms is not None:
                snap_dt = datetime.fromtimestamp(it.snapshot_ms / 1000.0, tz=timezone.utc)
                observed_snap_age = max(0.0, (now_dt - snap_dt).total_seconds())
                id_max_lag: float | None = observed_snap_age + cfg.refresh_interval_seconds
            else:
                id_max_lag = cfg.refresh_interval_seconds

            id_serve: dict[str, Any] = {
                "name": f"{id_alias}.{it.namespace}.{it.name}",
                "max_lag_seconds": id_max_lag,
            }
            if id_eng.pin_current_snapshot and it.current_snapshot_id is not None:
                id_serve["snapshot_id"] = it.current_snapshot_id
            serve[id_eng.name] = id_serve

        # Lakebase serve
        if cfg.lakebase is not None and synced_entry is not None:
            target = synced_entry.target
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

        # Single-serve selection for PII tables when multiple engines could serve the table
        if is_pii and len(serve) > 1:
            chosen_eng: str | None = None
            if cfg.lakebase is not None and cfg.lakebase.engine in serve:
                chosen_eng = cfg.lakebase.engine
            if chosen_eng is None:
                for s_eng in serve:
                    if s_eng in cfg.engines and cfg.engines[s_eng].kind != "duckdb":
                        chosen_eng = s_eng
                        break
            if chosen_eng is None:
                for s_eng in serve:
                    if s_eng in pii_engines_set:
                        chosen_eng = s_eng
                        break
            if chosen_eng is None:
                chosen_eng = next(iter(serve))
            serve = {chosen_eng: serve[chosen_eng]}

        if serve:
            tbl_entry_dict: dict[str, Any] = {
                "keys": keys,
                "row_count": it.row_count,
                "serve": serve,
            }
            if is_pii:
                tbl_entry_dict["pii"] = True
            tables_placement[logical] = tbl_entry_dict
            schemas_placement[logical] = list(it.columns)
        else:
            warnings.append(
                f"Table '{logical}' has no serve and was omitted from placement"
            )

    # Check for non-PII synced tables missing from Iceberg
    if cfg.lakebase is not None:
        for synced_logical in cfg.lakebase.synced_tables:
            if synced_logical.lower() not in iceberg_lower:
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
    placement_dict: dict[str, Any] = {
        "engines": engines_placement,
        "tables": tables_placement,
        "cubes": cubes_placement,
    }
    if cfg.pii_engines:
        placement_dict["policy"] = {"pii_engines": list(cfg.pii_engines)}

    # Validate by instantiating Placement
    Placement.from_dict(placement_dict)

    return DiscoveryResult(
        placement=placement_dict,
        schemas=schemas_placement,
        warnings=tuple(warnings),
        discovered_at=now_dt,
    )

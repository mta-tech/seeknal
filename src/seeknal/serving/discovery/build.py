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
    PostgresSourceConfig,
    _safe_disc_label,
)
from seeknal.serving.discovery.cube import CubeMeta, CubeSource, _sanitize
from seeknal.serving.discovery.lakebase import LakebaseSyncSource, SyncState
from seeknal.serving.discovery.lakekeeper import IcebergTable, LakekeeperSource
from seeknal.serving.discovery.postgres_introspect import (
    PostgresIntrospectedTable,
    PostgresIntrospectionResult,
    introspect_postgres_source,
)
from seeknal.serving.placement import VALID_ENGINE_KINDS, Placement


@dataclass(frozen=True)
class DiscoveryResult:
    """Result of catalog discovery containing placement, schemas, and diagnostics."""

    placement: dict
    schemas: dict[str, list[str]]
    warnings: tuple[str, ...]
    discovered_at: datetime


def _resolve_postgres_connect(
    eng_name: str,
    src_cfg: PostgresSourceConfig,
    *,
    postgres_connects: Mapping[str, Any] | None,
    lakebase_connect: Callable[[], Any] | None,
) -> Callable[[], Any]:
    """Resolve the DB-API connect callable for a postgres_sources entry."""
    if postgres_connects is not None:
        dotted = f"{eng_name}.{src_cfg.account}"
        if dotted in postgres_connects and callable(postgres_connects[dotted]):
            return postgres_connects[dotted]
        if eng_name in postgres_connects:
            entry = postgres_connects[eng_name]
            if isinstance(entry, Mapping):
                if src_cfg.account in entry and callable(entry[src_cfg.account]):
                    return entry[src_cfg.account]
                raise DiscoveryError(
                    f"Account '{src_cfg.account}' not found in postgres_connects for engine "
                    f"'{_safe_disc_label(eng_name)}'"
                )
            if callable(entry):
                return entry
    if lakebase_connect is not None:
        return lakebase_connect
    raise DiscoveryError(
        f"No connection factory provided for postgres_sources engine '{_safe_disc_label(eng_name)}'"
    )


def _resolve_source_connection_traits(
    eng_name: str,
    eng_kind: str,
    connections: Mapping[str, Any] | None,
) -> tuple[bool, str | None]:
    """Determine (read_only, endpoint_type) for a postgres or lakebase source."""
    conn_obj = connections.get(eng_name) if connections is not None else None
    endpoint_type: str | None = None
    mode: str | None = None

    if isinstance(conn_obj, Mapping):
        mode_raw = conn_obj.get("mode")
        if isinstance(mode_raw, str):
            mode = mode_raw
        lb_sub = conn_obj.get("lakebase")
        if isinstance(lb_sub, Mapping):
            ep_raw = lb_sub.get("endpoint_type", "read_write")
            if isinstance(ep_raw, str):
                endpoint_type = ep_raw
        elif isinstance(conn_obj.get("endpoint_type"), str):
            endpoint_type = conn_obj["endpoint_type"]
    elif conn_obj is not None:
        mode_attr = getattr(conn_obj, "mode", None)
        if isinstance(mode_attr, str):
            mode = mode_attr
        lb_sub = getattr(conn_obj, "lakebase", None)
        if lb_sub is not None:
            ep_attr = getattr(lb_sub, "endpoint_type", "read_write")
            if isinstance(ep_attr, str):
                endpoint_type = ep_attr
        elif isinstance(getattr(conn_obj, "endpoint_type", None), str):
            endpoint_type = getattr(conn_obj, "endpoint_type")

    if eng_kind == "postgres":
        read_only = mode != "read_write"
    else:
        read_only = endpoint_type == "read_only"

    return read_only, endpoint_type


def _resolve_effective_storage_class(
    eng_name: str,
    eng_cfg: EngineConfig,
    *,
    storage_classes: Mapping[str, str] | None,
    connections: Mapping[str, Any] | None,
) -> str:
    """Resolve effective storage_class ('local' | 'object_storage' | 'unknown') per §2.3."""
    if storage_classes is not None and eng_name in storage_classes:
        return str(storage_classes[eng_name])

    if connections is not None and eng_name in connections:
        conn_obj = connections[eng_name]
        try:
            from seeknal.serving.lakebase_endpoint import effective_storage_class

            if not isinstance(conn_obj, Mapping):
                return str(effective_storage_class(conn_obj))
        except Exception:
            pass

        explicit_sc = (
            conn_obj.get("storage_class")
            if isinstance(conn_obj, Mapping)
            else getattr(conn_obj, "storage_class", None)
        )
        if isinstance(explicit_sc, str) and explicit_sc.strip():
            return explicit_sc.strip()

        if eng_cfg.kind == "postgres":
            return "local"
        if eng_cfg.kind == "lakebase":
            lb_block = (
                conn_obj.get("lakebase")
                if isinstance(conn_obj, Mapping)
                else getattr(conn_obj, "lakebase", None)
            )
            return "object_storage" if lb_block is not None else "unknown"
        if eng_cfg.kind == "duckdb":
            prof = (
                conn_obj.get("profile")
                if isinstance(conn_obj, Mapping)
                else getattr(conn_obj, "profile", None)
            ) or eng_cfg.profile or "local_file"
            return "local" if prof == "local_file" else "object_storage"
        return "object_storage"

    if eng_cfg.kind == "postgres":
        return "local"
    if eng_cfg.kind == "duckdb" and eng_cfg.profile == "local_file":
        return "local"
    return "object_storage"


def discover(
    config: DiscoveryConfig | Mapping | str | os.PathLike,
    *,
    http_client: httpx.Client | None = None,
    lakebase_connect: Callable[[], Any] | None = None,
    postgres_connects: Mapping[str, Any] | None = None,
    storage_classes: Mapping[str, str] | None = None,
    connections: Mapping[str, Any] | None = None,
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

    cube_engine_name: str | None = None
    for eng_name, eng in cfg.engines.items():
        if eng.kind == "cube":
            cube_engine_name = eng_name
            break

    owns_http_client = False
    client = http_client
    if client is None and (cfg.lakekeeper is not None or cube_engine_name is not None):
        client = httpx.Client(timeout=cfg.timeout_seconds)
        owns_http_client = True

    try:
        return _discover_impl(
            cfg,
            http_client=client,
            cube_engine_name=cube_engine_name,
            lakebase_connect=lakebase_connect,
            postgres_connects=postgres_connects,
            storage_classes=storage_classes,
            connections=connections,
            env=env,
            now_dt=now_dt,
        )
    finally:
        if owns_http_client and client is not None:
            client.close()


def _discover_impl(
    cfg: DiscoveryConfig,
    *,
    http_client: httpx.Client | None,
    cube_engine_name: str | None,
    lakebase_connect: Callable[[], Any] | None,
    postgres_connects: Mapping[str, Any] | None,
    storage_classes: Mapping[str, str] | None,
    connections: Mapping[str, Any] | None,
    env: Mapping[str, str],
    now_dt: datetime,
) -> DiscoveryResult:
    warnings: list[str] = []

    # 3. Discover Iceberg tables (Lakekeeper)
    iceberg_tables: list[IcebergTable] = []
    if cfg.lakekeeper is not None:
        assert http_client is not None
        lk_source = LakekeeperSource(
            cfg.lakekeeper,
            http_client,
            env=env,
            timeout=cfg.timeout_seconds,
        )
        iceberg_tables = lk_source.discover()
        warnings.extend(lk_source.warnings)

    # 4. Read legacy Lakebase sync state
    lakebase_state: dict[str, SyncState] = {}
    if cfg.lakebase is not None:
        lb_conn_fn = lakebase_connect
        if (
            lb_conn_fn is None
            and postgres_connects is not None
            and cfg.lakebase.engine in postgres_connects
            and callable(postgres_connects[cfg.lakebase.engine])
        ):
            lb_conn_fn = postgres_connects[cfg.lakebase.engine]
        if lb_conn_fn is not None:
            lb_source = LakebaseSyncSource(lb_conn_fn)
            lakebase_state = lb_source.read_state()

    # Prepare Iceberg snapshot IDs and PII hints for Postgres introspection
    iceberg_snapshot_ids: dict[str, int | None] = {}
    extra_pii_names: set[str] = {t.lower() for t in cfg.pii_tables}
    if cfg.lakebase is not None:
        for logical_k, st_cfg in cfg.lakebase.synced_tables.items():
            if st_cfg.pii is True:
                extra_pii_names.add(logical_k.lower())
    for it in iceberg_tables:
        logical_it = f"{cfg.default_catalog}.{it.namespace}.{it.name}"
        iceberg_snapshot_ids[logical_it] = it.current_snapshot_id
        if bool(getattr(it, "pii", False)):
            extra_pii_names.add(logical_it.lower())

    # 4b. Introspect configured postgres_sources
    pg_results: dict[str, PostgresIntrospectionResult] = {}
    pg_tables_by_lower: dict[str, list[tuple[str, PostgresIntrospectedTable]]] = {}
    for pg_eng_name, src_cfg in cfg.postgres_sources.items():
        pg_eng = cfg.engines[pg_eng_name]
        conn_fn = _resolve_postgres_connect(
            pg_eng_name,
            src_cfg,
            postgres_connects=postgres_connects,
            lakebase_connect=lakebase_connect,
        )
        read_only, endpoint_type = _resolve_source_connection_traits(
            pg_eng_name,
            pg_eng.kind,
            connections,
        )
        pg_res = introspect_postgres_source(
            conn_fn,
            src_cfg,
            engine_kind=pg_eng.kind,
            read_only=read_only,
            endpoint_type=endpoint_type,
            require_filter_min_rows=cfg.require_filter_min_rows,
            now=now_dt,
            refresh_interval_seconds=cfg.refresh_interval_seconds,
            iceberg_snapshot_ids=iceberg_snapshot_ids,
            extra_pii_tables=extra_pii_names,
        )
        pg_results[pg_eng_name] = pg_res
        warnings.extend(pg_res.warnings)
        for pg_tbl in pg_res.tables.values():
            pg_tables_by_lower.setdefault(pg_tbl.logical_name.lower(), []).append(
                (pg_eng_name, pg_tbl)
            )

    # 5. Discover Cubes
    cube_metas: list[CubeMeta] = []
    if cube_engine_name is not None:
        assert http_client is not None
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

    # 8. Build tables placement and schemas (Lakekeeper + merge with postgres_sources)
    tables_placement: dict[str, Any] = {}
    schemas_placement: dict[str, list[str]] = {}
    iceberg_logical_names = {
        f"{cfg.default_catalog}.{it.namespace}.{it.name}" for it in iceberg_tables
    }
    iceberg_lower = {n.lower(): n for n in iceberg_logical_names}

    # Fail closed if any configured PII table entry matches no discovered table
    unmatched_pii: list[str] = []
    seen_unmatched_lower: set[str] = set()
    for pt in cfg.pii_tables:
        pt_l = pt.lower()
        if (
            pt_l not in iceberg_lower
            and pt_l not in pg_tables_by_lower
            and pt_l not in seen_unmatched_lower
        ):
            unmatched_pii.append(_safe_disc_label(pt))
            seen_unmatched_lower.add(pt_l)
    if cfg.lakebase is not None:
        for logical_name, st_cfg in cfg.lakebase.synced_tables.items():
            ln_l = logical_name.lower()
            if (
                st_cfg.pii is True
                and ln_l not in iceberg_lower
                and ln_l not in seen_unmatched_lower
            ):
                unmatched_pii.append(_safe_disc_label(logical_name))
                seen_unmatched_lower.add(ln_l)
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
    handled_pg_lowers: set[str] = set()

    for it in iceberg_tables:
        logical = f"{cfg.default_catalog}.{it.namespace}.{it.name}"
        logical_lower = logical.lower()
        lake_is_pii = (
            bool(getattr(it, "pii", False))
            or logical_lower in pii_tables_lower
            or logical_lower in synced_pii_lower
        )
        pg_matches = pg_tables_by_lower.get(logical_lower, [])

        # §2.5: PII tables NEVER merge — drop lake serves and keep only Postgres-wire serve!
        if pg_matches and (lake_is_pii or any(pt.pii for _, pt in pg_matches)):
            handled_pg_lowers.add(logical_lower)
            if len(pg_matches) > 1:
                raise DiscoveryError(
                    f"PII table '{logical}' is exposed by multiple postgres_sources; "
                    "PII tables must have a single serve"
                )
            pg_eng_name, pg_tbl = pg_matches[0]
            pg_serve_entry: dict[str, Any] = {"name": pg_tbl.physical_name}
            if pg_tbl.max_lag_seconds is not None:
                pg_serve_entry["max_lag_seconds"] = pg_tbl.max_lag_seconds
            tbl_entry_dict: dict[str, Any] = {
                "keys": list(pg_tbl.keys),
                "row_count": pg_tbl.row_count,
                "serve": {pg_eng_name: pg_serve_entry},
                "access": dict(pg_tbl.access),
                "pii": True,
            }
            if pg_tbl.tokenized:
                tbl_entry_dict["tokenized"] = True
            tables_placement[logical] = tbl_entry_dict
            schemas_placement[logical] = list(pg_tbl.columns)
            continue

        is_pii = lake_is_pii

        # Determine Lakekeeper keys
        synced_entry = synced_by_lower.get(logical_lower)
        if it.keys:
            keys = list(it.keys)
        elif synced_entry is not None and synced_entry.primary_key:
            keys = list(synced_entry.primary_key)
        else:
            keys = []

        # Determine lake serve(s)
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
                snap_dt = datetime.fromtimestamp(
                    it.snapshot_ms / 1000.0, tz=timezone.utc
                )
                observed_snap_age = max(0.0, (now_dt - snap_dt).total_seconds())
                id_max_lag: float | None = (
                    observed_snap_age + cfg.refresh_interval_seconds
                )
            else:
                id_max_lag = cfg.refresh_interval_seconds

            id_serve: dict[str, Any] = {
                "name": f"{id_alias}.{it.namespace}.{it.name}",
                "max_lag_seconds": id_max_lag,
            }
            if id_eng.pin_current_snapshot and it.current_snapshot_id is not None:
                id_serve["snapshot_id"] = it.current_snapshot_id
            serve[id_eng.name] = id_serve

        # Legacy Lakebase serve
        if (
            cfg.lakebase is not None
            and synced_entry is not None
            and cfg.lakebase.engine not in cfg.postgres_sources
        ):
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

        # Merge non-PII postgres_sources tables with same logical name (§2.5)
        merged_pg_tbl: PostgresIntrospectedTable | None = None
        if pg_matches:
            handled_pg_lowers.add(logical_lower)
            for pg_eng_name, pg_tbl in pg_matches:
                if list(pg_tbl.columns) == list(it.columns):
                    pg_serve_entry = {"name": pg_tbl.physical_name}
                    if pg_tbl.max_lag_seconds is not None:
                        pg_serve_entry["max_lag_seconds"] = pg_tbl.max_lag_seconds
                    serve[pg_eng_name] = pg_serve_entry
                    if merged_pg_tbl is None:
                        merged_pg_tbl = pg_tbl
                else:
                    warnings.append(
                        f"Table '{logical}' column list on '{pg_eng_name}' {list(pg_tbl.columns)} "
                        f"differs from Lakekeeper {list(it.columns)}; dropped '{pg_eng_name}' serve"
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
            eff_row_count = (
                it.row_count
                if it.row_count is not None
                else (merged_pg_tbl.row_count if merged_pg_tbl is not None else None)
            )
            eff_keys = (
                list(merged_pg_tbl.keys) if merged_pg_tbl is not None else keys
            )
            tbl_entry_dict = {
                "keys": eff_keys,
                "row_count": eff_row_count,
                "serve": serve,
            }
            if merged_pg_tbl is not None:
                tbl_entry_dict["access"] = dict(merged_pg_tbl.access)
                if merged_pg_tbl.tokenized:
                    tbl_entry_dict["tokenized"] = True
            if is_pii:
                tbl_entry_dict["pii"] = True
            tables_placement[logical] = tbl_entry_dict
            schemas_placement[logical] = list(it.columns)
        else:
            warnings.append(
                f"Table '{logical}' has no serve and was omitted from placement"
            )

    # Add postgres_sources tables that did not exist in Lakekeeper
    for logical_lower, pg_matches in pg_tables_by_lower.items():
        if logical_lower in handled_pg_lowers:
            continue
        primary_eng, primary_tbl = pg_matches[0]
        logical = primary_tbl.logical_name
        any_pg_pii = any(pt.pii for _, pt in pg_matches)
        if any_pg_pii and len(pg_matches) > 1:
            raise DiscoveryError(
                f"PII table '{logical}' is exposed by multiple postgres_sources; "
                "PII tables must have a single serve"
            )

        pg_serves: dict[str, Any] = {}
        for pg_eng_name, pg_tbl in pg_matches:
            if list(pg_tbl.columns) != list(primary_tbl.columns):
                warnings.append(
                    f"Table '{logical}' column list on '{pg_eng_name}' {list(pg_tbl.columns)} "
                    f"differs from '{primary_eng}' {list(primary_tbl.columns)}; dropped '{pg_eng_name}' serve"
                )
                continue
            pg_s: dict[str, Any] = {"name": pg_tbl.physical_name}
            if pg_tbl.max_lag_seconds is not None:
                pg_s["max_lag_seconds"] = pg_tbl.max_lag_seconds
            pg_serves[pg_eng_name] = pg_s

        tbl_entry_dict = {
            "keys": list(primary_tbl.keys),
            "row_count": primary_tbl.row_count,
            "serve": pg_serves,
            "access": dict(primary_tbl.access),
        }
        if any_pg_pii:
            tbl_entry_dict["pii"] = True
        if primary_tbl.tokenized:
            tbl_entry_dict["tokenized"] = True
        tables_placement[logical] = tbl_entry_dict
        schemas_placement[logical] = list(primary_tbl.columns)

    # Check for non-PII synced tables missing from Iceberg
    if cfg.lakebase is not None:
        for synced_logical in cfg.lakebase.synced_tables:
            if synced_logical.lower() not in iceberg_lower:
                warnings.append(
                    f"Synced table '{synced_logical}' not found in discovered Iceberg tables"
                )

    # 8b. Enforce §2.3 storage-class PII placement rule
    enforce_storage_class = (
        bool(cfg.postgres_sources)
        or any(e.kind == "postgres" for e in cfg.engines.values())
        or storage_classes is not None
        or connections is not None
        or "pii_object_storage_engines" in cfg._explicit_policy_keys
    )
    if enforce_storage_class:
        pii_obj_allowed = set(cfg.pii_object_storage_engines)
        for logical, tbl_spec in tables_placement.items():
            if tbl_spec.get("pii") is True and not tbl_spec.get("tokenized", False):
                for s_eng in tbl_spec["serve"]:
                    eng_cfg = cfg.engines[s_eng]
                    sc = _resolve_effective_storage_class(
                        s_eng,
                        eng_cfg,
                        storage_classes=storage_classes,
                        connections=connections,
                    )
                    if sc != "local" and s_eng not in pii_obj_allowed:
                        raise DiscoveryError(
                            f"table '{logical}' is pii: true but its serve engine '{s_eng}' "
                            f"(kind {eng_cfg.kind}) has storage_class {sc}; "
                            "CBN policy forbids PII in object storage — serve it from a postgres source "
                            "or list the engine in policy.pii_object_storage_engines"
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
    policy_out: dict[str, Any] = {}
    if cfg.pii_engines or "pii_engines" in cfg._explicit_policy_keys:
        policy_out["pii_engines"] = list(cfg.pii_engines)
    if (
        cfg.pii_object_storage_engines
        or "pii_object_storage_engines" in cfg._explicit_policy_keys
    ):
        policy_out["pii_object_storage_engines"] = list(cfg.pii_object_storage_engines)
    if (
        cfg.require_filter_min_rows != 100_000
        or "require_filter_min_rows" in cfg._explicit_policy_keys
    ):
        policy_out["require_filter_min_rows"] = cfg.require_filter_min_rows
    if cfg.max_in_list != 1_000 or "max_in_list" in cfg._explicit_policy_keys:
        policy_out["max_in_list"] = cfg.max_in_list
    if cfg.max_offset != 10_000 or "max_offset" in cfg._explicit_policy_keys:
        policy_out["max_offset"] = cfg.max_offset
    if cfg.guard_mode != "enforce" or "guard_mode" in cfg._explicit_policy_keys:
        policy_out["guard_mode"] = cfg.guard_mode
    if policy_out:
        placement_dict["policy"] = policy_out

    # Validate by instantiating Placement.
    # Pre-merge compatibility: if Sub-task R has not yet added 'postgres' to
    # VALID_ENGINE_KINDS in this worktree, normalize 'postgres' -> 'lakebase'
    # ONLY for the validation check while emitting the real placement_dict.
    if "postgres" in VALID_ENGINE_KINDS:
        Placement.from_dict(placement_dict)
    else:
        has_pg_kind = any(
            e.get("kind") == "postgres"
            for e in placement_dict["engines"].values()
        )
        if not has_pg_kind:
            Placement.from_dict(placement_dict)
        else:
            normalized_engines = {
                ename: (
                    {**espec, "kind": "lakebase"}
                    if espec.get("kind") == "postgres"
                    else dict(espec)
                )
                for ename, espec in placement_dict["engines"].items()
            }
            Placement.from_dict({**placement_dict, "engines": normalized_engines})

    return DiscoveryResult(
        placement=placement_dict,
        schemas=schemas_placement,
        warnings=tuple(warnings),
        discovered_at=now_dt,
    )

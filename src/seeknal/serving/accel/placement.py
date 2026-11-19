"""Generates Seeknal Serving placement and schemas files for DuckDB acceleration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import json
from pathlib import Path
import sys
from typing import Any, Optional, Union

from seeknal.serving.accel.settings import (
    DEFAULT_MAX_LAG_SECONDS,
    DEFAULT_PII_MODE,
    validate_pii_mode,
)
from seeknal.serving.accel.tables import (
    DEFAULT_LIVE_VIEWS,
    _load_json_data,
    _normalize_columns,
    _normalize_registry,
    validate_identifier,
)


def generate_placement(
    columns: Union[dict, list, str, Path],
    registry: Optional[Union[dict, list, str, Path]] = None,
    freshness: Optional[Union[dict, str, Path]] = None,
    duck: bool = True,
    live_views: Optional[Iterable[str]] = DEFAULT_LIVE_VIEWS,
    max_lag_seconds: int = DEFAULT_MAX_LAG_SECONDS,
) -> dict[str, Any]:
    """Generate the Seeknal Serving placement mapping."""
    norm_columns = _normalize_columns(columns)
    reg_map = _normalize_registry(registry) if registry is not None else {}
    live_set = set(live_views) if live_views is not None else set(DEFAULT_LIVE_VIEWS)

    freshness_data: dict[str, Any] = {}
    if freshness is not None:
        freshness_data = _load_json_data(freshness) or {}

    row_counts = (
        freshness_data.get("row_counts")
        or freshness_data.get("tables")
        or {}
    )

    if duck:
        engines = {
            "lb": {"kind": "lakebase", "dialect": "postgres"},
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        }
        policy = {
            "small_scan_order": ["lakebase", "duckdb"],
            "analytics_order": ["duckdb", "lakebase"],
        }
    else:
        engines = {
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        }
        policy = {
            "small_scan_order": ["lakebase"],
            "analytics_order": ["lakebase"],
        }

    tables: dict[str, Any] = {}

    for view_name, col_list, keys in norm_columns:
        validate_identifier(view_name)

        is_live = view_name in live_set
        if reg_map:
            entry = reg_map.get(view_name)
            if entry:
                if entry.get("status") == "blocked":
                    continue
                if bool(entry.get("live", False)):
                    is_live = True

        table_key = f"rpt.{view_name}"
        rc = row_counts.get(table_key, row_counts.get(view_name, 0))

        if duck and not is_live:
            serve: dict[str, Any] = {
                "lb": f"rpt_base.{view_name}",
                "duck": {
                    "name": table_key,
                    "max_lag_seconds": max_lag_seconds,
                },
            }
        else:
            serve = {
                "lb": f"rpt_base.{view_name}",
            }

        tables[table_key] = {
            "keys": list(keys),
            "row_count": int(rc),
            "serve": serve,
        }

    return {
        "engines": engines,
        "tables": tables,
        "policy": policy,
    }


def generate_schemas(
    columns: Union[dict, list, str, Path],
    registry: Optional[Union[dict, list, str, Path]] = None,
    pii_policy: Optional[Union[dict, str, Path]] = None,
    pii_mode: str = DEFAULT_PII_MODE,
) -> dict[str, list[str]]:
    """Generate the schemas mapping: {"rpt.<view>": ["col", ...]}."""
    mode = validate_pii_mode(pii_mode)
    norm_columns = _normalize_columns(columns)
    reg_map = _normalize_registry(registry) if registry is not None else {}

    pii_data: Mapping[str, Any] = {}
    if pii_policy is not None:
        loaded_pii = _load_json_data(pii_policy)
        if isinstance(loaded_pii, Mapping):
            pii_data = loaded_pii

    schemas: dict[str, list[str]] = {}

    for view_name, col_list, _ in norm_columns:
        validate_identifier(view_name)
        if reg_map:
            entry = reg_map.get(view_name)
            if entry and entry.get("status") == "blocked":
                continue

        col_names: list[str] = []
        for col in col_list:
            c_name = col.get("name")
            if c_name:
                validate_identifier(c_name)
                if mode == "exclude":
                    raw_pii = col.get("pii")
                    is_pii_col = (
                        raw_pii is True
                        or (isinstance(raw_pii, str) and bool(raw_pii.strip()))
                        or (c_name in pii_data)
                    )
                    if is_pii_col:
                        continue
                col_names.append(c_name)

        schemas[f"rpt.{view_name}"] = col_names

    return schemas


def write_placement_files(
    columns_path: Union[str, Path],
    registry_path: Optional[Union[str, Path]] = None,
    freshness_path: Optional[Union[str, Path]] = None,
    duck: bool = True,
    live_views: Optional[Iterable[str]] = DEFAULT_LIVE_VIEWS,
    out_dir: Optional[Union[str, Path]] = None,
    placement_out: Optional[Union[str, Path]] = None,
    schemas_out: Optional[Union[str, Path]] = None,
    pii_policy_path: Optional[Union[str, Path]] = None,
    pii_mode: str = DEFAULT_PII_MODE,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Generate and optionally write placement and schemas files."""
    placement = generate_placement(
        columns=columns_path,
        registry=registry_path,
        freshness=freshness_path,
        duck=duck,
        live_views=live_views,
    )
    schemas = generate_schemas(
        columns=columns_path,
        registry=registry_path,
        pii_policy=pii_policy_path,
        pii_mode=pii_mode,
    )

    if out_dir:
        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        p_file = out_path / "placement.json"
        s_file = out_path / "schemas.json"
        with open(p_file, "w", encoding="utf-8") as f:
            json.dump(placement, f, indent=2)
        with open(s_file, "w", encoding="utf-8") as f:
            json.dump(schemas, f, indent=2)

    if placement_out:
        p_path = Path(placement_out)
        p_path.parent.mkdir(parents=True, exist_ok=True)
        with open(p_path, "w", encoding="utf-8") as f:
            json.dump(placement, f, indent=2)

    if schemas_out:
        s_path = Path(schemas_out)
        s_path.parent.mkdir(parents=True, exist_ok=True)
        with open(s_path, "w", encoding="utf-8") as f:
            json.dump(schemas, f, indent=2)

    if not out_dir and not placement_out and not schemas_out:
        sys.stdout.write(json.dumps(placement, indent=2) + "\n")

    return placement, schemas

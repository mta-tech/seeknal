"""Configuration and placement model for Seeknal Serving."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

import yaml

VALID_ENGINE_KINDS = frozenset({"starrocks", "cube", "lakebase", "duckdb"})
VALID_TWIN_METHODS = frozenset({"redact", "null", "year", "last4"})
MAX_SIGNED_INT64 = 9_223_372_036_854_775_807


def _validate_snapshot_id(value: object, *, context: str = "TableServe") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PlacementError(
            f"{context} has invalid snapshot_id: must be a positive integer within signed 64-bit range"
        )
    if value <= 0 or value > MAX_SIGNED_INT64:
        raise PlacementError(
            f"{context} has invalid snapshot_id: must be a positive integer within signed 64-bit range (1..{MAX_SIGNED_INT64})"
        )
    return value


class PlacementError(ValueError):
    """Raised when a placement configuration is invalid."""


@dataclass(frozen=True)
class Engine:
    """Execution engine definition."""

    name: str
    kind: str
    dialect: str


@dataclass(frozen=True)
class TableServe:
    """How a table is served on a specific engine."""

    engine: str
    physical_name: str
    max_lag_seconds: float | None = None
    snapshot_id: int | None = None

    def __post_init__(self) -> None:
        if self.snapshot_id is not None:
            _validate_snapshot_id(
                self.snapshot_id,
                context=f"TableServe for engine '{self.engine}'",
            )


@dataclass(frozen=True)
class TableEntry:
    """Logical table entry with replication/serving bindings."""

    name: str
    keys: tuple[str, ...]
    row_count: int | None
    serves: tuple[TableServe, ...]
    pii: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.pii, bool):
            raise PlacementError(
                f"Table '{self.name}' field 'pii' must be a boolean, got {type(self.pii).__name__}"
            )


@dataclass(frozen=True)
class CubeEntry:
    """Semantic cube or view served by Cube SQL API."""

    name: str
    engine: str
    measures: tuple[str, ...]
    dimensions: tuple[str, ...]
    mask_variants: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.measures, tuple):
            object.__setattr__(self, "measures", tuple(self.measures))
        if not isinstance(self.dimensions, tuple):
            object.__setattr__(self, "dimensions", tuple(self.dimensions))
        mv: dict[str, tuple[str, ...]] = {}
        if self.mask_variants:
            if not isinstance(self.mask_variants, Mapping):
                raise PlacementError(f"Cube '{self.name}' mask_variants must be a mapping")
            dim_set = set(self.dimensions)
            for base, methods in self.mask_variants.items():
                base_str = str(base)
                if base_str not in dim_set:
                    raise PlacementError(
                        f"Cube '{self.name}' mask_variants base '{base_str}' is not a dimension of that cube"
                    )
                methods_tuple = tuple(str(m) for m in methods)
                for m in methods_tuple:
                    if m not in VALID_TWIN_METHODS:
                        raise PlacementError(
                            f"Cube '{self.name}' mask_variants has invalid method '{m}' for base '{base_str}'"
                        )
                mv[base_str] = methods_tuple
        object.__setattr__(self, "mask_variants", MappingProxyType(mv))


@dataclass(frozen=True)
class Policy:
    """Routing thresholds and engine preference order."""

    point_lookup_max_keys: int = 100
    analytics_min_rows: int = 1_000_000
    small_scan_order: tuple[str, ...] = ("lakebase", "starrocks", "duckdb")
    analytics_order: tuple[str, ...] = ("starrocks", "duckdb")
    pii_engines: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.pii_engines, (list, tuple)):
            raise PlacementError(
                "Policy 'pii_engines' must be a list or tuple of non-empty engine name strings"
            )
        cleaned: list[str] = []
        for item in self.pii_engines:
            if not isinstance(item, str) or not item.strip():
                raise PlacementError(
                    "Policy 'pii_engines' must contain only non-empty engine name strings"
                )
            cleaned.append(item.strip())
        object.__setattr__(self, "pii_engines", tuple(cleaned))


@dataclass(frozen=True)
class Placement:
    """Catalog of engines, tables, cubes, and routing policy."""

    engines: Mapping[str, Engine]
    tables: Mapping[str, TableEntry]
    cubes: Mapping[str, CubeEntry]
    policy: Policy = field(default_factory=Policy)

    def __post_init__(self) -> None:
        # Validate engines
        for eng_name, engine in self.engines.items():
            if not eng_name or not eng_name.strip():
                raise PlacementError("Engine name cannot be empty")
            if not engine.name or not engine.name.strip():
                raise PlacementError("Engine name cannot be empty")
            if engine.kind not in VALID_ENGINE_KINDS:
                raise PlacementError(
                    f"Unknown engine kind '{engine.kind}' for engine '{engine.name}'"
                )

        # Validate policy.pii_engines
        for pii_eng in self.policy.pii_engines:
            if pii_eng not in self.engines:
                raise PlacementError(
                    f"Policy 'pii_engines' references undefined engine '{pii_eng}'"
                )

        # Validate tables and collision
        seen_tables: dict[str, str] = {}
        for tbl_name, table in self.tables.items():
            if not tbl_name or not tbl_name.strip():
                raise PlacementError("Table name cannot be empty")
            if not table.name or not table.name.strip():
                raise PlacementError("Table name cannot be empty")
            lower = table.name.lower()
            if lower in seen_tables:
                raise PlacementError(
                    f"Table name '{table.name}' collides case-insensitively with '{seen_tables[lower]}'"
                )
            seen_tables[lower] = table.name

            if not isinstance(table.pii, bool):
                raise PlacementError(
                    f"Table '{table.name}' field 'pii' must be a boolean, got {type(table.pii).__name__}"
                )

            if not table.serves:
                raise PlacementError(f"Table '{table.name}' has no serve")
            for serve in table.serves:
                if serve.engine not in self.engines:
                    raise PlacementError(
                        f"Table '{table.name}' serve references undefined engine '{serve.engine}'"
                    )
                if not serve.physical_name or not serve.physical_name.strip():
                    raise PlacementError(
                        f"Table '{table.name}' has empty physical name for engine '{serve.engine}'"
                    )

            if table.pii:
                if len(table.serves) != 1:
                    raise PlacementError(
                        f"PII table '{table.name}' must have exactly one serve, got {len(table.serves)}"
                    )
                serve_eng = self.engines[table.serves[0].engine]
                if (
                    serve_eng.kind == "duckdb"
                    and serve_eng.name not in self.policy.pii_engines
                ):
                    raise PlacementError(
                        f"PII table '{table.name}' cannot be served on duckdb engine '{serve_eng.name}' unless listed in policy.pii_engines"
                    )

        # Validate cubes and collision
        seen_cubes: dict[str, str] = {}
        for cube_name, cube in self.cubes.items():
            if not cube_name or not cube_name.strip():
                raise PlacementError("Cube name cannot be empty")
            if not cube.name or not cube.name.strip():
                raise PlacementError("Cube name cannot be empty")
            lower = cube.name.lower()
            if lower in seen_cubes:
                raise PlacementError(
                    f"Cube name '{cube.name}' collides case-insensitively with '{seen_cubes[lower]}'"
                )
            if lower in seen_tables:
                raise PlacementError(
                    f"Cube name '{cube.name}' collides case-insensitively with table '{seen_tables[lower]}'"
                )
            seen_cubes[lower] = cube.name

            if cube.engine not in self.engines:
                raise PlacementError(
                    f"Cube '{cube.name}' references undefined engine '{cube.engine}'"
                )
            if self.engines[cube.engine].kind != "cube":
                raise PlacementError(
                    f"Cube '{cube.name}' engine '{cube.engine}' is not of kind 'cube'"
                )

            # Validate mask_variants on cube
            dim_set = set(cube.dimensions)
            for base, methods in cube.mask_variants.items():
                if base not in dim_set:
                    raise PlacementError(
                        f"Cube '{cube.name}' mask_variants base '{base}' is not a dimension of that cube"
                    )
                for m in methods:
                    if m not in VALID_TWIN_METHODS:
                        raise PlacementError(
                            f"Cube '{cube.name}' mask_variants has invalid method '{m}' for base '{base}'"
                        )

        # Build fast case-insensitive lookup indices
        object.__setattr__(
            self,
            "_tables_by_lower",
            {t.name.lower(): t for t in self.tables.values()},
        )
        object.__setattr__(
            self,
            "_cubes_by_lower",
            {c.name.lower(): c for c in self.cubes.values()},
        )

    def find_table(self, name: str) -> TableEntry | None:
        """Find a table entry by exact case-insensitive qualification."""
        indices: dict[str, TableEntry] = getattr(self, "_tables_by_lower", {})
        return indices.get(name.lower())

    def find_cube(self, name: str) -> CubeEntry | None:
        """Find a cube entry by exact case-insensitive qualification."""
        indices: dict[str, CubeEntry] = getattr(self, "_cubes_by_lower", {})
        return indices.get(name.lower())

    @classmethod
    def from_dict(cls, data: Mapping) -> Placement:
        """Construct Placement from a raw mapping."""
        # 1. Engines
        engines: dict[str, Engine] = {}
        engines_raw = data.get("engines") or {}
        for eng_name, eng_spec in engines_raw.items():
            if not eng_name or not str(eng_name).strip():
                raise PlacementError("Engine name cannot be empty")
            kind = eng_spec.get("kind", "")
            dialect = eng_spec.get("dialect", "")
            engines[eng_name] = Engine(name=str(eng_name), kind=kind, dialect=dialect)

        # 2. Tables
        tables: dict[str, TableEntry] = {}
        tables_raw = data.get("tables") or {}
        for tbl_name, tbl_spec in tables_raw.items():
            if not tbl_name or not str(tbl_name).strip():
                raise PlacementError("Table name cannot be empty")
            keys = tuple(str(k) for k in (tbl_spec.get("keys") or []))
            row_count_val = tbl_spec.get("row_count")
            row_count = int(row_count_val) if row_count_val is not None else None

            serve_raw = tbl_spec.get("serve")
            if not serve_raw:
                raise PlacementError(f"Table '{tbl_name}' has no serve")

            serves: list[TableServe] = []
            for eng_name, sval in serve_raw.items():
                if isinstance(sval, str):
                    serves.append(
                        TableServe(
                            engine=str(eng_name),
                            physical_name=sval,
                            max_lag_seconds=None,
                        )
                    )
                elif isinstance(sval, Mapping):
                    phys_name = sval.get("name", "")
                    lag_val = sval.get("max_lag_seconds")
                    max_lag = float(lag_val) if lag_val is not None else None
                    snap_raw = sval.get("snapshot_id")
                    snap_id: int | None = None
                    if snap_raw is not None:
                        snap_id = _validate_snapshot_id(
                            snap_raw,
                            context=f"Table '{tbl_name}' serve for engine '{eng_name}'",
                        )
                    serves.append(
                        TableServe(
                            engine=str(eng_name),
                            physical_name=str(phys_name),
                            max_lag_seconds=max_lag,
                            snapshot_id=snap_id,
                        )
                    )
                else:
                    raise PlacementError(
                        f"Table '{tbl_name}' serve has invalid format for engine '{eng_name}'"
                    )

            pii_raw = tbl_spec.get("pii", False)
            if not isinstance(pii_raw, bool):
                raise PlacementError(
                    f"Table '{tbl_name}' field 'pii' must be a boolean, got {type(pii_raw).__name__}"
                )

            tables[str(tbl_name)] = TableEntry(
                name=str(tbl_name),
                keys=keys,
                row_count=row_count,
                serves=tuple(serves),
                pii=pii_raw,
            )

        # 3. Cubes
        cubes: dict[str, CubeEntry] = {}
        cubes_raw = data.get("cubes") or {}
        for cube_name, cube_spec in cubes_raw.items():
            if not cube_name or not str(cube_name).strip():
                raise PlacementError("Cube name cannot be empty")
            cube_eng = str(cube_spec.get("engine", ""))
            measures = tuple(str(m) for m in (cube_spec.get("measures") or []))
            dimensions = tuple(str(d) for d in (cube_spec.get("dimensions") or []))
            mv_raw = cube_spec.get("mask_variants")
            if mv_raw is not None:
                if not isinstance(mv_raw, Mapping):
                    raise PlacementError(f"Cube '{cube_name}' mask_variants must be a mapping")
                mask_variants = {
                    str(k): tuple(str(m) for m in v)
                    for k, v in mv_raw.items()
                }
            else:
                mask_variants = {}
            cubes[str(cube_name)] = CubeEntry(
                name=str(cube_name),
                engine=cube_eng,
                measures=measures,
                dimensions=dimensions,
                mask_variants=mask_variants,
            )

        # 4. Policy
        policy_raw = data.get("policy")
        if policy_raw is None:
            policy = Policy()
        else:
            kwargs = {}
            if "point_lookup_max_keys" in policy_raw:
                kwargs["point_lookup_max_keys"] = int(
                    policy_raw["point_lookup_max_keys"]
                )
            if "analytics_min_rows" in policy_raw:
                kwargs["analytics_min_rows"] = int(policy_raw["analytics_min_rows"])
            if "small_scan_order" in policy_raw:
                kwargs["small_scan_order"] = tuple(
                    str(x) for x in policy_raw["small_scan_order"]
                )
            if "analytics_order" in policy_raw:
                kwargs["analytics_order"] = tuple(
                    str(x) for x in policy_raw["analytics_order"]
                )
            if "pii_engines" in policy_raw:
                raw_pii_engines = policy_raw["pii_engines"]
                if not isinstance(raw_pii_engines, (list, tuple)):
                    raise PlacementError(
                        "Policy 'pii_engines' must be a list or tuple of non-empty engine name strings"
                    )
                parsed_pii_engines: list[str] = []
                for item in raw_pii_engines:
                    if not isinstance(item, str) or not item.strip():
                        raise PlacementError(
                            "Policy 'pii_engines' must contain only non-empty engine name strings"
                        )
                    parsed_pii_engines.append(item.strip())
                kwargs["pii_engines"] = tuple(parsed_pii_engines)
            policy = Policy(**kwargs)

        return cls(engines=engines, tables=tables, cubes=cubes, policy=policy)

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> Placement:
        """Construct Placement from a YAML file."""
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls.from_dict(data)

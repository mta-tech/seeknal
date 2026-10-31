"""Configuration and placement model for Seeknal Serving."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

import yaml

VALID_ENGINE_KINDS = frozenset({"starrocks", "cube", "lakebase", "duckdb"})


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


@dataclass(frozen=True)
class TableEntry:
    """Logical table entry with replication/serving bindings."""

    name: str
    keys: tuple[str, ...]
    row_count: int | None
    serves: tuple[TableServe, ...]


@dataclass(frozen=True)
class CubeEntry:
    """Semantic cube or view served by Cube SQL API."""

    name: str
    engine: str
    measures: tuple[str, ...]
    dimensions: tuple[str, ...]


@dataclass(frozen=True)
class Policy:
    """Routing thresholds and engine preference order."""

    point_lookup_max_keys: int = 100
    analytics_min_rows: int = 1_000_000
    small_scan_order: tuple[str, ...] = ("lakebase", "starrocks", "duckdb")
    analytics_order: tuple[str, ...] = ("starrocks", "duckdb")


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
                    serves.append(
                        TableServe(
                            engine=str(eng_name),
                            physical_name=str(phys_name),
                            max_lag_seconds=max_lag,
                        )
                    )
                else:
                    raise PlacementError(
                        f"Table '{tbl_name}' serve has invalid format for engine '{eng_name}'"
                    )

            tables[str(tbl_name)] = TableEntry(
                name=str(tbl_name),
                keys=keys,
                row_count=row_count,
                serves=tuple(serves),
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
            cubes[str(cube_name)] = CubeEntry(
                name=str(cube_name),
                engine=cube_eng,
                measures=measures,
                dimensions=dimensions,
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
            policy = Policy(**kwargs)

        return cls(engines=engines, tables=tables, cubes=cubes, policy=policy)

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> Placement:
        """Construct Placement from a YAML file."""
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls.from_dict(data)

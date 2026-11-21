"""Configuration and placement model for Seeknal Serving."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

import yaml
from yaml.constructor import ConstructorError

VALID_ENGINE_KINDS = frozenset({"starrocks", "cube", "lakebase", "postgres", "duckdb"})
VALID_ENGINE_MODES = frozenset({"read_only", "read_write"})
VALID_ACCESS_PATTERNS = frozenset({"point", "filter", "scan"})
VALID_GUARD_MODES = frozenset({"enforce", "warn"})
VALID_TWIN_METHODS = frozenset({"redact", "null", "year", "last4"})
MAX_SIGNED_INT64 = 9_223_372_036_854_775_807

_VALID_TABLE_ACCESS_KEYS = frozenset(
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


def is_postgres_wire(kind: str | None) -> bool:
    """Return True when *kind* is a Postgres-wire engine ('postgres' or 'lakebase')."""
    return kind in ("postgres", "lakebase")


def _validate_col_tuple(
    val: object,
    field_name: str,
    *,
    context: str,
) -> tuple[str, ...]:
    if not isinstance(val, (list, tuple)):
        raise PlacementError(
            f"{context} field '{field_name}' must be a list or tuple of non-empty column name strings"
        )
    cleaned: list[str] = []
    for item in val:
        if not isinstance(item, str) or not item.strip():
            raise PlacementError(
                f"{context} field '{field_name}' must contain only non-empty column name strings"
            )
        cleaned.append(item.strip())
    return tuple(cleaned)


def _validate_engine_name_tuple(val: object, field_name: str) -> tuple[str, ...]:
    if not isinstance(val, (list, tuple)):
        raise PlacementError(
            f"Policy '{field_name}' must be a list or tuple of non-empty engine name strings"
        )
    cleaned: list[str] = []
    for item in val:
        if not isinstance(item, str) or not item.strip():
            raise PlacementError(
                f"Policy '{field_name}' must contain only non-empty engine name strings"
            )
        cleaned.append(item.strip())
    return tuple(cleaned)


def _validate_int_field(
    val: object,
    field_name: str,
    *,
    min_value: int = 0,
    context: str = "Policy",
) -> int:
    if isinstance(val, bool) or not isinstance(val, int) or val < min_value:
        bound_desc = "a positive integer" if min_value >= 1 else "a non-negative integer"
        raise PlacementError(
            f"{context} '{field_name}' must be {bound_desc}, got {val!r}"
        )
    return val


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


class DuplicateYamlKeyError(ConstructorError, PlacementError):
    """Raised when a YAML mapping contains duplicate keys."""

    def __init__(self, key: object, mark: yaml.Mark | None) -> None:
        self.duplicate_key = key
        super().__init__(
            None,
            None,
            f"found duplicate YAML key '{key}'",
            mark,
        )


class UniqueKeySafeLoader(yaml.SafeLoader):
    """SafeLoader subclass that rejects duplicate keys at every YAML mapping node."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict:
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        seen_keys: set[object] = set()
        for key_node, _value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                is_dup = key in seen_keys
            except TypeError as exc:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found unhashable key",
                    key_node.start_mark,
                ) from exc
            if is_dup:
                raise DuplicateYamlKeyError(key, key_node.start_mark)
            seen_keys.add(key)
        return super().construct_mapping(node, deep=deep)


@dataclass(frozen=True)
class Engine:
    """Execution engine definition."""

    name: str
    kind: str
    dialect: str
    mode: str | None = None

    def __post_init__(self) -> None:
        if self.mode is not None:
            if not isinstance(self.mode, str) or self.mode not in VALID_ENGINE_MODES:
                raise PlacementError(
                    f"Engine '{self.name}' has invalid mode '{self.mode}'; "
                    f"must be 'read_only' or 'read_write'"
                )

    @property
    def is_writable(self) -> bool:
        """Return True if this engine accepts write statements."""
        if self.kind == "lakebase":
            return (self.mode or "read_write") == "read_write"
        if self.kind == "postgres":
            return self.mode == "read_write"
        return False


@dataclass(frozen=True)
class TableAccess:
    """Access pattern and index guardrail metadata for a logical table."""

    pattern: str = "scan"
    keys: tuple[str, ...] = ()
    filterable: tuple[str, ...] = ()
    range_filterable: tuple[str, ...] = ()
    prefix_filterable: tuple[str, ...] = ()
    sortable: tuple[str, ...] = ()
    require_filter: bool = False
    max_rows: int | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.pattern, str)
            or self.pattern not in VALID_ACCESS_PATTERNS
        ):
            raise PlacementError(
                f"TableAccess has invalid pattern '{self.pattern}'; "
                f"must be one of {sorted(VALID_ACCESS_PATTERNS)}"
            )
        for fname in (
            "keys",
            "filterable",
            "range_filterable",
            "prefix_filterable",
            "sortable",
        ):
            cleaned = _validate_col_tuple(
                getattr(self, fname), fname, context="TableAccess"
            )
            object.__setattr__(self, fname, cleaned)
        if not isinstance(self.require_filter, bool):
            raise PlacementError(
                f"TableAccess field 'require_filter' must be a boolean, "
                f"got {type(self.require_filter).__name__}"
            )
        if self.max_rows is not None:
            if (
                isinstance(self.max_rows, bool)
                or not isinstance(self.max_rows, int)
                or self.max_rows <= 0
            ):
                raise PlacementError(
                    f"TableAccess field 'max_rows' must be a positive integer or None, "
                    f"got {self.max_rows!r}"
                )

    @classmethod
    def from_dict(
        cls,
        raw: Mapping,
        *,
        table_name: str = "",
        table_keys: tuple[str, ...] | list[str] = (),
    ) -> TableAccess:
        """Construct TableAccess from a raw mapping."""
        ctx = f"Table '{table_name}' access" if table_name else "TableAccess"
        if not isinstance(raw, Mapping):
            raise PlacementError(
                f"{ctx} must be a mapping, got {type(raw).__name__}"
            )
        unknown = set(raw.keys()) - _VALID_TABLE_ACCESS_KEYS
        if unknown:
            raise PlacementError(
                f"{ctx} contains unknown field(s): "
                f"{', '.join(sorted(str(k) for k in unknown))}"
            )
        pattern = raw.get("pattern", "scan")
        if not isinstance(pattern, str) or pattern not in VALID_ACCESS_PATTERNS:
            raise PlacementError(
                f"{ctx} has invalid pattern '{pattern}'; "
                f"must be one of {sorted(VALID_ACCESS_PATTERNS)}"
            )
        keys = _validate_col_tuple(raw.get("keys", ()), "keys", context=ctx)
        clean_table_keys = (
            _validate_col_tuple(table_keys, "keys", context=ctx)
            if table_keys
            else ()
        )
        effective_keys = keys if keys else clean_table_keys
        if pattern == "point" and not effective_keys:
            raise PlacementError(
                f"{ctx} with pattern 'point' requires non-empty 'keys'"
            )
        filterable = _validate_col_tuple(
            raw.get("filterable", ()), "filterable", context=ctx
        )
        range_filterable = _validate_col_tuple(
            raw.get("range_filterable", ()), "range_filterable", context=ctx
        )
        prefix_filterable = _validate_col_tuple(
            raw.get("prefix_filterable", ()), "prefix_filterable", context=ctx
        )
        sortable = _validate_col_tuple(
            raw.get("sortable", ()), "sortable", context=ctx
        )
        if "require_filter" in raw:
            require_filter = raw["require_filter"]
            if not isinstance(require_filter, bool):
                raise PlacementError(
                    f"{ctx} field 'require_filter' must be a boolean, "
                    f"got {type(require_filter).__name__}"
                )
        else:
            require_filter = pattern in ("point", "filter")

        max_rows = raw.get("max_rows", None)
        if max_rows is not None:
            if (
                isinstance(max_rows, bool)
                or not isinstance(max_rows, int)
                or max_rows <= 0
            ):
                raise PlacementError(
                    f"{ctx} field 'max_rows' must be a positive integer or None, "
                    f"got {max_rows!r}"
                )

        return cls(
            pattern=pattern,
            keys=effective_keys,
            filterable=filterable,
            range_filterable=range_filterable,
            prefix_filterable=prefix_filterable,
            sortable=sortable,
            require_filter=require_filter,
            max_rows=max_rows,
        )


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
    tokenized: bool = False
    access: TableAccess | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.keys, tuple):
            object.__setattr__(self, "keys", tuple(self.keys))
        if not isinstance(self.pii, bool):
            raise PlacementError(
                f"Table '{self.name}' field 'pii' must be a boolean, got {type(self.pii).__name__}"
            )
        if not isinstance(self.tokenized, bool):
            raise PlacementError(
                f"Table '{self.name}' field 'tokenized' must be a boolean, got {type(self.tokenized).__name__}"
            )
        if self.access is not None:
            if not isinstance(self.access, TableAccess):
                raise PlacementError(
                    f"Table '{self.name}' field 'access' must be a TableAccess or None, "
                    f"got {type(self.access).__name__}"
                )
            if not self.keys and self.access.keys:
                object.__setattr__(self, "keys", self.access.keys)
            elif self.keys and not self.access.keys:
                object.__setattr__(
                    self, "access", replace(self.access, keys=self.keys)
                )
            if self.access.pattern == "point" and not self.access.keys:
                raise PlacementError(
                    f"Table '{self.name}' access with pattern 'point' requires non-empty 'keys'"
                )


@dataclass(frozen=True)
class CubeEntry:
    """Semantic cube or view served by Cube SQL API."""

    name: str
    engine: str
    measures: tuple[str, ...]
    dimensions: tuple[str, ...]
    mask_variants: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    pii: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.pii, bool):
            raise PlacementError(
                f"Cube '{self.name}' field 'pii' must be a boolean, got {type(self.pii).__name__}"
            )
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
    pii_object_storage_engines: tuple[str, ...] = ()
    require_filter_min_rows: int = 100_000
    max_in_list: int = 1_000
    max_offset: int = 10_000
    guard_mode: str = "enforce"

    def __post_init__(self) -> None:
        _validate_int_field(
            self.point_lookup_max_keys, "point_lookup_max_keys", min_value=1
        )
        _validate_int_field(
            self.analytics_min_rows, "analytics_min_rows", min_value=0
        )
        _validate_int_field(
            self.require_filter_min_rows, "require_filter_min_rows", min_value=0
        )
        _validate_int_field(self.max_in_list, "max_in_list", min_value=1)
        _validate_int_field(self.max_offset, "max_offset", min_value=0)
        if (
            not isinstance(self.guard_mode, str)
            or self.guard_mode not in VALID_GUARD_MODES
        ):
            raise PlacementError(
                f"Policy 'guard_mode' must be 'enforce' or 'warn', got {self.guard_mode!r}"
            )
        object.__setattr__(
            self,
            "pii_engines",
            _validate_engine_name_tuple(self.pii_engines, "pii_engines"),
        )
        object.__setattr__(
            self,
            "pii_object_storage_engines",
            _validate_engine_name_tuple(
                self.pii_object_storage_engines, "pii_object_storage_engines"
            ),
        )


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

        # Validate policy.pii_engines and policy.pii_object_storage_engines
        for pii_eng in self.policy.pii_engines:
            if pii_eng not in self.engines:
                raise PlacementError(
                    f"Policy 'pii_engines' references undefined engine '{pii_eng}'"
                )
        for pii_obj_eng in self.policy.pii_object_storage_engines:
            if pii_obj_eng not in self.engines:
                raise PlacementError(
                    f"Policy 'pii_object_storage_engines' references undefined engine '{pii_obj_eng}'"
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

            if not isinstance(table.tokenized, bool):
                raise PlacementError(
                    f"Table '{table.name}' field 'tokenized' must be a boolean, got {type(table.tokenized).__name__}"
                )

            if table.pii and not table.tokenized:
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
            if not isinstance(cube.pii, bool):
                raise PlacementError(
                    f"Cube '{cube.name}' field 'pii' must be a boolean, got {type(cube.pii).__name__}"
                )
            if cube.pii and cube.engine not in self.policy.pii_engines:
                raise PlacementError(
                    f"PII cube '{cube.name}' engine '{cube.engine}' must be listed in policy.pii_engines"
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
            if not isinstance(eng_spec, Mapping):
                raise PlacementError(
                    f"Engine '{eng_name}' specification must be a mapping"
                )
            if "storage_class" in eng_spec:
                raise PlacementError(
                    f"Engine '{eng_name}' cannot declare 'storage_class' in placement.engines; "
                    f"declare 'storage_class' on connections.{eng_name} instead"
                )
            kind = eng_spec.get("kind", "")
            dialect = eng_spec.get("dialect", "")
            has_mode = "mode" in eng_spec and eng_spec["mode"] is not None
            has_ep = (
                "endpoint_type" in eng_spec
                and eng_spec["endpoint_type"] is not None
            )
            if has_mode and has_ep and eng_spec["mode"] != eng_spec["endpoint_type"]:
                raise PlacementError(
                    f"Engine '{eng_name}' specifies conflicting 'mode' ({eng_spec['mode']!r}) "
                    f"and 'endpoint_type' ({eng_spec['endpoint_type']!r})"
                )
            mode = (
                eng_spec["mode"]
                if "mode" in eng_spec and eng_spec["mode"] is not None
                else eng_spec.get("endpoint_type")
            )
            engines[str(eng_name)] = Engine(
                name=str(eng_name), kind=kind, dialect=dialect, mode=mode
            )

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

            tokenized_raw = tbl_spec.get("tokenized", False)
            if not isinstance(tokenized_raw, bool):
                raise PlacementError(
                    f"Table '{tbl_name}' field 'tokenized' must be a boolean, got {type(tokenized_raw).__name__}"
                )

            access_raw = tbl_spec.get("access")
            access_obj: TableAccess | None
            if access_raw is None:
                access_obj = None
            elif isinstance(access_raw, TableAccess):
                access_obj = access_raw
            else:
                access_obj = TableAccess.from_dict(
                    access_raw, table_name=str(tbl_name), table_keys=keys
                )

            tables[str(tbl_name)] = TableEntry(
                name=str(tbl_name),
                keys=keys,
                row_count=row_count,
                serves=tuple(serves),
                pii=pii_raw,
                tokenized=tokenized_raw,
                access=access_obj,
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
            cube_pii_raw = cube_spec.get("pii", False)
            if not isinstance(cube_pii_raw, bool):
                raise PlacementError(
                    f"Cube '{cube_name}' field 'pii' must be a boolean, got {type(cube_pii_raw).__name__}"
                )
            cubes[str(cube_name)] = CubeEntry(
                name=str(cube_name),
                engine=cube_eng,
                measures=measures,
                dimensions=dimensions,
                mask_variants=mask_variants,
                pii=cube_pii_raw,
            )

        # 4. Policy
        policy_raw = data.get("policy")
        if policy_raw is None:
            policy = Policy()
        else:
            if not isinstance(policy_raw, Mapping):
                raise PlacementError("'policy' must be a mapping")
            kwargs: dict[str, object] = {}
            if "point_lookup_max_keys" in policy_raw:
                raw_plmk = policy_raw["point_lookup_max_keys"]
                if isinstance(raw_plmk, bool):
                    raise PlacementError(
                        "Policy 'point_lookup_max_keys' must be a positive integer"
                    )
                try:
                    coerced_plmk = int(raw_plmk)
                except (ValueError, TypeError) as exc:
                    raise PlacementError(
                        f"Policy 'point_lookup_max_keys' must be a positive integer, got {raw_plmk!r}"
                    ) from exc
                kwargs["point_lookup_max_keys"] = _validate_int_field(
                    coerced_plmk,
                    "point_lookup_max_keys",
                    min_value=1,
                )
            if "analytics_min_rows" in policy_raw:
                raw_amr = policy_raw["analytics_min_rows"]
                if isinstance(raw_amr, bool):
                    raise PlacementError(
                        "Policy 'analytics_min_rows' must be a non-negative integer"
                    )
                try:
                    coerced_amr = int(raw_amr)
                except (ValueError, TypeError) as exc:
                    raise PlacementError(
                        f"Policy 'analytics_min_rows' must be a non-negative integer, got {raw_amr!r}"
                    ) from exc
                kwargs["analytics_min_rows"] = _validate_int_field(
                    coerced_amr,
                    "analytics_min_rows",
                    min_value=0,
                )
            if "small_scan_order" in policy_raw:
                kwargs["small_scan_order"] = tuple(
                    str(x) for x in policy_raw["small_scan_order"]
                )
            if "analytics_order" in policy_raw:
                kwargs["analytics_order"] = tuple(
                    str(x) for x in policy_raw["analytics_order"]
                )
            if "pii_engines" in policy_raw:
                kwargs["pii_engines"] = _validate_engine_name_tuple(
                    policy_raw["pii_engines"], "pii_engines"
                )
            if "pii_object_storage_engines" in policy_raw:
                kwargs["pii_object_storage_engines"] = _validate_engine_name_tuple(
                    policy_raw["pii_object_storage_engines"],
                    "pii_object_storage_engines",
                )
            if "require_filter_min_rows" in policy_raw:
                kwargs["require_filter_min_rows"] = _validate_int_field(
                    policy_raw["require_filter_min_rows"],
                    "require_filter_min_rows",
                    min_value=0,
                )
            if "max_in_list" in policy_raw:
                kwargs["max_in_list"] = _validate_int_field(
                    policy_raw["max_in_list"], "max_in_list", min_value=1
                )
            if "max_offset" in policy_raw:
                kwargs["max_offset"] = _validate_int_field(
                    policy_raw["max_offset"], "max_offset", min_value=0
                )
            if "guard_mode" in policy_raw:
                kwargs["guard_mode"] = policy_raw["guard_mode"]
            policy = Policy(**kwargs)

        # 5. Fold top-level pii_tables and policy.pii_tables into TableEntry.pii / CubeEntry.pii
        top_pii_tables = data.get("pii_tables")
        policy_pii_tables = (
            policy_raw.get("pii_tables") if isinstance(policy_raw, Mapping) else None
        )
        if top_pii_tables is not None or policy_pii_tables is not None:
            tables_by_lower = {k.lower(): k for k in tables}
            cubes_by_lower = {k.lower(): k for k in cubes}
            for src_label, raw_pii_list in (
                ("pii_tables", top_pii_tables),
                ("policy.pii_tables", policy_pii_tables),
            ):
                if raw_pii_list is None:
                    continue
                if not isinstance(raw_pii_list, (list, tuple)):
                    raise PlacementError(
                        f"'{src_label}' must be a list or tuple of non-empty table name strings"
                    )
                for t_ref in raw_pii_list:
                    if not isinstance(t_ref, str) or not t_ref.strip():
                        raise PlacementError(
                            f"'{src_label}' must contain only non-empty table name strings"
                        )
                    ref_lower = t_ref.strip().lower()
                    if ref_lower in tables_by_lower:
                        canon_key = tables_by_lower[ref_lower]
                        tables[canon_key] = replace(tables[canon_key], pii=True)
                    elif ref_lower in cubes_by_lower:
                        canon_key = cubes_by_lower[ref_lower]
                        cubes[canon_key] = replace(cubes[canon_key], pii=True)
                    else:
                        raise PlacementError(f"Unknown table '{t_ref}' in pii_tables")

        return cls(engines=engines, tables=tables, cubes=cubes, policy=policy)

    @classmethod
    def from_yaml(cls, path: str | os.PathLike) -> Placement:
        """Construct Placement from a YAML file."""
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=UniqueKeySafeLoader) or {}
        return cls.from_dict(data)

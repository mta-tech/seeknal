"""Table specifications and type mapping for Seeknal Serving DuckDB acceleration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any, Optional, Union

from seeknal.serving.accel.settings import DEFAULT_PII_MODE, validate_pii_mode

DEFAULT_LIVE_VIEWS: tuple[str, ...] = ()
MAX_SIGNED_INT64 = 9_223_372_036_854_775_807

IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")
ALLOWED_DUCKDB_TYPE_RE = re.compile(
    r"^(VARCHAR|INTEGER|BIGINT|BOOLEAN|DATE|TIMESTAMP|DOUBLE|DECIMAL\(\d+,\d+\))$"
)
DECIMAL_RE = re.compile(
    r"^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$",
    re.IGNORECASE,
)
VARCHAR_RE = re.compile(
    r"^(?:varchar|character\s+varying)(?:\(\s*\d+\s*\))?$",
    re.IGNORECASE,
)


def validate_snapshot_id(value: Any, *, context: str = "TableSpec") -> int:
    """Validate snapshot_id is a positive signed 64-bit integer."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"{context} has invalid snapshot_id {value!r}: must be a positive integer within signed 64-bit range"
        )
    if value <= 0 or value > MAX_SIGNED_INT64:
        raise ValueError(
            f"{context} has invalid snapshot_id {value!r}: must be between 1 and {MAX_SIGNED_INT64}"
        )
    return value


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    duckdb_type: str
    is_nulled: bool
    is_pii: bool = False

    def __post_init__(self) -> None:
        if self.is_nulled and not self.is_pii:
            object.__setattr__(self, "is_pii", True)

    @property
    def type(self) -> str:
        return self.duckdb_type

    @property
    def nulled(self) -> bool:
        return self.is_nulled

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


@dataclass(frozen=True)
class TableSpec:
    name: str
    source: str
    view: str
    columns: tuple[ColumnSpec, ...]
    keys: tuple[str, ...] = ()
    snapshot_id: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.columns, tuple):
            object.__setattr__(self, "columns", tuple(self.columns))
        if not isinstance(self.keys, tuple):
            object.__setattr__(self, "keys", tuple(self.keys))
        if self.snapshot_id is not None:
            validated = validate_snapshot_id(
                self.snapshot_id,
                context=f"TableSpec '{self.name}'",
            )
            object.__setattr__(self, "snapshot_id", validated)

    @property
    def logical_name(self) -> str:
        return self.name

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


def validate_identifier(name: str) -> None:
    """Validate identifier against ^[a-z][a-z0-9_]*$ with fullmatch and length <= 63."""
    if (
        not isinstance(name, str)
        or len(name) == 0
        or len(name) > 63
        or not IDENTIFIER_RE.fullmatch(name)
    ):
        raise ValueError(
            f"Invalid identifier: {name!r}; must match ^[a-z][a-z0-9_]*$, contain no whitespace/newlines, and be at most 63 chars"
        )


def validate_duckdb_type(duckdb_type: str, col_name: str) -> None:
    """Validate that duckdb_type is in the allowed set."""
    if not isinstance(duckdb_type, str) or not ALLOWED_DUCKDB_TYPE_RE.fullmatch(
        duckdb_type
    ):
        raise ValueError(
            f"Invalid DuckDB type {duckdb_type!r} for column {col_name!r}"
        )


def map_type(col_name: str, raw_type: str) -> str:
    """Map SQL, Postgres, and Iceberg column types to supported DuckDB types."""
    if not isinstance(raw_type, str):
        raise ValueError(f"Unknown type {raw_type!r} for column {col_name!r}")
    t = raw_type.strip().lower()

    if t in ("varchar", "text", "string", "character varying") or VARCHAR_RE.fullmatch(
        t
    ):
        return "VARCHAR"
    if t in ("integer", "int", "int4", "smallint", "int2"):
        return "INTEGER"
    if t in ("bigint", "int8", "long"):
        return "BIGINT"
    if t in ("boolean", "bool"):
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    if t in (
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
        "timestamptz",
        "timestamp_ns",
    ):
        return "TIMESTAMP"
    if t in ("double precision", "real", "float", "float8", "float4", "double"):
        return "DOUBLE"

    m = DECIMAL_RE.fullmatch(t)
    if m:
        p, s = m.group(1), m.group(2)
        return f"DECIMAL({p},{s})"

    raise ValueError(f"Unknown type {raw_type!r} for column {col_name!r}")


def _extract_duplicate_yaml_key(exc: Any) -> Optional[str]:
    """Return the duplicate key string if `exc` is a duplicate-YAML-key ConstructorError."""
    from yaml.constructor import ConstructorError

    if not isinstance(exc, ConstructorError):
        return None
    if hasattr(exc, "duplicate_key"):
        return str(getattr(exc, "duplicate_key"))
    err_text = f"{getattr(exc, 'problem', '')} {exc}"
    if "duplicate YAML key" not in err_text:
        return None
    m = re.search(r"duplicate YAML key '([^']*)'", err_text)
    if m is not None:
        return m.group(1)
    m = re.search(r"duplicate YAML key\s+(\S+)", err_text)
    return m.group(1) if m is not None else "<unknown>"


def _load_json_data(data_or_path: Any) -> Any:
    if isinstance(data_or_path, (str, Path)):
        p = Path(data_or_path)
        suffix = p.suffix.lower()
        with open(p, "r", encoding="utf-8") as f:
            if suffix in (".yaml", ".yml"):
                import yaml

                from seeknal.serving.accel.loader import BadInputError, scrub_text
                from seeknal.serving.placement import UniqueKeySafeLoader

                try:
                    return yaml.load(f, Loader=UniqueKeySafeLoader)
                except yaml.YAMLError as exc:
                    mark = getattr(exc, "problem_mark", None)
                    dup_key = _extract_duplicate_yaml_key(exc)
                    if dup_key is not None:
                        safe_dup = (
                            scrub_text(dup_key)
                            .replace("\n", " ")
                            .replace("\r", " ")
                        )
                        if mark is not None:
                            raise BadInputError(
                                f"Invalid YAML in '{p}' at line {mark.line + 1}, "
                                f"column {mark.column + 1}: duplicate YAML key '{safe_dup}'"
                            ) from exc
                        raise BadInputError(
                            f"Invalid YAML in '{p}': duplicate YAML key '{safe_dup}'"
                        ) from exc
                    if mark is not None:
                        raise BadInputError(
                            f"Invalid YAML in '{p}' at line {mark.line + 1}, column {mark.column + 1}"
                        ) from exc
                    raise BadInputError(f"Invalid YAML in '{p}'") from exc
            return json.load(f)
    return data_or_path


def _normalize_registry(registry_raw: Any) -> dict[str, dict[str, Any]]:
    reg_data = _load_json_data(registry_raw)
    if not isinstance(reg_data, dict) or "reports" not in reg_data:
        raise ValueError("Registry must be a JSON object containing a 'reports' key")
    reports = reg_data["reports"]
    if not isinstance(reports, list) or len(reports) == 0:
        raise ValueError("Registry 'reports' must be a non-empty list")
    result: dict[str, dict[str, Any]] = {}
    for r in reports:
        if isinstance(r, dict) and "view" in r:
            result[r["view"]] = r
    if not result:
        raise ValueError(
            "Registry 'reports' list must contain valid report objects with a 'view' field"
        )
    return result


def _normalize_columns_with_meta(
    columns_raw: Any,
) -> list[tuple[str, list[dict[str, Any]], list[str], dict[str, Any]]]:
    """Returns list of (view_name, column_dicts, keys, meta_dict)."""
    col_data = _load_json_data(columns_raw)
    views_dict: dict[str, Any] = {}
    if isinstance(col_data, dict):
        if "views" in col_data:
            raw_views = col_data["views"]
            if isinstance(raw_views, dict):
                views_dict = raw_views
            elif isinstance(raw_views, list):
                for item in raw_views:
                    if isinstance(item, dict):
                        v_name = item.get("view") or item.get("name")
                        if v_name:
                            views_dict[v_name] = item
        else:
            views_dict = {
                k: v for k, v in col_data.items() if not str(k).startswith("_")
            }
    elif isinstance(col_data, list):
        for item in col_data:
            if isinstance(item, dict):
                v_name = item.get("view") or item.get("name")
                if v_name:
                    views_dict[v_name] = item

    normalized: list[tuple[str, list[dict[str, Any]], list[str], dict[str, Any]]] = []
    for view_name, val in views_dict.items():
        keys: list[str] = []
        columns_list: list[dict[str, Any]] = []
        meta: dict[str, Any] = {}
        if isinstance(val, list):
            columns_list = val
        elif isinstance(val, dict):
            columns_list = val.get("columns", [])
            keys = val.get("keys", [])
            meta = {
                k: v
                for k, v in val.items()
                if k not in ("columns", "keys", "view", "name")
            }
        normalized.append((view_name, columns_list, keys, meta))
    return normalized


def _normalize_columns(
    columns_raw: Any,
) -> list[tuple[str, list[dict[str, Any]], list[str]]]:
    """Returns list of (view_name, column_dicts, keys)."""
    return [
        (view_name, cols, keys)
        for view_name, cols, keys, _ in _normalize_columns_with_meta(columns_raw)
    ]


def resolve_column_pii(
    col: Mapping[str, Any],
    *,
    col_name: str,
    view_name: str,
    pii_data: Mapping[str, Any],
    pii_mode: str = DEFAULT_PII_MODE,
    allow_non_hashed_pii: bool = False,
    i_understand_pii_leaves_unmasked: bool = False,
) -> tuple[bool, bool, bool]:
    """Resolve PII handling for a column.

    Returns `(is_pii, is_nulled, is_excluded)`.
    """
    mode = validate_pii_mode(pii_mode)
    has_explicit_pii = "pii" in col
    raw_col_pii = col.get("pii")

    pii_key: Optional[str] = None
    inline_boolean_pii = False

    if isinstance(raw_col_pii, bool):
        if raw_col_pii is True:
            if col_name in pii_data:
                pii_key = col_name
            else:
                inline_boolean_pii = True
        elif col_name in pii_data:
            pii_key = col_name
    elif raw_col_pii is not None:
        if not isinstance(raw_col_pii, str) or not raw_col_pii.strip():
            raise ValueError(
                f"Column {col_name!r} in view {view_name!r} specifies an empty pii key"
            )
        pii_key = raw_col_pii.strip()
    elif has_explicit_pii:
        raise ValueError(
            f"Column {col_name!r} in view {view_name!r} specifies an empty pii key"
        )
    elif col_name in pii_data:
        pii_key = col_name

    if inline_boolean_pii:
        if mode == "exclude":
            return True, False, True
        return True, True, False

    if pii_key is None:
        return False, False, False

    if pii_key not in pii_data:
        raise ValueError(
            f"Column {col_name!r} in view {view_name!r} specifies pii key {pii_key!r} which is missing from pii_columns policy"
        )

    policy_entry = pii_data[pii_key]
    if not isinstance(policy_entry, Mapping):
        raise ValueError(f"Invalid policy entry for key {pii_key!r}")

    if mode == "exclude":
        return True, False, True

    source_form = str(policy_entry.get("source_form", "")).lower()
    mask_kind = str(policy_entry.get("mask", "")).lower()

    if source_form == "sha256":
        if mask_kind not in ("redact", "null"):
            raise ValueError(
                f"Column {col_name!r} in view {view_name!r} uses mask kind {mask_kind!r} which is not engine-neutral. "
                "Hashed PII columns are nulled in the cache, so only 'redact' and 'null' masks are permitted."
            )
        return True, True, False

    if not allow_non_hashed_pii:
        raise ValueError(
            f"Column {col_name!r} in view {view_name!r} has PII source_form {source_form!r} (not sha256). "
            "Non-hashed PII must not be cached without --allow-non-hashed-pii and --i-understand-pii-leaves-unmasked."
        )
    if not i_understand_pii_leaves_unmasked:
        raise ValueError(
            f"Column {col_name!r} in view {view_name!r} has PII source_form {source_form!r} (not sha256). "
            "--allow-non-hashed-pii also requires --i-understand-pii-leaves-unmasked (i_understand_pii_leaves_unmasked=True)."
        )

    from seeknal.serving.accel.loader import log_event

    log_event(
        "non_hashed_pii_unmasked_warning",
        level="WARNING",
        view=view_name,
        column=col_name,
        source_form=source_form,
        message=(
            f"Caching non-hashed PII column '{view_name}.{col_name}' "
            f"(source_form={source_form!r}) UNMASKED in DuckDB cache"
        ),
    )
    return True, False, False


def build_table_specs(
    columns: Union[dict, list, str, Path],
    pii_policy: Union[dict, str, Path],
    registry: Union[dict, list, str, Path],
    live_views: Optional[Iterable[str]] = DEFAULT_LIVE_VIEWS,
    allow_non_hashed_pii: bool = False,
    pii_mode: str = DEFAULT_PII_MODE,
    snapshot_pins: Optional[Mapping[str, int]] = None,
    source_overrides: Optional[Mapping[str, str]] = None,
    target_schema: str = "rpt",
    i_understand_pii_leaves_unmasked: bool = False,
) -> list[TableSpec]:
    """Build ordered list of table specifications for DuckDB caching."""
    mode = validate_pii_mode(pii_mode)
    validate_identifier(target_schema)
    if allow_non_hashed_pii and not i_understand_pii_leaves_unmasked:
        raise ValueError(
            "--allow-non-hashed-pii requires --i-understand-pii-leaves-unmasked "
            "(i_understand_pii_leaves_unmasked=True)"
        )

    pii_data = _load_json_data(pii_policy)
    if not isinstance(pii_data, Mapping):
        pii_data = {}

    reg_map = _normalize_registry(registry)
    if not reg_map:
        raise ValueError(
            "Registry is empty or has no reports; valid report registry required"
        )

    norm_columns = _normalize_columns_with_meta(columns)
    live_set = set(live_views) if live_views is not None else set(DEFAULT_LIVE_VIEWS)

    table_specs: list[TableSpec] = []

    for view_name, col_list, keys, col_meta in norm_columns:
        validate_identifier(view_name)

        if view_name not in reg_map:
            raise ValueError(
                f"View {view_name!r} listed in columns is missing from registry"
            )
        reg_entry = reg_map[view_name]
        status = str(reg_entry.get("status", "")).lower()
        if status not in ("ready", "contract"):
            reason = reg_entry.get("blocked_reason", f"status is {status!r}")
            raise ValueError(
                f"View {view_name!r} has registry status {status!r}; only 'ready' or 'contract' views may be cached: {reason}"
            )

        if view_name in live_set or bool(reg_entry.get("live", False)):
            continue

        column_specs: list[ColumnSpec] = []
        for col in col_list:
            col_name = col.get("name")
            if not col_name:
                raise ValueError(f"Column in view {view_name!r} missing 'name'")
            validate_identifier(col_name)

            raw_type = col.get("type")
            if not raw_type:
                raise ValueError(
                    f"Column {col_name!r} in view {view_name!r} missing 'type'"
                )
            duckdb_type = map_type(col_name, raw_type)
            validate_duckdb_type(duckdb_type, col_name)

            is_pii, is_nulled, is_excluded = resolve_column_pii(
                col,
                col_name=col_name,
                view_name=view_name,
                pii_data=pii_data,
                pii_mode=mode,
                allow_non_hashed_pii=allow_non_hashed_pii,
                i_understand_pii_leaves_unmasked=i_understand_pii_leaves_unmasked,
            )
            if is_excluded:
                continue

            column_specs.append(
                ColumnSpec(
                    name=col_name,
                    duckdb_type=duckdb_type,
                    is_nulled=is_nulled,
                    is_pii=is_pii,
                )
            )

        if not column_specs:
            raise ValueError(
                f"View {view_name!r} has no remaining columns to cache (check columns list and pii_mode={mode!r})"
            )

        target_name = f"{target_schema}.{view_name}"

        raw_snapshot_id: Any = None
        if snapshot_pins is not None:
            if target_name in snapshot_pins:
                raw_snapshot_id = snapshot_pins[target_name]
            elif view_name in snapshot_pins:
                raw_snapshot_id = snapshot_pins[view_name]
        if raw_snapshot_id is None and "snapshot_id" in col_meta:
            raw_snapshot_id = col_meta["snapshot_id"]
        if raw_snapshot_id is None and "snapshot_id" in reg_entry:
            raw_snapshot_id = reg_entry["snapshot_id"]

        snap_id: Optional[int] = None
        if raw_snapshot_id is not None:
            snap_id = validate_snapshot_id(
                raw_snapshot_id,
                context=f"View '{view_name}'",
            )

        source_ref = f"rpt_base.{view_name}"
        if source_overrides is not None:
            if target_name in source_overrides:
                source_ref = source_overrides[target_name]
            elif view_name in source_overrides:
                source_ref = source_overrides[view_name]
        elif isinstance(col_meta.get("source"), str) and col_meta["source"].strip():
            source_ref = col_meta["source"].strip()
        elif isinstance(reg_entry.get("source"), str) and reg_entry["source"].strip():
            source_ref = reg_entry["source"].strip()

        table_specs.append(
            TableSpec(
                name=target_name,
                source=source_ref,
                view=view_name,
                columns=tuple(column_specs),
                keys=tuple(keys),
                snapshot_id=snap_id,
            )
        )

    return table_specs

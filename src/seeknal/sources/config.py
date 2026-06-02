"""Typed source registry and context-sync helpers.

The registry is intentionally conservative: it describes where data comes
from and how the agent should reason about it, without opening network
connections or changing pipeline behavior.  CLI sync writes lightweight
context artifacts that the Ask harness can summarize in prompts.
"""
from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable, Protocol


ALLOWED_SOURCE_KINDS = {"managed", "connected", "external", "virtual"}
ALLOWED_SOURCE_TYPES = {
    "database",
    "seeknal",
    "file",
    "api",
    "semantic",
    "metric",
    "other",
}
ALLOWED_ACCESS = {"read_only", "read_write", "write_only", "none"}
ALLOWED_MODES = {
    "auto",
    "analyst",
    "explore",
    "validate",
    "build",
    "pipeline",
    "connected_analyst",
    "pipeline_analyst",
    "hybrid_analyst",
}
ALLOWED_REFRESH_POLICIES = {"manual", "on_start", "scheduled"}
BEHAVIORAL_ROLES = {
    "business_source_of_truth",
    "derived_pipeline_outputs",
    "raw_ingest",
    "semantic_layer",
    "metrics_layer",
    "reference_data",
    "sandbox",
    "other",
}

DEFAULT_MODE = "analyst"
DEFAULT_STALE_AFTER_HOURS = 168
DEFAULT_CONTEXT_TEMPLATES = ["overview", "columns"]
CONTEXT_ROOT = Path(".seeknal") / "context" / "sources"
CATALOG_ROOT = Path(".seeknal") / "catalog"


class SourceContextRepl(Protocol):
    """Minimal REPL surface needed for read-only source-context sync."""

    attached: set[str]

    def execute_oneshot(
        self,
        sql: str,
        limit: int | None = None,
    ) -> tuple[list[str], list[tuple]]:
        ...


class SourceConfigError(ValueError):
    """Raised when a source registry configuration is invalid."""


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    raise SourceConfigError(f"Expected a string or list, got {type(value).__name__}")


def _as_int(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SourceConfigError(f"Expected integer value, got {value!r}") from exc
    return parsed


@dataclass(frozen=True)
class ContextSyncConfig:
    """Settings for generating local source-context artifacts."""

    enabled: bool = False
    refresh_policy: str = "manual"
    stale_after_hours: int = DEFAULT_STALE_AFTER_HOURS
    templates: list[str] = field(
        default_factory=lambda: list(DEFAULT_CONTEXT_TEMPLATES)
    )
    sample_rows: int = 0
    max_cell_length: int = 200
    low_cardinality_threshold: int = 50
    max_scan_rows: int = 10_000
    max_columns_per_table: int = 50
    max_seconds_per_source: int = 60

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ContextSyncConfig":
        data = data or {}
        if not isinstance(data, dict):
            raise SourceConfigError("context_sync must be a mapping")

        sample = data.get("sample") or {}
        profiling = data.get("profiling") or {}
        if not isinstance(sample, dict):
            raise SourceConfigError("context_sync.sample must be a mapping")
        if not isinstance(profiling, dict):
            raise SourceConfigError("context_sync.profiling must be a mapping")

        refresh_policy = str(data.get("refresh_policy", "manual"))
        if refresh_policy not in ALLOWED_REFRESH_POLICIES:
            allowed = ", ".join(sorted(ALLOWED_REFRESH_POLICIES))
            raise SourceConfigError(
                f"Unsupported refresh_policy '{refresh_policy}'. Expected one of: {allowed}"
            )

        return cls(
            enabled=bool(data.get("enabled", False)),
            refresh_policy=refresh_policy,
            stale_after_hours=_as_int(
                data.get("stale_after_hours"), DEFAULT_STALE_AFTER_HOURS
            ),
            templates=_as_list(data.get("templates"))
            or list(DEFAULT_CONTEXT_TEMPLATES),
            sample_rows=_as_int(sample.get("rows"), 0),
            max_cell_length=_as_int(sample.get("max_cell_length"), 200),
            low_cardinality_threshold=_as_int(
                profiling.get("low_cardinality_threshold"), 50
            ),
            max_scan_rows=_as_int(profiling.get("max_scan_rows"), 10_000),
            max_columns_per_table=_as_int(profiling.get("max_columns_per_table"), 50),
            max_seconds_per_source=_as_int(profiling.get("max_seconds_per_source"), 60),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "refresh_policy": self.refresh_policy,
            "stale_after_hours": self.stale_after_hours,
            "templates": list(self.templates),
            "sample": {
                "rows": self.sample_rows,
                "max_cell_length": self.max_cell_length,
            },
            "profiling": {
                "low_cardinality_threshold": self.low_cardinality_threshold,
                "max_scan_rows": self.max_scan_rows,
                "max_columns_per_table": self.max_columns_per_table,
                "max_seconds_per_source": self.max_seconds_per_source,
            },
        }


@dataclass(frozen=True)
class SourceConfig:
    """One source declaration from ``seeknal_agent.yml``."""

    name: str
    source_kind: str
    source_type: str
    namespace: str
    access: str
    role: str
    priority: int = 0
    description: str = ""
    connector: str | None = None
    resource: str | None = None
    dsn_env: str | None = None
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    context_sync: ContextSyncConfig = field(default_factory=ContextSyncConfig)

    @classmethod
    def from_dict(
        cls,
        name: str,
        data: dict[str, Any],
        *,
        custom_roles: Iterable[str] = (),
    ) -> "SourceConfig":
        if not isinstance(data, dict):
            raise SourceConfigError(f"sources.{name} must be a mapping")

        source_kind = str(data.get("source_kind", "connected"))
        source_type = str(data.get("source_type", "database"))
        namespace = str(data.get("namespace", name)).strip()
        access = str(data.get("access", "read_only"))
        role = str(data.get("role", "other"))
        description = str(data.get("description", "") or "").strip()
        custom_role_set = {str(item) for item in custom_roles}

        _validate_choice("source_kind", source_kind, ALLOWED_SOURCE_KINDS, name)
        _validate_choice("source_type", source_type, ALLOWED_SOURCE_TYPES, name)
        _validate_choice("access", access, ALLOWED_ACCESS, name)

        if role not in BEHAVIORAL_ROLES and role not in custom_role_set:
            allowed = ", ".join(sorted(BEHAVIORAL_ROLES | custom_role_set))
            raise SourceConfigError(
                f"sources.{name}.role '{role}' is not recognized. "
                f"Use one of: {allowed}, or declare it under source_roles."
            )
        if role == "other" and not description:
            raise SourceConfigError(
                f"sources.{name}.role is 'other'; add a description so the agent "
                "knows how to use the source."
            )
        if not namespace:
            raise SourceConfigError(f"sources.{name}.namespace cannot be empty")

        connector = data.get("connector")
        resource = data.get("resource")
        if source_type == "database" and not connector:
            raise SourceConfigError(
                f"sources.{name} has source_type 'database'; set connector "
                "(for example postgresql, mysql, duckdb)."
            )
        if source_kind == "managed" and not resource:
            raise SourceConfigError(
                f"sources.{name} has source_kind 'managed'; set resource "
                "(for example project_outputs or semantic_layer)."
            )

        return cls(
            name=name,
            source_kind=source_kind,
            source_type=source_type,
            namespace=namespace,
            access=access,
            role=role,
            priority=_as_int(data.get("priority"), 0),
            description=description,
            connector=str(connector) if connector else None,
            resource=str(resource) if resource else None,
            dsn_env=str(data.get("dsn_env")) if data.get("dsn_env") else None,
            include=_as_list(data.get("include")),
            exclude=_as_list(data.get("exclude")),
            context_sync=ContextSyncConfig.from_dict(data.get("context_sync")),
        )

    @property
    def is_read_only(self) -> bool:
        return self.access == "read_only"

    @property
    def context_path(self) -> Path:
        return CONTEXT_ROOT / self.namespace / "SOURCE.md"

    def to_summary(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "namespace": self.namespace,
            "source_kind": self.source_kind,
            "source_type": self.source_type,
            "connector": self.connector,
            "access": self.access,
            "role": self.role,
            "priority": self.priority,
            "description": self.description,
            "context_sync": self.context_sync.to_dict(),
        }


def _validate_choice(
    field_name: str, value: str, allowed: set[str], source_name: str
) -> None:
    if value not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise SourceConfigError(
            f"sources.{source_name}.{field_name} '{value}' is not supported. "
            f"Expected one of: {allowed_text}."
        )


@dataclass(frozen=True)
class SourceRegistry:
    """All source declarations plus harness routing defaults."""

    sources: dict[str, SourceConfig]
    default_mode: str = DEFAULT_MODE
    explicit: bool = False
    project_path: Path | None = None

    @classmethod
    def from_agent_config(
        cls,
        config: dict[str, Any],
        *,
        project_path: Path | None = None,
    ) -> "SourceRegistry":
        raw_sources = config.get("sources") or {}
        explicit = bool(raw_sources)
        if not explicit:
            return cls(
                sources=_implicit_sources(project_path),
                default_mode=_parse_default_mode(config),
                explicit=False,
                project_path=project_path,
            )
        if not isinstance(raw_sources, dict):
            raise SourceConfigError("sources must be a mapping of source names")

        raw_roles = config.get("source_roles") or {}
        if raw_roles and not isinstance(raw_roles, dict):
            raise SourceConfigError("source_roles must be a mapping")
        custom_roles = raw_roles.keys() if isinstance(raw_roles, dict) else ()

        sources: dict[str, SourceConfig] = {}
        namespaces: dict[str, str] = {}
        for name, data in raw_sources.items():
            source = SourceConfig.from_dict(str(name), data, custom_roles=custom_roles)
            if source.namespace in namespaces:
                raise SourceConfigError(
                    f"sources.{source.name}.namespace duplicates "
                    f"sources.{namespaces[source.namespace]}.namespace: "
                    f"{source.namespace}"
                )
            namespaces[source.namespace] = source.name
            sources[source.name] = source

        return cls(
            sources=dict(sorted(sources.items(), key=lambda item: -item[1].priority)),
            default_mode=_parse_default_mode(config),
            explicit=True,
            project_path=project_path,
        )

    def selected(self, names: Iterable[str] | None = None) -> list[SourceConfig]:
        if not names:
            return list(self.sources.values())
        selected: list[SourceConfig] = []
        missing: list[str] = []
        for name in names:
            source = self.sources.get(name)
            if source is None:
                source = next(
                    (item for item in self.sources.values() if item.namespace == name),
                    None,
                )
            if source is None:
                missing.append(name)
            else:
                selected.append(source)
        if missing:
            raise SourceConfigError(f"Unknown source(s): {', '.join(missing)}")
        return selected

    def to_prompt_context(self, sync_state: dict[str, Any] | None = None) -> str | None:
        """Render compact source guidance for the Ask system prompt."""
        if not self.sources:
            return None

        sync_state = sync_state or {}
        state_sources = (
            sync_state.get("sources") if isinstance(sync_state, dict) else {}
        )
        if not isinstance(state_sources, dict):
            state_sources = {}

        lines = ["## Data Sources & Mode Policy"]
        lines.append(
            f"- Default mode: `{self.default_mode}`. `auto` means choose the "
            "lowest-risk read-only analytical path from the declared sources: "
            "`connected_analyst` for read-only external databases, "
            "`pipeline_analyst` for managed Seeknal outputs, and "
            "`hybrid_analyst` when both are relevant."
        )
        lines.append(
            "- Use read-only analysis by default; do not build pipelines, write "
            "files, publish reports, or mutate external systems unless the user "
            "explicitly requests an elevated mode/action."
        )
        lines.append(
            "- Connected read-only sources are authoritative for analytics, but they are "
            "not pipeline inputs unless a pipeline explicitly references them."
        )
        lines.append(
            "- You may use SQL and Python analysis/modeling against registered read-only "
            "tables when that helps answer the business question."
        )

        for source in self.sources.values():
            state = state_sources.get(source.name) or {}
            synced_at = state.get("synced_at", "not synced")
            status = state.get("status", "unknown")
            connector = f"/{source.connector}" if source.connector else ""
            desc = f" — {source.description}" if source.description else ""
            lines.append(
                f"- `{source.name}` (`{source.namespace}`): "
                f"{source.source_kind} {source.source_type}{connector}, "
                f"access `{source.access}`, role `{source.role}`, "
                f"context {status} at {synced_at}{desc}"
            )

        return "\n".join(lines)


def _parse_default_mode(config: dict[str, Any]) -> str:
    mode_config = config.get("mode") or {}
    if isinstance(mode_config, dict):
        value = str(mode_config.get("default", DEFAULT_MODE))
    elif isinstance(mode_config, str):
        value = mode_config
    else:
        value = DEFAULT_MODE
    if value not in ALLOWED_MODES:
        allowed = ", ".join(sorted(ALLOWED_MODES))
        raise SourceConfigError(
            f"Unsupported default mode '{value}'. Expected: {allowed}"
        )
    return value


def _implicit_sources(project_path: Path | None) -> dict[str, SourceConfig]:
    """Return backward-compatible implicit managed source declarations."""
    sources: dict[str, SourceConfig] = {}
    project_path = project_path or Path.cwd()
    if (project_path / "target").exists() or (project_path / "seeknal").exists():
        sources["pipeline_outputs"] = SourceConfig(
            name="pipeline_outputs",
            source_kind="managed",
            source_type="seeknal",
            namespace="pipe",
            access="read_only",
            role="derived_pipeline_outputs",
            priority=0,
            description="Implicit Seeknal project outputs registered by the current project.",
            resource="project_outputs",
            context_sync=ContextSyncConfig(enabled=False),
        )
    return sources


def load_source_registry(project_path: Path) -> SourceRegistry:
    """Load the source registry from the optional agent config."""
    from seeknal.ask.config import load_agent_config

    return SourceRegistry.from_agent_config(
        load_agent_config(project_path), project_path=project_path
    )


def read_sync_state(project_path: Path) -> dict[str, Any]:
    """Read ``.seeknal/catalog/sync_state.json`` if present."""
    state_path = project_path / CATALOG_ROOT / "sync_state.json"
    if not state_path.exists():
        return {}
    try:
        data = json.loads(state_path.read_text())
    except json.JSONDecodeError as exc:
        raise SourceConfigError(f"Invalid sync state JSON: {state_path}") from exc
    return data if isinstance(data, dict) else {}


def write_source_context(
    registry: SourceRegistry,
    project_path: Path,
    *,
    source_names: Iterable[str] | None = None,
    repl: SourceContextRepl | None = None,
) -> list[dict[str, Any]]:
    """Write context files for configured sources.

    When a REPL with attached read-only database catalogs is provided, this
    writes NAO-inspired table context files (`columns.md`, optional
    `preview.md`, `profiling.md`, and source-level `relationships.md`).  When
    no REPL is provided, it falls back to metadata-only files so headless and
    offline environments remain safe.
    """
    selected = registry.selected(source_names)
    now = datetime.now(timezone.utc).isoformat()
    context_root = project_path / CONTEXT_ROOT
    catalog_root = project_path / CATALOG_ROOT
    context_root.mkdir(parents=True, exist_ok=True)
    catalog_root.mkdir(parents=True, exist_ok=True)

    prior_state = read_sync_state(project_path)
    source_states = prior_state.get("sources") if isinstance(prior_state, dict) else {}
    if not isinstance(source_states, dict):
        source_states = {}

    results: list[dict[str, Any]] = []
    for source in selected:
        source_dir = context_root / source.namespace
        source_dir.mkdir(parents=True, exist_ok=True)
        source_file = source_dir / "SOURCE.md"

        table_context = _sync_attached_database_context(
            source=source,
            source_dir=source_dir,
            repl=repl,
        )
        status = table_context["status"]
        source_state = {
            "name": source.name,
            "namespace": source.namespace,
            "status": status,
            "synced_at": now,
            "source_kind": source.source_kind,
            "source_type": source.source_type,
            "access": source.access,
            "role": source.role,
            "templates": list(source.context_sync.templates),
            "tables": table_context["tables"],
            "columns": table_context["columns"],
            "context_path": str(source.context_path),
            "_catalog_tables": table_context["_catalog_tables"],
            "_catalog_columns": table_context["_catalog_columns"],
        }
        source_file.write_text(
            _render_source_markdown(
                source,
                now,
                sync_status=status,
                table_count=table_context["tables"],
                column_count=table_context["columns"],
            ),
            encoding="utf-8",
        )
        source_states[source.name] = source_state
        results.append(source_state)

    public_source_states = {
        name: {
            key: value
            for key, value in source_state.items()
            if not key.startswith("_catalog_")
        }
        for name, source_state in source_states.items()
    }
    state = {
        "version": 1,
        "synced_at": now,
        "sources": public_source_states,
    }
    (catalog_root / "sync_state.json").write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n"
    )

    _write_catalog_jsonl(
        catalog_root / "tables.jsonl",
        [
            item
            for result in results
            for item in result.get("_catalog_tables", [])
            if isinstance(result.get("_catalog_tables"), list)
        ],
    )
    _write_catalog_jsonl(
        catalog_root / "columns.jsonl",
        [
            item
            for result in results
            for item in result.get("_catalog_columns", [])
            if isinstance(result.get("_catalog_columns"), list)
        ],
    )
    for result in results:
        result.pop("_catalog_tables", None)
        result.pop("_catalog_columns", None)
    return results


def _ensure_jsonl(path: Path) -> None:
    if not path.exists():
        path.write_text("")


def _write_catalog_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        _ensure_jsonl(path)
        return
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _sync_attached_database_context(
    *,
    source: SourceConfig,
    source_dir: Path,
    repl: SourceContextRepl | None,
) -> dict[str, Any]:
    """Best-effort read-only context sync for one attached database source."""
    context = {
        "status": "metadata_only",
        "tables": 0,
        "columns": 0,
        "_catalog_tables": [],
        "_catalog_columns": [],
    }

    if repl is None or not source.context_sync.enabled:
        return context
    if source.source_kind != "connected" or source.source_type != "database":
        return context
    if source.namespace not in (getattr(repl, "attached", set()) or set()):
        return context

    tables = _discover_source_tables(source, repl)
    if not tables:
        return context

    tables_dir = source_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    relationship_lines = [
        f"# Relationships for {source.name}",
        "",
        "These relationship hints are inferred from column-name conventions. "
        "Verify joins with data before relying on them for business-critical answers.",
        "",
    ]
    all_columns: dict[str, list[dict[str, str]]] = {}
    table_catalog: list[dict[str, Any]] = []
    column_catalog: list[dict[str, Any]] = []

    templates = {template.lower() for template in source.context_sync.templates}
    for table in tables:
        schema = table["schema"]
        table_name = table["table"]
        table_type = table["type"]
        qualified = f"{source.namespace}.{schema}.{table_name}"
        table_dir = tables_dir / _safe_context_name(f"{schema}.{table_name}")
        table_dir.mkdir(parents=True, exist_ok=True)

        columns = _discover_source_columns(source.namespace, schema, table_name, repl)
        all_columns[f"{schema}.{table_name}"] = columns
        table_catalog.append(
            {
                "source": source.name,
                "namespace": source.namespace,
                "schema": schema,
                "table": table_name,
                "qualified_name": qualified,
                "table_type": table_type,
            }
        )
        for index, col in enumerate(columns, start=1):
            column_catalog.append(
                {
                    "source": source.name,
                    "namespace": source.namespace,
                    "schema": schema,
                    "table": table_name,
                    "qualified_name": qualified,
                    "column": col["name"],
                    "type": col["type"],
                    "ordinal": index,
                }
            )

        if "overview" in templates:
            (table_dir / "overview.md").write_text(
                _render_table_overview(qualified, table_type, columns),
                encoding="utf-8",
            )
        if "columns" in templates:
            (table_dir / "columns.md").write_text(
                _render_columns_markdown(qualified, columns),
                encoding="utf-8",
            )
        if "preview" in templates and source.context_sync.sample_rows > 0:
            preview = _preview_table(
                qualified,
                repl,
                limit=source.context_sync.sample_rows,
                max_cell_length=source.context_sync.max_cell_length,
            )
            (table_dir / "preview.md").write_text(preview, encoding="utf-8")
        if "profiling" in templates:
            profile = _profile_table(
                qualified,
                columns,
                repl,
                max_columns=source.context_sync.max_columns_per_table,
                max_scan_rows=source.context_sync.max_scan_rows,
            )
            (table_dir / "profiling.md").write_text(profile, encoding="utf-8")

    if "relationships" in templates:
        relationship_lines.extend(_infer_relationships(all_columns, source.namespace))
        (source_dir / "relationships.md").write_text(
            "\n".join(relationship_lines).rstrip() + "\n",
            encoding="utf-8",
        )

    context.update(
        {
            "status": "synced",
            "tables": len(table_catalog),
            "columns": len(column_catalog),
            "_catalog_tables": table_catalog,
            "_catalog_columns": column_catalog,
        }
    )
    return context


def _discover_source_tables(
    source: SourceConfig,
    repl: SourceContextRepl,
) -> list[dict[str, str]]:
    sql = f"""
        SELECT table_schema, table_name, table_type
        FROM "{source.namespace}".information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """
    _columns, rows = repl.execute_oneshot(sql)
    tables: list[dict[str, str]] = []
    for schema, table, table_type in rows:
        schema_s = str(schema)
        table_s = str(table)
        if not _source_table_included(source, schema_s, table_s):
            continue
        tables.append(
            {
                "schema": schema_s,
                "table": table_s,
                "type": str(table_type or "table"),
            }
        )
    return tables


def _source_table_included(source: SourceConfig, schema: str, table: str) -> bool:
    full = f"{schema}.{table}"
    if source.include and not any(fnmatch.fnmatch(full, pattern) for pattern in source.include):
        return False
    if source.exclude and any(fnmatch.fnmatch(full, pattern) for pattern in source.exclude):
        return False
    return True


def _discover_source_columns(
    namespace: str,
    schema: str,
    table: str,
    repl: SourceContextRepl,
) -> list[dict[str, str]]:
    sql = f"""
        SELECT column_name, data_type
        FROM "{namespace}".information_schema.columns
        WHERE table_schema = '{schema.replace("'", "''")}'
          AND table_name = '{table.replace("'", "''")}'
        ORDER BY ordinal_position
    """
    _headers, rows = repl.execute_oneshot(sql)
    return [{"name": str(name), "type": str(data_type)} for name, data_type in rows]


def _render_table_overview(
    qualified: str,
    table_type: str,
    columns: list[dict[str, str]],
) -> str:
    return "\n".join(
        [
            f"# {qualified}",
            "",
            f"- Type: `{table_type}`",
            f"- Columns: `{len(columns)}`",
            "",
            "Use this file as a compact table overview. Load `columns.md`, "
            "`profiling.md`, or `preview.md` when deeper context is needed.",
            "",
        ]
    )


def _render_columns_markdown(qualified: str, columns: list[dict[str, str]]) -> str:
    lines = [f"# Columns for {qualified}", ""]
    for col in columns:
        lines.append(f"- `{col['name']}` ({col['type']})")
    lines.append("")
    return "\n".join(lines)


def _preview_table(
    qualified: str,
    repl: SourceContextRepl,
    *,
    limit: int,
    max_cell_length: int,
) -> str:
    try:
        columns, rows = repl.execute_oneshot(f"SELECT * FROM {qualified}", limit=limit)
        # Runtime governance: when Atlas is configured (ATLAS_API_URL), enforce read
        # access on the table and mask sensitive columns before sample rows are
        # persisted into the Ask context. Inactive when Atlas is not configured. A
        # denial raises AtlasPolicyDenied, which degrades below to "Preview
        # unavailable" so no governed rows leak into context.
        from seeknal.integrations.atlas_governance import (
            create_governance_gate_from_env,
            govern_read,
        )

        rows = govern_read(
            create_governance_gate_from_env(),
            resource=qualified,
            columns=columns,
            rows=rows,
        )
    except Exception as exc:  # noqa: BLE001 - sync should continue
        return f"# Preview for {qualified}\n\nPreview unavailable: {exc}\n"

    lines = [f"# Preview for {qualified}", "", f"Rows: `{len(rows)}`", ""]
    for row in rows:
        item = {
            str(col): _truncate_context_cell(value, max_cell_length)
            for col, value in zip(columns, row, strict=False)
        }
        lines.append(f"- {json.dumps(item, sort_keys=True, default=str)}")
    lines.append("")
    return "\n".join(lines)


def _profile_table(
    qualified: str,
    columns: list[dict[str, str]],
    repl: SourceContextRepl,
    *,
    max_columns: int,
    max_scan_rows: int,
) -> str:
    selected = columns[:max_columns]
    # Keep connected-source sync safe and predictable: fetch one bounded sample
    # per table and compute lightweight stats locally instead of issuing one
    # COUNT(DISTINCT ...) query per column against the external database.
    scan_limit = max(0, min(int(max_scan_rows), 1_000))
    lines = [
        f"# Profiling for {qualified}",
        "",
        f"Max scan rows: `{scan_limit}`",
        "",
    ]
    if scan_limit <= 0:
        lines.append("Profiling scan disabled by configuration.")
        lines.append("")
        return "\n".join(lines)

    try:
        fetched_columns, rows = repl.execute_oneshot(
            f"SELECT * FROM {qualified}",
            limit=scan_limit,
        )
    except Exception as exc:  # noqa: BLE001 - sync should continue
        return f"# Profiling for {qualified}\n\nProfiling unavailable: {exc}\n"

    rows = rows[:scan_limit]
    column_positions = {str(name): index for index, name in enumerate(fetched_columns)}
    for col in selected:
        name = col["name"]
        pos = column_positions.get(name)
        if pos is None:
            lines.append(f"- `{name}` ({col['type']}): not present in sample result")
            continue
        values = [row[pos] if pos < len(row) else None for row in rows]
        nulls = sum(1 for value in values if value is None)
        distinct_values = {str(value) for value in values if value is not None}
        lines.append(
            f"- `{name}` ({col['type']}): scanned={len(values)}, "
            f"nulls={nulls}, distinct={len(distinct_values)}"
        )
    if len(columns) > max_columns:
        lines.append(f"- ... {len(columns) - max_columns} more columns not profiled")
    lines.append("")
    return "\n".join(lines)


def _infer_relationships(
    all_columns: dict[str, list[dict[str, str]]],
    namespace: str,
) -> list[str]:
    key_locations: dict[str, list[str]] = {}
    for table, columns in all_columns.items():
        for col in columns:
            name = col["name"].lower()
            if name.startswith("id_") or name.endswith("_id") or name == "id":
                key_locations.setdefault(name, []).append(table)

    lines: list[str] = []
    for key, tables in sorted(key_locations.items()):
        if len(tables) < 2:
            continue
        qualified = [f"`{namespace}.{table}.{key}`" for table in tables]
        lines.append(f"- {key}: " + " ↔ ".join(qualified))
    if not lines:
        lines.append("- No obvious same-name key relationships inferred.")
    return lines


def _safe_context_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in value)


def _truncate_context_cell(value: Any, max_len: int) -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    text = str(value)
    if len(text) > max_len:
        return text[: max(0, max_len - 1)] + "…"
    return text


def _render_source_markdown(
    source: SourceConfig,
    synced_at: str,
    *,
    sync_status: str = "metadata_only",
    table_count: int = 0,
    column_count: int = 0,
) -> str:
    lines = [
        f"# Source: {source.name}",
        "",
        f"- Namespace: `{source.namespace}`",
        f"- Kind: `{source.source_kind}`",
        f"- Type: `{source.source_type}`",
        f"- Connector: `{source.connector or 'n/a'}`",
        f"- Access: `{source.access}`",
        f"- Role: `{source.role}`",
        f"- Priority: `{source.priority}`",
        f"- Synced at: `{synced_at}`",
        f"- Sync status: `{sync_status}`",
    ]
    if sync_status == "synced":
        lines.append(f"- Discovered tables: `{table_count}`")
        lines.append(f"- Discovered columns: `{column_count}`")
    if source.description:
        lines.append(f"- Description: {source.description}")
    if source.resource:
        lines.append(f"- Resource: `{source.resource}`")
    if source.dsn_env:
        lines.append(f"- DSN environment variable: `{source.dsn_env}`")
    if source.include:
        lines.append(
            f"- Include patterns: {', '.join(f'`{p}`' for p in source.include)}"
        )
    if source.exclude:
        lines.append(
            f"- Exclude patterns: {', '.join(f'`{p}`' for p in source.exclude)}"
        )

    lines.extend(
        [
            "",
            "## Context Sync",
            f"- Enabled: `{source.context_sync.enabled}`",
            f"- Refresh policy: `{source.context_sync.refresh_policy}`",
            f"- Stale after hours: `{source.context_sync.stale_after_hours}`",
            f"- Templates: {', '.join(f'`{t}`' for t in source.context_sync.templates)}",
            f"- Sample rows: `{source.context_sync.sample_rows}`",
            f"- Max scan rows: `{source.context_sync.max_scan_rows}`",
            "",
        ]
    )
    lines.extend(["## Discovery Status"])
    if sync_status == "synced":
        lines.extend(
            [
                "Connector discovery completed. Table context files are available under "
                "`tables/` for this source, and relationship hints may be available in "
                "`relationships.md` depending on the configured templates.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "This context file is metadata-only. Table and column catalogs will be "
                "populated by connector-specific discovery when available.",
                "",
            ]
        )
    return "\n".join(lines)

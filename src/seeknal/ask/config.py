"""Seeknal Ask agent configuration loader.

Reads ``seeknal_agent.yml`` from the project root and exposes typed helpers
for each config section.  Missing file or missing keys silently fall back
to defaults — the config file is entirely optional.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Optional

_CONFIG_FILENAME = "seeknal_agent.yml"
_CONFIG_CANDIDATES = (
    Path(_CONFIG_FILENAME),
    Path("seeknal") / _CONFIG_FILENAME,
)


def find_agent_config_path(project_path: Path) -> Optional[Path]:
    """Return the first existing ask config path for a project."""
    for candidate in _CONFIG_CANDIDATES:
        config_path = project_path / candidate
        if config_path.exists():
            return config_path
    return None


def load_agent_config(project_path: Path) -> dict[str, Any]:
    """Load ``seeknal_agent.yml`` from *project_path*, returning a dict.

    Returns an empty dict when the file does not exist.
    """
    config_path = find_agent_config_path(project_path)
    if config_path is None:
        return {}

    try:
        import yaml  # pyyaml — already a seeknal dependency
    except ImportError:
        warnings.warn(
            "PyYAML is required to load seeknal_agent.yml. "
            "Install with: pip install pyyaml"
        )
        return {}

    with open(config_path) as f:
        data = yaml.safe_load(f)

    return data if isinstance(data, dict) else {}


def get_agent_settings(config: dict[str, Any]) -> dict[str, Any]:
    """Return the nested ``agent`` settings map, if present."""
    agent = config.get("agent", {})
    return agent if isinstance(agent, dict) else {}


def _get_mapping(config: dict[str, Any], key: str) -> dict[str, Any]:
    """Return a nested mapping, or ``{}`` when the section is absent/invalid."""
    value = config.get(key, {})
    return value if isinstance(value, dict) else {}


def _coerce_bool(value: Any, default: bool) -> bool:
    """Coerce common YAML/env string booleans while preserving safe defaults."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
    return default


def _coerce_int(value: Any, default: int | None) -> int | None:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _coerce_float(value: Any, default: float | None) -> float | None:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _coerce_auto_bool(value: Any, default: str | bool = "auto") -> str | bool:
    """Return ``"auto"`` or a concrete boolean for tri-state settings."""
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() == "auto":
        return "auto"
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
    return default


def get_agent_profile_instructions(config: dict[str, Any]) -> Optional[str]:
    """Build dynamic prompt instructions from the ``agent`` section."""
    agent = get_agent_settings(config)
    if not agent:
        return None

    lines: list[str] = []

    name = agent.get("name")
    if name:
        lines.append(f"## Project Agent Profile\n\n- Agent name: **{name}**")

    description = agent.get("description")
    if description:
        if not lines:
            lines.append("## Project Agent Profile")
        lines.append(f"- Description: {str(description).strip()}")

    skills = agent.get("skills")
    if isinstance(skills, list) and skills:
        if not lines:
            lines.append("## Project Agent Profile")
        skill_list = ", ".join(f"`{skill}`" for skill in skills)
        lines.append(f"- Preferred project skills: {skill_list}")

    system_prompt = agent.get("system_prompt")
    if system_prompt:
        lines.append("\n## Project-Specific Instructions\n")
        lines.append(str(system_prompt).strip())

    return "\n".join(lines).strip() or None


# ---------------------------------------------------------------------------
# Locale helpers
# ---------------------------------------------------------------------------

def get_locale_instructions(config: dict[str, Any]) -> Optional[str]:
    """Build a system-prompt snippet from the ``locale`` section.

    Returns ``None`` when no locale is configured.

    Supported keys inside ``locale:``:
        currency        – ISO 4217 code (e.g. "IDR", "USD")
        currency_symbol – display symbol (e.g. "Rp", "$")
        language        – natural language for responses (e.g. "Indonesian")
        number_format   – locale tag for formatting (e.g. "id_ID", "en_US")
        timezone        – IANA timezone (e.g. "Asia/Jakarta")
    """
    locale: dict[str, Any] = config.get("locale", {})
    if not locale:
        return None

    lines: list[str] = ["\n## Locale & Formatting"]

    currency = locale.get("currency")
    symbol = locale.get("currency_symbol")
    if currency and symbol:
        lines.append(
            f"- All monetary values are in **{currency}** ({symbol}). "
            f"Always display amounts with the {symbol} symbol. "
            "Never convert to USD or other currencies unless the user explicitly asks."
        )
    elif currency:
        lines.append(
            f"- All monetary values are in **{currency}**. "
            "Never convert to USD or other currencies unless the user explicitly asks."
        )

    number_format = locale.get("number_format")
    if number_format:
        lines.append(f"- Use **{number_format}** number formatting conventions.")

    language = locale.get("language")
    if language:
        lines.append(f"- Respond in **{language}** unless the user writes in another language.")

    timezone = locale.get("timezone")
    if timezone:
        lines.append(f"- Assume timestamps are in **{timezone}** unless stated otherwise.")

    # Only a header — nothing useful to add
    if len(lines) == 1:
        return None

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Agent limits
# ---------------------------------------------------------------------------

_DEFAULT_REQUEST_LIMIT = 100
_DEFAULT_SQL_TIMEOUT_SECONDS = 60
_DEFAULT_DISCOVERY_CACHE_TTL_SECONDS = 300


def get_request_limit(config: dict[str, Any]) -> int:
    """Return the ``request_limit`` from config, or the default (100).

    Controls how many LLM requests a single agent turn can make before
    pydantic-ai raises a ``UsageLimitExceeded`` error.
    """
    value = config.get("request_limit")
    if value is None:
        return _DEFAULT_REQUEST_LIMIT
    try:
        limit = int(value)
        return limit if limit > 0 else _DEFAULT_REQUEST_LIMIT
    except (TypeError, ValueError):
        return _DEFAULT_REQUEST_LIMIT


def get_sql_timeout_seconds(config: dict[str, Any]) -> int:
    """Return the per-query SQL timeout in seconds.

    This protects read-only tap-in sessions from accidental long scans against
    attached databases. Set ``sql_timeout_seconds: 0`` to disable the timeout.
    """
    value = config.get("sql_timeout_seconds")
    if value is None:
        return _DEFAULT_SQL_TIMEOUT_SECONDS
    try:
        timeout = int(value)
        return timeout if timeout >= 0 else _DEFAULT_SQL_TIMEOUT_SECONDS
    except (TypeError, ValueError):
        return _DEFAULT_SQL_TIMEOUT_SECONDS


def get_discovery_cache_ttl_seconds(config: dict[str, Any]) -> int:
    """Return the table/schema discovery cache TTL in seconds.

    Set ``discovery_cache_ttl_seconds: 0`` to disable in-session discovery
    caching. The cache is per Ask session and never shared across projects.
    """
    value = config.get("discovery_cache_ttl_seconds")
    if value is None:
        return _DEFAULT_DISCOVERY_CACHE_TTL_SECONDS
    try:
        ttl = int(value)
        return ttl if ttl >= 0 else _DEFAULT_DISCOVERY_CACHE_TTL_SECONDS
    except (TypeError, ValueError):
        return _DEFAULT_DISCOVERY_CACHE_TTL_SECONDS


# ---------------------------------------------------------------------------
# Background threshold
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Context budget
# ---------------------------------------------------------------------------

_DEFAULT_CONTEXT_BUDGET = 8000  # chars ≈ 2000 tokens


def get_context_budget(config: dict[str, Any]) -> int:
    """Return the ``context_budget`` from config, or the default (8000 chars).

    Controls the maximum character size of the project context injected
    into the agent's system prompt per turn.  Approximately 4 chars per token.
    """
    value = config.get("context_budget")
    if value is None:
        return _DEFAULT_CONTEXT_BUDGET
    try:
        budget = int(value)
        return budget if budget > 0 else _DEFAULT_CONTEXT_BUDGET
    except (TypeError, ValueError):
        return _DEFAULT_CONTEXT_BUDGET


# ---------------------------------------------------------------------------
# Background threshold
# ---------------------------------------------------------------------------

_DEFAULT_BACKGROUND_THRESHOLD = 60  # seconds


def get_background_threshold(config: dict[str, Any]) -> int:
    """Return the ``background_threshold`` from config, or the default (60s).

    Tool calls exceeding this duration are auto-backgrounded.
    """
    value = config.get("background_threshold")
    if value is None:
        return _DEFAULT_BACKGROUND_THRESHOLD
    try:
        threshold = int(value)
        return threshold if threshold > 0 else _DEFAULT_BACKGROUND_THRESHOLD
    except (TypeError, ValueError):
        return _DEFAULT_BACKGROUND_THRESHOLD


# ---------------------------------------------------------------------------
# pydantic-deep agent harness configuration
# ---------------------------------------------------------------------------

_DEFAULT_HARNESS_SECTION = "agent_harness"


def get_agent_harness_settings(config: dict[str, Any]) -> dict[str, Any]:
    """Return optional low-level pydantic-deep harness settings.

    The ``agent_harness`` section exposes framework/runtime controls without
    mixing them into the business-facing ``agent`` profile. Missing or invalid
    values intentionally fall back to the historical Seeknal Ask behavior.
    """
    return _get_mapping(config, _DEFAULT_HARNESS_SECTION)


def get_auto_summarization_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized auto-summarization/context-management config."""
    harness = get_agent_harness_settings(config)
    section = _get_mapping(harness, "auto_summarization")
    microcompact = _get_mapping(section, "microcompact")
    sql_compactor = _get_mapping(section, "sql_result_compactor")

    return {
        "enabled": _coerce_bool(section.get("enabled"), True),
        "context_manager": _coerce_auto_bool(section.get("context_manager"), "auto"),
        "context_manager_max_tokens": _coerce_int(
            section.get("context_manager_max_tokens"), None
        ),
        "summarization_model": section.get("summarization_model") or None,
        "eviction_token_limit": _coerce_int(section.get("eviction_token_limit"), 20000),
        "patch_tool_calls": _coerce_bool(section.get("patch_tool_calls"), True),
        "microcompact": {
            "enabled": _coerce_bool(microcompact.get("enabled"), True),
            "keep_recent_turns_analysis": _coerce_int(
                microcompact.get("keep_recent_turns_analysis"), 2
            ),
            "keep_recent_turns_full": _coerce_int(
                microcompact.get("keep_recent_turns_full"), 3
            ),
        },
        "sql_result_compactor": {
            "enabled": _coerce_bool(sql_compactor.get("enabled"), True),
            "min_chars_analysis": _coerce_int(
                sql_compactor.get("min_chars_analysis"), 250
            ),
            "min_chars_full": _coerce_int(sql_compactor.get("min_chars_full"), 500),
        },
    }


def get_cost_tracking_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized pydantic-deep cost tracking config."""
    section = _get_mapping(get_agent_harness_settings(config), "cost_tracking")
    return {
        "enabled": _coerce_bool(section.get("enabled"), True),
        "budget_usd": _coerce_float(section.get("budget_usd"), None),
    }


def get_hooks_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized pydantic-deep hook enablement config."""
    section = _get_mapping(get_agent_harness_settings(config), "hooks")
    return {
        "enabled": _coerce_bool(section.get("enabled"), True),
        "sql_security": _coerce_bool(section.get("sql_security"), True),
        "sql_self_correction": _coerce_bool(section.get("sql_self_correction"), True),
    }


def get_plan_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized plan-mode config for pydantic-deep."""
    section = _get_mapping(get_agent_harness_settings(config), "plan")
    return {
        "enabled": _coerce_auto_bool(section.get("enabled"), "auto"),
        "plans_dir": str(section.get("plans_dir") or ".seeknal/plans"),
    }


def get_stuck_loop_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized stuck-loop detection config."""
    section = _get_mapping(get_agent_harness_settings(config), "stuck_loop_detection")
    return {"enabled": _coerce_bool(section.get("enabled"), True)}


def get_subagents_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized parent-child subagent config."""
    section = _get_mapping(get_agent_harness_settings(config), "subagents")
    return {
        "enabled": _coerce_auto_bool(section.get("enabled"), "auto"),
        "include_builtin": _coerce_bool(section.get("include_builtin"), True),
        "lineage_investigator": _coerce_bool(
            section.get("lineage_investigator"), True
        ),
    }


def get_teams_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return normalized pydantic-deep TeamCapability config."""
    section = _get_mapping(get_agent_harness_settings(config), "teams")
    return {"enabled": _coerce_bool(section.get("enabled"), False)}


# ---------------------------------------------------------------------------
# Source registry prompt helpers
# ---------------------------------------------------------------------------

def get_source_registry_instructions(config: dict[str, Any]) -> Optional[str]:
    """Build dynamic prompt guidance from ``sources`` config and sync state.

    ``create_agent`` injects ``__project_path`` into the config copy before
    calling the prompt builder. Tests or callers that build prompts directly
    can still pass plain config; in that case the section renders without sync
    state and uses the current directory only as a fallback.
    """
    if not config:
        return None

    # Avoid rendering an implicit source section for projects with no explicit
    # registry when callers build a generic prompt outside a Seeknal project.
    has_sources = isinstance(config.get("sources"), dict) and bool(config.get("sources"))
    has_mode = "mode" in config
    if not has_sources and not has_mode:
        return None

    try:
        from seeknal.sources.config import (
            SourceConfigError,
            SourceRegistry,
            read_sync_state,
        )
    except ImportError:
        return None

    project_value = config.get("__project_path")
    project_path = Path(project_value) if project_value else Path.cwd()

    try:
        registry = SourceRegistry.from_agent_config(config, project_path=project_path)
        sync_state = read_sync_state(project_path)
    except SourceConfigError as exc:
        return (
            "## Data Sources & Mode Policy\n"
            f"- Source registry config is invalid: {exc}\n"
            "- Proceed conservatively with read-only project tables only; do not "
            "elevate to build/pipeline actions from this config."
        )

    if not registry.explicit and not has_mode:
        return None
    return registry.to_prompt_context(sync_state)


# ---------------------------------------------------------------------------
# Toolset routing
# ---------------------------------------------------------------------------

_READ_ONLY_ANALYST_MODES = {
    "analyst",
    "explore",
    "validate",
    "connected_analyst",
    "pipeline_analyst",
    "hybrid_analyst",
}


def get_ask_toolset_mode(config: dict[str, Any]) -> str:
    """Return the concrete Ask toolset mode for an agent config.

    ``full`` is the legacy default for projects without an explicit source
    registry or for build/pipeline modes. ``analysis`` is a physically narrower
    tool surface for read-only business-question users: query/discovery plus
    Python analysis, with build/publish/write/ingest tools omitted.
    """
    if not config:
        return "full"

    try:
        from seeknal.sources.config import SourceConfigError, SourceRegistry

        registry = SourceRegistry.from_agent_config(config)
    except (ImportError, SourceConfigError):
        return "full"

    mode = registry.default_mode
    if mode in {"build", "pipeline"}:
        return "full"

    if not registry.explicit:
        return "full"

    sources = list(registry.sources.values())
    has_read_only_connected = any(
        source.source_kind == "connected" and source.access == "read_only"
        for source in sources
    )
    has_managed_read_only = any(
        source.source_kind == "managed" and source.access == "read_only"
        for source in sources
    )

    if mode == "auto":
        return "analysis" if (has_read_only_connected or has_managed_read_only) else "full"
    if mode in _READ_ONLY_ANALYST_MODES:
        return "analysis"
    return "full"

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

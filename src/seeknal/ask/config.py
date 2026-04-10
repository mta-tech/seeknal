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


def load_agent_config(project_path: Path) -> dict[str, Any]:
    """Load ``seeknal_agent.yml`` from *project_path*, returning a dict.

    Returns an empty dict when the file does not exist.
    """
    config_path = project_path / _CONFIG_FILENAME
    if not config_path.exists():
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

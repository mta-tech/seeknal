"""Tests for seeknal.ask.config — agent config loader and locale helpers."""

import textwrap
from pathlib import Path

import pytest

from seeknal.ask.config import (
    find_agent_config_path,
    get_agent_profile_instructions,
    get_discovery_cache_ttl_seconds,
    get_locale_instructions,
    get_sql_timeout_seconds,
    load_agent_config,
)


# ---------------------------------------------------------------------------
# load_agent_config
# ---------------------------------------------------------------------------

def test_load_missing_file(tmp_path: Path):
    """Returns empty dict when seeknal_agent.yml does not exist."""
    assert load_agent_config(tmp_path) == {}


def test_load_valid_yaml(tmp_path: Path):
    (tmp_path / "seeknal_agent.yml").write_text(
        textwrap.dedent("""\
        model: gemini-3-flash-preview
        locale:
          currency: IDR
          currency_symbol: Rp
        """)
    )
    config = load_agent_config(tmp_path)
    assert config["model"] == "gemini-3-flash-preview"
    assert config["locale"]["currency"] == "IDR"


def test_load_nested_seeknal_yaml(tmp_path: Path):
    nested = tmp_path / "seeknal"
    nested.mkdir()
    (nested / "seeknal_agent.yml").write_text("agent:\n  name: Nested config\n")

    config = load_agent_config(tmp_path)
    assert config["agent"]["name"] == "Nested config"


def test_root_config_precedence_over_nested(tmp_path: Path):
    (tmp_path / "seeknal_agent.yml").write_text("agent:\n  name: Root config\n")
    nested = tmp_path / "seeknal"
    nested.mkdir()
    (nested / "seeknal_agent.yml").write_text("agent:\n  name: Nested config\n")

    assert find_agent_config_path(tmp_path) == tmp_path / "seeknal_agent.yml"
    assert load_agent_config(tmp_path)["agent"]["name"] == "Root config"


def test_load_empty_yaml(tmp_path: Path):
    """Empty YAML file returns empty dict (safe_load gives None)."""
    (tmp_path / "seeknal_agent.yml").write_text("")
    assert load_agent_config(tmp_path) == {}


# ---------------------------------------------------------------------------
# get_locale_instructions
# ---------------------------------------------------------------------------

def test_locale_none_when_missing():
    assert get_locale_instructions({}) is None
    assert get_locale_instructions({"locale": {}}) is None


def test_locale_currency_with_symbol():
    config = {"locale": {"currency": "IDR", "currency_symbol": "Rp"}}
    result = get_locale_instructions(config)
    assert result is not None
    assert "IDR" in result
    assert "Rp" in result
    assert "Never convert to USD" in result


def test_locale_currency_without_symbol():
    config = {"locale": {"currency": "EUR"}}
    result = get_locale_instructions(config)
    assert result is not None
    assert "EUR" in result
    assert "Never convert to USD" in result


def test_locale_number_format():
    config = {"locale": {"number_format": "id_ID"}}
    result = get_locale_instructions(config)
    assert "id_ID" in result


def test_locale_language():
    config = {"locale": {"language": "Indonesian"}}
    result = get_locale_instructions(config)
    assert "Indonesian" in result


def test_locale_timezone():
    config = {"locale": {"timezone": "Asia/Jakarta"}}
    result = get_locale_instructions(config)
    assert "Asia/Jakarta" in result


def test_locale_full_config():
    config = {
        "locale": {
            "currency": "IDR",
            "currency_symbol": "Rp",
            "language": "Indonesian",
            "number_format": "id_ID",
            "timezone": "Asia/Jakarta",
        }
    }
    result = get_locale_instructions(config)
    assert result is not None
    assert "## Locale & Formatting" in result
    assert "IDR" in result
    assert "Rp" in result
    assert "Indonesian" in result
    assert "id_ID" in result
    assert "Asia/Jakarta" in result


def test_agent_profile_none_when_missing():
    assert get_agent_profile_instructions({}) is None


def test_agent_profile_instructions_from_agent_section():
    config = {
        "agent": {
            "name": "CBN CI",
            "description": "Competitive analyst",
            "skills": ["competitive_intel"],
            "system_prompt": "Lead with the threat matrix.",
        }
    }
    result = get_agent_profile_instructions(config)
    assert result is not None
    assert "CBN CI" in result
    assert "Competitive analyst" in result
    assert "competitive_intel" in result
    assert "Lead with the threat matrix." in result


def test_performance_defaults_and_overrides():
    assert get_sql_timeout_seconds({}) == 60
    assert get_discovery_cache_ttl_seconds({}) == 300
    assert get_sql_timeout_seconds({"sql_timeout_seconds": 0}) == 0
    assert get_sql_timeout_seconds({"sql_timeout_seconds": "12"}) == 12
    assert get_discovery_cache_ttl_seconds(
        {"discovery_cache_ttl_seconds": "15"}
    ) == 15

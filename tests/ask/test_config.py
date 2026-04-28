"""Tests for seeknal.ask.config — agent config loader and locale helpers."""

import textwrap
from pathlib import Path

import pytest

from seeknal.ask.config import (
    find_agent_config_path,
    get_agent_profile_instructions,
    get_auto_summarization_config,
    get_cost_tracking_config,
    get_discovery_cache_ttl_seconds,
    get_hooks_config,
    get_locale_instructions,
    get_plan_config,
    get_stuck_loop_config,
    get_sql_timeout_seconds,
    get_subagents_config,
    get_teams_config,
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


def test_agent_harness_defaults_preserve_current_ask_behavior():
    auto = get_auto_summarization_config({})
    assert auto["enabled"] is True
    assert auto["context_manager"] == "auto"
    assert auto["eviction_token_limit"] == 20000
    assert auto["patch_tool_calls"] is True
    assert auto["microcompact"]["keep_recent_turns_analysis"] == 2
    assert auto["microcompact"]["keep_recent_turns_full"] == 3
    assert auto["sql_result_compactor"]["min_chars_analysis"] == 250
    assert auto["sql_result_compactor"]["min_chars_full"] == 500

    assert get_cost_tracking_config({}) == {"enabled": True, "budget_usd": None}
    assert get_hooks_config({}) == {
        "enabled": True,
        "sql_security": True,
        "sql_self_correction": True,
    }
    assert get_plan_config({}) == {"enabled": "auto", "plans_dir": ".seeknal/plans"}
    assert get_stuck_loop_config({}) == {"enabled": True}
    assert get_subagents_config({}) == {
        "enabled": "auto",
        "include_builtin": True,
        "lineage_investigator": True,
    }
    assert get_teams_config({}) == {"enabled": False}


def test_agent_harness_overrides_are_normalized():
    config = {
        "agent_harness": {
            "auto_summarization": {
                "enabled": "true",
                "context_manager": "false",
                "context_manager_max_tokens": "128000",
                "summarization_model": "openai:gpt-4.1-mini",
                "eviction_token_limit": "50000",
                "patch_tool_calls": "off",
                "microcompact": {
                    "enabled": "no",
                    "keep_recent_turns_analysis": "4",
                    "keep_recent_turns_full": "8",
                },
                "sql_result_compactor": {
                    "enabled": "yes",
                    "min_chars_analysis": "100",
                    "min_chars_full": "900",
                },
            },
            "cost_tracking": {"enabled": "on", "budget_usd": "1.25"},
            "hooks": {"enabled": True, "sql_security": False},
            "plan": {"enabled": False, "plans_dir": ".seeknal/custom-plans"},
            "stuck_loop_detection": {"enabled": "off"},
            "subagents": {"enabled": True, "include_builtin": False},
            "teams": {"enabled": True},
        }
    }

    auto = get_auto_summarization_config(config)
    assert auto["context_manager"] is False
    assert auto["context_manager_max_tokens"] == 128000
    assert auto["summarization_model"] == "openai:gpt-4.1-mini"
    assert auto["eviction_token_limit"] == 50000
    assert auto["patch_tool_calls"] is False
    assert auto["microcompact"]["enabled"] is False
    assert auto["microcompact"]["keep_recent_turns_full"] == 8
    assert auto["sql_result_compactor"]["min_chars_analysis"] == 100

    assert get_cost_tracking_config(config) == {"enabled": True, "budget_usd": 1.25}
    assert get_hooks_config(config)["sql_security"] is False
    assert get_hooks_config(config)["sql_self_correction"] is True
    assert get_plan_config(config) == {
        "enabled": False,
        "plans_dir": ".seeknal/custom-plans",
    }
    assert get_stuck_loop_config(config) == {"enabled": False}
    assert get_subagents_config(config)["include_builtin"] is False
    assert get_teams_config(config) == {"enabled": True}

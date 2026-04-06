"""Tests for seeknal.ask.config — agent config loader and locale helpers."""

import textwrap
from pathlib import Path

import pytest

from seeknal.ask.config import get_locale_instructions, load_agent_config


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

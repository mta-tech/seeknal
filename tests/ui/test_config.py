"""Tests for UI theme configuration via env vars and CLI flags."""

import os

import pytest
from typer.testing import CliRunner

from seeknal.cli.main import app
from seeknal.ui.console import reset

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clean_theme_env(monkeypatch):
    """Ensure SEEKNAL_THEME is cleared before and after each test."""
    monkeypatch.delenv("SEEKNAL_THEME", raising=False)
    reset()
    yield
    reset()


def test_seeknal_theme_env_var_changes_theme(monkeypatch):
    """Setting SEEKNAL_THEME env var should influence theme selection."""
    from seeknal.ui.console import _resolve_theme_name

    monkeypatch.setenv("SEEKNAL_THEME", "dark")
    reset()
    assert _resolve_theme_name() == "dark"

    monkeypatch.setenv("SEEKNAL_THEME", "light")
    reset()
    assert _resolve_theme_name() == "light"


def test_no_animation_flag_disables_animation():
    """--no-animation flag should set the internal animation-disabled flag."""
    from seeknal.ui import console

    # Baseline: animation is enabled
    reset()
    assert console._animation_disabled is False

    # Invoke a no-op via the CLI with --no-animation
    result = runner.invoke(app, ["--no-animation", "info"])
    # The flag callback should have fired regardless of command outcome
    assert console._animation_disabled is True

    # Clean up
    reset()
    assert console._animation_disabled is False


def test_theme_flag_sets_env_var():
    """--theme flag should set SEEKNAL_THEME env var."""
    result = runner.invoke(app, ["--theme", "light", "info"])
    assert os.environ.get("SEEKNAL_THEME") == "light"

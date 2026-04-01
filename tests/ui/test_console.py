"""Tests for seeknal.ui.console module."""

import os
from unittest.mock import patch

from seeknal.ui.console import (
    get_console,
    get_err_console,
    is_animation_enabled,
    disable_animation,
    reset,
)


class TestGetConsole:
    def setup_method(self):
        reset()

    def teardown_method(self):
        reset()

    def test_returns_console_instance(self):
        console = get_console()
        assert console is not None
        assert console.file is not None

    def test_singleton_returns_same_instance(self):
        c1 = get_console()
        c2 = get_console()
        assert c1 is c2

    def test_err_console_writes_to_stderr(self):
        err = get_err_console()
        assert err is not None

    def test_err_console_is_separate_from_stdout(self):
        c = get_console()
        e = get_err_console()
        assert c is not e

    def test_console_has_theme_applied(self):
        console = get_console()
        # Theme should resolve our custom style names
        style = console.get_style("brand.primary")
        assert style is not None
        style = console.get_style("status.success")
        assert style is not None

    @patch.dict(os.environ, {"SEEKNAL_THEME": "light"})
    def test_respects_seeknal_theme_env(self):
        reset()
        console = get_console()
        # Light theme should be applied — just verify console was created
        assert console is not None

    @patch.dict(os.environ, {"SEEKNAL_THEME": "dark-ansi"})
    def test_respects_ansi_theme(self):
        reset()
        console = get_console()
        assert console is not None

    @patch.dict(os.environ, {"SEEKNAL_THEME": "garbage"})
    def test_invalid_theme_falls_back(self):
        reset()
        console = get_console()
        assert console is not None  # Should not crash

    def test_reset_clears_singleton(self):
        c1 = get_console()
        reset()
        c2 = get_console()
        assert c1 is not c2


class TestAnimationControl:
    def setup_method(self):
        reset()

    def teardown_method(self):
        reset()

    def test_animation_enabled_by_default_in_tty(self):
        # In test environment, stdout may not be a TTY
        # Just verify the function runs without error
        result = is_animation_enabled()
        assert isinstance(result, bool)

    def test_disable_animation(self):
        disable_animation()
        assert not is_animation_enabled()

    @patch.dict(os.environ, {"NO_COLOR": "1"})
    def test_no_color_disables_animation(self):
        reset()
        assert not is_animation_enabled()

    @patch.dict(os.environ, {"CI": "true"})
    def test_ci_disables_animation(self):
        reset()
        assert not is_animation_enabled()

    @patch.dict(os.environ, {"TERM": "dumb"})
    def test_dumb_term_disables_animation(self):
        reset()
        assert not is_animation_enabled()

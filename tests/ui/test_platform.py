"""Tests for platform-aware glyph tier detection."""

import os
from unittest.mock import patch

import pytest

from seeknal.ui import figures


class TestGlyphTierDetection:
    """Tests for _detect_glyph_tier() platform / terminal heuristics."""

    def setup_method(self):
        figures.reset()

    def teardown_method(self):
        figures.reset()

    def test_dumb_term_returns_ascii(self):
        with patch.dict(os.environ, {"TERM": "dumb"}, clear=False):
            figures.reset()
            assert figures._detect_glyph_tier() == "ascii_only"

    def test_not_a_tty_returns_ascii(self):
        with patch("sys.stdout") as mock_stdout:
            mock_stdout.isatty.return_value = False
            figures.reset()
            assert figures._detect_glyph_tier() == "ascii_only"

    def test_apple_terminal_returns_unicode_safe(self):
        env = {"TERM_PROGRAM": "Apple_Terminal", "TERM": "xterm-256color"}
        with patch.dict(os.environ, env, clear=False):
            with patch("sys.stdout") as mock_stdout:
                mock_stdout.isatty.return_value = True
                with patch.object(figures.sys, "platform", "darwin"):
                    figures.reset()
                    assert figures._detect_glyph_tier() == "unicode_safe"

    def test_macos_non_apple_terminal_returns_unicode_full(self):
        env = {"TERM_PROGRAM": "iTerm.app", "TERM": "xterm-256color"}
        # Ensure WT_SESSION doesn't interfere
        cleaned = {k: v for k, v in os.environ.items() if k != "WT_SESSION"}
        with patch.dict(os.environ, {**cleaned, **env}, clear=True):
            with patch("sys.stdout") as mock_stdout:
                mock_stdout.isatty.return_value = True
                with patch.object(figures.sys, "platform", "darwin"):
                    figures.reset()
                    assert figures._detect_glyph_tier() == "unicode_full"

    def test_windows_terminal_returns_unicode_full(self):
        env = {"WT_SESSION": "some-guid", "TERM": "xterm"}
        with patch.dict(os.environ, env, clear=False):
            with patch("sys.stdout") as mock_stdout:
                mock_stdout.isatty.return_value = True
                with patch.object(figures.sys, "platform", "win32"):
                    figures.reset()
                    assert figures._detect_glyph_tier() == "unicode_full"

    def test_modern_terminal_ghostty(self):
        env = {"TERM_PROGRAM": "ghostty", "TERM": "xterm-256color"}
        with patch.dict(os.environ, env, clear=False):
            with patch("sys.stdout") as mock_stdout:
                mock_stdout.isatty.return_value = True
                figures.reset()
                assert figures._detect_glyph_tier() == "unicode_full"

    def test_modern_terminal_kitty(self):
        env = {"TERM_PROGRAM": "kitty", "TERM": "xterm-kitty"}
        with patch.dict(os.environ, env, clear=False):
            with patch("sys.stdout") as mock_stdout:
                mock_stdout.isatty.return_value = True
                figures.reset()
                assert figures._detect_glyph_tier() == "unicode_full"

    def test_unknown_linux_terminal_returns_unicode_safe(self):
        env = {"TERM_PROGRAM": "", "TERM": "xterm"}
        # Ensure WT_SESSION doesn't interfere
        cleaned = {k: v for k, v in os.environ.items() if k != "WT_SESSION"}
        with patch.dict(os.environ, {**cleaned, **env}, clear=True):
            with patch("sys.stdout") as mock_stdout:
                mock_stdout.isatty.return_value = True
                with patch.object(figures.sys, "platform", "linux"):
                    figures.reset()
                    assert figures._detect_glyph_tier() == "unicode_safe"

    def test_dumb_takes_priority_over_modern_terminal(self):
        """TERM=dumb should win even if TERM_PROGRAM is set."""
        env = {"TERM": "dumb", "TERM_PROGRAM": "ghostty"}
        with patch.dict(os.environ, env, clear=False):
            figures.reset()
            assert figures._detect_glyph_tier() == "ascii_only"


class TestGlyphRetrieval:
    """Tests for get() and get_tier() public API."""

    def setup_method(self):
        figures.reset()

    def teardown_method(self):
        figures.reset()

    def test_get_returns_correct_glyph_for_ascii_tier(self):
        with patch.dict(os.environ, {"TERM": "dumb"}, clear=False):
            figures.reset()
            assert figures.get("success") == "[OK]"
            assert figures.get("error") == "[ERR]"

    def test_get_unknown_key_returns_question_mark(self):
        figures.reset()
        assert figures.get("nonexistent_glyph") == "?"

    def test_get_tier_matches_detection(self):
        with patch.dict(os.environ, {"TERM": "dumb"}, clear=False):
            figures.reset()
            # Force load
            figures.get("success")
            assert figures.get_tier() == "ascii_only"

    def test_get_spinner_frames_returns_list(self):
        figures.reset()
        frames = figures.get_spinner_frames()
        assert isinstance(frames, list)
        assert len(frames) > 0
        assert all(isinstance(f, str) for f in frames)

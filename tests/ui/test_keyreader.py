"""Tests for KeyReader — raw terminal input layer."""

from unittest.mock import MagicMock, patch

import pytest


class TestKeyReader:
    """Test KeyReader context manager and key parsing."""

    def test_context_manager_restores_settings(self):
        """Terminal settings are restored even on normal exit."""
        import seeknal.ui.keyreader as kr_mod

        mock_old_settings = [1, 2, 3]

        with (
            patch.object(kr_mod.sys.stdin, "fileno", return_value=5),
            patch.object(kr_mod.termios, "tcgetattr", return_value=mock_old_settings),
            patch.object(kr_mod.termios, "tcsetattr") as mock_tcsetattr,
            patch.object(kr_mod.tty, "setcbreak"),
        ):
            reader = kr_mod.KeyReader()
            reader.__enter__()
            reader.__exit__(None, None, None)

            mock_tcsetattr.assert_called_once_with(
                5, kr_mod.termios.TCSADRAIN, mock_old_settings
            )

    def test_context_manager_restores_on_exception(self):
        """Terminal settings are restored even when an exception occurs."""
        import seeknal.ui.keyreader as kr_mod

        mock_old_settings = [1, 2, 3]

        with (
            patch.object(kr_mod.sys.stdin, "fileno", return_value=5),
            patch.object(kr_mod.termios, "tcgetattr", return_value=mock_old_settings),
            patch.object(kr_mod.termios, "tcsetattr") as mock_tcsetattr,
            patch.object(kr_mod.tty, "setcbreak"),
        ):
            reader = kr_mod.KeyReader()
            reader.__enter__()

            # Simulate exception during use
            try:
                raise ValueError("test error")
            except ValueError:
                reader.__exit__(ValueError, None, None)

            mock_tcsetattr.assert_called_once()

    def test_read_key_arrow_up(self):
        """Escape sequence \\x1b[A maps to 'up'."""
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch.object(reader, "_fd", 5), patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(side_effect=["\x1b", "[", "A"])
            assert reader.read_key() == "up"

    def test_read_key_arrow_down(self):
        """Escape sequence \\x1b[B maps to 'down'."""
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(side_effect=["\x1b", "[", "B"])
            assert reader.read_key() == "down"

    def test_read_key_arrow_left(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(side_effect=["\x1b", "[", "D"])
            assert reader.read_key() == "left"

    def test_read_key_arrow_right(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(side_effect=["\x1b", "[", "C"])
            assert reader.read_key() == "right"

    def test_read_key_shift_tab(self):
        """Escape sequence \\x1b[Z maps to 'shift-tab'."""
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(side_effect=["\x1b", "[", "Z"])
            assert reader.read_key() == "shift-tab"

    def test_read_key_escape_alone(self):
        """Bare escape (no bracket following) maps to 'escape'."""
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(side_effect=["\x1b", "x"])
            assert reader.read_key() == "escape"

    def test_read_key_enter_cr(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value="\r")
            assert reader.read_key() == "enter"

    def test_read_key_enter_lf(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value="\n")
            assert reader.read_key() == "enter"

    def test_read_key_space(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value=" ")
            assert reader.read_key() == "space"

    def test_read_key_tab(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value="\t")
            assert reader.read_key() == "tab"

    def test_read_key_backspace_0x7f(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value="\x7f")
            assert reader.read_key() == "backspace"

    def test_read_key_backspace_0x08(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value="\x08")
            assert reader.read_key() == "backspace"

    def test_read_key_regular_char(self):
        """Regular printable characters pass through as-is."""
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value="a")
            assert reader.read_key() == "a"

    def test_read_key_digit(self):
        from seeknal.ui.keyreader import KeyReader

        reader = KeyReader()
        with patch("sys.stdin") as mock_stdin:
            mock_stdin.read = MagicMock(return_value="5")
            assert reader.read_key() == "5"

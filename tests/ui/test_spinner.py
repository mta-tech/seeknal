"""Tests for seeknal.ui.spinner module."""

import os
import time
from unittest.mock import patch, MagicMock

from rich._spinners import SPINNERS

from seeknal.ui import console as console_mod
from seeknal.ui import figures as figures_mod
from seeknal.ui.spinner import seeknal_status, StalledSpinner


class TestSpinnerRegistration:
    """Verify the custom spinner is registered in Rich's SPINNERS dict."""

    def test_seeknal_spinner_registered(self):
        assert "seeknal" in SPINNERS

    def test_spinner_has_interval(self):
        assert SPINNERS["seeknal"]["interval"] == 120

    def test_spinner_has_frames(self):
        frames = SPINNERS["seeknal"]["frames"]
        assert isinstance(frames, list)
        assert len(frames) >= 4


class TestSeeknalStatus:
    """Tests for the seeknal_status context manager."""

    def setup_method(self):
        console_mod.reset()

    def teardown_method(self):
        console_mod.reset()

    def test_context_manager_runs_body(self):
        """The body inside the context manager executes."""
        executed = False
        console_mod.disable_animation()
        with seeknal_status("Working..."):
            executed = True
        assert executed

    def test_animation_disabled_prints_message(self, capsys):
        """When animation is off, the message is printed statically."""
        console_mod.disable_animation()
        console = MagicMock()
        with patch("seeknal.ui.spinner.get_console", return_value=console):
            with patch("seeknal.ui.spinner.is_animation_enabled", return_value=False):
                with seeknal_status("Loading..."):
                    pass
        console.print.assert_called_once_with("Loading...")

    def test_animation_enabled_uses_status(self):
        """When animation is on, Console.status() is used."""
        console = MagicMock()
        status_ctx = MagicMock()
        console.status.return_value = status_ctx
        status_ctx.__enter__ = MagicMock(return_value=status_ctx)
        status_ctx.__exit__ = MagicMock(return_value=False)

        with patch("seeknal.ui.spinner.get_console", return_value=console):
            with patch("seeknal.ui.spinner.is_animation_enabled", return_value=True):
                with seeknal_status("Running..."):
                    pass

        console.status.assert_called_once_with(
            "Running...", spinner="seeknal", spinner_style="spinner.active"
        )


class TestStalledSpinner:
    """Tests for the StalledSpinner class."""

    def setup_method(self):
        console_mod.reset()

    def teardown_method(self):
        console_mod.reset()

    def test_context_manager_enters_and_exits(self):
        """StalledSpinner works as a context manager without errors."""
        console_mod.disable_animation()
        with StalledSpinner("Working...") as s:
            assert s is not None

    def test_elapsed_before_enter_is_zero(self):
        s = StalledSpinner("test")
        assert s.elapsed == 0.0

    def test_elapsed_increases_after_enter(self):
        console_mod.disable_animation()
        with StalledSpinner("test") as s:
            assert s.elapsed >= 0.0

    def test_is_stalled_false_initially(self):
        console_mod.disable_animation()
        with StalledSpinner("test", stall_threshold=30.0) as s:
            assert not s.is_stalled

    def test_stall_threshold_triggers_style_change(self):
        """After exceeding stall_threshold, style changes to spinner.stalled."""
        console_mod.disable_animation()
        s = StalledSpinner("test", stall_threshold=0.0)

        # Manually set start_time in the past to simulate elapsed time
        s._start_time = time.monotonic() - 1.0

        assert s.is_stalled
        # Trigger the check
        s.update("still working...")
        assert s._stalled is True

    def test_update_changes_message(self):
        console_mod.disable_animation()
        s = StalledSpinner("initial")
        s._start_time = time.monotonic()
        s.update("updated")
        assert s._message == "updated"

    def test_stall_only_transitions_once(self):
        """Once stalled, _check_stalled is a no-op."""
        console_mod.disable_animation()
        s = StalledSpinner("test", stall_threshold=0.0)
        s._start_time = time.monotonic() - 1.0
        s.update("msg1")
        assert s._stalled is True
        # Should not error on second call
        s.update("msg2")
        assert s._stalled is True


class TestSpinnerFramesByTier:
    """Verify frame sets match the glyph tier."""

    def setup_method(self):
        figures_mod.reset()

    def teardown_method(self):
        figures_mod.reset()

    @patch.dict(os.environ, {"TERM": "dumb"}, clear=False)
    def test_ascii_tier_frames(self):
        figures_mod.reset()
        frames = figures_mod.get_spinner_frames()
        assert frames == ["-", "\\", "|", "/"]

    def test_frames_are_list_of_strings(self):
        frames = figures_mod.get_spinner_frames()
        assert isinstance(frames, list)
        assert all(isinstance(f, str) for f in frames)

"""
Custom spinner system for seeknal CLI.

Registers a platform-aware "seeknal" spinner into Rich's spinner registry
and provides convenience wrappers for animated status display.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

from rich._spinners import SPINNERS
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text

from seeknal.ui.console import get_console, is_animation_enabled
from seeknal.ui.figures import get_spinner_frames, get_tier

# =============================================================================
# Register custom spinner at import time
# =============================================================================

_INTERVAL_MS = 120

SPINNERS["seeknal"] = {
    "interval": _INTERVAL_MS,
    "frames": get_spinner_frames(),
}


# =============================================================================
# seeknal_status context manager
# =============================================================================


@contextmanager
def seeknal_status(message: str) -> Iterator[None]:
    """Display an animated spinner with *message* while the body executes.

    When animations are disabled (CI, dumb terminal, ``--no-animation``),
    the message is printed once without any spinner.

    Usage::

        with seeknal_status("Loading data..."):
            do_work()
    """
    console = get_console()

    if not is_animation_enabled():
        console.print(message)
        yield
        return

    with console.status(message, spinner="seeknal", spinner_style="spinner.active"):
        yield


# =============================================================================
# StalledSpinner — spinner with stalled-state detection
# =============================================================================


class StalledSpinner:
    """A spinner that changes style after a stall threshold.

    After *stall_threshold* seconds of elapsed time the spinner switches
    from ``spinner.active`` to ``spinner.stalled`` to visually signal that
    an operation is taking longer than expected.

    Parameters
    ----------
    message:
        Initial status message.
    stall_threshold:
        Seconds before switching to the stalled style.  Default 30.
    """

    def __init__(self, message: str, stall_threshold: float = 30.0) -> None:
        self._message = message
        self._stall_threshold = stall_threshold
        self._start_time: float = 0.0
        self._stalled = False
        self._console = get_console()
        self._spinner = Spinner("seeknal", text=message, style="spinner.active")
        self._live: Live | None = None

    # -- context manager ------------------------------------------------------

    def __enter__(self) -> StalledSpinner:
        self._start_time = time.monotonic()
        if not is_animation_enabled():
            self._console.print(self._message)
            return self
        self._live = Live(
            self._spinner,
            console=self._console,
            refresh_per_second=1000 / _INTERVAL_MS,
            transient=True,
        )
        self._live.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        if self._live is not None:
            self._live.__exit__(exc_type, exc_val, exc_tb)
            self._live = None

    # -- public API -----------------------------------------------------------

    @property
    def elapsed(self) -> float:
        """Seconds elapsed since the spinner was entered."""
        if self._start_time == 0.0:
            return 0.0
        return time.monotonic() - self._start_time

    @property
    def is_stalled(self) -> bool:
        """Whether the spinner has exceeded the stall threshold."""
        return self.elapsed >= self._stall_threshold

    def update(self, message: str) -> None:
        """Update the displayed message and check for stall transition."""
        self._message = message
        self._check_stalled()
        if self._live is not None:
            self._spinner.update(text=message)

    # -- internals ------------------------------------------------------------

    def _check_stalled(self) -> None:
        if self._stalled:
            return
        if self.is_stalled:
            self._stalled = True
            if self._live is not None:
                self._spinner.update(style="spinner.stalled")

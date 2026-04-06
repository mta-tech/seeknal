"""
Claude Code-style spinner for seeknal ask.

Shows animated glyph + message + elapsed time + token count:
  ✶ Thinking… (12s · ↓ 1.4k tokens)

Also provides subagent display:
  ⏺ Delegating to lineage_investigator…
    ⎿ Tracing data flow through pipeline definitions
"""

from __future__ import annotations

import threading
import time

from rich.live import Live
from rich.text import Text

from seeknal.ui.console import get_console, is_animation_enabled
from seeknal.ui.figures import get, get_spinner_frames

# Spinner glyph frames (seeknal brand set)
_FRAMES = get_spinner_frames()
_FRAME_INTERVAL = 0.12  # seconds per frame


def _format_elapsed(seconds: float) -> str:
    """Format elapsed time: '5s', '1m 23s'."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    return f"{m}m {s}s"


def _format_tokens(n: int) -> str:
    """Format token count: '450', '1.4k', '12.3k'."""
    if n < 1000:
        return str(n)
    return f"{n / 1000:.1f}k"


class AskSpinner:
    """Animated spinner with elapsed time and token counter.

    Usage::

        spinner = AskSpinner("Thinking")
        spinner.start()
        # ... do work, call spinner.add_tokens(n) as data arrives ...
        spinner.stop()
    """

    def __init__(self, message: str = "Thinking") -> None:
        self._message = message
        self._start = 0.0
        self._tokens = 0
        self._tool_uses = 0
        self._live: Live | None = None
        self._active = False
        self._console = get_console()
        self._lock = threading.Lock()

    # -- Public API -----------------------------------------------------------

    def start(self) -> None:
        """Start the animated spinner."""
        if self._active:
            return
        self._active = True
        if self._start == 0.0:
            self._start = time.monotonic()

        if not self._console.is_terminal or not is_animation_enabled():
            return

        self._live = Live(
            self._render(),
            console=self._console,
            refresh_per_second=10,
            transient=True,
        )
        self._live.start()
        # Kick off a refresh thread so the timer updates
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop, daemon=True
        )
        self._refresh_thread.start()

    def stop(self) -> None:
        """Stop the spinner and clear display."""
        if not self._active:
            return
        self._active = False
        if self._live is not None:
            try:
                self._live.stop()
            except Exception:
                pass
            self._live = None

    def update_message(self, msg: str) -> None:
        """Change the displayed message (e.g., 'Processing results')."""
        with self._lock:
            self._message = msg

    def add_tokens(self, n: int) -> None:
        """Add to the cumulative token count."""
        with self._lock:
            self._tokens += n

    def set_tokens(self, n: int) -> None:
        """Set the absolute token count (e.g. from usage API)."""
        with self._lock:
            self._tokens = n

    def increment_tool_uses(self) -> None:
        """Increment tool use counter."""
        with self._lock:
            self._tool_uses += 1

    @property
    def elapsed(self) -> float:
        if self._start == 0.0:
            return 0.0
        return time.monotonic() - self._start

    @property
    def tokens(self) -> int:
        return self._tokens

    @property
    def tool_uses(self) -> int:
        return self._tool_uses

    # -- Rendering ------------------------------------------------------------

    def _render(self) -> Text:
        """Build the spinner line as Rich Text."""
        with self._lock:
            msg = self._message
            tokens = self._tokens
            tool_uses = self._tool_uses

        elapsed = self.elapsed

        # Animated glyph
        frame_idx = int(elapsed / _FRAME_INTERVAL) % len(_FRAMES)
        glyph = _FRAMES[frame_idx]

        text = Text()
        text.append(f"{glyph} ", style="brand.primary")
        text.append(f"{msg}… ", style="brand.primary")

        # Stats suffix: (12s · ↓ 1.4k tokens)
        parts = []
        if elapsed >= 1.0:
            parts.append(_format_elapsed(elapsed))
        if tokens > 0:
            parts.append(f"↓ {_format_tokens(tokens)} tokens")
        if parts:
            text.append("(", style="text.dim")
            text.append(" · ".join(parts), style="text.dim")
            text.append(")", style="text.dim")

        return text

    def _refresh_loop(self) -> None:
        """Background thread to update the Live display."""
        while self._active and self._live is not None:
            try:
                self._live.update(self._render())
            except Exception:
                break
            time.sleep(0.1)

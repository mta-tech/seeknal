"""Live catalog refresher for Seeknal Serving."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from seeknal.serving.discovery.build import DiscoveryResult
from seeknal.serving.placement import Placement

logger = logging.getLogger("seeknal.serving.discovery")


class PlacementRefresher:
    """Periodically discovers catalog updates and swaps the serving pipeline catalog."""

    def __init__(
        self,
        discover_fn: Callable[[], DiscoveryResult],
        pipeline: Any,
        *,
        interval_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.discover_fn = discover_fn
        self.pipeline = pipeline
        self.interval_seconds = float(interval_seconds)
        self.clock = clock

        self._lock = threading.Lock()
        self._last_success_at: float | None = None
        self._last_error: str | None = None
        self._consecutive_failures: int = 0

        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    @property
    def last_success_at(self) -> float | None:
        """Timestamp of last successful refresh (via clock)."""
        with self._lock:
            return self._last_success_at

    @property
    def last_error(self) -> str | None:
        """Last error formatted as '<ErrorClass>', or None."""
        with self._lock:
            return self._last_error

    @property
    def consecutive_failures(self) -> int:
        """Number of consecutive failed refresh attempts."""
        with self._lock:
            return self._consecutive_failures

    def refresh_once(self) -> bool:
        """Run one discovery iteration and update the serving pipeline catalog."""
        try:
            result = self.discover_fn()
            new_placement = Placement.from_dict(result.placement)
            self.pipeline.update_catalog(new_placement, result.schemas)
            with self._lock:
                self._last_success_at = self.clock()
                self._last_error = None
                self._consecutive_failures = 0
            return True
        except Exception as exc:
            exc_class = type(exc).__name__
            logger.warning("Catalog refresh failed: %s", exc_class)
            with self._lock:
                self._last_error = exc_class
                self._consecutive_failures += 1
            return False

    def _run(self) -> None:
        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=self.interval_seconds):
                break
            self.refresh_once()

    def start(self) -> None:
        """Start the background daemon thread to refresh periodically."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="seeknal-placement-refresher",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        """Signal the background thread to stop and wait for completion."""
        self._stop_event.set()
        with self._lock:
            thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0 * self.interval_seconds)

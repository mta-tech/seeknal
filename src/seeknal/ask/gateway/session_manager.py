"""WebSocket connection tracking for the seeknal gateway.

Tracks which WebSocket connections are associated with which session IDs.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any


class SessionManager:
    """Track active WebSocket connections per session."""

    def __init__(self) -> None:
        self._connections: dict[str, set[Any]] = defaultdict(set)
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def connect(self, session_id: str, websocket: Any) -> None:
        async with self._get_lock():
            self._connections[session_id].add(websocket)

    async def disconnect(self, session_id: str, websocket: Any) -> None:
        async with self._get_lock():
            self._connections[session_id].discard(websocket)
            if not self._connections[session_id]:
                del self._connections[session_id]

    async def broadcast(self, session_id: str, message: str) -> None:
        """Send a message to all WebSocket connections for a session."""
        async with self._get_lock():
            connections = list(self._connections.get(session_id, set()))
        for ws in connections:
            try:
                await ws.send_text(message)
            except Exception:
                pass

    @property
    def active_sessions(self) -> list[str]:
        return list(self._connections.keys())

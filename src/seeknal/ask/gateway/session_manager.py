"""Session manager for seeknal ask gateway.

Tracks active WebSocket connections per session_id, supporting
multiple concurrent connections to the same session (e.g. multiple
browser tabs). Provides broadcast to all connections in a session.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class SessionManager:
    """Tracks active WebSocket connections grouped by session_id.

    Thread-safe: all mutation is done from the asyncio event loop.
    """

    def __init__(self) -> None:
        # session_id -> set of websocket connections
        self._connections: dict[str, set[Any]] = {}

    def connect(self, session_id: str, websocket: Any) -> None:
        """Register a WebSocket connection for a session."""
        if session_id not in self._connections:
            self._connections[session_id] = set()
        self._connections[session_id].add(websocket)
        logger.info("WS connected: session=%s total=%d", session_id,
                     len(self._connections[session_id]))

    def disconnect(self, session_id: str, websocket: Any) -> None:
        """Remove a WebSocket connection from a session."""
        conns = self._connections.get(session_id)
        if conns is None:
            return
        conns.discard(websocket)
        if not conns:
            del self._connections[session_id]
        logger.info("WS disconnected: session=%s remaining=%d", session_id,
                     len(self._connections.get(session_id, set())))

    async def send_to_session(self, session_id: str, data: dict) -> None:
        """Broadcast a JSON message to all connections in a session.

        Silently skips connections that have already closed.
        """
        conns = self._connections.get(session_id)
        if not conns:
            return
        tasks = []
        for ws in list(conns):
            tasks.append(self._safe_send(ws, data))
        await asyncio.gather(*tasks)

    @staticmethod
    async def _safe_send(websocket: Any, data: dict) -> None:
        """Send JSON to a websocket, ignoring closed-connection errors."""
        try:
            await websocket.send_json(data)
        except Exception:
            logger.debug("Failed to send to websocket (likely closed)")

    def active_sessions(self) -> list[str]:
        """Return list of session_ids with at least one connection."""
        return list(self._connections.keys())

    def connection_count(self, session_id: str) -> int:
        """Return number of active connections for a session."""
        return len(self._connections.get(session_id, set()))

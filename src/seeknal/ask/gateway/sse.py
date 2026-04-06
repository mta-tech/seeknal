"""Server-Sent Events (SSE) pub/sub broadcaster for seeknal gateway.

Provides per-session event queues that SSE clients can subscribe to.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict


class SSEBroadcaster:
    """In-memory pub/sub for SSE event distribution per session."""

    def __init__(self, maxsize: int = 100) -> None:
        self._subscribers: dict[str, list[asyncio.Queue]] = defaultdict(list)
        self._maxsize = maxsize

    def subscribe(self, session_id: str) -> asyncio.Queue:
        """Create and return a new subscriber queue for a session."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        self._subscribers[session_id].append(queue)
        return queue

    def unsubscribe(self, session_id: str, queue: asyncio.Queue) -> None:
        """Remove a subscriber queue."""
        subs = self._subscribers.get(session_id, [])
        if queue in subs:
            subs.remove(queue)
        if not subs and session_id in self._subscribers:
            del self._subscribers[session_id]

    async def publish(self, session_id: str, event: str) -> None:
        """Publish an event to all subscribers for a session."""
        for queue in self._subscribers.get(session_id, []):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop events for slow consumers

    def publish_sync(self, session_id: str, event: str) -> None:
        """Publish an event synchronously (uses put_nowait only).

        Safe to call from sync code within an async context since
        ``put_nowait`` never blocks. Cost is negligible when no
        subscribers exist (~100ns dict lookup returning empty list).
        """
        for queue in self._subscribers.get(session_id, []):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop events for slow consumers

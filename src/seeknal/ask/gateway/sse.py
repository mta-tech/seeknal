"""Server-Sent Events (SSE) pub/sub broadcaster for seeknal gateway.

Provides per-session event queues that SSE clients can subscribe to.
Multi-tenant: subscriptions are keyed on ``(tenant_id, session_id)``
so the same session_id across tenants is fully isolated.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict

from seeknal.ask.gateway.tenant import DEFAULT_TENANT, scoped_key


class SSEBroadcaster:
    """In-memory pub/sub for SSE event distribution per tenant + session."""

    def __init__(self, maxsize: int = 100) -> None:
        # Keys are composite ``"{tenant_id}:{session_id}"`` strings so
        # same session_id across tenants doesn't collide.
        self._subscribers: dict[str, list[asyncio.Queue]] = defaultdict(list)
        self._maxsize = maxsize

    def subscribe(
        self, session_id: str, tenant_id: str = DEFAULT_TENANT
    ) -> asyncio.Queue:
        """Create and return a new subscriber queue for a tenant's session."""
        key = scoped_key(tenant_id, session_id)
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        self._subscribers[key].append(queue)
        return queue

    def unsubscribe(
        self,
        session_id: str,
        queue: asyncio.Queue,
        tenant_id: str = DEFAULT_TENANT,
    ) -> None:
        """Remove a subscriber queue."""
        key = scoped_key(tenant_id, session_id)
        subs = self._subscribers.get(key, [])
        if queue in subs:
            subs.remove(queue)
        if not subs and key in self._subscribers:
            del self._subscribers[key]

    async def publish(
        self, session_id: str, event: str, tenant_id: str = DEFAULT_TENANT
    ) -> None:
        """Publish an event to all subscribers for a tenant's session."""
        key = scoped_key(tenant_id, session_id)
        for queue in self._subscribers.get(key, []):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop events for slow consumers

    def publish_sync(
        self, session_id: str, event: str, tenant_id: str = DEFAULT_TENANT
    ) -> None:
        """Publish an event synchronously (uses put_nowait only).

        Safe to call from sync code within an async context since
        ``put_nowait`` never blocks. Cost is negligible when no
        subscribers exist (~100ns dict lookup returning empty list).
        """
        key = scoped_key(tenant_id, session_id)
        for queue in self._subscribers.get(key, []):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop events for slow consumers

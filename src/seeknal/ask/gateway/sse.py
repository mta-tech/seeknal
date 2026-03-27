"""SSE broadcaster for seeknal ask gateway.

Provides an in-memory pub/sub layer between event producers (Redis
subscriber, in-process agent) and SSE client connections. Each session
can have multiple subscribers (browser tabs), each backed by a bounded
asyncio.Queue.

Architecture:
- ``SSEBroadcaster``: per-session queue management
- ``redis_subscriber``: background coroutine that bridges Redis pub/sub
  to the broadcaster
- SSE endpoint in server.py creates an async generator that reads from
  the subscriber queue
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SSEBroadcaster:
    """In-memory broadcaster for Server-Sent Events.

    Maintains per-session sets of bounded asyncio.Queues. Producers call
    ``publish()`` to fan out events to all subscribers of a session.
    Slow subscribers that have a full queue silently drop events.
    """

    def __init__(self) -> None:
        # session_id -> set of asyncio.Queue
        self._subscribers: dict[str, set[asyncio.Queue]] = {}

    def subscribe(self, session_id: str) -> asyncio.Queue:
        """Create and return a bounded queue for a new subscriber.

        Args:
            session_id: The session to subscribe to.

        Returns:
            An asyncio.Queue (maxsize=100) that will receive events.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        if session_id not in self._subscribers:
            self._subscribers[session_id] = set()
        self._subscribers[session_id].add(queue)
        logger.debug(
            "SSE subscriber added: session=%s total=%d",
            session_id,
            len(self._subscribers[session_id]),
        )
        return queue

    def unsubscribe(self, session_id: str, queue: asyncio.Queue) -> None:
        """Remove a subscriber queue from a session.

        Args:
            session_id: The session to unsubscribe from.
            queue: The queue to remove.
        """
        subs = self._subscribers.get(session_id)
        if subs is None:
            return
        subs.discard(queue)
        if not subs:
            del self._subscribers[session_id]
        logger.debug(
            "SSE subscriber removed: session=%s remaining=%d",
            session_id,
            len(self._subscribers.get(session_id, set())),
        )

    def publish(self, session_id: str, event: dict) -> None:
        """Fan out an event to all subscribers of a session.

        Uses ``put_nowait`` so a full queue drops the event rather than
        blocking the publisher.

        Args:
            session_id: Target session.
            event: JSON-serializable event dict.
        """
        subs = self._subscribers.get(session_id)
        if not subs:
            return
        for queue in list(subs):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.debug(
                    "SSE queue full, dropping event for session=%s", session_id
                )

    def subscriber_count(self, session_id: str) -> int:
        """Return the number of active subscribers for a session."""
        return len(self._subscribers.get(session_id, set()))


async def redis_subscriber(
    broadcaster: SSEBroadcaster,
    session_id: str,
    redis_url: str = "redis://localhost:6379",
) -> None:
    """Subscribe to a Redis pub/sub channel and push events to the broadcaster.

    This is a long-running coroutine meant to be spawned as a background
    task. It subscribes to ``session:{session_id}`` and pushes each
    message to the broadcaster.

    Args:
        broadcaster: The SSEBroadcaster to publish to.
        session_id: Session whose Redis channel to subscribe to.
        redis_url: Redis connection URL.
    """
    import redis.asyncio as aioredis

    channel_name = f"session:{session_id}"
    client = aioredis.from_url(redis_url)
    pubsub = client.pubsub()

    try:
        await pubsub.subscribe(channel_name)
        logger.info("Redis subscriber started: channel=%s", channel_name)

        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                data = json.loads(message["data"])
                broadcaster.publish(session_id, data)
            except (json.JSONDecodeError, TypeError):
                logger.debug("Invalid JSON from Redis channel %s", channel_name)
    except asyncio.CancelledError:
        logger.info("Redis subscriber cancelled: channel=%s", channel_name)
    except Exception:
        logger.exception("Redis subscriber error: channel=%s", channel_name)
    finally:
        await pubsub.unsubscribe(channel_name)
        await pubsub.close()
        await client.close()

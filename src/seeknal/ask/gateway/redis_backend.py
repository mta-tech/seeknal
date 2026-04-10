"""Redis-backed state layer for multi-replica seeknal gateway.

Provides Redis-backed implementations of session storage, SSE pub/sub
broadcasting, and distributed session locks — replacing the in-process
singletons in server.py for horizontal scaling.

Follows the same interface as the local components (SSEBroadcaster,
SessionStore) so the gateway can switch between local and Redis mode
at startup via create_gateway_app(redis_url=...).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Protocol

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Broadcaster Protocol — shared interface for local and Redis SSE
# ---------------------------------------------------------------------------


class Broadcaster(Protocol):
    """Protocol for SSE event broadcasting (local or Redis-backed)."""

    async def publish(self, session_id: str, event: str) -> None: ...
    def subscribe(self, session_id: str) -> asyncio.Queue: ...
    def unsubscribe(self, session_id: str, queue: asyncio.Queue) -> None: ...


# ---------------------------------------------------------------------------
# Redis SSE Broadcaster
# ---------------------------------------------------------------------------


class RedisSSEBroadcaster:
    """Redis pub/sub based SSE event broadcaster.

    Publishes events to a Redis channel per session. Each gateway replica
    subscribes to active session channels and relays events to locally
    connected SSE clients.
    """

    CHANNEL_PREFIX = "seeknal:sse:"

    def __init__(self, redis: Redis, maxsize: int = 100) -> None:
        self._redis = redis
        self._maxsize = maxsize
        self._local_subscribers: dict[str, list[asyncio.Queue]] = {}
        self._pubsub = redis.pubsub()
        self._listener_task: asyncio.Task | None = None

    def subscribe(self, session_id: str) -> asyncio.Queue:
        """Create a local subscriber queue and subscribe to Redis channel."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        if session_id not in self._local_subscribers:
            self._local_subscribers[session_id] = []
            # Subscribe to Redis channel in background
            asyncio.ensure_future(
                self._pubsub.subscribe(f"{self.CHANNEL_PREFIX}{session_id}")
            )
        self._local_subscribers[session_id].append(queue)
        return queue

    def unsubscribe(self, session_id: str, queue: asyncio.Queue) -> None:
        """Remove a local subscriber queue."""
        subs = self._local_subscribers.get(session_id, [])
        if queue in subs:
            subs.remove(queue)
        if not subs and session_id in self._local_subscribers:
            del self._local_subscribers[session_id]
            asyncio.ensure_future(
                self._pubsub.unsubscribe(f"{self.CHANNEL_PREFIX}{session_id}")
            )

    async def publish(self, session_id: str, event: str) -> None:
        """Publish an event to Redis channel (fans out to all replicas)."""
        try:
            await self._redis.publish(f"{self.CHANNEL_PREFIX}{session_id}", event)
        except Exception:
            logger.warning("Redis publish failed for session %s", session_id)

    async def start_listener(self) -> None:
        """Start background task that relays Redis messages to local queues."""
        self._listener_task = asyncio.create_task(self._listen())

    async def stop_listener(self) -> None:
        """Stop the background listener."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        await self._pubsub.close()

    async def _listen(self) -> None:
        """Background loop: read from Redis pubsub and dispatch to local queues."""
        try:
            async for message in self._pubsub.listen():
                if message["type"] != "message":
                    continue
                channel = message["channel"]
                if isinstance(channel, bytes):
                    channel = channel.decode()
                session_id = channel.removeprefix(self.CHANNEL_PREFIX)
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()

                for queue in self._local_subscribers.get(session_id, []):
                    try:
                        queue.put_nowait(data)
                    except asyncio.QueueFull:
                        pass  # Drop events for slow consumers
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Redis SSE listener error")


# ---------------------------------------------------------------------------
# Redis Session Store
# ---------------------------------------------------------------------------


class RedisSessionStore:
    """Redis-backed session storage.

    Replaces the filesystem-based SessionStore for multi-replica deployments.
    Session metadata and messages are stored as Redis keys with optional TTL.
    """

    META_PREFIX = "seeknal:session:"
    MSG_PREFIX = "seeknal:messages:"
    INDEX_KEY = "seeknal:sessions"
    DEFAULT_TTL = 86400 * 7  # 7 days

    def __init__(self, redis: Redis, ttl: int | None = None) -> None:
        self._redis = redis
        self._ttl = ttl or self.DEFAULT_TTL

    async def create(self, name: str) -> str:
        """Create a new session."""
        now = datetime.now(timezone.utc).isoformat()
        metadata = {
            "name": name,
            "status": "active",
            "created_at": now,
            "updated_at": now,
            "message_count": 0,
            "last_question": None,
        }
        key = f"{self.META_PREFIX}{name}"
        await self._redis.set(key, json.dumps(metadata), ex=self._ttl)
        await self._redis.sadd(self.INDEX_KEY, name)
        return name

    async def get(self, name: str) -> dict | None:
        """Fetch session metadata."""
        data = await self._redis.get(f"{self.META_PREFIX}{name}")
        if data is None:
            return None
        return json.loads(data)

    async def list(self) -> list[dict]:
        """List all sessions."""
        names = await self._redis.smembers(self.INDEX_KEY)
        sessions = []
        for name in names:
            if isinstance(name, bytes):
                name = name.decode()
            meta = await self.get(name)
            if meta:
                sessions.append(meta)
        sessions.sort(key=lambda s: s.get("updated_at", ""), reverse=True)
        return sessions

    async def update(self, name: str, **kwargs: Any) -> None:
        """Update session metadata fields."""
        meta = await self.get(name)
        if meta is None:
            return
        allowed = {"status", "message_count", "last_question", "updated_at"}
        for k, v in kwargs.items():
            if k in allowed:
                meta[k] = v
        if "updated_at" not in kwargs:
            meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        key = f"{self.META_PREFIX}{name}"
        await self._redis.set(key, json.dumps(meta), ex=self._ttl)

    async def delete(self, name: str) -> bool:
        """Delete a session."""
        key = f"{self.META_PREFIX}{name}"
        msg_key = f"{self.MSG_PREFIX}{name}"
        result = await self._redis.delete(key, msg_key)
        await self._redis.srem(self.INDEX_KEY, name)
        return result > 0

    async def save_messages(self, name: str, messages_json: bytes) -> None:
        """Save serialized conversation history."""
        key = f"{self.MSG_PREFIX}{name}"
        await self._redis.set(key, messages_json, ex=self._ttl)

    async def load_messages(self, name: str) -> bytes | None:
        """Load serialized conversation history."""
        return await self._redis.get(f"{self.MSG_PREFIX}{name}")


# ---------------------------------------------------------------------------
# Redis Distributed Lock
# ---------------------------------------------------------------------------


class RedisSessionLock:
    """Redis-based distributed session lock.

    Replaces the per-process asyncio.Lock dict for multi-replica
    session serialization. Uses Redis SETNX with auto-expiry.
    """

    LOCK_PREFIX = "seeknal:lock:"
    DEFAULT_TIMEOUT = 300  # 5 minutes

    def __init__(self, redis: Redis, timeout: int | None = None) -> None:
        self._redis = redis
        self._timeout = timeout or self.DEFAULT_TIMEOUT

    async def acquire(self, session_id: str) -> Any:
        """Acquire a distributed lock for a session. Returns the lock object."""
        lock = self._redis.lock(
            f"{self.LOCK_PREFIX}{session_id}",
            timeout=self._timeout,
            blocking_timeout=self._timeout,
        )
        await lock.acquire()
        return lock

    async def release(self, lock: Any) -> None:
        """Release a distributed lock."""
        try:
            await lock.release()
        except Exception:
            logger.warning("Failed to release Redis lock")

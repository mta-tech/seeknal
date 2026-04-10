"""Redis-backed state layer for multi-replica seeknal gateway.

Provides Redis-backed implementations of session storage, SSE pub/sub
broadcasting, and distributed session locks — replacing the in-process
singletons in server.py for horizontal scaling.

Multi-tenant: all keys and channels are tenant-prefixed so two tenants
using the same ``session_id`` never collide. Default tenant uses legacy
unprefixed keys for backward compatibility.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Protocol

from redis.asyncio import Redis

from seeknal.ask.gateway.tenant import DEFAULT_TENANT

logger = logging.getLogger(__name__)


def _tenant_segment(tenant_id: str) -> str:
    """Return a Redis key segment for a tenant. Empty string for default."""
    if tenant_id == DEFAULT_TENANT:
        return ""
    return f"{tenant_id}:"


# ---------------------------------------------------------------------------
# Broadcaster Protocol — shared interface for local and Redis SSE
# ---------------------------------------------------------------------------


class Broadcaster(Protocol):
    """Protocol for SSE event broadcasting (local or Redis-backed)."""

    async def publish(
        self, session_id: str, event: str, tenant_id: str = DEFAULT_TENANT
    ) -> None: ...
    def subscribe(
        self, session_id: str, tenant_id: str = DEFAULT_TENANT
    ) -> asyncio.Queue: ...
    def unsubscribe(
        self,
        session_id: str,
        queue: asyncio.Queue,
        tenant_id: str = DEFAULT_TENANT,
    ) -> None: ...


# ---------------------------------------------------------------------------
# Redis SSE Broadcaster
# ---------------------------------------------------------------------------


class RedisSSEBroadcaster:
    """Redis pub/sub based SSE event broadcaster.

    Publishes events to a Redis channel per tenant + session. Each
    gateway replica subscribes to active session channels and relays
    events to locally connected SSE clients.
    """

    CHANNEL_PREFIX = "seeknal:sse:"

    def __init__(self, redis: Redis, maxsize: int = 100) -> None:
        self._redis = redis
        self._maxsize = maxsize
        # Keyed by composite "{tenant}:{session}" (or "{session}" for default)
        self._local_subscribers: dict[str, list[asyncio.Queue]] = {}
        self._pubsub = redis.pubsub()
        self._listener_task: asyncio.Task | None = None

    def _channel(self, tenant_id: str, session_id: str) -> str:
        return f"{self.CHANNEL_PREFIX}{_tenant_segment(tenant_id)}{session_id}"

    def subscribe(
        self, session_id: str, tenant_id: str = DEFAULT_TENANT
    ) -> asyncio.Queue:
        """Create a local subscriber queue and subscribe to Redis channel."""
        channel = self._channel(tenant_id, session_id)
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        if channel not in self._local_subscribers:
            self._local_subscribers[channel] = []
            asyncio.ensure_future(self._pubsub.subscribe(channel))
        self._local_subscribers[channel].append(queue)
        return queue

    def unsubscribe(
        self,
        session_id: str,
        queue: asyncio.Queue,
        tenant_id: str = DEFAULT_TENANT,
    ) -> None:
        """Remove a local subscriber queue."""
        channel = self._channel(tenant_id, session_id)
        subs = self._local_subscribers.get(channel, [])
        if queue in subs:
            subs.remove(queue)
        if not subs and channel in self._local_subscribers:
            del self._local_subscribers[channel]
            asyncio.ensure_future(self._pubsub.unsubscribe(channel))

    async def publish(
        self, session_id: str, event: str, tenant_id: str = DEFAULT_TENANT
    ) -> None:
        """Publish an event to the Redis channel (fans out to all replicas)."""
        channel = self._channel(tenant_id, session_id)
        try:
            await self._redis.publish(channel, event)
        except Exception:
            logger.warning(
                "Redis publish failed for tenant=%s session=%s",
                tenant_id,
                session_id,
            )

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
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                for queue in self._local_subscribers.get(channel, []):
                    try:
                        queue.put_nowait(data)
                    except asyncio.QueueFull:
                        pass
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
    Tenant-scoped: keys include a tenant segment so the same session_id
    across tenants is isolated. Default tenant uses legacy unprefixed keys
    for backward compatibility.
    """

    META_PREFIX = "seeknal:session:"
    MSG_PREFIX = "seeknal:messages:"
    INDEX_PREFIX = "seeknal:sessions"  # + ":{tenant}" for non-default
    DEFAULT_TTL = 86400 * 7  # 7 days

    def __init__(self, redis: Redis, ttl: int | None = None) -> None:
        self._redis = redis
        self._ttl = ttl or self.DEFAULT_TTL

    def _meta_key(self, tenant_id: str, name: str) -> str:
        return f"{self.META_PREFIX}{_tenant_segment(tenant_id)}{name}"

    def _msg_key(self, tenant_id: str, name: str) -> str:
        return f"{self.MSG_PREFIX}{_tenant_segment(tenant_id)}{name}"

    def _index_key(self, tenant_id: str) -> str:
        if tenant_id == DEFAULT_TENANT:
            return self.INDEX_PREFIX
        return f"{self.INDEX_PREFIX}:{tenant_id}"

    async def create(self, name: str, tenant_id: str = DEFAULT_TENANT) -> str:
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
        await self._redis.set(
            self._meta_key(tenant_id, name), json.dumps(metadata), ex=self._ttl
        )
        await self._redis.sadd(self._index_key(tenant_id), name)
        return name

    async def get(
        self, name: str, tenant_id: str = DEFAULT_TENANT
    ) -> dict | None:
        """Fetch session metadata."""
        data = await self._redis.get(self._meta_key(tenant_id, name))
        if data is None:
            return None
        return json.loads(data)

    async def list(self, tenant_id: str = DEFAULT_TENANT) -> list[dict]:
        """List all sessions for a tenant."""
        names = await self._redis.smembers(self._index_key(tenant_id))
        sessions = []
        for name in names:
            if isinstance(name, bytes):
                name = name.decode()
            meta = await self.get(name, tenant_id=tenant_id)
            if meta:
                sessions.append(meta)
        sessions.sort(key=lambda s: s.get("updated_at", ""), reverse=True)
        return sessions

    async def update(
        self, name: str, *, tenant_id: str = DEFAULT_TENANT, **kwargs: Any
    ) -> None:
        """Update session metadata fields."""
        meta = await self.get(name, tenant_id=tenant_id)
        if meta is None:
            return
        allowed = {"status", "message_count", "last_question", "updated_at"}
        for k, v in kwargs.items():
            if k in allowed:
                meta[k] = v
        if "updated_at" not in kwargs:
            meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        await self._redis.set(
            self._meta_key(tenant_id, name), json.dumps(meta), ex=self._ttl
        )

    async def delete(self, name: str, tenant_id: str = DEFAULT_TENANT) -> bool:
        """Delete a session."""
        result = await self._redis.delete(
            self._meta_key(tenant_id, name), self._msg_key(tenant_id, name)
        )
        await self._redis.srem(self._index_key(tenant_id), name)
        return result > 0

    async def save_messages(
        self,
        name: str,
        messages_json: bytes,
        tenant_id: str = DEFAULT_TENANT,
    ) -> None:
        """Save serialized conversation history."""
        await self._redis.set(
            self._msg_key(tenant_id, name), messages_json, ex=self._ttl
        )

    async def load_messages(
        self, name: str, tenant_id: str = DEFAULT_TENANT
    ) -> bytes | None:
        """Load serialized conversation history."""
        return await self._redis.get(self._msg_key(tenant_id, name))


# ---------------------------------------------------------------------------
# Redis Distributed Lock
# ---------------------------------------------------------------------------


class RedisSessionLock:
    """Redis-based distributed session lock.

    Replaces the per-process asyncio.Lock dict for multi-replica
    session serialization. Uses Redis SETNX with auto-expiry.
    Tenant-scoped so the same session_id across tenants has independent
    locks.
    """

    LOCK_PREFIX = "seeknal:lock:"
    DEFAULT_TIMEOUT = 300  # 5 minutes

    def __init__(self, redis: Redis, timeout: int | None = None) -> None:
        self._redis = redis
        self._timeout = timeout or self.DEFAULT_TIMEOUT

    def _key(self, tenant_id: str, session_id: str) -> str:
        return f"{self.LOCK_PREFIX}{_tenant_segment(tenant_id)}{session_id}"

    async def acquire(
        self, session_id: str, tenant_id: str = DEFAULT_TENANT
    ) -> Any:
        """Acquire a distributed lock for a tenant's session."""
        lock = self._redis.lock(
            self._key(tenant_id, session_id),
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

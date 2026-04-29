"""HTTP-only worker broker for gateway-routed Seeknal workers.

This module is intentionally dependency-light. It provides an in-process async
broker used when the gateway hosts the Temporal workflow/activity, but the agent
execution is delegated to an on-prem worker that only talks HTTP(S) to the
gateway/kc-service.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import time
from typing import Any
from uuid import uuid4

from seeknal.ask.gateway.tenant import DEFAULT_TENANT


@dataclass(frozen=True)
class HttpWorkerItem:
    """A single unit of agent work delivered to an HTTP-only worker."""

    work_id: str
    tenant_id: str
    session_id: str
    question: str
    project_path: str
    provider: str | None = None
    model: str | None = None
    created_at: float = field(default_factory=time.time)

    def public_dict(self) -> dict[str, Any]:
        return {
            "work_id": self.work_id,
            "tenant_id": self.tenant_id,
            "session_id": self.session_id,
            "question": self.question,
            "project_path": self.project_path,
            "provider": self.provider,
            "model": self.model,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class HttpWorkerResult:
    """Final result posted by an HTTP-only worker."""

    answer: str = ""
    event_count: int = 0
    error: str | None = None


class HttpWorkerBroker:
    """In-memory queue for HTTP-only workers.

    This broker is scoped to a single gateway process. It is a deliberately thin
    first implementation: production multi-replica deployments can put the same
    endpoint contract in front of Redis or another durable broker without
    changing worker behavior.
    """

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._pending: list[HttpWorkerItem] = []
        self._inflight: dict[str, HttpWorkerItem] = {}
        self._futures: dict[str, asyncio.Future[HttpWorkerResult]] = {}

    async def reset(self) -> None:
        """Clear broker state. Intended for tests and controlled shutdown."""
        async with self._condition:
            for future in self._futures.values():
                if not future.done():
                    future.cancel()
            self._pending.clear()
            self._inflight.clear()
            self._futures.clear()
            self._condition.notify_all()

    async def enqueue_and_wait(
        self,
        *,
        session_id: str,
        question: str,
        project_path: str,
        tenant_id: str = DEFAULT_TENANT,
        provider: str | None = None,
        model: str | None = None,
        timeout: float | None = None,
    ) -> HttpWorkerResult:
        """Enqueue work and wait for a matching completion POST."""
        loop = asyncio.get_running_loop()
        item = HttpWorkerItem(
            work_id=uuid4().hex,
            tenant_id=tenant_id,
            session_id=session_id,
            question=question,
            project_path=project_path,
            provider=provider,
            model=model,
        )
        future: asyncio.Future[HttpWorkerResult] = loop.create_future()
        async with self._condition:
            self._pending.append(item)
            self._futures[item.work_id] = future
            self._condition.notify_all()

        try:
            if timeout is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout)
        except BaseException:
            await self.discard(item.work_id)
            raise

    async def claim_next(
        self,
        *,
        tenant_id: str,
        timeout: float = 30.0,
    ) -> HttpWorkerItem | None:
        """Return the next pending item for a tenant, or ``None`` on timeout."""
        deadline = time.monotonic() + max(timeout, 0.0)
        async with self._condition:
            while True:
                for index, item in enumerate(self._pending):
                    if item.tenant_id == tenant_id:
                        claimed = self._pending.pop(index)
                        self._inflight[claimed.work_id] = claimed
                        return claimed

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return None

    async def complete(
        self,
        *,
        work_id: str,
        tenant_id: str,
        result: HttpWorkerResult,
    ) -> bool:
        """Complete an in-flight item if it belongs to the tenant."""
        async with self._condition:
            item = self._inflight.get(work_id)
            if item is None or item.tenant_id != tenant_id:
                return False
            self._inflight.pop(work_id, None)
            future = self._futures.pop(work_id, None)
            if future is not None and not future.done():
                future.set_result(result)
            self._condition.notify_all()
            return True

    async def fail(self, *, work_id: str, tenant_id: str, error: str) -> bool:
        return await self.complete(
            work_id=work_id,
            tenant_id=tenant_id,
            result=HttpWorkerResult(error=error),
        )

    async def discard(self, work_id: str) -> None:
        """Remove work after cancellation/timeout."""
        async with self._condition:
            self._pending = [item for item in self._pending if item.work_id != work_id]
            self._inflight.pop(work_id, None)
            future = self._futures.pop(work_id, None)
            if future is not None and not future.done():
                future.cancel()
            self._condition.notify_all()

    async def owns(self, *, work_id: str, tenant_id: str) -> HttpWorkerItem | None:
        """Return item if a tenant owns it, otherwise ``None``."""
        async with self._condition:
            item = self._inflight.get(work_id)
            if item is not None and item.tenant_id == tenant_id:
                return item
            return None


http_worker_broker = HttpWorkerBroker()

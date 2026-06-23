"""Per-session registry pairing ask_user prompts to client responses.

Used by gateway WebSocket endpoint to bridge agent ↔ client for two-way
clarification. When agent calls `ask_user`, gateway registers a Future
keyed by (session_id, prompt_id), broadcasts the question+options to the
WebSocket client, then awaits the Future.

Timeout behavior:
- timeout=None (default, production): wait forever for client response.
- timeout=N (experimentation): auto-select recommended after N seconds.
"""

from __future__ import annotations

import asyncio
from typing import Any


def _pick_recommended(options: list[dict[str, Any]] | None) -> str:
    """Pick the recommended option label, or the first option, or empty string."""
    if not options:
        return ""
    for opt in options:
        if str(opt.get("recommended", "")).lower() == "true":
            return str(opt.get("label", ""))
    return str(options[0].get("label", ""))


class AskUserRendezvous:
    """Per-session registry pairing ask_user prompts to client responses."""

    def __init__(self) -> None:
        self._futures: dict[str, dict[str, asyncio.Future[str]]] = {}
        self._lock = asyncio.Lock()

    async def register(self, session_id: str, prompt_id: str) -> asyncio.Future[str]:
        async with self._lock:
            session_prompts = self._futures.setdefault(session_id, {})
            if prompt_id in session_prompts and not session_prompts[prompt_id].done():
                raise ValueError(
                    f"prompt_id {prompt_id!r} already pending in session {session_id!r}"
                )
            loop = asyncio.get_event_loop()
            future: asyncio.Future[str] = loop.create_future()
            session_prompts[prompt_id] = future
            return future

    async def resolve(self, session_id: str, prompt_id: str, answer: str) -> bool:
        async with self._lock:
            session_prompts = self._futures.get(session_id)
            if not session_prompts:
                return False
            future = session_prompts.get(prompt_id)
            if future is None or future.done():
                return False
            future.set_result(answer)
            return True

    async def wait(
        self,
        session_id: str,
        prompt_id: str,
        timeout: float | None = None,
        options: list[dict[str, Any]] | None = None,
    ) -> str:
        async with self._lock:
            future = self._futures.get(session_id, {}).get(prompt_id)

        if future is None:
            return _pick_recommended(options)

        if timeout is None:
            return await future

        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except asyncio.TimeoutError:
            recommended = _pick_recommended(options)
            if not future.done():
                future.set_result(recommended)
            return future.result()

    async def cleanup(self, session_id: str) -> None:
        async with self._lock:
            session_prompts = self._futures.pop(session_id, {})
        for future in session_prompts.values():
            if not future.done():
                future.cancel()

    async def pending_count(self, session_id: str) -> int:
        async with self._lock:
            return sum(1 for f in self._futures.get(session_id, {}).values() if not f.done())


_ask_user_rendezvous: AskUserRendezvous | None = None


def get_global_rendezvous() -> AskUserRendezvous:
    global _ask_user_rendezvous
    if _ask_user_rendezvous is None:
        _ask_user_rendezvous = AskUserRendezvous()
    return _ask_user_rendezvous

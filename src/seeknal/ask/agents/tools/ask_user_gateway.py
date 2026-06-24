"""Gateway ask_user callback factory — transport agnostic.

Bridges the agent's ask_user tool to the client via AskUserRendezvous.
The broadcast_event callable is the only transport-specific piece: pass
an SSE publisher for in-process /ask, or an HTTP POST for HTTP workers.

Usage::

    rendezvous = get_global_rendezvous()

    async def broadcast_event(event: dict) -> None:
        await _publish_event_async(session_id, event, broadcaster=broadcaster)

    callback = await make_gateway_ask_user(
        rendezvous=rendezvous,
        session_id="s1",
        broadcast_event=broadcast_event,
        timeout=None,  # production: wait forever; N = auto-select after N seconds
    )

    agent, deps, *_ = create_agent(
        project_path,
        environment="gateway",
        ask_user_callback=callback,
    )
"""

from __future__ import annotations

import json
from typing import Any, Awaitable, Callable
from uuid import uuid4

from seeknal.ask.gateway._ask_rendezvous import AskUserRendezvous

BroadcastFn = Callable[[dict[str, Any]], Awaitable[None]]


async def make_gateway_ask_user(
    rendezvous: AskUserRendezvous,
    session_id: str,
    broadcast_event: BroadcastFn,
    timeout: float | None = None,
):
    """Factory: build an async ask_user callback for a gateway session.

    timeout=None (default): production, wait forever.
    timeout=N: experimentation, auto-select recommended after N seconds.
    """
    async def gateway_ask_user(question: str, options: list[dict[str, Any]]) -> str:
        prompt_id = uuid4().hex
        await rendezvous.register(session_id, prompt_id)

        try:
            await broadcast_event(
                {
                    "type": "ask_user",
                    "prompt_id": prompt_id,
                    "question": question,
                    "options": options,
                    "timeout_seconds": timeout,
                    "session_id": session_id,
                }
            )
        except Exception:
            await rendezvous.cleanup(session_id)
            raise

        return await rendezvous.wait(
            session_id, prompt_id, timeout=timeout, options=options
        )

    return gateway_ask_user

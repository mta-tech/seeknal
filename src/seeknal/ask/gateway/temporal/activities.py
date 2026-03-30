"""Temporal activity definitions for seeknal ask agent turns.

Defines the ``run_agent_turn`` activity which creates an agent via
``create_agent()`` and runs a single turn. Also defines ``RedisSink``
which publishes streaming events to a Redis pub/sub channel so that
the SSE endpoint can relay them to clients.

All temporalio and redis imports are lazy (optional dependencies).
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Optional

from seeknal.ask.event_sink import EventSink

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RedisSink — publishes agent events to Redis pub/sub
# ---------------------------------------------------------------------------


class RedisSink:
    """EventSink that publishes events to a Redis pub/sub channel.

    Channel name format: ``session:{session_id}``

    Each event is published as a JSON string with ``type`` and ``data`` keys,
    matching the same format used by WebSocketSink for consistency.
    """

    def __init__(self, session_id: str, redis_url: str = "redis://localhost:6379") -> None:
        self.session_id = session_id
        self._redis_url = redis_url
        self._client: Any = None
        self._channel = f"session:{session_id}"

    def _get_client(self) -> Any:
        """Lazily create the Redis client."""
        if self._client is None:
            import redis

            self._client = redis.Redis.from_url(self._redis_url)
        return self._client

    def _publish(self, event: dict) -> None:
        """Publish a JSON event to the session channel."""
        try:
            client = self._get_client()
            client.publish(self._channel, json.dumps(event))
        except Exception:
            logger.debug("Failed to publish to Redis channel %s", self._channel)

    async def on_token(self, text: str) -> None:
        self._publish({"type": "token", "data": text})

    async def on_tool_start(self, name: str, args: dict[str, Any]) -> None:
        self._publish({
            "type": "tool_start",
            "data": {"name": name, "args": args},
        })

    async def on_tool_end(self, name: str, result: str) -> None:
        self._publish({
            "type": "tool_end",
            "data": {"name": name, "result": result},
        })

    async def on_answer(self, text: str) -> None:
        self._publish({"type": "answer", "data": text})

    async def on_error(self, error: str) -> None:
        self._publish({"type": "error", "data": error})

    def close(self) -> None:
        """Close the Redis client if it was created."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# Verify protocol conformance at import time
assert isinstance(RedisSink.__new__(RedisSink), EventSink)


# ---------------------------------------------------------------------------
# Activity definition
# ---------------------------------------------------------------------------


def _get_activity_fn():
    """Lazily define and return the activity function.

    This avoids importing temporalio at module level.
    """
    from temporalio import activity

    from seeknal.ask.gateway.temporal.workflows import AgentTurnParams, AgentTurnResult

    @activity.defn
    async def run_agent_turn(params: AgentTurnParams) -> AgentTurnResult:
        """Execute a single agent turn as a Temporal activity.

        Creates the agent via ``create_agent()``, runs it with ``ask()``,
        and heartbeats before and after the agent call so Temporal knows
        the activity is still alive.

        Args:
            params: Agent turn parameters (question, project path, etc.).

        Returns:
            AgentTurnResult with the response text and metadata.
        """
        import asyncio

        activity.heartbeat("starting agent turn")

        try:
            from seeknal.ask.agents.agent import ask as sync_ask
            from seeknal.ask.agents.agent import create_agent

            project_path = Path(params.project_path)

            agent, config = create_agent(
                project_path=project_path,
                provider=params.config_overrides.get("provider"),
                model=params.config_overrides.get("model"),
                api_key=params.config_overrides.get("api_key"),
                question=params.question,
                session_name=params.session_name,
            )

            activity.heartbeat("running agent")

            answer = await asyncio.to_thread(
                sync_ask, agent, config, params.question,
            )

            activity.heartbeat("agent turn complete")

            return AgentTurnResult(
                response_text=answer,
                tool_calls_count=0,
                success=True,
            )

        except Exception as exc:
            logger.exception("Agent turn failed: %s", exc)
            return AgentTurnResult(
                response_text=f"Error: {exc}",
                tool_calls_count=0,
                success=False,
            )

    return run_agent_turn


# Module-level accessor used by workflow and worker
_activity_fn = None
_activity_lock = threading.Lock()


def get_activity_fn():
    """Return the run_agent_turn activity function (lazily defined, thread-safe)."""
    global _activity_fn
    with _activity_lock:
        if _activity_fn is None:
            _activity_fn = _get_activity_fn()
        return _activity_fn


# For direct import by the workflow module — triggers lazy init
def run_agent_turn(params):
    """Placeholder referenced by workflow execute_activity.

    The actual decorated function is returned by get_activity_fn().
    This exists so the workflow can reference it by name at definition time.
    """
    raise RuntimeError("Use get_activity_fn() to get the real activity function")

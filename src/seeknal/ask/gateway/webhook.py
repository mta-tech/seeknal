"""Webhook-based streaming for seeknal ask gateway.

Client POSTs a question to /ask/stream, gets an immediate 201,
and receives events via HTTP POST callbacks to a configurable
webhook URL. Events follow a sparse-envelope schema with typed
payloads per event kind.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# How often heartbeat events fire while the agent is running
_HEARTBEAT_INTERVAL = 15


@dataclass
class WebhookConfig:
    message_id: str
    session_id: str
    webhook_url: str


class WebhookClient:
    """Delivers agent events to a remote webhook URL via HTTP POST."""

    def __init__(self, config: WebhookConfig) -> None:
        self._config = config
        self._running = False
        self._heartbeat_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # URL construction
    # ------------------------------------------------------------------

    @staticmethod
    def build_webhook_url(webhook_path: str) -> str:
        """Construct a full webhook URL from a path.

        Reads ``SEEKNAL_WEBHOOK_BASE_URL`` from the environment and appends
        *webhook_path*.  The path **must** start with ``/`` to prevent
        external-URL injection (e.g. ``https://evil.com/steal``).

        Raises:
            ValueError: If the env var is missing or the path is invalid.
        """
        base_url = os.environ.get("SEEKNAL_WEBHOOK_BASE_URL", "").strip()
        if not base_url:
            raise ValueError("SEEKNAL_WEBHOOK_BASE_URL is not set")
        base_url = base_url.rstrip("/")
        if not webhook_path.startswith("/"):
            raise ValueError(
                "webhook_path must start with '/' "
                "(use a path, not a full URL)"
            )
        return base_url + webhook_path

    # ------------------------------------------------------------------
    # Event envelope
    # ------------------------------------------------------------------

    def _make_event(
        self, event_name: str, payload: dict | None = None,
    ) -> dict[str, Any]:
        """Build a webhook envelope with sparse payload."""
        return {
            "event": event_name,
            "message_id": self._config.message_id,
            "conversation_id": self._config.session_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload or {},
        }

    # ------------------------------------------------------------------
    # HTTP delivery (fire-and-forget)
    # ------------------------------------------------------------------

    async def _post(self, payload: dict) -> None:
        """POST a single event.  Retries once on failure; never raises."""
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    resp = await client.post(
                        self._config.webhook_url, json=payload,
                    )
                    resp.raise_for_status()
                    return
            except Exception:
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                logger.warning(
                    "Webhook delivery failed for event=%s",
                    payload.get("event"),
                )

    # ------------------------------------------------------------------
    # Event mapping
    # ------------------------------------------------------------------

    def _map_event(self, raw: dict) -> dict | None:
        """Map a raw agent event to a webhook envelope.

        Returns ``None`` for events that should be skipped (e.g. ``token``).
        """
        kind = raw.get("type")
        data = raw.get("data")

        if kind == "token":
            return None

        if kind == "reasoning":
            return self._make_event("message", {"answer": data})

        if kind == "answer":
            return self._make_event("message", {"answer": data})

        if kind == "tool_start":
            return self._make_event("tool_call", {
                "tool_name": data.get("name", ""),
                "tool_args": data.get("args", {}),
            })

        if kind == "tool_end":
            return self._make_event("tool_result", {
                "tool_name": data.get("name", ""),
                "tool_result": data.get("output", ""),
            })

        # Unknown event types are silently skipped
        return None

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Fire heartbeat events every 15 s while the agent is running."""
        while self._running:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            if self._running:
                await self._post(self._make_event("heartbeat"))

    # ------------------------------------------------------------------
    # Main driver
    # ------------------------------------------------------------------

    async def run_and_deliver(
        self,
        project_path,
        session_id: str,
        question: str,
    ) -> None:
        """Run the agent and deliver mapped events to the webhook.

        Emits: ``start`` → … mapped events … → ``done``.
        On exception emits ``error`` instead of ``done``.
        """
        # Lazy import matches the Telegram channel pattern
        from seeknal.ask.gateway.server import _run_agent_streaming

        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        try:
            await self._post(self._make_event("start"))

            async for event in _run_agent_streaming(
                project_path, session_id, question,
            ):
                mapped = self._map_event(event)
                if mapped is not None:
                    await self._post(mapped)

        except Exception as exc:
            await self._post(self._make_event("error", {
                "error_code": "agent_error",
                "error_message": str(exc),
            }))
        finally:
            self._running = False
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
            await self._post(self._make_event("done"))

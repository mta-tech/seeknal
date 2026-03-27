"""Base protocol for gateway channel plugins.

A Channel represents an external messaging platform (Telegram, Slack, etc.)
that can receive user messages and deliver agent responses. The gateway
server manages channel lifecycles via start/stop and routes outbound
messages via deliver().
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Channel(Protocol):
    """Protocol for gateway channel plugins."""

    async def start(self) -> None:
        """Initialize the channel (e.g., start bot, register webhook)."""
        ...

    async def stop(self) -> None:
        """Shut down the channel gracefully."""
        ...

    async def deliver(self, session_id: str, text: str) -> None:
        """Deliver a message to the channel for the given session."""
        ...

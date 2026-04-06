"""Base channel protocol for seeknal gateway."""

from __future__ import annotations

from typing import Protocol


class Channel(Protocol):
    """Protocol for gateway communication channels."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def deliver(self, session_id: str, question: str) -> str: ...

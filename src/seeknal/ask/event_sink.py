"""Event sink protocol for seeknal ask streaming output.

Defines a protocol that abstracts where agent events are delivered:
- ConsoleSink: Rich Console rendering (CLI)
- WebSocketSink: JSON events over WebSocket (gateway)
- SSESink: Server-Sent Events (Temporal worker)

All sinks receive the same events from the agent execution loop,
enabling identical agent behavior regardless of output target.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class EventSink(Protocol):
    """Protocol for receiving agent streaming events."""

    async def on_token(self, text: str) -> None:
        """Called for each incremental LLM token."""
        ...

    async def on_tool_start(self, name: str, args: dict[str, Any]) -> None:
        """Called when a tool invocation begins."""
        ...

    async def on_tool_end(self, name: str, result: str) -> None:
        """Called when a tool invocation completes."""
        ...

    async def on_answer(self, text: str) -> None:
        """Called with the final answer text."""
        ...

    async def on_error(self, error: str) -> None:
        """Called when an error occurs during agent execution."""
        ...


class NullSink:
    """Sink that discards all events. Useful for quiet/headless mode."""

    async def on_token(self, text: str) -> None:
        pass

    async def on_tool_start(self, name: str, args: dict[str, Any]) -> None:
        pass

    async def on_tool_end(self, name: str, result: str) -> None:
        pass

    async def on_answer(self, text: str) -> None:
        pass

    async def on_error(self, error: str) -> None:
        pass


class CollectorSink:
    """Sink that collects all events into lists. Useful for testing."""

    def __init__(self) -> None:
        self.tokens: list[str] = []
        self.tool_starts: list[tuple[str, dict]] = []
        self.tool_ends: list[tuple[str, str]] = []
        self.answers: list[str] = []
        self.errors: list[str] = []

    async def on_token(self, text: str) -> None:
        self.tokens.append(text)

    async def on_tool_start(self, name: str, args: dict[str, Any]) -> None:
        self.tool_starts.append((name, args))

    async def on_tool_end(self, name: str, result: str) -> None:
        self.tool_ends.append((name, result))

    async def on_answer(self, text: str) -> None:
        self.answers.append(text)

    async def on_error(self, error: str) -> None:
        self.errors.append(error)

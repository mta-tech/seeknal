"""Tool context — shared state passed to all agent tools.

The ToolContext holds the REPL instance and artifact discovery service.
It's set once per session at agent initialization and accessed by tools
via get_tool_context().

Uses contextvars.ContextVar for per-asyncio-task isolation, enabling
concurrent sessions in the gateway without state leakage.
"""

from __future__ import annotations

import contextvars
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from seeknal.ask.memory.store import MemoryStore
    from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery
    from seeknal.cli.repl import REPL

_tool_context: contextvars.ContextVar[ToolContext] = contextvars.ContextVar("tool_context")


@dataclass
class ToolContext:
    """Shared context for all agent tools.

    Includes a threading lock to protect the DuckDB connection from
    concurrent access — LangGraph may invoke tools in parallel threads.
    """

    repl: REPL
    artifact_discovery: ArtifactDiscovery
    project_path: Path
    console: Any = None  # Optional Rich Console for progress display
    db_lock: threading.Lock = field(default_factory=threading.Lock)
    fs_lock: threading.Lock = field(default_factory=threading.Lock)
    plan_steps: list[str] = field(default_factory=list)
    plan_step_index: int = 0  # next step to complete (0-indexed)
    exposure_mode: bool = False  # True when running non-interactive exposure
    subagent_tool_count: int = 0  # tool calls made during current subagent run
    interactive: bool = False  # True in chat mode; ask_user blocks on input
    max_questions: int = 5  # reset target for questions_remaining
    questions_remaining: int = 5  # decremented by ask_user; reset per stream_ask turn
    memory_store: Optional[MemoryStore] = None  # persistent memory (set by create_agent)


def set_tool_context(ctx: ToolContext) -> contextvars.Token:
    """Set the tool context for the current async task / thread.

    Returns a token that can be used to reset the context later.
    In CLI mode (single session), this is called once.
    In gateway mode, each session handler sets its own context.
    """
    return _tool_context.set(ctx)


def get_tool_context() -> ToolContext:
    """Get the current tool context.

    Raises:
        RuntimeError: If context hasn't been initialized for this task.
    """
    try:
        return _tool_context.get()
    except LookupError:
        raise RuntimeError(
            "Tool context not initialized. "
            "Call set_tool_context() before using agent tools."
        )

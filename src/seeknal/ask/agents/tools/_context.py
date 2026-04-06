"""Tool context — shared state passed to all agent tools.

The ToolContext holds the REPL instance and artifact discovery service.
It's set once per session at agent initialization and accessed by tools
via get_tool_context().

Uses ``contextvars.ContextVar`` for async-safe per-session isolation so
multiple concurrent ask sessions don't cross-contaminate each other's
DuckDB connections or project artifacts.
"""

from __future__ import annotations

import contextvars
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from seeknal.ask.background import BackgroundRegistry
    from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery
    from seeknal.cli.repl import REPL

# Per-task (async-safe) context variable instead of a module-level global.
# Each asyncio task / OS process gets its own ToolContext.
_tool_context_var: contextvars.ContextVar[ToolContext] = contextvars.ContextVar(
    "seeknal_tool_context"
)


@dataclass
class ToolContext:
    """Shared context for all agent tools within one session.

    Includes a threading lock to protect the DuckDB connection from
    concurrent access — pydantic-ai may invoke tools in parallel threads.
    """

    repl: REPL
    artifact_discovery: ArtifactDiscovery
    project_path: Path
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    db_lock: threading.Lock = field(default_factory=threading.Lock)
    require_report_approval: bool = False
    report_approval_granted: bool = False
    background_registry: BackgroundRegistry = field(default_factory=lambda: _make_registry())


def _make_registry():
    """Lazy factory to avoid circular import at module load."""
    from seeknal.ask.background import BackgroundRegistry
    return BackgroundRegistry()


def set_tool_context(ctx: ToolContext) -> None:
    """Set the tool context for the current async task / thread."""
    _tool_context_var.set(ctx)


def get_tool_context() -> ToolContext:
    """Get the current session's tool context.

    Raises:
        RuntimeError: If context hasn't been initialized for this task.
    """
    try:
        return _tool_context_var.get()
    except LookupError:
        raise RuntimeError(
            "Tool context not initialized. "
            "Call set_tool_context() before using agent tools."
        )


_REPORT_APPROVAL_OPTIONS = frozenset({
    "continue analysis",
    "generate report now",
    "done for now",
    "type your own",
})


def reset_report_approval(required: bool) -> None:
    """Reset per-turn report approval state."""
    ctx = get_tool_context()
    ctx.require_report_approval = required
    ctx.report_approval_granted = False


def record_ask_user_response(options: list[dict[str, Any]], answer: str) -> None:
    """Persist explicit report approval when the user grants it."""
    ctx = get_tool_context()
    if not ctx.require_report_approval:
        return

    labels = {
        str(option.get("label", "")).strip().lower()
        for option in options
        if option.get("label")
    }
    if _REPORT_APPROVAL_OPTIONS.issubset(labels):
        ctx.report_approval_granted = answer.strip().lower() == "generate report now"


def require_report_approval(tool_name: str) -> str | None:
    """Return an actionable error when a report tool is used without approval."""
    ctx = get_tool_context()
    if not ctx.require_report_approval or ctx.report_approval_granted:
        return None
    return (
        f"Approval required before {tool_name}. Summarize the current findings and proposed next step, then use ask_user with exactly these options: 'Continue analysis', 'Generate report now', 'Done for now', and 'Type your own'. Do not print those choices as plain text, bullets, or numbered lists. Only call {tool_name} after the user explicitly chooses 'Generate report now'."
    )

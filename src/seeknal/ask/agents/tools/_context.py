"""Tool context — shared state passed to all agent tools.

The ToolContext holds the REPL instance and artifact discovery service.
It's set once at agent initialization and accessed by tools via get_tool_context().
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery
    from seeknal.cli.repl import REPL

_tool_context: Optional["ToolContext"] = None


@dataclass
class ToolContext:
    """Shared context for all agent tools.

    Includes a threading lock to protect the DuckDB connection from
    concurrent access — LangGraph may invoke tools in parallel threads.
    """

    repl: REPL
    artifact_discovery: ArtifactDiscovery
    project_path: Path
    db_lock: threading.Lock = field(default_factory=threading.Lock)
    fs_lock: threading.Lock = field(default_factory=threading.Lock)


def set_tool_context(ctx: ToolContext) -> None:
    """Set the global tool context. Called once at agent startup."""
    global _tool_context
    _tool_context = ctx


def get_tool_context() -> ToolContext:
    """Get the current tool context.

    Raises:
        RuntimeError: If context hasn't been initialized.
    """
    if _tool_context is None:
        raise RuntimeError(
            "Tool context not initialized. "
            "Call set_tool_context() before using agent tools."
        )
    return _tool_context

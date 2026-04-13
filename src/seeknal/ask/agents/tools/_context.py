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
    fs_lock: threading.Lock = field(default_factory=threading.Lock)
    request_limit: int = 100
    background_threshold: int = 60
    require_report_approval: bool = True
    report_approval_granted: bool = False
    require_proof_publish_approval: bool = True
    proof_publish_approval_granted: bool = False
    require_proof_edit_approval: bool = True
    proof_edit_approval_granted: bool = False
    require_seeknal_report_publish_approval: bool = True
    seeknal_report_publish_approval_granted: bool = False
    background_registry: BackgroundRegistry = field(default_factory=lambda: _make_registry())
    console: Any = None
    plan_steps: list[str] = field(default_factory=list)
    plan_step_index: int = 0


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


# Unique discriminator labels — each approval is keyed off a single unique
# option label. The agent must present a menu with the discriminator as one
# of the options AND the user must explicitly select it. We deliberately do
# NOT require a strict superset of 4 exact labels, because the model
# frequently paraphrases one of them (e.g. "Just done" vs "Done for now"),
# which silently dropped approval even when the user clearly picked the
# discriminator. See the last ask session bug report.
_REPORT_APPROVAL_DISCRIMINATOR = "generate report now"
_PROOF_PUBLISH_APPROVAL_DISCRIMINATOR = "publish memo to proof"
_PROOF_EDIT_APPROVAL_DISCRIMINATOR = "apply edit to proof"
_SEEKNAL_REPORT_PUBLISH_DISCRIMINATOR = "publish to seeknal report server"


def reset_report_approval() -> None:
    """Reset per-turn approval state for report AND proof tools.

    The agent must re-ask the user every turn before generating reports, or
    publishing/editing documents on Proof — no approval carries across turns.
    """
    ctx = get_tool_context()
    ctx.require_report_approval = True
    ctx.report_approval_granted = False
    ctx.require_proof_publish_approval = True
    ctx.proof_publish_approval_granted = False
    ctx.require_proof_edit_approval = True
    ctx.proof_edit_approval_granted = False
    ctx.require_seeknal_report_publish_approval = True
    ctx.seeknal_report_publish_approval_granted = False


def record_ask_user_response(options: list[dict[str, Any]], answer: str) -> None:
    """Persist explicit report or proof-publish approval when the user grants it.

    Each approval is keyed off a single unique discriminator label:
      - `generate report now`  → grants report generation
      - `publish memo to proof` → grants publishing to Proof / memokami
      - `apply edit to proof`  → grants editing an existing Proof doc

    Grant conditions (all must hold):
      1. The discriminator label appears as one of the options shown.
      2. The user's selected answer equals that discriminator (case/whitespace
         insensitive).
      3. The corresponding `require_*_approval` flag is still enabled.

    Previously this also required a strict 4-element subset of exact labels,
    which silently dropped approval whenever the LLM paraphrased even one of
    them. That strictness was removed after a repeated session-level lockout
    where the user selected `Publish memo to Proof` three times and the tool
    kept returning "Approval required". The discriminator is unique enough
    that a false grant would require the agent to deliberately reuse the
    same discriminator label for an unrelated menu.
    """
    ctx = get_tool_context()

    labels = {
        str(option.get("label", "")).strip().lower()
        for option in options
        if option.get("label")
    }
    normalized_answer = answer.strip().lower()

    if (
        getattr(ctx, "require_report_approval", False)
        and _REPORT_APPROVAL_DISCRIMINATOR in labels
    ):
        ctx.report_approval_granted = (
            normalized_answer == _REPORT_APPROVAL_DISCRIMINATOR
        )

    if (
        getattr(ctx, "require_proof_publish_approval", False)
        and _PROOF_PUBLISH_APPROVAL_DISCRIMINATOR in labels
    ):
        ctx.proof_publish_approval_granted = (
            normalized_answer == _PROOF_PUBLISH_APPROVAL_DISCRIMINATOR
        )

    if (
        getattr(ctx, "require_proof_edit_approval", False)
        and _PROOF_EDIT_APPROVAL_DISCRIMINATOR in labels
    ):
        ctx.proof_edit_approval_granted = (
            normalized_answer == _PROOF_EDIT_APPROVAL_DISCRIMINATOR
        )

    if (
        getattr(ctx, "require_seeknal_report_publish_approval", False)
        and _SEEKNAL_REPORT_PUBLISH_DISCRIMINATOR in labels
    ):
        ctx.seeknal_report_publish_approval_granted = (
            normalized_answer == _SEEKNAL_REPORT_PUBLISH_DISCRIMINATOR
        )


def require_report_approval(tool_name: str) -> str | None:
    """Return an actionable error when a report tool is used without approval."""
    ctx = get_tool_context()
    if not ctx.require_report_approval or ctx.report_approval_granted:
        return None
    return (
        f"Approval required before {tool_name}. Summarize the current findings and proposed next step, then use ask_user with exactly these options: 'Continue analysis', 'Generate report now', 'Done for now', and 'Type your own'. Do not print those choices as plain text, bullets, or numbered lists. Only call {tool_name} after the user explicitly chooses 'Generate report now'."
    )


def require_proof_publish_approval(tool_name: str) -> str | None:
    """Return an actionable error when publish_to_proof is used without approval."""
    ctx = get_tool_context()
    if (
        not getattr(ctx, "require_proof_publish_approval", True)
        or getattr(ctx, "proof_publish_approval_granted", False)
    ):
        return None
    return (
        f"Approval required before {tool_name}. Summarize the memo content and its intended audience, "
        "then use ask_user with exactly these options: 'Continue analysis', 'Publish memo to Proof', "
        "'Done for now', and 'Type your own'. Do not print those choices as plain text, bullets, or "
        f"numbered lists. Only call {tool_name} after the user explicitly chooses 'Publish memo to Proof'."
    )


def require_seeknal_report_publish_approval(tool_name: str) -> str | None:
    """Return an actionable error when publish_to_seeknal_report is used without approval."""
    ctx = get_tool_context()
    if (
        not getattr(ctx, "require_seeknal_report_publish_approval", True)
        or getattr(ctx, "seeknal_report_publish_approval_granted", False)
    ):
        return None
    return (
        f"Approval required before {tool_name}. Summarize what will be published and to which server, "
        "then use ask_user with exactly these options: 'Continue analysis', 'Publish to Seeknal Report Server', "
        "'Publish memo to Proof', 'Done for now', and 'Type your own'. Do not print those choices as plain text, "
        "bullets, or numbered lists. Only call "
        f"{tool_name} after the user explicitly chooses 'Publish to Seeknal Report Server'."
    )


def require_proof_edit_approval(tool_name: str) -> str | None:
    """Return an actionable error when edit_proof_document is used without approval."""
    ctx = get_tool_context()
    if (
        not getattr(ctx, "require_proof_edit_approval", True)
        or getattr(ctx, "proof_edit_approval_granted", False)
    ):
        return None
    return (
        f"Approval required before {tool_name}. Summarize the proposed edit (which "
        "document, what changes) and then use ask_user with exactly these options: "
        "'Continue analysis', 'Apply edit to Proof', 'Done for now', and 'Type your own'. "
        "Do not print those choices as plain text, bullets, or numbered lists. Only call "
        f"{tool_name} after the user explicitly chooses 'Apply edit to Proof'."
    )

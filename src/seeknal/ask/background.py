"""Auto-background long-running tool calls.

When a subprocess tool (execute_python, generate_report) exceeds a
configurable threshold (default 60s), the tool returns immediately with
a "[Backgrounded]" placeholder.  The subprocess continues running as an
``asyncio.Task``; when it finishes, the result is enqueued to a
notification queue that the chat loop drains before the next agent turn.

Inspired by Claude Code's background task pattern.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class BackgroundTask:
    """A tool call that was moved to the background."""

    task_id: str
    tool_name: str
    description: str
    started_at: float
    asyncio_task: asyncio.Task[str] | None = None
    status: Literal["running", "completed", "failed"] = "running"
    result: str | None = None


# ---------------------------------------------------------------------------
# Registry — one per session (stored on ToolContext)
# ---------------------------------------------------------------------------

class BackgroundRegistry:
    """Tracks background tasks and collects their completion notifications."""

    def __init__(self) -> None:
        self._tasks: dict[str, BackgroundTask] = {}
        self._notifications: asyncio.Queue[BackgroundTask] | None = None

    def _get_queue(self) -> asyncio.Queue[BackgroundTask]:
        if self._notifications is None:
            self._notifications = asyncio.Queue()
        return self._notifications

    # -- submit ---------------------------------------------------------------

    def submit(
        self,
        tool_name: str,
        description: str,
        coro: Any,  # Coroutine[Any, Any, str]
    ) -> str:
        """Register a background task and start it.

        Args:
            tool_name: Name of the tool (e.g. "execute_python").
            description: Human-readable summary.
            coro: Awaitable that produces the tool result string.

        Returns:
            The generated task_id.
        """
        task_id = uuid.uuid4().hex[:8]
        bg = BackgroundTask(
            task_id=task_id,
            tool_name=tool_name,
            description=description,
            started_at=time.time(),
        )
        self._tasks[task_id] = bg

        async def _run() -> None:
            try:
                result = await coro
                bg.status = "completed"
                bg.result = result
            except Exception as exc:
                bg.status = "failed"
                bg.result = f"Background task failed: {exc}"
            await self._get_queue().put(bg)

        bg.asyncio_task = asyncio.create_task(_run())
        return task_id

    # -- drain ----------------------------------------------------------------

    async def drain_notifications(self) -> list[BackgroundTask]:
        """Non-blocking drain of all completed task notifications."""
        completed: list[BackgroundTask] = []
        if self._notifications is None:
            return completed
            
        while True:
            try:
                task = self._notifications.get_nowait()
                completed.append(task)
            except asyncio.QueueEmpty:
                break
        return completed

    # -- format ---------------------------------------------------------------

    @staticmethod
    def format_notification(task: BackgroundTask) -> str:
        """Format a completed background task as XML context for the agent."""
        elapsed = time.time() - task.started_at
        result_text = task.result or "(no output)"
        # Truncate very large results to avoid bloating the context
        if len(result_text) > 5000:
            result_text = result_text[:5000] + "\n... (truncated)"
        return (
            f'<background_task_result tool="{task.tool_name}" '
            f'task_id="{task.task_id}" status="{task.status}" '
            f'elapsed="{elapsed:.0f}s">\n'
            f"{result_text}\n"
            f"</background_task_result>"
        )

    @property
    def running_count(self) -> int:
        return sum(1 for t in self._tasks.values() if t.status == "running")


# ---------------------------------------------------------------------------
# Async subprocess helper with auto-background
# ---------------------------------------------------------------------------

_BACKGROUNDED_PREFIX = "[Backgrounded]"


async def run_with_auto_background(
    cmd: list[str],
    *,
    tool_name: str,
    description: str,
    registry: BackgroundRegistry,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 300,
    background_threshold: int = 60,
) -> tuple[str, str, int] | str:
    """Run a subprocess; auto-background if it exceeds *background_threshold*.

    Returns:
        ``(stdout, stderr, returncode)`` if the process finishes within the
        threshold (fast path).

        A ``"[Backgrounded] ..."`` placeholder string if the process is
        still running after *background_threshold* seconds.  The process
        continues in the background and the result is enqueued to
        *registry* when done.
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
        env=env,
    )

    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(),
            timeout=background_threshold,
        )
        # Fast path — completed within threshold
        return (
            stdout_bytes.decode(errors="replace"),
            stderr_bytes.decode(errors="replace"),
            proc.returncode or 0,
        )
    except asyncio.TimeoutError:
        # Subprocess still running — background it
        remaining = max(timeout - background_threshold, 30)

        async def _continuation() -> str:
            """Wait for the subprocess to finish and return raw output."""
            try:
                stdout_b, stderr_b = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=remaining,
                )
                return (
                    f"STDOUT:\n{stdout_b.decode(errors='replace')}\n"
                    f"STDERR:\n{stderr_b.decode(errors='replace')}\n"
                    f"EXIT_CODE:{proc.returncode or 0}"
                )
            except asyncio.TimeoutError:
                proc.kill()
                return f"Background task timed out after {timeout}s total."

        task_id = registry.submit(tool_name, description, _continuation())
        return (
            f"{_BACKGROUNDED_PREFIX} This operation is taking longer than "
            f"{background_threshold}s. It will continue in the background "
            f"(task_id: {task_id}). You will be notified when it completes — "
            f"continue with other work or answer the user."
        )

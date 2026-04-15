"""Tests for seeknal.ask.background — auto-backgrounding of long-running tools."""

import asyncio
import sys

import anyio
import pytest

from seeknal.ask.background import (
    BackgroundRegistry,
    BackgroundTask,
    _BACKGROUNDED_PREFIX,
    run_with_auto_background,
)


def _run(coro):
    """Run an async function synchronously using anyio."""
    result = [None]

    async def _wrapper():
        result[0] = await coro

    anyio.run(_wrapper)
    return result[0]


# ---------------------------------------------------------------------------
# BackgroundRegistry
# ---------------------------------------------------------------------------

def test_registry_submit_and_drain():
    """Submit a task, let it complete, drain notifications."""

    async def _test():
        registry = BackgroundRegistry()

        async def _quick():
            return "done"

        task_id = registry.submit("test_tool", "quick task", _quick())
        assert registry.running_count == 1
        await asyncio.sleep(0.1)

        completed = await registry.drain_notifications()
        assert len(completed) == 1
        assert completed[0].task_id == task_id
        assert completed[0].status == "completed"
        assert completed[0].result == "done"

    asyncio.run(_test())


def test_registry_failed_task():
    """A task that raises is marked as failed."""

    async def _test():
        registry = BackgroundRegistry()

        async def _fail():
            raise ValueError("boom")

        registry.submit("test_tool", "failing task", _fail())
        await asyncio.sleep(0.1)

        completed = await registry.drain_notifications()
        assert len(completed) == 1
        assert completed[0].status == "failed"
        assert "boom" in completed[0].result

    asyncio.run(_test())


def test_drain_returns_empty_when_no_tasks():

    async def _test():
        registry = BackgroundRegistry()
        completed = await registry.drain_notifications()
        assert completed == []

    asyncio.run(_test())


def test_format_notification():
    task = BackgroundTask(
        task_id="abc123",
        tool_name="execute_python",
        description="test",
        started_at=0,
        status="completed",
        result="Hello world",
    )
    xml = BackgroundRegistry.format_notification(task)
    assert "execute_python" in xml
    assert "abc123" in xml
    assert "completed" in xml
    assert "Hello world" in xml


# ---------------------------------------------------------------------------
# run_with_auto_background
# ---------------------------------------------------------------------------

def test_fast_command_returns_tuple():
    """A command that finishes quickly returns (stdout, stderr, returncode)."""

    async def _test():
        registry = BackgroundRegistry()
        result = await run_with_auto_background(
            [sys.executable, "-c", "print('hello')"],
            tool_name="test",
            description="fast",
            registry=registry,
            background_threshold=5,
            timeout=10,
        )
        assert isinstance(result, tuple)
        stdout, stderr, returncode = result
        assert "hello" in stdout
        assert returncode == 0

    asyncio.run(_test())


def test_slow_command_is_backgrounded():
    """A command exceeding the threshold returns a backgrounded string."""

    async def _test():
        registry = BackgroundRegistry()
        result = await run_with_auto_background(
            [sys.executable, "-c", "import time; time.sleep(3); print('done')"],
            tool_name="test",
            description="slow",
            registry=registry,
            background_threshold=1,
            timeout=30,
        )
        assert isinstance(result, str)
        assert result.startswith(_BACKGROUNDED_PREFIX)
        assert registry.running_count == 1

        # Wait for background task to finish
        await asyncio.sleep(4)
        completed = await registry.drain_notifications()
        assert len(completed) == 1
        assert completed[0].status == "completed"
        assert "done" in completed[0].result

    asyncio.run(_test())


def test_background_message_no_task_id_leak():
    """Regression: the user-facing backgrounded message must NOT leak the
    internal task_id. A leaked task_id caused the agent to call
    check_task('<our id>') which is from subagents_pydantic_ai (a DIFFERENT
    task registry) and got 'Task not found' — then retried generate_report
    under a new title, creating a duplicate report directory.
    """

    async def _test():
        registry = BackgroundRegistry()
        result = await run_with_auto_background(
            [sys.executable, "-c", "import time; time.sleep(3); print('done')"],
            tool_name="test",
            description="slow",
            registry=registry,
            background_threshold=1,
            timeout=30,
        )
        assert isinstance(result, str)
        assert result.startswith(_BACKGROUNDED_PREFIX)

        # 1. No raw internal task_id leaked into the agent-facing message.
        #    The registry still tracks one internally (see _tasks dict below)
        #    but the agent must not see it.
        internal_ids = list(registry._tasks.keys())
        assert len(internal_ids) == 1
        assert internal_ids[0] not in result, (
            f"Internal task_id {internal_ids[0]!r} leaked into the agent-"
            f"facing message: {result!r}. The agent will try to pass this ID "
            f"to check_task() (from subagents_pydantic_ai) which has no "
            f"knowledge of it — see commit 58bcada + live verification."
        )

        # 2. The message must NOT contain the substring "task_id:" which
        #    was the exact misleading phrase.
        assert "task_id:" not in result

        # 3. The message MUST steer the agent away from calling check_task()
        #    or list_active_tasks() on this operation.
        assert "check_task" in result
        assert "Do NOT" in result or "do NOT" in result

        # 4. The message MUST tell the agent the completion arrives
        #    automatically (so it knows not to poll).
        assert "background_task_result" in result

        # Clean up
        await asyncio.sleep(4)
        await registry.drain_notifications()

    asyncio.run(_test())

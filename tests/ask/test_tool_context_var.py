"""Tests for ToolContext with contextvars.ContextVar."""

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from seeknal.ask.agents.tools._context import (
    ToolContext,
    get_tool_context,
    set_tool_context,
)


def _make_ctx(name: str = "default") -> ToolContext:
    """Create a minimal ToolContext for testing."""
    return ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=Path(f"/tmp/test-{name}"),
    )


class TestBasicAPI:
    def test_set_and_get(self):
        ctx = _make_ctx()
        set_tool_context(ctx)
        assert get_tool_context() is ctx

    def test_raises_when_not_set(self):
        """get_tool_context() raises RuntimeError in a fresh context."""
        import contextvars

        # Run in a clean context where _tool_context has no value
        def _check_in_clean_context():
            with pytest.raises(RuntimeError, match="Tool context not initialized"):
                get_tool_context()

        clean_ctx = contextvars.Context()
        clean_ctx.run(_check_in_clean_context)

    def test_set_returns_token(self):
        ctx = _make_ctx()
        token = set_tool_context(ctx)
        assert token is not None


class TestAsyncIsolation:
    def test_concurrent_tasks_get_independent_contexts(self):
        """Two concurrent asyncio tasks should have independent ToolContexts."""

        async def _run():
            ctx_a = _make_ctx("a")
            ctx_b = _make_ctx("b")

            results = {}

            async def task_a():
                set_tool_context(ctx_a)
                await asyncio.sleep(0.01)  # yield to task_b
                results["a"] = get_tool_context().project_path

            async def task_b():
                set_tool_context(ctx_b)
                await asyncio.sleep(0.01)  # yield to task_a
                results["b"] = get_tool_context().project_path

            await asyncio.gather(task_a(), task_b())

            assert results["a"] == Path("/tmp/test-a")
            assert results["b"] == Path("/tmp/test-b")

        asyncio.run(_run())

    def test_child_task_inherits_parent_context(self):
        """asyncio.create_task() should inherit context from parent."""

        async def _run():
            ctx = _make_ctx("parent")
            set_tool_context(ctx)

            result = [None]

            async def child():
                result[0] = get_tool_context().project_path

            await asyncio.create_task(child())
            assert result[0] == Path("/tmp/test-parent")

        asyncio.run(_run())

    def test_child_task_changes_dont_affect_parent(self):
        """Child task's set_tool_context should not affect parent."""

        async def _run():
            parent_ctx = _make_ctx("parent")
            child_ctx = _make_ctx("child")
            set_tool_context(parent_ctx)

            async def child():
                set_tool_context(child_ctx)

            await asyncio.create_task(child())
            # Parent context should be unchanged
            assert get_tool_context().project_path == Path("/tmp/test-parent")

        asyncio.run(_run())


class TestThreadSafety:
    def test_context_available_in_thread(self):
        """asyncio.to_thread copies context to the thread."""

        async def _run():
            ctx = _make_ctx("thread-test")
            set_tool_context(ctx)

            def sync_check():
                return get_tool_context().project_path

            result = await asyncio.to_thread(sync_check)
            assert result == Path("/tmp/test-thread-test")

        asyncio.run(_run())

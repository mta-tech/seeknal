"""Tests for Temporal workflow/activity dataclasses and RedisSink JSON format."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from seeknal.ask.event_sink import EventSink


# ---------------------------------------------------------------------------
# AgentTurnParams / AgentTurnResult dataclass tests
# ---------------------------------------------------------------------------


class TestAgentTurnParams:
    def test_defaults(self):
        from seeknal.ask.gateway.temporal.workflows import AgentTurnParams

        params = AgentTurnParams(
            question="What is 2+2?",
            project_path="/tmp/project",
            session_name="test-session",
        )
        assert params.question == "What is 2+2?"
        assert params.project_path == "/tmp/project"
        assert params.session_name == "test-session"
        assert params.config_overrides == {}

    def test_with_config_overrides(self):
        from seeknal.ask.gateway.temporal.workflows import AgentTurnParams

        params = AgentTurnParams(
            question="Build a pipeline",
            project_path="/my/project",
            session_name="s1",
            config_overrides={"model": "gemini-2.5-pro"},
        )
        assert params.config_overrides == {"model": "gemini-2.5-pro"}


class TestAgentTurnResult:
    def test_success_result(self):
        from seeknal.ask.gateway.temporal.workflows import AgentTurnResult

        result = AgentTurnResult(
            response_text="The answer is 4",
            tool_calls_count=2,
            success=True,
        )
        assert result.response_text == "The answer is 4"
        assert result.tool_calls_count == 2
        assert result.success is True

    def test_failure_result(self):
        from seeknal.ask.gateway.temporal.workflows import AgentTurnResult

        result = AgentTurnResult(
            response_text="Error: something went wrong",
            tool_calls_count=0,
            success=False,
        )
        assert result.success is False


# ---------------------------------------------------------------------------
# RedisSink tests
# ---------------------------------------------------------------------------


class TestRedisSink:
    def test_is_event_sink(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        sink = RedisSink.__new__(RedisSink)
        assert isinstance(sink, EventSink)

    def test_on_token_publishes_json(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        async def _run():
            sink = RedisSink("test-session")
            mock_client = MagicMock()
            sink._client = mock_client

            await sink.on_token("Hello")

            mock_client.publish.assert_called_once()
            channel, data = mock_client.publish.call_args[0]
            assert channel == "session:test-session"
            parsed = json.loads(data)
            assert parsed == {"type": "token", "data": "Hello"}

        asyncio.run(_run())

    def test_on_tool_start_publishes_json(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        async def _run():
            sink = RedisSink("s1")
            mock_client = MagicMock()
            sink._client = mock_client

            await sink.on_tool_start("execute_sql", {"sql": "SELECT 1"})

            channel, data = mock_client.publish.call_args[0]
            assert channel == "session:s1"
            parsed = json.loads(data)
            assert parsed["type"] == "tool_start"
            assert parsed["data"]["name"] == "execute_sql"
            assert parsed["data"]["args"] == {"sql": "SELECT 1"}

        asyncio.run(_run())

    def test_on_tool_end_publishes_json(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        async def _run():
            sink = RedisSink("s1")
            mock_client = MagicMock()
            sink._client = mock_client

            await sink.on_tool_end("execute_sql", "| 1 |")

            channel, data = mock_client.publish.call_args[0]
            parsed = json.loads(data)
            assert parsed == {
                "type": "tool_end",
                "data": {"name": "execute_sql", "result": "| 1 |"},
            }

        asyncio.run(_run())

    def test_on_answer_publishes_json(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        async def _run():
            sink = RedisSink("s1")
            mock_client = MagicMock()
            sink._client = mock_client

            await sink.on_answer("The answer is 42")

            channel, data = mock_client.publish.call_args[0]
            parsed = json.loads(data)
            assert parsed == {"type": "answer", "data": "The answer is 42"}

        asyncio.run(_run())

    def test_on_error_publishes_json(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        async def _run():
            sink = RedisSink("s1")
            mock_client = MagicMock()
            sink._client = mock_client

            await sink.on_error("Something failed")

            channel, data = mock_client.publish.call_args[0]
            parsed = json.loads(data)
            assert parsed == {"type": "error", "data": "Something failed"}

        asyncio.run(_run())

    def test_publish_failure_does_not_raise(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        async def _run():
            sink = RedisSink("s1")
            mock_client = MagicMock()
            mock_client.publish.side_effect = ConnectionError("Redis down")
            sink._client = mock_client

            # Should not raise
            await sink.on_token("test")

        asyncio.run(_run())

    def test_channel_name_format(self):
        from seeknal.ask.gateway.temporal.activities import RedisSink

        sink = RedisSink("my-session-123")
        assert sink._channel == "session:my-session-123"

    def test_lazy_redis_import(self):
        """RedisSink should not import redis at construction time."""
        from seeknal.ask.gateway.temporal.activities import RedisSink

        sink = RedisSink("s1", redis_url="redis://fake:6379")
        assert sink._client is None


# ---------------------------------------------------------------------------
# Workflow class structure tests (mocked temporalio)
# ---------------------------------------------------------------------------


class TestWorkflowDefinition:
    def test_workflow_class_has_expected_methods(self):
        """Verify the workflow class defines the expected signal/query methods.

        We mock temporalio decorators to avoid needing the SDK installed,
        then verify the class structure.
        """
        # Create mock decorators that act as pass-throughs
        mock_workflow = MagicMock()
        mock_workflow.defn = lambda name=None, **kw: lambda cls: cls
        mock_workflow.run = lambda fn: fn
        mock_workflow.signal = lambda fn: fn
        mock_workflow.query = lambda fn: fn
        mock_workflow.execute_activity = MagicMock()
        mock_workflow.wait_condition = MagicMock()
        mock_workflow.unsafe = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__enter__ = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__exit__ = MagicMock()

        import sys
        mock_temporalio = MagicMock()
        mock_temporalio.workflow = mock_workflow

        with patch.dict(sys.modules, {"temporalio": mock_temporalio, "temporalio.workflow": mock_workflow}):
            # Force reimport
            import importlib
            from seeknal.ask.gateway.temporal import workflows
            importlib.reload(workflows)

            wf_class = workflows.get_workflow_class()
            instance = wf_class.__new__(wf_class)
            instance.__init__()

            assert hasattr(instance, "run")
            assert hasattr(instance, "user_message")
            assert hasattr(instance, "shutdown")
            assert hasattr(instance, "get_status")
            assert instance._status == "initialized"
            assert instance._pending_messages == []
            assert instance._shutdown is False

    def test_user_message_signal_queues_text(self):
        """Verify user_message adds to pending list."""
        mock_workflow = MagicMock()
        mock_workflow.defn = lambda name=None, **kw: lambda cls: cls
        mock_workflow.run = lambda fn: fn
        mock_workflow.signal = lambda fn: fn
        mock_workflow.query = lambda fn: fn
        mock_workflow.unsafe = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__enter__ = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__exit__ = MagicMock()

        import sys
        with patch.dict(sys.modules, {"temporalio": MagicMock(workflow=mock_workflow), "temporalio.workflow": mock_workflow}):
            import importlib
            from seeknal.ask.gateway.temporal import workflows
            importlib.reload(workflows)

            wf_class = workflows.get_workflow_class()
            instance = wf_class.__new__(wf_class)
            instance.__init__()

            instance.user_message("Hello")
            instance.user_message("World")
            assert instance._pending_messages == ["Hello", "World"]

    def test_shutdown_signal_sets_flag(self):
        """Verify shutdown sets the flag."""
        mock_workflow = MagicMock()
        mock_workflow.defn = lambda name=None, **kw: lambda cls: cls
        mock_workflow.run = lambda fn: fn
        mock_workflow.signal = lambda fn: fn
        mock_workflow.query = lambda fn: fn
        mock_workflow.unsafe = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__enter__ = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__exit__ = MagicMock()

        import sys
        with patch.dict(sys.modules, {"temporalio": MagicMock(workflow=mock_workflow), "temporalio.workflow": mock_workflow}):
            import importlib
            from seeknal.ask.gateway.temporal import workflows
            importlib.reload(workflows)

            wf_class = workflows.get_workflow_class()
            instance = wf_class.__new__(wf_class)
            instance.__init__()

            assert instance._shutdown is False
            instance.shutdown()
            assert instance._shutdown is True

    def test_get_status_query(self):
        """Verify get_status returns the current status."""
        mock_workflow = MagicMock()
        mock_workflow.defn = lambda name=None, **kw: lambda cls: cls
        mock_workflow.run = lambda fn: fn
        mock_workflow.signal = lambda fn: fn
        mock_workflow.query = lambda fn: fn
        mock_workflow.unsafe = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__enter__ = MagicMock()
        mock_workflow.unsafe.imports_passed_through.return_value.__exit__ = MagicMock()

        import sys
        with patch.dict(sys.modules, {"temporalio": MagicMock(workflow=mock_workflow), "temporalio.workflow": mock_workflow}):
            import importlib
            from seeknal.ask.gateway.temporal import workflows
            importlib.reload(workflows)

            wf_class = workflows.get_workflow_class()
            instance = wf_class.__new__(wf_class)
            instance.__init__()

            assert instance.get_status() == "initialized"
            instance._status = "running"
            assert instance.get_status() == "running"


# ---------------------------------------------------------------------------
# Task queue constant
# ---------------------------------------------------------------------------


class TestConstants:
    def test_task_queue_name(self):
        from seeknal.ask.gateway.temporal.workflows import TASK_QUEUE

        assert TASK_QUEUE == "seeknal-ask"

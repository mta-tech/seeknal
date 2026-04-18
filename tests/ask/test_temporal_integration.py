"""Integration tests for Temporal gateway integration.

Tests the Temporal workflow/activity definitions, SSE broadcaster
wiring, session locking, and the /temporal/start endpoint.

Tests marked ``@pytest.mark.integration`` require a running Temporal
dev server (``temporal server start-dev``).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seeknal.ask.gateway.server import (
    _get_session_lock,
    _publish_event,
    _run_agent_streaming,
    create_gateway_app,
    sse_broadcaster,
)
from seeknal.ask.gateway.sse import SSEBroadcaster
from seeknal.ask.gateway.temporal import (
    TEMPORAL_AVAILABLE,
    AgentWorkflowInput,
    AgentWorkflowOutput,
    _require_temporal,
)


# ---------------------------------------------------------------------------
# Unit tests (no Temporal server required)
# ---------------------------------------------------------------------------


class TestTemporalModuleScaffold:
    """Test the optional dependency scaffold."""

    def test_dataclass_serialization(self):
        inp = AgentWorkflowInput(
            session_id="test-123",
            question="What is 2+2?",
            project_path="/tmp/project",
        )
        d = asdict(inp)
        assert d["session_id"] == "test-123"
        assert d["provider"] is None
        assert json.dumps(d)  # JSON-serializable

        out = AgentWorkflowOutput(answer="4", event_count=5)
        d2 = asdict(out)
        assert d2["answer"] == "4"
        assert d2["error"] is None
        assert json.dumps(d2)

    def test_require_temporal_when_unavailable(self):
        with patch("seeknal.ask.gateway.temporal.TEMPORAL_AVAILABLE", False):
            with pytest.raises(ImportError, match="pip install seeknal"):
                _require_temporal()


class TestSSEBroadcaster:
    """Test the SSE broadcaster pub/sub."""

    def test_subscribe_and_publish_sync(self):
        b = SSEBroadcaster(maxsize=10)
        q = b.subscribe("session-1")
        b.publish_sync("session-1", '{"type": "token", "data": "hi"}')
        assert not q.empty()
        assert q.get_nowait() == '{"type": "token", "data": "hi"}'

    def test_publish_to_empty_session(self):
        b = SSEBroadcaster()
        # Should not raise — just a no-op dict lookup
        b.publish_sync("nonexistent", '{"type": "token"}')

    def test_multiple_subscribers(self):
        b = SSEBroadcaster(maxsize=10)
        q1 = b.subscribe("session-1")
        q2 = b.subscribe("session-1")
        b.publish_sync("session-1", '{"type": "done"}')
        assert q1.get_nowait() == '{"type": "done"}'
        assert q2.get_nowait() == '{"type": "done"}'

    def test_unsubscribe(self):
        b = SSEBroadcaster(maxsize=10)
        q = b.subscribe("session-1")
        b.unsubscribe("session-1", q)
        b.publish_sync("session-1", '{"type": "token"}')
        assert q.empty()

    def test_queue_full_drops_event(self):
        b = SSEBroadcaster(maxsize=1)
        q = b.subscribe("session-1")
        b.publish_sync("session-1", "event1")
        b.publish_sync("session-1", "event2")  # Should be dropped
        assert q.get_nowait() == "event1"
        assert q.empty()


class TestSessionLocking:
    """Test per-session lock mechanics."""

    def test_get_session_lock_returns_same_lock(self):
        lock1 = _get_session_lock("test-lock")
        lock2 = _get_session_lock("test-lock")
        assert lock1 is lock2

    def test_different_sessions_get_different_locks(self):
        lock1 = _get_session_lock("session-a")
        lock2 = _get_session_lock("session-b")
        assert lock1 is not lock2


class TestPublishEvent:
    """Test the _publish_event helper."""

    def test_publish_event_serializes_to_json(self):
        b = SSEBroadcaster(maxsize=10)
        q = b.subscribe("test-pub")

        # Temporarily replace the module-level broadcaster
        import seeknal.ask.gateway.server as srv
        original = srv.sse_broadcaster
        srv.sse_broadcaster = b
        try:
            _publish_event("test-pub", {"type": "token", "data": "hello"})
            event = q.get_nowait()
            parsed = json.loads(event)
            assert parsed["type"] == "token"
            assert parsed["data"] == "hello"
        finally:
            srv.sse_broadcaster = original


class TestGatewayApp:
    """Test the gateway app factory."""

    def test_create_app_without_temporal(self):
        app = create_gateway_app("/tmp/test-project")
        assert not hasattr(app.state, "temporal_client")

    def test_create_app_with_temporal_client(self):
        mock_client = MagicMock()
        app = create_gateway_app("/tmp/test-project", temporal_client=mock_client)
        assert app.state.temporal_client is mock_client

    def test_temporal_start_returns_503_without_client(self):
        """POST /temporal/start should return 503 when Temporal is not enabled."""
        from starlette.testclient import TestClient

        app = create_gateway_app("/tmp/test-project")
        client = TestClient(app)
        resp = client.post(
            "/temporal/start",
            json={"question": "test", "session_id": "test-1"},
        )
        assert resp.status_code == 503
        assert "not enabled" in resp.json()["error"].lower()


@pytest.mark.asyncio
class TestRunAgentStreamingSSE:
    """Test that _run_agent_streaming publishes to SSE and yields done/error events."""

    async def test_events_published_to_sse(self, tmp_path):
        """Verify SSE broadcaster receives events from _run_agent_streaming."""
        import seeknal.ask.gateway.server as srv

        test_broadcaster = SSEBroadcaster(maxsize=50)
        original = srv.sse_broadcaster
        srv.sse_broadcaster = test_broadcaster

        q = test_broadcaster.subscribe("sse-test")

        # Mock the inner agent to yield a simple token + answer
        async def mock_inner(*args, **kwargs):
            yield {"type": "token", "data": "Hello"}
            yield {"type": "answer", "data": "Hello world"}

        try:
            with patch.object(srv, "_run_agent_inner", mock_inner):
                events = []
                async for event in _run_agent_streaming(
                    tmp_path, "sse-test", "test question",
                ):
                    events.append(event)

            # Check yielded events include done
            event_types = [e["type"] for e in events]
            assert "token" in event_types
            assert "answer" in event_types
            assert "done" in event_types

            # Check SSE broadcaster received the same events
            sse_events = []
            while not q.empty():
                sse_events.append(json.loads(q.get_nowait()))
            sse_types = [e["type"] for e in sse_events]
            assert "token" in sse_types
            assert "answer" in sse_types
            assert "done" in sse_types

        finally:
            srv.sse_broadcaster = original

    async def test_error_event_on_exception(self, tmp_path):
        """Verify error + done events are published when agent fails."""
        import seeknal.ask.gateway.server as srv

        test_broadcaster = SSEBroadcaster(maxsize=50)
        original = srv.sse_broadcaster
        srv.sse_broadcaster = test_broadcaster

        q = test_broadcaster.subscribe("err-test")

        async def mock_inner_fail(*args, **kwargs):
            yield {"type": "token", "data": "partial"}
            raise RuntimeError("LLM exploded")

        try:
            with patch.object(srv, "_run_agent_inner", mock_inner_fail):
                events = []
                with pytest.raises(RuntimeError, match="LLM exploded"):
                    async for event in _run_agent_streaming(
                        tmp_path, "err-test", "test",
                    ):
                        events.append(event)

            event_types = [e["type"] for e in events]
            assert "error" in event_types
            assert "done" in event_types

            # SSE broadcaster also got the error
            sse_events = []
            while not q.empty():
                sse_events.append(json.loads(q.get_nowait()))
            sse_types = [e["type"] for e in sse_events]
            assert "error" in sse_types
            assert "done" in sse_types

        finally:
            srv.sse_broadcaster = original


# ---------------------------------------------------------------------------
# Temporal-specific tests (require temporalio installed)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not TEMPORAL_AVAILABLE, reason="temporalio not installed")
class TestTemporalActivity:
    """Test the Temporal activity definition (without a running server)."""

    def test_activity_is_defined(self):
        from seeknal.ask.gateway.temporal import run_agent_activity

        assert callable(run_agent_activity)

    def test_workflow_is_defined(self):
        from seeknal.ask.gateway.temporal import AgentWorkflow

        assert hasattr(AgentWorkflow, "run")
        assert hasattr(AgentWorkflow, "get_status")
        assert hasattr(AgentWorkflow, "cancel_run")


@pytest.mark.skipif(not TEMPORAL_AVAILABLE, reason="temporalio not installed")
class TestTemporalWorkerFactory:
    """Test worker and client factory functions."""

    def test_create_worker(self):
        # Newer temporalio versions reject non-bridge clients in the Worker
        # constructor, so patch Worker to capture the factory's argument
        # wiring without constructing a real Worker.
        from unittest.mock import patch

        from seeknal.ask.gateway.temporal import (
            AgentWorkflow,
            create_temporal_worker,
            run_agent_activity,
        )

        mock_client = MagicMock()
        with patch(
            "seeknal.ask.gateway.temporal.Worker"
        ) as mock_worker_cls:
            mock_worker_cls.return_value = MagicMock(name="worker")
            worker = create_temporal_worker(mock_client, task_queue="test-queue")

        assert worker is not None
        mock_worker_cls.assert_called_once()
        _, kwargs = mock_worker_cls.call_args
        assert kwargs["task_queue"] == "test-queue"
        assert AgentWorkflow in kwargs["workflows"]
        assert run_agent_activity in kwargs["activities"]

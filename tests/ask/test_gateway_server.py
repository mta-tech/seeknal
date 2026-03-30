"""Tests for seeknal ask gateway — server, session manager, and WebSocketSink."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seeknal.ask.event_sink import EventSink
from seeknal.ask.gateway.session_manager import SessionManager

# Starlette is an optional dependency (seeknal[gateway]).
# Tests that need it are skipped when it's not installed.
starlette = pytest.importorskip("starlette", reason="requires seeknal[gateway]")


# ---------------------------------------------------------------------------
# SessionManager tests
# ---------------------------------------------------------------------------


class TestSessionManager:
    def test_connect_adds_websocket(self):
        mgr = SessionManager()
        ws = MagicMock()
        mgr.connect("session-1", ws)
        assert mgr.connection_count("session-1") == 1

    def test_connect_multiple_to_same_session(self):
        mgr = SessionManager()
        ws1, ws2 = MagicMock(), MagicMock()
        mgr.connect("session-1", ws1)
        mgr.connect("session-1", ws2)
        assert mgr.connection_count("session-1") == 2

    def test_disconnect_removes_websocket(self):
        mgr = SessionManager()
        ws = MagicMock()
        mgr.connect("session-1", ws)
        mgr.disconnect("session-1", ws)
        assert mgr.connection_count("session-1") == 0

    def test_disconnect_cleans_up_empty_session(self):
        mgr = SessionManager()
        ws = MagicMock()
        mgr.connect("session-1", ws)
        mgr.disconnect("session-1", ws)
        assert "session-1" not in mgr._connections

    def test_disconnect_nonexistent_session_is_noop(self):
        mgr = SessionManager()
        ws = MagicMock()
        mgr.disconnect("nonexistent", ws)  # should not raise

    def test_active_sessions(self):
        mgr = SessionManager()
        mgr.connect("a", MagicMock())
        mgr.connect("b", MagicMock())
        assert sorted(mgr.active_sessions()) == ["a", "b"]

    def test_active_sessions_empty(self):
        mgr = SessionManager()
        assert mgr.active_sessions() == []

    def test_send_to_session_broadcasts(self):
        async def _run():
            mgr = SessionManager()
            ws1 = AsyncMock()
            ws2 = AsyncMock()
            mgr.connect("session-1", ws1)
            mgr.connect("session-1", ws2)
            await mgr.send_to_session("session-1", {"type": "token", "data": "hi"})
            ws1.send_json.assert_awaited_once_with({"type": "token", "data": "hi"})
            ws2.send_json.assert_awaited_once_with({"type": "token", "data": "hi"})

        asyncio.run(_run())

    def test_send_to_nonexistent_session_is_noop(self):
        async def _run():
            mgr = SessionManager()
            await mgr.send_to_session("nonexistent", {"data": "test"})

        asyncio.run(_run())

    def test_send_ignores_closed_connections(self):
        async def _run():
            mgr = SessionManager()
            ws = AsyncMock()
            ws.send_json.side_effect = RuntimeError("connection closed")
            mgr.connect("s1", ws)
            # Should not raise
            await mgr.send_to_session("s1", {"data": "test"})

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# WebSocketSink tests
# ---------------------------------------------------------------------------


class TestWebSocketSink:
    def test_is_event_sink(self):
        from seeknal.ask.gateway.server import WebSocketSink

        ws = MagicMock()
        sink = WebSocketSink(ws)
        assert isinstance(sink, EventSink)

    def test_on_token(self):
        async def _run():
            from seeknal.ask.gateway.server import WebSocketSink

            ws = AsyncMock()
            sink = WebSocketSink(ws)
            await sink.on_token("Hello")
            ws.send_json.assert_awaited_once_with(
                {"type": "token", "data": "Hello"}
            )

        asyncio.run(_run())

    def test_on_tool_start(self):
        async def _run():
            from seeknal.ask.gateway.server import WebSocketSink

            ws = AsyncMock()
            sink = WebSocketSink(ws)
            await sink.on_tool_start("execute_sql", {"sql": "SELECT 1"})
            ws.send_json.assert_awaited_once_with({
                "type": "tool_start",
                "data": {"name": "execute_sql", "args": {"sql": "SELECT 1"}},
            })

        asyncio.run(_run())

    def test_on_tool_end(self):
        async def _run():
            from seeknal.ask.gateway.server import WebSocketSink

            ws = AsyncMock()
            sink = WebSocketSink(ws)
            await sink.on_tool_end("execute_sql", "| 1 |")
            ws.send_json.assert_awaited_once_with({
                "type": "tool_end",
                "data": {"name": "execute_sql", "result": "| 1 |"},
            })

        asyncio.run(_run())

    def test_on_answer(self):
        async def _run():
            from seeknal.ask.gateway.server import WebSocketSink

            ws = AsyncMock()
            sink = WebSocketSink(ws)
            await sink.on_answer("The answer is 42")
            ws.send_json.assert_awaited_once_with(
                {"type": "answer", "data": "The answer is 42"}
            )

        asyncio.run(_run())

    def test_on_error(self):
        async def _run():
            from seeknal.ask.gateway.server import WebSocketSink

            ws = AsyncMock()
            sink = WebSocketSink(ws)
            await sink.on_error("Something went wrong")
            ws.send_json.assert_awaited_once_with(
                {"type": "error", "data": "Something went wrong"}
            )

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Health endpoint test
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_health_returns_ok(self, tmp_path):
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# WebSocket endpoint tests
# ---------------------------------------------------------------------------


class TestWebSocketEndpoint:
    def test_ws_accepts_connection(self, tmp_path):
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        with client.websocket_connect("/ws/test-session") as ws:
            # Connection accepted — send a ping-style message to verify
            ws.send_json({"type": "message", "text": "hello"})
            # We expect either an answer or an error (since agent isn't
            # fully wired in tests), but the connection itself should work.
            resp = ws.receive_json()
            assert "type" in resp

    def test_ws_rejects_invalid_json(self, tmp_path):
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        with client.websocket_connect("/ws/test-session") as ws:
            ws.send_text("not valid json")
            resp = ws.receive_json()
            assert resp["type"] == "error"
            assert "Invalid JSON" in resp["data"]

    def test_ws_rejects_unknown_message_type(self, tmp_path):
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        with client.websocket_connect("/ws/test-session") as ws:
            ws.send_json({"type": "unknown", "text": "hello"})
            resp = ws.receive_json()
            assert resp["type"] == "error"
            assert "Unknown message type" in resp["data"]

    def test_ws_rejects_empty_text(self, tmp_path):
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        with client.websocket_connect("/ws/test-session") as ws:
            ws.send_json({"type": "message", "text": ""})
            resp = ws.receive_json()
            assert resp["type"] == "error"
            assert "Empty message" in resp["data"]

    def test_ws_sends_answer_on_agent_success(self, tmp_path):
        """Mock the agent to return a known answer, verify WS sends it."""
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)

        mock_agent = MagicMock()
        mock_config = {"configurable": {"thread_id": "test"}}

        with patch(
            "seeknal.ask.agents.agent.create_agent",
            return_value=(mock_agent, mock_config),
        ), patch(
            "seeknal.ask.agents.agent.ask",
            return_value="Mocked answer",
        ):
            client = TestClient(app)
            with client.websocket_connect("/ws/test-session") as ws:
                ws.send_json({"type": "message", "text": "What is 2+2?"})
                resp = ws.receive_json()
                assert resp["type"] == "answer"
                assert resp["data"] == "Mocked answer"

    def test_ws_sends_error_on_agent_failure(self, tmp_path):
        """When agent raises, WS sends an error event."""
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)

        with patch(
            "seeknal.ask.agents.agent.create_agent",
            side_effect=RuntimeError("Agent setup failed"),
        ):
            client = TestClient(app)
            with client.websocket_connect("/ws/test-session") as ws:
                ws.send_json({"type": "message", "text": "hello"})
                resp = ws.receive_json()
                assert resp["type"] == "error"
                assert "internal error" in resp["data"].lower()


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------


class TestGatewayConfig:
    def test_defaults(self):
        from seeknal.ask.config import get_gateway_config

        cfg = get_gateway_config({})
        assert cfg["host"] == "127.0.0.1"
        assert cfg["port"] == 18789

    def test_override_host(self):
        from seeknal.ask.config import get_gateway_config

        cfg = get_gateway_config({"gateway": {"host": "0.0.0.0"}})
        assert cfg["host"] == "0.0.0.0"
        assert cfg["port"] == 18789  # default preserved

    def test_override_port(self):
        from seeknal.ask.config import get_gateway_config

        cfg = get_gateway_config({"gateway": {"port": 9999}})
        assert cfg["port"] == 9999

    def test_invalid_gateway_section_uses_defaults(self):
        from seeknal.ask.config import get_gateway_config

        cfg = get_gateway_config({"gateway": "invalid"})
        assert cfg["host"] == "127.0.0.1"
        assert cfg["port"] == 18789


# ---------------------------------------------------------------------------
# Session ID validation tests
# ---------------------------------------------------------------------------


class TestSessionIdValidation:
    def test_valid_session_ids(self):
        from seeknal.ask.gateway.server import _validate_session_id

        assert _validate_session_id("test-session") is True
        assert _validate_session_id("calm-river-315") is True
        assert _validate_session_id("session_1") is True
        assert _validate_session_id("abc123") is True

    def test_rejects_path_traversal(self):
        from seeknal.ask.gateway.server import _validate_session_id

        assert _validate_session_id("../../../etc/passwd") is False
        assert _validate_session_id("session/../../etc") is False

    def test_rejects_special_characters(self):
        from seeknal.ask.gateway.server import _validate_session_id

        assert _validate_session_id("session id") is False
        assert _validate_session_id("session;rm -rf") is False
        assert _validate_session_id("") is False

    def test_rejects_too_long(self):
        from seeknal.ask.gateway.server import _validate_session_id

        assert _validate_session_id("a" * 129) is False
        assert _validate_session_id("a" * 128) is True

    def test_ws_rejects_invalid_session_id(self, tmp_path):
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        # Path traversal session_id should be rejected
        with pytest.raises(Exception):
            # WebSocket close before accept raises in test client
            with client.websocket_connect("/ws/../../../etc/passwd"):
                pass

    def test_sse_rejects_invalid_session_id(self, tmp_path):
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        response = client.get("/events/bad session id!")
        assert response.status_code == 400
        assert "Invalid session_id" in response.json()["error"]


# ---------------------------------------------------------------------------
# Agent timeout tests
# ---------------------------------------------------------------------------


class TestAgentTimeout:
    def test_ws_sends_error_on_agent_timeout(self, tmp_path):
        """When agent hangs past timeout, WS sends a timeout error.

        Verifies that:
        1. The mock agent was actually called (agent started)
        2. The response arrived fast (timeout cut it short, not 10s sleep)
        3. The error message indicates timeout
        """
        import time as time_mod

        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)

        mock_agent = MagicMock()
        mock_config = {"configurable": {"thread_id": "test"}}
        mock_ask = MagicMock(side_effect=lambda *a, **kw: time_mod.sleep(1))

        with patch(
            "seeknal.ask.agents.agent.create_agent",
            return_value=(mock_agent, mock_config),
        ), patch(
            "seeknal.ask.agents.agent.ask",
            mock_ask,
        ), patch(
            "seeknal.ask.gateway.server._AGENT_TIMEOUT", 0.1,  # 100ms for test
        ):
            client = TestClient(app)
            start = time_mod.monotonic()
            with client.websocket_connect("/ws/test-session") as ws:
                ws.send_json({"type": "message", "text": "hello"})
                resp = ws.receive_json()
            elapsed = time_mod.monotonic() - start

            assert resp["type"] == "error"
            assert "timed out" in resp["data"]
            mock_ask.assert_called_once()  # Agent was started
            assert elapsed < 3.0  # Timeout fired (0.1s), orphaned thread exits after 1s


# ---------------------------------------------------------------------------
# SSE queue overflow visibility tests
# ---------------------------------------------------------------------------


class TestSSEQueueOverflow:
    def test_first_drop_logs_warning(self, caplog):
        """First dropped event should log at WARNING level."""
        import logging

        from seeknal.ask.gateway.sse import SSEBroadcaster

        broadcaster = SSEBroadcaster()
        queue = broadcaster.subscribe("s1")

        # Fill the queue (maxsize=100)
        for i in range(100):
            broadcaster.publish("s1", {"type": "token", "data": str(i)})

        with caplog.at_level(logging.WARNING, logger="seeknal.ask.gateway.sse"):
            # This event should be dropped and trigger WARNING
            broadcaster.publish("s1", {"type": "token", "data": "overflow"})

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_records) == 1
        assert "dropped 1 event" in warning_records[0].getMessage()

    def test_drop_count_resets_on_full_unsubscribe(self):
        """Drop counter should be cleaned up when session has no subscribers."""
        from seeknal.ask.gateway.sse import SSEBroadcaster

        broadcaster = SSEBroadcaster()
        queue = broadcaster.subscribe("s1")

        # Fill and overflow
        for i in range(101):
            broadcaster.publish("s1", {"type": "token", "data": str(i)})

        assert "s1" in broadcaster._drop_counts

        broadcaster.unsubscribe("s1", queue)

        assert "s1" not in broadcaster._drop_counts


# ---------------------------------------------------------------------------
# WebSocket message size limit tests
# ---------------------------------------------------------------------------


class TestWebSocketMessageSizeLimit:
    def test_ws_rejects_oversized_message(self, tmp_path):
        """Messages exceeding the size limit should be rejected."""
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)

        with patch("seeknal.ask.gateway.server._MAX_WS_MESSAGE_SIZE", 100):
            with client.websocket_connect("/ws/test-session") as ws:
                # Send a message larger than 100 bytes
                ws.send_json({"type": "message", "text": "x" * 200})
                resp = ws.receive_json()
                assert resp["type"] == "error"
                assert "too large" in resp["data"].lower()

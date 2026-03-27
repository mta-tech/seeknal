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
                assert "Agent setup failed" in resp["data"]


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

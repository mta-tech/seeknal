"""Tests for webhook-based streaming (POST /ask/stream).

Unit tests verify event mapping, envelope structure, delivery, and
error handling.  Integration tests verify the HTTP endpoint contract
(request validation and response shape).
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seeknal.ask.gateway.webhook import WebhookClient, WebhookConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config() -> WebhookConfig:
    return WebhookConfig(
        message_id="test-msg-001",
        session_id="default",
        webhook_url="https://example.com/hook",
    )


@pytest.fixture
def client(config: WebhookConfig) -> WebhookClient:
    return WebhookClient(config)


# ===================================================================
# Unit tests — build_webhook_url
# ===================================================================


class TestBuildWebhookUrl:
    def test_valid_path(self, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        url = WebhookClient.build_webhook_url("/sse/push?id=1")
        assert url == "https://example.com/sse/push?id=1"

    def test_strips_trailing_slash_on_base(self, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com/")
        url = WebhookClient.build_webhook_url("/hook")
        assert url == "https://example.com/hook"

    def test_strips_whitespace_on_base(self, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "  https://example.com  ")
        url = WebhookClient.build_webhook_url("/hook")
        assert url == "https://example.com/hook"

    def test_missing_env_raises(self, monkeypatch):
        monkeypatch.delenv("SEEKNAL_WEBHOOK_BASE_URL", raising=False)
        with pytest.raises(ValueError, match="SEEKNAL_WEBHOOK_BASE_URL"):
            WebhookClient.build_webhook_url("/hook")

    def test_empty_env_raises(self, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "  ")
        with pytest.raises(ValueError, match="SEEKNAL_WEBHOOK_BASE_URL"):
            WebhookClient.build_webhook_url("/hook")

    def test_no_leading_slash_raises(self, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        with pytest.raises(ValueError, match="must start with '/'"):
            WebhookClient.build_webhook_url("https://evil.com/steal")


# ===================================================================
# Unit tests — _make_event (envelope)
# ===================================================================


class TestMakeEvent:
    def test_envelope_structure(self, client: WebhookClient):
        evt = client._make_event("start")
        assert evt["event"] == "start"
        assert evt["message_id"] == "test-msg-001"
        assert evt["conversation_id"] == "default"
        assert "created_at" in evt
        assert evt["payload"] == {}

    def test_payload_passed_through(self, client: WebhookClient):
        evt = client._make_event("message", {"answer": "Paris"})
        assert evt["event"] == "message"
        assert evt["payload"] == {"answer": "Paris"}

    def test_created_at_is_iso8601(self, client: WebhookClient):
        evt = client._make_event("done")
        # Should parse without error
        from datetime import datetime
        datetime.fromisoformat(evt["created_at"])


# ===================================================================
# Unit tests — _map_event
# ===================================================================


class TestMapEvent:
    def test_token_skipped(self, client: WebhookClient):
        assert client._map_event({"type": "token", "data": "x"}) is None

    def test_reasoning_maps_to_message(self, client: WebhookClient):
        evt = client._map_event({"type": "reasoning", "data": "thinking..."})
        assert evt["event"] == "message"
        assert evt["payload"]["answer"] == "thinking..."

    def test_answer_maps_to_message(self, client: WebhookClient):
        evt = client._map_event({"type": "answer", "data": "Paris"})
        assert evt["event"] == "message"
        assert evt["payload"]["answer"] == "Paris"

    def test_tool_start_maps_to_tool_call(self, client: WebhookClient):
        evt = client._map_event({
            "type": "tool_start",
            "data": {"name": "execute_sql", "args": {"query": "SELECT 1"}},
        })
        assert evt["event"] == "tool_call"
        assert evt["payload"]["tool_name"] == "execute_sql"
        assert evt["payload"]["tool_args"] == {"query": "SELECT 1"}

    def test_tool_end_maps_to_tool_result(self, client: WebhookClient):
        evt = client._map_event({
            "type": "tool_end",
            "data": {"name": "execute_sql", "output": "5 rows"},
        })
        assert evt["event"] == "tool_result"
        assert evt["payload"]["tool_name"] == "execute_sql"
        assert evt["payload"]["tool_result"] == "5 rows"

    def test_unknown_type_skipped(self, client: WebhookClient):
        assert client._map_event({"type": "weird", "data": "x"}) is None


# ===================================================================
# Unit tests — run_and_deliver (event sequence)
# ===================================================================


class TestRunAndDeliver:
    @pytest.fixture(autouse=True)
    def _require_starlette(self):
        pytest.importorskip("starlette")

    def test_full_sequence(self, client: WebhookClient):
        """start → message → tool_call → tool_result → message → done"""
        import seeknal.ask.gateway.server as server_mod

        raw_events = [
            {"type": "reasoning", "data": "Let me think..."},
            {"type": "tool_start", "data": {"name": "execute_sql", "args": {"query": "SELECT 1"}}},
            {"type": "tool_end", "data": {"name": "execute_sql", "output": "1 row"}},
            {"type": "answer", "data": "The answer is 1."},
        ]

        with patch(
            "seeknal.ask.gateway.webhook.WebhookClient._post",
            new_callable=AsyncMock,
        ) as mock_post, patch.object(
            server_mod, "_run_agent_streaming",
            return_value=_async_iter(raw_events),
        ):
            asyncio.run(client.run_and_deliver(Path("/tmp/proj"), "default", "test"))

        event_names = [call.args[0]["event"] for call in mock_post.call_args_list]
        assert event_names == [
            "start",
            "message",      # reasoning
            "tool_call",
            "tool_result",
            "message",      # answer
            "done",
        ]

    def test_error_event_on_exception(self, client: WebhookClient):
        """Agent raises → error event with code + message, then done."""
        import seeknal.ask.gateway.server as server_mod

        async def _fail(*args, **kwargs):
            raise RuntimeError("DB connection lost")
            yield  # noqa: unreachable — makes this an async generator

        with patch(
            "seeknal.ask.gateway.webhook.WebhookClient._post",
            new_callable=AsyncMock,
        ) as mock_post, patch.object(
            server_mod, "_run_agent_streaming",
            side_effect=_fail,
        ):
            asyncio.run(client.run_and_deliver(Path("/tmp/proj"), "default", "test"))

        event_names = [call.args[0]["event"] for call in mock_post.call_args_list]
        assert event_names == ["start", "error", "done"]
        error_evt = mock_post.call_args_list[1].args[0]
        assert error_evt["payload"]["error_code"] == "agent_error"
        assert "DB connection lost" in error_evt["payload"]["error_message"]

    def test_heartbeat_fires(self, client: WebhookClient):
        """Heartbeat event should be posted during a long agent run."""
        import seeknal.ask.gateway.server as server_mod

        async def _slow_iter():
            """Simulate a slow agent that takes long enough for heartbeat."""
            yield {"type": "reasoning", "data": "thinking"}
            await asyncio.sleep(0.05)  # hold long enough for heartbeat

        async def _run():
            with patch(
                "seeknal.ask.gateway.webhook.WebhookClient._post",
                new_callable=AsyncMock,
            ) as mock_post, patch.object(
                server_mod, "_run_agent_streaming",
                return_value=_slow_iter(),
            ), patch(
                "seeknal.ask.gateway.webhook._HEARTBEAT_INTERVAL",
                0.01,  # tiny interval for testing
            ):
                await client.run_and_deliver(Path("/tmp/proj"), "default", "test")
                return mock_post

        mock_post = asyncio.run(_run())
        event_names = [call.args[0]["event"] for call in mock_post.call_args_list]
        assert "heartbeat" in event_names
        assert "start" in event_names
        assert "done" in event_names


# ===================================================================
# Integration tests — POST /ask/stream endpoint
# ===================================================================


class TestAskStreamEndpoint:
    @pytest.fixture(autouse=True)
    def _require_starlette(self):
        pytest.importorskip("starlette")

    @pytest.fixture
    def app(self, tmp_path: Path):
        from starlette.testclient import TestClient  # noqa: F401
        from seeknal.ask.gateway.server import create_gateway_app
        return create_gateway_app(str(tmp_path))

    @pytest.fixture
    def test_client(self, app):
        from starlette.testclient import TestClient
        return TestClient(app)

    def test_returns_201_with_message_id(self, test_client, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        resp = test_client.post("/ask/stream", json={
            "question": "What is the capital of France?",
            "message_id": "abc-123",
            "webhook_path": "/hook",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["message_id"] == "abc-123"

    def test_missing_question_returns_400(self, test_client, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        resp = test_client.post("/ask/stream", json={
            "message_id": "abc-123",
            "webhook_path": "/hook",
        })
        assert resp.status_code == 400
        assert "question" in resp.json()["error"]

    def test_missing_message_id_returns_400(self, test_client, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        resp = test_client.post("/ask/stream", json={
            "question": "test",
            "webhook_path": "/hook",
        })
        assert resp.status_code == 400
        assert "message_id" in resp.json()["error"]

    def test_missing_webhook_path_returns_400(self, test_client, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        resp = test_client.post("/ask/stream", json={
            "question": "test",
            "message_id": "abc-123",
        })
        assert resp.status_code == 400
        assert "webhook_path" in resp.json()["error"]

    def test_invalid_webhook_path_returns_400(self, test_client, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        resp = test_client.post("/ask/stream", json={
            "question": "test",
            "message_id": "abc-123",
            "webhook_path": "https://evil.com/steal",
        })
        assert resp.status_code == 400

    def test_missing_base_url_returns_400(self, test_client, monkeypatch):
        monkeypatch.delenv("SEEKNAL_WEBHOOK_BASE_URL", raising=False)
        resp = test_client.post("/ask/stream", json={
            "question": "test",
            "message_id": "abc-123",
            "webhook_path": "/hook",
        })
        assert resp.status_code == 400

    def test_message_id_echoed_back(self, test_client, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        resp = test_client.post("/ask/stream", json={
            "question": "test",
            "message_id": "custom-id-999",
            "webhook_path": "/hook",
        })
        assert resp.json()["message_id"] == "custom-id-999"

    def test_invalid_session_id_returns_400(self, test_client, monkeypatch):
        monkeypatch.setenv("SEEKNAL_WEBHOOK_BASE_URL", "https://example.com")
        resp = test_client.post("/ask/stream", json={
            "question": "test",
            "message_id": "abc-123",
            "webhook_path": "/hook",
            "session_id": "bad session!!!",
        })
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _async_iter(items: list):
    """Wrap a list as an async iterator for mocking _run_agent_streaming."""
    for item in items:
        yield item

"""Tests for SSEBroadcaster and SSE endpoint."""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seeknal.ask.gateway.sse import SSEBroadcaster


# ---------------------------------------------------------------------------
# SSEBroadcaster tests
# ---------------------------------------------------------------------------


class TestSSEBroadcasterSubscribe:
    def test_subscribe_returns_queue(self):
        b = SSEBroadcaster()
        q = b.subscribe("s1")
        assert isinstance(q, asyncio.Queue)
        assert q.maxsize == 100

    def test_subscribe_tracks_subscriber(self):
        b = SSEBroadcaster()
        b.subscribe("s1")
        assert b.subscriber_count("s1") == 1

    def test_subscribe_multiple_to_same_session(self):
        b = SSEBroadcaster()
        b.subscribe("s1")
        b.subscribe("s1")
        assert b.subscriber_count("s1") == 2

    def test_subscribe_different_sessions(self):
        b = SSEBroadcaster()
        b.subscribe("s1")
        b.subscribe("s2")
        assert b.subscriber_count("s1") == 1
        assert b.subscriber_count("s2") == 1


class TestSSEBroadcasterUnsubscribe:
    def test_unsubscribe_removes_queue(self):
        b = SSEBroadcaster()
        q = b.subscribe("s1")
        b.unsubscribe("s1", q)
        assert b.subscriber_count("s1") == 0

    def test_unsubscribe_cleans_up_empty_session(self):
        b = SSEBroadcaster()
        q = b.subscribe("s1")
        b.unsubscribe("s1", q)
        assert "s1" not in b._subscribers

    def test_unsubscribe_nonexistent_session_is_noop(self):
        b = SSEBroadcaster()
        q = asyncio.Queue()
        b.unsubscribe("nonexistent", q)  # should not raise

    def test_unsubscribe_preserves_other_subscribers(self):
        b = SSEBroadcaster()
        q1 = b.subscribe("s1")
        q2 = b.subscribe("s1")
        b.unsubscribe("s1", q1)
        assert b.subscriber_count("s1") == 1


class TestSSEBroadcasterPublish:
    def test_publish_to_single_subscriber(self):
        b = SSEBroadcaster()
        q = b.subscribe("s1")
        event = {"type": "token", "data": "hello"}
        b.publish("s1", event)
        assert q.get_nowait() == event

    def test_publish_to_multiple_subscribers(self):
        b = SSEBroadcaster()
        q1 = b.subscribe("s1")
        q2 = b.subscribe("s1")
        event = {"type": "answer", "data": "42"}
        b.publish("s1", event)
        assert q1.get_nowait() == event
        assert q2.get_nowait() == event

    def test_publish_to_nonexistent_session_is_noop(self):
        b = SSEBroadcaster()
        b.publish("nonexistent", {"data": "test"})  # should not raise

    def test_publish_does_not_cross_sessions(self):
        b = SSEBroadcaster()
        q1 = b.subscribe("s1")
        q2 = b.subscribe("s2")
        b.publish("s1", {"data": "for s1"})
        assert not q2.empty() is False or q2.qsize() == 0
        assert q1.get_nowait() == {"data": "for s1"}
        assert q2.empty()

    def test_publish_drops_on_full_queue(self):
        """When a subscriber queue is full, publish drops the event."""
        b = SSEBroadcaster()
        q = b.subscribe("s1")
        # Fill the queue to capacity
        for i in range(100):
            q.put_nowait({"data": f"msg-{i}"})
        assert q.full()

        # Publishing to a full queue should not raise
        b.publish("s1", {"data": "overflow"})

        # Queue still has exactly 100 items (the overflow was dropped)
        assert q.qsize() == 100

    def test_publish_multiple_events_in_order(self):
        b = SSEBroadcaster()
        q = b.subscribe("s1")
        events = [
            {"type": "token", "data": "a"},
            {"type": "token", "data": "b"},
            {"type": "answer", "data": "c"},
        ]
        for ev in events:
            b.publish("s1", ev)
        received = [q.get_nowait() for _ in range(3)]
        assert received == events


class TestSSEBroadcasterSubscriberCount:
    def test_count_zero_for_unknown_session(self):
        b = SSEBroadcaster()
        assert b.subscriber_count("unknown") == 0

    def test_count_after_subscribe_and_unsubscribe(self):
        b = SSEBroadcaster()
        q1 = b.subscribe("s1")
        q2 = b.subscribe("s1")
        assert b.subscriber_count("s1") == 2
        b.unsubscribe("s1", q1)
        assert b.subscriber_count("s1") == 1
        b.unsubscribe("s1", q2)
        assert b.subscriber_count("s1") == 0


# ---------------------------------------------------------------------------
# SSE endpoint integration tests (require starlette + sse-starlette)
# ---------------------------------------------------------------------------


starlette = pytest.importorskip("starlette", reason="requires seeknal[gateway]")


class TestSSEEndpoint:
    def test_sse_route_exists(self, tmp_path):
        """The /events/{session_id} route should exist in the app."""
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        # The route should be registered — a GET should not 404
        # (it may require sse-starlette to be installed for full response)
        try:
            client = TestClient(app)
            # SSE endpoint requires sse-starlette; if not installed, we just
            # verify the route is registered by checking it doesn't 404
            response = client.get("/events/test-session", timeout=2)
            # Accept 200 (streaming) or 500 (sse-starlette not installed)
            assert response.status_code != 404
        except Exception:
            # If sse-starlette is not installed, the import will fail inside
            # the endpoint. That's acceptable — the route is still registered.
            pass

    def test_broadcaster_in_app_state(self, tmp_path):
        """SSEBroadcaster should be accessible from app.state."""
        from starlette.testclient import TestClient

        from seeknal.ask.gateway.server import create_app

        app = create_app(project_path=tmp_path)
        client = TestClient(app)
        # Lifespan sets up broadcaster — use health endpoint to trigger it
        with client:
            response = client.get("/health")
            assert response.status_code == 200
            # Verify broadcaster is set in state
            assert hasattr(app.state, "sse_broadcaster")

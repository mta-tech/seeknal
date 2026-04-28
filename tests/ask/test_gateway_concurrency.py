"""Concurrency/performance controls for the Ask gateway."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from seeknal.ask.gateway.server import (
    _run_agent_streaming,
    create_gateway_app,
)


async def _slow_inner(*args, **kwargs):
    yield {"type": "token", "data": "working"}
    await asyncio.sleep(0.2)
    yield {"type": "answer", "data": "done"}


@pytest.mark.asyncio
async def test_run_agent_streaming_can_be_cancelled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("seeknal.ask.gateway.server._run_agent_inner", _slow_inner)

    events: list[dict] = []

    async def collect():
        async for event in _run_agent_streaming(tmp_path, "s1", "question"):
            events.append(event)
            if event["type"] == "token":
                from seeknal.ask.gateway.server import _request_cancel

                _request_cancel("s1")

    await collect()

    assert any(event["type"] == "cancelled" for event in events)
    assert events[-1]["type"] == "done"
    assert "elapsed_ms" in events[-1]


class _FakeLockBackend:
    def __init__(self):
        self.acquired = False
        self.released = False

    async def acquire(self, session_id: str, tenant_id: str = "default"):
        self.acquired = True
        return object()

    async def release(self, lock):
        self.released = True


@pytest.mark.asyncio
async def test_run_agent_streaming_uses_configured_lock_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        "seeknal.ask.gateway.server._run_agent_inner",
        lambda *args, **kwargs: _single_answer(),
    )
    lock_backend = _FakeLockBackend()

    events = [
        event
        async for event in _run_agent_streaming(
            tmp_path,
            "s1",
            "question",
            lock_backend=lock_backend,
        )
    ]

    assert lock_backend.acquired is True
    assert lock_backend.released is True
    assert any(event["type"] == "answer" for event in events)


async def _single_answer():
    yield {"type": "answer", "data": "ok"}


def test_cancel_session_endpoint(tmp_path: Path):
    pytest.importorskip("starlette")
    from starlette.testclient import TestClient

    app = create_gateway_app(project_path=tmp_path)
    with TestClient(app) as client:
        res = client.post("/sessions/s1/cancel")

    assert res.status_code == 200
    assert res.json()["status"] == "cancel_requested"


def test_gateway_app_wires_redis_lock_backend(tmp_path: Path, monkeypatch):
    pytest.importorskip("starlette")

    class FakeRedis:
        @classmethod
        def from_url(cls, url):
            return cls()

        def pubsub(self):
            class PubSub:
                async def listen(self):
                    await asyncio.Event().wait()
                    if False:
                        yield {}

                async def subscribe(self, channel):
                    return None

                async def unsubscribe(self, channel):
                    return None

                async def close(self):
                    return None

            return PubSub()

        def lock(self, *args, **kwargs):
            return object()

        async def close(self):
            return None

    monkeypatch.setattr("redis.asyncio.Redis", FakeRedis)

    from starlette.testclient import TestClient

    app = create_gateway_app(project_path=tmp_path, redis_url="redis://example")
    with TestClient(app) as client:
        assert client.app.state.redis_enabled is True
        assert client.app.state.session_lock is not None

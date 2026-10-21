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


# ---------------------------------------------------------------------------
# Generator lifecycle (GW1): no terminal yield inside `finally`
# ---------------------------------------------------------------------------


async def _two_events_slow(*_args, **_kwargs):
    yield {"type": "token", "data": "one"}
    await asyncio.sleep(0.2)
    yield {"type": "answer", "data": "two"}


async def _collect_one_then_close():
    """Consume exactly one event, then abandon (aclose) the generator."""
    agen = _run_agent_streaming(Path("/tmp/does-not-matter"), "s1", "question")
    events: list[dict] = []
    async for event in agen:
        events.append(event)
        break
    await agen.aclose()
    return events


@pytest.mark.asyncio
async def test_aclose_after_first_event_releases_session_lock(
    monkeypatch: pytest.MonkeyPatch,
):
    """Closing a half-consumed stream must release the session lock.

    Regression for GW1/G2: `_run_agent_streaming` used to yield `done` from a
    `finally` block. When a consumer closed the generator mid-stream, Python
    re-raised `GeneratorExit` at that terminal yield as
    `RuntimeError: async generator ignored GeneratorExit` and truncated the
    `_session_lock_context` before release — leaving `_session_locks[...]
    locked() == True` for the rest of the idle-TTL (30 min). Today's
    production logs show exactly that RuntimeError (`seeknal-neo.log`).
    """
    monkeypatch.setattr("seeknal.ask.gateway.server._run_agent_inner", _two_events_slow)
    from seeknal.ask.gateway.server import _get_session_lock

    events = await _collect_one_then_close()

    assert events and events[0]["type"] == "token"
    assert not _get_session_lock("s1").locked(), (
        "session lock stayed held after the stream was closed mid-turn"
    )


@pytest.mark.asyncio
async def test_second_turn_can_start_after_half_consumed_stream(
    monkeypatch: pytest.MonkeyPatch,
):
    """The session's next turn must not block on a leaked lock (GW1 P1 core)."""
    monkeypatch.setattr("seeknal.ask.gateway.server._run_agent_inner", _two_events_slow)
    from seeknal.ask.gateway.server import _get_session_lock

    await _collect_one_then_close()

    lock = _get_session_lock("s1")
    # If the lock were still held this would time out with TimeoutError.
    await asyncio.wait_for(lock.acquire(), timeout=1.0)
    lock.release()


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


@pytest.mark.asyncio
async def test_gateway_streaming_passes_configured_tool_call_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """The gateway path must forward the configured limit (5), not a default."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    import seeknal.ask.gateway.server as server
    from seeknal.ask.agents.tools._context import ToolContext

    captured: dict = {}

    class _FakeRun:
        result = SimpleNamespace(output="ok", all_messages=lambda: [])

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

    class _FakeAgent:
        def iter(self, *_args, **kwargs):
            captured["usage_limits"] = kwargs["usage_limits"]
            return _FakeRun()

        async def run(self, *_args, **_kwargs):
            return SimpleNamespace(output="ok", all_messages=lambda: [])

    class _FakeStore:
        def __init__(self, *_args, **_kwargs):
            pass

        def get(self, *_args):
            return object()

        def create(self, *_args, **_kwargs):
            return None

        def load_messages(self, *_args):
            return []

        def save_messages(self, *_args):
            pass

        def update(self, *_args, **_kwargs):
            pass

    async def _no_directive(*_args, **_kwargs):
        return None

    monkeypatch.setattr("seeknal.ask.sessions.SessionStore", _FakeStore)
    monkeypatch.setattr(
        "seeknal.ask.agents.agent.create_agent",
        lambda *_a, **_k: (_FakeAgent(), MagicMock(), [], None),
    )
    monkeypatch.setattr(
        "seeknal.ask.agents.agent.compact_history_for_analysis_mode",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "seeknal.ask.directives.try_direct_tool_directive", _no_directive
    )
    for name, value in {
        "lookup_prior_turn_answer": None,
        "record_prior_turn_answer": None,
        "reset_report_approval": None,
        "reset_turn_governor": None,
        "seed_prior_turn_from_history": None,
    }.items():
        monkeypatch.setattr(
            f"seeknal.ask.agents.tools._context.{name}", lambda *a, **k: value
        )

    tool_ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        request_limit=33,
        tool_call_limit=5,
        disable_quality_gate=True,
    )
    monkeypatch.setattr(
        "seeknal.ask.agents.tools._context.get_tool_context",
        lambda: tool_ctx,
    )

    events = [
        event
        async for event in server._run_agent_inner(
            tmp_path, "s-limit", "count rows", tenant_id="default",
        )
    ]

    assert captured["usage_limits"].request_limit == 33
    assert captured["usage_limits"].tool_calls_limit == 5
    assert any(event["type"] == "answer" for event in events)

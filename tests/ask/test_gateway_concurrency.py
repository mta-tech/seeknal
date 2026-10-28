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


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_mode", ["aclose", "cancel", "cancel_inner", "user_cancel", "error"])
async def test_inner_task_bound_cleanup_precedes_session_unlock(
    tmp_path, monkeypatch, exit_mode,
):
    """Close the inner cancel scope in its owner task before another turn starts."""
    import anyio
    import seeknal.ask.gateway.server as server

    session_id = f"cleanup-{exit_mode}"
    lock = server._get_session_lock(session_id)
    first_event = asyncio.Event()
    continue_turn = asyncio.Event()
    cleanup = {}

    async def inner(*args, **kwargs):
        owner = asyncio.current_task()
        with anyio.CancelScope():
            try:
                yield {"type": "token", "data": "one"}
                if exit_mode == "error":
                    raise ValueError("inner failed")
                if exit_mode == "cancel_inner":
                    first_event.set()
                    await asyncio.Event().wait()
                yield {"type": "answer", "data": "two"}
            finally:
                cleanup["same_task"] = asyncio.current_task() is owner
                cleanup["lock_held"] = lock.locked()
                await asyncio.sleep(0)
                cleanup["finished"] = True

    monkeypatch.setattr(server, "_run_agent_inner", inner)

    async def consume():
        stream = server._run_agent_streaming(tmp_path, session_id, "question")
        await anext(stream)
        if exit_mode != "cancel_inner":
            first_event.set()
        try:
            if exit_mode != "cancel_inner":
                await continue_turn.wait()
            if exit_mode == "aclose":
                await stream.aclose()
            else:
                if exit_mode == "user_cancel":
                    server._request_cancel(session_id)
                async for _ in stream:
                    pass
        finally:
            await stream.aclose()
        assert cleanup.get("finished"), "aclose returned before inner cleanup"

    task = asyncio.create_task(consume())
    await first_event.wait()

    async def next_turn():
        async with lock:
            return cleanup.get("finished", False)

    waiter = asyncio.create_task(next_turn())
    await asyncio.sleep(0)
    if exit_mode in {"cancel", "cancel_inner"}:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif exit_mode == "error":
        continue_turn.set()
        with pytest.raises(ValueError, match="inner failed"):
            await task
    else:
        continue_turn.set()
        await task

    assert await waiter, "next turn acquired the lock before inner cleanup"
    assert cleanup == {"same_task": True, "lock_held": True, "finished": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_mode", ["aclose", "cancel", "normal", "error", "user_cancel"])
async def test_stream_terminal_done_is_published_once(tmp_path, monkeypatch, exit_mode):
    """Hard cancellation and consumer close still terminate the SSE stream."""
    import seeknal.ask.gateway.server as server

    published = []

    async def publish(session_id, event, **kwargs):
        published.append(event)

    async def inner(*args, **kwargs):
        yield {"type": "token", "data": "one"}
        if exit_mode == "error":
            raise ValueError("inner failed")
        if exit_mode == "cancel":
            await asyncio.Event().wait()
        yield {"type": "answer", "data": "two"}

    monkeypatch.setattr(server, "_run_agent_inner", inner)
    monkeypatch.setattr(server, "_publish_event_async", publish)
    started = asyncio.Event()
    yielded = []

    async def consume():
        stream = server._run_agent_streaming(tmp_path, f"done-{exit_mode}", "question")
        try:
            yielded.append(await anext(stream))
            started.set()
            if exit_mode == "aclose":
                await stream.aclose()
                return
            if exit_mode == "user_cancel":
                server._request_cancel(f"done-{exit_mode}")
            async for event in stream:
                yielded.append(event)
        finally:
            await stream.aclose()

    task = asyncio.create_task(consume())
    await started.wait()
    if exit_mode == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif exit_mode == "error":
        with pytest.raises(ValueError, match="inner failed"):
            await task
    else:
        await task

    assert [e["type"] for e in published].count("done") == 1
    assert [e["type"] for e in yielded].count("done") == (
        0 if exit_mode in {"aclose", "cancel"} else 1
    )


@pytest.mark.asyncio
async def test_aclose_after_done_does_not_publish_another_done(tmp_path, monkeypatch):
    import seeknal.ask.gateway.server as server

    published = []

    async def publish(session_id, event, **kwargs):
        published.append(event)

    monkeypatch.setattr(server, "_run_agent_inner", lambda *a, **k: _single_answer())
    monkeypatch.setattr(server, "_publish_event_async", publish)
    stream = server._run_agent_streaming(tmp_path, "close-after-done", "question")
    assert (await anext(stream))["type"] == "answer"
    assert (await anext(stream))["type"] == "done"
    await stream.aclose()
    assert [e["type"] for e in published] == ["answer", "done"]


@pytest.mark.asyncio
@pytest.mark.parametrize("publish_mode", ["failure", "timeout"])
async def test_aclose_terminal_publish_is_bounded_best_effort(tmp_path, monkeypatch, publish_mode):
    import seeknal.ask.gateway.server as server

    attempts = []

    async def publish(session_id, event, **kwargs):
        if event["type"] == "done":
            attempts.append(event)
            if publish_mode == "failure":
                raise RuntimeError("publisher unavailable")
            await asyncio.Event().wait()

    monkeypatch.setattr(server, "_run_agent_inner", _two_events_slow)
    monkeypatch.setattr(server, "_publish_event_async", publish)
    stream = server._run_agent_streaming(tmp_path, "close-best-effort", "question")
    await anext(stream)
    # asyncio.timeout keeps aclose and its cleanup in the consuming task.
    async with asyncio.timeout(2):
        await stream.aclose()
    assert len(attempts) == 1
    assert not server._get_session_lock("close-best-effort").locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_mode", ["aclose", "cancel"])
async def test_failing_inner_cleanup_preserves_consumer_exit_and_unlocks(
    tmp_path, monkeypatch, caplog, exit_mode,
):
    import anyio
    import seeknal.ask.gateway.server as server

    session_id = f"failing-cleanup-{exit_mode}"
    lock = server._get_session_lock(session_id)
    publishing = asyncio.Event()
    published = []
    cleanup = []

    async def inner(*args, **kwargs):
        owner = asyncio.current_task()
        with anyio.CancelScope():
            try:
                yield {"type": "answer", "data": "one"}
            finally:
                cleanup.append((asyncio.current_task() is owner, lock.locked()))
                await asyncio.sleep(0)
                raise RuntimeError("injected inner cleanup failure")

    async def publish(session_id, event, **kwargs):
        if event["type"] == "answer" and exit_mode == "cancel":
            publishing.set()
            await asyncio.Event().wait()
        published.append(event)

    monkeypatch.setattr(server, "_run_agent_inner", inner)
    monkeypatch.setattr(server, "_publish_event_async", publish)

    async def consume():
        stream = server._run_agent_streaming(tmp_path, session_id, "question")
        try:
            await anext(stream)
            await stream.aclose()
        finally:
            # Also cleans up the pre-fix generator left suspended at an error yield.
            await stream.aclose()

    if exit_mode == "cancel":
        task = asyncio.create_task(consume())
        await publishing.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        await consume()
    assert cleanup == [(True, True)]
    assert not lock.locked()
    async with asyncio.timeout(1):
        async with lock:
            pass
    assert [event["type"] for event in published] == (
        ["answer", "done"] if exit_mode == "aclose" else ["done"]
    )
    assert "injected inner cleanup failure" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_after_acceptance", [False, True])
async def test_cancellation_during_done_publish_awaits_one_accepted_done(
    tmp_path, monkeypatch, cancel_after_acceptance,
):
    import seeknal.ask.gateway.server as server

    session_id = f"cancel-during-done-{cancel_after_acceptance}"
    publishing = asyncio.Event()
    finish_publish = asyncio.Event()
    accepted = []
    attempts = []

    async def publish(session_id, event, **kwargs):
        if event["type"] == "done":
            attempts.append(event)
            if cancel_after_acceptance:
                accepted.append(event)
            publishing.set()
            await finish_publish.wait()
            if not cancel_after_acceptance:
                accepted.append(event)

    monkeypatch.setattr(server, "_run_agent_inner", lambda *a, **k: _single_answer())
    monkeypatch.setattr(server, "_publish_event_async", publish)

    async def consume():
        stream = server._run_agent_streaming(tmp_path, session_id, "question")
        try:
            async for _ in stream:
                pass
        finally:
            await stream.aclose()

    task = asyncio.create_task(consume())
    await publishing.wait()
    task.cancel()
    await asyncio.sleep(0)
    finish_publish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(attempts) == 1
    assert len(accepted) == 1
    assert not server._get_session_lock(session_id).locked()


@pytest.mark.asyncio
async def test_cancellation_during_inner_cleanup_propagates_after_finalizer_unwinds(
    tmp_path, monkeypatch,
):
    import seeknal.ask.gateway.server as server

    session_id = "cancel-during-inner-cleanup"
    lock = server._get_session_lock(session_id)
    cleanup_started = asyncio.Event()
    cleanup_finished = asyncio.Event()
    cleanup_lock_states = []

    async def inner(*args, **kwargs):
        try:
            yield {"type": "token", "data": "one"}
        finally:
            cleanup_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_lock_states.append(lock.locked())
                await asyncio.sleep(0)
                cleanup_finished.set()

    monkeypatch.setattr(server, "_run_agent_inner", inner)

    async def consume():
        stream = server._run_agent_streaming(tmp_path, session_id, "question")
        await anext(stream)
        await stream.aclose()

    task = asyncio.create_task(consume())
    await cleanup_started.wait()
    task.cancel("cancel while closing inner")
    with pytest.raises(asyncio.CancelledError, match="cancel while closing inner"):
        await task
    assert cleanup_finished.is_set()
    assert cleanup_lock_states == [True]
    assert not lock.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_outcome", ["transformed", "suppressed"])
async def test_fresh_cancel_survives_inner_cleanup_exception_handling(
    tmp_path, monkeypatch, cleanup_outcome,
):
    import seeknal.ask.gateway.server as server

    session_id = f"cleanup-handled-cancel-{cleanup_outcome}"
    lock = server._get_session_lock(session_id)
    cleanup_started = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def inner(*args, **kwargs):
        try:
            yield {"type": "token", "data": "one"}
        finally:
            cleanup_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                assert lock.locked()
                cleanup_finished.set()
                if cleanup_outcome == "transformed":
                    raise RuntimeError("cleanup transformed cancellation")

    monkeypatch.setattr(server, "_run_agent_inner", inner)

    async def consume():
        stream = server._run_agent_streaming(tmp_path, session_id, "question")
        await anext(stream)
        await stream.aclose()

    task = asyncio.create_task(consume())
    await cleanup_started.wait()
    task.cancel("fresh cancel")
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await task
    if cleanup_outcome == "transformed":
        assert str(exc_info.value) == "fresh cancel"
    assert cleanup_finished.is_set()
    assert not lock.locked()


@pytest.mark.asyncio
async def test_repeated_cancel_finishes_terminal_publish_before_next_turn(tmp_path, monkeypatch):
    import seeknal.ask.gateway.server as server

    session_id = "double-cancel-terminal"
    lock = server._get_session_lock(session_id)
    publishing = asyncio.Event()
    finish_publish = asyncio.Event()
    publish_finished = asyncio.Event()
    accepted = []

    async def publish(session_id, event, **kwargs):
        if event["type"] == "done":
            publishing.set()
            try:
                await finish_publish.wait()
                accepted.append(event)
            finally:
                publish_finished.set()

    monkeypatch.setattr(server, "_run_agent_inner", lambda *a, **k: _single_answer())
    monkeypatch.setattr(server, "_publish_event_async", publish)

    async def consume():
        stream = server._run_agent_streaming(tmp_path, session_id, "question")
        try:
            async for _ in stream:
                pass
        finally:
            await stream.aclose()

    async def next_turn():
        async with lock:
            return publish_finished.is_set()

    task = asyncio.create_task(consume())
    await publishing.wait()
    waiter = asyncio.create_task(next_turn())
    task.cancel("first cancel")
    await asyncio.sleep(0)
    task.cancel("second cancel")
    await asyncio.sleep(0)
    held_after_second_cancel = lock.locked()
    finish_publish.set()
    with pytest.raises(asyncio.CancelledError, match="first cancel"):
        await task
    assert await waiter, "terminal publish was still pending when the next turn acquired the lock"
    assert held_after_second_cancel
    assert len(accepted) == 1
    assert publish_finished.is_set()


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

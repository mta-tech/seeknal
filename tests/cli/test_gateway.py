"""Regression tests for seeknal.cli.gateway."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from seeknal.cli import gateway as gateway_module


@pytest.mark.asyncio
async def test_http_only_worker_retry_path_does_not_raise_nameerror():
    """The httpx.RequestError retry path must complete without NameError.

    Regression for #60: `_run_http_only_worker` is async-def at module
    scope and calls `await asyncio.sleep(5)` in its retry branch. If
    `asyncio` is not in scope (neither module-level nor function-local),
    the retry path raises NameError on the first transient gateway
    error. This drives one ConnectError → sleep → re-poll cycle and
    asserts no NameError, regardless of whether the import is at module
    or function scope.
    """

    class _StopLoop(Exception):
        pass

    class _FakeClient:
        def __init__(self):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                raise httpx.ConnectError("simulated gateway down")
            raise _StopLoop()

        async def post(self, *_args, **_kwargs):  # pragma: no cover - unused
            raise AssertionError("post() must not be called on retry path")

    sleep_calls: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    with (
        patch("asyncio.sleep", _fake_sleep),
        patch("httpx.AsyncClient", return_value=_FakeClient()),
    ):
        with pytest.raises(_StopLoop):
            await gateway_module._run_http_only_worker(
                project_path=Path("/tmp/does-not-matter"),
                gateway_url="http://example.invalid",
                api_token="dummy",
                poll_timeout=1.0,
            )

    # The retry branch must have run exactly once and slept for 5s.
    assert sleep_calls == [5]


# ---------------------------------------------------------------------------
# Concurrency tests (issue #63)
# ---------------------------------------------------------------------------


def _make_fake_response(*, status_code: int, payload: dict | None = None):
    """Tiny stand-in for ``httpx.Response`` exposing only what the worker uses."""

    class _Resp:
        def __init__(self) -> None:
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise httpx.HTTPStatusError(
                    "boom", request=None, response=self  # type: ignore[arg-type]
                )

        def json(self) -> dict:
            assert payload is not None
            return payload

    return _Resp()


class _StopLoop(Exception):
    """Sentinel raised inside fake gateway to unwind the worker's poll loop."""


class _ScriptedGateway:
    """A scripted fake of the gateway endpoints the HTTP worker calls.

    ``poll_results`` is a list of responses (or exceptions) returned one by
    one from /work-stream. After exhaustion, ``_StopLoop`` is raised so the
    test can drive the loop to exit and ``drain`` happens deterministically.
    """

    def __init__(self, poll_results: list) -> None:
        self._poll_results = list(poll_results)
        self.events: dict[str, list[dict]] = {}
        self.completions: dict[str, dict] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url: str, **_kwargs):
        if not self._poll_results:
            raise _StopLoop()
        nxt = self._poll_results.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt

    async def post(self, url: str, *, json: dict, **_kwargs):
        # URL shape: .../internal/worker/work/{work_id}/event|complete
        parts = url.rstrip("/").split("/")
        verb = parts[-1]
        work_id = parts[-2]
        if verb == "event":
            self.events.setdefault(work_id, []).append(json)
        elif verb == "complete":
            self.completions[work_id] = json
        return _make_fake_response(status_code=200, payload={})


@pytest.mark.asyncio
async def test_http_worker_fans_out_with_max_concurrency():
    """With ``max_concurrency=3``, three work items run concurrently in one process.

    Each spawned task calls ``_run_agent_streaming`` once; we use an asyncio.Event
    to hold them all open until released, proving they ran in parallel rather
    than sequentially (would otherwise sum to ~30s wall time at 1 each).
    """
    release = asyncio.Event()
    concurrent_high_water = 0
    in_flight = 0
    in_flight_lock = asyncio.Lock()

    async def fake_streaming(_project, _session, _q, **_kw):
        nonlocal in_flight, concurrent_high_water
        async with in_flight_lock:
            in_flight += 1
            concurrent_high_water = max(concurrent_high_water, in_flight)
        await release.wait()
        async with in_flight_lock:
            in_flight -= 1
        yield {"type": "answer", "data": "done"}

    fake = _ScriptedGateway(poll_results=[
        _make_fake_response(status_code=200, payload={
            "work_id": "w-aaaaaaaa", "session_id": "s1", "question": "q1",
        }),
        _make_fake_response(status_code=200, payload={
            "work_id": "w-bbbbbbbb", "session_id": "s2", "question": "q2",
        }),
        _make_fake_response(status_code=200, payload={
            "work_id": "w-cccccccc", "session_id": "s3", "question": "q3",
        }),
    ])

    # Release the streaming tasks shortly after all three have started.
    async def releaser():
        for _ in range(50):
            if concurrent_high_water >= 3:
                break
            await asyncio.sleep(0.01)
        release.set()

    asyncio.create_task(releaser())

    with (
        patch("httpx.AsyncClient", return_value=fake),
        patch(
            "seeknal.ask.gateway.server._run_agent_streaming",
            fake_streaming,
        ),
    ):
        with pytest.raises(_StopLoop):
            await gateway_module._run_http_only_worker(
                project_path=Path("/tmp/does-not-matter"),
                gateway_url="http://example.invalid",
                api_token="dummy",
                poll_timeout=0.1,
                max_concurrency=3,
                shutdown_timeout=5.0,
            )

    assert concurrent_high_water == 3, (
        f"expected 3 concurrent in-flight tasks, saw {concurrent_high_water}"
    )
    # All three work items completed and POSTed `complete`.
    assert set(fake.completions.keys()) == {"w-aaaaaaaa", "w-bbbbbbbb", "w-cccccccc"}
    for work_id, body in fake.completions.items():
        assert body["error"] is None, f"{work_id} unexpectedly errored: {body}"


@pytest.mark.asyncio
async def test_http_worker_isolates_mid_flight_failure():
    """One failing task must not break others; it must still POST ``complete``."""
    started = asyncio.Event()
    fail_started = asyncio.Event()

    async def fake_streaming(_project, _session, _q, **_kw):
        if _session == "fail":
            fail_started.set()
            raise RuntimeError("simulated agent failure")
        started.set()
        yield {"type": "answer", "data": "ok"}

    fake = _ScriptedGateway(poll_results=[
        _make_fake_response(status_code=200, payload={
            "work_id": "w-failure", "session_id": "fail", "question": "boom",
        }),
        _make_fake_response(status_code=200, payload={
            "work_id": "w-success", "session_id": "ok", "question": "fine",
        }),
    ])

    with (
        patch("httpx.AsyncClient", return_value=fake),
        patch(
            "seeknal.ask.gateway.server._run_agent_streaming",
            fake_streaming,
        ),
    ):
        with pytest.raises(_StopLoop):
            await gateway_module._run_http_only_worker(
                project_path=Path("/tmp/does-not-matter"),
                gateway_url="http://example.invalid",
                api_token="dummy",
                poll_timeout=0.1,
                max_concurrency=2,
                shutdown_timeout=5.0,
            )

    # Both completions must have been POSTed (slot leak prevention).
    assert "w-failure" in fake.completions
    assert "w-success" in fake.completions
    # Failing item carries the error string; success does not.
    assert "simulated agent failure" in (fake.completions["w-failure"]["error"] or "")
    assert fake.completions["w-success"]["error"] is None
    # Failing item also emitted error+done events to the gateway.
    fail_events = [e["type"] for e in fake.events.get("w-failure", [])]
    assert "error" in fail_events and "done" in fail_events


@pytest.mark.asyncio
async def test_http_worker_drains_on_shutdown():
    """KeyboardInterrupt stops polling and drains in-flight tasks within timeout."""
    started_count = 0
    started_event = asyncio.Event()
    allow_finish = asyncio.Event()

    async def fake_streaming(_project, _session, _q, **_kw):
        nonlocal started_count
        started_count += 1
        if started_count >= 2:
            started_event.set()
        await allow_finish.wait()
        yield {"type": "answer", "data": "late"}

    class _ShutdownGateway(_ScriptedGateway):
        """Like _ScriptedGateway, but raises KeyboardInterrupt after N polls."""
        def __init__(self, poll_results, *, interrupt_after: int) -> None:
            super().__init__(poll_results)
            self._poll_count = 0
            self._interrupt_after = interrupt_after

        async def get(self, url, **kwargs):
            self._poll_count += 1
            if self._poll_count > self._interrupt_after:
                raise KeyboardInterrupt()
            return await super().get(url, **kwargs)

    fake = _ShutdownGateway(
        poll_results=[
            _make_fake_response(status_code=200, payload={
                "work_id": "w-slow-1", "session_id": "s1", "question": "q1",
            }),
            _make_fake_response(status_code=200, payload={
                "work_id": "w-slow-2", "session_id": "s2", "question": "q2",
            }),
        ],
        interrupt_after=2,
    )

    async def trigger_finish():
        await started_event.wait()
        # Give the worker a moment to attempt a 3rd poll → KeyboardInterrupt fires
        await asyncio.sleep(0.05)
        allow_finish.set()

    asyncio.create_task(trigger_finish())

    with (
        patch("httpx.AsyncClient", return_value=fake),
        patch(
            "seeknal.ask.gateway.server._run_agent_streaming",
            fake_streaming,
        ),
    ):
        # KeyboardInterrupt is caught inside the worker; it returns cleanly.
        await gateway_module._run_http_only_worker(
            project_path=Path("/tmp/does-not-matter"),
            gateway_url="http://example.invalid",
            api_token="dummy",
            poll_timeout=0.05,
            max_concurrency=2,
            shutdown_timeout=5.0,
        )

    # Both in-flight tasks must have POSTed complete (no broker leak).
    assert "w-slow-1" in fake.completions
    assert "w-slow-2" in fake.completions


@pytest.mark.asyncio
async def test_http_worker_rejects_invalid_concurrency():
    with pytest.raises(ValueError, match="max_concurrency"):
        await gateway_module._run_http_only_worker(
            project_path=Path("/tmp/does-not-matter"),
            gateway_url="http://example.invalid",
            api_token="dummy",
            max_concurrency=0,
        )

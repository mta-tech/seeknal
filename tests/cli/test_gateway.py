"""Regression tests for seeknal.cli.gateway."""

from __future__ import annotations

import asyncio
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from typer.testing import CliRunner

from seeknal.ask.gateway.temporal import TEMPORAL_AVAILABLE
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


# ---------------------------------------------------------------------------
# Back-off floor on instant 204s (P4-2 / worker-side hardening)
#
# These drive the real `_run_http_only_worker` loop against a real local
# HTTP server over loopback TCP -- no mocking of httpx.AsyncClient itself --
# so the elapsed-time measurement around the real network call is exercised,
# not a stand-in for it.
# ---------------------------------------------------------------------------


def _make_instant_204_handler(counter: list[int], *, delay: float = 0.0):
    """A handler that answers every GET with an immediate 204, after `delay`."""

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib naming
            counter[0] += 1
            if delay:
                time.sleep(delay)
            self.send_response(204)
            self.end_headers()

        def log_message(self, format, *args):  # noqa: A002 - stdlib signature
            pass  # silence request logging in test output

    return _Handler


class _LocalServer:
    """A background-thread stdlib HTTP server, torn down on context exit."""

    def __init__(self, handler_cls) -> None:
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> str:
        self._thread.start()
        return f"http://127.0.0.1:{self._server.server_port}"

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


async def _run_worker_for(seconds: float, **kwargs) -> None:
    """Run the real worker loop as a background task, then cancel it.

    `_run_http_only_worker` catches `asyncio.CancelledError` internally and
    returns normally (see its `except (KeyboardInterrupt, asyncio.CancelledError)`
    clause), so awaiting the cancelled task does not raise here.
    """
    task = asyncio.create_task(gateway_module._run_http_only_worker(**kwargs))
    await asyncio.sleep(seconds)
    task.cancel()
    await task


@pytest.mark.asyncio
async def test_http_worker_backoff_floor_caps_poll_rate_against_instant_204():
    """Default `min_poll_interval` caps an instant-204 gateway at ~1 poll/s.

    This is the exact regression the review's P4-2/worker-hardening item
    describes: a gateway that answers 204 immediately (ignoring `timeout`,
    as the IBA gateway did for three days) must not drive this worker into
    a busy spin.
    """
    poll_count = [0]
    with _LocalServer(_make_instant_204_handler(poll_count)) as base_url:
        await _run_worker_for(
            1.5,
            project_path=Path("/tmp/does-not-matter"),
            gateway_url=base_url,
            api_token="dummy",
            poll_timeout=0.05,
            min_poll_interval=1.0,
        )

    assert 1 <= poll_count[0] <= 2, f"expected <=2 polls in 1.5s, saw {poll_count[0]}"


@pytest.mark.asyncio
async def test_http_worker_zero_backoff_polls_many_times_against_instant_204():
    """`min_poll_interval=0` restores the old (unbounded) instant-204 poll rate."""
    poll_count = [0]
    with _LocalServer(_make_instant_204_handler(poll_count)) as base_url:
        await _run_worker_for(
            1.5,
            project_path=Path("/tmp/does-not-matter"),
            gateway_url=base_url,
            api_token="dummy",
            poll_timeout=0.05,
            min_poll_interval=0.0,
        )

    assert poll_count[0] >= 20, (
        f"expected many polls with no floor, saw only {poll_count[0]}"
    )


@pytest.mark.asyncio
async def test_http_worker_backoff_floor_counts_elapsed_time_not_additive():
    """A gateway that already holds the request near the floor adds no extra sleep.

    The handler holds each request for 0.3s before answering 204, and
    `min_poll_interval` is also 0.3s. If the elapsed request time were
    ignored (i.e. the floor slept unconditionally on top of it), each cycle
    would take ~0.6s and this window would see about half as many polls.
    """
    poll_count = [0]
    with _LocalServer(_make_instant_204_handler(poll_count, delay=0.3)) as base_url:
        await _run_worker_for(
            1.5,
            project_path=Path("/tmp/does-not-matter"),
            gateway_url=base_url,
            api_token="dummy",
            poll_timeout=0.05,
            min_poll_interval=0.3,
        )

    # ~1.5s / 0.3s per cycle =~ 5 polls. A floor that stacks on top of the
    # 0.3s handling time would instead yield ~1.5s / 0.6s =~ 2.
    assert 3 <= poll_count[0] <= 6, (
        f"expected ~5 polls (no additive sleep), saw {poll_count[0]}"
    )


# ---------------------------------------------------------------------------
# Broker-supplied project_path containment (P2-4)
# ---------------------------------------------------------------------------


def test_resolve_broker_project_path_refuses_when_root_unset(tmp_path):
    child = tmp_path / "proj"
    child.mkdir()

    with pytest.raises(gateway_module.BrokerProjectPathRefused, match="SEEKNAL_PROJECT_ROOT"):
        gateway_module._resolve_broker_project_path(str(child), project_root=None)


def test_resolve_broker_project_path_refuses_relative_path(tmp_path):
    root = tmp_path / "root"
    root.mkdir()

    with pytest.raises(gateway_module.BrokerProjectPathRefused, match="absolute"):
        gateway_module._resolve_broker_project_path("relative/proj", project_root=root)


def test_resolve_broker_project_path_refuses_nonexistent(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    missing = root / "does-not-exist"

    with pytest.raises(gateway_module.BrokerProjectPathRefused, match="does not exist"):
        gateway_module._resolve_broker_project_path(str(missing), project_root=root)


def test_resolve_broker_project_path_refuses_dotdot_traversal(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    traversal = str(root / ".." / "outside")

    with pytest.raises(gateway_module.BrokerProjectPathRefused, match="outside its base"):
        gateway_module._resolve_broker_project_path(traversal, project_root=root)


def test_resolve_broker_project_path_refuses_symlink_escape(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    # A sibling whose name is `root`'s name plus a suffix: a string-prefix
    # check (`str(resolved).startswith(str(root))`) would misreport this as
    # living inside `root`, the exact "/data/tenant-a-evil looks like a
    # child of /data/tenant-a" case `safe_paths.py` is written against.
    sibling = tmp_path / (root.name + "-evil")
    sibling.mkdir()
    escape = root / "escape"
    escape.symlink_to(sibling, target_is_directory=True)

    with pytest.raises(gateway_module.BrokerProjectPathRefused, match="outside its base"):
        gateway_module._resolve_broker_project_path(str(escape), project_root=root)


def test_resolve_broker_project_path_accepts_genuine_child(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    child = root / "proj1"
    child.mkdir()

    resolved = gateway_module._resolve_broker_project_path(str(child), project_root=root)

    assert resolved == child.resolve()


def test_resolve_broker_project_path_creates_nothing_on_refusal(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    before = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))

    refusals = [
        (str(root / "nope"), None),
        ("relative/proj", root),
        (str(root / "missing"), root),
        (str(root / ".." / "outside"), root),
    ]
    for raw, project_root in refusals:
        with pytest.raises(gateway_module.BrokerProjectPathRefused):
            gateway_module._resolve_broker_project_path(raw, project_root=project_root)

    after = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*"))
    assert before == after, "a refusal must never create anything on disk"


@pytest.mark.skipif(not TEMPORAL_AVAILABLE, reason="temporalio not installed")
def test_gateway_worker_ignores_broker_project_path_when_project_flag_given(tmp_path):
    """`--project` wins outright; a hostile broker `project_path` is never even checked."""
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    def fake_httpx_get(url, headers=None, timeout=None):
        return _make_fake_response(status_code=200, payload={
            "worker_transport": "temporal",
            "project_path": "/etc/should-not-be-used",
        })

    class _FakeWorker:
        async def run(self):
            return None

    with (
        patch("httpx.get", fake_httpx_get),
        patch(
            "seeknal.ask.gateway.temporal.connect_temporal_client",
            AsyncMock(return_value=object()),
        ),
        patch(
            "seeknal.ask.gateway.temporal.create_temporal_worker",
            return_value=_FakeWorker(),
        ),
    ):
        runner = CliRunner()
        result = runner.invoke(gateway_module.gateway_app, [
            "worker",
            "--project", str(project_dir),
            "--gateway-url", "http://example.invalid",
            "--api-token", "dummy",
        ])

    assert result.exit_code == 0, result.output
    assert "Refusing broker-supplied project_path" not in result.output
    assert f"Project: {project_dir}" in result.output

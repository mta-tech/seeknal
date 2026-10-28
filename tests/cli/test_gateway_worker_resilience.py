"""GW1 regression tests — HTTP worker delivery resilience & stream lifecycle.

Covers the combined integration branch (tool-call limit + gateway worker
resilience, spec
`iba-deploy-runbook/specs/2026-10-21-spec-gw1-seeknal-ask-limits-gateway-resilience.md`):

- P1 from PR #106 review: the 404/410 early-abort path must release the
  per-session lock (real `_run_agent_streaming` + real `_process_http_work_item`),
  send no terminal error/done events to a vanished work, and let the next
  turn for the same session start.
- Delivery retry: DNS/connect blips are retried with backoff and must never
  abort an in-flight turn on their own.
- Completion retry: the `complete` POST survives transient failures.
- Event idempotency: `X-Seeknal-Event-Id` is attached to every event POST and
  `HttpWorkerBroker.mark_event_seen` suppresses replays.

Tests that assert lifecycle/poll behaviour use the REAL production stream
wrapper — fake-only streams were exactly what let PR #106's P1 slip past a
green CI.

"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

import seeknal.ask.gateway.http_worker as http_worker_module
from seeknal.cli import gateway as gateway_module
from seeknal.cli.gateway import _process_http_work_item


def _response(status_code: int):
    class _Resp:
        def __init__(self, code=status_code) -> None:
            self.status_code = code

        def json(self):  # pragma: no cover - unused for event posts
            return {}

    return _Resp(status_code)


class _RecordingClient:
    """Fake httpx.AsyncClient that records POSTs and scripts failures."""

    def __init__(self, behavior=None):
        # `behavior` — optional list of per-POST outcomes: int status, or
        # Exception instances to raise. Extra POSTs succeed with 200.
        self._behavior = list(behavior or [])
        self.posts: list[dict] = []
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, *, headers=None, json=None, timeout=None):
        self.calls += 1
        self.posts.append({
            "url": url,
            "headers": dict(headers or {}),
            "json": json,
        })
        if self._behavior:
            outcome = self._behavior.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return _response(outcome)
        return _response(200)

    async def get(self, *_args, **_kwargs):  # pragma: no cover - poll not used here
        raise AssertionError("get() not used in delivery tests")


def _work(work_id: str, session_id: str) -> dict:
    return {
        "work_id": work_id,
        "tenant_id": "default",
        "session_id": session_id,
        "question": "q",
        "project_path": "/tmp/does-not-matter",
    }


async def _stub_inner(*_args, **_kwargs):
    yield {"type": "answer", "data": "ok"}
    yield {"type": "token", "data": "tick"}
    yield {"type": "answer", "data": "final"}


async def _run_handler(client, work_id="w-10010010", session_id="sess-1", inner=None):
    """Run the real worker handler with a stubbed agent stream.

    ``inner`` optionally replaces the default answer/token stub — custom
    event shapes (clarification turns, charts) need their own event script.
    """
    semaphore = asyncio.Semaphore(1)
    started = asyncio.Event()

    async def _notify(*_a, **_k):
        started.set()

    with (
        patch.object(gateway_module, "_retry_jitter", lambda: 1.0),
        patch(
            "seeknal.ask.gateway.server._run_agent_inner",
            inner if inner is not None else _stub_inner,
        ),
    ):
        task = asyncio.create_task(_process_http_work_item(
            _work(work_id, session_id),
            client=client,
            base_url="http://gateway.example",
            headers={"Authorization": "Bearer t"},
            project_path=Path("/tmp/does-not-matter"),
            semaphore=semaphore,
        ))
        task.add_done_callback(lambda t: asyncio.get_running_loop().call_soon(_notify))
        try:
            await asyncio.wait_for(task, timeout=10.0)
        except asyncio.CancelledError:
            pass
    return task


# ---------------------------------------------------------------------------
# P1: abort on vanished work — lock release + clean terminal sequence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_work_vanished_404_releases_session_lock():
    """PR #106 P1, reproduced with the REAL stream wrapper + real handler.

    The gateway returns 404 on the first event POST (the broker discarded the
    expired work). The handler must abort the stream, release the session
    lock (no `RuntimeError: async generator ignored GeneratorExit`), and mark
    the run failed via `complete`.
    """
    from seeknal.ask.gateway.server import _get_session_lock

    client = _RecordingClient(behavior=[404])  # first event POST → 404; rest 200
    task = await asyncio.wait_for(
        _run_handler(client), timeout=10.0,
    )
    assert task.done() and task.exception() is None  # clean abort, no crash

    lock = _get_session_lock("sess-1")
    assert not lock.locked(), "session lock leaked after the vanished-work abort"
    # The next turn for the same session must be able to start immediately.
    await asyncio.wait_for(lock.acquire(), timeout=1.0)
    lock.release()


@pytest.mark.asyncio
async def test_work_vanished_sends_no_terminal_events_but_completes():
    client = _RecordingClient(behavior=[404])
    await asyncio.wait_for(_run_handler(client), timeout=10.0)

    post_calls = [p for p in client.posts if p["url"].endswith("/event")]
    complete_calls = [p for p in client.posts if p["url"].endswith("/complete")]
    assert len(post_calls) == 1, "no further event (incl. error/done) after abort"
    assert len(complete_calls) == 1, "completion must still be attempted"
    body = complete_calls[0]["json"]
    assert body["event_count"] == 1
    assert "gateway no longer knows work" in (body["error"] or "")
    assert body["dropped_event_count"] == 1  # the first POST was never delivered


@pytest.mark.asyncio
async def test_second_turn_runs_after_vanished_work_abort():
    """The reviewer's explicit requirement: a second turn for the SAME session
    must start (and fully complete) after the first turn aborted."""
    from seeknal.ask.gateway.server import _get_session_lock

    client = _RecordingClient(behavior=[404])
    await asyncio.wait_for(_run_handler(client, session_id="sess-2"), timeout=10.0)

    # Run a full second turn against the same real stream machinery; the
    # event POSTs succeed this time — proves the lock did not leak.
    recovered = _RecordingClient()
    await asyncio.wait_for(
        _run_handler(recovered, work_id="w-20020020", session_id="sess-2"),
        timeout=10.0,
    )
    complete_calls = [p for p in recovered.posts if p["url"].endswith("/complete")]
    assert complete_calls, "recovered turn did not complete"
    assert complete_calls[0]["json"]["error"] is None
    lock = _get_session_lock("sess-2")
    assert not lock.locked()


# ---------------------------------------------------------------------------
# Delivery retry: DNS/connect blips must not abort the turn
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_event_retries_on_dns_error_then_succeeds():
    """First event hits DNS failure twice, then success — turn continues."""
    # 3 × answer attempt (fail, fail, ok) + 1 × tick + 1 × done… the stub
    # yields answer, token, answer, done → 4 events; first gets 3 attempts.
    behavior = [
        httpx.ConnectError("[Errno -2] Name or service not known"),
        httpx.ConnectError("[Errno -2] Name or service not known"),
        200,
    ]
    client = _RecordingClient(behavior=behavior)
    await asyncio.wait_for(_run_handler(client), timeout=15.0)

    event_calls = [p for p in client.posts if p["url"].endswith("/event")]
    assert len(event_calls) >= 5  # 3 attempts for event 1 + events 2..4
    complete_calls = [p for p in client.posts if p["url"].endswith("/complete")]
    body = complete_calls[0]["json"]
    assert body["error"] is None, "a transient DNS blip must not fail the turn"
    assert body["event_count"] == 4
    # idempotency header present on delivery, stable per logical event
    ids = [p["headers"].get("X-Seeknal-Event-Id") for p in event_calls]
    assert ids and all(ids)
    assert ids[0] == ids[1] == ids[2], "retries of the same event reuse one id"


@pytest.mark.asyncio
async def test_permanent_delivery_failure_does_not_abort_turn():
    """All attempts fail with DNS for every POST — the turn still completes
    with error=None and a positive dropped count (finding §9.2-1: non-fatal).

    The completion POST now gets a time-boxed persistence window; with the
    window already exhausted by the event retries it gives up after the
    first attempt and reports `answer_dropped=True` so the gateway can
    reconcile the answer from the completion payload."""

    class _AlwaysFail(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            self.calls += 1
            self.posts.append({"url": url, "headers": dict(headers or {}), "json": json})
            raise httpx.ConnectError("[Errno -3] Temporary failure in name resolution")

    failing = _AlwaysFail()
    with (
        patch.object(gateway_module, "_COMPLETE_MAX_WAIT_S", 0.5),
        patch.object(gateway_module, "_COMPLETE_RETRY_DELAY_S", 0.01),
    ):
        task = await asyncio.wait_for(_run_handler(failing), timeout=20.0)
    assert task.done() and task.exception() is None

    complete_calls = [p for p in failing.posts if p["url"].endswith("/complete")]
    body = complete_calls[0]["json"]
    assert body["event_count"] == 4
    assert body["dropped_event_count"] == 4  # all four events (incl. done) dropped
    assert body["answer_dropped"] is True  # answers lost — gateway must reconcile
    assert body["error"] is None, "delivery failure alone must not fail the run"


@pytest.mark.asyncio
async def test_completion_post_retries_on_transient_failure():
    """The `complete` POST itself gets retried against DNS blips within the
    time-boxed window (delay patched small for a fast test)."""
    # Event POSTs succeed (implicit 200s); the FIRST two complete attempts fail.
    client = _RecordingClient()
    original_post = _RecordingClient.post

    async def script(self, url, *, headers=None, json=None, timeout=None):
        if url.endswith("/complete"):
            self.calls += 1
            self.posts.append({"url": url, "headers": dict(headers or {}), "json": json})
            self._complete_calls = getattr(self, "_complete_calls", 0) + 1
            if self._complete_calls <= 2:
                raise httpx.ConnectError("[Errno -2] Name or service not known")
            return _response(200)
        return await original_post(self, url, headers=headers, json=json, timeout=timeout)

    with (
        patch.object(gateway_module, "_COMPLETE_RETRY_DELAY_S", 0.01),
        patch.object(type(client), "post", script),
    ):
        await asyncio.wait_for(_run_handler(client), timeout=15.0)

    complete_calls = [p for p in client.posts if p["url"].endswith("/complete")]
    assert len(complete_calls) >= 3, "completion must retry until delivered"
    assert complete_calls[-1]["json"]["answer_dropped"] is False


@pytest.mark.asyncio
async def test_completion_failure_does_not_inflate_event_loss_counter():
    """A failed completion POST must not be counted as a dropped EVENT.

    Regression guard: completion retries are time-boxed, not counted, and a
    lost completion must not mix "SSE event lost" with "completion delivery
    lost". Here every EVENT is delivered (`answer_dropped=False`) while the
    completion itself is undeliverable.
    """

    class _CompleteDown(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            self.calls += 1
            self.posts.append({"url": url, "headers": dict(headers or {}), "json": json})
            if url.endswith("/complete"):
                raise httpx.ConnectError("[Errno -2] Name or service not known")
            return _response(200)

    client = _CompleteDown()
    with (
        patch.object(gateway_module, "_COMPLETE_MAX_WAIT_S", 0.5),
        patch.object(gateway_module, "_COMPLETE_RETRY_DELAY_S", 0.01),
    ):
        task = await asyncio.wait_for(_run_handler(client), timeout=15.0)
    assert task.done() and task.exception() is None

    complete_calls = [p for p in client.posts if p["url"].endswith("/complete")]
    assert len(complete_calls) >= 2, "completion retried within its window"
    assert all(
        p["json"].get("dropped_event_count") == 0 for p in complete_calls
    ), "completion failure must not count as a dropped event"
    assert all(
        p["json"].get("answer_dropped") is False for p in complete_calls
    ), "delivered events must not be flagged as answer_dropped"


@pytest.mark.asyncio
async def test_answer_dropped_flag_marks_lost_answer_event():
    """The completion body reports whether the answer event reached the
    gateway — the signal the gateway uses to reconcile a final answer."""

    class _EventsFail(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            self.calls += 1
            self.posts.append({"url": url, "headers": dict(headers or {}), "json": json})
            if url.endswith("/event"):
                raise httpx.ConnectError("[Errno -2] Name or service not known")
            return _response(200)

    client = _EventsFail()
    with (
        patch.object(gateway_module, "_COMPLETE_MAX_WAIT_S", 0.5),
        patch.object(gateway_module, "_COMPLETE_RETRY_DELAY_S", 0.01),
    ):
        task = await asyncio.wait_for(_run_handler(client), timeout=15.0)
    assert task.done() and task.exception() is None

    complete_calls = [p for p in client.posts if p["url"].endswith("/complete")]
    assert complete_calls, "completion must still be attempted"
    assert complete_calls[0]["json"]["answer_dropped"] is True
    assert complete_calls[0]["json"]["answer"] == "final"


# ---------------------------------------------------------------------------
# Poll backoff (deterministic, jitter pinned)
# ---------------------------------------------------------------------------


def test_poll_5xx_backoff_deterministic():
    """5xx polls must never kill the worker and must back off 2,4,8…"""
    sleep_calls: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    class _StopLoop(Exception):
        pass

    class _DegradedGateway:
        def __init__(self):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls > 3:
                raise _StopLoop()
            return _response(500)

        async def post(self, *_args, **_kwargs):  # pragma: no cover
            raise AssertionError("no work claimed → no posts")

    with (
        patch("asyncio.sleep", _fake_sleep),
        patch.object(gateway_module, "_retry_jitter", lambda: 1.0),
        patch("httpx.AsyncClient", return_value=_DegradedGateway()),
    ):
        with pytest.raises(_StopLoop):
            asyncio.run(gateway_module._run_http_only_worker(
                project_path=Path("/tmp/does-not-matter"),
                gateway_url="http://example.invalid",
                api_token="dummy",
                poll_timeout=0.05,
            ))

    assert sleep_calls == [2.0, 4.0, 8.0]


# ---------------------------------------------------------------------------
# Event idempotency (broker ledger)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_broker_mark_event_seen_dedupes_then_cleans():
    broker = http_worker_module.HttpWorkerBroker()
    waiter = asyncio.create_task(broker.enqueue_and_wait(
        session_id="s1", question="q", project_path="/tmp/p", timeout=None,
    ))
    await asyncio.sleep(0)  # let the enqueue publish into the pending list
    claimed = await broker.claim_next(tenant_id="default", timeout=1.0)
    assert claimed is not None
    assert claimed.session_id == "s1"

    ev = f"{claimed.work_id}:1"
    assert await broker.mark_event_seen(
        work_id=claimed.work_id, tenant_id="default", event_id=ev,
    ) is True
    assert await broker.mark_event_seen(
        work_id=claimed.work_id, tenant_id="default", event_id=ev,
    ) is False
    # unknown work → no ledger, treated as not-new (the endpoint 404s it first)
    assert await broker.mark_event_seen(
        work_id="nope", tenant_id="default", event_id=ev,
    ) is False
    # replay suppression is scoped per work id
    assert await broker.mark_event_seen(
        work_id=claimed.work_id, tenant_id="default", event_id=f"{claimed.work_id}:2",
    ) is True

    await broker.complete(
        work_id=claimed.work_id,
        tenant_id="default",
        result=http_worker_module.HttpWorkerResult(
            answer="a", event_count=1, dropped_event_count=2,
        ),
    )
    result = await asyncio.wait_for(
        asyncio.wait_for(waiter, timeout=1.0), timeout=1.0,
    )
    assert result.answer == "a"
    assert result.dropped_event_count == 2, "loss metric must survive the broker hop"
    # ledger is cleaned with the work item (could be re-claimed fresh after)
    assert broker._seen_events.get(claimed.work_id) is None


@pytest.mark.asyncio
async def test_lost_side_channel_event_is_replayed_in_completion():
    """F8: a dropped ask_user/visualization/upload_complete event carries its
    full payload in the completion so the gateway can replay it to the UI."""

    async def _clarify_inner(*_args, **_kwargs):
        yield {"type": "tool_start", "data": {"name": "read_skill"}}
        yield {"type": "tool_end", "data": {"name": "read_skill"}}
        yield {"type": "ask_user", "data": {"prompts": [{"question": "ERBA/ERLA?", "options": []}]}}
        yield {"type": "done", "data": ""}

    class _DropTypes(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            self.calls += 1
            self.posts.append({"url": url, "headers": dict(headers or {}), "json": json})
            if url.endswith("/event") and json.get("type") in ("ask_user", "done"):
                raise httpx.ConnectError("[Errno -2] lost")
            return _response(200)

    client = _DropTypes()
    with (
        patch.object(gateway_module, "_retry_jitter", lambda: 1.0),
        patch("seeknal.ask.gateway.server._run_agent_inner", _clarify_inner),
    ):
        await asyncio.wait_for(_run_handler(client, inner=_clarify_inner), timeout=15.0)

    complete_calls = [p for p in client.posts if p["url"].endswith("/complete")]
    body = complete_calls[0]["json"]
    assert body["answer_dropped"] is False, "no answer event existed in clarify turns"
    assert body["done_dropped"] is True
    replay = body["undelivered_events"]
    assert [e["type"] for e in replay] == ["ask_user"]
    assert replay[0]["data"]["prompts"][0]["question"] == "ERBA/ERLA?"


@pytest.mark.asyncio
async def test_delivered_side_channel_is_not_replayed():
    """When the chart event was delivered but later events dropped, no replay
    payload for it may appear in the completion."""
    class _DropAnswerDone(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            self.calls += 1
            self.posts.append({"url": url, "headers": dict(headers or {}), "json": json})
            if url.endswith("/event") and json.get("type") in ("answer", "done"):
                raise httpx.ConnectError("[Errno -2] lost")
            return _response(200)

    client = _DropAnswerDone()

    async def _chart_inner(*_args, **_kwargs):
        yield {"type": "visualization", "data": {"chart": "x"}}
        yield {"type": "answer", "data": "final"}
        yield {"type": "done", "data": ""}

    with (
        patch.object(gateway_module, "_retry_jitter", lambda: 1.0),
        patch("seeknal.ask.gateway.server._run_agent_inner", _chart_inner),
    ):
        await asyncio.wait_for(_run_handler(client, inner=_chart_inner), timeout=15.0)

    complete_calls = [p for p in client.posts if p["url"].endswith("/complete")]
    body = complete_calls[0]["json"]
    assert "undelivered_events" not in body, "delivered events must not replay"
    assert body["answer_dropped"] is True
    assert body["done_dropped"] is True


def test_bound_event_size_caps_oversized_payload():
    big = {"type": "visualization", "data": {"blob": "x" * 100_000}}
    bounded = gateway_module._bound_event_size(big)
    assert bounded.get("payload_truncated") is True
    assert "data" not in bounded
    assert gateway_module._bound_event_size({"type": "done", "data": ""})["data"] == ""


@pytest.mark.parametrize("event", [
    {"type": "visualization", "data": [{"x": i, "y": "x" * 40} for i in range(1000)]},
    {"type": "ask_user", "data": {"prompts": [{"question": "q" * 20000, "options": []}]}},
    {"type": "visualization", "data": {"title": "漢😀" * 10000}},
    {"type": "upload_complete", "data": {"url": "x" * 20000}},
])
def test_replay_payload_bound_preserves_schema_or_explicit_placeholder(event):
    import json

    bounded = gateway_module._bound_event_size(event)
    encoded = json.dumps(bounded, ensure_ascii=False).encode("utf-8")
    assert len(encoded) <= gateway_module._UNDELIVERED_EVENT_MAX_BYTES
    assert bounded["type"] == event["type"]
    assert bounded["payload_truncated"] is True
    assert bounded["original_bytes"] == len(
        json.dumps(event, ensure_ascii=False).encode("utf-8")
    )
    # Explicit omitted-data placeholders are allowed; strings replacing objects aren't.
    assert "data" not in bounded or type(bounded["data"]) is type(event["data"])
    if "data" in bounded and event["type"] == "ask_user":
        assert isinstance(bounded["data"]["prompts"], list)
        assert all(isinstance(p, dict) for p in bounded["data"]["prompts"])
    assert event["data"], "sizing must not mutate the source event"


def test_unicode_replay_within_utf8_bound_is_preserved():
    event = {"type": "ask_user", "data": {"prompts": [
        {"question": "漢" * 3000, "options": []},
    ]}}
    assert gateway_module._bound_event_size(event) == event


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transport", "http"])
@pytest.mark.parametrize("first_duration", [80.0, 89.9, 90.0])
async def test_completion_absolute_deadline(failure, first_duration):
    from types import SimpleNamespace

    now = 0.0
    requests = []
    sleeps = []

    async def sleep(seconds):
        nonlocal now
        sleeps.append(seconds)
        now += seconds

    class Client(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            nonlocal now
            if not url.endswith("/complete"):
                return _response(200)
            requests.append((now, timeout))
            now += first_duration if len(requests) == 1 else timeout
            if failure == "transport":
                raise httpx.ConnectError("offline")
            return httpx.Response(503, headers={"retry-after": "30"})

    with (
        patch.object(gateway_module, "time", SimpleNamespace(monotonic=lambda: now)),
        patch("asyncio.sleep", sleep),
    ):
        await _run_handler(Client())
    assert requests
    assert all(start < 90 and timeout <= 90 - start for start, timeout in requests)
    assert now <= 90.0
    assert all(seconds <= 90.0 - first_duration for seconds in sleeps)


@pytest.mark.asyncio
async def test_completion_deadline_rechecked_after_sleep_overhead():
    from types import SimpleNamespace

    now = 0.0
    attempts = []

    async def sleep(seconds):
        nonlocal now
        # The scheduler resumes us after the deadline, despite a short sleep.
        now += seconds + 90.0

    class Client(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            if url.endswith("/complete"):
                attempts.append(now)
                raise httpx.ConnectError("offline")
            return _response(200)

    with (
        patch.object(gateway_module, "time", SimpleNamespace(monotonic=lambda: now)),
        patch("asyncio.sleep", sleep),
    ):
        await _run_handler(Client())
    assert attempts == [0.0], "no new request may start after the deadline"


@pytest.mark.asyncio
async def test_cancelled_work_attempts_completion_once_with_short_timeout():
    waiting = asyncio.Event()
    calls = []

    async def inner(*_args, **_kwargs):
        yield {"type": "token", "data": "started"}
        waiting.set()
        await asyncio.Event().wait()

    class Client(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            calls.append((url, timeout, waiting.is_set()))
            raise httpx.ConnectError("offline")

    semaphore = asyncio.Semaphore(0)
    with (
        patch("seeknal.ask.gateway.server._run_agent_inner", inner),
        patch.object(gateway_module, "_EVENT_RETRY_BASE_S", 0.001),
        patch.object(gateway_module, "_TERMINAL_RETRY_DELAY_S", 0.001),
        patch.object(gateway_module, "_COMPLETE_MAX_WAIT_S", 0.08),
        patch.object(gateway_module, "_COMPLETE_RETRY_DELAY_S", 0.001),
    ):
        task = asyncio.create_task(_process_http_work_item(
            _work("w-cancel", "sess-cancel"), client=Client(),
            base_url="http://gateway.example", headers={},
            project_path=Path("/tmp/does-not-matter"), semaphore=semaphore,
        ))
        await asyncio.wait_for(waiting.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
    completions = [c for c in calls if c[0].endswith("/complete")]
    assert len(completions) == 1
    assert all(timeout <= 5 for _, timeout, cancelled in calls if cancelled)
    assert semaphore._value == 1


@pytest.mark.asyncio
async def test_shutdown_bounds_stalled_terminal_and_completion_posts():
    waiting = asyncio.Event()
    shutdown_calls = []
    real_sleep = asyncio.sleep

    async def inner(*_args, **_kwargs):
        waiting.set()
        await asyncio.Event().wait()
        yield {}  # pragma: no cover

    class Client(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            shutdown_calls.append(url)
            await real_sleep(10)  # ignores the httpx timeout argument

    semaphore = asyncio.Semaphore(0)
    with (
        patch("seeknal.ask.gateway.server._run_agent_inner", inner),
        patch.object(gateway_module, "_SHUTDOWN_DELIVERY_TIMEOUT_S", 0.02, create=True),
    ):
        task = asyncio.create_task(_process_http_work_item(
            _work("w-stalled", "sess-stalled"), client=Client(),
            base_url="http://gateway.example", headers={},
            project_path=Path("/tmp/does-not-matter"), semaphore=semaphore,
        ))
        await asyncio.wait_for(waiting.wait(), 1)
        task.cancel()
        try:
            done, _ = await asyncio.wait({task}, timeout=0.2)
            assert task in done, "shutdown delivery exceeded its short total budget"
            assert task.cancelled()
            assert len([u for u in shutdown_calls if u.endswith('/complete')]) == 1
            assert semaphore._value == 1
        finally:
            # Even baseline code needs a clean test teardown after the assertion fails.
            for _ in range(6):
                if task.done():
                    break
                task.cancel()
                await real_sleep(0)
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("stalled_type", ["error", "done", None])
async def test_shutdown_terminal_timeout_requests_done_reconciliation(stalled_type):
    """Request terminal replay when shutdown times out before done is acknowledged."""
    waiting = asyncio.Event()

    async def inner(*_args, **_kwargs):
        waiting.set()
        await asyncio.Event().wait()
        yield {}  # pragma: no cover

    class Client(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            if url.endswith("/event") and json["type"] == stalled_type:
                self.posts.append({"url": url, "json": json})
                await asyncio.Event().wait()
            return await super().post(url, headers=headers, json=json, timeout=timeout)

    client = Client()
    semaphore = asyncio.Semaphore(0)
    with (
        patch("seeknal.ask.gateway.server._run_agent_inner", inner),
        patch.object(gateway_module, "_SHUTDOWN_DELIVERY_TIMEOUT_S", 0.02),
    ):
        task = asyncio.create_task(_process_http_work_item(
            _work("w-terminal", "sess-terminal"), client=client,
            base_url="http://gateway.example", headers={},
            project_path=Path("/tmp/does-not-matter"), semaphore=semaphore,
        ))
        await asyncio.wait_for(waiting.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)

    completions = [p["json"] for p in client.posts if p["url"].endswith("/complete")]
    assert len(completions) == 1, "completion must succeed in one shutdown attempt"
    payload = completions[0]
    if stalled_type is not None:
        assert payload.get("done_dropped") is True
        assert payload["dropped_event_count"] == 1
        events = [p["json"]["type"] for p in client.posts if p["url"].endswith("/event")]
        assert events == (["error"] if stalled_type == "error" else ["error", "done"])
    else:
        assert "done_dropped" not in payload
        assert payload["dropped_event_count"] == 0
    assert semaphore._value == 1


@pytest.mark.asyncio
async def test_drainer_bounds_final_gather_for_slow_cancellation():
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_cleanup():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    task = asyncio.create_task(slow_cleanup())
    await started.wait()
    with patch.object(gateway_module, "_CANCEL_DRAIN_TIMEOUT_S", 0.02, create=True):
        drainer = asyncio.create_task(gateway_module._drain_or_cancel_tasks({task}, 0))
        try:
            done, _ = await asyncio.wait({drainer}, timeout=0.2)
            assert drainer in done, "drainer's final gather has no upper bound"
            await drainer
        finally:
            release.set()
            await asyncio.gather(task, drainer, return_exceptions=True)


@pytest.mark.asyncio
async def test_delivery_degraded_mode_drops_tokens_and_recovers():
    posts = []
    slept = []
    recovered = False

    async def sleep(seconds):
        slept.append(seconds)

    async def inner(*_args, **_kwargs):
        for i in range(50):
            yield {"type": "token", "data": str(i)}
        yield {"type": "answer", "data": "answer"}
        yield {"type": "ask_user", "data": {"prompts": [{"question": "q"}]}}
        yield {"type": "token", "data": "after recovery"}

    class Client(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            nonlocal recovered
            if url.endswith("/complete"):
                self.completion = json
                return _response(200)
            posts.append(json)
            if json["type"] == "ask_user":
                recovered = True
            if recovered:
                return _response(200)
            raise httpx.ConnectError("offline")

    client = Client()
    with patch("asyncio.sleep", sleep):
        await _run_handler(client, inner=inner)
    tokens = [p["data"] for p in posts if p["type"] == "token"]
    assert tokens == ["0"] * 3 + ["after recovery"]
    assert len([p for p in posts if p["type"] == "answer"]) == 3
    assert len(slept) == 4
    assert sum(slept) == 3.0
    assert client.completion["dropped_event_count"] == 51
    assert client.completion["answer_dropped"] is True


@pytest.mark.parametrize("status, level", [(401, "ERROR"), (403, "ERROR"), (503, "WARNING")])
def test_poll_failures_remain_visible_and_log_recovery(caplog, status, level):
    import logging

    class StopLoop(Exception):
        pass

    class Client(_RecordingClient):
        async def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls <= 11:
                return _response(status)
            if self.calls <= 13:
                return _response(204)
            raise StopLoop()

    async def sleep(_seconds):
        pass

    client = Client()
    with (
        caplog.at_level(logging.INFO, logger=gateway_module.__name__),
        patch("httpx.AsyncClient", return_value=client),
        patch("asyncio.sleep", sleep),
        patch.object(gateway_module, "_retry_jitter", lambda: 1),
    ):
        with pytest.raises(StopLoop):
            asyncio.run(gateway_module._run_http_only_worker(
                project_path=Path("/tmp/does-not-matter"),
                gateway_url="http://gateway.example", api_token="t",
            ))
    failures = [r for r in caplog.records if "retrying" in r.message and r.levelname == level]
    assert len(failures) >= 4, "initial failures and every tenth attempt must remain visible"
    assert any("attempt=10" in r.message for r in failures)
    if status in (401, 403):
        assert all("worker token rejected" in r.message for r in failures)
        assert all("SEEKNAL_API_TOKEN" in r.message for r in failures)
    recovered = [r for r in caplog.records if "poll recovered after 11 attempts" in r.message]
    assert len(recovered) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("types, expected, evicted", [
    (["ask_user"] + ["visualization"] * 6, ["ask_user", "3", "4", "5", "6"], 2),
    (["upload_complete", "visualization"] + ["upload_complete"] * 4,
     ["0", "2", "3", "4", "5"], 1),
    (["ask_user"] * 6, ["1", "2", "3", "4", "5"], 1),
])
async def test_replay_buffer_evicts_charts_before_clarifications(types, expected, evicted):
    async def inner(*_args, **_kwargs):
        for i, event_type in enumerate(types):
            yield {"type": event_type, "data": {"id": str(i)}}

    class Client(_RecordingClient):
        async def post(self, url, *, headers=None, json=None, timeout=None):
            if url.endswith("/event") and json["type"] in gateway_module._REPLAYABLE_EVENT_TYPES:
                raise httpx.ConnectError("offline")
            if url.endswith("/complete"):
                self.completion = json
            return _response(200)

    async def sleep(_seconds):
        pass

    client = Client()
    with patch("asyncio.sleep", sleep):
        await _run_handler(client, inner=inner)
    replay = client.completion["undelivered_events"]
    actual = [e["type"] if e["type"] == "ask_user" and types.count("ask_user") == 1
              else e["data"]["id"] for e in replay]
    assert actual == expected
    assert client.completion["undelivered_evicted_count"] == evicted

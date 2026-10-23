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


async def _run_handler(client, work_id="w-10010010", session_id="sess-1"):
    semaphore = asyncio.Semaphore(1)
    started = asyncio.Event()

    async def _notify(*_a, **_k):
        started.set()

    with (
        patch.object(gateway_module, "_retry_jitter", lambda: 1.0),
        patch("seeknal.ask.gateway.server._run_agent_inner", _stub_inner),
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
    with patch.object(gateway_module, "_COMPLETE_MAX_WAIT_S", 0.5):
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

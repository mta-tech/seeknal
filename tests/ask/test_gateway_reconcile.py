"""Regression coverage for HTTP worker delivery and completion reconciliation."""

import asyncio
import json

import httpx
import pytest

from seeknal.ask.gateway.http_worker import HttpWorkerBroker, HttpWorkerResult
from seeknal.ask.gateway.redis_backend import RedisSSEBroadcaster
from seeknal.ask.gateway.server import create_gateway_app
from seeknal.ask.gateway.sse import SSEBroadcaster


@pytest.fixture
async def work(tmp_path, monkeypatch):
    broker = HttpWorkerBroker()
    monkeypatch.setattr("seeknal.ask.gateway.http_worker.http_worker_broker", broker)
    app = create_gateway_app(project_path=None, sessions_dir=tmp_path, worker_transport="http")
    app.state.broadcaster = SSEBroadcaster()
    queue = app.state.broadcaster.subscribe("session", tenant_id="tenant")
    task = asyncio.create_task(broker.enqueue_and_wait(
        session_id="session", tenant_id="tenant", question="question", project_path=str(tmp_path),
    ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://gateway",
        headers={"X-Tenant-ID": "tenant"},
    ) as client:
        response = await client.get("/internal/worker/work-stream?timeout=1")
        assert response.status_code == 200
        url = f"/internal/worker/work/{response.json()['work_id']}"
        try:
            yield client, app, queue, task, url
        finally:
            await broker.reset()
            await asyncio.gather(task, return_exceptions=True)


async def test_failed_publish_releases_event_id_for_retry(work, monkeypatch):
    client, app, queue, _, url = work
    publish = app.state.broadcaster.publish
    attempts = 0

    async def fail_first(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("failed before publish")
        await publish(*args, **kwargs)

    monkeypatch.setattr(app.state.broadcaster, "publish", fail_first)
    event = {"type": "answer", "data": "hello"}
    headers = {"X-Seeknal-Event-Id": "event-1"}
    first = await client.post(f"{url}/event", json=event, headers=headers)
    assert first.status_code == 500
    retry = await client.post(f"{url}/event", json=event, headers=headers)
    assert retry.json() == {"status": "published"}
    assert attempts == 2
    assert json.loads(queue.get_nowait()) == event
    assert queue.empty()


class _FailingRedis:
    def __init__(self, failures=1):
        self.failures = failures
        self.attempts = 0
        self.accepted = []

    def pubsub(self):
        return object()

    async def publish(self, channel, event):
        self.attempts += 1
        if self.attempts <= self.failures:
            raise ConnectionError("injected Redis publish failure")
        self.accepted.append((channel, json.loads(event)))
        # Zero subscribers still means Redis accepted the publication.
        return 0


async def test_redis_publish_failure_releases_event_id_for_retry(work):
    client, app, _, _, url = work
    redis = _FailingRedis()
    app.state.broadcaster = RedisSSEBroadcaster(redis)
    event = {"type": "answer", "data": "hello"}
    headers = {"X-Seeknal-Event-Id": "redis-event-1"}

    failed = await client.post(f"{url}/event", json=event, headers=headers)
    assert failed.status_code == 500
    assert redis.accepted == []
    retry = await client.post(f"{url}/event", json=event, headers=headers)
    assert retry.json() == {"status": "published"}
    assert redis.accepted == [("seeknal:sse:tenant:session", event)]
    lost_ack_retry = await client.post(f"{url}/event", json=event, headers=headers)
    assert lost_ack_retry.json() == {"status": "duplicate"}
    assert redis.attempts == 2
    assert len(redis.accepted) == 1


async def test_redis_publish_failure_leaves_completion_replay_retryable(work):
    client, app, _, task, url = work
    redis = _FailingRedis()
    app.state.broadcaster = RedisSSEBroadcaster(redis)
    payload = {"answer": "saved", "answer_dropped": True, "done_dropped": True}
    failed = await client.post(f"{url}/complete", json=payload)
    assert failed.status_code == 500
    assert redis.accepted == []
    assert not task.done()
    retry = await client.post(f"{url}/complete", json=payload)
    assert retry.status_code == 200
    assert [event for _, event in redis.accepted] == [
        {"type": "answer", "data": "saved"}, {"type": "done"},
    ]
    assert (await task).answer == "saved"


async def test_redis_outage_keeps_agent_stream_best_effort(tmp_path, monkeypatch):
    import seeknal.ask.gateway.server as server

    async def inner(*args, **kwargs):
        yield {"type": "answer", "data": "still available to consumer"}

    monkeypatch.setattr(server, "_run_agent_inner", inner)
    redis = _FailingRedis(failures=100)
    events = [event async for event in server._run_agent_streaming(
        tmp_path, "redis-outage", "question", broadcaster=RedisSSEBroadcaster(redis),
    )]
    assert events[0] == {"type": "answer", "data": "still available to consumer"}
    assert [event["type"] for event in events] == ["answer", "done"]
    assert redis.attempts == 2
    assert redis.accepted == []


async def test_redis_outage_keeps_legacy_callback_and_cancel_best_effort(work):
    import seeknal.ask.gateway.server as server

    client, app, _, _, _ = work
    redis = _FailingRedis(failures=100)
    app.state.broadcaster = RedisSSEBroadcaster(redis)
    callback = await client.post(
        "/internal/events/redis-outage/publish", json={"type": "token", "data": "hello"},
    )
    assert callback.status_code == 200
    assert callback.json() == {"status": "published"}
    try:
        cancel = await client.post("/sessions/redis-outage/cancel")
        assert cancel.status_code == 200
        assert server._is_cancelled("redis-outage", tenant_id="tenant")
    finally:
        server._clear_cancel("redis-outage", tenant_id="tenant")
    assert redis.attempts == 2
    assert redis.accepted == []


async def test_accepted_event_with_lost_ack_is_not_republished(work):
    client, _, queue, _, url = work
    event = {"type": "token", "data": "hello"}
    headers = {"X-Seeknal-Event-Id": "event-1"}
    # Discard the first response as if its ACK had been lost in transit.
    await client.post(f"{url}/event", json=event, headers=headers)
    retry = await client.post(f"{url}/event", json=event, headers=headers)
    assert retry.json() == {"status": "duplicate"}
    assert json.loads(queue.get_nowait()) == event
    assert queue.empty()


async def test_cancelled_publish_releases_event_id_for_retry(work, monkeypatch):
    client, app, queue, _, url = work
    publish = app.state.broadcaster.publish
    started = asyncio.Event()

    async def blocked(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app.state.broadcaster, "publish", blocked)
    event = {"type": "answer", "data": "hello"}
    headers = {"X-Seeknal-Event-Id": "event-1"}
    first = asyncio.create_task(client.post(f"{url}/event", json=event, headers=headers))
    await started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    monkeypatch.setattr(app.state.broadcaster, "publish", publish)
    retry = await client.post(f"{url}/event", json=event, headers=headers)
    assert retry.json() == {"status": "published"}
    assert json.loads(queue.get_nowait()) == event
    assert queue.empty()


async def test_events_without_id_keep_legacy_delivery(work):
    client, _, queue, _, url = work
    event = {"type": "token", "data": "hello"}
    for _ in range(2):
        response = await client.post(f"{url}/event", json=event)
        assert response.json() == {"status": "published"}
        assert json.loads(queue.get_nowait()) == event


@pytest.mark.parametrize("first_fails", [False, True])
async def test_concurrent_event_duplicate_is_retryable_until_publish_finishes(
    work, monkeypatch, first_fails,
):
    client, app, queue, _, url = work
    publish = app.state.broadcaster.publish
    started = asyncio.Event()
    release = asyncio.Event()
    attempts = 0

    async def blocked(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            started.set()
            await release.wait()
            if first_fails:
                raise RuntimeError("first publish failed")
        await publish(*args, **kwargs)

    monkeypatch.setattr(app.state.broadcaster, "publish", blocked)
    event = {"type": "token", "data": "hello"}
    headers = {"X-Seeknal-Event-Id": "event-1"}
    first = asyncio.create_task(client.post(f"{url}/event", json=event, headers=headers))
    await started.wait()
    try:
        duplicate = await client.post(f"{url}/event", json=event, headers=headers)
    finally:
        release.set()
        first_response = await first
    assert duplicate.status_code == 503
    assert first_response.status_code == (500 if first_fails else 200)
    retry = await client.post(f"{url}/event", json=event, headers=headers)
    assert retry.json()["status"] == ("published" if first_fails else "duplicate")
    assert json.loads(queue.get_nowait()) == event
    assert queue.empty()


@pytest.mark.parametrize("payload, expected", [
    ({"undelivered_events": [
        {"type": "ask_user", "data": {"question": "Which?"}},
        {"type": "visualization", "data": [], "payload_truncated": True},
    ]}, [
        {"type": "ask_user", "data": {"question": "Which?"}},
        {"type": "visualization", "data": [], "payload_truncated": True},
    ]),
    ({"answer": "saved answer", "answer_dropped": True},
     [{"type": "answer", "data": "saved answer"}]),
    ({"answer": "", "answer_dropped": True}, []),
    ({"answer": "already delivered"}, []),
    ({"done_dropped": True}, [{"type": "done"}]),
    ({
        "undelivered_events": [{"type": "upload_complete", "data": {"url": "artifact"}}],
        "answer": "saved answer", "answer_dropped": True, "done_dropped": True,
    }, [
        {"type": "upload_complete", "data": {"url": "artifact"}},
        {"type": "answer", "data": "saved answer"},
        {"type": "done"},
    ]),
])
async def test_completion_reconciles_to_session_in_order_and_retains_fields(work, payload, expected):
    client, _, queue, task, url = work
    response = await client.post(f"{url}/complete", json=payload)
    assert response.status_code == 200
    received = []
    while not queue.empty():
        received.append(json.loads(queue.get_nowait()))
    assert received == expected
    result = await task
    assert result.answer == payload.get("answer", "")
    assert result.answer_dropped == payload.get("answer_dropped", False)
    assert result.done_dropped == payload.get("done_dropped", False)
    assert result.undelivered_events == payload.get("undelivered_events", [])


@pytest.mark.parametrize("value", ["malformed", {}, [], {"bad": 1}, [1], None, "", -1])
async def test_completion_malformed_dropped_count_defaults_to_zero(work, value):
    client, _, _, task, url = work
    response = await client.post(f"{url}/complete", json={"dropped_event_count": value})
    assert response.status_code == 200
    assert (await task).dropped_event_count == 0


async def test_completion_retains_valid_counts_and_error(work):
    client, _, _, task, url = work
    response = await client.post(f"{url}/complete", json={
        "answer": "partial", "event_count": 7, "dropped_event_count": "3", "error": "stopped",
    })
    assert response.status_code == 200
    result = await task
    assert result.event_count == 7
    assert result.dropped_event_count == 3
    assert result.error == "stopped"


@pytest.mark.parametrize("value, expected", [(2, 2), ("3", 3), ("bad", 0), ({"bad": 1}, 0)])
async def test_completion_retains_defensive_replay_eviction_count(work, value, expected):
    client, _, queue, task, url = work
    response = await client.post(f"{url}/complete", json={"undelivered_evicted_count": value})
    assert response.status_code == 200
    assert (await task).undelivered_evicted_count == expected
    assert queue.empty()


async def test_completion_cannot_replay_another_tenants_work(work):
    client, _, queue, task, url = work
    response = await client.post(
        f"{url}/complete", headers={"X-Tenant-ID": "other"},
        json={"answer": "secret", "answer_dropped": True, "done_dropped": True},
    )
    assert response.status_code == 404
    assert queue.empty()
    assert not task.done()


async def test_completion_retry_does_not_repeat_already_reconciled_events(work, monkeypatch):
    client, app, queue, task, url = work
    publish = app.state.broadcaster.publish
    failed = False

    async def fail_answer_once(session_id, event, **kwargs):
        nonlocal failed
        if json.loads(event)["type"] == "answer" and not failed:
            failed = True
            raise RuntimeError("answer publish failed")
        await publish(session_id, event, **kwargs)

    monkeypatch.setattr(app.state.broadcaster, "publish", fail_answer_once)
    payload = {
        "undelivered_events": [{"type": "ask_user", "data": {"question": "Which?"}}],
        "answer": "saved", "answer_dropped": True, "done_dropped": True,
    }
    first = await client.post(f"{url}/complete", json=payload)
    assert first.status_code == 500
    assert not task.done()
    retry = await client.post(f"{url}/complete", json=payload)
    assert retry.status_code == 200
    assert [json.loads(queue.get_nowait())["type"] for _ in range(queue.qsize())] == [
        "ask_user", "answer", "done",
    ]
    await task


def test_http_worker_result_reconciliation_defaults_are_additive():
    result = HttpWorkerResult()
    assert result.answer_dropped is False
    assert result.done_dropped is False
    assert result.undelivered_events == []
    assert result.undelivered_evicted_count == 0

"""Tests for the in-process SSE broadcaster (P2-6).

In HTTP-worker mode there are no SSE subscribers for a run's session id --
the worker process never serves ``/events/{session_id}``, so nothing ever
calls ``SSEBroadcaster.subscribe``. ``_publish_event_async`` is still called
once per agent event with ``broadcaster=None`` (the default when
``_run_agent_streaming`` gets no explicit broadcaster, i.e. HTTP-worker
mode), which falls through to the module-global ``sse_broadcaster.publish_sync``.
The security review asked whether that in-process structure is bounded, or
whether every event of every run accumulates in it with nothing ever
draining it -- a slow leak for a long-running worker.

It is already bounded: ``_subscribers`` is a ``defaultdict(list)``, but both
``publish`` and ``publish_sync`` read it with ``.get(key, [])`` rather than
``self._subscribers[key]`` -- ``.get`` never triggers the defaultdict's
default-factory, so a session id with no subscriber never gets an entry at
all. These tests pin that down directly (no code change made for this
finding) and would go red under the natural regression: swapping ``.get``
for ``[]``-indexing, which *would* insert an empty-list entry per distinct
session id even with zero subscribers.
"""

from __future__ import annotations

import pytest

from seeknal.ask.gateway.sse import SSEBroadcaster


def test_publish_sync_with_no_subscribers_retains_nothing():
    broadcaster = SSEBroadcaster()

    for i in range(10_000):
        broadcaster.publish_sync("session-1", f"event-{i}", tenant_id="tenant-a")

    # Internal state is the only place a leak could show up: there is no
    # public "how many buffered events" accessor, because there is nothing
    # to buffer in the zero-subscriber case this finding is about.
    assert broadcaster._subscribers == {}


@pytest.mark.asyncio
async def test_publish_async_with_no_subscribers_retains_nothing():
    broadcaster = SSEBroadcaster()

    for i in range(10_000):
        await broadcaster.publish("session-1", f"event-{i}", tenant_id="tenant-a")

    assert broadcaster._subscribers == {}


def test_publish_sync_across_many_distinct_sessions_retains_nothing():
    """Not just one hot session id -- many distinct ones, still nothing kept.

    A long-running worker processes many different session ids over its
    lifetime, so this is the shape that would actually leak if ``.get``
    were ever replaced by defaultdict ``[]``-indexing (one empty-list entry
    created per distinct key, even though it's never populated).
    """
    broadcaster = SSEBroadcaster()

    for i in range(10_000):
        broadcaster.publish_sync(f"session-{i}", "event", tenant_id="tenant-a")

    assert broadcaster._subscribers == {}


def test_publish_sync_still_delivers_to_an_actual_subscriber():
    """Positive control: the zero-subscriber discard above is specific to
    having no subscribers, not ``publish_sync`` silently dropping everything.
    """
    broadcaster = SSEBroadcaster()
    queue = broadcaster.subscribe("session-1", tenant_id="tenant-a")

    broadcaster.publish_sync("session-1", "hello", tenant_id="tenant-a")

    assert queue.get_nowait() == "hello"


@pytest.mark.asyncio
async def test_worker_mode_publish_event_async_with_default_broadcaster_retains_nothing(
    monkeypatch,
):
    """Drives the actual worker-mode call boundary, not just SSEBroadcaster
    in isolation: ``_publish_event_async(..., broadcaster=None)`` is exactly
    what ``_run_agent_streaming`` calls for every event in HTTP-worker mode
    (``_process_http_work_item`` never passes a ``broadcaster``), and it
    falls through to the module-global ``sse_broadcaster.publish_sync``. A
    fresh broadcaster is swapped in for the duration of the test so this
    neither depends on nor pollutes global state shared with other tests.
    """
    from seeknal.ask.gateway import server as server_module

    fresh = SSEBroadcaster()
    monkeypatch.setattr(server_module, "sse_broadcaster", fresh)

    for i in range(10_000):
        await server_module._publish_event_async(
            "session-1",
            {"type": "answer", "data": f"event-{i}"},
            tenant_id="tenant-a",
            broadcaster=None,
        )

    assert fresh._subscribers == {}

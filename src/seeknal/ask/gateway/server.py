"""Seeknal Ask Gateway — Starlette ASGI server.

Provides WebSocket, SSE, and REST endpoints for interacting with
the seeknal ask agent over HTTP. Uses pydantic-ai's agent.iter()
for streaming responses.

Usage:
    seeknal gateway start --project path --port 8000
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any, Callable

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, StreamingResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

from seeknal.ask.gateway.session_manager import SessionManager
from seeknal.ask.gateway.sse import SSEBroadcaster

# Validate session IDs: alphanumeric, hyphens, underscores, max 128 chars
_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")

session_manager = SessionManager()
sse_broadcaster = SSEBroadcaster()

# Per-session locks to serialize concurrent runs on the same session_id.
# Prevents message history corruption (last-writer-wins on JSON files)
# and garbled SSE event streams from interleaved responses.
# Uses LRU eviction to prevent unbounded memory growth.
_session_locks: dict[str, tuple[asyncio.Lock, float]] = {}
_LOCK_IDLE_TTL = 1800  # 30 minutes


def _get_session_lock(session_id: str) -> asyncio.Lock:
    """Get or create an asyncio.Lock for a session_id with LRU tracking."""
    now = time.monotonic()
    if session_id in _session_locks:
        lock, _ = _session_locks[session_id]
        _session_locks[session_id] = (lock, now)
        return lock
    lock = asyncio.Lock()
    _session_locks[session_id] = (lock, now)
    return lock


def _evict_idle_locks() -> int:
    """Remove locks that have been idle for longer than _LOCK_IDLE_TTL.

    Returns the number of evicted entries.
    """
    now = time.monotonic()
    stale = [
        sid for sid, (lock, last_access) in _session_locks.items()
        if now - last_access > _LOCK_IDLE_TTL and not lock.locked()
    ]
    for sid in stale:
        del _session_locks[sid]
    return len(stale)


def _validate_session_id(session_id: str) -> bool:
    return bool(_SESSION_ID_RE.match(session_id))


def _publish_event(session_id: str, event: dict) -> None:
    """Publish an event to all SSE subscribers for this session."""
    sse_broadcaster.publish_sync(session_id, json.dumps(event))


# ---------------------------------------------------------------------------
# Agent execution (shared by WebSocket, SSE, and REST)
# ---------------------------------------------------------------------------


async def _run_agent_streaming(
    project_path: Path,
    session_id: str,
    question: str,
    provider: str | None = None,
    model: str | None = None,
):
    """Run agent and yield JSON event dicts as they occur.

    This generator is the sole owner of SSE broadcast — every event
    yielded is also published to ``sse_broadcaster`` so that SSE
    subscribers on ``/events/{session_id}`` receive events in real-time.

    A per-session ``asyncio.Lock`` serializes concurrent runs on the
    same ``session_id`` to prevent message history corruption.
    """
    lock = _get_session_lock(session_id)
    async with lock:
        try:
            async for event in _run_agent_inner(
                project_path, session_id, question, provider, model,
            ):
                _publish_event(session_id, event)
                yield event
        except Exception as exc:
            error_event = {"type": "error", "data": str(exc)}
            _publish_event(session_id, error_event)
            yield error_event
            raise
        finally:
            done_event = {"type": "done", "data": ""}
            _publish_event(session_id, done_event)
            yield done_event


async def _run_agent_inner(
    project_path: Path,
    session_id: str,
    question: str,
    provider: str | None = None,
    model: str | None = None,
):
    """Inner agent execution without locking or SSE publishing."""
    from pydantic_ai import Agent
    from pydantic_ai._agent_graph import End, UserPromptNode
    from pydantic_ai.messages import (
        FunctionToolCallEvent,
        FunctionToolResultEvent,
        PartDeltaEvent,
        TextPartDelta,
    )

    from seeknal.ask.agents.agent import create_agent
    from seeknal.ask.sessions import SessionStore

    store = SessionStore(project_path)
    if store.get(session_id) is None:
        store.create(name=session_id)

    message_history = store.load_messages(session_id)

    agent, deps, _, _ = create_agent(
        project_path, provider=provider, model=model,
        environment="gateway",
    )

    text_buffer: list[str] = []

    async with agent.iter(
        question, deps=deps, message_history=message_history,
    ) as run:
        async for node in run:
            if isinstance(node, UserPromptNode):
                continue

            elif Agent.is_model_request_node(node):
                async with node.stream(run.ctx) as stream:
                    async for event in stream:
                        if isinstance(event, PartDeltaEvent):
                            if isinstance(event.delta, TextPartDelta):
                                text_buffer.append(event.delta.content_delta)
                                yield {
                                    "type": "token",
                                    "data": event.delta.content_delta,
                                }

            elif Agent.is_call_tools_node(node):
                # Flush reasoning
                if text_buffer:
                    yield {
                        "type": "reasoning",
                        "data": "".join(text_buffer),
                    }
                    text_buffer.clear()

                async with node.stream(run.ctx) as handle_stream:
                    async for event in handle_stream:
                        if isinstance(event, FunctionToolCallEvent):
                            yield {
                                "type": "tool_start",
                                "data": {
                                    "name": event.part.tool_name,
                                    "args": event.part.args_as_dict(),
                                },
                            }
                        elif isinstance(event, FunctionToolResultEvent):
                            content = event.result.content
                            yield {
                                "type": "tool_end",
                                "data": {
                                    "name": event.result.tool_name,
                                    "output": str(content)[:2000] if content else "",
                                },
                            }

            elif isinstance(node, End):
                break

        result = run.result

    # Final answer
    if text_buffer:
        answer = "".join(text_buffer)
    else:
        answer = result.output or ""

    if answer:
        yield {"type": "answer", "data": answer}

    # Save conversation
    all_messages = result.all_messages()
    store.save_messages(session_id, all_messages)
    msg_count = len([m for m in all_messages if hasattr(m, 'parts')])
    store.update(
        session_id,
        message_count=msg_count,
        last_question=question[:200],
        status="active",
    )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "seeknal-ask-gateway"})


async def list_sessions(request: Request) -> JSONResponse:
    project_path = Path(request.app.state.project_path)
    from seeknal.ask.sessions import SessionStore

    with SessionStore(project_path) as store:
        sessions = store.list()
    return JSONResponse(sessions)


async def ask_oneshot(request: Request) -> JSONResponse:
    """One-shot question → JSON response."""
    body = await request.json()
    question = body.get("question", "")
    session_id = body.get("session_id", "default")
    if not question:
        return JSONResponse({"error": "question is required"}, status_code=400)
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    project_path = Path(request.app.state.project_path)
    events = []
    answer = ""
    async for event in _run_agent_streaming(project_path, session_id, question):
        events.append(event)
        if event["type"] == "answer":
            answer = event["data"]

    return JSONResponse({"answer": answer, "events": events})


async def websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket streaming per session."""
    session_id = websocket.path_params["session_id"]
    if not _validate_session_id(session_id):
        await websocket.close(code=4000, reason="invalid session_id")
        return

    await websocket.accept()
    project_path = Path(websocket.app.state.project_path)
    await session_manager.connect(session_id, websocket)

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            question = msg.get("question", "")
            if not question:
                await websocket.send_text(
                    json.dumps({"type": "error", "data": "question is required"})
                )
                continue

            async for event in _run_agent_streaming(
                project_path, session_id, question,
            ):
                await websocket.send_text(json.dumps(event))

    except WebSocketDisconnect:
        pass
    finally:
        await session_manager.disconnect(session_id, websocket)


async def sse_endpoint(request: Request) -> StreamingResponse:
    """SSE event stream for a session.

    Sends keepalive events every 30 seconds during idle periods.
    Stays open across multiple turns — client decides when to close.
    Safety-valve timeout (10 minutes idle) prevents infinite hangs.
    """
    session_id = request.path_params["session_id"]
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    queue = sse_broadcaster.subscribe(session_id)

    # Safety-valve: max 10 minutes total to prevent infinite hangs
    _MAX_SSE_DURATION = 600  # seconds
    _KEEPALIVE_INTERVAL = 30  # seconds

    async def event_generator():
        import time

        start = time.monotonic()
        try:
            while True:
                # Check safety-valve timeout
                elapsed = time.monotonic() - start
                if elapsed >= _MAX_SSE_DURATION:
                    yield 'data: {"type": "keepalive"}\n\n'
                    break

                # Check client disconnect
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(
                        queue.get(), timeout=_KEEPALIVE_INTERVAL,
                    )
                    yield f"data: {event}\n\n"

                    # Reset safety-valve timer on each event (idle timeout, not total)
                    start = time.monotonic()

                except asyncio.TimeoutError:
                    # Send keepalive and continue loop
                    yield 'data: {"type": "keepalive"}\n\n'

        except asyncio.CancelledError:
            pass
        finally:
            sse_broadcaster.unsubscribe(session_id, queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Session history endpoint
# ---------------------------------------------------------------------------


async def session_history(request: Request) -> JSONResponse:
    """Return conversation history as raw SSE-format events for UI replay.

    Converts pydantic-ai message objects into the same event format
    that ``_run_agent_inner`` yields during live streaming.
    """
    session_id = request.path_params["session_id"]
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    project_path = Path(request.app.state.project_path)

    from seeknal.ask.sessions import SessionStore

    store = SessionStore(project_path)
    if store.get(session_id) is None:
        return JSONResponse([])

    messages = store.load_messages(session_id)
    if not messages:
        return JSONResponse([])

    events = _messages_to_events(messages)
    return JSONResponse(events)


def _messages_to_events(messages: list) -> list[dict]:
    """Convert pydantic-ai message history to SSE event format."""
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        ToolCallPart,
        ToolReturnPart,
        UserPromptPart,
    )

    events: list[dict] = []

    for msg in messages:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, UserPromptPart):
                    events.append({"type": "user_message", "data": part.content})
                elif isinstance(part, ToolReturnPart):
                    events.append({
                        "type": "tool_end",
                        "data": {
                            "name": part.tool_name,
                            "output": str(part.content)[:2000] if part.content else "",
                        },
                    })
        elif isinstance(msg, ModelResponse):
            for part in msg.parts:
                if isinstance(part, TextPart):
                    events.append({"type": "answer", "data": part.content})
                elif isinstance(part, ToolCallPart):
                    events.append({
                        "type": "tool_start",
                        "data": {
                            "name": part.tool_name,
                            "args": part.args_as_dict() if hasattr(part, 'args_as_dict') else {},
                        },
                    })

    return events


# ---------------------------------------------------------------------------
# Temporal endpoint
# ---------------------------------------------------------------------------


async def temporal_start(request: Request) -> JSONResponse:
    """Start an agent workflow via Temporal.

    Requires the gateway to have been started with ``--temporal``.
    Returns 503 if Temporal is not enabled or connected.
    """
    if not hasattr(request.app.state, "temporal_client") or not request.app.state.temporal_client:
        return JSONResponse(
            {"error": "Temporal not enabled. Start gateway with --temporal"},
            status_code=503,
        )

    body = await request.json()
    question = body.get("question", "")
    session_id = body.get("session_id", "default")
    if not question:
        return JSONResponse({"error": "question is required"}, status_code=400)
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    from seeknal.ask.gateway.temporal import AgentWorkflowInput, AgentWorkflow

    import time

    # Project path: request body > worker_project_path > app config
    project_path = str(
        body.get("project_path")
        or getattr(request.app.state, "worker_project_path", None)
        or request.app.state.project_path
    )

    # Push URL: request body > app config (allows external services to override SSE delivery)
    push_url = (
        body.get("push_url")
        or getattr(request.app.state, "callback_base_url", None)
    )
    api_key = (
        body.get("api_key")
        or getattr(request.app.state, "callback_auth_token", None)
    )

    task_queue = body.get("task_queue") or getattr(request.app.state, "temporal_task_queue", "seeknal-ask")
    workflow_id = f"ask-{session_id}-{int(time.time())}"

    client = request.app.state.temporal_client
    handle = await client.start_workflow(
        AgentWorkflow.run,
        AgentWorkflowInput(
            session_id=session_id,
            question=question,
            project_path=project_path,
            callback_url=push_url,
            callback_auth_token=api_key,
        ),
        id=workflow_id,
        task_queue=task_queue,
    )

    return JSONResponse({
        "workflow_id": handle.id,
        "session_id": session_id,
    })


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


async def chat_ui(request: Request) -> FileResponse:
    """Serve the chat UI HTML file."""
    static_dir = Path(__file__).parent / "static"
    return FileResponse(static_dir / "chat.html", media_type="text/html")


async def publish_event_callback(request: Request) -> JSONResponse:
    """Receive streaming chunk from on-prem worker and publish to SSE.

    Called by the on-prem Temporal worker via HTTP POST for each streaming
    event (token, tool_start, tool_end, answer, done).
    Authenticated via shared secret (CALLBACK_AUTH_TOKEN env var).
    """
    # Authenticate callback request
    expected_token = getattr(request.app.state, "callback_auth_token", None)
    if expected_token:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer ") or auth_header[7:] != expected_token:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

    session_id = request.path_params["session_id"]
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    body = await request.json()
    event_json = json.dumps(body)

    broadcaster = request.app.state.broadcaster
    await broadcaster.publish(session_id, event_json)

    return JSONResponse({"status": "published"})


def create_gateway_app(
    project_path: str | Path,
    lifespan: Callable | None = None,
    temporal_client: Any = None,
    redis_url: str | None = None,
    callback_base_url: str | None = None,
    callback_auth_token: str | None = None,
) -> Starlette:
    """Create the Starlette ASGI application.

    Args:
        project_path: Path to the seeknal project directory.
        lifespan: Optional async context manager for startup/shutdown hooks.
            Receives the ``Starlette`` app instance as its argument.
        temporal_client: Optional connected Temporal client for workflow
            submission via ``POST /temporal/start``.
        redis_url: Optional Redis URL for multi-replica mode. When set,
            uses Redis-backed session store, SSE broadcaster, and locks.
        callback_base_url: Base URL for on-prem worker event callbacks.
        callback_auth_token: Shared secret for authenticating callback POSTs.
    """
    routes = [
        Route("/", chat_ui),
        Route("/health", health),
        Route("/sessions", list_sessions),
        Route("/sessions/{session_id}/history", session_history),
        Route("/ask", ask_oneshot, methods=["POST"]),
        Route("/temporal/start", temporal_start, methods=["POST"]),
        WebSocketRoute("/ws/{session_id}", websocket_endpoint),
        Route("/events/{session_id}", sse_endpoint),
        # Internal callback for on-prem worker to push streaming events
        Route(
            "/internal/events/{session_id}/publish",
            publish_event_callback,
            methods=["POST"],
        ),
    ]

    # Mount static files if directory exists
    static_dir = Path(__file__).parent / "static"
    if static_dir.is_dir():
        routes.append(Mount("/static", StaticFiles(directory=str(static_dir)), name="static"))

    # Wrap user lifespan to include lock eviction task and Redis setup
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _lifespan(app):
        redis_broadcaster = None

        # Initialize Redis-backed components if redis_url is set
        if redis_url:
            from redis.asyncio import Redis
            from seeknal.ask.gateway.redis_backend import (
                RedisSSEBroadcaster,
            )

            redis_client = Redis.from_url(redis_url)
            redis_broadcaster = RedisSSEBroadcaster(redis_client)
            app.state.broadcaster = redis_broadcaster
            app.state.redis_client = redis_client
            app.state.redis_enabled = True
            await redis_broadcaster.start_listener()
        else:
            app.state.broadcaster = sse_broadcaster
            app.state.redis_client = None
            app.state.redis_enabled = False

        async def _eviction_loop():
            while True:
                await asyncio.sleep(300)  # every 5 minutes
                _evict_idle_locks()

        eviction_task = asyncio.create_task(_eviction_loop())
        try:
            if lifespan is not None:
                async with lifespan(app):
                    yield
            else:
                yield
        finally:
            eviction_task.cancel()
            try:
                await eviction_task
            except asyncio.CancelledError:
                pass
            if redis_broadcaster:
                await redis_broadcaster.stop_listener()
            if app.state.redis_client:
                await app.state.redis_client.close()

    app = Starlette(routes=routes, lifespan=_lifespan)
    app.state.project_path = str(project_path)
    app.state.callback_base_url = callback_base_url
    app.state.callback_auth_token = callback_auth_token
    if temporal_client is not None:
        app.state.temporal_client = temporal_client
    return app

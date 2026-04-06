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
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from seeknal.ask.gateway.session_manager import SessionManager
from seeknal.ask.gateway.sse import SSEBroadcaster

# Validate session IDs: alphanumeric, hyphens, underscores, max 128 chars
_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")

session_manager = SessionManager()
sse_broadcaster = SSEBroadcaster()


def _validate_session_id(session_id: str) -> bool:
    return bool(_SESSION_ID_RE.match(session_id))


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
    """Run agent and yield JSON event dicts as they occur."""
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
    """SSE event stream for a session."""
    session_id = request.path_params["session_id"]
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    queue = sse_broadcaster.subscribe(session_id)

    async def event_generator():
        try:
            while True:
                event = await asyncio.wait_for(queue.get(), timeout=30)
                yield f"data: {event}\n\n"
        except asyncio.TimeoutError:
            yield "data: {\"type\": \"keepalive\"}\n\n"
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
# App factory
# ---------------------------------------------------------------------------


def create_gateway_app(project_path: str | Path) -> Starlette:
    """Create the Starlette ASGI application."""
    routes = [
        Route("/health", health),
        Route("/sessions", list_sessions),
        Route("/ask", ask_oneshot, methods=["POST"]),
        WebSocketRoute("/ws/{session_id}", websocket_endpoint),
        Route("/events/{session_id}", sse_endpoint),
    ]
    app = Starlette(routes=routes)
    app.state.project_path = str(project_path)
    return app

"""Starlette gateway server for seeknal ask.

Provides a WebSocket endpoint for real-time agent interaction, an SSE
endpoint for Temporal worker event delivery, and a health check endpoint.
Uses lazy imports for starlette/uvicorn since they are optional
dependencies (installed via ``seeknal[gateway]``).

Architecture:
- ``GET /health`` -- JSON health check
- ``WS /ws/{session_id}`` -- per-session agent interaction over WebSocket
- ``GET /events/{session_id}`` -- SSE stream for Temporal worker events
- Uvicorn launched programmatically via ``run_gateway()``
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from seeknal.ask.event_sink import EventSink
from seeknal.ask.gateway.session_manager import SessionManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# WebSocketSink — EventSink implementation for WebSocket delivery
# ---------------------------------------------------------------------------


class WebSocketSink:
    """EventSink that sends JSON events over a Starlette WebSocket."""

    def __init__(self, websocket: Any) -> None:
        self.websocket = websocket

    async def on_token(self, text: str) -> None:
        await self.websocket.send_json({"type": "token", "data": text})

    async def on_tool_start(self, name: str, args: dict[str, Any]) -> None:
        await self.websocket.send_json({
            "type": "tool_start",
            "data": {"name": name, "args": args},
        })

    async def on_tool_end(self, name: str, result: str) -> None:
        await self.websocket.send_json({
            "type": "tool_end",
            "data": {"name": name, "result": result},
        })

    async def on_answer(self, text: str) -> None:
        await self.websocket.send_json({"type": "answer", "data": text})

    async def on_error(self, error: str) -> None:
        await self.websocket.send_json({"type": "error", "data": error})


# Verify protocol conformance at import time
assert isinstance(WebSocketSink.__new__(WebSocketSink), EventSink)


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app(
    project_path: Path,
    config: Optional[dict] = None,
) -> Any:
    """Create a Starlette ASGI application for the gateway.

    Args:
        project_path: Path to the seeknal project root.
        config: Agent configuration dict (from ``seeknal_agent.yml``).

    Returns:
        A Starlette ``Application`` instance.
    """
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route, WebSocketRoute
    from starlette.websockets import WebSocket

    from seeknal.ask.gateway.sse import SSEBroadcaster

    agent_config = config or {}
    manager = SessionManager()
    broadcaster = SSEBroadcaster()

    # -- Lifespan ---------------------------------------------------------

    @asynccontextmanager
    async def lifespan(app):
        logger.info("Gateway starting up (project=%s)", project_path)
        app.state.session_manager = manager
        app.state.project_path = project_path
        app.state.agent_config = agent_config
        app.state.sse_broadcaster = broadcaster

        # Start HeartbeatRunner if configured
        heartbeat_task = None
        hb_config = agent_config.get("heartbeat")
        if isinstance(hb_config, dict) and hb_config:
            from seeknal.ask.gateway.heartbeat.runner import HeartbeatRunner

            channels = app.state.__dict__.get("channels", {})
            runner = HeartbeatRunner(config=hb_config, channels=channels)
            heartbeat_task = asyncio.create_task(runner.run(project_path))
            logger.info("Heartbeat runner started as background task")

        yield

        # Cancel heartbeat on shutdown
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            logger.info("Heartbeat runner stopped")

        logger.info("Gateway shutting down")

    # -- Health endpoint --------------------------------------------------

    async def health(request):
        return JSONResponse({"status": "ok"})

    # -- WebSocket endpoint -----------------------------------------------

    async def ws_endpoint(websocket: WebSocket):
        session_id = websocket.path_params["session_id"]
        await websocket.accept()
        manager.connect(session_id, websocket)
        try:
            await _handle_session(websocket, session_id, project_path,
                                  agent_config, manager)
        except Exception:
            logger.exception("WS handler error: session=%s", session_id)
        finally:
            manager.disconnect(session_id, websocket)

    # -- SSE endpoint -----------------------------------------------------

    async def sse_endpoint(request):
        session_id = request.path_params["session_id"]
        return _create_sse_response(broadcaster, session_id)

    # -- Routes -----------------------------------------------------------

    routes = [
        Route("/health", health, methods=["GET"]),
        Route("/events/{session_id}", sse_endpoint, methods=["GET"]),
        WebSocketRoute("/ws/{session_id}", ws_endpoint),
    ]

    return Starlette(routes=routes, lifespan=lifespan)


# ---------------------------------------------------------------------------
# SSE response helper
# ---------------------------------------------------------------------------


def _create_sse_response(broadcaster: Any, session_id: str) -> Any:
    """Create an SSE EventSourceResponse for a session.

    Uses sse-starlette's EventSourceResponse with an async generator
    that reads from the broadcaster queue. Sends a ping keepalive
    every 15 seconds when no events are available.

    Args:
        broadcaster: SSEBroadcaster instance.
        session_id: Session to subscribe to.

    Returns:
        An EventSourceResponse.
    """
    from sse_starlette.sse import EventSourceResponse

    async def event_generator():
        queue = broadcaster.subscribe(session_id)
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield {
                        "event": event.get("type", "message"),
                        "data": json.dumps(event),
                    }
                except asyncio.TimeoutError:
                    # Keepalive ping
                    yield {"event": "ping", "data": ""}
        except asyncio.CancelledError:
            pass
        finally:
            broadcaster.unsubscribe(session_id, queue)

    return EventSourceResponse(event_generator())


# ---------------------------------------------------------------------------
# WebSocket session handler
# ---------------------------------------------------------------------------


async def _handle_session(
    websocket: Any,
    session_id: str,
    project_path: Path,
    agent_config: dict,
    manager: SessionManager,
) -> None:
    """Handle messages for a single WebSocket session.

    Receives JSON messages from the client, creates/resumes the agent
    session, runs the agent with a WebSocketSink, and streams events
    back to the client.
    """
    from starlette.websockets import WebSocketDisconnect

    # Lazy imports for agent machinery
    from seeknal.ask.sessions import SessionStore

    store = SessionStore(project_path)
    try:
        # Ensure session exists in the store
        if store.get(session_id) is None:
            store.create(session_id)

        while True:
            try:
                raw = await websocket.receive_text()
            except WebSocketDisconnect:
                break

            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "data": "Invalid JSON message",
                })
                continue

            msg_type = message.get("type")
            if msg_type != "message":
                await websocket.send_json({
                    "type": "error",
                    "data": f"Unknown message type: {msg_type}",
                })
                continue

            text = message.get("text", "").strip()
            if not text:
                await websocket.send_json({
                    "type": "error",
                    "data": "Empty message text",
                })
                continue

            # Run the agent turn
            await _run_agent_turn(
                websocket, session_id, text, project_path,
                agent_config, store,
            )
    finally:
        store.close()


async def _run_agent_turn(
    websocket: Any,
    session_id: str,
    question: str,
    project_path: Path,
    agent_config: dict,
    session_store: Any,
) -> None:
    """Execute a single agent turn and stream events over WebSocket.

    Creates the agent in-process via ``create_agent()``, sets up a
    per-session ToolContext, and runs the agent with a WebSocketSink.
    """
    from seeknal.ask.agents.agent import create_agent
    from seeknal.ask.agents.tools._context import set_tool_context

    sink = WebSocketSink(websocket)
    try:
        agent, config = create_agent(
            project_path=project_path,
            provider=agent_config.get("provider"),
            model=agent_config.get("model"),
            api_key=agent_config.get("api_key"),
            question=question,
            session_store=session_store,
            session_name=session_id,
        )

        # Update session metadata
        session_store.update(
            session_id,
            last_question=question[:500],
        )

        # Run agent — use sync ask() in a thread to avoid blocking the
        # event loop, while streaming events through the WebSocketSink.
        from seeknal.ask.agents.agent import ask as sync_ask

        answer = await asyncio.to_thread(sync_ask, agent, config, question)

        await sink.on_answer(answer)

    except Exception as exc:
        logger.exception("Agent error: session=%s", session_id)
        await sink.on_error(str(exc))


# ---------------------------------------------------------------------------
# Uvicorn programmatic startup
# ---------------------------------------------------------------------------


async def run_gateway(
    project_path: Path,
    host: str = "127.0.0.1",
    port: int = 18789,
    config: Optional[dict] = None,
) -> None:
    """Start the gateway server programmatically with Uvicorn.

    Args:
        project_path: Path to the seeknal project root.
        host: Bind address (default: 127.0.0.1).
        port: Listen port (default: 18789).
        config: Agent configuration dict.
    """
    import uvicorn

    app = create_app(project_path, config=config)

    uvi_config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(uvi_config)
    await server.serve()

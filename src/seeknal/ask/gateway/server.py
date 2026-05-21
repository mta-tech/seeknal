"""Seeknal Ask Gateway — Starlette ASGI server.

Provides WebSocket, SSE, and REST endpoints for interacting with
the seeknal ask agent over HTTP. Uses pydantic-ai's agent.iter()
for streaming responses.

Usage:
    seeknal gateway start --project path --port 8000
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import re
import time
from pathlib import Path
from typing import Any, Callable

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

from seeknal.ask.gateway.auth import (
    TokenAuthError,
    TokenPrincipal,
    TokenRegistry,
    WorkerRuntimeConfig,
    extract_api_token,
    load_token_registry,
)
from seeknal.ask.gateway.pairing import (
    FilePairingStore,
    PublicSessionStore,
    TelegramLinkStore,
)
from seeknal.ask.gateway.session_manager import SessionManager
from seeknal.ask.gateway.sse import SSEBroadcaster
from seeknal.ask.gateway.tenant import (
    DEFAULT_TENANT,
    InvalidTenantError,
    make_workflow_id,
    resolve_tenant,
    resolve_tenant_ws,
    scoped_key,
    task_queue_for_tenant,
)

# Validate session IDs: alphanumeric, hyphens, underscores, max 128 chars
_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")

# Upload constraints (kept here so they're easy to surface in error responses)
_UPLOAD_MAX_BYTES = 200 * 1024 * 1024  # 200 MB
_UPLOAD_ALLOWED_SUFFIXES = {".xlsx", ".csv", ".tsv", ".json"}
_UPLOAD_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif"}
_IMAGE_MAX_BYTES = 20 * 1024 * 1024  # 20 MB

session_manager = SessionManager()
sse_broadcaster = SSEBroadcaster()

# Per-session locks to serialize concurrent runs on the same (tenant, session_id).
# Prevents message history corruption (last-writer-wins on JSON files)
# and garbled SSE event streams from interleaved responses.
# Uses LRU eviction to prevent unbounded memory growth.
_session_locks: dict[str, tuple[asyncio.Lock, float]] = {}
_LOCK_IDLE_TTL = 1800  # 30 minutes
_session_cancellations: set[str] = set()


def _get_session_lock(
    session_id: str, tenant_id: str = DEFAULT_TENANT
) -> asyncio.Lock:
    """Get or create an asyncio.Lock for a (tenant, session_id) with LRU tracking."""
    key = scoped_key(tenant_id, session_id)
    now = time.monotonic()
    if key in _session_locks:
        lock, _ = _session_locks[key]
        _session_locks[key] = (lock, now)
        return lock
    lock = asyncio.Lock()
    _session_locks[key] = (lock, now)
    return lock


def _evict_idle_locks() -> int:
    """Remove locks that have been idle for longer than _LOCK_IDLE_TTL.

    Returns the number of evicted entries.
    """
    now = time.monotonic()
    stale = [
        key for key, (lock, last_access) in _session_locks.items()
        if now - last_access > _LOCK_IDLE_TTL and not lock.locked()
    ]
    for key in stale:
        del _session_locks[key]
    return len(stale)


def _cancel_key(session_id: str, tenant_id: str = DEFAULT_TENANT) -> str:
    return scoped_key(tenant_id, session_id)


def _request_cancel(session_id: str, tenant_id: str = DEFAULT_TENANT) -> None:
    _session_cancellations.add(_cancel_key(session_id, tenant_id))


def _clear_cancel(session_id: str, tenant_id: str = DEFAULT_TENANT) -> None:
    _session_cancellations.discard(_cancel_key(session_id, tenant_id))


def _is_cancelled(session_id: str, tenant_id: str = DEFAULT_TENANT) -> bool:
    return _cancel_key(session_id, tenant_id) in _session_cancellations


@asynccontextmanager
async def _session_lock_context(
    session_id: str,
    tenant_id: str = DEFAULT_TENANT,
    lock_backend: Any = None,
):
    """Acquire the configured distributed/session lock for one run."""
    if lock_backend is not None:
        lock = await lock_backend.acquire(session_id, tenant_id=tenant_id)
        try:
            yield
        finally:
            await lock_backend.release(lock)
        return

    lock = _get_session_lock(session_id, tenant_id=tenant_id)
    async with lock:
        yield


def _validate_session_id(session_id: str) -> bool:
    return bool(_SESSION_ID_RE.match(session_id))


def _publish_event(
    session_id: str, event: dict, tenant_id: str = DEFAULT_TENANT
) -> None:
    """Publish an event to all SSE subscribers for this (tenant, session_id)."""
    sse_broadcaster.publish_sync(session_id, json.dumps(event), tenant_id=tenant_id)


async def _publish_event_async(
    session_id: str,
    event: dict,
    tenant_id: str = DEFAULT_TENANT,
    broadcaster: Any = None,
) -> None:
    """Publish an event through the configured broadcaster."""
    if broadcaster is not None:
        await broadcaster.publish(session_id, json.dumps(event), tenant_id=tenant_id)
        return
    _publish_event(session_id, event, tenant_id=tenant_id)


def _extract_gateway_tool_result_text(result: Any) -> str:
    """Get a plain-text view of a pydantic-ai tool result."""
    if result is None:
        return ""
    model_str = getattr(result, "model_response_str", None)
    if callable(model_str):
        try:
            rendered = model_str()
        except Exception:
            rendered = None
        if isinstance(rendered, str):
            return rendered
    content = getattr(result, "content", result)
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
                continue
            inner = getattr(part, "content", None)
            parts.append(inner if isinstance(inner, str) else str(part))
        return "".join(parts)
    return str(content)


def _record_gateway_tool_result(
    tool_name: str,
    output: str,
    args: dict[str, Any] | None = None,
) -> None:
    """Record gateway tool results with the same signatures as the TUI path."""
    if tool_name in {"execute_sql", "execute_python"}:
        return
    from seeknal.ask.agents.tools._context import record_tool_result

    record_tool_result(tool_name, output, args=args or {})


def _gateway_tool_stop_reason(tool_name: str) -> str | None:
    """Return a stop reason when a tool result should end the gateway turn."""
    from seeknal.ask.agents.tools._context import (
        get_tool_context,
        has_sufficient_evidence,
        should_synthesize_after_authoritative_sql_pair,
    )

    if (
        tool_name == "execute_sql_pair"
        and should_synthesize_after_authoritative_sql_pair()
    ):
        return "authoritative sql pair result"
    ctx = get_tool_context()
    if (
        getattr(ctx, "disable_quality_gate", False)
        and has_sufficient_evidence()
        and ctx.terminal_tool_errors_this_turn
    ):
        return "terminal tool error after sufficient evidence"
    return None


@dataclass(frozen=True)
class AuthContext:
    """Resolved request authorization context."""

    tenant_id: str
    principal: TokenPrincipal | None = None


def _token_registry_from_state(app_state: Any) -> TokenRegistry | None:
    registry = getattr(app_state, "token_registry", None)
    if isinstance(registry, TokenRegistry) and registry.enabled:
        return registry
    return None


def _safe_auth_context(request: Request) -> tuple[AuthContext, JSONResponse | None]:
    """Resolve tenant/auth context for a request.

    Compatibility mode (no token registry): preserve legacy tenant resolution
    from X-Tenant-ID / ?tenant= / default.

    Token mode: require a valid API token and derive tenant from that token,
    ignoring user-supplied tenant headers/query values for isolation.
    """
    registry = _token_registry_from_state(request.app.state)
    if registry is not None:
        try:
            principal = registry.resolve(extract_api_token(request))
        except TokenAuthError as exc:
            return AuthContext(DEFAULT_TENANT), JSONResponse(
                {"error": str(exc)}, status_code=401
            )
        return AuthContext(principal.tenant_id, principal), None

    try:
        return AuthContext(resolve_tenant(request)), None
    except InvalidTenantError as exc:
        return AuthContext(DEFAULT_TENANT), JSONResponse(
            {"error": str(exc)}, status_code=400
        )


def _safe_resolve_tenant(request: Request) -> tuple[str, JSONResponse | None]:
    """Resolve tenant from a request, returning (tenant_id, error_response)."""
    auth, err = _safe_auth_context(request)
    return auth.tenant_id, err


def _safe_auth_context_ws(websocket: WebSocket) -> tuple[AuthContext, str | None]:
    """Resolve auth context for WebSocket routes.

    Returns (context, close_reason). ``close_reason`` is None on success.
    """
    registry = _token_registry_from_state(websocket.app.state)
    if registry is not None:
        try:
            principal = registry.resolve(extract_api_token(websocket))
        except TokenAuthError as exc:
            return AuthContext(DEFAULT_TENANT), str(exc)
        return AuthContext(principal.tenant_id, principal), None

    try:
        return AuthContext(resolve_tenant_ws(websocket)), None
    except InvalidTenantError as exc:
        return AuthContext(DEFAULT_TENANT), str(exc)


# ---------------------------------------------------------------------------
# Agent execution (shared by WebSocket, SSE, and REST)
# ---------------------------------------------------------------------------


async def _run_agent_streaming(
    project_path: Path,
    session_id: str,
    question: str,
    provider: str | None = None,
    model: str | None = None,
    tenant_id: str = DEFAULT_TENANT,
    auto_approve: bool = False,
    include_web: bool = False,
    lock_backend: Any = None,
    broadcaster: Any = None,
):
    """Run agent and yield JSON event dicts as they occur.

    This generator is the sole owner of SSE broadcast — every event
    yielded is also published to ``sse_broadcaster`` so that SSE
    subscribers on ``/events/{session_id}`` receive events in real-time.

    A per-(tenant, session) ``asyncio.Lock`` serializes concurrent runs
    to prevent message history corruption.
    """
    turn_started = time.monotonic()
    _clear_cancel(session_id, tenant_id=tenant_id)
    async with _session_lock_context(
        session_id,
        tenant_id=tenant_id,
        lock_backend=lock_backend,
    ):
        try:
            async for event in _run_agent_inner(
                project_path, session_id, question, provider, model,
                tenant_id=tenant_id, auto_approve=auto_approve,
                include_web=include_web,
            ):
                if _is_cancelled(session_id, tenant_id=tenant_id):
                    cancel_event = {
                        "type": "cancelled",
                        "data": "Run cancelled by user.",
                    }
                    await _publish_event_async(
                        session_id,
                        cancel_event,
                        tenant_id=tenant_id,
                        broadcaster=broadcaster,
                    )
                    yield cancel_event
                    break
                await _publish_event_async(
                    session_id,
                    event,
                    tenant_id=tenant_id,
                    broadcaster=broadcaster,
                )
                yield event
        except Exception as exc:
            error_event = {"type": "error", "data": str(exc)}
            await _publish_event_async(
                session_id,
                error_event,
                tenant_id=tenant_id,
                broadcaster=broadcaster,
            )
            yield error_event
            raise
        finally:
            _clear_cancel(session_id, tenant_id=tenant_id)
            done_event = {
                "type": "done",
                "data": "",
                "elapsed_ms": int((time.monotonic() - turn_started) * 1000),
            }
            await _publish_event_async(
                session_id,
                done_event,
                tenant_id=tenant_id,
                broadcaster=broadcaster,
            )
            yield done_event


async def _run_agent_inner(
    project_path: Path,
    session_id: str,
    question: str,
    provider: str | None = None,
    model: str | None = None,
    tenant_id: str = DEFAULT_TENANT,
    auto_approve: bool = False,
    include_web: bool = False,
):
    """Inner agent execution without locking or SSE publishing."""
    from pydantic_ai import Agent
    from pydantic_ai._agent_graph import End, UserPromptNode
    from pydantic_ai.messages import (
        FunctionToolCallEvent,
        FunctionToolResultEvent,
        ModelRequest,
        ModelResponse,
        PartDeltaEvent,
        PartStartEvent,
        TextPart,
        TextPartDelta,
        UserPromptPart,
    )

    from seeknal.ask.agents.agent import compact_history_for_analysis_mode, create_agent
    from seeknal.ask.sessions import SessionStore

    store = SessionStore(project_path, tenant_id=tenant_id)
    if store.get(session_id) is None:
        store.create(name=session_id)

    message_history = store.load_messages(session_id)

    agent, deps, _, _ = create_agent(
        project_path, provider=provider, model=model,
        environment="gateway", include_web=include_web,
    )
    from seeknal.ask.agents.tools._context import (
        build_verbatim_restate_response,
        lookup_prior_turn_answer,
        record_prior_turn_answer,
        reset_report_approval,
        reset_turn_governor,
    )

    reset_report_approval()
    reset_turn_governor(question)

    # Verbatim re-ask short-circuit: same question as the prior turn returns
    # the prior answer with a bilingual restate notice BEFORE any LLM/tool
    # call. The gate must fire after reset_turn_governor but before the agent
    # loop starts so we don't burn tool budget on an identical lookup.
    _prior_answer = lookup_prior_turn_answer(question)
    if _prior_answer is not None:
        restate = build_verbatim_restate_response(_prior_answer)
        # Persist the UNWRAPPED prior answer so a triple-ask doesn't recursively
        # wrap the wrapper — the user-facing restate is decorated, but the
        # stored prior stays canonical.
        record_prior_turn_answer(question=question, answer=_prior_answer)
        # Synthesize the request/response pair so session replay and the next
        # turn's message_history both reflect that the user re-asked and got
        # the restate. Without this, save_messages would skip recording the
        # turn entirely (result is None), and the next load_messages would
        # miss the exchange.
        message_history.append(
            ModelRequest(parts=[UserPromptPart(content=question)])
        )
        message_history.append(
            ModelResponse(parts=[TextPart(content=restate)])
        )
        store.save_messages(session_id, message_history)
        yield {"type": "answer", "data": restate}
        store.update(
            session_id,
            last_question=question[:200],
            status="active",
        )
        return

    # Auto-grant all approval gates (for Telegram and other headless channels)
    if auto_approve:
        from seeknal.ask.agents.tools._context import get_tool_context
        ctx = get_tool_context()
        ctx.require_report_approval = False
        ctx.require_proof_publish_approval = False
        ctx.require_proof_edit_approval = False
        ctx.require_seeknal_report_publish_approval = False

    from seeknal.ask.directives import try_direct_tool_directive

    direct_result = await try_direct_tool_directive(question)
    if direct_result is not None:
        for direct_event in direct_result.events:
            tool_started = time.monotonic()
            yield {
                "type": "tool_start",
                "data": {
                    "name": direct_event.name,
                    "args": direct_event.args,
                },
            }
            yield {
                "type": "tool_end",
                "data": {
                    "name": direct_event.name,
                    "output": direct_event.output[:2000],
                    "elapsed_ms": int((time.monotonic() - tool_started) * 1000),
                },
            }
        record_prior_turn_answer(question=question, answer=direct_result.answer)
        yield {"type": "answer", "data": direct_result.answer}
        store.update(
            session_id,
            last_question=question[:200],
            status="active",
        )
        return

    text_buffer: list[str] = []
    result = None

    try:
        from pydantic_ai.exceptions import UsageLimitExceeded
        from pydantic_ai.usage import UsageLimits
        from seeknal.ask.agents.tools._context import get_tool_context
        ctx = get_tool_context()
        compact_history_for_analysis_mode(message_history)
        async with agent.iter(
            question,
            deps=deps,
            message_history=message_history,
            usage_limits=UsageLimits(
                request_limit=ctx.request_limit,
                tool_calls_limit=ctx.tool_call_limit,
            ),
        ) as run:
            async for node in run:
                if isinstance(node, UserPromptNode):
                    continue

                elif Agent.is_model_request_node(node):
                    async with node.stream(run.ctx) as stream:
                        async for event in stream:
                            if isinstance(event, PartStartEvent):
                                if (
                                    isinstance(event.part, TextPart)
                                    and event.part.content
                                ):
                                    text_buffer.append(event.part.content)
                                    yield {
                                        "type": "token",
                                        "data": event.part.content,
                                    }
                            elif isinstance(event, PartDeltaEvent):
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

                    active_tool_args: dict[str, tuple[str, dict[str, Any]]] = {}
                    active_tool_started: dict[str, float] = {}
                    async with node.stream(run.ctx) as handle_stream:
                        async for event in handle_stream:
                            if isinstance(event, FunctionToolCallEvent):
                                tool_name = event.part.tool_name
                                tool_args = event.part.args_as_dict()
                                call_id = (
                                    str(getattr(event.part, "tool_call_id", ""))
                                    or f"{tool_name}:{len(active_tool_args)}"
                                )
                                active_tool_args[call_id] = (tool_name, tool_args)
                                active_tool_started[call_id] = time.monotonic()
                                yield {
                                    "type": "tool_start",
                                    "data": {
                                        "name": tool_name,
                                        "args": tool_args,
                                    },
                                }
                            elif isinstance(event, FunctionToolResultEvent):
                                tool_name = event.result.tool_name
                                content = _extract_gateway_tool_result_text(event.result)
                                result_call_id = (
                                    str(getattr(event.result, "tool_call_id", ""))
                                    or ""
                                )
                                recorded_args: dict[str, Any] = {}
                                elapsed_ms = 0
                                if result_call_id in active_tool_args:
                                    _recorded_name, recorded_args = active_tool_args.pop(
                                        result_call_id
                                    )
                                    started_at = active_tool_started.pop(
                                        result_call_id,
                                        None,
                                    )
                                    if started_at is not None:
                                        elapsed_ms = int(
                                            (time.monotonic() - started_at) * 1000
                                        )
                                _record_gateway_tool_result(
                                    tool_name,
                                    content,
                                    args=recorded_args,
                                )
                                yield {
                                    "type": "tool_end",
                                    "data": {
                                        "name": tool_name,
                                        "output": content[:2000] if content else "",
                                        "elapsed_ms": elapsed_ms,
                                    },
                                }
                                stop_reason = _gateway_tool_stop_reason(tool_name)
                                if stop_reason:
                                    raise UsageLimitExceeded(stop_reason)

                elif isinstance(node, End):
                    break

            result = run.result

        # Final answer
        if text_buffer:
            answer = "".join(text_buffer)
        else:
            answer = result.output or ""

        if not answer:
            from seeknal.ask.agents.agent import _NO_RESPONSE

            retry_prompt = (
                "Please summarize your findings from the tool calls above "
                "and provide your analysis as a text response. Do not call tools."
            )
            try:
                retry_history = (
                    result.all_messages() if result is not None else message_history
                )
                retry = await agent.run(
                    retry_prompt,
                    deps=deps,
                    message_history=retry_history,
                    usage_limits=UsageLimits(
                        request_limit=ctx.request_limit,
                        tool_calls_limit=0,
                    ),
                )
                result = retry
                answer = retry.output or _NO_RESPONSE
            except UsageLimitExceeded:
                answer = _NO_RESPONSE

        if answer:
            record_prior_turn_answer(question=question, answer=answer)
            yield {"type": "answer", "data": answer}

    except UsageLimitExceeded as exc:
        from pydantic_ai.usage import UsageLimits
        from seeknal.ask.agents.tools._context import (
            build_evidence_synthesis_prompt,
            get_tool_context,
            synthesize_evidence_fallback,
        )

        if "authoritative sql pair" in str(exc):
            reason = (
                "authoritative SQL pair produced the answer for this "
                "business question"
            )
        elif "terminal tool error" in str(exc):
            reason = "terminal tool error after sufficient evidence"
        else:
            reason = (
                "tool-call budget reached before the model produced a final answer"
            )
        prompt = build_evidence_synthesis_prompt(reason)
        deterministic = synthesize_evidence_fallback(reason)
        try:
            ctx = get_tool_context()
            result = await agent.run(
                prompt,
                deps=deps,
                message_history=message_history,
                usage_limits=UsageLimits(
                    request_limit=ctx.request_limit,
                    tool_calls_limit=0,
                ),
            )
            answer = result.output or deterministic
        except UsageLimitExceeded:
            answer = deterministic
        record_prior_turn_answer(question=question, answer=answer)
        yield {"type": "answer", "data": answer}

    finally:
        # Always save conversation — even when the consumer closes the generator
        # early (e.g. Telegram breaking on answer event). Without this, session
        # history is lost and the agent can't reference prior messages.
        if result is not None:
            try:
                all_messages = result.all_messages()
                store.save_messages(session_id, all_messages)
                msg_count = len([m for m in all_messages if hasattr(m, 'parts')])
                store.update(
                    session_id,
                    message_count=msg_count,
                    last_question=question[:200],
                    status="active",
                )
            except Exception:
                import logging
                logging.getLogger(__name__).exception(
                    "[gateway] failed to save session %s", session_id
                )


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "seeknal-ask-gateway"})


def _make_session_store(
    app_state, tenant_id: str = DEFAULT_TENANT
) -> "SessionStore":  # noqa: F821
    """Construct a tenant-scoped SessionStore from app.state."""
    from seeknal.ask.sessions import SessionStore

    sessions_dir = getattr(app_state, "sessions_dir", None)
    if sessions_dir:
        return SessionStore(sessions_dir=Path(sessions_dir), tenant_id=tenant_id)
    project_path = getattr(app_state, "project_path", None)
    if project_path:
        return SessionStore(Path(project_path), tenant_id=tenant_id)
    raise RuntimeError("SessionStore requires project_path or sessions_dir")


async def list_sessions(request: Request) -> JSONResponse:
    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err
    store = _make_session_store(request.app.state, tenant_id=tenant_id)
    sessions = store.list()
    return JSONResponse(sessions)


async def ask_oneshot(request: Request) -> JSONResponse:
    """One-shot question → JSON response."""
    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err
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
    async for event in _run_agent_streaming(
        project_path,
        session_id,
        question,
        tenant_id=tenant_id,
        lock_backend=getattr(request.app.state, "session_lock", None),
        broadcaster=getattr(request.app.state, "broadcaster", None),
    ):
        events.append(event)
        if event["type"] == "answer":
            answer = event["data"]

    return JSONResponse({"answer": answer, "events": events})


async def cancel_session_run(request: Request) -> JSONResponse:
    """Request cancellation for the currently running turn in a session."""
    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err
    session_id = request.path_params["session_id"]
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)
    _request_cancel(session_id, tenant_id=tenant_id)
    event = {"type": "cancel_requested", "data": "Cancellation requested."}
    await _publish_event_async(
        session_id,
        event,
        tenant_id=tenant_id,
        broadcaster=getattr(request.app.state, "broadcaster", None),
    )
    return JSONResponse({"status": "cancel_requested", "session_id": session_id})


async def _send_websocket_run_events(
    websocket: WebSocket,
    project_path: Path,
    session_id: str,
    question: str,
    tenant_id: str,
) -> None:
    """Run one agent turn and send events to a WebSocket client."""
    async for event in _run_agent_streaming(
        project_path,
        session_id,
        question,
        tenant_id=tenant_id,
        lock_backend=getattr(websocket.app.state, "session_lock", None),
        broadcaster=getattr(websocket.app.state, "broadcaster", None),
    ):
        await websocket.send_text(json.dumps(event))


async def websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket streaming per session."""
    session_id = websocket.path_params["session_id"]
    if not _validate_session_id(session_id):
        await websocket.close(code=4000, reason="invalid session_id")
        return

    auth, auth_error = _safe_auth_context_ws(websocket)
    if auth_error:
        await websocket.close(code=4001, reason=auth_error)
        return
    tenant_id = auth.tenant_id

    await websocket.accept()
    project_path = Path(websocket.app.state.project_path)
    await session_manager.connect(session_id, websocket)
    run_task: asyncio.Task | None = None

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

            run_task = asyncio.create_task(
                _send_websocket_run_events(
                    websocket,
                    project_path,
                    session_id,
                    question,
                    tenant_id,
                )
            )
            while not run_task.done():
                receive_task = asyncio.create_task(websocket.receive_text())
                done, pending = await asyncio.wait(
                    {run_task, receive_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receive_task in done:
                    try:
                        followup = json.loads(receive_task.result())
                    except json.JSONDecodeError:
                        followup = {}
                    if followup.get("type") == "cancel":
                        _request_cancel(session_id, tenant_id=tenant_id)
                    else:
                        await websocket.send_text(
                            json.dumps(
                                {
                                    "type": "error",
                                    "data": "wait for current run or send type=cancel",
                                }
                            )
                        )
                else:
                    receive_task.cancel()
                for task in pending:
                    if task is not run_task:
                        task.cancel()
            await run_task
            run_task = None

    except WebSocketDisconnect:
        _request_cancel(session_id, tenant_id=tenant_id)
        if run_task is not None and not run_task.done():
            run_task.cancel()
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

    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err

    queue = sse_broadcaster.subscribe(session_id, tenant_id=tenant_id)

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
            sse_broadcaster.unsubscribe(session_id, queue, tenant_id=tenant_id)

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

    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err

    store = _make_session_store(request.app.state, tenant_id=tenant_id)
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

    auth, err = _safe_auth_context(request)
    if err:
        return err
    tenant_id = auth.tenant_id

    body = await request.json()
    question = body.get("question", "")
    session_id = body.get("session_id", "default")
    if not question:
        return JSONResponse({"error": "question is required"}, status_code=400)
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    from seeknal.ask.gateway.temporal import AgentWorkflowInput, AgentWorkflow

    token_mode = auth.principal is not None
    if token_mode and any(
        key in body for key in ("task_queue", "project_path", "push_url", "api_key")
    ):
        return JSONResponse(
            {
                "error": (
                    "task_queue/project_path/push_url/api_key overrides are "
                    "not allowed in token mode"
                )
            },
            status_code=403,
        )

    if auth.principal is not None:
        # Token mode: tenant, queue, callback and optional project path are
        # derived from the authenticated token record. User-supplied headers,
        # query params, and body overrides cannot steer work into another tenant.
        project_path = str(
            auth.principal.project_path
            or getattr(request.app.state, "worker_project_path", None)
            or request.app.state.project_path
        )
        push_url = auth.principal.callback_url or getattr(
            request.app.state, "callback_base_url", None
        )
        api_key = auth.principal.callback_bearer_token
        task_queue = auth.principal.task_queue
    else:
        # Compatibility mode: preserve legacy trusted-local behavior.
        project_path = str(
            body.get("project_path")
            or getattr(request.app.state, "worker_project_path", None)
            or request.app.state.project_path
        )
        push_url = (
            body.get("push_url")
            or getattr(request.app.state, "callback_base_url", None)
        )
        api_key = (
            body.get("api_key")
            or getattr(request.app.state, "callback_auth_token", None)
        )
        task_queue = (
            body.get("task_queue")
            or task_queue_for_tenant(tenant_id)
        )
    workflow_id = make_workflow_id(tenant_id, session_id)

    client = request.app.state.temporal_client
    handle = await client.start_workflow(
        AgentWorkflow.run,
        AgentWorkflowInput(
            session_id=session_id,
            question=question,
            project_path=project_path,
            callback_url=push_url,
            callback_auth_token=api_key,
            tenant_id=tenant_id,
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


async def upload_file(request: Request) -> JSONResponse:
    """Accept a multipart file upload, stage it on disk, return a reference path.

    The staged file is written under
    ``{project}/target/ask_ingest/_staging/{tenant}/{uuid}/{filename}`` and
    the returned ``path`` can be passed directly to the ``read_tabular``
    tool. Tenant is resolved via the standard ``X-Tenant-ID`` header path
    so uploads from different tenants stay isolated on disk.
    """
    import uuid as _uuid

    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err

    project_path_raw = getattr(request.app.state, "project_path", None)
    if not project_path_raw:
        return JSONResponse(
            {"error": "upload requires gateway to be started with --project"},
            status_code=503,
        )
    project_path = Path(project_path_raw)

    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type.lower():
        return JSONResponse(
            {"error": "content-type must be multipart/form-data"},
            status_code=400,
        )

    try:
        form = await request.form()
    except Exception as exc:
        return JSONResponse({"error": f"invalid form data: {exc}"}, status_code=400)

    upload = form.get("file")
    if upload is None or not hasattr(upload, "read") or not hasattr(upload, "filename"):
        return JSONResponse(
            {"error": "no 'file' field in multipart form"},
            status_code=400,
        )

    original_name = Path(upload.filename or "upload").name
    suffix = Path(original_name).suffix.lower()
    is_image = suffix in _UPLOAD_IMAGE_SUFFIXES
    if suffix not in _UPLOAD_ALLOWED_SUFFIXES and not is_image:
        return JSONResponse(
            {
                "error": (
                    f"unsupported file extension '{suffix}'. "
                    f"Allowed: "
                    f"{', '.join(sorted(_UPLOAD_ALLOWED_SUFFIXES | _UPLOAD_IMAGE_SUFFIXES))}"
                ),
            },
            status_code=400,
        )

    # Images get a tighter size cap because Gemini vision rejects very large
    # payloads and it keeps us honest about phone-camera screenshots.
    size_cap = _IMAGE_MAX_BYTES if is_image else _UPLOAD_MAX_BYTES

    staging_dir = (
        project_path
        / "target"
        / "ask_ingest"
        / "_staging"
        / tenant_id
        / _uuid.uuid4().hex[:12]
    )
    staging_dir.mkdir(parents=True, exist_ok=True)
    staged_path = staging_dir / original_name

    total = 0
    try:
        with staged_path.open("wb") as fh:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > size_cap:
                    fh.close()
                    staged_path.unlink(missing_ok=True)
                    cap_mb = size_cap / 1024 / 1024
                    return JSONResponse(
                        {"error": f"file exceeds {cap_mb:.0f} MB limit"},
                        status_code=413,
                    )
                fh.write(chunk)
    except Exception as exc:
        staged_path.unlink(missing_ok=True)
        return JSONResponse({"error": f"failed to stage upload: {exc}"}, status_code=500)
    finally:
        try:
            await upload.close()
        except Exception:
            pass

    return JSONResponse(
        {
            "path": str(staged_path),
            "filename": original_name,
            "size": total,
            "kind": "image" if is_image else "tabular",
            "next": (
                "POST /record with {\"text\": \"ingest the image at "
                f"{staged_path}\"}} to route into the record-entry skill."
                if is_image
                else "Pass the path to read_tabular / write_ingested_table via "
                "the ask agent."
            ),
        }
    )


async def post_record(request: Request) -> JSONResponse:
    """Accept a free-form record text (or image-path prompt) and run the agent.

    Payload: {"text": "/record fitra, 1 mie ayam", "session_id": "..."}

    The agent will route the request through the ``record-entry`` skill —
    parsing the text with ``parse_record`` or reading any image path via
    ``extract_from_image``, then asking the user until the draft is
    complete, then writing via ``write_ingested_table``.
    """
    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err

    project_path_raw = getattr(request.app.state, "project_path", None)
    if not project_path_raw:
        return JSONResponse(
            {"error": "record requires gateway started with --project"},
            status_code=503,
        )

    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse({"error": f"invalid JSON: {exc}"}, status_code=400)

    text = (body.get("text") or "").strip()
    session_id = body.get("session_id", "default")
    if not text:
        return JSONResponse({"error": "text is required"}, status_code=400)
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    project_path = Path(project_path_raw)
    prompt = (
        "The user wants to record an event. Load the 'record-entry' skill "
        "and walk through parse → propose_record_table → ask_user (strict) "
        "→ write_ingested_table.\n\nUser input:\n"
        f"{text}"
    )

    events: list = []
    answer = ""
    async for event in _run_agent_streaming(
        project_path,
        session_id,
        prompt,
        tenant_id=tenant_id,
        lock_backend=getattr(request.app.state, "session_lock", None),
        broadcaster=getattr(request.app.state, "broadcaster", None),
    ):
        events.append(event)
        if event["type"] == "answer":
            answer = event["data"]

    return JSONResponse({"answer": answer, "events": events})


async def publish_event_callback(request: Request) -> JSONResponse:
    """Receive streaming chunk from an on-prem worker and publish to SSE.

    In token mode the callback bearer token is resolved through the API token
    registry, so tenant routing is derived from the token instead of
    caller-provided X-Tenant-ID. In compatibility mode the legacy shared
    CALLBACK_AUTH_TOKEN + X-Tenant-ID path is preserved.
    """
    registry = _token_registry_from_state(request.app.state)
    if registry is not None:
        try:
            principal = registry.resolve_callback(extract_api_token(request))
        except TokenAuthError as exc:
            return JSONResponse({"error": str(exc)}, status_code=401)
        tenant_id = principal.tenant_id
    else:
        expected_token = getattr(request.app.state, "callback_auth_token", None)
        if expected_token:
            auth_header = request.headers.get("Authorization", "")
            if (
                not auth_header.startswith("Bearer ")
                or auth_header[7:] != expected_token
            ):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
        tenant_id, err = _safe_resolve_tenant(request)
        if err:
            return err

    session_id = request.path_params["session_id"]
    if not _validate_session_id(session_id):
        return JSONResponse({"error": "invalid session_id"}, status_code=400)

    body = await request.json()
    event_json = json.dumps(body)

    broadcaster = request.app.state.broadcaster
    await broadcaster.publish(session_id, event_json, tenant_id=tenant_id)

    return JSONResponse({"status": "published"})




async def http_worker_work_stream(request: Request) -> Response:
    """Long-poll one work item for an HTTP-only worker.

    Token mode derives the tenant from the worker API token. Compatibility mode
    falls back to the existing tenant header/query resolver for local tests.
    """
    auth, err = _safe_auth_context(request)
    if err:
        return err

    try:
        timeout = float(request.query_params.get("timeout", "30"))
    except ValueError:
        timeout = 30.0
    timeout = max(0.0, min(timeout, 60.0))

    from seeknal.ask.gateway.http_worker import http_worker_broker

    item = await http_worker_broker.claim_next(
        tenant_id=auth.tenant_id,
        timeout=timeout,
    )
    if item is None:
        return Response(status_code=204)
    return JSONResponse(item.public_dict())


async def http_worker_work_event(request: Request) -> JSONResponse:
    """Receive a streaming event from an HTTP-only worker and publish it to SSE."""
    auth, err = _safe_auth_context(request)
    if err:
        return err

    work_id = request.path_params["work_id"]
    from seeknal.ask.gateway.http_worker import http_worker_broker

    item = await http_worker_broker.owns(work_id=work_id, tenant_id=auth.tenant_id)
    if item is None:
        return JSONResponse({"error": "unknown work_id"}, status_code=404)

    body = await request.json()
    event_json = json.dumps(body)
    await request.app.state.broadcaster.publish(
        item.session_id,
        event_json,
        tenant_id=item.tenant_id,
    )
    return JSONResponse({"status": "published"})


async def http_worker_work_complete(request: Request) -> JSONResponse:
    """Receive final completion from an HTTP-only worker."""
    auth, err = _safe_auth_context(request)
    if err:
        return err

    work_id = request.path_params["work_id"]
    body = await request.json()
    from seeknal.ask.gateway.http_worker import HttpWorkerResult, http_worker_broker

    ok = await http_worker_broker.complete(
        work_id=work_id,
        tenant_id=auth.tenant_id,
        result=HttpWorkerResult(
            answer=str(body.get("answer") or ""),
            event_count=int(body.get("event_count") or 0),
            error=body.get("error"),
        ),
    )
    if not ok:
        return JSONResponse({"error": "unknown work_id"}, status_code=404)
    return JSONResponse({"status": "completed"})


async def worker_config(request: Request) -> JSONResponse:
    """Return token-derived runtime config for a standalone worker.

    Secure deployments start workers with only gateway URL + API token. This
    endpoint maps the token to tenant queue, callback credentials, and optional
    Temporal connection settings. When no token registry is configured, it
    returns legacy default settings for local compatibility.
    """
    registry = _token_registry_from_state(request.app.state)
    if registry is not None:
        try:
            cfg = registry.worker_config_for(
                extract_api_token(request),
                default_callback_url=getattr(
                    request.app.state, "callback_base_url", None
                ),
                default_temporal_address=getattr(
                    request.app.state, "temporal_address", None
                ),
                default_temporal_namespace=getattr(
                    request.app.state, "temporal_namespace", None
                ),
                default_project_path=getattr(
                    request.app.state, "worker_project_path", None
                ),
                default_worker_transport=getattr(
                    request.app.state, "worker_transport", None
                ),
            )
        except TokenAuthError as exc:
            return JSONResponse({"error": str(exc)}, status_code=401)
        return JSONResponse(cfg.public_dict())

    tenant_id, err = _safe_resolve_tenant(request)
    if err:
        return err
    cfg = WorkerRuntimeConfig(
        tenant_id=tenant_id,
        task_queue=task_queue_for_tenant(tenant_id),
        callback_url=getattr(request.app.state, "callback_base_url", None),
        callback_auth_token=getattr(request.app.state, "callback_auth_token", None),
        temporal_address=getattr(request.app.state, "temporal_address", None),
        temporal_namespace=getattr(request.app.state, "temporal_namespace", None),
        project_path=getattr(request.app.state, "worker_project_path", None),
        worker_transport=getattr(request.app.state, "worker_transport", None),
    )
    return JSONResponse(cfg.public_dict())


def create_gateway_app(
    project_path: str | Path | None = None,
    lifespan: Callable | None = None,
    temporal_client: Any = None,
    redis_url: str | None = None,
    callback_base_url: str | None = None,
    callback_auth_token: str | None = None,
    sessions_dir: str | Path | None = None,
    token_registry: TokenRegistry | None = None,
    token_config: str | Path | None = None,
    temporal_address: str | None = None,
    temporal_namespace: str | None = None,
    worker_transport: str | None = None,
) -> Starlette:
    """Create the Starlette ASGI application.

    Args:
        project_path: Path to the seeknal project directory. Required for
            in-process agent execution (``/ask``, ``/ws``, Telegram). Pass
            ``None`` for backend-only mode (Temporal-only dispatch).
        lifespan: Optional async context manager for startup/shutdown hooks.
        temporal_client: Optional connected Temporal client for workflow
            submission via ``POST /temporal/start``.
        redis_url: Optional Redis URL for multi-replica mode.
        callback_base_url: Base URL for on-prem worker event callbacks.
        callback_auth_token: Shared secret for authenticating callback POSTs.
        sessions_dir: Optional direct path for session storage. When set,
            session files go here instead of ``<project>/.seeknal/sessions/``.
            Used in backend mode where no project is available.
        token_registry: Optional preloaded API-token registry. When enabled,
            tenant/queue/callback routing is derived from bearer tokens.
        token_config: Optional JSON/YAML token registry path. Ignored when
            token_registry is provided.
        temporal_address: Temporal address advertised to token-authenticated
            workers through ``/internal/worker/config``.
        temporal_namespace: Temporal namespace advertised to workers.
        worker_transport: Worker execution transport advertised to token-routed
            workers (for example ``temporal`` or ``http``).
    """
    # Backend-only mode: no project_path means no in-process agent execution.
    # Only Temporal dispatch, SSE fan-out, and session listing work.
    backend_only = project_path is None

    routes = [
        Route("/", chat_ui),
        Route("/health", health),
        Route("/sessions", list_sessions),
        Route("/sessions/{session_id}/cancel", cancel_session_run, methods=["POST"]),
        Route("/sessions/{session_id}/history", session_history),
        Route("/temporal/start", temporal_start, methods=["POST"]),
        Route("/events/{session_id}", sse_endpoint),
        # Internal callback for on-prem worker to push streaming events
        Route(
            "/internal/events/{session_id}/publish",
            publish_event_callback,
            methods=["POST"],
        ),
        Route("/internal/worker/config", worker_config, methods=["GET"]),
        Route("/internal/worker/work-stream", http_worker_work_stream, methods=["GET"]),
        Route("/internal/worker/work/{work_id}/event", http_worker_work_event, methods=["POST"]),
        Route("/internal/worker/work/{work_id}/complete", http_worker_work_complete, methods=["POST"]),
    ]

    # In-process agent execution routes are only available when a project
    # is configured. In backend-only mode the client must use /temporal/start.
    if not backend_only:
        routes.insert(4, Route("/ask", ask_oneshot, methods=["POST"]))
        routes.append(Route("/upload", upload_file, methods=["POST"]))
        routes.append(Route("/record", post_record, methods=["POST"]))
        routes.append(WebSocketRoute("/ws/{session_id}", websocket_endpoint))

    # Mount static files if directory exists
    static_dir = Path(__file__).parent / "static"
    if static_dir.is_dir():
        routes.append(Mount("/static", StaticFiles(directory=str(static_dir)), name="static"))

    # Wrap user lifespan to include lock eviction task and Redis setup
    @asynccontextmanager
    async def _lifespan(app):
        redis_broadcaster = None

        # Initialize Redis-backed components if redis_url is set
        if redis_url:
            from redis.asyncio import Redis
            from seeknal.ask.gateway.redis_backend import (
                RedisSSEBroadcaster,
                RedisSessionLock,
            )

            redis_client = Redis.from_url(redis_url)
            redis_broadcaster = RedisSSEBroadcaster(redis_client)
            app.state.broadcaster = redis_broadcaster
            app.state.session_lock = RedisSessionLock(redis_client)
            app.state.redis_client = redis_client
            app.state.redis_enabled = True
            if project_path is not None:
                app.state.pairing_store = FilePairingStore(Path(project_path).resolve())
                app.state.telegram_link_store = TelegramLinkStore(Path(project_path).resolve())
                app.state.public_session_store = PublicSessionStore(Path(project_path).resolve())
            else:
                pair_base = Path(app.state.sessions_dir).resolve().parent
                app.state.pairing_store = FilePairingStore(base_dir=pair_base)
                app.state.telegram_link_store = TelegramLinkStore(base_dir=pair_base)
                app.state.public_session_store = PublicSessionStore(base_dir=pair_base)
            await redis_broadcaster.start_listener()
        else:
            app.state.broadcaster = sse_broadcaster
            app.state.session_lock = None
            app.state.redis_client = None
            app.state.redis_enabled = False
            if project_path is not None:
                app.state.pairing_store = FilePairingStore(Path(project_path).resolve())
                app.state.telegram_link_store = TelegramLinkStore(Path(project_path).resolve())
                app.state.public_session_store = PublicSessionStore(Path(project_path).resolve())
            else:
                pair_base = Path(app.state.sessions_dir).resolve().parent
                app.state.pairing_store = FilePairingStore(base_dir=pair_base)
                app.state.telegram_link_store = TelegramLinkStore(base_dir=pair_base)
                app.state.public_session_store = PublicSessionStore(base_dir=pair_base)

        async def _eviction_loop():
            while True:
                await asyncio.sleep(300)  # every 5 minutes
                _evict_idle_locks()
                await app.state.pairing_store.cleanup_expired_codes()

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
    app.state.project_path = str(project_path) if project_path is not None else None
    app.state.backend_only = backend_only
    # Resolve sessions dir for backend-only mode (no project path available).
    if sessions_dir is not None:
        app.state.sessions_dir = str(sessions_dir)
    elif backend_only:
        default_dir = Path.home() / ".seeknal" / "gateway-sessions"
        default_dir.mkdir(parents=True, exist_ok=True)
        app.state.sessions_dir = str(default_dir)
    else:
        app.state.sessions_dir = None
    app.state.callback_base_url = callback_base_url
    app.state.callback_auth_token = callback_auth_token
    app.state.token_registry = token_registry or load_token_registry(token_config)
    app.state.temporal_address = temporal_address
    app.state.temporal_namespace = temporal_namespace
    app.state.worker_transport = worker_transport
    if temporal_client is not None:
        app.state.temporal_client = temporal_client
    return app

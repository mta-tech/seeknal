"""Gateway CLI — seeknal gateway start.

Starts the seeknal ask HTTP gateway (WebSocket + SSE + REST + Telegram + Temporal).
"""

from __future__ import annotations

import logging
import os
import random
from contextlib import aclosing, asynccontextmanager
from pathlib import Path
from typing import Optional

import typer

from seeknal.ask.project import find_project_path

logger = logging.getLogger(__name__)

# --- Event/completion delivery retry policy (GW1) ---------------------------
# One transient DNS/connect blip must never kill an in-flight turn: the LLM
# cost is already spent by the time events stream back. See
# iba-deploy-runbook/specs/2026-10-21-spec-gw1-seeknal-ask-limits-gateway-resilience.md
_EVENT_MAX_ATTEMPTS = 3
_EVENT_RETRY_BASE_S = 0.5
_EVENT_RETRY_CAP_S = 30.0
_TERMINAL_MAX_ATTEMPTS = 2
_TERMINAL_RETRY_DELAY_S = 0.5
_DELIVERY_TIMEOUT_S = 15.0

# --- Polling retry policy (GW1) --------------------------------------------
_POLL_RETRY_BASE_S = 2.0
_POLL_RETRY_CAP_S = 30.0
_POLL_LOG_VERBOSE_ATTEMPTS = 3

_EVENT_ID_HEADER = "X-Seeknal-Event-Id"


class _WorkVanished(Exception):
    """Gateway no longer knows this work (404/410) — stop streaming."""


def _retry_jitter() -> float:
    """Multiplicative jitter (1.0..1.25) applied to retry sleeps.

    Module-level so tests can pin it to 1.0 for deterministic assertions.
    """
    return 1.0 + random.random() * 0.25


def _retry_after_seconds(response) -> Optional[float]:
    """Extract a capped Retry-After hint from a 429/5xx response, if any."""
    try:
        raw = response.headers.get("retry-after")
    except AttributeError:
        return None
    if not raw:
        return None
    try:
        return max(0.0, min(float(raw.strip()), _EVENT_RETRY_CAP_S))
    except (TypeError, ValueError):
        return None


def _poll_retry_delay(attempt: int) -> float:
    """Exponential poll backoff: 2,4,8,… capped, with jitter."""
    delay = min(_POLL_RETRY_CAP_S, _POLL_RETRY_BASE_S * 2 ** max(attempt - 1, 0))
    return delay * _retry_jitter()


def _log_poll_failure(attempt: int, message: str) -> None:
    """WARNING for the first few poll failures, DEBUG after that (log-storm control)."""
    if attempt <= _POLL_LOG_VERBOSE_ATTEMPTS:
        logger.warning("%s", message)
    else:
        logger.debug("%s (attempt=%d)", message, attempt)

gateway_app = typer.Typer(
    name="gateway",
    help="Seeknal Ask HTTP gateway server.",
    no_args_is_help=True,
)


@gateway_app.command("start")
def gateway_start(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path (auto-detected if not set)"
    ),
    port: int = typer.Option(
        8000, "--port", help="Port to listen on"
    ),
    host: str = typer.Option(
        "0.0.0.0", "--host", help="Host to bind to"
    ),
    telegram: bool = typer.Option(
        False, "--telegram", help="Enable Telegram bot channel"
    ),
    temporal: bool = typer.Option(
        False, "--temporal", help="Enable Temporal client for durable agent execution"
    ),
    no_worker: bool = typer.Option(
        False, "--no-worker", help="Connect to Temporal as client only (no local worker). Use for cloud/gateway-only mode."
    ),
    max_activities: int = typer.Option(
        15, "--max-activities", envvar="TEMPORAL_MAX_CONCURRENT_ACTIVITIES",
        help="Maximum concurrent Temporal activities (agent executions per worker)"
    ),
    redis: Optional[str] = typer.Option(
        None, "--redis", help="Redis URL for multi-replica mode (e.g. redis://localhost:6379)"
    ),
    callback_url: Optional[str] = typer.Option(
        None, "--callback-url", help="Base URL for on-prem worker event callbacks"
    ),
    callback_auth_token: Optional[str] = typer.Option(
        None, "--callback-auth-token", envvar="CALLBACK_AUTH_TOKEN",
        help="Shared secret for authenticating worker callback POSTs"
    ),
    worker_project_path: Optional[str] = typer.Option(
        None, "--worker-project-path", envvar="WORKER_PROJECT_PATH",
        help="Project path on the remote worker (for split topology where gateway and worker are on different machines)"
    ),
    token_config: Optional[Path] = typer.Option(
        None, "--token-config", envvar="SEEKNAL_TOKEN_CONFIG",
        help="JSON/YAML API token registry for tenant-scoped worker routing"
    ),
    worker_transport: str = typer.Option(
        "temporal", "--worker-transport", envvar="SEEKNAL_WORKER_TRANSPORT",
        help="Temporal activity execution transport: temporal/local or http for HTTP-only workers"
    ),
):
    """Start the seeknal ask gateway server."""
    project_path = project or find_project_path()

    # Load project .env (FIRECRAWL_API_KEY, GOOGLE_API_KEY, etc.)
    from seeknal.cli.ask import _load_project_env
    _load_project_env(project_path)

    try:
        from seeknal.ask.gateway.server import create_gateway_app
    except ImportError:
        typer.echo(typer.style(
            "Gateway dependencies are missing from this environment.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install --upgrade seeknal", fg=typer.colors.CYAN
        ))
        raise typer.Exit(1)

    import uvicorn

    # --- Resolve optional channels/integrations ---

    telegram_channel = None
    if telegram:
        try:
            from seeknal.ask.gateway.channels.telegram import TelegramChannel

            telegram_channel = TelegramChannel(project_path)
            typer.echo(typer.style("Telegram channel enabled", fg=typer.colors.GREEN))
        except ImportError:
            typer.echo(typer.style(
                "Telegram dependencies are missing; install or upgrade seeknal.",
                fg=typer.colors.YELLOW,
            ))

    temporal_enabled = False
    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    temporal_task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "seeknal-ask")

    if temporal:
        try:
            from seeknal.ask.gateway.temporal import (
                _require_temporal,
            )
            _require_temporal()
            temporal_enabled = True
        except ImportError:
            typer.echo(typer.style(
                "Temporal requires: pip install seeknal[temporal]",
                fg=typer.colors.YELLOW,
            ))

    # --- Build lifespan ---

    temporal_client_holder: list = []  # mutable container for closure

    @asynccontextmanager
    async def lifespan(app):
        # Startup: Telegram
        if telegram_channel is not None:
            telegram_channel.set_pairing_store(app.state.pairing_store)
            telegram_channel.set_link_store(app.state.telegram_link_store)
            telegram_channel.set_public_session_store(app.state.public_session_store)
            app.state.telegram_channel = telegram_channel
            await telegram_channel.start()

        # Startup: Temporal
        worker = None
        worker_task = None
        if temporal_enabled:
            from seeknal.ask.gateway.temporal import (
                connect_temporal_client,
                create_temporal_worker,
            )

            client = await connect_temporal_client(
                address=temporal_address,
                namespace=temporal_namespace,
            )
            if client is not None:
                temporal_client_holder.append(client)
                app.state.temporal_client = client
                app.state.temporal_task_queue = temporal_task_queue

                if not no_worker:
                    import asyncio

                    if worker_transport:
                        os.environ["SEEKNAL_WORKER_TRANSPORT"] = worker_transport
                    worker = create_temporal_worker(
                        client,
                        task_queue=temporal_task_queue,
                        max_concurrent_activities=max_activities,
                    )
                    worker_task = asyncio.create_task(worker.run())
                    typer.echo(typer.style(
                        f"Temporal worker enabled (queue={temporal_task_queue}, max_activities={max_activities}, transport={worker_transport})",
                        fg=typer.colors.GREEN,
                    ))
                else:
                    typer.echo(typer.style(
                        "Temporal client-only mode (no local worker)",
                        fg=typer.colors.GREEN,
                    ))
            else:
                typer.echo(typer.style(
                    "Temporal unavailable — running in degraded mode",
                    fg=typer.colors.YELLOW,
                ))

        try:
            yield
        finally:
            # Shutdown: Temporal worker
            if worker_task is not None and worker is not None:
                await worker.shutdown()
                await worker_task

            # Shutdown: Telegram
            if telegram_channel is not None:
                await telegram_channel.stop()

    # Pass temporal_client if already connected (for sync startup path)
    # The lifespan will also set it on app.state for the async path
    # Default callback_url to self ONLY in --no-worker mode (gateway-only).
    # When the worker is embedded (default), _run_agent_streaming() already
    # publishes to SSE directly — adding a callback to self would double-publish.
    # For remote/split workers, the callback lets them POST events back.
    effective_callback_url = callback_url
    if temporal_enabled and no_worker and not effective_callback_url:
        effective_callback_url = f"http://{host}:{port}"
        typer.echo(typer.style(
            f"Callback URL defaulted to {effective_callback_url} (for worker event delivery)",
            fg=typer.colors.CYAN,
        ))

    app = create_gateway_app(
        project_path,
        lifespan=lifespan,
        redis_url=redis,
        callback_base_url=effective_callback_url,
        callback_auth_token=callback_auth_token,
        token_config=token_config,
        temporal_address=temporal_address,
        temporal_namespace=temporal_namespace,
        worker_transport=worker_transport,
    )
    if worker_project_path:
        app.state.worker_project_path = worker_project_path

    typer.echo(f"Starting gateway on {host}:{port}")
    typer.echo(f"Project: {project_path}")
    if redis:
        typer.echo(typer.style(f"Redis: {redis}", fg=typer.colors.GREEN))
    typer.echo("Endpoints:")
    typer.echo("  GET  /health")
    typer.echo("  GET  /sessions")
    typer.echo("  POST /ask")
    typer.echo("  POST /upload")
    typer.echo("  POST /record")
    typer.echo("  POST /temporal/start")
    typer.echo("  POST /internal/events/{session_id}/publish")
    typer.echo("  GET  /internal/worker/config")
    typer.echo("  WS   /ws/{session_id}")
    typer.echo("  GET  /events/{session_id}")

    uvicorn.run(app, host=host, port=port, log_level="info")


@gateway_app.command("backend")
def gateway_backend(
    port: int = typer.Option(
        8000, "--port", help="Port to listen on"
    ),
    host: str = typer.Option(
        "0.0.0.0", "--host", help="Host to bind to"
    ),
    redis: Optional[str] = typer.Option(
        None, "--redis", help="Redis URL for multi-replica mode (e.g. redis://localhost:6379)"
    ),
    callback_url: Optional[str] = typer.Option(
        None, "--callback-url",
        help="Public URL workers should POST streaming events to (defaults to http://{host}:{port})"
    ),
    callback_auth_token: Optional[str] = typer.Option(
        None, "--callback-auth-token", envvar="CALLBACK_AUTH_TOKEN",
        help="Shared secret for authenticating worker callback POSTs"
    ),
    worker_project_path: Optional[str] = typer.Option(
        None, "--worker-project-path", envvar="WORKER_PROJECT_PATH",
        help="Default project path to pass to remote workers via Temporal workflow input"
    ),
    sessions_dir: Optional[Path] = typer.Option(
        None, "--sessions-dir",
        help="Gateway-local sessions directory (default: ~/.seeknal/gateway-sessions/)"
    ),
    token_config: Optional[Path] = typer.Option(
        None, "--token-config", envvar="SEEKNAL_TOKEN_CONFIG",
        help="JSON/YAML API token registry for tenant-scoped worker routing"
    ),
    max_activities: int = typer.Option(
        15, "--max-activities", envvar="TEMPORAL_MAX_CONCURRENT_ACTIVITIES",
        help="Maximum concurrent Temporal activities for gateway-hosted broker workers"
    ),
    worker_transport: str = typer.Option(
        "temporal", "--worker-transport", envvar="SEEKNAL_WORKER_TRANSPORT",
        help="Set to http to make the gateway-hosted Temporal activity broker work to HTTP-only workers"
    ),
):
    """Start the seeknal ask gateway in backend-only mode.

    This mode serves the web UI, SSE streams, and Temporal dispatch but
    does NOT run agents in-process. All queries are dispatched to remote
    workers via Temporal. Use this for cloud deployments where compute
    lives on separate on-prem machines.

    The /ask and /ws routes are disabled — clients must use /temporal/start.
    """
    try:
        from seeknal.ask.gateway.server import create_gateway_app
    except ImportError:
        typer.echo(typer.style(
            "Gateway dependencies are missing from this environment.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install seeknal[temporal]", fg=typer.colors.CYAN
        ))
        raise typer.Exit(1)

    import uvicorn

    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    temporal_task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "seeknal-ask")

    try:
        from seeknal.ask.gateway.temporal import _require_temporal
        _require_temporal()
    except ImportError:
        typer.echo(typer.style(
            "Backend mode requires: pip install seeknal[temporal]",
            fg=typer.colors.RED,
        ))
        raise typer.Exit(1)

    temporal_client_holder: list = []

    @asynccontextmanager
    async def lifespan(app):
        from seeknal.ask.gateway.temporal import connect_temporal_client, create_temporal_worker

        client = await connect_temporal_client(
            address=temporal_address,
            namespace=temporal_namespace,
        )
        if client is not None:
            temporal_client_holder.append(client)
            app.state.temporal_client = client
            app.state.temporal_task_queue = temporal_task_queue
            typer.echo(typer.style(
                f"Temporal client connected (queue={temporal_task_queue})",
                fg=typer.colors.GREEN,
            ))
            worker = None
            worker_task = None
            if worker_transport.strip().lower() in {"http", "gateway", "poll"}:
                import asyncio

                os.environ["SEEKNAL_WORKER_TRANSPORT"] = "http"
                worker = create_temporal_worker(
                    client,
                    task_queue=temporal_task_queue,
                    max_concurrent_activities=max_activities,
                )
                worker_task = asyncio.create_task(worker.run())
                app.state.http_broker_worker = worker
                app.state.http_broker_worker_task = worker_task
                typer.echo(typer.style(
                    f"HTTP worker broker enabled (queue={temporal_task_queue}, max_activities={max_activities})",
                    fg=typer.colors.GREEN,
                ))
        else:
            typer.echo(typer.style(
                f"Failed to connect to Temporal at {temporal_address}",
                fg=typer.colors.RED,
            ))
            raise typer.Exit(1)

        try:
            yield
        finally:
            worker_task = getattr(app.state, "http_broker_worker_task", None)
            worker = getattr(app.state, "http_broker_worker", None)
            if worker_task is not None and worker is not None:
                await worker.shutdown()
                await worker_task

    effective_callback_url = callback_url or f"http://{host}:{port}"

    app = create_gateway_app(
        project_path=None,  # backend-only: no local project
        lifespan=lifespan,
        redis_url=redis,
        callback_base_url=effective_callback_url,
        callback_auth_token=callback_auth_token,
        sessions_dir=sessions_dir,
        token_config=token_config,
        temporal_address=temporal_address,
        temporal_namespace=temporal_namespace,
        worker_transport=worker_transport,
    )
    if worker_project_path:
        app.state.worker_project_path = worker_project_path

    typer.echo(f"Starting gateway backend on {host}:{port}")
    typer.echo(f"Mode: {typer.style('backend-only', fg=typer.colors.CYAN)} (no in-process agent execution)")
    typer.echo(f"Temporal: {temporal_address}")
    typer.echo(f"Callback: {effective_callback_url}")
    if redis:
        typer.echo(typer.style(f"Redis: {redis}", fg=typer.colors.GREEN))
    typer.echo(f"Sessions: {app.state.sessions_dir}")
    typer.echo("Endpoints:")
    typer.echo("  GET  /health")
    typer.echo("  GET  /sessions")
    typer.echo("  POST /temporal/start")
    typer.echo("  POST /internal/events/{session_id}/publish")
    typer.echo("  GET  /internal/worker/config")
    typer.echo("  GET  /events/{session_id}")
    typer.echo(typer.style("  (no /ask, no /ws — backend mode)", fg=typer.colors.YELLOW))

    uvicorn.run(app, host=host, port=port, log_level="info")


async def _process_http_work_item(
    work: dict,
    *,
    client,  # httpx.AsyncClient — typed loosely to avoid module-level httpx import
    base_url: str,
    headers: dict,
    project_path: Path,
    semaphore,  # asyncio.Semaphore
) -> None:
    """Process a single claimed work item end-to-end.

    Guarantees on every exit path:
      - POSTs ``complete`` to the gateway so the broker resolves the future
      - Releases the semaphore slot
      - Emits a lifecycle log line tagged with work_id + session_id

    Cancellation (graceful shutdown) is handled by attempting to surface an
    error+done event and a ``complete`` POST before re-raising so the calling
    drainer sees the task as cancelled.
    """
    import asyncio
    import httpx

    from seeknal.ask.gateway.server import _run_agent_streaming
    from seeknal.ask.gateway.tenant import DEFAULT_TENANT

    work_id = work["work_id"]
    session_id = work["session_id"]
    tenant_id = work.get("tenant_id") or DEFAULT_TENANT
    question = work["question"]
    short_id = work_id[:8]

    typer.echo(f"[work={short_id} session={session_id}] start")

    answer = ""
    error: Optional[str] = None
    event_count = 0
    event_seq = 0
    dropped_events = 0
    terminal_sent = False

    async def _post_with_retry(
        url: str,
        *,
        json_body: dict,
        extra_headers: Optional[dict] = None,
        timeout: float = _DELIVERY_TIMEOUT_S,
        abort_on_gone: bool = True,
        max_attempts: int = _EVENT_MAX_ATTEMPTS,
        base_delay: float = _EVENT_RETRY_BASE_S,
    ) -> tuple[bool, Optional[object]]:
        """POST with transient-failure retry (GW1 §3.3).

        Args:
            url: Absolute gateway endpoint.
            json_body: Event or completion payload.
            extra_headers: Extra headers; must include the idempotency key for
                deliveries that may be replayed after server-side acceptance.
            timeout: Per-attempt timeout in seconds.
            abort_on_gone: Raise ``_WorkVanished`` on HTTP 404/410. The
                streaming path sets this so a discarded work stops burning
                LLM tokens; the completion path clears it and logs at debug.
            max_attempts: Total attempts, including the first one.
            base_delay: First backoff delay; doubles per attempt.

        Returns:
            ``(delivered, response)``. ``delivered`` is True for any 2xx.
            A failed delivery returns ``False`` plus the last response (or
            ``None`` for transport exhaustion); the caller decides whether
            that should affect the turn (it never should by itself).

        Raises:
            _WorkVanished: When the gateway replies 404/410 and
                ``abort_on_gone`` is true.
            asyncio.CancelledError: Propagated unchanged (task cancellation).
        """
        delay = base_delay
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.post(
                    url,
                    headers={**headers, **(extra_headers or {})},
                    json=json_body,
                    timeout=timeout,
                )
            except asyncio.CancelledError:
                raise
            except (httpx.RequestError, OSError) as exc:
                # Transport-level only: DNS, connect, read/write timeout.
                # Programming errors must propagate (fail fast) instead of
                # being retried as if they were network blips.
                if attempt < max_attempts:
                    logger.warning(
                        "[work=%s session=%s] delivery attempt=%d/%d failed: %s:%s",
                        short_id, session_id, attempt, max_attempts,
                        type(exc).__name__, exc,
                    )
                    await asyncio.sleep(
                        min(delay, _EVENT_RETRY_CAP_S) * _retry_jitter()
                    )
                    delay *= 2
                    continue
                logger.exception(
                    "[work=%s session=%s] delivery failed permanently after %d attempts — "
                    "continuing turn (event dropped)",
                    short_id, session_id, max_attempts,
                )
                return False, None
            status = response.status_code
            if status in (404, 410):
                if abort_on_gone:
                    raise _WorkVanished(
                        f"gateway no longer knows work {work_id} (HTTP {status})"
                    )
                logger.debug(
                    "[work=%s session=%s] gateway already discarded work (HTTP %d)",
                    short_id, session_id, status,
                )
                return False, response
            if 200 <= status < 300:
                return True, response
            if status == 429 or status >= 500:
                if attempt < max_attempts:
                    retry_after = _retry_after_seconds(response)
                    server_hint = (
                        f" (Retry-After={retry_after:.0f}s)" if retry_after is not None else ""
                    )
                    logger.warning(
                        "[work=%s session=%s] gateway %d on attempt=%d/%d%s — retrying",
                        short_id, session_id, status, attempt, max_attempts, server_hint,
                    )
                    await asyncio.sleep(
                        (retry_after if retry_after is not None else delay)
                        * _retry_jitter()
                    )
                    delay *= 2
                    continue
                logger.warning(
                    "[work=%s session=%s] gateway %d persisted after %d attempts — "
                    "event dropped",
                    short_id, session_id, status, max_attempts,
                )
                return False, response
            logger.warning(
                "[work=%s session=%s] gateway returned HTTP %d — not retryable, event dropped",
                short_id, session_id, status,
            )
            return False, response
        return False, None

    async def post_event(event: dict) -> None:
        """Deliver one streamed event, counting it as dropped if undeliverable."""
        nonlocal event_seq, dropped_events
        event_seq += 1
        delivered, _ = await _post_with_retry(
            f"{base_url}/internal/worker/work/{work_id}/event",
            json_body=event,
            extra_headers={_EVENT_ID_HEADER: f"{work_id}:{event_seq}"},
        )
        if not delivered:
            dropped_events += 1

    async def _send_terminal_best_effort(*terminal_events: dict) -> None:
        """Deliver terminal error/done events best-effort; never raise.

        Capped at ``_TERMINAL_MAX_ATTEMPTS`` per event. During shutdown the
        task sits in a cancelled frame, so a per-attempt CancelledError is
        logged at debug and swallowed — the ORIGINAL turn error must never be
        masked by delivery mechanics.
        """
        nonlocal event_seq, dropped_events
        for terminal_event in terminal_events:
            event_seq += 1
            extra_headers = {_EVENT_ID_HEADER: f"{work_id}:{event_seq}"}
            for attempt in range(1, _TERMINAL_MAX_ATTEMPTS + 1):
                try:
                    response = await client.post(
                        f"{base_url}/internal/worker/work/{work_id}/event",
                        headers={**headers, **extra_headers},
                        json=terminal_event,
                        timeout=_DELIVERY_TIMEOUT_S,
                    )
                except asyncio.CancelledError:
                    logger.debug(
                        "[work=%s session=%s] terminal %s attempt=%d cancelled",
                        short_id, session_id, terminal_event.get("type"), attempt,
                    )
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "[work=%s session=%s] terminal %s attempt=%d failed",
                        short_id, session_id, terminal_event.get("type"), attempt,
                    )
                else:
                    if response.status_code in (404, 410):
                        return  # work gone — further terminal sends are pointless
                    if 200 <= response.status_code < 300:
                        break
                if attempt < _TERMINAL_MAX_ATTEMPTS:
                    try:
                        await asyncio.sleep(
                            _TERMINAL_RETRY_DELAY_S * _retry_jitter()
                        )
                    except asyncio.CancelledError:
                        continue
            else:
                dropped_events += 1

    try:
        try:
            async with aclosing(_run_agent_streaming(
                project_path,
                session_id,
                question,
                provider=work.get("provider"),
                model=work.get("model"),
                tenant_id=tenant_id,
            )) as stream:
                async for event in stream:
                    event_count += 1
                    if event.get("type") == "answer":
                        answer = str(event.get("data") or "")
                    elif event.get("type") == "error":
                        error = str(event.get("data") or "")
                    elif event.get("type") == "done":
                        # The generator emits done as its terminal event
                        # (before re-raising); the except handlers below must
                        # not post a second error/done pair on top of it.
                        terminal_sent = True
                    await post_event(event)
        except _WorkVanished as exc:
            # The work record is gone on the gateway side (expired/discarded).
            # Abort promptly; sending error/done events back is pointless —
            # both would 404. Still attempt the completion POST below.
            dropped_events += 1  # the in-flight event was never delivered
            error = error or str(exc)
            logger.warning(
                "[work=%s session=%s] work vanished after %d events — aborting run: %s",
                short_id, session_id, event_count, exc,
            )
        except asyncio.CancelledError:
            error = error or "worker shutting down"
            if not terminal_sent:
                await _send_terminal_best_effort(
                    {"type": "error", "data": error},
                    {"type": "done"},
                )
            raise
        except Exception as exc:  # noqa: BLE001 - surface worker failures to gateway
            logger.exception(
                "[work=%s session=%s] unhandled failure",
                short_id, session_id,
            )
            error = error or str(exc)
            if not terminal_sent:
                await _send_terminal_best_effort(
                    {"type": "error", "data": error},
                    {"type": "done"},
                )
    finally:
        try:
            await _post_with_retry(
                f"{base_url}/internal/worker/work/{work_id}/complete",
                json_body={
                    "answer": answer,
                    "event_count": event_count,
                    "dropped_event_count": dropped_events,
                    "error": error,
                },
                abort_on_gone=False,
                timeout=_DELIVERY_TIMEOUT_S,
            )
        except Exception as exc:  # noqa: BLE001 — never mask the turn outcome
            logger.exception(
                "[work=%s session=%s] completion POST crashed", short_id, session_id
            )
        finally:
            semaphore.release()
            status = "error" if error else "ok"
            typer.echo(
                f"[work={short_id} session={session_id}] complete "
                f"events={event_count} dropped={dropped_events} status={status}"
            )


async def _drain_or_cancel_tasks(tasks: set, timeout: float) -> None:
    """Wait for in-flight tasks to finish, then cancel any stragglers.

    Each task's own ``finally`` is responsible for the complete POST and
    semaphore release — even under cancellation — so the broker does not
    leak in-flight entries on shutdown.
    """
    import asyncio

    if not tasks:
        return
    typer.echo(
        f"  Draining {len(tasks)} in-flight task(s) (timeout {timeout:.0f}s)..."
    )
    pending = set(tasks)
    done, still_pending = await asyncio.wait(pending, timeout=timeout)
    if still_pending:
        typer.echo(typer.style(
            f"  Cancelling {len(still_pending)} task(s) past shutdown timeout",
            fg=typer.colors.YELLOW,
        ))
        for task in still_pending:
            task.cancel()
        await asyncio.gather(*still_pending, return_exceptions=True)
    typer.echo(
        f"  Drained {len(done)} task(s), cancelled {len(still_pending)}."
    )


async def _run_http_only_worker(
    *,
    project_path: Path,
    gateway_url: str,
    api_token: str,
    poll_timeout: float = 30.0,
    max_concurrency: int = 1,
    shutdown_timeout: float = 60.0,
) -> None:
    """Run a worker that talks only HTTP(S) to the gateway/kc-service.

    ``max_concurrency`` caps the number of agent executions that run in
    parallel within this worker process. The default of 1 preserves the
    historical sequential behavior. Backpressure: the semaphore is acquired
    *before* polling so the worker does not claim work it cannot start —
    this preserves broker fairness across workers.

    SIGINT and SIGTERM are handled explicitly via ``loop.add_signal_handler``
    so the worker shuts down cleanly under K8s / systemd / docker stop —
    the default ``asyncio.run`` SIGINT handling can fail to cancel the main
    task reliably when child agent tasks are in flight.
    """
    import asyncio
    import signal
    import httpx

    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")

    base_url = gateway_url.rstrip("/")
    headers = {"Authorization": f"Bearer {api_token}"}

    typer.echo("Seeknal HTTP worker started")
    typer.echo(f"  Project: {project_path}")
    typer.echo(f"  Gateway: {base_url}")
    typer.echo("  Transport: http-only")
    typer.echo(f"  Max concurrency: {max_concurrency}")

    semaphore = asyncio.Semaphore(max_concurrency)
    live_tasks: set[asyncio.Task] = set()
    poll_attempt = 0  # failing polls in a row; reset on any healthy response

    # Install explicit signal handlers that cancel the main task. This is
    # more reliable than asyncio.run's default SIGINT path under Python
    # 3.11+ when child tasks are running. SIGTERM is also handled so K8s /
    # docker stop trigger the same graceful drain.
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def _request_shutdown() -> None:
        if main_task is not None and not main_task.done():
            main_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except NotImplementedError:
            # add_signal_handler is not implemented on Windows; the default
            # KeyboardInterrupt path will still cover SIGINT there.
            pass

    async with httpx.AsyncClient(timeout=None) as client:
        try:
            while True:
                # Backpressure: acquire a slot before claiming new work so
                # the broker keeps unclaimed items available to other workers.
                await semaphore.acquire()
                claimed = False
                try:
                    try:
                        response = await client.get(
                            f"{base_url}/internal/worker/work-stream",
                            headers=headers,
                            params={"timeout": poll_timeout},
                            timeout=poll_timeout + 10,
                        )
                    except httpx.RequestError as exc:
                        # DNS/connect storm: exponential backoff (2,4,8,… cap 30s
                        # ± jitter) instead of a fixed 5s, with WARNING only for
                        # the first few attempts so episodes don't flood logs.
                        poll_attempt += 1
                        delay = _poll_retry_delay(poll_attempt)
                        _log_poll_failure(
                            poll_attempt,
                            f"Gateway poll failed: {exc}; retrying in {delay:.1f}s",
                        )
                        await asyncio.sleep(delay)
                        continue
                    if response.status_code == 204:
                        poll_attempt = 0
                        continue
                    # A gateway error response (5xx during deploys, 429 rate
                    # limit, 401 bad token, …) must never kill the worker
                    # process: raise_for_status() here caused 52–60 restart
                    # crash-loops on 28 Sep while the gateway returned 500s.
                    # Log, back off, and keep polling instead.
                    if response.status_code != 200:
                        poll_attempt += 1
                        delay = _poll_retry_delay(poll_attempt)
                        _log_poll_failure(
                            poll_attempt,
                            f"Gateway poll returned HTTP {response.status_code}; "
                            f"retrying in {delay:.1f}s",
                        )
                        await asyncio.sleep(delay)
                        continue
                    try:
                        work = response.json()
                    except ValueError as exc:
                        poll_attempt += 1
                        delay = _poll_retry_delay(poll_attempt)
                        _log_poll_failure(
                            poll_attempt,
                            f"Gateway poll returned non-JSON body: {exc}; "
                            f"retrying in {delay:.1f}s",
                        )
                        await asyncio.sleep(delay)
                        continue
                    poll_attempt = 0
                    task = asyncio.create_task(_process_http_work_item(
                        work,
                        client=client,
                        base_url=base_url,
                        headers=headers,
                        project_path=project_path,
                        semaphore=semaphore,
                    ))
                    live_tasks.add(task)
                    task.add_done_callback(live_tasks.discard)
                    claimed = True
                finally:
                    if not claimed:
                        semaphore.release()
        except (KeyboardInterrupt, asyncio.CancelledError):
            typer.echo("\nStopping HTTP worker...")
        finally:
            await _drain_or_cancel_tasks(live_tasks, shutdown_timeout)


@gateway_app.command("worker")
def gateway_worker(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path (auto-detected if not set)"
    ),
    max_activities: int = typer.Option(
        15, "--max-activities", envvar="TEMPORAL_MAX_CONCURRENT_ACTIVITIES",
        help="Maximum concurrent Temporal activities (agent executions per worker)"
    ),
    callback_url: Optional[str] = typer.Option(
        None, "--callback-url", envvar="CALLBACK_BASE_URL",
        help="Gateway URL for streaming event callbacks"
    ),
    callback_auth_token: Optional[str] = typer.Option(
        None, "--callback-auth-token", envvar="CALLBACK_AUTH_TOKEN",
        help="Shared secret for authenticating callback POSTs"
    ),
    tenant: Optional[str] = typer.Option(
        None, "--tenant", envvar="SEEKNAL_TENANT",
        help="Tenant ID this worker serves (maps to task queue seeknal-ask-{tenant}; 'default' uses legacy seeknal-ask queue). Overrides TEMPORAL_TASK_QUEUE."
    ),
    gateway_url: Optional[str] = typer.Option(
        None, "--gateway-url", envvar="SEEKNAL_GATEWAY_URL",
        help="Gateway URL to fetch token-derived worker runtime config"
    ),
    api_token: Optional[str] = typer.Option(
        None, "--api-token", envvar="SEEKNAL_API_TOKEN",
        help="Worker API token used to fetch tenant queue/callback config from the gateway"
    ),
    transport: str = typer.Option(
        "auto", "--transport", envvar="SEEKNAL_WORKER_TRANSPORT",
        help="Worker transport: auto, temporal, or http. http uses no Temporal SDK connection."
    ),
    max_concurrency: int = typer.Option(
        1, "--max-concurrency", envvar="SEEKNAL_WORKER_CONCURRENCY",
        help="Maximum concurrent agents per HTTP worker process (HTTP transport only). For Temporal transport, use --max-activities instead."
    ),
    shutdown_timeout: float = typer.Option(
        60.0, "--shutdown-timeout", envvar="SEEKNAL_WORKER_SHUTDOWN_TIMEOUT",
        help="Seconds to wait for in-flight tasks to drain on shutdown before cancelling (HTTP transport only)."
    ),
):
    """Start a standalone worker.

    In temporal mode, connects to Temporal and polls a task queue. In HTTP
    mode, talks only to the gateway/kc-service using SEEKNAL_GATEWAY_URL and
    SEEKNAL_API_TOKEN; the gateway owns Temporal routing.
    """
    import asyncio

    project_path = project or find_project_path()
    from seeknal.cli.ask import _load_project_env
    _load_project_env(project_path)
    transport = (transport or "auto").strip().lower()

    if transport in {"http", "gateway", "poll"}:
        if not gateway_url or not api_token:
            typer.echo(typer.style(
                "HTTP worker mode requires --gateway-url and --api-token",
                fg=typer.colors.RED,
            ))
            raise typer.Exit(1)
        try:
            asyncio.run(_run_http_only_worker(
                project_path=project_path,
                gateway_url=gateway_url,
                api_token=api_token,
                max_concurrency=max_concurrency,
                shutdown_timeout=shutdown_timeout,
            ))
        except KeyboardInterrupt:
            typer.echo("\nHTTP worker stopped.")
        return

    if transport == "temporal" and max_concurrency != 1:
        typer.echo(typer.style(
            "Note: --max-concurrency is HTTP-only; use --max-activities for Temporal transport.",
            fg=typer.colors.YELLOW,
        ))

    from seeknal.ask.gateway.tenant import task_queue_for_tenant

    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    # Task queue resolution in compatibility mode:
    # --tenant > TEMPORAL_TASK_QUEUE env var > legacy default.
    if tenant:
        temporal_task_queue = task_queue_for_tenant(tenant)
    else:
        temporal_task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "seeknal-ask")

    if gateway_url or api_token:
        if not gateway_url or not api_token:
            typer.echo(typer.style(
                "--gateway-url and --api-token must be provided together",
                fg=typer.colors.RED,
            ))
            raise typer.Exit(1)
        try:
            import httpx

            config_url = gateway_url.rstrip("/") + "/internal/worker/config"
            response = httpx.get(
                config_url,
                headers={"Authorization": f"Bearer {api_token}"},
                timeout=15.0,
            )
            response.raise_for_status()
            runtime_config = response.json()
            config_transport = str(runtime_config.get("worker_transport") or "").strip().lower()
            if transport == "auto" and config_transport in {"http", "gateway", "poll"}:
                asyncio.run(_run_http_only_worker(
                    project_path=project_path,
                    gateway_url=gateway_url,
                    api_token=api_token,
                    max_concurrency=max_concurrency,
                    shutdown_timeout=shutdown_timeout,
                ))
                return
        except Exception as exc:
            typer.echo(typer.style(
                f"Failed to fetch worker config from gateway: {exc}",
                fg=typer.colors.RED,
            ))
            raise typer.Exit(1)

        temporal_task_queue = runtime_config.get("task_queue") or temporal_task_queue
        temporal_address = runtime_config.get("temporal_address") or temporal_address
        temporal_namespace = runtime_config.get("temporal_namespace") or temporal_namespace
        callback_url = runtime_config.get("callback_url") or callback_url
        callback_auth_token = runtime_config.get("callback_auth_token") or callback_auth_token
        if project is None and runtime_config.get("project_path"):
            project_path = Path(runtime_config["project_path"])
        tenant = runtime_config.get("tenant_id") or tenant

    try:
        from seeknal.ask.gateway.temporal import (
            _require_temporal,
            connect_temporal_client,
            create_temporal_worker,
        )
        _require_temporal()
    except ImportError:
        typer.echo(typer.style(
            "Temporal requires: pip install seeknal[temporal]",
            fg=typer.colors.RED,
        ))
        raise typer.Exit(1)

    async def _run_worker():
        client = await connect_temporal_client(
            address=temporal_address,
            namespace=temporal_namespace,
        )
        if client is None:
            typer.echo(typer.style(
                f"Failed to connect to Temporal at {temporal_address}",
                fg=typer.colors.RED,
            ))
            raise typer.Exit(1)

        worker = create_temporal_worker(
            client,
            task_queue=temporal_task_queue,
            max_concurrent_activities=max_activities,
        )

        typer.echo("Seeknal worker started")
        typer.echo(f"  Project: {project_path}")
        typer.echo(f"  Temporal: {temporal_address}")
        typer.echo(f"  Queue: {temporal_task_queue}")
        typer.echo(f"  Max activities: {max_activities}")
        if callback_url:
            typer.echo(f"  Callback: {callback_url}")

        # Set environment for the worker activity to find
        os.environ["SEEKNAL_PROJECT_PATH"] = str(project_path)
        if callback_url:
            os.environ["CALLBACK_BASE_URL"] = callback_url
        if callback_auth_token:
            os.environ["CALLBACK_AUTH_TOKEN"] = callback_auth_token

        await worker.run()

    try:
        asyncio.run(_run_worker())
    except KeyboardInterrupt:
        typer.echo("\nWorker stopped.")

"""Gateway CLI — seeknal gateway start.

Starts the seeknal ask HTTP gateway (WebSocket + SSE + REST + Telegram + Temporal).
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import typer

from seeknal.ask.project import find_project_path
from seeknal.ask.safe_paths import UnsafePathSegment, contained_path

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


def _parse_worker_allowed_models(raw: str) -> set[tuple[str, str]]:
    """Parse ``SEEKNAL_WORKER_ALLOWED_MODELS`` into ``(provider, model)`` pairs.

    Format: comma-separated ``provider:model`` entries, e.g.
    ``"openai:gpt-4o,anthropic:claude-sonnet-4-6"``. Blank or malformed
    entries are skipped rather than raising — an operator typo in an env var
    must not crash the worker process.
    """
    allowed: set[tuple[str, str]] = set()
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        provider, _, model = entry.partition(":")
        provider = provider.strip()
        model = model.strip()
        if provider and model:
            allowed.add((provider, model))
    return allowed


def _resolve_worker_model_choice(
    work_provider: object, work_model: object
) -> tuple[Optional[str], Optional[str]]:
    """Refuse a broker-chosen provider/model that is not the operator's own.

    P2-3 (security review 2026-09-01, Part 2 §2.4): ``_process_http_work_item``
    used to pass ``work.get("provider")``/``work.get("model")`` straight into
    ``create_agent`` → ``get_model_string``. A compromised or malicious broker
    could use that to route the customer's prompt to a different configured
    provider than the operator chose. IBA's gateway does not send these
    fields today and has decided it never will — the premises node owns
    model selection — so a value the broker sends that is not already the
    operator's own configuration is refused rather than trusted.

    A value is honoured only if it equals the operator's configured
    provider/model (``SEEKNAL_ASK_LLM_PROVIDER`` / ``SEEKNAL_ASK_MODEL``, the
    same resolution ``get_model_string`` would apply with no override) or
    appears in ``SEEKNAL_WORKER_ALLOWED_MODELS`` (comma-separated
    ``provider:model`` entries; unset means only the configured pair is
    accepted). Missing entirely (today's IBA gateway) is treated the same as
    "matches configuration" — the pair is simply not present to disagree with.

    A refusal is deliberately **not fatal to the work item** — the IBA
    gateway never sends these fields, so a stricter failure would break
    nothing today, but would turn a future accidental/misconfigured key into
    a hard outage for every run. Instead the run proceeds with the
    operator's configuration, which is exactly what happens when the broker
    sends nothing at all.

    Returns the ``(provider, model)`` pair to actually use — either the
    broker's values (honoured) or ``(None, None)`` (refused or absent), which
    makes ``get_model_string`` resolve the operator's configured
    provider/model from the environment.
    """
    provider = str(work_provider) if work_provider else None
    model = str(work_model) if work_model else None
    if provider is None and model is None:
        return None, None

    from seeknal.ask.agents.providers import resolve_provider_config

    configured = resolve_provider_config(provider=None, model=None)
    configured_provider = configured["provider"]
    configured_model = configured["model"]

    effective_provider = provider or configured_provider
    effective_model = model or configured_model
    if (effective_provider, effective_model) == (configured_provider, configured_model):
        return provider, model

    allowed = _parse_worker_allowed_models(
        os.environ.get("SEEKNAL_WORKER_ALLOWED_MODELS", "")
    )
    if (effective_provider, effective_model) in allowed:
        return provider, model

    typer.echo(typer.style(
        f"[worker] refusing broker-supplied provider={provider!r} model={model!r}: "
        f"not the configured {configured_provider}:{configured_model} and not in "
        "SEEKNAL_WORKER_ALLOWED_MODELS -- proceeding with operator configuration",
        fg=typer.colors.YELLOW,
    ))
    return None, None


MAX_WORKER_RESUME_TURNS = 200
"""Same value as IBA's ``MAX_TRAJECTORY_TURNS`` (``iba_backend/trajectory.py``).

IBA already refuses to issue or verify a trajectory longer than this, so a
correctly-behaving broker never sends more. Kept equal rather than smaller so
this never refuses a claim IBA itself considers valid; kept no larger so a
broker that ignores its own bound (compromised, buggy, or not IBA at all)
cannot hand the worker more history than IBA's own security review assumed
was possible."""

MAX_WORKER_RESUME_TURNS_BYTES = 1_000_000
"""Same value as IBA's ``MAX_TRAJECTORY_BYTES`` (``iba_backend/trajectory.py``).
See ``MAX_WORKER_RESUME_TURNS`` for why it is equal rather than smaller."""

_RESUME_TURN_ROLES = frozenset({"user", "assistant", "system"})


def _resolve_worker_resume_turns(
    raw_resume_turns: object,
) -> "tuple[Optional[list], bool]":
    """Validate a broker-supplied ``resume_turns`` claim field, or refuse it.

    P2-1 (security review 2026-09-01, Part 2 §2.7): this worker used to read
    ``work_id``, ``session_id``, ``tenant_id``, ``question``, ``provider`` and
    ``model`` off the work item and never ``resume_turns`` -- so IBA's
    HMAC-sealed trajectory (``iba_backend/trajectory.py``,
    ``build_pause_trajectory``/``verify_trajectory``) travelled all the way to
    the claim payload and was dropped on the floor. This function is what
    makes the worker actually read it.

    ``resume_turns`` crosses the broker->worker trust boundary exactly like
    ``question`` and the broker-chosen provider/model do (see
    ``_resolve_worker_model_choice`` above, the sibling this mirrors): IBA's
    seal proves the turns are ones *IBA* previously issued to *this* identity
    and conversation -- it does not prove they are safe model input, any more
    than a verified ``question`` is. So this worker re-validates shape and
    bounds itself rather than trusting IBA's admission checks, exactly as it
    already refuses a broker-chosen provider/model rather than trusting IBA's
    allowlist.

    Bounds mirror IBA's own, so nothing a correctly-behaving broker sends is
    ever refused here -- see ``MAX_WORKER_RESUME_TURNS`` /
    ``MAX_WORKER_RESUME_TURNS_BYTES``. A broker that ignores its own bounds is
    what gets refused.

    Returns ``(turns, refused)``:

    - ``(None, False)`` -- ``resume_turns`` was absent, or present as an
      empty list. IBA's own contract already treats "absent" and "empty" as
      the same claim shape (ADR-0013: "Absence and an empty list say the same
      thing") and never emits an empty list on a real claim, so this is the
      ordinary case and produces no log line. The caller keeps today's
      behavior: history comes from the session store, unchanged.
    - ``(None, True)`` -- ``resume_turns`` was present but malformed. Refused
      with exactly one log line naming the reason, never the content. The
      caller must run this turn with NO history at all -- not a fall back to
      the session store -- because a broker that sent a malformed trust
      boundary field is not one whose ``session_id`` claim should be trusted
      to address the right history either.
    - ``(turns, False)`` -- ``resume_turns`` was present, well-formed, and
      within bounds. One log line records the count and byte size, never
      content. The caller replaces the session-store history with these
      turns for this run; see ``_run_agent_inner`` for why resume REPLACES
      rather than appends.
    """
    if raw_resume_turns is None:
        return None, False

    def _refuse(reason: str) -> "tuple[None, bool]":
        typer.echo(typer.style(
            f"[worker] refusing broker-supplied resume_turns: {reason} -- "
            "running this turn without history",
            fg=typer.colors.YELLOW,
        ))
        return None, True

    if not isinstance(raw_resume_turns, list):
        return _refuse(f"expected a list, got {type(raw_resume_turns).__name__}")
    if not raw_resume_turns:
        return None, False
    if len(raw_resume_turns) > MAX_WORKER_RESUME_TURNS:
        return _refuse(
            f"{len(raw_resume_turns)} turns exceeds the limit of "
            f"{MAX_WORKER_RESUME_TURNS}"
        )
    try:
        size = len(json.dumps(raw_resume_turns).encode("utf-8"))
    except (TypeError, ValueError):
        return _refuse("turns are not JSON-serializable")
    if size > MAX_WORKER_RESUME_TURNS_BYTES:
        return _refuse(
            f"{size} bytes exceeds the limit of {MAX_WORKER_RESUME_TURNS_BYTES}"
        )
    for turn in raw_resume_turns:
        if not isinstance(turn, dict):
            return _refuse(f"turn is not an object ({type(turn).__name__})")
        role = turn.get("role")
        if not isinstance(role, str) or role not in _RESUME_TURN_ROLES:
            return _refuse(f"turn has an invalid role ({role!r})")
        if not isinstance(turn.get("content"), str):
            return _refuse("turn has non-string content")

    typer.echo(
        f"[worker] seeding history from resume_turns: "
        f"turns={len(raw_resume_turns)} bytes={size}"
    )
    return raw_resume_turns, False


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

    from seeknal.ask.gateway.server import _run_agent_streaming
    from seeknal.ask.gateway.tenant import DEFAULT_TENANT

    work_id = work["work_id"]
    session_id = work["session_id"]
    tenant_id = work.get("tenant_id") or DEFAULT_TENANT
    question = work["question"]
    short_id = work_id[:8]
    worker_provider, worker_model = _resolve_worker_model_choice(
        work.get("provider"), work.get("model")
    )
    resume_turns, resume_turns_refused = _resolve_worker_resume_turns(
        work.get("resume_turns")
    )

    typer.echo(f"[work={short_id} session={session_id}] start")

    answer = ""
    error: Optional[str] = None
    event_count = 0

    async def post_event(event: dict) -> None:
        await client.post(
            f"{base_url}/internal/worker/work/{work_id}/event",
            headers=headers,
            json=event,
            timeout=15.0,
        )

    try:
        try:
            async for event in _run_agent_streaming(
                project_path,
                session_id,
                question,
                provider=worker_provider,
                model=worker_model,
                tenant_id=tenant_id,
                resume_turns=resume_turns,
                resume_turns_refused=resume_turns_refused,
            ):
                event_count += 1
                if event.get("type") == "answer":
                    answer = str(event.get("data") or "")
                elif event.get("type") == "error":
                    error = str(event.get("data") or "")
                await post_event(event)
        except asyncio.CancelledError:
            error = error or "worker shutting down"
            try:
                await post_event({"type": "error", "data": error})
                await post_event({"type": "done"})
            except Exception:  # noqa: BLE001
                pass
            raise
        except Exception as exc:  # noqa: BLE001 - surface worker failures to gateway
            error = str(exc)
            try:
                await post_event({"type": "error", "data": error})
                await post_event({"type": "done"})
            except Exception:  # noqa: BLE001
                pass
    finally:
        try:
            await client.post(
                f"{base_url}/internal/worker/work/{work_id}/complete",
                headers=headers,
                json={
                    "answer": answer,
                    "event_count": event_count,
                    "error": error,
                },
                timeout=15.0,
            )
        except Exception as exc:  # noqa: BLE001
            typer.echo(typer.style(
                f"[work={short_id} session={session_id}] complete POST failed: {exc}",
                fg=typer.colors.YELLOW,
            ))
        finally:
            semaphore.release()
            status = "error" if error else "ok"
            typer.echo(
                f"[work={short_id} session={session_id}] complete events={event_count} status={status}"
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
    min_poll_interval: float = 1.0,
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

    ``min_poll_interval`` is a floor on how often this worker re-polls after
    an instant ``204``. The wire contract (ADR-0014) is a long-poll — the
    gateway should hold the request open for up to ``timeout`` seconds before
    answering ``204`` — but this worker cannot assume every gateway it talks
    to honours that. A gateway that answers ``204`` immediately would
    otherwise drive this loop as fast as it can dial, as happened for three
    days against a gateway that ignored ``timeout`` entirely. Measuring
    elapsed wall time around the poll (rather than sleeping unconditionally)
    means a gateway that *does* long-poll for close to ``min_poll_interval``
    adds no extra delay.
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
                    poll_started = loop.time()
                    try:
                        response = await client.get(
                            f"{base_url}/internal/worker/work-stream",
                            headers=headers,
                            params={"timeout": poll_timeout},
                            timeout=poll_timeout + 10,
                        )
                    except httpx.RequestError as exc:
                        typer.echo(typer.style(
                            f"Gateway poll failed: {exc}; retrying",
                            fg=typer.colors.YELLOW,
                        ))
                        await asyncio.sleep(5)
                        continue
                    if response.status_code == 204:
                        # Back-off floor: a gateway that answers 204 instantly
                        # (rather than long-polling for ~poll_timeout seconds)
                        # must not drive this loop into a busy spin. Elapsed
                        # wall time is measured, not assumed, so a gateway
                        # that already held the request near min_poll_interval
                        # adds no extra sleep.
                        elapsed = loop.time() - poll_started
                        remaining = min_poll_interval - elapsed
                        if remaining > 0:
                            await asyncio.sleep(remaining)
                        continue
                    response.raise_for_status()
                    work = response.json()
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


class BrokerProjectPathRefused(RuntimeError):
    """A broker-supplied ``project_path`` failed the operator's containment rule."""


def _resolve_broker_project_path(
    raw_project_path: object, *, project_root: Optional[Path]
) -> Path:
    """Validate a broker-supplied ``project_path`` against the operator's root.

    Reached only when ``--project`` was omitted, so it is the gateway's own
    ``/internal/worker/config`` response naming the directory this worker is
    about to operate on (P2-4 in the IBA v2 security review, 2026-09-01,
    Part 2 §2.4). That is the same trust boundary C-1 closed for session and
    tenant ids in ``safe_paths.py`` — the broker does not get to choose a
    worker filesystem root the operator never authorised.

    Raises:
        BrokerProjectPathRefused: with an operator-facing message, if there
            is no configured root, the path is not absolute, it does not
            exist as a directory, or it resolves outside ``project_root``
            (symlinks followed, compared as resolved path objects — see
            ``safe_paths.contained_path`` — never by string prefix).
    """
    if project_root is None:
        raise BrokerProjectPathRefused(
            "the gateway supplied a project_path but SEEKNAL_PROJECT_ROOT is "
            "not set. Set --project-root / SEEKNAL_PROJECT_ROOT to the "
            "directory under which broker-chosen projects may live, or pass "
            "--project explicitly so the broker's value is ignored."
        )
    candidate = Path(str(raw_project_path))
    if not candidate.is_absolute():
        raise BrokerProjectPathRefused(
            f"broker-supplied project_path {str(candidate)!r} is not an absolute path"
        )
    if not candidate.is_dir():
        raise BrokerProjectPathRefused(
            f"broker-supplied project_path {str(candidate)!r} does not exist as a directory"
        )
    try:
        return contained_path(project_root, candidate, label="broker-supplied project_path")
    except UnsafePathSegment as exc:
        raise BrokerProjectPathRefused(str(exc)) from exc


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
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", envvar="SEEKNAL_PROJECT_ROOT",
        help="Operator-configured root directory a broker-supplied project_path must be "
             "contained within (Temporal transport, --project omitted only). Required for "
             "the worker to accept a project_path from the gateway's /internal/worker/config."
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
    min_poll_interval: float = typer.Option(
        1.0, "--min-poll-interval", envvar="SEEKNAL_WORKER_MIN_POLL_INTERVAL",
        help="Minimum seconds between poll requests when the gateway answers 204 instantly "
             "instead of long-polling (HTTP transport only)."
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
                min_poll_interval=min_poll_interval,
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
                    min_poll_interval=min_poll_interval,
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
            try:
                project_path = _resolve_broker_project_path(
                    runtime_config["project_path"], project_root=project_root
                )
            except BrokerProjectPathRefused as exc:
                typer.echo(typer.style(f"Refusing broker-supplied project_path: {exc}", fg=typer.colors.RED))
                raise typer.Exit(1)
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

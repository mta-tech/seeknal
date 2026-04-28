"""Gateway CLI — seeknal gateway start.

Starts the seeknal ask HTTP gateway (WebSocket + SSE + REST + Telegram + Temporal).
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import typer

from seeknal.ask.project import find_project_path

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

                    worker = create_temporal_worker(
                        client,
                        task_queue=temporal_task_queue,
                        max_concurrent_activities=max_activities,
                    )
                    worker_task = asyncio.create_task(worker.run())
                    typer.echo(typer.style(
                        f"Temporal worker enabled (queue={temporal_task_queue}, max_activities={max_activities})",
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
        from seeknal.ask.gateway.temporal import connect_temporal_client

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
        else:
            typer.echo(typer.style(
                f"Failed to connect to Temporal at {temporal_address}",
                fg=typer.colors.RED,
            ))
            raise typer.Exit(1)

        yield

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
):
    """Start a standalone Temporal worker (no HTTP server).

    Connects to Temporal, polls the task queue, and executes agent
    activities locally. Use this for on-prem worker deployments where
    the gateway runs elsewhere.

    Streaming events are POSTed to the gateway's callback URL.
    """
    import asyncio

    project_path = project or find_project_path()

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

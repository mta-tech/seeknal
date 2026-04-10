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
        False, "--temporal", help="Enable Temporal worker for durable agent execution"
    ),
):
    """Start the seeknal ask gateway server."""
    project_path = project or find_project_path()

    try:
        from seeknal.ask.gateway.server import create_gateway_app
    except ImportError:
        typer.echo(typer.style(
            "Gateway dependencies not installed.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install seeknal[ask]", fg=typer.colors.CYAN
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
                "Telegram requires: pip install seeknal[telegram]",
                fg=typer.colors.YELLOW,
            ))

    temporal_enabled = False
    temporal_address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    temporal_namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    temporal_task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "seeknal-ask")
    temporal_tls = os.environ.get("TEMPORAL_TLS", "false").lower() in ("true", "1", "yes")

    if temporal:
        try:
            from seeknal.ask.gateway.temporal import (
                TEMPORAL_AVAILABLE,
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
            app.state.telegram_channel = telegram_channel
            await telegram_channel.start()

        # Startup: Temporal
        worker_task = None
        if temporal_enabled:
            from seeknal.ask.gateway.temporal import (
                connect_temporal_client,
                create_temporal_worker,
            )

            client = await connect_temporal_client(
                address=temporal_address,
                namespace=temporal_namespace,
                tls=temporal_tls,
            )
            if client is not None:
                temporal_client_holder.append(client)
                app.state.temporal_client = client
                app.state.temporal_task_queue = temporal_task_queue

                import asyncio

                worker = create_temporal_worker(client, task_queue=temporal_task_queue)
                worker_task = asyncio.create_task(worker.run())
                typer.echo(typer.style(
                    f"Temporal worker enabled (queue={temporal_task_queue})",
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
            if worker_task is not None:
                await worker.shutdown()
                await worker_task

            # Shutdown: Telegram
            if telegram_channel is not None:
                await telegram_channel.stop()

    # Pass temporal_client if already connected (for sync startup path)
    # The lifespan will also set it on app.state for the async path
    app = create_gateway_app(project_path, lifespan=lifespan)

    typer.echo(f"Starting gateway on {host}:{port}")
    typer.echo(f"Project: {project_path}")
    typer.echo(f"Endpoints:")
    typer.echo(f"  GET  /health")
    typer.echo(f"  GET  /sessions")
    typer.echo(f"  POST /ask")
    typer.echo(f"  POST /temporal/start")
    typer.echo(f"  WS   /ws/{{session_id}}")
    typer.echo(f"  GET  /events/{{session_id}}")

    uvicorn.run(app, host=host, port=port, log_level="info")

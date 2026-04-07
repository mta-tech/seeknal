"""Gateway CLI — seeknal gateway start.

Starts the seeknal ask HTTP gateway (WebSocket + SSE + REST + Telegram).
"""

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

    app = create_gateway_app(project_path)

    # Wire Telegram if requested
    if telegram:
        try:
            from seeknal.ask.gateway.channels.telegram import TelegramChannel

            channel = TelegramChannel(project_path)
            # Store for startup/shutdown hooks
            app.state.telegram_channel = channel

            @app.on_event("startup")
            async def _start_telegram():
                await channel.start()

            @app.on_event("shutdown")
            async def _stop_telegram():
                await channel.stop()

            typer.echo(typer.style("Telegram channel enabled", fg=typer.colors.GREEN))
        except ImportError:
            typer.echo(typer.style(
                "Telegram requires: pip install seeknal[telegram]",
                fg=typer.colors.YELLOW,
            ))

    typer.echo(f"Starting gateway on {host}:{port}")
    typer.echo(f"Project: {project_path}")
    typer.echo(f"Endpoints:")
    typer.echo(f"  GET  /health")
    typer.echo(f"  GET  /sessions")
    typer.echo(f"  POST /ask")
    typer.echo(f"  POST /ask/stream  (webhook streaming)")
    typer.echo(f"  WS   /ws/{{session_id}}")
    typer.echo(f"  GET  /events/{{session_id}}")

    uvicorn.run(app, host=host, port=port, log_level="info")

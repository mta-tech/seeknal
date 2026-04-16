from __future__ import annotations

from typing import Optional
from pathlib import Path

import typer

report_server_app = typer.Typer(
    name="report-server",
    help="Seeknal Report Server for hosting published Evidence.dev reports.",
    no_args_is_help=True,
)


@report_server_app.command("start")
def report_server_start(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind to"),
    port: int = typer.Option(8787, "--port", help="Port to listen on"),
    data_dir: Optional[Path] = typer.Option(
        None, "--data-dir", help="Directory for storing published report data"
    ),
) -> None:
    """Start the Seeknal Report Server."""
    try:
        from seeknal.report_server import create_app
        from seeknal.report_server.config import ServerConfig
    except ImportError:
        typer.echo(typer.style(
            "Report server dependencies not installed.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install seeknal[report-server]", fg=typer.colors.CYAN
        ))
        raise typer.Exit(1)

    import uvicorn

    config = ServerConfig.from_env()
    if data_dir:
        config.data_dir = data_dir
    config.validate()
    app = create_app(config)

    typer.echo(f"Starting Seeknal Report Server on {host}:{port}")
    typer.echo(f"  Auth mode: {typer.style(config.auth_mode, fg=typer.colors.CYAN)}")
    if config.auth_mode == "open":
        typer.echo(typer.style(
            "  WARNING: open mode — do not expose to the internet",
            fg=typer.colors.YELLOW,
        ))
    typer.echo(f"  Data dir:  {config.data_dir}")

    uvicorn.run(app, host=host, port=port, log_level="info")

"""Heartbeat CLI — seeknal heartbeat run/status.

Provides manual heartbeat execution and status inspection for
HEARTBEAT.md-driven monitoring.
"""

import asyncio
from pathlib import Path
from typing import Optional

import typer

heartbeat_app = typer.Typer(
    name="heartbeat",
    help="HEARTBEAT.md-driven project monitoring.",
    no_args_is_help=True,
)


@heartbeat_app.command("run")
def heartbeat_run(
    project_path: Optional[str] = typer.Option(
        None, "--project-path", help="Path to seeknal project"
    ),
):
    """Run a one-off heartbeat check and print the result."""
    from seeknal.ask.config import load_agent_config
    from seeknal.ask.gateway.heartbeat.runner import HeartbeatRunner

    pp = Path(project_path) if project_path else Path.cwd()
    agent_config = load_agent_config(pp)
    hb_config = agent_config.get("heartbeat", {})
    if not isinstance(hb_config, dict):
        hb_config = {}

    runner = HeartbeatRunner(config=hb_config, channels={})

    typer.echo(f"Running heartbeat for project: {pp}")
    result = asyncio.run(runner.run_once(pp))
    typer.echo("")
    typer.echo(result)


@heartbeat_app.command("status")
def heartbeat_status(
    project_path: Optional[str] = typer.Option(
        None, "--project-path", help="Path to seeknal project"
    ),
):
    """Show heartbeat status — last run, result, and next scheduled."""
    from seeknal.ask.gateway.heartbeat.runner import _load_state

    pp = Path(project_path) if project_path else Path.cwd()
    state = _load_state(pp)

    if not state:
        typer.echo("Never run")
        typer.echo("")
        typer.echo("Run a heartbeat with:")
        typer.echo(
            typer.style("  seeknal heartbeat run", fg=typer.colors.CYAN)
        )
        return

    typer.echo(f"Last run:    {state.get('last_run', 'unknown')}")
    typer.echo(f"Result:      {state.get('last_result', 'unknown')}")
    last_msg = state.get("last_message", "")
    if last_msg:
        # Truncate for display
        display_msg = last_msg[:200]
        if len(last_msg) > 200:
            display_msg += "..."
        typer.echo(f"Message:     {display_msg}")

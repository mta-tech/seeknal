"""Session management CLI — seeknal session list/show/delete.

Manages persistent chat sessions stored in `.seeknal/sessions.db`.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from seeknal.ask.project import find_project_path

session_app = typer.Typer(
    name="session",
    help="Manage persistent chat sessions.",
    no_args_is_help=True,
)


@session_app.command("list")
def session_list(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path (auto-detected if not set)"
    ),
):
    """List all chat sessions for the current project."""
    from seeknal.utils.path_security import is_insecure_path

    project_path = project or find_project_path()
    if is_insecure_path(str(project_path)):
        typer.echo(typer.style(f"Insecure project path: {project_path}", fg=typer.colors.RED))
        raise typer.Exit(1)

    from seeknal.ask.sessions import SessionStore

    with SessionStore(project_path) as store:
        sessions = store.list()

    if not sessions:
        typer.echo("No sessions found. Start one with:")
        typer.echo(typer.style("  seeknal ask chat", fg=typer.colors.CYAN))
        return

    # Table header
    typer.echo(
        f"{'Name':<25} {'Status':<10} {'Messages':<10} "
        f"{'Updated':<20} {'Last Question'}"
    )
    typer.echo("-" * 90)

    for s in sessions:
        name = s["name"]
        status = s["status"]
        msg_count = s.get("message_count", 0)
        updated = s.get("updated_at", "")[:19]  # Trim microseconds
        last_q = (s.get("last_question") or "")[:40]
        if len(s.get("last_question") or "") > 40:
            last_q += "..."
        typer.echo(f"{name:<25} {status:<10} {msg_count:<10} {updated:<20} {last_q}")


@session_app.command("show")
def session_show(
    name: str = typer.Argument(..., help="Session name"),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
):
    """Show details for a specific session."""
    from seeknal.utils.path_security import is_insecure_path

    project_path = project or find_project_path()
    if is_insecure_path(str(project_path)):
        typer.echo(typer.style(f"Insecure project path: {project_path}", fg=typer.colors.RED))
        raise typer.Exit(1)

    from seeknal.ask.sessions import SessionStore

    with SessionStore(project_path) as store:
        session = store.get(name)

    if session is None:
        typer.echo(typer.style(f"Session '{name}' not found.", fg=typer.colors.RED))
        raise typer.Exit(1)

    typer.echo(f"Name:          {session['name']}")
    typer.echo(f"Status:        {session['status']}")
    typer.echo(f"Created:       {session['created_at']}")
    typer.echo(f"Updated:       {session['updated_at']}")
    typer.echo(f"Messages:      {session.get('message_count', 0)}")
    last_q = session.get("last_question") or "(none)"
    typer.echo(f"Last Question: {last_q}")
    typer.echo()
    typer.echo(
        "Resume with: "
        + typer.style(f"seeknal ask chat --session {name}", fg=typer.colors.CYAN)
    )


@session_app.command("delete")
def session_delete(
    name: str = typer.Argument(..., help="Session name to delete"),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip confirmation prompt"
    ),
):
    """Delete a session and its checkpoint data."""
    from seeknal.utils.path_security import is_insecure_path

    project_path = project or find_project_path()
    if is_insecure_path(str(project_path)):
        typer.echo(typer.style(f"Insecure project path: {project_path}", fg=typer.colors.RED))
        raise typer.Exit(1)

    from seeknal.ask.sessions import SessionStore

    with SessionStore(project_path) as store:
        session = store.get(name)

        if session is None:
            typer.echo(typer.style(f"Session '{name}' not found.", fg=typer.colors.RED))
            raise typer.Exit(1)

        if not force:
            confirm = typer.confirm(f"Delete session '{name}'?")
            if not confirm:
                typer.echo("Cancelled.")
                return

        store.delete(name)
        typer.echo(f"Session '{name}' deleted.")

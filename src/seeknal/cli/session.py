"""Session management CLI — seeknal session list/show/delete.

Manages persistent chat sessions stored in `.seeknal/sessions/`.
"""

import asyncio
from pathlib import Path
from typing import Optional

import typer

from seeknal.ask.project import find_project_path

session_app = typer.Typer(
    name="session",
    help="Manage persistent chat sessions.",
    no_args_is_help=True,
)
public_app = typer.Typer(
    name="public",
    help="Manage a public session that Telegram can use without pairing.",
    no_args_is_help=True,
)


@session_app.command("list")
def session_list(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path (auto-detected if not set)"
    ),
):
    """List all chat sessions for the current project."""
    project_path = project or find_project_path()

    from seeknal.ask.sessions import SessionStore

    with SessionStore(project_path) as store:
        sessions = store.list()

    if not sessions:
        typer.echo("No sessions found. Start one with:")
        typer.echo(typer.style("  seeknal ask chat", fg=typer.colors.CYAN))
        return

    typer.echo(
        f"{'Name':<25} {'Status':<10} {'Messages':<10} "
        f"{'Updated':<20} {'Last Question'}"
    )
    typer.echo("-" * 90)

    for s in sessions:
        name = s["name"]
        status = s["status"]
        msg_count = s.get("message_count", 0)
        updated = s.get("updated_at", "")[:19]
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
    project_path = project or find_project_path()

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
    """Delete a session and its conversation data."""
    project_path = project or find_project_path()

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


@session_app.command("pair")
def session_pair(
    name: str = typer.Argument(..., help="Existing session name to share"),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
    ttl_minutes: int = typer.Option(
        10, "--ttl-minutes", min=1, help="How long the pair code stays valid"
    ),
):
    """Generate a one-time pair code for an existing session."""
    project_path = project or find_project_path()

    from seeknal.ask.gateway.pairing import FilePairingStore
    from seeknal.ask.sessions import SessionStore

    with SessionStore(project_path) as store:
        session = store.get(name)

    if session is None:
        typer.echo(typer.style(f"Session '{name}' not found.", fg=typer.colors.RED))
        raise typer.Exit(1)

    pairing_store = FilePairingStore(project_path)
    record = asyncio.run(
        pairing_store.create_pair_code(
            name,
            ttl_seconds=ttl_minutes * 60,
        )
    )

    typer.echo(f"Session:   {name}")
    typer.echo(f"Pair code: {record.code}")
    typer.echo(f"Expires:   {record.expires_at.isoformat()}")
    typer.echo(
        "Telegram:  "
        + typer.style(f"/pair {record.code}", fg=typer.colors.CYAN)
    )


@session_app.command("paired")
def session_paired(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
    session_name: Optional[str] = typer.Option(
        None, "--session", help="Filter by session name"
    ),
):
    """List paired Telegram device/chat ids."""
    project_path = project or find_project_path()

    from seeknal.ask.gateway.pairing import TelegramLinkStore

    link_store = TelegramLinkStore(project_path)
    links = link_store.list_links(session_id=session_name)

    if not links:
        if session_name:
            typer.echo(f"No paired Telegram device ids found for session '{session_name}'.")
        else:
            typer.echo("No paired Telegram device ids found.")
        return

    typer.echo(
        f"{'Device ID':<18} {'Session':<25} {'Linked At'}"
    )
    typer.echo("-" * 72)
    for item in links:
        typer.echo(
            f"{item.get('chat_id', ''):<18} "
            f"{item.get('session_id', ''):<25} "
            f"{item.get('linked_at', '')[:19]}"
        )


@public_app.command("set")
def session_public_set(
    name: str = typer.Argument(..., help="Session name to expose publicly"),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
):
    """Allow Telegram chats to use a session without pairing."""
    project_path = project or find_project_path()

    from seeknal.ask.gateway.pairing import PublicSessionStore
    from seeknal.ask.sessions import SessionStore

    with SessionStore(project_path) as store:
        session = store.get(name)

    if session is None:
        typer.echo(typer.style(f"Session '{name}' not found.", fg=typer.colors.RED))
        raise typer.Exit(1)

    public_store = PublicSessionStore(project_path)
    public_store.set_session(name)

    typer.echo(f"Public Telegram session set to: {name}")


@public_app.command("show")
def session_public_show(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
):
    """Show the current public Telegram session."""
    project_path = project or find_project_path()

    from seeknal.ask.gateway.pairing import PublicSessionStore

    public_store = PublicSessionStore(project_path)
    session_id = public_store.get_session_id()
    if not session_id:
        typer.echo("No public Telegram session configured.")
        return
    typer.echo(f"Public Telegram session: {session_id}")


@public_app.command("clear")
def session_public_clear(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
):
    """Disable public Telegram access without pairing."""
    project_path = project or find_project_path()

    from seeknal.ask.gateway.pairing import PublicSessionStore

    public_store = PublicSessionStore(project_path)
    if public_store.clear():
        typer.echo("Cleared public Telegram session.")
    else:
        typer.echo("No public Telegram session configured.")


session_app.add_typer(public_app, name="public")

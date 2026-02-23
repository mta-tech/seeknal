"""Source management CLI commands.

Provides:
  seeknal source add <name> --url <connection_url>
  seeknal source list
  seeknal source remove <name>
"""
from __future__ import annotations

import typer
from urllib.parse import urlparse

from seeknal.utils.encryption import encrypt_value, is_encryption_configured
from seeknal.request import ReplSourceRequest

app = typer.Typer(help="Manage data sources for REPL")


def _mask_url_password(url: str) -> str:
    """Replace password in URL with ****."""
    parsed = urlparse(url)
    if parsed.password:
        # Reconstruct URL with masked password
        masked_netloc = (
            f"{parsed.username}:****@{parsed.hostname}"
            if parsed.hostname
            else ""
        )
        if parsed.port:
            masked_netloc += f":{parsed.port}"
        return parsed._replace(netloc=masked_netloc).geturl()
    return url


def _parse_and_encrypt_url(url: str) -> dict:
    """Parse URL and encrypt sensitive parts.

    Returns dict with: source_type, masked_url, encrypted_credentials,
                       host, port, database, username
    """
    parsed = urlparse(url)

    # Determine source type
    scheme = parsed.scheme.lower() if parsed.scheme else ""
    if scheme in ("postgres", "postgresql"):
        source_type = "postgres"
    elif scheme == "mysql":
        source_type = "mysql"
    elif scheme == "sqlite":
        source_type = "sqlite"
    elif url.endswith(".parquet"):
        source_type = "parquet"
    elif url.endswith(".csv"):
        source_type = "csv"
    else:
        source_type = "unknown"

    # Encrypt password if present
    encrypted_credentials = None
    if parsed.password:
        encrypted_credentials = encrypt_value(parsed.password)

    # Create masked URL for display
    masked_url = _mask_url_password(url)

    return {
        "source_type": source_type,
        "masked_url": masked_url,
        "encrypted_credentials": encrypted_credentials,
        "host": parsed.hostname,
        "port": parsed.port,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "username": parsed.username,
    }


@app.command("add")
def add_source(
    name: str = typer.Argument(..., help="Unique name for this source"),
    url: str = typer.Option(..., "--url", "-u", help="Connection URL"),
):
    """Add a new data source with encrypted credentials.

    Example:
        seeknal source add mydb --url postgres://user:pass@localhost/mydb
    """
    if not is_encryption_configured():
        typer.echo(
            "Error: SEEKNAL_ENCRYPT_KEY not set. Generate one with:\n"
            "  python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'\n"
            "Then: export SEEKNAL_ENCRYPT_KEY=<your-key>",
            err=True,
        )
        raise typer.Exit(1)

    # Parse and encrypt
    source_data = _parse_and_encrypt_url(url)
    source_data["name"] = name

    # Store in database
    existing = ReplSourceRequest.select_by_name(name)
    if existing:
        typer.echo(f"Source '{name}' already exists. Use 'seeknal source remove {name}' first.")
        raise typer.Exit(1)

    req = ReplSourceRequest(body=source_data)
    req.save()
    typer.echo(f"Added source '{name}': {source_data['masked_url']}")


@app.command("list")
def list_sources():
    """List all saved data sources."""
    sources = ReplSourceRequest.select_all()

    if not sources:
        typer.echo("No sources configured. Add one with: seeknal source add <name> --url <url>")
        return

    typer.echo(f"{'NAME':<20} {'TYPE':<12} {'URL'}")
    typer.echo("-" * 60)
    for s in sources:
        typer.echo(f"{s.name:<20} {s.source_type:<12} {s.masked_url}")


@app.command("remove")
def remove_source(name: str = typer.Argument(..., help="Name of source to remove")):
    """Remove a saved data source."""
    if not ReplSourceRequest.select_by_name(name):
        typer.echo(f"Source '{name}' not found.")
        raise typer.Exit(1)

    ReplSourceRequest.delete_by_name(name)
    typer.echo(f"Removed source '{name}'")

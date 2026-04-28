"""Source management CLI commands.

Provides:
  seeknal source add <name> --url <connection_url>
  seeknal source list
  seeknal source status
  seeknal source inspect <name>
  seeknal source sync [name]
  seeknal source remove <name>
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import typer
import yaml

from seeknal.request import ReplSourceRequest
from seeknal.sources.config import (
    SourceConfigError,
    load_source_registry,
    read_sync_state,
    write_source_context,
)
from seeknal.utils.encryption import encrypt_value, is_encryption_configured

app = typer.Typer(help="Manage data sources for REPL and Ask")


def _mask_url_password(url: str) -> str:
    """Replace password in URL with ****."""
    parsed = urlparse(url)
    if parsed.password:
        # Reconstruct URL with masked password
        masked_netloc = (
            f"{parsed.username}:****@{parsed.hostname}" if parsed.hostname else ""
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


def _project_path(project: Path | None = None) -> Path:
    if project is not None:
        return project
    try:
        from seeknal.ask.project import find_project_path

        return find_project_path()
    except Exception:
        return Path.cwd()


def _load_registry(project: Path | None = None):
    return load_source_registry(_project_path(project))


def _format_bool(value: bool) -> str:
    return "yes" if value else "no"


@app.command("add")
def add_source(
    name: str = typer.Argument(..., help="Unique name for this source"),
    url: str = typer.Option(..., "--url", "-u", help="Connection URL"),
):
    """Add a new REPL data source with encrypted credentials.

    Example:
        seeknal source add mydb --url postgres://user:pass@localhost/mydb
    """
    if not is_encryption_configured():
        typer.echo(
            "Error: SEEKNAL_ENCRYPT_KEY not set. Generate one with:\n"
            "  python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'\n"
            "Then: export SEEKNAL_ENCRYPT_KEY=<your-key>",
        )
        raise typer.Exit(1)

    # Parse and encrypt
    source_data = _parse_and_encrypt_url(url)
    source_data["name"] = name

    # Store in database
    existing = ReplSourceRequest.select_by_name(name)
    if existing:
        typer.echo(
            f"Source '{name}' already exists. Use 'seeknal source remove {name}' first."
        )
        raise typer.Exit(1)

    req = ReplSourceRequest(body=source_data)
    req.save()
    typer.echo(f"Added source '{name}': {source_data['masked_url']}")


@app.command("list")
def list_sources(
    project: Path
    | None = typer.Option(
        None, "--project", help="Seeknal project path for configured Ask sources"
    ),
):
    """List configured Ask sources or saved REPL sources.

    ``seeknal_agent.yml`` sources take precedence because they steer the Ask
    agent harness.  When no project sources are configured, this preserves the
    legacy encrypted REPL source list.
    """
    try:
        registry = _load_registry(project)
    except SourceConfigError as exc:
        typer.echo(f"Invalid source config: {exc}", err=True)
        raise typer.Exit(1) from exc

    if registry.explicit:
        typer.echo(
            f"{'NAME':<20} {'NS':<10} {'KIND':<10} {'TYPE':<10} {'ACCESS':<10} {'ROLE'}"
        )
        typer.echo("-" * 86)
        for source in registry.sources.values():
            typer.echo(
                f"{source.name:<20} {source.namespace:<10} {source.source_kind:<10} "
                f"{source.source_type:<10} {source.access:<10} {source.role}"
            )
        return

    sources = ReplSourceRequest.select_all()

    if not sources:
        typer.echo(
            "No sources configured. Add one with: seeknal source add <name> --url <url>"
        )
        return

    typer.echo(f"{'NAME':<20} {'TYPE':<12} {'URL'}")
    typer.echo("-" * 60)
    for s in sources:
        typer.echo(f"{s.name:<20} {s.source_type:<12} {s.masked_url}")


@app.command("status")
def status_sources(
    project: Path
    | None = typer.Option(
        None, "--project", help="Seeknal project path for configured Ask sources"
    ),
):
    """Show Ask source registry and context-sync status."""
    project_path = _project_path(project)
    try:
        registry = load_source_registry(project_path)
        sync_state = read_sync_state(project_path)
    except SourceConfigError as exc:
        typer.echo(f"Invalid source config: {exc}", err=True)
        raise typer.Exit(1) from exc

    if not registry.sources:
        typer.echo(
            "No project source registry found. Ask will use the current project tables only."
        )
        return

    typer.echo(f"Project: {project_path}")
    typer.echo(f"Default mode: {registry.default_mode}")
    typer.echo(f"Explicit registry: {_format_bool(registry.explicit)}")
    typer.echo("")
    typer.echo(f"{'NAME':<20} {'NS':<10} {'ACCESS':<10} {'SYNC':<14} {'SYNCED_AT'}")
    typer.echo("-" * 86)
    state_sources = sync_state.get("sources") if isinstance(sync_state, dict) else {}
    if not isinstance(state_sources, dict):
        state_sources = {}
    for source in registry.sources.values():
        state = state_sources.get(source.name) or {}
        typer.echo(
            f"{source.name:<20} {source.namespace:<10} {source.access:<10} "
            f"{state.get('status', 'not_synced'):<14} {state.get('synced_at', '-') }"
        )


@app.command("inspect")
def inspect_source(
    name: str = typer.Argument(..., help="Source name or namespace"),
    project: Path
    | None = typer.Option(
        None, "--project", help="Seeknal project path for configured Ask sources"
    ),
):
    """Inspect one Ask source declaration."""
    project_path = _project_path(project)
    try:
        registry = load_source_registry(project_path)
        source = registry.selected([name])[0]
        sync_state = read_sync_state(project_path)
    except SourceConfigError as exc:
        typer.echo(f"Invalid source config: {exc}", err=True)
        raise typer.Exit(1) from exc

    state_sources = sync_state.get("sources") if isinstance(sync_state, dict) else {}
    if not isinstance(state_sources, dict):
        state_sources = {}
    state = state_sources.get(source.name) or {}

    typer.echo(f"Name: {source.name}")
    typer.echo(f"Namespace: {source.namespace}")
    typer.echo(f"Kind: {source.source_kind}")
    typer.echo(f"Type: {source.source_type}")
    typer.echo(f"Connector: {source.connector or '-'}")
    typer.echo(f"Access: {source.access}")
    typer.echo(f"Role: {source.role}")
    typer.echo(f"Priority: {source.priority}")
    typer.echo(f"Description: {source.description or '-'}")
    typer.echo(f"Include: {', '.join(source.include) if source.include else '-'}")
    typer.echo(f"Exclude: {', '.join(source.exclude) if source.exclude else '-'}")
    typer.echo(f"Context sync enabled: {_format_bool(source.context_sync.enabled)}")
    typer.echo(f"Refresh policy: {source.context_sync.refresh_policy}")
    typer.echo(f"Stale after hours: {source.context_sync.stale_after_hours}")
    typer.echo(f"Templates: {', '.join(source.context_sync.templates)}")
    typer.echo(f"Sync status: {state.get('status', 'not_synced')}")
    typer.echo(f"Synced at: {state.get('synced_at', '-')}")
    typer.echo(f"Context path: {source.context_path}")


@app.command("sync")
def sync_sources(
    names: list[str]
    | None = typer.Argument(None, help="Optional source names/namespaces to sync"),
    project: Path
    | None = typer.Option(
        None, "--project", help="Seeknal project path for configured Ask sources"
    ),
):
    """Write metadata-only context artifacts for Ask source steering.

    This command is intentionally read-only with respect to the data source:
    it does not open database connections yet.  It creates local context files
    under ``.seeknal/context/sources`` and sync state under ``.seeknal/catalog``.
    """
    project_path = _project_path(project)
    try:
        registry = load_source_registry(project_path)
        repl = None
        selected_sources = registry.selected(names)
        if any(source.context_sync.enabled for source in selected_sources):
            try:
                from seeknal.cli.repl import REPL

                repl = REPL(project_path=project_path, skip_history=True)
            except Exception:
                repl = None
        results = write_source_context(
            registry,
            project_path,
            source_names=names,
            repl=repl,
        )
    except SourceConfigError as exc:
        typer.echo(f"Invalid source config: {exc}", err=True)
        raise typer.Exit(1) from exc

    if not results:
        typer.echo("No sources to sync.")
        return

    for result in results:
        typer.echo(
            f"Synced {result['name']} ({result['namespace']}) -> "
            f"{result['context_path']} [{result['status']}]"
        )


@app.command("test")
def test_source(
    name: str = typer.Argument(..., help="Source name or namespace to test"),
    project: Path
    | None = typer.Option(
        None, "--project", help="Seeknal project path for configured Ask sources"
    ),
):
    """Test a configured Ask source using read-only discovery queries."""
    project_path = _project_path(project)
    try:
        registry = load_source_registry(project_path)
        source = registry.selected([name])[0]
    except SourceConfigError as exc:
        typer.echo(f"Invalid source config: {exc}", err=True)
        raise typer.Exit(1) from exc

    try:
        from seeknal.cli.repl import REPL

        repl = REPL(project_path=project_path, skip_history=True)
        if source.namespace not in getattr(repl, "attached", set()):
            typer.echo(
                f"Source '{source.name}' was not attached as namespace "
                f"'{source.namespace}'. Check dsn_env/profiles.yml credentials."
            )
            raise typer.Exit(1)

        sql = f"""
            SELECT COUNT(*) AS table_count
            FROM "{source.namespace}".information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """
        _columns, rows = repl.execute_oneshot(sql, limit=1)
        table_count = rows[0][0] if rows else 0
    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"Source test failed: {exc}", err=True)
        raise typer.Exit(1) from exc

    typer.echo(
        f"Source '{source.name}' attached as '{source.namespace}' "
        f"({table_count} table(s) visible, read-only)."
    )


@app.command("connect")
def connect_source(
    name: str = typer.Argument(..., help="Source registry name"),
    connector: str = typer.Option(
        ..., "--connector", help="Connector type, e.g. postgresql"
    ),
    namespace: str
    | None = typer.Option(
        None,
        "--namespace",
        help="Queryable namespace/catalog name exposed to Ask",
    ),
    dsn_env: str
    | None = typer.Option(
        None,
        "--dsn-env",
        help="Environment variable containing the connection DSN",
    ),
    access: str = typer.Option("read_only", "--access", help="Access policy"),
    role: str = typer.Option(
        "business_source_of_truth",
        "--role",
        help="Behavioral role for agent steering",
    ),
    mode: str = typer.Option("auto", "--mode", help="Default Ask mode"),
    description: str
    | None = typer.Option(None, "--description", help="Source description"),
    project: Path
    | None = typer.Option(None, "--project", help="Seeknal project path"),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing source entry with the same name",
    ),
):
    """Create/update a read-only Ask source registry entry.

    This command writes only `seeknal_agent.yml`; it never stores the DSN
    secret. Put the actual secret in `.env` or the process environment and
    reference it with `--dsn-env`.
    """
    project_path = _project_path(project)
    config_path = project_path / "seeknal_agent.yml"
    config = _load_agent_yaml(config_path)
    sources = config.setdefault("sources", {})
    if not isinstance(sources, dict):
        typer.echo("Invalid seeknal_agent.yml: sources must be a mapping", err=True)
        raise typer.Exit(1)
    if name in sources and not force:
        typer.echo(
            f"Source '{name}' already exists. Re-run with --force to replace it.",
            err=True,
        )
        raise typer.Exit(1)

    config["mode"] = {"default": mode}
    sources[name] = {
        "source_kind": "connected",
        "source_type": "database",
        "connector": connector,
        "namespace": namespace or name,
        "access": access,
        "role": role,
        "priority": 100,
        "context_sync": {
            "enabled": True,
            "refresh_policy": "manual",
            "stale_after_hours": 24,
            "templates": ["overview", "columns", "relationships", "profiling"],
        },
    }
    if dsn_env:
        sources[name]["dsn_env"] = dsn_env
    if description:
        sources[name]["description"] = description

    try:
        from seeknal.sources.config import SourceRegistry

        SourceRegistry.from_agent_config(config, project_path=project_path)
    except SourceConfigError as exc:
        typer.echo(f"Invalid source config: {exc}", err=True)
        raise typer.Exit(1) from exc

    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    typer.echo(
        f"Configured source '{name}' as namespace '{sources[name]['namespace']}' "
        f"in {config_path}."
    )


@app.command("remove")
def remove_source(name: str = typer.Argument(..., help="Name of source to remove")):
    """Remove a saved REPL data source."""
    if not ReplSourceRequest.select_by_name(name):
        typer.echo(f"Source '{name}' not found.")
        raise typer.Exit(1)

    ReplSourceRequest.delete_by_name(name)
    typer.echo(f"Removed source '{name}'")


def _load_agent_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise typer.BadParameter(f"Invalid YAML in {path}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise typer.BadParameter(f"{path} must contain a YAML mapping")
    return data

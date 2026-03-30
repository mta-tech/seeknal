"""Gateway CLI — seeknal gateway start / setup / temporal-worker.

Starts the seeknal ask gateway HTTP server for multi-channel access
(Telegram, SSE, etc.) to the agent, or starts a Temporal activity worker.
The ``setup`` command provides an interactive wizard to generate
``seeknal_agent.yml``.
"""

import asyncio
import json
import os
import random
import time
from pathlib import Path
from typing import Any

import typer

def _resolve_project_path(project_path: str | None) -> Path:
    """Resolve and validate the project path."""
    from seeknal.utils.path_security import is_insecure_path

    pp = Path(project_path).resolve() if project_path else Path.cwd()
    if is_insecure_path(str(pp)):
        typer.echo(typer.style(f"Insecure project path: {pp}", fg=typer.colors.RED))
        raise typer.Exit(1)
    if not pp.is_dir():
        typer.echo(typer.style(f"Project path not found: {pp}", fg=typer.colors.RED))
        raise typer.Exit(1)
    return pp


gateway_app = typer.Typer(
    name="gateway",
    help="Gateway server for seeknal ask agent.",
    no_args_is_help=True,
)


@gateway_app.command("start")
def start(
    host: str = typer.Option(None, help="Host to bind to"),
    port: int = typer.Option(None, help="Port to bind to"),
    channel: list[str] = typer.Option(
        None, "--channel", help="Enable specific channel(s) (e.g., telegram)"
    ),
    project_path: str = typer.Option(
        None, "--project", help="Path to seeknal project"
    ),
):
    """Start the seeknal ask gateway server."""
    from seeknal.ask.config import get_gateway_config, load_agent_config
    from seeknal.ask.gateway.server import run_gateway

    pp = _resolve_project_path(project_path)
    agent_config = load_agent_config(pp)
    gw_config = get_gateway_config(agent_config)

    # CLI flags override config
    final_host = host or gw_config.get("host", "127.0.0.1")
    final_port = port or gw_config.get("port", 18789)

    typer.echo(f"Starting seeknal gateway on {final_host}:{final_port}")
    typer.echo(f"Project: {pp}")
    if channel:
        typer.echo(f"Channels: {', '.join(channel)}")

    asyncio.run(
        run_gateway(
            project_path=pp,
            host=final_host,
            port=final_port,
            config=agent_config,
            channels_filter=channel or None,
        )
    )


@gateway_app.command("temporal-worker")
def temporal_worker(
    server: str = typer.Option(
        None, "--server", help="Temporal server address (host:port)"
    ),
    namespace: str = typer.Option(
        None, "--namespace", help="Temporal namespace"
    ),
    task_queue: str = typer.Option(
        None, "--task-queue", help="Temporal task queue name"
    ),
    project_path: str = typer.Option(
        None, "--project", help="Path to seeknal project"
    ),
):
    """Start a Temporal activity worker for seeknal ask."""
    from seeknal.ask.config import load_agent_config

    pp = _resolve_project_path(project_path)
    agent_config = load_agent_config(pp)

    # Extract temporal config section, CLI flags override
    temporal_config = agent_config.get("gateway", {})
    if isinstance(temporal_config, dict):
        temporal_config = temporal_config.get("temporal", {})
    if not isinstance(temporal_config, dict):
        temporal_config = {}

    final_server = server or temporal_config.get("server", "localhost:7233")
    final_namespace = namespace or temporal_config.get("namespace", "seeknal")
    final_task_queue = task_queue or temporal_config.get("task_queue", "seeknal-ask")

    typer.echo(f"Starting Temporal worker: server={final_server}")
    typer.echo(f"  namespace={final_namespace} queue={final_task_queue}")
    typer.echo(f"  project={pp}")

    from seeknal.ask.gateway.temporal.worker import run_temporal_worker

    asyncio.run(
        run_temporal_worker(
            project_path=pp,
            server=final_server,
            namespace=final_namespace,
            task_queue=final_task_queue,
        )
    )


# ---------------------------------------------------------------------------
# Echo helpers (local to avoid circular imports from main.py)
# ---------------------------------------------------------------------------


def _echo_success(message: str) -> None:
    typer.echo(typer.style(f"  {message}", fg=typer.colors.GREEN))


def _echo_error(message: str) -> None:
    typer.echo(typer.style(f"  {message}", fg=typer.colors.RED))


def _echo_info(message: str) -> None:
    typer.echo(typer.style(f"  {message}", fg=typer.colors.BLUE))


# ---------------------------------------------------------------------------
# Telegram token validation
# ---------------------------------------------------------------------------


def _validate_telegram_token(token: str) -> str | None:
    """Validate a Telegram bot token by calling the getMe API.

    Returns the bot username on success, or None on failure.
    """
    import urllib.request
    import urllib.error

    url = f"https://api.telegram.org/bot{token}/getMe"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("ok") and data.get("result", {}).get("username"):
                return data["result"]["username"]
    except urllib.error.HTTPError:
        pass
    except urllib.error.URLError:
        pass
    except (json.JSONDecodeError, KeyError, OSError):
        pass
    return None


# ---------------------------------------------------------------------------
# Telegram pairing — capture chat_id by having user send a code to the bot
# ---------------------------------------------------------------------------


def _generate_pairing_code() -> str:
    """Generate a random 4-digit pairing code."""
    return f"SEEKNAL-{random.randint(1000, 9999)}"


def _wait_for_pairing(token: str, code: str, bot_username: str, timeout: int = 120) -> dict | None:
    """Poll Telegram getUpdates for a message containing the pairing code.

    Returns dict with chat_id, user_id, first_name on success, or None on timeout.
    Uses raw HTTP to avoid importing python-telegram-bot.
    """
    import urllib.request
    import urllib.error

    base_url = f"https://api.telegram.org/bot{token}"

    # Flush old updates first
    try:
        with urllib.request.urlopen(f"{base_url}/getUpdates?offset=-1", timeout=5) as resp:
            data = json.loads(resp.read().decode())
            updates = data.get("result", [])
            last_update_id = updates[-1]["update_id"] + 1 if updates else 0
    except Exception:
        last_update_id = 0

    start_time = time.monotonic()
    while time.monotonic() - start_time < timeout:
        try:
            url = f"{base_url}/getUpdates?offset={last_update_id}&timeout=10"
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read().decode())

            for update in data.get("result", []):
                last_update_id = update["update_id"] + 1
                msg = update.get("message", {})
                text = msg.get("text", "")
                if code in text:
                    chat = msg.get("chat", {})
                    user = msg.get("from", {})
                    return {
                        "chat_id": str(chat.get("id", "")),
                        "chat_title": chat.get("title") or chat.get("first_name", ""),
                        "user_id": str(user.get("id", "")),
                        "first_name": user.get("first_name", ""),
                    }
        except Exception:
            time.sleep(2)

    return None


# ---------------------------------------------------------------------------
# Section collectors
# ---------------------------------------------------------------------------


def _collect_agent_config() -> dict[str, Any]:
    """Collect agent model configuration."""
    typer.echo()
    typer.echo(typer.style("Agent Model Configuration", bold=True))
    typer.echo("  Configure the LLM model for the ask agent.")
    typer.echo()

    model = typer.prompt("  Model name", default="gemini-2.5-flash")
    temperature = typer.prompt("  Temperature (0.0-1.0)", default="0.0")

    try:
        temp_val = float(temperature)
    except ValueError:
        temp_val = 0.0

    profile = typer.prompt(
        "  Default profile (analysis/build/full)", default="analysis"
    )

    return {"model": model, "temperature": temp_val, "default_profile": profile}


def _collect_telegram_config() -> tuple[dict[str, Any], dict | None] | None:
    """Collect Telegram channel configuration with live token validation and pairing.

    Returns (telegram_config, pairing_info) on success, or None if cancelled.
    pairing_info contains chat_id/user_id from the pairing step, or None if skipped.
    """
    typer.echo()
    typer.echo(typer.style("Telegram Channel Configuration", bold=True))
    typer.echo("  Connect a Telegram bot to the gateway.")
    typer.echo("  Create a bot via @BotFather on Telegram first.")
    typer.echo()

    token = ""
    username = ""
    for attempt in range(3):
        token = typer.prompt("  Bot token")
        if not token.strip():
            _echo_error("Token cannot be empty.")
            continue

        typer.echo("  Validating token...")
        username = _validate_telegram_token(token.strip()) or ""
        if username:
            _echo_success(f"Bot validated: @{username}")
            break
        else:
            _echo_error("Invalid token or Telegram API unreachable.")
            if attempt < 2:
                if not typer.confirm("  Try again?", default=True):
                    if typer.confirm("  Skip validation and use this token anyway?", default=False):
                        break
                    return None
            else:
                if not typer.confirm("  Skip validation and use this token anyway?", default=False):
                    return None

    token = token.strip()

    # --- Pairing step ---
    pairing_info = None
    if username and typer.confirm("  Pair with a Telegram chat now?", default=True):
        code = _generate_pairing_code()
        bot_mention = f"@{username}" if username else "your bot"
        typer.echo()
        typer.echo(typer.style("  Pairing Mode", bold=True))
        typer.echo(f"  Send this code to {bot_mention} on Telegram:")
        typer.echo()
        typer.echo(typer.style(f"    {code}", bold=True, fg=typer.colors.YELLOW))
        typer.echo()
        typer.echo("  Waiting for your message (up to 2 minutes)...")

        pairing_info = _wait_for_pairing(token, code, username)
        if pairing_info:
            _echo_success(
                f"Paired! chat_id={pairing_info['chat_id']} "
                f"({pairing_info['chat_title'] or pairing_info['first_name']})"
            )
        else:
            _echo_error("Pairing timed out. You can set chat_id manually later.")

    polling = typer.confirm(
        "  Use polling mode for local development?", default=True
    )

    typer.echo()
    _echo_info("Add this to your shell profile:")
    typer.echo(f"    export TELEGRAM_BOT_TOKEN=\"{token}\"")

    tg_config: dict[str, Any] = {"token": "${TELEGRAM_BOT_TOKEN}", "polling": polling}
    return tg_config, pairing_info


def _collect_temporal_config() -> dict[str, Any]:
    """Collect Temporal worker configuration."""
    typer.echo()
    typer.echo(typer.style("Temporal Worker Configuration", bold=True))
    typer.echo("  Configure connection to a Temporal server.")
    typer.echo()

    server = typer.prompt("  Temporal server address", default="localhost:7233")
    namespace = typer.prompt("  Namespace", default="seeknal")
    task_queue = typer.prompt("  Task queue", default="seeknal-ask")

    return {"server": server, "namespace": namespace, "task_queue": task_queue}


def _collect_heartbeat_config(paired_chat_id: str | None = None) -> dict[str, Any]:
    """Collect heartbeat monitoring configuration.

    Args:
        paired_chat_id: Chat ID from Telegram pairing step, used as default.
    """
    typer.echo()
    typer.echo(typer.style("Heartbeat Monitoring Configuration", bold=True))
    typer.echo("  Schedule periodic health checks via the agent.")
    typer.echo()

    every = typer.prompt("  Check interval (e.g., 30m, 1h, 2h)", default="1h")
    channel = typer.prompt("  Delivery channel", default="telegram")

    if paired_chat_id:
        chat_id = typer.prompt("  Target chat ID", default=paired_chat_id)
    else:
        chat_id = typer.prompt("  Target chat ID (Telegram group/user ID)")

    config: dict[str, Any] = {
        "every": every,
        "target": {"channel": channel, "chat_id": chat_id},
    }

    if typer.confirm("  Configure active hours?", default=False):
        start = typer.prompt("    Start time (HH:MM)", default="08:00")
        end = typer.prompt("    End time (HH:MM)", default="22:00")
        tz = typer.prompt("    Timezone", default="UTC")
        config["active_hours"] = {"start": start, "end": end, "timezone": tz}

    return config


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge override into base. Override values win."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


# ---------------------------------------------------------------------------
# Setup command
# ---------------------------------------------------------------------------


@gateway_app.command("setup")
def setup(
    project_path: str = typer.Option(
        None, "--project", help="Path to seeknal project"
    ),
):
    """Interactive wizard to generate seeknal_agent.yml."""
    pp = _resolve_project_path(project_path)

    config_path = pp / "seeknal_agent.yml"

    typer.echo()
    typer.echo(typer.style("Seeknal Gateway Setup", bold=True, fg=typer.colors.CYAN))
    typer.echo("  This wizard will generate seeknal_agent.yml for your project.")
    typer.echo()

    config: dict[str, Any] = {}
    sections_configured: list[str] = []
    paired_chat_id: str | None = None

    # --- Section 1: Agent model ---
    if typer.confirm("Configure agent model?", default=True):
        config.update(_collect_agent_config())
        sections_configured.append("Agent model")

    # --- Section 2: Telegram ---
    if typer.confirm("Configure Telegram channel?", default=True):
        result = _collect_telegram_config()
        if result is not None:
            tg_config, pairing_info = result
            config.setdefault("gateway", {})["telegram"] = tg_config
            sections_configured.append("Telegram channel")
            if pairing_info:
                paired_chat_id = pairing_info.get("chat_id")

    # --- Section 3: Temporal ---
    if typer.confirm("Configure Temporal worker?", default=False):
        config.setdefault("gateway", {})["temporal"] = _collect_temporal_config()
        sections_configured.append("Temporal worker")

    # --- Section 4: Heartbeat ---
    if typer.confirm("Configure heartbeat monitoring?", default=False):
        config["heartbeat"] = _collect_heartbeat_config(paired_chat_id=paired_chat_id)
        sections_configured.append("Heartbeat monitoring")

    if not config:
        typer.echo()
        _echo_info("No sections configured. Nothing to write.")
        return

    # --- Handle existing config ---
    if config_path.exists():
        if typer.confirm(
            "seeknal_agent.yml already exists. Merge new settings?", default=True
        ):
            import yaml

            existing = yaml.safe_load(config_path.read_text()) or {}
            if isinstance(existing, dict):
                config = _deep_merge(existing, config)
        # else: overwrite with new config only

    # --- Write YAML ---
    import yaml

    tmp_path = config_path.with_suffix(".tmp")
    tmp_path.write_text(yaml.safe_dump(config, default_flow_style=False, sort_keys=False))
    os.replace(str(tmp_path), str(config_path))

    # --- Summary ---
    typer.echo()
    typer.echo(typer.style("Setup Complete!", bold=True, fg=typer.colors.GREEN))
    typer.echo()
    typer.echo("  Configured sections:")
    for section in sections_configured:
        _echo_success(section)
    typer.echo()
    typer.echo(f"  Config written to: {config_path}")
    typer.echo()
    typer.echo("  To start the gateway:")
    typer.echo(f"    seeknal gateway start --project {pp}")
    typer.echo()

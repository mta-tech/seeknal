"""Gateway CLI — seeknal gateway start / temporal-worker.

Starts the seeknal ask gateway HTTP server for multi-channel access
(Telegram, SSE, etc.) to the agent, or starts a Temporal activity worker.
"""

import asyncio

import typer

gateway_app = typer.Typer(
    name="gateway",
    help="Gateway server for seeknal ask agent.",
    no_args_is_help=True,
)


@gateway_app.command("start")
def start(
    host: str = typer.Option(None, help="Host to bind to"),
    port: int = typer.Option(None, help="Port to bind to"),
    project_path: str = typer.Option(
        None, "--project-path", help="Path to seeknal project"
    ),
):
    """Start the seeknal ask gateway server."""
    from pathlib import Path

    from seeknal.ask.config import get_gateway_config, load_agent_config
    from seeknal.ask.gateway.server import run_gateway

    pp = Path(project_path) if project_path else Path.cwd()
    agent_config = load_agent_config(pp)
    gw_config = get_gateway_config(agent_config)

    # CLI flags override config
    final_host = host or gw_config.get("host", "127.0.0.1")
    final_port = port or gw_config.get("port", 18789)

    typer.echo(f"Starting seeknal gateway on {final_host}:{final_port}")
    typer.echo(f"Project: {pp}")

    asyncio.run(
        run_gateway(
            project_path=pp,
            host=final_host,
            port=final_port,
            config=agent_config,
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
        None, "--project-path", help="Path to seeknal project"
    ),
):
    """Start a Temporal activity worker for seeknal ask."""
    from pathlib import Path

    from seeknal.ask.config import load_agent_config

    pp = Path(project_path) if project_path else Path.cwd()
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

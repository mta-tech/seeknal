"""Temporal worker runner for seeknal ask.

Starts a Temporal worker that processes SeekналAskWorkflow workflows
and run_agent_turn activities. Designed to be launched via the CLI
command ``seeknal gateway temporal-worker``.

All temporalio imports are lazy (optional dependency).
"""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

logger = logging.getLogger(__name__)

_MAX_CONNECT_RETRIES = 5
_CONNECT_RETRY_BASE_SECONDS = 2.0


async def run_temporal_worker(
    project_path: Path,
    server: str = "localhost:7233",
    namespace: str = "seeknal",
    task_queue: str = "seeknal-ask",
) -> None:
    """Start a Temporal worker and run until interrupted.

    Retries the initial connection with exponential backoff if the
    Temporal server is temporarily unavailable.

    Args:
        project_path: Path to the seeknal project root.
        server: Temporal server address (host:port).
        namespace: Temporal namespace.
        task_queue: Temporal task queue name.
    """
    from temporalio.client import Client
    from temporalio.worker import Worker

    from seeknal.ask.gateway.temporal.activities import get_activity_fn
    from seeknal.ask.gateway.temporal.workflows import get_workflow_class

    workflow_class = get_workflow_class()
    activity_fn = get_activity_fn()

    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received, stopping worker...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Connect with retry + exponential backoff
    client = None
    attempt = 0
    while not shutdown_event.is_set():
        try:
            client = await Client.connect(server, namespace=namespace)
            break
        except Exception as exc:
            attempt += 1
            if attempt >= _MAX_CONNECT_RETRIES:
                logger.error(
                    "Failed to connect to Temporal after %d attempts: %s",
                    attempt, exc,
                )
                return
            delay = _CONNECT_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Temporal connect attempt %d failed: %s. Retrying in %.1fs...",
                attempt, exc, delay,
            )
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
                return  # shutdown requested during backoff
            except asyncio.TimeoutError:
                continue

    if client is None:
        logger.info("Shutdown requested before connection established.")
        return

    logger.info(
        "Starting Temporal worker: server=%s namespace=%s queue=%s",
        server,
        namespace,
        task_queue,
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[workflow_class],
        activities=[activity_fn],
    ):
        logger.info("Temporal worker running. Press Ctrl+C to stop.")
        await shutdown_event.wait()

    logger.info("Temporal worker stopped.")

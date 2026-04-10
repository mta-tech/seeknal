"""
Temporal integration for Seeknal Ask gateway durable agent execution.

Provides Temporal workflow and activity definitions for running the
pydantic-ai agent with durable execution, automatic retries, and
heartbeats. The Temporal worker runs in-process alongside the
Starlette gateway (shared asyncio event loop).

Optional dependency: install with `pip install seeknal[temporal]`
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger("seeknal.temporal")


# ---------------------------------------------------------------------------
# Optional Temporal dependency handling
# ---------------------------------------------------------------------------

TEMPORAL_AVAILABLE = False

try:
    from temporalio import workflow, activity
    from temporalio.client import Client
    from temporalio.worker import Worker
    from temporalio.common import RetryPolicy

    TEMPORAL_AVAILABLE = True
except ImportError:
    pass


def _require_temporal() -> None:
    """Raise ImportError with install instructions if Temporal is missing."""
    if not TEMPORAL_AVAILABLE:
        raise ImportError(
            "Temporal not installed. Install with: pip install seeknal[temporal]"
        )


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class AgentWorkflowInput:
    """Input for the agent Temporal workflow.

    All fields are JSON-serializable primitives (str, int, None).
    """

    session_id: str
    question: str
    project_path: str
    provider: str | None = None
    model: str | None = None
    callback_url: str | None = None
    callback_auth_token: str | None = None


@dataclass
class AgentWorkflowOutput:
    """Output from the agent Temporal workflow.

    All fields are JSON-serializable primitives (str, int, None).
    """

    answer: str
    event_count: int
    error: str | None = None


# ---------------------------------------------------------------------------
# Temporal activity
# ---------------------------------------------------------------------------

if TEMPORAL_AVAILABLE:
    import asyncio
    from datetime import timedelta
    from pathlib import Path

    @activity.defn
    async def run_agent_activity(
        input: AgentWorkflowInput,
    ) -> AgentWorkflowOutput:
        """Run the seeknal ask agent as a Temporal activity.

        Iterates ``_run_agent_streaming()`` from the gateway server,
        which handles SSE broadcast internally. When a ``callback_url``
        is provided (on-prem worker mode), events are also POSTed to
        the cloud SSE endpoint for cross-replica fan-out.

        Does NOT call ``sse_broadcaster.publish()`` — that is handled
        inside ``_run_agent_streaming()`` (single SSE publish owner).
        """
        import json as _json

        from seeknal.ask.gateway.server import _run_agent_streaming

        # Set up HTTP client for callback if URL provided (on-prem worker mode)
        http_session = None
        callback_headers: dict[str, str] = {}
        if input.callback_url:
            try:
                import aiohttp
                http_session = aiohttp.ClientSession()
                if input.callback_auth_token:
                    callback_headers["Authorization"] = f"Bearer {input.callback_auth_token}"
            except ImportError:
                logger.warning("aiohttp not installed — callback streaming disabled")

        # Background heartbeat coroutine
        async def _heartbeat_loop():
            while True:
                await asyncio.sleep(10)
                activity.heartbeat("agent running")

        heartbeat_task = asyncio.create_task(_heartbeat_loop())

        answer = ""
        error = None
        event_count = 0

        # Worker override: SEEKNAL_PROJECT_PATH env var takes precedence over
        # input.project_path. This lets a standalone worker use its own local
        # project path instead of the gateway's (for split topologies).
        import os as _os
        effective_project_path = _os.environ.get("SEEKNAL_PROJECT_PATH") or input.project_path

        try:
            async for event in _run_agent_streaming(
                Path(effective_project_path),
                input.session_id,
                input.question,
                provider=input.provider,
                model=input.model,
            ):
                event_count += 1
                event_type = event.get("type")

                # POST event to cloud callback URL (best-effort)
                if http_session and input.callback_url:
                    try:
                        import aiohttp
                        url = f"{input.callback_url}/internal/events/{input.session_id}/publish"
                        await http_session.post(
                            url,
                            data=_json.dumps(event),
                            headers={**callback_headers, "Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        )
                    except Exception:
                        pass  # Best-effort delivery

                if event_type == "answer":
                    answer = event.get("data", "")
                elif event_type == "error":
                    error = event.get("data", "")
                elif event_type == "done":
                    break

        except Exception as exc:
            if not error:
                error = str(exc)
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            if http_session:
                await http_session.close()

        return AgentWorkflowOutput(
            answer=answer,
            event_count=event_count,
            error=error,
        )

    # ---------------------------------------------------------------------------
    # Temporal workflow
    # ---------------------------------------------------------------------------

    @workflow.defn
    class AgentWorkflow:
        """Temporal workflow that orchestrates agent execution via an activity.

        The workflow is a thin deterministic orchestrator. All
        side-effect-bearing work (LLM calls, tool execution, SSE
        broadcast) happens inside ``run_agent_activity``.
        """

        def __init__(self) -> None:
            self._status = "pending"

        @workflow.run
        async def run(self, input: AgentWorkflowInput) -> AgentWorkflowOutput:
            self._status = "running"
            try:
                result = await workflow.execute_activity(
                    run_agent_activity,
                    input,
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=2),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=2,
                        non_retryable_error_types=["ValueError"],
                    ),
                )
                if result.error:
                    self._status = "failed"
                else:
                    self._status = "complete"
                return result
            except Exception:
                self._status = "failed"
                raise

        @workflow.query
        def get_status(self) -> str:
            """Return current workflow state: pending/running/complete/failed."""
            return self._status

        @workflow.signal
        async def cancel_run(self) -> None:
            """Signal to request graceful cancellation."""
            self._status = "cancelled"

    # ---------------------------------------------------------------------------
    # Worker factory and client connection helpers
    # ---------------------------------------------------------------------------

    async def connect_temporal_client(
        address: str = "localhost:7233",
        namespace: str = "default",
    ) -> Client | None:
        """Connect to a Temporal server with a 10-second timeout.

        Returns ``None`` on connection failure (degraded mode) instead
        of crashing the gateway.
        """
        import warnings

        try:
            client = await asyncio.wait_for(
                Client.connect(address, namespace=namespace),
                timeout=10.0,
            )
            logger.info("Connected to Temporal at %s (namespace=%s)", address, namespace)
            return client
        except asyncio.TimeoutError:
            warnings.warn(
                f"Temporal connection timed out after 10s ({address}). "
                "Gateway running in degraded mode — /temporal/start will return 503.",
                stacklevel=2,
            )
            return None
        except Exception as exc:
            warnings.warn(
                f"Failed to connect to Temporal ({address}): {exc}. "
                "Gateway running in degraded mode — /temporal/start will return 503.",
                stacklevel=2,
            )
            return None

    def create_temporal_worker(
        client: Client,
        task_queue: str = "seeknal-ask",
        max_concurrent_activities: int | None = None,
    ) -> Worker:
        """Create a Temporal worker configured for agent execution.

        Args:
            client: Connected Temporal client.
            task_queue: Temporal task queue name.
            max_concurrent_activities: Max parallel agent executions.
                Falls back to ``TEMPORAL_MAX_CONCURRENT_ACTIVITIES`` env var,
                then default of 15.

        Returns:
            Configured ``Worker`` instance. Call ``worker.run()`` to
            start polling.
        """
        import os

        if max_concurrent_activities is None:
            max_concurrent_activities = int(
                os.environ.get("TEMPORAL_MAX_CONCURRENT_ACTIVITIES", "15")
            )

        return Worker(
            client,
            task_queue=task_queue,
            workflows=[AgentWorkflow],
            activities=[run_agent_activity],
            max_concurrent_activities=max_concurrent_activities,
            graceful_shutdown_timeout=timedelta(minutes=5),
        )

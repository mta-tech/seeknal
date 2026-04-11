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
    # External push — when set, the activity POSTs events to this URL
    # instead of (or in addition to) the local SSE broadcaster.
    push_url: str | None = None
    api_key: str | None = None


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
        which handles SSE broadcast internally. When ``push_url`` is
        set on the input, events are also POSTed to that external URL
        so consumers like kc-service can receive them via their own
        SSE endpoint.
        """
        from seeknal.ask.gateway.server import _run_agent_streaming

        # Set up external push if configured
        push_client = None
        push_headers: dict[str, str] = {}
        if input.push_url:
            import httpx

            push_url = input.push_url
            if not push_url.startswith("http"):
                import os
                base = os.getenv("SEEKNAL_KC_SERVICE_URL", "http://localhost:8000")
                push_url = f"{base}{push_url}"
            push_client = httpx.AsyncClient(timeout=10)
            push_headers["Content-Type"] = "application/json"
            if input.api_key:
                push_headers["X-API-Key"] = input.api_key

        async def _push_event(event_type: str, **payload: object) -> None:
            """POST a single event to the external push URL."""
            if push_client is None:
                return
            try:
                await push_client.post(
                    push_url,  # type: ignore[possibly-undefined]
                    json={
                        "event": event_type,
                        "message_id": input.session_id,
                        "conversation_id": input.session_id,
                        "payload": payload,
                    },
                    headers=push_headers,
                )
            except Exception:
                activity.logger.warning("Failed to push event to %s", push_url)

        # Background heartbeat coroutine
        async def _heartbeat_loop():
            while True:
                await asyncio.sleep(10)
                activity.heartbeat("agent running")

        heartbeat_task = asyncio.create_task(_heartbeat_loop())

        answer = ""
        error = None
        event_count = 0

        try:
            await _push_event("start")
            event_count += 1

            from seeknal.ask.agents.agent import create_agent
            from seeknal.ask.sessions import SessionStore
            from pathlib import Path as _Path
            from pydantic_ai.usage import UsageLimits
            from seeknal.ask.config import _active_request_limit

            # Load conversation history for this session
            store = SessionStore(_Path(input.project_path))
            if store.get(input.session_id) is None:
                store.create(name=input.session_id)
            _history = store.load_messages(input.session_id)

            _agent, _deps, _, _ = create_agent(
                _Path(input.project_path),
                provider=input.provider,
                model=input.model,
                environment="gateway",
            )
            # Run the agent directly on the Temporal main event loop.
            # DuckDB tool calls (execute_sql, list_tables, describe_table) are
            # individually offloaded to a thread pool via asyncio.to_thread()
            # inside each tool, so they do not block the heartbeat or other awaits.
            result = await _agent.run(
                input.question,
                deps=_deps,
                message_history=_history,
                usage_limits=UsageLimits(request_limit=_active_request_limit),
            )
            # Persist conversation history for future turns
            store.save_messages(input.session_id, result.all_messages())
            answer = result.output or ""
            event_count += 1

            if answer:
                await _push_event("message", answer=answer)
                event_count += 1

            await _push_event("done")
            event_count += 1

        except Exception as exc:
            import traceback
            tb = traceback.format_exc()
            activity.logger.error(
                "run_agent_activity failed: %s\n%s", exc, tb
            )
            if not error:
                error = str(exc)
            await _push_event("error", error_message=str(exc))
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            if push_client is not None:
                await push_client.aclose()

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
        tls: bool = False,
    ) -> Client | None:
        """Connect to a Temporal server with a 10-second timeout.

        Returns ``None`` on connection failure (degraded mode) instead
        of crashing the gateway.
        """
        import warnings

        try:
            client = await asyncio.wait_for(
                Client.connect(address, namespace=namespace, tls=tls),
                timeout=10.0,
            )
            logger.info(
                "Connected to Temporal at %s (namespace=%s, tls=%s)",
                address, namespace, tls,
            )
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
    ) -> Worker:
        """Create a Temporal worker configured for agent execution.

        Args:
            client: Connected Temporal client.
            task_queue: Temporal task queue name.

        Returns:
            Configured ``Worker`` instance. Call ``worker.run()`` to
            start polling.
        """
        import os
        from concurrent.futures import ThreadPoolExecutor
        
        _max_concurrent = int(os.environ.get("MAX_CONCURRENT_ACTIVITIES", "5"))
        return Worker(
            client,
            task_queue=task_queue,
            workflows=[AgentWorkflow],
            activities=[run_agent_activity],
            max_concurrent_activities=_max_concurrent,
            activity_executor=ThreadPoolExecutor(max_workers=_max_concurrent),
            graceful_shutdown_timeout=timedelta(minutes=5),
        )

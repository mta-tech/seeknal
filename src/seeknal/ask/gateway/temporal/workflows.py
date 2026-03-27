"""Temporal workflow definition for seeknal ask agent sessions.

Defines SeekналAskWorkflow which manages a long-running agent session
as a Temporal workflow. The workflow executes agent turns as activities,
accepts follow-up messages via signals, and reports status via queries.

All temporalio imports are lazy since it is an optional dependency.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data transfer objects
# ---------------------------------------------------------------------------


@dataclass
class AgentTurnParams:
    """Parameters for a single agent turn activity."""

    question: str
    project_path: str
    session_name: str
    config_overrides: dict = field(default_factory=dict)


@dataclass
class AgentTurnResult:
    """Result from a single agent turn activity."""

    response_text: str
    tool_calls_count: int
    success: bool


# ---------------------------------------------------------------------------
# Workflow definition
# ---------------------------------------------------------------------------

# Task queue name shared between workflow starter and worker
TASK_QUEUE = "seeknal-ask"


def _get_workflow_class():
    """Lazily define and return the workflow class.

    This function contains all temporalio imports so they are only
    resolved when actually needed (optional dependency).
    """
    from datetime import timedelta

    with __import__("temporalio").workflow.unsafe.imports_passed_through():
        from seeknal.ask.gateway.temporal.activities import run_agent_turn

    from temporalio import workflow

    @workflow.defn(name="SeekналAskWorkflow")
    class SeekналAskWorkflow:
        """Temporal workflow for a seeknal ask agent session.

        Executes an initial agent turn, then loops waiting for follow-up
        messages via signals. A shutdown signal terminates the loop.
        """

        def __init__(self) -> None:
            self._pending_messages: list[str] = []
            self._shutdown = False
            self._status = "initialized"

        @workflow.run
        async def run(
            self,
            session_id: str,
            project_path: str,
            initial_question: str,
        ) -> str:
            """Execute the workflow.

            Args:
                session_id: Unique session identifier.
                project_path: Path to the seeknal project root.
                initial_question: The first user question.

            Returns:
                The last agent response text.
            """
            self._status = "running"
            last_response = ""

            # Execute initial question
            result = await workflow.execute_activity(
                run_agent_turn,
                AgentTurnParams(
                    question=initial_question,
                    project_path=project_path,
                    session_name=session_id,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(seconds=30),
            )
            last_response = result.response_text
            self._status = "waiting"

            # Loop: wait for follow-up messages or shutdown
            while not self._shutdown:
                await workflow.wait_condition(
                    lambda: len(self._pending_messages) > 0 or self._shutdown,
                )

                if self._shutdown:
                    break

                # Process all queued messages
                while self._pending_messages:
                    msg = self._pending_messages.pop(0)
                    self._status = "running"
                    result = await workflow.execute_activity(
                        run_agent_turn,
                        AgentTurnParams(
                            question=msg,
                            project_path=project_path,
                            session_name=session_id,
                        ),
                        start_to_close_timeout=timedelta(minutes=5),
                        heartbeat_timeout=timedelta(seconds=30),
                    )
                    last_response = result.response_text
                    self._status = "waiting"

            self._status = "completed"
            return last_response

        @workflow.signal
        def user_message(self, text: str) -> None:
            """Queue a follow-up message for the agent."""
            self._pending_messages.append(text)

        @workflow.signal
        def shutdown(self) -> None:
            """Signal the workflow to shut down after current turn."""
            self._shutdown = True

        @workflow.query
        def get_status(self) -> str:
            """Return the current workflow status."""
            return self._status

    return SeekналAskWorkflow


# Public accessor — callers use this to get the class without
# importing temporalio at module level.
def get_workflow_class():
    """Return the SeekналAskWorkflow class (lazily defined)."""
    return _get_workflow_class()

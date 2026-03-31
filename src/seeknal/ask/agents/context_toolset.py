"""Dynamic project context injection via FunctionToolset.

Provides a no-tool toolset that injects seeknal project context (entities,
DAG, intermediates, pipelines) into the agent's system prompt on every turn
via ``get_instructions()``.

Pattern: same as pydantic-deep's ContextToolset — a FunctionToolset subclass
with no tools, only ``get_instructions()``.
"""

from __future__ import annotations

from typing import Any

from pydantic_ai import RunContext
from pydantic_ai.toolsets import FunctionToolset

from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery


class SeeknaContextToolset(FunctionToolset[Any]):
    """Toolset that injects seeknal project context into agent system prompt.

    Has no tools — only provides instructions via ``get_instructions()``.
    Calls ``ArtifactDiscovery.get_context_for_prompt()`` to build the context
    string containing entities, DAG, intermediates, and pipelines.
    """

    def __init__(self, discovery: ArtifactDiscovery) -> None:
        """Initialize the context toolset.

        Args:
            discovery: ArtifactDiscovery instance for the current project.
        """
        super().__init__(id="seeknal-context")
        self._discovery = discovery

    def get_instructions(self, ctx: RunContext[Any]) -> str | None:
        """Build and return project context for system prompt injection.

        Args:
            ctx: The run context (unused — context comes from ArtifactDiscovery).

        Returns:
            Formatted project context string, or None if no context available.
        """
        context = self._discovery.get_context_for_prompt()
        if not context or not context.strip():
            return None
        return f"## Data Context\n\n{context}"

"""Subagent configurations for seeknal ask.

Defines context-isolated subagents that the main agent can delegate to
via the `task()` tool. The main agent decides autonomously when to
delegate — no keyword routing.
"""

from pydantic_ai.toolsets import FunctionToolset

from pydantic_deep.types import SubAgentConfig


def create_pipeline_toolset() -> FunctionToolset:
    """Create a restricted toolset with only pipeline/file reading tools.

    This toolset is used by the lineage investigator subagent — it cannot
    execute SQL or Python, only read pipeline definitions and project files.
    """
    from seeknal.ask.agents.tools.read_pipeline import read_pipeline
    from seeknal.ask.agents.tools.read_project_file import read_project_file
    from seeknal.ask.agents.tools.search_pipelines import search_pipelines
    from seeknal.ask.agents.tools.search_project_files import search_project_files

    return FunctionToolset(
        tools=[search_pipelines, read_pipeline, search_project_files, read_project_file],
        id="seeknal-pipeline",
    )


def get_subagent_configs() -> list[SubAgentConfig]:
    """Return subagent configurations for the seeknal ask agent.

    Currently includes:
    - lineage_investigator: traces data flow through pipeline definitions
    """
    return [
        SubAgentConfig(
            name="lineage_investigator",
            description=(
                "Investigates data lineage by reading pipeline definitions and "
                "project files. Delegate when you need to understand how a table "
                "is produced or trace data flow."
            ),
            instructions=(
                "You are a data lineage investigator. Your job is to trace how "
                "data flows through seeknal pipeline definitions. Read YAML and "
                "Python pipeline files to understand transformations. Summarize "
                "your findings concisely (under 500 tokens). Focus on: what "
                "inputs are used, what transformations are applied, and what "
                "output is produced."
            ),
            toolsets=[create_pipeline_toolset()],
        ),
    ]

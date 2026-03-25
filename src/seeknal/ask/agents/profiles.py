"""Tool profiles for Seeknal Ask agent.

Profiles control which tools are available based on detected intent.
This provides structural enforcement — in analysis mode, the agent
literally cannot call write tools like draft_node or run_pipeline.

Profiles:
  analysis: read-only (queries, profiling, reports)
  build:    analysis + write tools (draft, apply, run)
  full:     build + search tools (pipelines, project files)
"""

# Import all tools
from seeknal.ask.agents.tools.apply_draft import apply_draft
from seeknal.ask.agents.tools.describe_table import describe_table
from seeknal.ask.agents.tools.draft_node import draft_node
from seeknal.ask.agents.tools.dry_run_draft import dry_run_draft
from seeknal.ask.agents.tools.edit_node import edit_node
from seeknal.ask.agents.tools.execute_python import execute_python
from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.agents.tools.generate_report import generate_report
from seeknal.ask.agents.tools.get_entities import get_entities
from seeknal.ask.agents.tools.get_entity_schema import get_entity_schema
from seeknal.ask.agents.tools.inspect_output import inspect_output
from seeknal.ask.agents.tools.list_tables import list_tables
from seeknal.ask.agents.tools.plan_pipeline import plan_pipeline
from seeknal.ask.agents.tools.profile_data import profile_data
from seeknal.ask.agents.tools.read_pipeline import read_pipeline
from seeknal.ask.agents.tools.read_project_file import read_project_file
from seeknal.ask.agents.tools.run_pipeline import run_pipeline
from seeknal.ask.agents.tools.save_report_exposure import save_report_exposure
from seeknal.ask.agents.tools.search_pipelines import search_pipelines
from seeknal.ask.agents.tools.search_project_files import search_project_files
from seeknal.ask.agents.tools.show_lineage import show_lineage

# Analysis tools: read-only, no pipeline modifications
_ANALYSIS_TOOLS = [
    profile_data, list_tables, describe_table,
    execute_sql, inspect_output,
    get_entities, get_entity_schema,
    execute_python,
    generate_report, save_report_exposure,
]

# Build tools: analysis + write/execute
_BUILD_TOOLS = _ANALYSIS_TOOLS + [
    draft_node, dry_run_draft, apply_draft,
    edit_node, plan_pipeline, show_lineage,
    run_pipeline,
]

# Full tools: build + search/exploration
_FULL_TOOLS = _BUILD_TOOLS + [
    read_pipeline, search_pipelines,
    search_project_files, read_project_file,
]

TOOL_PROFILES = {
    "analysis": _ANALYSIS_TOOLS,
    "build": _BUILD_TOOLS,
    "full": _FULL_TOOLS,
}

# Keywords used to auto-detect profile from user's question
_BUILD_KEYWORDS = {
    "build", "create", "add", "design", "pipeline", "draft",
    "deploy", "run", "transform", "source", "model", "feature",
}
_REPORT_KEYWORDS = {
    "report", "dashboard", "visualization", "chart", "evidence",
}


def select_profile(question: str) -> str:
    """Auto-detect the best tool profile from the user's question.

    Args:
        question: The user's natural language question.

    Returns:
        Profile name: "analysis", "build", or "full".
    """
    words = set(question.lower().split())
    # Check report keywords first (more specific)
    if words & _REPORT_KEYWORDS:
        return "full"
    if words & _BUILD_KEYWORDS:
        return "build"
    return "analysis"


def get_tools_for_profile(
    profile: str,
    disabled_tools: list[str] | None = None,
) -> list:
    """Get tools for the given profile, minus any disabled tools.

    Args:
        profile: One of "analysis", "build", "full".
        disabled_tools: Optional list of tool names to exclude.

    Returns:
        List of tool objects for the profile.
    """
    tools = TOOL_PROFILES.get(profile, _ANALYSIS_TOOLS)
    if not disabled_tools:
        return list(tools)
    disabled = set(disabled_tools)
    return [t for t in tools if t.name not in disabled]

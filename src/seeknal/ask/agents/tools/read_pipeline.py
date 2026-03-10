"""Read pipeline tool — reads a specific YAML/Python pipeline definition."""

from langchain_core.tools import tool


@tool
def read_pipeline(file_path: str) -> str:
    """Read a seeknal pipeline definition file (YAML or Python).

    Use this to understand HOW data is produced — the SQL transforms,
    source configs, feature group definitions, and aggregation logic.

    Args:
        file_path: Relative path to the pipeline file
                   (e.g., 'seeknal/transforms/clean_orders.yaml').
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    content = ctx.artifact_discovery.get_pipeline_content(file_path)

    if content is None:
        return f"Pipeline file '{file_path}' not found or not accessible."

    return f"```\n# {file_path}\n{content}\n```"

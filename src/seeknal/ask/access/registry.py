"""Per-tool ``min_role`` registry.

Single source of truth for which role is required to invoke each Ask tool.
A contract test enforces that every tool registered in
``seeknal.ask.agents.tools.toolset`` has an entry here — any missing entry
fails the build (Principle 7: fail-closed; AC15).
"""

from __future__ import annotations

from typing import Iterable

from seeknal.ask.access.roles import Role

# Tools the Telegram bot exposes to all listed users (read-only discovery
# + low-risk reads).
_VIEWER_TOOLS = {
    "list_tables",
    "describe_table",
    "query_metric",
    "preview_query",
    "execute_sql",
    "list_ask_tests",
    "read_ask_test",
    "list_ask_test_results",
    "read_ask_test_result",
    "read_pipeline",
    "read_project_file",
    "read_source_context",
    "read_sql_pair",
    "list_sql_pairs",
    "list_context_files",
    "list_source_context",
    "read_tabular",
    "show_lineage",
    "get_entities",
    "get_entity_schema",
    "profile_data",
    "inspect_output",
    "open_in_browser",
    "web_firecrawl",
    "read_proof_document",
    "parse_record",
    "extract_from_image",
    "extract_finance_document",
    "ask_user",
}

# Analysts can additionally save personal preferences, draft pipelines, and
# generate reports — but they cannot run pipelines or write to bronze tables.
_ANALYST_TOOLS = {
    "save_metric",
    "save_preference",
    "save_ingestion_skill",
    "save_report_exposure",
    "draft_node",
    "dry_run_draft",
    "propose_record_table",
    "plan_pipeline",
    "submit_plan",
    "generate_report",
    "publish_to_proof",
    "edit_proof_document",
    "bootstrap_semantic_model",
    "execute_python",
    "execute_uv_script",
    "execute_sql_pair",
    "run_ask_test",
}

# Operators can ingest data into bronze tables, run pipelines, and publish.
_OPERATOR_TOOLS = {
    "write_ingested_table",
    "run_pipeline",
    "check_ingestion_drift",
    "search_pipelines",
    "search_project_files",
    "publish_to_seeknal_report",
}

# Engineers can edit project structure.
_ENGINEER_TOOLS = {
    "apply_draft",
    "edit_node",
    "write_project_file",
    "write_seeknal_project_asset",
}


tool_min_role: dict[str, Role] = {}
for name in _VIEWER_TOOLS:
    tool_min_role[name] = Role.VIEWER
for name in _ANALYST_TOOLS:
    tool_min_role[name] = Role.ANALYST
for name in _OPERATOR_TOOLS:
    tool_min_role[name] = Role.OPERATOR
for name in _ENGINEER_TOOLS:
    tool_min_role[name] = Role.ENGINEER


def register_tool_role(name: str, role: Role, *, overwrite: bool = False) -> None:
    """Add a tool → role mapping. Used by tests and future plugins."""
    if not overwrite and name in tool_min_role:
        raise ValueError(
            f"Tool {name!r} already registered with role "
            f"{tool_min_role[name].label()!r}. Pass overwrite=True to replace."
        )
    tool_min_role[name] = role


def iter_tool_min_roles() -> Iterable[tuple[str, Role]]:
    """Yield ``(tool_name, role)`` pairs (sorted, stable order)."""
    return sorted(tool_min_role.items())


__all__ = [
    "tool_min_role",
    "register_tool_role",
    "iter_tool_min_roles",
]

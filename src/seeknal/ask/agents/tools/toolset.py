"""Seeknal Ask tool registry — collects tools into pydantic-ai toolsets.

The default toolset stays backward-compatible and exposes the full Seeknal
surface.  Read-only connected-source sessions can opt into the narrower
``analysis`` surface, which physically omits build, publish, ingest, and write
tools instead of relying on prompt-only steering.
"""

from pydantic_ai.toolsets import FunctionToolset

from seeknal.ask.agents.tools.apply_draft import apply_draft
from seeknal.ask.agents.tools.ask_user_tool import ask_user
from seeknal.ask.agents.tools.bootstrap_semantic_model import bootstrap_semantic_model
from seeknal.ask.agents.tools.check_ingestion_drift import check_ingestion_drift
from seeknal.ask.agents.tools.describe_table import describe_table
from seeknal.ask.agents.tools.draft_node import draft_node
from seeknal.ask.agents.tools.dry_run_draft import dry_run_draft
from seeknal.ask.agents.tools.edit_node import edit_node
from seeknal.ask.agents.tools.edit_proof_document import edit_proof_document
from seeknal.ask.agents.tools.execute_python import execute_python
from seeknal.ask.agents.tools.execute_sql_pair import execute_sql_pair
from seeknal.ask.agents.tools.execute_uv_script import execute_uv_script
from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.agents.tools.extract_from_image import extract_from_image
from seeknal.ask.agents.tools.generate_report import generate_report
from seeknal.ask.agents.tools.get_entities import get_entities
from seeknal.ask.agents.tools.get_entity_schema import get_entity_schema
from seeknal.ask.agents.tools.inspect_output import inspect_output
from seeknal.ask.agents.tools.list_ask_test_results import list_ask_test_results
from seeknal.ask.agents.tools.list_ask_tests import list_ask_tests
from seeknal.ask.agents.tools.list_context_files import list_context_files
from seeknal.ask.agents.tools.list_source_context import list_source_context
from seeknal.ask.agents.tools.list_sql_pairs import list_sql_pairs
from seeknal.ask.agents.tools.list_tables import list_tables
from seeknal.ask.agents.tools.open_in_browser import open_in_browser
from seeknal.ask.agents.tools.parse_record import parse_record
from seeknal.ask.agents.tools.plan_pipeline import plan_pipeline
from seeknal.ask.agents.tools.preview_query import preview_query
from seeknal.ask.agents.tools.profile_data import profile_data
from seeknal.ask.agents.tools.propose_record_table import propose_record_table
from seeknal.ask.agents.tools.publish_to_proof import publish_to_proof
from seeknal.ask.agents.tools.publish_to_seeknal_report import publish_to_seeknal_report
from seeknal.ask.agents.tools.query_metric import query_metric
from seeknal.ask.agents.tools.read_pipeline import read_pipeline
from seeknal.ask.agents.tools.read_ask_test import read_ask_test
from seeknal.ask.agents.tools.read_ask_test_result import read_ask_test_result
from seeknal.ask.agents.tools.read_tabular import read_tabular
from seeknal.ask.agents.tools.read_proof_document import read_proof_document
from seeknal.ask.agents.tools.read_project_file import read_project_file
from seeknal.ask.agents.tools.read_source_context import read_source_context
from seeknal.ask.agents.tools.read_sql_pair import read_sql_pair
from seeknal.ask.agents.tools.run_pipeline import run_pipeline
from seeknal.ask.agents.tools.run_ask_test import run_ask_test
from seeknal.ask.agents.tools.save_ingestion_skill import save_ingestion_skill
from seeknal.ask.agents.tools.save_metric import save_metric
from seeknal.ask.agents.tools.save_preference import save_preference
from seeknal.ask.agents.tools.save_report_exposure import save_report_exposure
from seeknal.ask.agents.tools.search_pipelines import search_pipelines
from seeknal.ask.agents.tools.search_project_files import search_project_files
from seeknal.ask.agents.tools.show_lineage import show_lineage
from seeknal.ask.agents.tools.submit_plan import submit_plan
from seeknal.ask.agents.tools.write_ingested_table import write_ingested_table
from seeknal.ask.agents.tools.write_project_file import write_project_file


_DATABASE_ANALYSIS_TOOLS = [
    execute_sql,
    preview_query,
    list_tables,
    describe_table,
]

_PROJECT_READ_TOOLS = [
    read_pipeline,
    search_pipelines,
    search_project_files,
]

_PROJECT_MEMORY_TOOLS = [
    # Safe project-local teaching surface. These tools only touch local project
    # memory (`preferences.yml` and `context/`) and never mutate connected data
    # sources, pipelines, reports, or external systems.
    list_context_files,
    read_project_file,
    write_project_file,
    save_preference,
]

_SEMANTIC_ARTIFACT_TOOLS = [
    get_entities,
    get_entity_schema,
]

_ANALYSIS_TOOLS = [
    # Keep execute_python available for "deep analyst" work (segmentation,
    # modeling, correlations). Its connection is read-guarded separately.
    execute_python,
]

_READ_ONLY_CONTEXT_TOOLS = [
    # NAO-inspired generated source context and reusable SQL examples. These
    # tools are read-only and intentionally safe for connected-source analysis
    # mode; they keep domain knowledge in files/skills instead of harness code.
    list_source_context,
    read_source_context,
    list_sql_pairs,
    read_sql_pair,
    execute_sql_pair,
    list_ask_tests,
    read_ask_test,
    run_ask_test,
    list_ask_test_results,
    read_ask_test_result,
]

_FULL_ONLY_TOOLS = [
    execute_uv_script,
    generate_report,
    open_in_browser,
    save_report_exposure,
    publish_to_proof,
    publish_to_seeknal_report,
    read_proof_document,
    edit_proof_document,
    draft_node,
    dry_run_draft,
    apply_draft,
    edit_node,
    run_pipeline,
    plan_pipeline,
    show_lineage,
    inspect_output,
    profile_data,
    bootstrap_semantic_model,
    query_metric,
    save_metric,
    submit_plan,
    read_tabular,
    write_ingested_table,
    save_ingestion_skill,
    check_ingestion_drift,
    parse_record,
    extract_from_image,
    propose_record_table,
]


def create_ask_toolset(
    *,
    mode: str = "full",
    include_ask_user: bool = True,
) -> FunctionToolset:
    """Create the seeknal-ask toolset.

    Args:
        mode: ``"full"`` keeps the legacy all-tools surface. ``"analysis"``
            exposes only read/discovery/analysis tools for connected-source or
            other read-only analyst sessions.
        include_ask_user: Include the direct interactive ``ask_user`` tool.
            Headless channels pass ``False`` so tool schemas cannot trigger
            blocking user input.
    """
    if mode == "analysis":
        # Keep the connected-source/read-only surface deliberately thin:
        # database discovery/query tools, read-only context lookup, and Python
        # analysis. Pipeline/build/external-write/publish tools remain in full mode.
        tools = [
            *_DATABASE_ANALYSIS_TOOLS,
            *_READ_ONLY_CONTEXT_TOOLS,
            *_PROJECT_MEMORY_TOOLS,
            *_ANALYSIS_TOOLS,
        ]
        toolset_id = "seeknal-ask-analysis"
    elif mode == "full":
        tools = [
            *_DATABASE_ANALYSIS_TOOLS,
            *_PROJECT_READ_TOOLS,
            *_READ_ONLY_CONTEXT_TOOLS,
            *_PROJECT_MEMORY_TOOLS,
            *_SEMANTIC_ARTIFACT_TOOLS,
            *_FULL_ONLY_TOOLS,
            *_ANALYSIS_TOOLS,
        ]
        toolset_id = "seeknal-ask"
    else:
        raise ValueError(f"Unsupported ask toolset mode: {mode!r}")

    if include_ask_user:
        tools.append(ask_user)

    return FunctionToolset(
        tools=tools,
        id=toolset_id,
    )

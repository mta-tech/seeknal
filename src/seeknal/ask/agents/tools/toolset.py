"""Seeknal Ask tool registry — collects tools into pydantic-ai toolsets.

The default toolset stays backward-compatible and exposes the full Seeknal
surface.  Read-only connected-source sessions can opt into the narrower
``analysis`` surface, which physically omits build, publish, ingest, and write
tools instead of relying on prompt-only steering.
"""

from pydantic_ai.toolsets import FunctionToolset

from seeknal.ask.agents.tools.anomaly import detect_anomaly
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
from seeknal.ask.agents.tools.forecast import run_forecast
from seeknal.ask.agents.tools.generate_report import generate_report
from seeknal.ask.agents.tools.get_entities import get_entities
from seeknal.ask.agents.tools.get_entity_schema import get_entity_schema
from seeknal.ask.agents.tools.inspect_output import inspect_output
from seeknal.ask.agents.tools.intel_knowledge import (
    intel_knowledge_list,
    intel_knowledge_read,
    intel_knowledge_search,
)
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
from seeknal.ask.agents.tools.request_clarification import request_clarification
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
from seeknal.ask.agents.tools.upload_to_s3 import upload_to_s3
from seeknal.ask.agents.tools.write_ingested_table import write_ingested_table
from seeknal.ask.agents.tools.write_project_file import write_project_file
from seeknal.ask.agents.tools.write_report import write_report


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

_INTEL_KNOWLEDGE_TOOLS = [
    # Credential-backed laptop capability. create_agent enables this only for
    # the interactive CLI; gateway/telegram must not inherit a local grant.
    intel_knowledge_list,
    intel_knowledge_search,
    intel_knowledge_read,
]

# Tools whose effect leaves the node: they publish to a hosted third-party
# service (Proof Editor at memokami.exe.xyz / proofeditor.ai, or a configured
# Seeknal Report Server) or open a local GUI browser, which is meaningless on
# a headless premises worker and not something a customer's data should ever
# drive. Stripped from the toolset in the gateway environment regardless of
# project mode -- see ``strip_gateway_egress_tools`` on ``create_ask_toolset``.
# ``execute_uv_script`` is deliberately NOT here: it only runs ``uv run`` as a
# local subprocess against project-local parquet files and makes no network
# call, so it stays on the node like ``execute_python``.
_GATEWAY_EGRESS_TOOLS = [
    open_in_browser,
    publish_to_proof,
    publish_to_seeknal_report,
    read_proof_document,
    edit_proof_document,
]

_FULL_ONLY_TOOLS = [
    execute_uv_script,
    generate_report,
    *_GATEWAY_EGRESS_TOOLS,
    save_report_exposure,
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

# Deterministic forecast trigger tool. Gated by ``include_forecast`` --
# registered only in non-interactive environments when
# ``agent.forecast.enabled`` is true.
_FORECAST_TOOLS = [
    run_forecast,
]

# Anomaly-awareness tool. Sibling of run_forecast: same gating pattern, same
# engine. Gated by ``include_anomaly``.
_ANOMALY_TOOLS = [
    detect_anomaly,
]

# CSV export tool. Gated by ``include_upload_to_s3`` -- registered only in
# non-interactive environments when ``agent.upload_to_s3.enabled`` is true.
_EXPORT_TOOLS = [
    upload_to_s3,
]

_ACTION_DELIVERY_TOOLS = [write_report]


def create_ask_toolset(
    *,
    mode: str = "full",
    include_ask_user: bool = True,
    include_request_clarification: bool = False,
    include_forecast: bool = False,
    include_anomaly: bool = False,
    include_upload_to_s3: bool = False,
    include_intel_knowledge: bool = False,
    action_delivery: bool = False,
    strip_gateway_egress_tools: bool = False,
) -> FunctionToolset:
    """Create the seeknal-ask toolset.

    Args:
        mode: ``"full"`` keeps the legacy all-tools surface. ``"analysis"``
            exposes read/discovery/analysis tools for connected-source work.
            ``"intel_work"`` exposes only explicitly enabled Intel knowledge
            tools for prompt-free assigned-work execution.
        include_ask_user: Include the direct interactive ``ask_user`` tool.
            Headless channels pass ``False`` so tool schemas cannot trigger
            blocking user input.
        include_request_clarification: Include the headless ``request_clarification``
            tool (Model B). Registered for gateway/telegram; the interactive CLI
            keeps ``ask_user`` instead, so the two are never combined.
        include_forecast: Include the deterministic ``run_forecast`` trigger
            tool. Registered only in non-interactive environments when
            ``agent.forecast.enabled`` is true in ``seeknal_agent.yml``.
        include_anomaly: Include the ``detect_anomaly`` tool. Registered
            only in non-interactive environments when ``agent.anomaly.enabled``
            is true in ``seeknal_agent.yml``.
        include_upload_to_s3: Include the generic CSV export tool ``upload_to_s3``.
            Registered only in non-interactive environments when
            ``agent.upload_to_s3.enabled`` is true in ``seeknal_agent.yml``.
        include_intel_knowledge: Include credential-backed Intel knowledge
            tools. ``create_agent`` enables these only for the interactive CLI
            so remote channels cannot consume a laptop-local grant.
        action_delivery: Replace the regular blocking ``ask_user`` function
            tool with the typed output action owned by the IBA worker path.
        strip_gateway_egress_tools: Remove ``_GATEWAY_EGRESS_TOOLS`` (hosted
            Proof Editor publish/read/edit, the Seeknal Report Server
            publish, and opening a local browser) from the toolset no matter
            what ``mode`` resolves to. ``create_agent`` sets this for
            ``environment == "gateway"`` — a project without an explicit
            source registry resolves to ``"full"`` mode, and on a premises
            worker that surface must not include tools whose effect is to
            send project data to a third-party host or pop a GUI browser.
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
    elif mode == "intel_work":
        # The caller adds only _INTEL_KNOWLEDGE_TOOLS below. Keep this base
        # empty so assigned work has no prompt, database, project-write,
        # publish, or generic execution surface.
        tools = []
        toolset_id = "seeknal-intel-work"
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

    if include_ask_user and not action_delivery and mode != "intel_work":
        tools.append(ask_user)

    if include_request_clarification and mode != "intel_work":
        tools.append(request_clarification)

    if include_forecast and mode != "intel_work":
        tools.extend(_FORECAST_TOOLS)

    if include_anomaly and mode != "intel_work":
        tools.extend(_ANOMALY_TOOLS)

    if include_upload_to_s3 and mode != "intel_work":
        tools.extend(_EXPORT_TOOLS)

    if include_intel_knowledge:
        tools.extend(_INTEL_KNOWLEDGE_TOOLS)

    if action_delivery and mode == "full":
        tools.extend(_ACTION_DELIVERY_TOOLS)

    if strip_gateway_egress_tools:
        tools = [tool for tool in tools if tool not in _GATEWAY_EGRESS_TOOLS]

    return FunctionToolset(
        tools=tools,
        id=toolset_id,
        # pydantic-ai's FunctionToolset defaults to max_retries=1. The SQL
        # security / self-correction hooks raise ModelRetry on a bad query, so a
        # single retry exhaustion would raise UnexpectedModelBehavior and kill the
        # whole turn. Give the model room to self-correct before failing.
        max_retries=3,
    )

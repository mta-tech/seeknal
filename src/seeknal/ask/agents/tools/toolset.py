"""Seeknal Ask tool registry — collects all tools into a pydantic-ai FunctionToolset."""

from pydantic_ai.toolsets import FunctionToolset

from seeknal.ask.agents.tools.describe_table import describe_table
from seeknal.ask.agents.tools.execute_python import execute_python
from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.agents.tools.generate_report import generate_report
from seeknal.ask.agents.tools.get_entities import get_entities
from seeknal.ask.agents.tools.get_entity_schema import get_entity_schema
from seeknal.ask.agents.tools.list_tables import list_tables
from seeknal.ask.agents.tools.read_pipeline import read_pipeline
from seeknal.ask.agents.tools.read_project_file import read_project_file
from seeknal.ask.agents.tools.save_report_exposure import save_report_exposure
from seeknal.ask.agents.tools.search_pipelines import search_pipelines
from seeknal.ask.agents.tools.search_project_files import search_project_files


def create_ask_toolset() -> FunctionToolset:
    """Create the seeknal-ask toolset with all 12 tools registered."""
    return FunctionToolset(
        tools=[
            execute_sql,
            list_tables,
            describe_table,
            get_entities,
            get_entity_schema,
            read_pipeline,
            search_pipelines,
            search_project_files,
            read_project_file,
            execute_python,
            generate_report,
            save_report_exposure,
        ],
        id="seeknal-ask",
    )

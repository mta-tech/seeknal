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


_LINEAGE_INSTRUCTIONS = """\
You are a data lineage investigator for seeknal data pipelines.

## Your Job
Trace how data flows through seeknal pipeline definitions. Read YAML and
Python pipeline files to understand transformations, then produce a clear
summary of the data lineage.

## Seeknal Pipeline YAML Structure
Pipeline files use this format:
```yaml
kind: source | transform | feature_group
name: <node_name>
description: <what this node does>
inputs:
  - ref: <kind>.<name>      # e.g. source.raw_orders, transform.orders_cleaned
sql: |
  SELECT ... FROM <input_table>
```

Key fields:
- `kind`: node type (source, transform, feature_group)
- `inputs[].ref`: upstream dependencies in `kind.name` format
- `sql`: the DuckDB SQL transformation applied

## Workflow
1. Use `search_pipelines` to find relevant pipeline files by name or content
2. Use `read_pipeline` to read each YAML/Python definition
3. Follow the `inputs[].ref` chain to trace upstream dependencies
4. Summarize the full lineage from source to final output

## Output Format
For each node in the lineage, document:

### <node_name> (<kind>)
- **Inputs**: list upstream refs
- **Transform**: summarize the SQL or logic (what it filters, joins, aggregates)
- **Output**: key columns produced

## Example Output
### orders_cleaned (transform)
- **Inputs**: source.raw_orders
- **Transform**: Filters to status='completed', calculates revenue = amount * quantity
- **Output**: order_id, customer_id, order_date, amount, category, quantity, revenue

### monthly_revenue (transform)
- **Inputs**: transform.orders_cleaned
- **Transform**: Groups by month, aggregates SUM(revenue), COUNT(*), AVG(revenue)
- **Output**: month, total_revenue, total_orders, avg_order_value

## Rules
- Always trace back to the original source(s)
- If a pipeline file is missing, note it and continue with what you have
- Keep summaries concise but complete — focus on WHAT transforms, not HOW SQL works
- If you're unsure about a ref, ask for clarification
"""


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
                "is produced, trace data flow, or do impact analysis."
            ),
            instructions=_LINEAGE_INSTRUCTIONS,
            toolsets=[create_pipeline_toolset()],
            can_ask_questions=True,
            max_questions=2,
            typical_complexity="complex",
            agent_kwargs={"retries": 5},
        ),
    ]

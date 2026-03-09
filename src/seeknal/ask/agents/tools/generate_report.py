"""Generate report tool — create interactive HTML reports with Evidence.dev.

The agent generates Evidence-compatible markdown pages (SQL queries +
chart/table components), and this tool scaffolds an Evidence project,
builds static HTML, and returns the output path.
"""

import json

from langchain_core.tools import tool


def _do_generate(title: str, pages_json: str, project_path) -> str:
    """Core report generation logic, separated for testability.

    Args:
        title: Report title.
        pages_json: JSON string of page definitions [{name, content}, ...].
        project_path: Path to the seeknal project root.

    Returns:
        Success message with path, or error message string.
    """
    from pathlib import Path

    from seeknal.ask.report.builder import build_report
    from seeknal.ask.report.scaffolder import scaffold_report

    if not title or not title.strip():
        return "Error: Report title cannot be empty."

    # Parse pages — accept both list and JSON string
    if isinstance(pages_json, list):
        pages = pages_json
    else:
        try:
            pages = json.loads(pages_json)
        except (json.JSONDecodeError, TypeError) as e:
            return f"Error: Invalid pages JSON — {e}"

    if not isinstance(pages, list) or not pages:
        return "Error: 'pages' must be a non-empty array of {name, content} objects."

    for i, page in enumerate(pages):
        if not isinstance(page, dict):
            return f"Error: Page {i} must be a {{name, content}} object."
        if "name" not in page or "content" not in page:
            return f"Error: Page {i} is missing 'name' or 'content' field."

    # Scaffold Evidence project
    try:
        report_dir = scaffold_report(
            project_path=Path(project_path),
            title=title,
            pages=pages,
        )
    except ValueError as e:
        return f"Error scaffolding report: {e}"

    # Build HTML
    result = build_report(report_dir)

    # Check if result is a path (success) or error message
    if result.endswith(".html") or result.endswith("index.html"):
        return f"Report built successfully!\n\nOpen: {result}"
    else:
        return result


@tool
def generate_report(title: str, pages: list) -> str:
    """Generate an interactive HTML report with charts and tables.

    Creates an Evidence.dev project from markdown pages containing SQL
    queries and visualization components, then builds it into a static
    HTML dashboard.

    Write Evidence-compatible markdown for each page. Pages should contain:
    - SQL queries in fenced blocks: ```sql query_name\\nSELECT ...\\n```
    - Chart components referencing queries: <BarChart data={query_name} x=col y=col />
    - Markdown text for explanations and section headers

    Args:
        title: Report title (e.g., "Customer Segmentation Analysis").
        pages: Array of page objects, each with 'name' and 'content' fields.
            Example: [{"name": "overview", "content": "# Overview\\n```sql ..."}]
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    return _do_generate(title, pages, ctx.project_path)

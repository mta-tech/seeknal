"""Generate report tool — create interactive HTML reports with Evidence.dev.

The agent generates Evidence-compatible markdown pages (SQL queries +
chart/table components), and this tool scaffolds an Evidence project,
builds static HTML, and returns the output path.
"""

import json
import re

from langchain_core.tools import tool


def _do_generate(title: str, pages_json, project_path, console=None) -> str:
    """Core report generation logic, separated for testability.

    Args:
        title: Report title.
        pages_json: Pages as list[dict], or JSON string of [{name, content}, ...].
        project_path: Path to the seeknal project root.
        console: Optional Rich Console for streaming build progress.

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
    elif isinstance(pages_json, str):
        try:
            pages = json.loads(pages_json)
        except (json.JSONDecodeError, TypeError) as e:
            return f"Error: Invalid pages JSON — {e}"
    else:
        return "Error: 'pages' must be a non-empty array of {name, content} objects."

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
    result = build_report(report_dir, console=console)

    # Check if result is a path (success) or error message
    if result.endswith(".html") or result.endswith("index.html"):
        return f"Report built successfully!\n\nOpen: {result}"
    else:
        return result


@tool
def generate_report(title: str, page_content: str, confirmed: bool = False) -> str:
    """Generate an interactive HTML report with charts and tables.

    Creates an Evidence.dev project from markdown content containing SQL
    queries and visualization components, then builds it into a static
    HTML dashboard.

    IMPORTANT: This is a slow operation (installs npm dependencies, builds HTML).
    You MUST set confirmed=True to proceed. Before calling with confirmed=True,
    present your analysis findings to the user and ask for confirmation.

    Write Evidence-compatible markdown content. The content should include:
    - SQL queries in fenced blocks: ```sql query_name
      SELECT ... FROM table_name
      ```
    - Chart components referencing queries: <BarChart data={query_name} x="col" y="col" />
    - Table components: <DataTable data={query_name} />
    - Markdown text for explanations and section headers

    Example content:
      # Sales Overview
      ```sql total_sales
      SELECT region, SUM(amount) as total FROM sales GROUP BY region
      ```
      <BarChart data={total_sales} x="region" y="total" />

    Args:
        title: Report title (e.g., "Customer Segmentation Analysis").
        page_content: Evidence-compatible markdown content for the report page.
        confirmed: Must be True to actually generate. Present findings first.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    # Auto-confirm in exposure mode (non-interactive, no user to confirm)
    if not confirmed:
        try:
            if get_tool_context().exposure_mode:
                confirmed = True
        except RuntimeError:
            pass

    if not confirmed:
        return (
            "Report generation requires confirmation. "
            "Present your analysis findings to the user first, then ask: "
            "'Would you like me to generate an interactive HTML report based on "
            "this analysis? This may take a minute.' "
            "Call generate_report again with confirmed=True after user confirms."
        )

    ctx = get_tool_context()
    page_content = _fix_evidence_syntax(page_content)
    pages = [{"name": "index", "content": page_content}]
    return _do_generate(title, pages, ctx.project_path, console=ctx.console)


def _fix_evidence_syntax(content: str) -> str:
    """Fix common LLM mistakes in Evidence markdown.

    LLMs frequently generate ``data={{query}}`` (double braces) instead
    of the correct ``data={query}`` (single braces) for Evidence components.
    Also strips trailing semicolons from SQL blocks.
    """
    # Fix double braces in component props: data={{name}} → data={name}
    content = re.sub(r'\{\{(\w+)\}\}', r'{\1}', content)

    # Strip trailing semicolons from SQL fenced blocks
    def _strip_sql_semicolons(match: re.Match) -> str:
        sql = match.group(1).rstrip().rstrip(";")
        return sql + "\n```"

    content = re.sub(
        r'(```sql\s+\w+\s*\n.*?);\s*\n```',
        _strip_sql_semicolons,
        content,
        flags=re.DOTALL,
    )

    return content

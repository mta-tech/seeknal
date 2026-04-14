"""Generate report tool — create interactive HTML reports with Evidence.dev.

The agent generates Evidence-compatible markdown pages (SQL queries +
chart/table components), and this tool scaffolds an Evidence project,
builds static HTML, and returns the output path.
"""

import json
import re

def _do_generate(title: str, pages_json, project_path) -> str:
    """Core report generation logic, separated for testability.

    Args:
        title: Report title.
        pages_json: Pages as list[dict], or JSON string of [{name, content}, ...].
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
    result = build_report(report_dir)

    # Check if result is a path (success) or error message
    if result.endswith(".html") or result.endswith("index.html"):
        index_path = Path(result).resolve()
        python_launcher, shell_launcher = _write_report_launchers(report_dir, index_path)
        return _format_success_message(index_path, python_launcher, shell_launcher)
    else:
        return result


def _write_report_launchers(report_dir, index_path):
    """Create local launchers that serve the built report and open it."""
    from pathlib import Path

    report_dir = Path(report_dir).resolve()
    index_path = Path(index_path).resolve()
    build_dir = index_path.parent
    report_dir.mkdir(parents=True, exist_ok=True)

    python_launcher = report_dir / "open_report.py"
    python_launcher.write_text(
        f"""#!/usr/bin/env python3
import http.server
import socketserver
import webbrowser
from functools import partial
from pathlib import Path

HOST = "127.0.0.1"
BUILD_DIR = Path({str(build_dir)!r})

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return

Handler = partial(QuietHandler, directory=str(BUILD_DIR))

with socketserver.TCPServer((HOST, 0), Handler) as server:
    port = server.server_address[1]
    url = "http://{{}}:{{}}/".format(HOST, port)
    print("Serving {{}} at {{}}".format(BUILD_DIR, url))
    print("Press Ctrl+C to stop the server.")
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
""",
        encoding="utf-8",
    )
    python_launcher.chmod(0o755)

    shell_launcher = report_dir / "open_report.sh"
    shell_launcher.write_text(
        f'#!/bin/sh\nexec python3 {str(python_launcher)!r} "$@"\n',
        encoding="utf-8",
    )
    shell_launcher.chmod(0o755)

    return python_launcher, shell_launcher


def _format_success_message(index_path, python_launcher, shell_launcher):
    """Return a success message with absolute paths and working launch targets."""
    index_path = index_path.resolve()
    python_launcher = python_launcher.resolve()
    shell_launcher = shell_launcher.resolve()
    return (
        "Report built successfully!\n\n"
        f"Open: {index_path}\n"
        f"Open in browser: {shell_launcher}\n"
        f"Fallback: python3 {python_launcher}"
    )


async def generate_report(title: str, page_content: str) -> str:
    """Build an Evidence.dev report from markdown content and return the HTML path.

    See the `report-generation` skill for the full workflow: table exploration,
    content drafting, the approval-gate menu (discriminator: "Generate report
    now"), the post-build menu, Evidence syntax, and the report quality bar.

    Args:
        title: Report title (e.g., "Customer Segmentation Analysis").
        page_content: Evidence-compatible markdown content for the report page.
    """
    from seeknal.ask.agents.tools._context import get_tool_context, require_report_approval

    guard_error = require_report_approval("generate_report")
    if guard_error:
        return guard_error

    ctx = get_tool_context()
    page_content = _fix_evidence_syntax(page_content)
    pages = [{"name": "index", "content": page_content}]
    return await _async_do_generate(title, pages, ctx.project_path)


async def _async_do_generate(title: str, pages_json, project_path) -> str:
    """Async report generation with auto-background build step."""
    from pathlib import Path

    from seeknal.ask.report.builder import async_build_report
    from seeknal.ask.report.scaffolder import scaffold_report

    if not title or not title.strip():
        return "Error: Report title cannot be empty."

    # Parse pages (same logic as _do_generate)
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

    # Scaffold (fast, sync is fine)
    try:
        report_dir = scaffold_report(
            project_path=Path(project_path),
            title=title,
            pages=pages,
        )
    except ValueError as e:
        return f"Error scaffolding report: {e}"

    # Build HTML (potentially slow — auto-backgrounds if needed)
    result = await async_build_report(report_dir)

    from seeknal.ask.background import _BACKGROUNDED_PREFIX
    if result.startswith(_BACKGROUNDED_PREFIX):
        return result

    if result.endswith(".html") or result.endswith("index.html"):
        index_path = Path(result).resolve()
        python_launcher, shell_launcher = _write_report_launchers(report_dir, index_path)
        return _format_success_message(index_path, python_launcher, shell_launcher)
    else:
        return result


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

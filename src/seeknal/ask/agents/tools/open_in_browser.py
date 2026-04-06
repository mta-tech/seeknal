"""Open in browser tool — display report HTML in the user's default browser.

For Evidence reports that have a launcher (``open_report.py``), starts a
background HTTP server so relative asset paths resolve correctly.  For
plain HTML files, opens them directly via ``file://``.
"""

import os
import subprocess
import sys
import webbrowser
from pathlib import Path


def _find_launcher(resolved: Path) -> Path | None:
    """Locate ``open_report.py`` relative to the given path.

    - If *resolved* is a directory (report root), look inside it.
    - If *resolved* is an HTML file inside ``build/``, look in the parent
      report directory (two levels up from the file).
    """
    if resolved.is_dir():
        candidate = resolved / "open_report.py"
        if candidate.is_file():
            return candidate
        return None

    # HTML file — launcher lives in the report root (grandparent of build/index.html)
    report_dir = resolved.parent.parent
    candidate = report_dir / "open_report.py"
    if candidate.is_file():
        return candidate
    return None


def _do_open(report_path: str, project_path: Path) -> str:
    """Core logic for opening a report, separated for testability.

    Args:
        report_path: Relative path from project root (directory or HTML file).
        project_path: Absolute path to the seeknal project root.

    Returns:
        Success or error message string.
    """
    from seeknal.ask.agents.tools._security import validate_report_path

    # Validate path is within target/reports/
    try:
        resolved = validate_report_path(report_path, project_path)
    except ValueError as e:
        return f"Error: {e}"

    # Must exist
    if not resolved.exists():
        return f"Error: Report not found: {report_path}"

    # SSH detection — can't open a browser remotely
    if os.environ.get("SSH_CONNECTION"):
        return (
            f"SSH session detected — cannot open browser remotely.\n"
            f"Open this file manually: {resolved}"
        )

    # Try the launcher (starts HTTP server + opens browser)
    launcher = _find_launcher(resolved)
    if launcher:
        try:
            subprocess.Popen(
                [sys.executable, str(launcher)],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return (
                f"Report server starting — browser should open automatically.\n"
                f"Launcher: {launcher}"
            )
        except OSError as e:
            return f"Error launching report server: {e}"

    # Fallback: direct file:// open for plain HTML
    if resolved.is_file():
        try:
            webbrowser.open(f"file://{resolved}")
            return f"Opened {resolved} in the default browser."
        except OSError as e:
            return f"Error opening browser: {e}"

    return f"Error: No HTML file or launcher found at {report_path}"


def open_in_browser(report_path: str) -> str:
    """Open a generated report in the user's default web browser.

    Opens Evidence reports via their local HTTP server launcher, or
    plain HTML files directly. Only files within ``target/reports/``
    are allowed.

    Args:
        report_path: Path to the report directory or HTML file, relative
            to the project root. Examples:
            - ``target/reports/customer-analysis`` (report directory)
            - ``target/reports/customer-analysis/build/index.html`` (HTML file)
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    return _do_open(report_path, ctx.project_path)

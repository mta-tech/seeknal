"""List saved Seeknal Ask SQL test run results."""

from __future__ import annotations

import json


def list_ask_test_results() -> str:
    """List saved Ask SQL QA result JSON files, newest first."""
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.testing import discover_ask_sql_result_files

    ctx = get_tool_context()
    files = discover_ask_sql_result_files(ctx.project_path)
    if not files:
        return "No Ask SQL test results found. Run `run_ask_test()` or `seeknal ask test` first."

    lines = [f"## Ask SQL test results ({len(files)} found)", ""]
    for path in files[:40]:
        rel = path.relative_to(ctx.project_path.resolve()).as_posix()
        summary = ""
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            payload = data.get("summary") if isinstance(data, dict) else None
            if isinstance(payload, dict):
                summary = (
                    f" — {payload.get('passed', 0)}/{payload.get('total', 0)} passed, "
                    f"{payload.get('failed', 0)} failed"
                )
        except Exception:
            summary = " — unreadable summary"
        lines.append(f"- `{rel}`{summary}")
    if len(files) > 40:
        lines.append(f"- ... {len(files) - 40} more")
    lines.append("")
    lines.append("Use `read_ask_test_result(name_or_path='latest')` for details.")
    return "\n".join(lines)


__all__ = ["list_ask_test_results"]

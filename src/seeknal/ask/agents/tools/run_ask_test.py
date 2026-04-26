"""Run project-local Seeknal Ask SQL tests from the TUI/chat agent."""

from __future__ import annotations


def run_ask_test(
    name_or_path: str | None = None,
    sql_only: bool = True,
    agent_mode: bool = False,
) -> str:
    """Run Ask SQL QA tests and return a compact result summary.

    Args:
        name_or_path: Optional test name/path to run. Omit to run all tests.
        sql_only: Validate expected SQL only. Defaults to True for fast,
            deterministic TUI checks.
        agent_mode: When True, run the real Ask agent too. This may spend LLM
            tokens and should be used only when the user explicitly wants agent
            answer QA. If True, it overrides `sql_only`.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.testing import run_ask_sql_tests

    ctx = get_tool_context()
    suite = run_ask_sql_tests(
        ctx.project_path,
        select=name_or_path,
        sql_only=not agent_mode and sql_only,
    )

    mode = "agent" if not (not agent_mode and sql_only) else "sql-only"
    lines = [
        f"## Ask SQL test run ({mode})",
        "",
        f"Summary: {suite.passed}/{suite.total} passed, {suite.failed} failed, {suite.skipped} skipped",
    ]
    if suite.output_file:
        lines.append(f"Results: `{suite.output_file}`")
    lines.append("")
    for result in suite.results[:40]:
        if result.skipped:
            status = "SKIP"
        elif result.passed:
            status = "PASS"
        else:
            status = "FAIL"
        lines.append(f"- {status} `{result.name}` — {result.message}")
        if result.sql_error:
            lines.append(f"  SQL error: {result.sql_error[:400]}")
        for missing in result.missing_assertions[:3]:
            lines.append(f"  - {missing}")
    if len(suite.results) > 40:
        lines.append(f"- ... {len(suite.results) - 40} more results in saved JSON")
    return "\n".join(lines)


__all__ = ["run_ask_test"]

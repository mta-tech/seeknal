"""Non-terminal report delivery tool for the IBA action-delivery path."""

from __future__ import annotations

from pydantic_ai import ModelRetry

from seeknal.ask.agents.actions import WriteReport


def write_report(report: str) -> str:
    """Return the full Markdown report for the IBA bridge to deliver.

    This is deliberately a regular function tool, not a ``ToolOutput``: the
    model must receive its observation and decide on the next turn whether to
    finish. The IBA bridge recognises this tool's normal ``tool_end`` event and
    translates its string output into DF's report action and report-channel
    delta.
    """
    payload = WriteReport(report=report)
    if not payload.report.strip():
        raise ModelRetry("write_report requires non-empty Markdown in report.")
    return payload.report

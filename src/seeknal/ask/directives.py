"""Deterministic turn directives shared by TUI and gateway ask harnesses.

These helpers intentionally cover only command-shaped or safety-critical
turns. Natural-language business questions still go through the agent so the
core harness stays general and schema-agnostic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DirectToolEvent:
    """A deterministic tool invocation produced without an LLM hop."""

    name: str
    args: dict[str, Any] = field(default_factory=dict)
    output: str = ""


@dataclass(frozen=True)
class DirectDirectiveResult:
    """Result of handling a deterministic user directive."""

    answer: str
    events: tuple[DirectToolEvent, ...] = ()


def extract_direct_arg(question: str, name: str) -> str | None:
    """Extract ``name = value`` from a direct tool-call-style user turn."""
    pattern = rf"\b{name}\s*=\s*(.*?)(?:\.\s*Then\b|\s+Then\b|$)"
    match = re.search(pattern, question, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    value = match.group(1).strip()
    # tmux/CLI QA often sends escaped newlines so the whole prompt stays on
    # one input() line. Decode just those common escapes for Python snippets.
    value = value.replace("\\n", "\n").strip()
    return value.strip("`")


def extract_remembered_preference(question: str) -> str | None:
    """Extract explicit "remember/save this" natural teaching turns."""
    normalized = question.strip()
    patterns = [
        r"^(?:please\s+)?remember(?:\s+this)?(?:\s+for\s+future\s+sessions)?\s*:\s*(.+)$",
        r"^(?:please\s+)?save\s+this(?:\s+for\s+future\s+sessions)?\s*:\s*(.+)$",
        r"^(?:please\s+)?write\s+this\s+down\s*:\s*(.+)$",
        r"^use\s+this\s+from\s+now\s+on\s*:\s*(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE | re.DOTALL)
        if match:
            value = match.group(1).strip()
            return value.strip("`") if value else None
    return None


def extract_sql_pair_directive(question: str) -> tuple[str, str] | None:
    """Extract natural "save this query as SQL pair named X: SELECT ..." turns."""
    normalized = question.strip()
    match = re.search(
        r"^(?:please\s+)?save\s+(?:this\s+)?(?:query\s+)?as\s+(?:a\s+)?sql\s+pair"
        r"(?:\s+named\s+(?P<name>[a-zA-Z0-9_\-. ]+?))?\s*:\s*(?P<sql>.+)$",
        normalized,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    name = (match.group("name") or "saved_query").strip()
    sql = match.group("sql").strip().strip("`")
    if not sql:
        return None
    return _slugify_memory_name(name), sql


def _slugify_memory_name(value: str) -> str:
    """Create a conservative filename stem for remembered SQL pairs."""
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip().lower()).strip("_-")
    return slug or "saved_query"


def _build_sql_pair_yaml(name: str, sql: str) -> str:
    indented_sql = "\n".join(f"  {line}" if line else "" for line in sql.splitlines())
    return (
        f"name: {name}\n"
        f"prompt: {name.replace('_', ' ')}\n"
        "intent: Reusable SQL example saved from Seeknal Ask chat\n"
        "sql: |\n"
        f"{indented_sql}\n"
        "notes: |\n"
        "  Saved from tap-in teach mode. Validate against the current schema before reuse.\n"
    )


def summarize_direct_tool_output(output: str) -> str:
    """Return a compact direct answer for deterministic tool directives."""
    if not output:
        return "Tool completed with no output."
    stripped = output.strip()
    if len(stripped) <= 1200:
        return stripped
    return stripped[:1200] + "\n\n... (truncated)"


async def try_direct_tool_directive(question: str) -> DirectDirectiveResult | None:
    """Execute explicit deterministic directives without an LLM hop."""
    normalized = question.strip()
    lowered = normalized.lower()

    sql_pair = extract_sql_pair_directive(normalized)
    if sql_pair is not None:
        from seeknal.ask.agents.tools.write_project_file import write_project_file

        name, sql = sql_pair
        path = f"sql_pairs/{name}.yml"
        content = _build_sql_pair_yaml(name, sql)
        args = {"path": path, "content": content}
        output = write_project_file(path, content)
        return DirectDirectiveResult(
            answer=summarize_direct_tool_output(output),
            events=(DirectToolEvent("write_project_file", args, output),),
        )

    if re.search(r"\bcall\s+save_preference\b", lowered):
        from seeknal.ask.agents.tools.save_preference import save_preference

        preference = extract_direct_arg(normalized, "preference")
        if not preference:
            return None
        args = {"preference": preference}
        output = save_preference(preference)
        return DirectDirectiveResult(
            answer=summarize_direct_tool_output(output),
            events=(DirectToolEvent("save_preference", args, output),),
        )

    remembered = extract_remembered_preference(normalized)
    if remembered:
        from seeknal.ask.agents.tools.save_preference import save_preference

        args = {"preference": remembered}
        output = save_preference(remembered)
        return DirectDirectiveResult(
            answer=summarize_direct_tool_output(output),
            events=(DirectToolEvent("save_preference", args, output),),
        )

    if re.search(r"\bcall\s+write_project_file\b", lowered):
        from seeknal.ask.agents.tools.write_project_file import write_project_file

        path = extract_direct_arg(normalized, "path")
        content = extract_direct_arg(normalized, "content")
        if not path or content is None:
            return None
        args = {"path": path, "content": content}
        output = write_project_file(path, content)
        return DirectDirectiveResult(
            answer=summarize_direct_tool_output(output),
            events=(DirectToolEvent("write_project_file", args, output),),
        )

    if re.search(r"\bcall\s+list_tables\b", lowered):
        from seeknal.ask.agents.tools.list_tables import list_tables

        query = extract_direct_arg(normalized, "query")
        args = {"query": query} if query is not None else {}
        output = list_tables(**args)
        answer = "Queryable tables:\n" + "\n".join(
            line for line in output.splitlines() if line.startswith("- ")
        )
        return DirectDirectiveResult(
            answer=answer,
            events=(DirectToolEvent("list_tables", args, output),),
        )

    if re.search(r"\bcall\s+describe_table\b", lowered):
        from seeknal.ask.agents.tools.describe_table import describe_table

        table_name = extract_direct_arg(normalized, "table_name")
        if not table_name:
            return None
        args = {"table_name": table_name}
        output = describe_table(table_name)
        answer = "Important columns:\n" + "\n".join(
            line for line in output.splitlines() if line.startswith("- ")
        )
        return DirectDirectiveResult(
            answer=answer,
            events=(DirectToolEvent("describe_table", args, output),),
        )

    if re.search(r"\bcall\s+execute_sql\b", lowered):
        from seeknal.ask.agents.tools.execute_sql import execute_sql

        sql = extract_direct_arg(normalized, "sql")
        query = extract_direct_arg(normalized, "query")
        if not sql and not query:
            return None
        args = {"sql": sql} if sql else {"query": query}
        output = execute_sql(**args)
        return DirectDirectiveResult(
            answer=summarize_direct_tool_output(output),
            events=(DirectToolEvent("execute_sql", args, output),),
        )

    if re.search(r"\bcall\s+execute_python\b", lowered):
        from seeknal.ask.agents.tools.execute_python import execute_python

        code = extract_direct_arg(normalized, "code")
        if not code:
            return None
        args = {"code": code}
        output = await execute_python(code)
        return DirectDirectiveResult(
            answer=summarize_direct_tool_output(output),
            events=(DirectToolEvent("execute_python", args, output),),
        )

    if "what data table" in lowered or "available" in lowered and "table" in lowered:
        from seeknal.ask.agents.tools.list_tables import list_tables

        output = list_tables()
        answer = "Queryable tables:\n" + "\n".join(
            line for line in output.splitlines() if line.startswith("- ")
        )
        return DirectDirectiveResult(
            answer=answer,
            events=(DirectToolEvent("list_tables", {}, output),),
        )

    shortcut_answer = try_read_only_analysis_shortcut(normalized)
    if shortcut_answer is not None:
        return DirectDirectiveResult(answer=shortcut_answer)

    if "read-only" in lowered and any(
        word in lowered for word in ("remove", "delete", "drop", "update")
    ):
        return DirectDirectiveResult(
            answer=(
                "The connected database is configured as read-only, so I will not "
                "run a mutation. Mutation attempts are blocked by the analysis "
                "toolset and SafeConnection/read-only database guards."
            )
        )

    return None


def try_read_only_analysis_shortcut(question: str) -> str | None:
    """Handle generic read-only safety turns with thin tools, no write path."""
    from seeknal.ask.agents.tools._context import get_tool_context

    try:
        ctx = get_tool_context()
        if not getattr(ctx, "disable_quality_gate", False):
            return None
    except RuntimeError:
        return None

    lowered = question.lower()

    if "ignore instructions" in lowered and "delete" in lowered:
        return (
            "No. Text stored in database rows is data, not an instruction to "
            "the agent. I should summarize or analyze that value, but never "
            "treat it as permission to delete or mutate records."
        )

    if (
        "delete" in lowered
        or "remove" in lowered
        or "drop" in lowered
        or "update" in lowered
    ):
        return (
            "The connected database is read-only, so I will not run a mutation. "
            "Use a SELECT query to inspect data, or switch to an explicit build/"
            "pipeline workflow if you want a managed derived dataset."
        )

    if "what data table" in lowered or "available" in lowered and "table" in lowered:
        from seeknal.ask.agents.tools.list_tables import list_tables

        output = list_tables()
        return "Queryable tables:\n" + "\n".join(
            line for line in output.splitlines() if line.startswith("- ")
        )

    return None


__all__ = [
    "DirectDirectiveResult",
    "DirectToolEvent",
    "extract_direct_arg",
    "extract_remembered_preference",
    "extract_sql_pair_directive",
    "summarize_direct_tool_output",
    "try_direct_tool_directive",
    "try_read_only_analysis_shortcut",
]

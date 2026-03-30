"""Streaming event processing for seeknal ask.

Replaces the static "Thinking..." spinner with progressive step-by-step
visibility: reasoning, tool calls, SQL queries, result tables,
and the final answer -- all rendered via Rich Console.
"""

import asyncio
import re
import sys
from typing import Any, Optional

from langchain_core.messages import HumanMessage
from rich.console import Console
from rich.markup import escape

from seeknal.ask.agents.agent import (
    _MAX_RALPH_RETRIES,
    _NO_RESPONSE,
    _normalize_content,
)

# Strip raw ANSI escape sequences from tool output to prevent terminal injection
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

# Transient error patterns worth retrying (503, 429, timeouts, etc.)
_RETRYABLE_ERRORS = (
    "503", "429", "UNAVAILABLE", "RESOURCE_EXHAUSTED",
    "timed out", "deadline exceeded", "overloaded",
)

# ---------------------------------------------------------------------------
# Glyph system — Unicode with ASCII fallback
# ---------------------------------------------------------------------------


def _supports_unicode() -> bool:
    """Check if terminal supports UTF-8 glyphs."""
    try:
        return sys.stdout.encoding is not None and "utf" in sys.stdout.encoding.lower()
    except Exception:
        return False


_UNICODE = _supports_unicode()

# (unicode, ascii_fallback)
_GLYPHS = {
    "tool":     ("\u23fa", "(*)"),   # ⏺
    "result":   ("\u23bf", " \u2502"),  # ⎿ / |
    "ok":       ("\u2713", "[OK]"),  # ✓
    "fail":     ("\u2717", "[X]"),   # ✗
    "thinking": ("\u23fa", "(*)"),   # ⏺
    "arrow":    ("\u25b8", ">"),     # ▸
    "check":    ("\u2705", "[x]"),   # ✅
    "uncheck":  ("\u2b1c", "[ ]"),   # ⬜
    "current":  ("\u25b6", "[>]"),   # ▶
}


def _g(name: str) -> str:
    """Get glyph by name, with ASCII fallback."""
    pair = _GLYPHS.get(name, ("?", "?"))
    return pair[0] if _UNICODE else pair[1]


# ---------------------------------------------------------------------------
# Color scheme (Rich markup)
# ---------------------------------------------------------------------------

_C_PRIMARY = "#10b981"  # emerald green — success, prompts
_C_TOOL = "#fbbf24"     # amber — tool calls
_C_DIM = "dim"          # gray — metadata
_C_ERR = "red"          # errors
_C_OK = "green"         # success results


# Milestone tools that advance the plan step counter
_PLAN_MILESTONE_TOOLS = {
    "profile_data", "draft_node", "apply_draft", "dry_run_draft",
    "run_pipeline", "inspect_output", "execute_sql", "execute_python",
    "plan_pipeline", "generate_report", "query_metric",
}


def _show_plan(console: Console, steps: list[str], current: int) -> None:
    """Render plan as a progress checklist."""
    lines = []
    for i, step in enumerate(steps):
        if i < current:
            lines.append(f"  {_g('check')} [strike dim]{escape(step)}[/strike dim]")
        elif i == current:
            lines.append(f"  {_g('current')} [bold]{escape(step)}[/bold]")
        else:
            lines.append(f"  {_g('uncheck')} [{_C_DIM}]{escape(step)}[/{_C_DIM}]")
    console.print()
    console.print(f"[{_C_PRIMARY}]Plan ({current}/{len(steps)})[/{_C_PRIMARY}]")
    for line in lines:
        console.print(line)
    console.print()


def _advance_plan(console: Console) -> None:
    """Advance plan step and re-render if a plan is active."""
    try:
        from seeknal.ask.agents.tools._context import get_tool_context
        ctx = get_tool_context()
        if ctx.plan_steps and ctx.plan_step_index < len(ctx.plan_steps):
            ctx.plan_step_index += 1
            _show_plan(console, ctx.plan_steps, ctx.plan_step_index)
    except RuntimeError:
        pass


def _is_retryable(error: Exception) -> bool:
    """Check if an error is transient and worth retrying."""
    # Async exceptions (TimeoutError, CancelledError) have empty str() — check type first
    if isinstance(error, (asyncio.TimeoutError, asyncio.CancelledError)):
        return True
    msg = str(error).lower()
    return any(kw.lower() in msg for kw in _RETRYABLE_ERRORS)


def _sanitize_output(text: str) -> str:
    """Strip raw ANSI escape codes from tool output before rendering."""
    return _ANSI_ESCAPE.sub("", text)


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def _show_reasoning(console: Console, text: str) -> None:
    """Display LLM reasoning as subtle inline text."""
    console.print(f"[{_C_DIM}]{_g('thinking')} Thinking...[/{_C_DIM}]")
    truncated = text.strip()
    if len(truncated) > 300:
        truncated = truncated[:300] + "..."
    console.print(f"[{_C_DIM}]  {escape(truncated)}[/{_C_DIM}]")


def _show_tool_start(console: Console, name: str, args: Optional[dict] = None) -> None:
    """Display tool invocation with glyph and smart arg summary."""
    glyph = _g("tool")

    # ask_user: suppress — tool renders its own question and menu
    if name == "ask_user":
        return

    if args and name == "task":
        # Subagent delegation — show header with agent type
        subagent_type = args.get("subagent_type", "unknown")
        desc = str(args.get("description", ""))
        brief_desc = desc[:80].split("\n")[0] if desc else ""
        console.print(
            f"\n[{_C_TOOL}]{glyph} Running {escape(subagent_type)} agent...[/{_C_TOOL}]"
            f"[{_C_DIM}] {escape(brief_desc)}[/{_C_DIM}]"
        )
        # Reset subagent tool counter
        try:
            from seeknal.ask.agents.tools._context import get_tool_context
            get_tool_context().subagent_tool_count = 0
        except (RuntimeError, ImportError):
            pass
        return

    elif args and name == "execute" and "command" in args:
        console.print(f"\n[{_C_TOOL}]{glyph} {escape(name)}[/{_C_TOOL}]")
        _show_bash(console, args["command"])
    elif args and name == "execute_sql" and "sql" in args:
        console.print(f"\n[{_C_TOOL}]{glyph} {escape(name)}[/{_C_TOOL}]")
        _show_sql(console, args["sql"])
    elif args and name == "query_metric" and "metrics" in args:
        parts = [f"metrics: {args['metrics']}"]
        if args.get("dimensions"):
            parts.append(f"by: {args['dimensions']}")
        if args.get("filters"):
            parts.append(f"where: {args['filters']}")
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}[/{_C_TOOL}]"
            f"[{_C_DIM}]  {escape('  '.join(parts))}[/{_C_DIM}]"
        )
    elif args and name == "execute_python" and "code" in args:
        console.print(f"\n[{_C_TOOL}]{glyph} {escape(name)}[/{_C_TOOL}]")
        _show_python(console, args["code"])
    elif args and name == "generate_report" and "title" in args:
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(args['title'])})[/{_C_DIM}]"
        )
    elif args and name == "save_report_exposure" and "name" in args:
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(args['name'])})[/{_C_DIM}]"
        )
    elif args and name == "draft_node":
        parts = []
        if "node_type" in args:
            parts.append(f"type={args['node_type']}")
        if "name" in args:
            parts.append(f"name={args['name']}")
        summary = ", ".join(parts) if parts else ""
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(summary)})[/{_C_DIM}]"
        )
    elif args and name == "inspect_output" and "node_name" in args:
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(args['node_name'])})[/{_C_DIM}]"
        )
    elif args and name == "run_pipeline":
        label = "full" if args.get("full") else "incremental"
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({label})[/{_C_DIM}]"
        )
    elif args and name == "dry_run_draft" and "file_path" in args:
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(args['file_path'])})[/{_C_DIM}]"
        )
    elif args and name == "apply_draft" and "file_path" in args:
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(args['file_path'])})[/{_C_DIM}]"
        )
    elif args and name in ("edit_file", "edit_node") and "file_path" in args:
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(args['file_path'])})[/{_C_DIM}]"
        )
    elif args:
        arg_str = ", ".join(f"{k}={v!r}" for k, v in args.items())
        if len(arg_str) > 200:
            arg_str = arg_str[:200] + "..."
        console.print(
            f"\n[{_C_TOOL}]{glyph} {escape(name)}"
            f"[/{_C_TOOL}][{_C_DIM}]({escape(arg_str)})[/{_C_DIM}]"
        )
    else:
        console.print(f"\n[{_C_TOOL}]{glyph} {escape(name)}[/{_C_TOOL}]")


def _show_tool_end(console: Console, name: str, output: str) -> None:
    """Display tool result with status glyph."""
    output = _sanitize_output(output)
    rg = _g("result")

    # Handle task (subagent delegation): show summary with tool count
    if name == "task":
        tool_count = 0
        try:
            from seeknal.ask.agents.tools._context import get_tool_context
            ctx = get_tool_context()
            tool_count = ctx.subagent_tool_count
            ctx.subagent_tool_count = 0  # reset for next subagent call
        except (RuntimeError, ImportError):
            pass
        # Show completion line with tool count
        if tool_count > 0:
            console.print(f"  [{_C_DIM}]\u2514\u2500 {tool_count} tool uses[/{_C_DIM}]")
        # Show the subagent's summary result
        if output:
            brief = output[:300].replace("\n", " ") if len(output) > 300 else output
            console.print(f"  {rg} [{_C_OK}]{escape(brief)}[/{_C_OK}]")
        return

    # Handle submit_plan: render the plan checklist
    if name == "submit_plan":
        try:
            from seeknal.ask.agents.tools._context import get_tool_context
            ctx = get_tool_context()
            if ctx.plan_steps:
                _show_plan(console, ctx.plan_steps, ctx.plan_step_index)
                return
        except RuntimeError:
            pass
        console.print(f"  {rg} [{_C_OK}]{_g('ok')} Plan submitted[/{_C_OK}]")
        return

    # ask_user: show the user's selection compactly
    if name == "ask_user":
        if "skipped" in output or "budget" in output:
            console.print(f"  {rg} [{_C_DIM}]{escape(output)}[/{_C_DIM}]")
        else:
            console.print(f"  {rg} [{_C_OK}]{_g('ok')} {escape(output)}[/{_C_OK}]")
        return

    if name == "execute_sql" and "|" in output:
        _show_sql_result_table(console, output)
    elif name == "query_metric":
        _show_query_metric_output(console, output)
    elif name == "execute_python":
        _show_python_output(console, output)
    elif name == "generate_report":
        _show_report_output(console, output)
    elif name == "run_pipeline":
        lines = output.strip().split("\n")
        first_line = lines[0] if lines else output
        if "successfully" in first_line.lower():
            console.print(f"  {rg} [{_C_OK}]{_g('ok')} {escape(first_line)}[/{_C_OK}]")
        elif "FAILED" in first_line:
            console.print(f"  {rg} [{_C_ERR}]{_g('fail')} {escape(first_line)}[/{_C_ERR}]")
        else:
            summary = output[:200] + "..." if len(output) > 200 else output
            console.print(f"  {rg} [{_C_DIM}]{escape(summary)}[/{_C_DIM}]")
        if "Available outputs" in output:
            suggestions_start = output.index("Available outputs")
            console.print(f"  [{_C_DIM}]{escape(output[suggestions_start:])}[/{_C_DIM}]")
    elif name == "save_report_exposure":
        if output.startswith("Error"):
            console.print(f"  {rg} [{_C_ERR}]{_g('fail')} {escape(output)}[/{_C_ERR}]")
        else:
            console.print(f"  {rg} [{_C_OK}]{_g('ok')} {escape(output)}[/{_C_OK}]")
    else:
        summary = output[:200] + "..." if len(output) > 200 else output
        console.print(f"  {rg} [{_C_DIM}]{_g('ok')} {escape(summary)}[/{_C_DIM}]")

    # Auto-advance plan on milestone tool completion
    if name in _PLAN_MILESTONE_TOOLS:
        _advance_plan(console)


def _show_sql(console: Console, sql: str) -> None:
    """Display syntax-highlighted SQL."""
    from rich.syntax import Syntax

    console.print(Syntax(sql.strip(), "sql", theme="monokai", padding=(0, 2)))


def _show_python(console: Console, code: str) -> None:
    """Display syntax-highlighted Python code."""
    from rich.syntax import Syntax

    console.print(Syntax(code.strip(), "python", theme="monokai", padding=(0, 2)))


def _show_bash(console: Console, command: str) -> None:
    """Display syntax-highlighted shell command."""
    from rich.syntax import Syntax

    console.print(Syntax(command.strip(), "bash", theme="monokai", padding=(0, 2)))


def _show_python_output(console: Console, output: str) -> None:
    """Display Python execution output, showing full content."""
    rg = _g("result")
    if not output:
        console.print(f"  {rg} [{_C_DIM}]{_g('ok')} Done (no output)[/{_C_DIM}]")
        return

    if "Plots saved:" in output:
        parts = output.split("Plots saved:")
        if parts[0].strip():
            console.print(f"  {rg} [{_C_DIM}]{escape(parts[0].strip())}[/{_C_DIM}]")
        console.print(f"  {rg} [{_C_OK}]{_g('ok')} Plots saved:{escape(parts[1])}[/{_C_OK}]")
    else:
        display = output[:2000] + "..." if len(output) > 2000 else output
        console.print(f"  {rg} [{_C_DIM}]{escape(display)}[/{_C_DIM}]")


def _show_report_output(console: Console, output: str) -> None:
    """Display report generation result."""
    rg = _g("result")
    if not output:
        console.print(f"  {rg} [{_C_DIM}]{_g('ok')} Done (no output)[/{_C_DIM}]")
        return

    if "Report built successfully" in output or output.strip().endswith(".html"):
        console.print(f"  {rg} [{_C_OK}]{_g('ok')} {escape(output.strip())}[/{_C_OK}]")
    elif "requires confirmation" in output:
        console.print(f"  {rg} [{_C_TOOL}]Awaiting confirmation to generate report[/{_C_TOOL}]")
    elif output.startswith("Error") or "failed" in output.lower():
        console.print(f"  {rg} [{_C_ERR}]{_g('fail')} {escape(output.strip())}[/{_C_ERR}]")
    else:
        console.print(f"  {rg} [{_C_DIM}]{escape(output[:500])}[/{_C_DIM}]")


def _show_query_metric_output(console: Console, output: str) -> None:
    """Display query_metric result: compiled SQL (dim) + result table."""
    rg = _g("result")
    if not output:
        console.print(f"  {rg} [{_C_DIM}]{_g('ok')} Done (no output)[/{_C_DIM}]")
        return

    # Split metadata (containing ```sql block) from the result table.
    # The output format is: metadata lines, then a markdown table.
    # Extract compiled SQL from ```sql ... ``` block and show in dim styling.
    sql_match = re.search(r"```sql\s*\n(.*?)```", output, re.DOTALL)
    if sql_match:
        compiled_sql = sql_match.group(1).strip()
        console.print(f"  [{_C_DIM}]Compiled SQL: {escape(compiled_sql)}[/{_C_DIM}]")

    # Render the result table using the shared helper
    if "|" in output:
        # Extract just the table portion (lines starting with |)
        lines = output.strip().split("\n")
        table_lines = [ln for ln in lines if ln.startswith("|")]
        if table_lines:
            table_text = "\n".join(table_lines)
            # Append row count if present
            row_match = re.search(r"\((\d+) rows?\)", output)
            if row_match:
                table_text += f"\n({row_match.group(1)} rows)"
            _show_sql_result_table(console, table_text)
            return

    # Fallback: no table found, show as summary
    summary = output[:300] + "..." if len(output) > 300 else output
    console.print(f"  {rg} [{_C_DIM}]{escape(summary)}[/{_C_DIM}]")


def _show_sql_result_table(console: Console, output: str) -> None:
    """Parse markdown table output from execute_sql and render as Rich Table."""
    from rich.table import Table

    lines = output.strip().split("\n")

    # Find header and data lines (skip separator line with ---)
    table_lines = [ln for ln in lines if ln.startswith("|")]
    if len(table_lines) < 2:
        summary = output[:200] + "..." if len(output) > 200 else output
        console.print(f"  [{_C_DIM}]{_g('ok')} {summary}[/{_C_DIM}]")
        return

    # Parse header
    header = table_lines[0]
    columns = [c.strip() for c in header.split("|") if c.strip()]

    # Parse data rows (skip separator which is table_lines[1])
    data_lines = [
        ln for ln in table_lines[1:]
        if not all(c in "|- " for c in ln)
    ]

    table = Table(show_header=True, header_style="bold cyan")
    for col in columns:
        table.add_column(col)

    # Show first 10 rows
    shown = 0
    for line in data_lines:
        if shown >= 10:
            break
        cells = [c.strip() for c in line.split("|") if c.strip()]
        if cells:
            table.add_row(*cells)
            shown += 1

    console.print(table)

    # Footer with row count
    remaining = len(data_lines) - shown
    row_match = re.search(r"\((\d+) rows?\)", output)
    if row_match:
        total = int(row_match.group(1))
        if total > shown:
            console.print(f"  [{_C_DIM}]({total - shown} more rows)[/{_C_DIM}]")
    elif remaining > 0:
        console.print(f"  [{_C_DIM}]({remaining} more rows)[/{_C_DIM}]")


def _show_answer(console: Console, text: str) -> None:
    """Display final answer as Markdown with clean separator."""
    from rich.markdown import Markdown

    console.print()
    console.print(f"[{_C_PRIMARY}]{'─' * min(console.width, 60)}[/{_C_PRIMARY}]")
    console.print(Markdown(text.strip()))
    console.print()


def _show_retry(console: Console, attempt: int, max_retries: int) -> None:
    """Display retry separator for Ralph Loop."""
    console.print(
        f"\n[{_C_DIM}]{_g('thinking')} Continuing... "
        f"(attempt {attempt}/{max_retries})[/{_C_DIM}]\n"
    )


# ---------------------------------------------------------------------------
# Core streaming functions
# ---------------------------------------------------------------------------


async def _stream_one_pass(
    agent: Any, config: dict, question: str, console: Console
) -> tuple[str, bool, set]:
    """Run a single astream_events pass and render events progressively.

    Returns:
        A tuple of (answer_text, had_tool_calls, tool_names_called).
        answer_text is empty string if no answer was produced.
        had_tool_calls indicates whether any tools were invoked.
        tool_names_called is the set of tool names that were called.
    """
    text_buffer: list[str] = []
    accumulated = ""  # Cached join to avoid O(n^2) re-joining
    last_ai_text = ""  # Fallback: capture from on_chat_model_end
    had_tool_calls = False
    tool_names_called: set[str] = set()

    # Show spinner until first meaningful event arrives
    spinner = console.status(f"[{_C_PRIMARY}]{_g('thinking')} Thinking...[/{_C_PRIMARY}]")
    spinner.start()

    try:
        async for event in agent.astream_events(
            {"messages": [HumanMessage(content=question)]},
            config=config,
            version="v2",
        ):
            kind = event.get("event", "")

            # Stop spinner on first meaningful event
            if spinner is not None and kind in (
                "on_chat_model_stream", "on_tool_start", "on_chat_model_end",
            ):
                spinner.stop()
                spinner = None

            if kind == "on_chat_model_stream":
                chunk = event.get("data", {}).get("chunk")
                if chunk is None:
                    continue
                content = getattr(chunk, "content", "")
                token = _normalize_content(content)
                if not token:
                    continue

                # Gemini cumulative dedup: if the new token contains all
                # previously buffered text as a prefix, it's cumulative --
                # replace the buffer. Otherwise it's a delta -- append.
                if accumulated and token.startswith(accumulated) and len(token) > len(accumulated):
                    # Cumulative mode: token = all previous + new
                    text_buffer.clear()
                    text_buffer.append(token)
                    accumulated = token
                else:
                    # Delta mode: each chunk is new content
                    text_buffer.append(token)
                    accumulated += token

            elif kind == "on_chat_model_end":
                # Fallback: capture the final AI message content.
                # This fires when the LLM finishes a generation pass.
                # Gemini sometimes delivers the full answer here without
                # streaming individual tokens via on_chat_model_stream.
                output = event.get("data", {}).get("output")
                if output is not None:
                    content = getattr(output, "content", "")
                    text = _normalize_content(content)
                    if text:
                        last_ai_text = text

            elif kind == "on_tool_start":
                # Flush text buffer as reasoning (if any text accumulated)
                if text_buffer:
                    _show_reasoning(console, accumulated)
                    text_buffer.clear()
                    accumulated = ""

                had_tool_calls = True
                name = event.get("name", "unknown")
                tool_names_called.add(name)
                args = event.get("data", {}).get("input", {})
                # Track subagent type for task() calls so we can
                # distinguish builder vs verifier delegation.
                if name == "task" and isinstance(args, dict):
                    subagent_type = args.get("subagent_type", "")
                    tool_names_called.add(f"task:{subagent_type}")
                _show_tool_start(console, name, args)

            elif kind == "on_tool_end":
                name = event.get("name", "unknown")
                output = event.get("data", {}).get("output", "")
                if hasattr(output, "content"):
                    output = output.content
                output = str(output) if output else ""
                _show_tool_end(console, name, output)
    finally:
        # Ensure spinner is stopped even on exception
        if spinner is not None:
            spinner.stop()

    # Stream ended -- flush remaining buffer as answer
    if text_buffer:
        _show_answer(console, accumulated)
        return accumulated, had_tool_calls, tool_names_called

    # Fallback: if streaming didn't capture text but on_chat_model_end did,
    # use the last AI text (common with Gemini after tool calls).
    if last_ai_text:
        _show_answer(console, last_ai_text)
        return last_ai_text, had_tool_calls, tool_names_called

    return "", had_tool_calls, tool_names_called


async def stream_ask(
    agent: Any,
    config: dict,
    question: str,
    console: Console,
    quiet: bool = False,
) -> str:
    """Stream agent events with step-by-step visibility and Ralph Loop retry.

    Args:
        agent: The compiled LangGraph agent.
        config: Agent invocation config (with thread_id).
        question: User's natural language question.
        console: Rich Console instance for rendering.
        quiet: If True, suppress step-by-step output (only show final answer).

    Returns:
        The agent's text response.
    """
    if quiet:
        # Quiet mode: use sync ask() with spinner
        from seeknal.ask.agents.agent import ask as sync_ask

        with console.status(f"[{_C_PRIMARY}]{_g('thinking')} Thinking...[/{_C_PRIMARY}]"):
            return sync_ask(agent, config, question)

    # Inject console into ToolContext so tools (e.g. run_pipeline) can
    # print progress directly to the terminal.  Also reset the ask_user
    # question budget at the start of each turn.
    try:
        from seeknal.ask.agents.tools._context import get_tool_context

        ctx = get_tool_context()
        ctx.console = console
        ctx.questions_remaining = ctx.max_questions
    except RuntimeError:
        pass  # No context set yet (shouldn't happen in normal flow)

    # Streaming mode with progressive rendering
    current_question = question
    for attempt in range(_MAX_RALPH_RETRIES + 1):
        if attempt > 0:
            _show_retry(console, attempt, _MAX_RALPH_RETRIES)

        # Retry transient LLM errors (503, 429, timeouts) with exponential backoff
        max_llm_retries = 3
        for llm_attempt in range(max_llm_retries + 1):
            try:
                answer, had_tool_calls, tools_called = await _stream_one_pass(
                    agent, config, current_question, console
                )
                break  # Success — exit retry loop
            except Exception as e:
                if _is_retryable(e) and llm_attempt < max_llm_retries:
                    wait = min(2 ** llm_attempt * 5, 60)
                    console.print(
                        f"\n[yellow]LLM error: {e}[/yellow]\n"
                        f"[{_C_DIM}]Retrying in {wait}s "
                        f"(attempt {llm_attempt + 1}/{max_llm_retries})...[/{_C_DIM}]\n"
                    )
                    await asyncio.sleep(wait)
                    continue
                if _is_retryable(e):
                    # Retries exhausted but error is transient — fall through
                    # to Ralph Loop which eventually falls back to sync mode
                    console.print(
                        f"\n[yellow]LLM retries exhausted: {e}[/yellow]\n"
                        f"[{_C_DIM}]Falling back to alternative mode...[/{_C_DIM}]\n"
                    )
                    answer, had_tool_calls, tools_called = "", False, set()
                    break
                raise  # Non-retryable errors still propagate

        if answer:
            # Quality check: if pipeline ran without verification, nudge once.
            # Verification counts as either inspect_output directly or
            # delegating specifically to the verifier subagent via task().
            ran_pipeline = "run_pipeline" in tools_called
            verified = (
                "inspect_output" in tools_called
                or "task:verifier" in tools_called
            )
            if ran_pipeline and not verified and attempt < _MAX_RALPH_RETRIES:
                console.print(
                    f"\n[{_C_DIM}]{_g('thinking')} Pipeline ran but no verification. "
                    f"Nudging to verify output...[/{_C_DIM}]\n"
                )
                current_question = (
                    "The pipeline ran successfully but you did not verify the output. "
                    "Delegate to the verifier subagent via task(), or call "
                    "inspect_output() on 2-3 key nodes to show real data rows, "
                    "then provide your final summary."
                )
                continue

            return answer

        # No answer — retry with draft-aware nudge
        if had_tool_calls:
            # Targeted nudge: exposure mode with plan complete but no report generated
            try:
                ctx = get_tool_context()
                plan_done = (
                    ctx.plan_steps
                    and ctx.plan_step_index >= len(ctx.plan_steps)
                )
                if plan_done and "generate_report" not in tools_called and ctx.exposure_mode:
                    current_question = (
                        "Your analysis plan is complete. You MUST now call "
                        "generate_report(confirmed=True) with the report content. "
                        "Do not summarize as text — generate the HTML report."
                    )
                    continue
            except RuntimeError:
                pass

            partial = ""
            try:
                from seeknal.ask.agents.agent import _scan_partial_work

                partial = _scan_partial_work(get_tool_context().project_path)
            except (RuntimeError, ImportError):
                pass
            base_nudge = (
                "You have not provided a final response. "
                "If you were building a pipeline, continue calling tools until done. "
                "If you were analyzing data, summarize your findings as text."
            )
            current_question = (
                f"{base_nudge}\n\n{partial}" if partial else base_nudge
            )
        else:
            current_question = (
                f"Start by using list_tables to discover available data, "
                f"then answer: {question}"
            )

    # Last resort: fall back to sync invoke which uses a different code path
    # and may succeed where astream_events fails (common with Gemini).
    from seeknal.ask.agents.agent import ask as sync_ask

    console.print(f"\n[{_C_DIM}]{_g('thinking')} Retrying with sync mode...[/{_C_DIM}]\n")
    answer = await asyncio.to_thread(sync_ask, agent, config, question)
    if answer and answer != _NO_RESPONSE:
        _show_answer(console, answer)
        return answer

    return _NO_RESPONSE


# ---------------------------------------------------------------------------
# Choice detection — structural enforcement for interactive options
# ---------------------------------------------------------------------------

# Matches numbered options: "1. text", "1) text", "1 text"
_NUMBERED_OPT = re.compile(r"^\s*(\d+)[.)]\s+(.+)")
# Matches bullet options: "• text", "- text", "* text"
_BULLET_OPT = re.compile(r"^\s*[•\-\*]\s+(.+)")


def _extract_choices(answer: str) -> list[str]:
    """Extract choice options from the tail of an agent response.

    Parses the last contiguous block of numbered or bulleted options.
    Returns a list of option texts, or empty list if none found.
    Conservative: only matches clear patterns at the end of the response.
    """
    lines = answer.strip().splitlines()
    if len(lines) < 3:
        return []

    # Walk backwards from the end to find the option block
    options: list[str] = []
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            if options:
                break  # Empty line after we found options = end of block
            continue

        m_num = _NUMBERED_OPT.match(stripped)
        m_bul = _BULLET_OPT.match(stripped)

        if m_num:
            options.append(m_num.group(2).strip())
        elif m_bul:
            options.append(m_bul.group(1).strip())
        else:
            break  # Non-option line = end of block

    options.reverse()

    # Need at least 2 options to be a real choice
    if len(options) < 2:
        return []

    return options


def _present_choices_menu(
    console: Console, options: list[str]
) -> str | None:
    """Present choices as an interactive arrow-key menu. Returns selected text."""
    from seeknal.ask.agents.tools.ask_user import (
        _TYPE_YOUR_OWN,
        _select_with_arrow_keys,
        _select_with_numbered_input,
    )

    display = list(options) + [_TYPE_YOUR_OWN]

    console.print()
    try:
        idx = _select_with_arrow_keys(display)
    except Exception:
        idx = _select_with_numbered_input(console, display)

    if idx is None:
        return None

    selected = display[idx]
    if selected == _TYPE_YOUR_OWN:
        console.print()
        try:
            custom = input("  ▸ ").strip()
        except (EOFError, KeyboardInterrupt):
            return None
        return custom if custom else None

    return selected


async def chat_session(
    agent: Any,
    config: dict,
    console: Console,
    quiet: bool = False,
    session_store=None,
    session_name: str | None = None,
) -> None:
    """Run an interactive chat session with streaming visibility.

    Uses asyncio.to_thread for non-blocking input in the async context.
    """
    import asyncio

    # Enable interactive mode so ask_user can block on user input
    try:
        from seeknal.ask.agents.tools._context import get_tool_context

        get_tool_context().interactive = True
    except RuntimeError:
        pass

    if session_name:
        prompt_str = f"{session_name} {_g('arrow')} "
    else:
        prompt_str = f"{_g('arrow')} "
    msg_count = 0
    if session_store and session_name:
        existing = session_store.get(session_name)
        if existing:
            msg_count = existing.get("message_count", 0)

    while True:
        try:
            question = await asyncio.to_thread(input, prompt_str)
        except (EOFError, KeyboardInterrupt):
            console.print(f"\n[{_C_DIM}]Goodbye![/{_C_DIM}]")
            break

        question = question.strip()
        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            console.print(f"[{_C_DIM}]Goodbye![/{_C_DIM}]")
            break

        try:
            answer = await stream_ask(agent, config, question, console, quiet=quiet)
            if not answer or answer == _NO_RESPONSE:
                console.print(f"[{_C_DIM}]{_NO_RESPONSE}[/{_C_DIM}]")
            else:
                msg_count += 1
                # Update session metadata after successful turn
                if session_store and session_name:
                    session_store.update(
                        session_name,
                        message_count=msg_count,
                        last_question=question[:200],
                    )

                # Detect choices in the response and present interactive menu
                choices = _extract_choices(answer)
                if choices:
                    selection = await asyncio.to_thread(
                        _present_choices_menu, console, choices
                    )
                    if selection:
                        # Auto-submit the selection as the next chat message
                        console.print(
                            f"\n[{_C_DIM}]{_g('arrow')} {escape(selection)}[/{_C_DIM}]"
                        )
                        question = selection
                        continue  # Skip input(), go straight to stream_ask

            console.print()
        except KeyboardInterrupt:
            console.print(f"\n[{_C_DIM}]Cancelled.[/{_C_DIM}]\n")
        except Exception as e:
            if _is_retryable(e):
                console.print(f"[yellow]LLM temporarily unavailable: {e}[/yellow]")
                console.print(f"[{_C_DIM}]Try again or wait a moment.[/{_C_DIM}]\n")
            else:
                err_msg = str(e) or type(e).__name__
                console.print(f"[{_C_ERR}]Error: {err_msg}[/{_C_ERR}]\n")

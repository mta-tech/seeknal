"""Streaming event processing for seeknal ask.

Replaces the static "Thinking..." spinner with progressive step-by-step
visibility: reasoning panels, tool calls, SQL queries, result tables,
and the final answer -- all rendered via Rich Console.
"""

import asyncio
import re
from typing import Any, Optional

from langchain_core.messages import HumanMessage
from rich.console import Console
from rich.markup import escape

from seeknal.ask.agents.agent import (
    _BUILD_KEYWORDS,
    _BUILD_TOOL_NAMES,
    _MAX_RALPH_RETRIES,
    _NO_RESPONSE,
    _normalize_content,
)

# Strip raw ANSI escape sequences from tool output to prevent terminal injection
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def _sanitize_output(text: str) -> str:
    """Strip raw ANSI escape codes from tool output before rendering."""
    return _ANSI_ESCAPE.sub("", text)


# ---------------------------------------------------------------------------
# Rendering helpers -- thin wrappers around Rich Console
# ---------------------------------------------------------------------------


def _show_reasoning(console: Console, text: str) -> None:
    """Display LLM reasoning text in a dim Panel."""
    from rich.panel import Panel

    console.print(Panel(text.strip(), title="Reasoning", border_style="dim"))


def _show_tool_start(console: Console, name: str, args: Optional[dict] = None) -> None:
    """Display tool invocation start."""
    console.print(f"\n[bold]> {escape(name)}[/bold]")
    if args and name == "execute_sql" and "sql" in args:
        _show_sql(console, args["sql"])
    elif args and name == "execute_python" and "code" in args:
        _show_python(console, args["code"])
    elif args and name == "generate_report" and "title" in args:
        console.print(f"  [dim]Title: {escape(args['title'])}[/dim]")
    elif args and name == "save_report_exposure" and "name" in args:
        console.print(f"  [dim]Name: {escape(args['name'])}[/dim]")
    elif args:
        # Show truncated args for other tools
        arg_str = ", ".join(f"{k}={v!r}" for k, v in args.items())
        if len(arg_str) > 200:
            arg_str = arg_str[:200] + "..."
        console.print(f"  [dim]{escape(arg_str)}[/dim]")


def _show_tool_end(console: Console, name: str, output: str) -> None:
    """Display tool result summary, with special handling for execute_sql/execute_python."""
    output = _sanitize_output(output)

    if name == "execute_sql" and "|" in output:
        _show_sql_result_table(console, output)
    elif name == "execute_python":
        _show_python_output(console, output)
    elif name == "generate_report":
        _show_report_output(console, output)
    elif name == "save_report_exposure":
        if output.startswith("Error"):
            console.print(f"  [red]{escape(output)}[/red]")
        else:
            console.print(f"  [bold green]{escape(output)}[/bold green]")
    else:
        summary = output[:200] + "..." if len(output) > 200 else output
        console.print(f"  [dim]Done: {escape(summary)}[/dim]")


def _show_sql(console: Console, sql: str) -> None:
    """Display syntax-highlighted SQL."""
    from rich.syntax import Syntax

    console.print(Syntax(sql.strip(), "sql", theme="monokai", padding=(0, 2)))


def _show_python(console: Console, code: str) -> None:
    """Display syntax-highlighted Python code."""
    from rich.syntax import Syntax

    console.print(Syntax(code.strip(), "python", theme="monokai", padding=(0, 2)))


def _show_python_output(console: Console, output: str) -> None:
    """Display Python execution output, showing full content."""
    if not output:
        console.print("  [dim]Done (no output)[/dim]")
        return

    # Show plot paths prominently
    if "Plots saved:" in output:
        parts = output.split("Plots saved:")
        if parts[0].strip():
            console.print(f"  [dim]{escape(parts[0].strip())}[/dim]")
        console.print(f"  [bold green]Plots saved:{escape(parts[1])}[/bold green]")
    else:
        # Show full output (not truncated like generic tools)
        display = output[:2000] + "..." if len(output) > 2000 else output
        console.print(f"  [dim]{escape(display)}[/dim]")


def _show_report_output(console: Console, output: str) -> None:
    """Display report generation result."""
    if not output:
        console.print("  [dim]Done (no output)[/dim]")
        return

    if "Report built successfully" in output or output.strip().endswith(".html"):
        console.print(f"  [bold green]{escape(output.strip())}[/bold green]")
    elif output.startswith("Error") or "failed" in output.lower():
        from rich.panel import Panel

        console.print(Panel(
            escape(output.strip()),
            title="Report Build Error",
            border_style="red",
        ))
    else:
        console.print(f"  [dim]{escape(output[:500])}[/dim]")


def _show_sql_result_table(console: Console, output: str) -> None:
    """Parse markdown table output from execute_sql and render as Rich Table."""
    from rich.table import Table

    lines = output.strip().split("\n")

    # Find header and data lines (skip separator line with ---)
    table_lines = [ln for ln in lines if ln.startswith("|")]
    if len(table_lines) < 2:
        # Not a proper table, show as text
        summary = output[:200] + "..." if len(output) > 200 else output
        console.print(f"  [dim]Done: {summary}[/dim]")
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
    # Extract row count from the output footer if present
    row_match = re.search(r"\((\d+) rows?\)", output)
    if row_match:
        total = int(row_match.group(1))
        if total > shown:
            console.print(f"  [dim]({total - shown} more rows)[/dim]")
    elif remaining > 0:
        console.print(f"  [dim]({remaining} more rows)[/dim]")


def _show_answer(console: Console, text: str) -> None:
    """Display final answer as Markdown in a Panel."""
    from rich.markdown import Markdown
    from rich.panel import Panel

    console.print(Panel(Markdown(text.strip()), title="Answer", border_style="green"))


def _show_retry(console: Console, attempt: int, max_retries: int,
                is_build: bool = False) -> None:
    """Display retry separator for Ralph Loop."""
    label = "Nudging agent to continue building" if is_build else "Requesting summary"
    console.print(
        f"\n[dim]--- {label} (attempt {attempt}/{max_retries})... ---[/dim]\n"
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

    async for event in agent.astream_events(
        {"messages": [HumanMessage(content=question)]},
        config=config,
        version="v2",
    ):
        kind = event.get("event", "")

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
            _show_tool_start(console, name, args)

        elif kind == "on_tool_end":
            name = event.get("name", "unknown")
            output = event.get("data", {}).get("output", "")
            if hasattr(output, "content"):
                output = output.content
            output = str(output) if output else ""
            _show_tool_end(console, name, output)

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

        with console.status("[bold green]Thinking..."):
            return sync_ask(agent, config, question)

    # Detect if this is a BUILD request (one-shot pipeline build)
    question_lower = question.lower()
    is_build_request = any(kw in question_lower for kw in _BUILD_KEYWORDS)

    # Streaming mode with progressive rendering
    current_question = question
    for attempt in range(_MAX_RALPH_RETRIES + 1):
        if attempt > 0:
            _show_retry(console, attempt, _MAX_RALPH_RETRIES, is_build=is_build_request)

        answer, had_tool_calls, tools_called = await _stream_one_pass(
            agent, config, current_question, console
        )

        if answer:
            # Early-exit detection: if this is a BUILD request but the agent
            # only profiled data without calling any build tools (draft_node,
            # apply_draft, run_pipeline, etc.), don't accept the answer —
            # nudge the agent to actually build the pipeline.
            actually_built = bool(tools_called & _BUILD_TOOL_NAMES)
            only_profiled = tools_called <= {"profile_data", "list_tables"}

            if is_build_request and not actually_built and attempt < _MAX_RALPH_RETRIES:
                console.print(
                    "\n[dim]--- Agent returned text without building. "
                    "Nudging to continue... ---[/dim]\n"
                )
                if only_profiled:
                    # Fast-path: agent stalled after profiling — use stronger nudge
                    current_question = (
                        "You called profile_data but then stopped. In one-shot mode, "
                        "you must keep going. Call draft_node for each source file now, "
                        "then build transforms, then run. Do NOT output any text. "
                        "Do NOT call list_tables or profile_data again — you already have the data profile."
                    )
                else:
                    current_question = (
                        "You described a pipeline design but did NOT build it. "
                        "In one-shot BUILD mode you MUST call draft_node, apply_draft, "
                        "and run_pipeline — not just describe the plan. "
                        "Start building now: call draft_node for the first source node, "
                        "then continue with all remaining nodes. Do NOT respond with "
                        "text until the pipeline is built, run, and inspected. "
                        "Do NOT call list_tables or profile_data again — you already have the data profile."
                    )
                continue

            # Second check: pipeline ran but agent didn't inspect results
            ran_pipeline = "run_pipeline" in tools_called
            inspected = "inspect_output" in tools_called
            if is_build_request and ran_pipeline and not inspected and attempt < _MAX_RALPH_RETRIES:
                console.print(
                    "\n[dim]--- Pipeline ran but no inspect_output. "
                    "Nudging to show data... ---[/dim]\n"
                )
                current_question = (
                    "The pipeline ran successfully but you did not call inspect_output() "
                    "to show the actual data. Call inspect_output() on 2-3 key nodes "
                    "to show real data rows, then provide your final summary."
                )
                continue

            return answer

        # Fast sync fallback: if we've nudged once and the agent still
        # hasn't built anything, skip remaining streaming retries.
        if attempt >= 1 and is_build_request and not bool(tools_called & _BUILD_TOOL_NAMES):
            console.print(
                "\n[dim]Agent not responding to nudges — switching to sync mode...[/dim]\n"
            )
            break  # Exit loop → falls through to sync fallback

        # Adapt retry prompt based on what happened:
        if had_tool_calls:
            # Tools ran but LLM didn't produce text — nudge to continue building
            if is_build_request:
                current_question = (
                    "You have not finished building the pipeline. "
                    "Continue calling draft_node, apply_draft, and run_pipeline "
                    "until the pipeline is built, run, and inspected. "
                    "Do NOT provide a text summary — keep calling tools. "
                    "Do NOT call list_tables or profile_data again — you already have the data profile."
                )
            else:
                current_question = (
                    "Please summarize your findings from the tool calls above "
                    "and provide your analysis as a text response."
                )
        else:
            # LLM returned empty with no tool calls — re-ask with guidance
            current_question = (
                f"Start by using list_tables to discover available data, "
                f"then answer: {question}"
            )

    # Last resort: fall back to sync invoke which uses a different code path
    # and may succeed where astream_events fails (common with Gemini).
    from seeknal.ask.agents.agent import ask as sync_ask

    console.print("\n[dim]Retrying with sync mode...[/dim]\n")
    answer = await asyncio.to_thread(sync_ask, agent, config, question)
    if answer and answer != _NO_RESPONSE:
        _show_answer(console, answer)
        return answer

    return _NO_RESPONSE


async def chat_session(
    agent: Any,
    config: dict,
    console: Console,
    quiet: bool = False,
) -> None:
    """Run an interactive chat session with streaming visibility.

    Uses asyncio.to_thread for non-blocking input in the async context.
    """
    import asyncio

    while True:
        try:
            question = await asyncio.to_thread(input, "You: ")
        except (EOFError, KeyboardInterrupt):
            console.print("\nGoodbye!")
            break

        question = question.strip()
        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            console.print("Goodbye!")
            break

        try:
            answer = await stream_ask(agent, config, question, console, quiet=quiet)
            if not answer or answer == _NO_RESPONSE:
                console.print(f"[dim]{_NO_RESPONSE}[/dim]")
            console.print()
        except KeyboardInterrupt:
            console.print("\n[dim]Cancelled.[/dim]\n")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]\n")

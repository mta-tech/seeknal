"""Streaming event processing for seeknal ask.

Replaces the static "Thinking..." spinner with progressive step-by-step
visibility: reasoning panels, tool calls, SQL queries, result tables,
and the final answer -- all rendered via Rich Console.

Uses pydantic-ai's agent.iter() with typed node streaming.
"""

import re
from typing import Any, Optional

from rich.console import Console
from rich.markup import escape

from seeknal.ask.agents.agent import (
    _DIMINISHING_RETURNS_MSG,
    _LOW_OUTPUT_RETRIES,
    _LOW_OUTPUT_THRESHOLD,
    _MAX_RALPH_RETRIES,
    _NO_RESPONSE,
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


def _show_retry(console: Console, attempt: int, max_retries: int) -> None:
    """Display retry separator for Ralph Loop."""
    console.print(
        f"\n[dim]--- Requesting summary (attempt {attempt}/{max_retries})... ---[/dim]\n"
    )


# ---------------------------------------------------------------------------
# Core streaming functions
# ---------------------------------------------------------------------------


async def _stream_one_pass(
    agent: Any, deps: Any, message_history: list,
    question: str, console: Console
) -> tuple[str, list]:
    """Run a single agent.iter() pass and render events progressively.

    Returns (answer_text, updated_message_history).
    """
    from pydantic_ai import Agent
    from pydantic_ai._agent_graph import End, UserPromptNode
    from pydantic_ai.messages import (
        FunctionToolCallEvent,
        FunctionToolResultEvent,
        PartDeltaEvent,
        TextPartDelta,
    )

    text_buffer: list[str] = []

    async with agent.iter(
        question,
        deps=deps,
        message_history=message_history,
    ) as run:
        async for node in run:
            if isinstance(node, UserPromptNode):
                continue

            elif Agent.is_model_request_node(node):
                # Stream text tokens from the model
                async with node.stream(run.ctx) as request_stream:
                    async for event in request_stream:
                        if isinstance(event, PartDeltaEvent):
                            if isinstance(event.delta, TextPartDelta):
                                text_buffer.append(event.delta.content_delta)

            elif Agent.is_call_tools_node(node):
                # Flush text buffer as reasoning before tool calls
                if text_buffer:
                    _show_reasoning(console, "".join(text_buffer))
                    text_buffer.clear()

                # Stream tool call and result events
                async with node.stream(run.ctx) as handle_stream:
                    async for event in handle_stream:
                        if isinstance(event, FunctionToolCallEvent):
                            tool_name = event.part.tool_name
                            tool_args = event.part.args_as_dict()
                            _show_tool_start(console, tool_name, tool_args)
                        elif isinstance(event, FunctionToolResultEvent):
                            tool_name = event.result.tool_name
                            content = event.result.content
                            output = str(content) if content else ""
                            _show_tool_end(console, tool_name, output)

            elif isinstance(node, End):
                break

        result = run.result

    # Stream ended -- flush remaining buffer as answer
    if text_buffer:
        answer = "".join(text_buffer)
        _show_answer(console, answer)
        return answer, result.all_messages()

    # Check result.output for text that wasn't streamed token-by-token
    output = result.output or ""
    if output:
        _show_answer(console, output)
        return output, result.all_messages()

    return "", result.all_messages()


async def stream_ask(
    agent: Any,
    deps: Any,
    message_history: list,
    question: str,
    console: Console,
    quiet: bool = False,
) -> str:
    """Stream agent events with step-by-step visibility and Ralph Loop retry.

    Args:
        agent: The pydantic-ai Agent.
        deps: DeepAgentDeps instance.
        message_history: Conversation history (mutated in place).
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
            return sync_ask(agent, deps, message_history, question)

    # Streaming mode with progressive rendering
    current_question = question
    low_output_streak = 0

    for attempt in range(_MAX_RALPH_RETRIES + 1):
        if attempt > 0:
            _show_retry(console, attempt, _MAX_RALPH_RETRIES)
            current_question = (
                "Please summarize your findings from the tool calls above "
                "and provide your analysis as a text response."
            )

        answer, updated_history = await _stream_one_pass(
            agent, deps, message_history, current_question, console
        )
        # Update message history in place
        message_history.clear()
        message_history.extend(updated_history)

        if answer:
            # Quality gate: check and optionally retry once
            answer = await _stream_quality_gate(
                agent, deps, message_history, answer, console
            )
            return answer

        # Diminishing returns: track low-output retries
        if attempt > 0:
            output_chars = len(answer)
            if output_chars < _LOW_OUTPUT_THRESHOLD:
                low_output_streak += 1
            else:
                low_output_streak = 0

            if low_output_streak >= _LOW_OUTPUT_RETRIES:
                _show_answer(console, _DIMINISHING_RETURNS_MSG)
                return _DIMINISHING_RETURNS_MSG

    return _NO_RESPONSE


async def _stream_quality_gate(
    agent: Any,
    deps: Any,
    message_history: list,
    answer: str,
    console: Console,
) -> str:
    """Check answer quality and retry once via streaming if needed."""
    from seeknal.ask.agents.quality import check_answer_quality

    passes, reason = check_answer_quality(answer)
    if passes:
        return answer

    # One quality retry with guidance
    retry_answer, updated_history = await _stream_one_pass(
        agent, deps, message_history, reason, console
    )
    message_history.clear()
    message_history.extend(updated_history)

    return retry_answer if retry_answer else answer


async def chat_session(
    agent: Any,
    deps: Any,
    message_history: list,
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
            answer = await stream_ask(
                agent, deps, message_history, question, console, quiet=quiet
            )
            if not answer or answer == _NO_RESPONSE:
                console.print(f"[dim]{_NO_RESPONSE}[/dim]")
            console.print()
        except KeyboardInterrupt:
            console.print("\n[dim]Cancelled.[/dim]\n")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]\n")

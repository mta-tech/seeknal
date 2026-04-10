"""Streaming event processing for seeknal ask.

Replaces the static "Thinking..." spinner with progressive step-by-step
visibility: reasoning panels, tool calls, SQL queries, result tables,
and the final answer -- all rendered via Rich Console.

Uses pydantic-ai's agent.iter() with typed node streaming.
"""

import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

from rich.console import Console  # type annotation only
from rich.markup import escape

from seeknal.ask.agents.agent import (
    _DIMINISHING_RETURNS_MSG,
    _LOW_OUTPUT_RETRIES,
    _LOW_OUTPUT_THRESHOLD,
    _MAX_RALPH_RETRIES,
    _NO_RESPONSE,
)
from seeknal.ask.agents.tools._context import reset_report_approval

# Strip raw ANSI escape sequences from tool output to prevent terminal injection
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def _sanitize_output(text: str) -> str:
    """Strip raw ANSI escape codes from tool output before rendering."""
    return _ANSI_ESCAPE.sub("", text)




# ---------------------------------------------------------------------------
# Rendering helpers -- thin wrappers around Rich Console
# ---------------------------------------------------------------------------


def _show_reasoning(console: Console, text: str) -> None:
    """Display intermediate LLM reasoning in subordinate style."""
    stripped = text.strip()
    if not stripped:
        return
    lines = stripped.split('\n')
    console.print()
    console.print(f"[grey62]⎿  {escape(lines[0])}[/]")
    for line in lines[1:]:
        console.print(f"[grey62]   {escape(line)}[/]")


def _show_tool_start(console: Console, name: str, args: Optional[dict] = None) -> None:
    """Display tool invocation start."""
    # ask_user handles its own rendering via the callback
    if name == "ask_user":
        return
    # Subagent task: show branded delegation header
    if name == "task" and args:
        subagent = args.get("subagent_type", "agent")
        desc = str(args.get("description", ""))[:80]
        console.print(f"\n[brand.accent]⏺ Delegating to {escape(subagent)}[/]")
        if desc:
            console.print(f"  [text.dim]⎿ {escape(desc)}[/]")
        return
    # check_task: compact status check
    if name == "check_task" and args:
        task_id = args.get("task_id", "")
        console.print(f"\n[dim]> check_task({escape(str(task_id))})[/dim]")
        return
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

    # Subagent task result: compact summary
    if name == "task":
        summary = output[:300] + "..." if len(output) > 300 else output
        console.print(f"  [status.success]⎿ Done[/]")
        if summary.strip():
            # Show first 2 lines of result
            lines = summary.strip().split("\n")[:2]
            for line in lines:
                console.print(f"    [dim]{escape(line)}[/]")
        return
    # check_task result
    if name == "check_task":
        summary = output[:200] + "..." if len(output) > 200 else output
        console.print(f"  [dim]{escape(summary)}[/dim]")
        return

    if name == "execute_sql" and "|" in output:
        _show_sql_result_table(console, output)
    elif name == "execute_python":
        _show_python_output(console, output)
    elif name == "generate_report":
        _show_report_output(console, output)
    elif name == "save_report_exposure":
        if output.startswith("Error"):
            console.print(f"  [status.error]{escape(output)}[/]")
        else:
            console.print(f"  [status.success]{escape(output)}[/]")
    else:
        summary = output[:200] + "..." if len(output) > 200 else output
        console.print(f"  [dim]Done: {escape(summary)}[/dim]")




def _tool_spinner_message(tool_name: str, args: Optional[dict[str, Any]] = None) -> str | None:
    """Return a spinner message for long-running tool calls."""
    if tool_name == "generate_report":
        title = ""
        if args:
            title = str(args.get("title", "")).strip()
        return f"Generating report: {title}" if title else "Generating report"
    return None

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
        console.print(f"  [status.success]Plots saved:{escape(parts[1])}[/]")
    else:
        # Show full output (not truncated like generic tools)
        display = output[:2000] + "..." if len(output) > 2000 else output
        console.print(f"  [dim]{escape(display)}[/dim]")


def _extract_report_paths(output: str) -> tuple[Path | None, Path | None]:
    """Extract the HTML path and browser launcher path from report output."""
    html_path: Path | None = None
    browser_path: Path | None = None

    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Open: "):
            html_path = Path(stripped.removeprefix("Open: ").strip())
        elif stripped.startswith("Open in browser: "):
            browser_path = Path(stripped.removeprefix("Open in browser: ").strip())

    return html_path, browser_path


def _launch_report_action(console: Console, launcher_path: Path) -> None:
    """Launch the generated report helper in a detached process."""
    try:
        subprocess.Popen(
            [str(launcher_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        console.print(f"  [status.success]Opened in browser via {escape(str(launcher_path))}[/]")
    except OSError as exc:
        console.print(f"  [status.error]Failed to open report: {escape(str(exc))}[/]")



def _maybe_offer_report_action(console: Console, output: str) -> None:
    """Offer a direct TUI action to open a freshly built report."""
    if not sys.stdout.isatty():
        return

    _html_path, browser_path = _extract_report_paths(output)
    if browser_path is None or not browser_path.exists():
        return

    from seeknal.ui.interactive_menu import InteractiveMenu

    menu = InteractiveMenu(
        question="Report ready. What do you want to do next?",
        options=[
            {
                "label": "Open in browser",
                "description": "Launch the generated report from this TUI session",
                "recommended": "true",
            },
            {
                "label": "Continue",
                "description": "Keep working without opening the report right now",
            },
        ],
        console=console,
    )
    answer = menu.run()
    if answer == "Open in browser":
        _launch_report_action(console, browser_path)



def _show_report_output(console: Console, output: str) -> None:
    """Display report generation result."""
    if not output:
        console.print("  [dim]Done (no output)[/dim]")
        return

    if "Report built successfully" in output or output.strip().endswith(".html"):
        console.print(f"  [status.success]{escape(output.strip())}[/]")
        _maybe_offer_report_action(console, output)
    elif output.startswith("Error") or "failed" in output.lower():
        from rich.panel import Panel

        console.print(Panel(
            escape(output.strip()),
            title="Report Build Error",
            border_style="status.error",
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

    table = Table(show_header=True, header_style="table.header")
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

    console.print(Panel(Markdown(text.strip()), title="Answer", border_style="status.success"))


def _show_retry(console: Console, attempt: int, max_retries: int) -> None:
    """Display retry separator for Ralph Loop."""
    console.print(
        f"\n[dim]--- Requesting summary (attempt {attempt}/{max_retries})... ---[/dim]\n"
    )


def _show_turn_stats(console: Console, spinner, usage=None) -> None:
    """Display a compact stats line after each agent turn.

    Shows token usage, elapsed time, and tool call count:
      ↓ 1.4k tokens · 12s · 3 tool calls

    Args:
        usage: Optional pydantic-ai RunUsage for real token/tool counts.
    """
    from seeknal.ui.ask_spinner import _format_tokens, _format_elapsed

    parts = []
    tokens = usage.total_tokens if usage else spinner.tokens
    if tokens > 0:
        parts.append(f"↓ {_format_tokens(tokens)} tokens")
    elapsed = spinner.elapsed
    if elapsed >= 1.0:
        parts.append(_format_elapsed(elapsed))
    tool_calls = usage.tool_calls if usage else spinner.tool_uses
    if tool_calls > 0:
        parts.append(f"{tool_calls} tool call{'s' if tool_calls != 1 else ''}")
    if parts:
        console.print(f"\n[grey62]  {' · '.join(parts)}[/]")


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

    from pydantic_ai.usage import UsageLimits

    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ui.ask_spinner import AskSpinner

    text_buffer: list[str] = []
    spinner = AskSpinner("Thinking")
    spinner.start()

    ctx = get_tool_context()
    async with agent.iter(
        question,
        deps=deps,
        message_history=message_history,
        usage_limits=UsageLimits(request_limit=ctx.request_limit),
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
                                spinner.stop()
                                text_buffer.append(event.delta.content_delta)
                # Update spinner with real token count from usage API
                spinner.set_tokens(run.usage().total_tokens)

            elif Agent.is_call_tools_node(node):
                spinner.stop()
                # Flush text buffer as reasoning before tool calls
                if text_buffer:
                    _show_reasoning(console, "".join(text_buffer))
                    text_buffer.clear()

                # Stream tool call and result events
                subagent_spinner: AskSpinner | None = None
                tool_spinner: AskSpinner | None = None
                async with node.stream(run.ctx) as handle_stream:
                    async for event in handle_stream:
                        if isinstance(event, FunctionToolCallEvent):
                            tool_name = event.part.tool_name
                            tool_args = event.part.args_as_dict()
                            spinner.increment_tool_uses()
                            _show_tool_start(console, tool_name, tool_args)
                            # Start a spinner while subagent runs
                            if tool_name == "task":
                                subagent_name = tool_args.get("subagent_type", "agent")
                                subagent_spinner = AskSpinner(
                                    f"Running {subagent_name}"
                                )
                                subagent_spinner.start()
                            spinner_message = _tool_spinner_message(tool_name, tool_args)
                            if spinner_message:
                                tool_spinner = AskSpinner(spinner_message)
                                tool_spinner.start()
                        elif isinstance(event, FunctionToolResultEvent):
                            # Stop tool spinners before showing result
                            if subagent_spinner is not None:
                                subagent_spinner.stop()
                                subagent_spinner = None
                            if tool_spinner is not None:
                                tool_spinner.stop()
                                tool_spinner = None
                            tool_name = event.result.tool_name
                            content = event.result.content
                            output = str(content) if content else ""
                            from seeknal.ask.background import _BACKGROUNDED_PREFIX
                            if output.startswith(_BACKGROUNDED_PREFIX):
                                console.print(
                                    "  [brand.accent]Task moved to background — "
                                    "will notify when done[/]"
                                )
                            _show_tool_end(console, tool_name, output)
                # Clean up any lingering tool spinners
                if subagent_spinner is not None:
                    subagent_spinner.stop()
                if tool_spinner is not None:
                    tool_spinner.stop()

                # Restart spinner while model processes tool results
                spinner.update_message("Processing results")
                spinner.start()

            elif isinstance(node, End):
                break

        result = run.result
        usage = run.usage()

    spinner.stop()

    # Stream ended -- flush remaining buffer as answer
    if text_buffer:
        answer = "".join(text_buffer)
        _show_answer(console, answer)
        _show_turn_stats(console, spinner, usage)
        return answer, result.all_messages()

    # Check result.output for text that wasn't streamed token-by-token
    output = result.output or ""
    if output:
        _show_answer(console, output)
        _show_turn_stats(console, spinner, usage)
        return output, result.all_messages()

    _show_turn_stats(console, spinner, usage)
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
    reset_report_approval()

    if quiet:
        # Quiet mode: use sync ask() with spinner
        from seeknal.ask.agents.agent import ask as sync_ask

        with console.status("[spinner.active]Thinking..."):
            return sync_ask(agent, deps, message_history, question)

    # Streaming mode with progressive rendering
    current_question = question.strip()
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
    cost_info: Optional[dict] = None,
    session_store: Any = None,
    session_name: Optional[str] = None,
) -> None:
    """Run an interactive chat session with streaming visibility.

    Uses asyncio.to_thread for non-blocking input in the async context.
    Automatically saves conversation to the session store after each turn.
    """
    import asyncio

    def _save_session() -> None:
        """Save current message history to session store."""
        if session_store and session_name:
            try:
                session_store.save_messages(session_name, message_history)
                msg_count = len([m for m in message_history if hasattr(m, 'parts')])
                session_store.update(
                    session_name,
                    message_count=msg_count,
                    status="active",
                )
            except Exception:
                pass  # Best-effort save

    while True:
        try:
            question = await asyncio.to_thread(input, "You: ")
        except (EOFError, KeyboardInterrupt):
            _save_session()
            if session_name:
                console.print(f"\nSession saved: [brand.primary]{session_name}[/]")
            else:
                console.print("\nGoodbye!")
            break

        question = question.strip()
        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            _save_session()
            if session_name:
                console.print(f"Session saved: [brand.primary]{session_name}[/]")
            else:
                console.print("Goodbye!")
            break

        # Track last question for session metadata
        if session_store and session_name:
            session_store.update(session_name, last_question=question[:200])

        # Drain background task notifications and prepend to question
        try:
            from seeknal.ask.agents.tools._context import get_tool_context
            ctx = get_tool_context()
            completed = await ctx.background_registry.drain_notifications()
            if completed:
                notifications = "\n".join(
                    ctx.background_registry.format_notification(t) for t in completed
                )
                question = f"{notifications}\n\nUser: {question}"
                console.print(
                    f"[status.success]{len(completed)} background task(s) completed[/]"
                )
        except RuntimeError:
            pass  # No tool context yet

        try:
            answer = await stream_ask(
                agent, deps, message_history, question, console, quiet=quiet
            )
            if not answer or answer == _NO_RESPONSE:
                console.print(f"[dim]{_NO_RESPONSE}[/dim]")

            # Auto-save after each turn
            _save_session()

            # Show cost info if available
            if cost_info and "latest" in cost_info:
                ci = cost_info["latest"]
                tokens = getattr(ci, "cumulative_tokens", 0)
                usd = getattr(ci, "cumulative_usd", 0.0)
                if tokens > 0:
                    tok_str = f"{tokens / 1000:.1f}K" if tokens >= 1000 else str(tokens)
                    console.print(f"[text.dim]Tokens: {tok_str} | Cost: ${usd:.4f}[/]")

            console.print()
        except KeyboardInterrupt:
            console.print("\n[dim]Cancelled.[/dim]\n")
        except Exception as e:
            # Handle checkpoint rewind
            from pydantic_deep import RewindRequested
            if isinstance(e, RewindRequested):
                message_history.clear()
                message_history.extend(e.messages)
                console.print(f"\n[brand.primary]Rewound to:[/] [dim]{e.label}[/]\n")
                continue
            console.print(f"[status.error]Error: {e}[/]\n")

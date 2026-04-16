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

# Broader CSI family — catches function keys (\x1b[15~ for F5), bracketed-
# paste markers (\x1b[200~ … \x1b[201~), and any other escape that leaks
# into Python's input() from IDE shortcut passthrough. Covers both the
# a-zA-Z SGR terminator and the ~ terminator used for non-alphabetic
# sequences like F1-F12.
_CSI_ANY = re.compile(r"\x1b\[[0-9;?]*[a-zA-Z~]")


def _sanitize_output(text: str) -> str:
    """Strip raw ANSI escape codes from tool output before rendering."""
    return _ANSI_ESCAPE.sub("", text)


def _sanitize_user_input(text: str) -> str:
    """Strip ANSI/CSI escape sequences from user input.

    Terminal escape sequences leak into Python's ``input()`` from:
    1. IDE keyboard shortcuts (e.g. VS Code's "Developer: Reload Window"
       which arrives as a pasted multi-line block wrapped in bracketed-
       paste markers).
    2. Function keys (F1-F12) passed through by unusual tmux bindings.
    3. Mouse/cursor sequences when the terminal is misconfigured.

    Dropping all of these keeps the agent's question clean.
    """
    if not text:
        return text
    return _CSI_ANY.sub("", text)


def _extract_tool_result_text(result: Any) -> str:
    """Get a plain-text view of a pydantic-ai tool result.

    ``ToolReturnPart.content`` may be a string, a list of content parts
    (TextPart / MultiModalContent), or an arbitrary Python object. Calling
    ``str()`` on a list produces the Python repr, not the joined text, which
    breaks the downstream ``'|' in output`` SQL-table guard. Prefer the
    ``model_response_str`` helper when available, then fall through.
    """
    if result is None:
        return ""
    # Preferred: pydantic-ai's own text-serialization helper
    model_str = getattr(result, "model_response_str", None)
    if callable(model_str):
        try:
            rendered = model_str()
        except Exception:
            rendered = None
        if isinstance(rendered, str):
            return rendered
    content = getattr(result, "content", result)
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
                continue
            inner = getattr(part, "content", None)
            if isinstance(inner, str):
                parts.append(inner)
            else:
                parts.append(str(part))
        return "".join(parts)
    return str(content)




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
    # submit_plan: render the plan as a titled panel so the user can see
    # what the agent is about to do. Without this branch the plan is dumped
    # as a dim one-liner and easily missed.
    if name == "submit_plan" and args and "steps" in args:
        from rich.panel import Panel
        from rich.text import Text

        raw_steps = args.get("steps") or []
        if isinstance(raw_steps, list) and raw_steps:
            body = Text()
            for i, step in enumerate(raw_steps, 1):
                body.append(f"{i}. ", style="bold")
                body.append(f"{step}\n")
            console.print()
            console.print(
                Panel(
                    body,
                    title="Plan",
                    border_style="brand.accent",
                    padding=(0, 1),
                )
            )
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
    elif args and name == "publish_to_proof" and "title" in args:
        console.print(f"  [dim]Title: {escape(args['title'])}[/dim]")
    elif args and name == "read_proof_document" and "url" in args:
        console.print(f"  [dim]URL: {escape(str(args['url']))}[/dim]")
    elif args and name == "edit_proof_document" and "url" in args:
        console.print(f"  [dim]URL: {escape(str(args['url']))}[/dim]")
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
        console.print("  [status.success]⎿ Done[/]")
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

    # Suppress the trailing "Done: Plan submitted with N steps..." line for
    # submit_plan — the plan panel rendered by _show_tool_start is the only
    # visible surface the user needs.
    if name == "submit_plan":
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
    elif name == "publish_to_proof":
        if output.startswith("Error"):
            console.print(f"  [status.error]{escape(output)}[/]")
        else:
            console.print(f"  [status.success]{escape(output)}[/]")
    elif name in ("read_proof_document", "edit_proof_document"):
        if output.startswith("Error"):
            console.print(f"  [status.error]{escape(output)}[/]")
        else:
            console.print(f"  [status.success]{escape(output[:500])}[/]")
            if len(output) > 500:
                console.print(f"  [dim]... ({len(output) - 500} more chars)[/dim]")
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
    """Offer a direct TUI action on a freshly built report.

    Options include opening the local build in a browser, publishing the
    build to a Seeknal Report Server (returns a shareable URL), or hinting
    the Proof memo path. The Seeknal publish action runs entirely in-process
    — it packages the build tree, POSTs to the configured server, and prints
    the share URL + owner secret.
    """
    if not sys.stdout.isatty():
        return

    html_path, browser_path = _extract_report_paths(output)
    if browser_path is None or not browser_path.exists():
        return

    # Extract report slug from the build path (target/reports/{slug}/build/index.html)
    report_slug: Optional[str] = None
    if html_path is not None:
        try:
            parts = html_path.parts
            idx = parts.index("reports")
            report_slug = parts[idx + 1]
        except (ValueError, IndexError):
            report_slug = None

    from seeknal.ui.interactive_menu import InteractiveMenu

    options = [
        {
            "label": "Open in browser",
            "description": "Launch the generated report from this TUI session",
            "recommended": "true",
        },
    ]
    if report_slug:
        options.append({
            "label": "Publish to Seeknal Report Server",
            "description": "Upload the built report and get a shareable URL from a Seeknal Report Server",
        })
    options.extend([
        {
            "label": "Publish memo to Proof",
            "description": "Draft and publish a markdown summary to Proof (ask the agent in the next turn)",
        },
        {
            "label": "Continue",
            "description": "Keep working without opening the report right now",
        },
    ])

    menu = InteractiveMenu(
        question="Report ready. What do you want to do next?",
        options=options,
        console=console,
    )
    answer = menu.run()
    if answer == "Open in browser":
        _launch_report_action(console, browser_path)
    elif answer == "Publish to Seeknal Report Server" and report_slug:
        _publish_to_seeknal_report_action(console, report_slug)
    elif answer == "Publish memo to Proof":
        console.print(
            "  [dim]Type your next message: 'publish this as a memo to Proof' — "
            "the agent will compose the markdown and upload it.[/]"
        )


def _publish_to_seeknal_report_action(console: Console, report_slug: str) -> None:
    """Package and POST the built report to the Seeknal Report Server.

    Resolves server + api_key from env vars (SEEKNAL_PUBLISH_SERVER /
    SEEKNAL_PUBLISH_TOKEN) first, then falls back to the `publish.default`
    section of the project's profiles.yml. Prints the share URL + owner
    secret on success.
    """
    import os
    from pathlib import Path

    # Use the ask agent's tool context so `--project` (or auto-detected
    # project path) is respected. Falling back to Path.cwd() broke users
    # running `seeknal ask chat --project <elsewhere>` from the seeknal
    # repo itself.
    try:
        from seeknal.ask.agents.tools._context import get_tool_context
        project_path = get_tool_context().project_path
    except RuntimeError:
        project_path = Path.cwd()

    build_dir = project_path / "target" / "reports" / report_slug / "build"
    if not build_dir.is_dir():
        console.print(f"  [status.error]Build directory not found: {build_dir}[/]")
        return

    server = os.environ.get("SEEKNAL_PUBLISH_SERVER")
    api_key = os.environ.get("SEEKNAL_PUBLISH_TOKEN")
    if not server:
        try:
            from seeknal.workflow.materialization.profile_loader import ProfileLoader

            # Prefer the project's local profiles.yml over ~/.seeknal/profiles.yml
            # so publish config lives next to the project it publishes.
            local_profiles = project_path / "profiles.yml"
            loader = (
                ProfileLoader(profile_path=local_profiles)
                if local_profiles.exists()
                else ProfileLoader()
            )
            profile = loader.load_publish_profile("default")
            server = profile.get("server")
            if not api_key:
                api_key = profile.get("api_key")
        except Exception as exc:  # noqa: BLE001
            console.print(f"  [status.error]Could not load publish profile: {escape(str(exc))}[/]")
            return
    if not server:
        console.print(
            "  [status.error]No publish server configured. Set SEEKNAL_PUBLISH_SERVER or "
            "add publish.default.server to profiles.yml.[/]"
        )
        return

    try:
        from seeknal.publish.client import PublishClient
        from seeknal.publish.ledger import LedgerEntry, append_entry
        from seeknal.publish.packager import package_build

        try:
            rel = build_dir.relative_to(project_path)
        except ValueError:
            rel = build_dir
        console.print(f"  [dim]Packaging {rel}...[/]")
        tarball = package_build(build_dir)
        try:
            console.print(f"  [dim]Publishing to {server}...[/]")
            client = PublishClient(server, api_key)
            response = client.publish(tarball, report_slug, report_slug)
        finally:
            tarball.unlink(missing_ok=True)

        try:
            append_entry(
                LedgerEntry(
                    slug=response.slug,
                    server=server,
                    share_url=response.share_url,
                    report_name=report_slug,
                    published_at=response.created_at,
                )
            )
        except Exception:  # noqa: BLE001
            pass

        full_url = (
            response.share_url
            if response.share_url.startswith("http")
            else f"{server.rstrip('/')}{response.share_url}"
        )
        console.print(f"  [status.success]Published: {escape(full_url)}[/]")
        console.print(
            f"  [dim]Owner secret (save this — it is not retrievable later): "
            f"{escape(response.owner_secret)}[/]"
        )
    except Exception as exc:  # noqa: BLE001
        console.print(f"  [status.error]Publish failed: {escape(str(exc))}[/]")



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
        PartStartEvent,
        TextPart,
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
                # Stream text tokens from the model. We must handle
                # PartStartEvent as well as PartDeltaEvent: when a new text
                # part starts, its initial `content` arrives inside the
                # start event's `part`, and subsequent chunks arrive as
                # TextPartDelta. Dropping PartStartEvent silently loses
                # the first characters of the model's answer (e.g. the
                # final answer appearing as "...ai ini dihitung..." with
                # the leading "Nil" missing).
                async with node.stream(run.ctx) as request_stream:
                    async for event in request_stream:
                        if isinstance(event, PartStartEvent):
                            if isinstance(event.part, TextPart) and event.part.content:
                                spinner.stop()
                                text_buffer.append(event.part.content)
                        elif isinstance(event, PartDeltaEvent):
                            if isinstance(event.delta, TextPartDelta):
                                spinner.stop()
                                text_buffer.append(event.delta.content_delta)
                # Update spinner with real token count from usage API
                spinner.set_tokens(run.usage().total_tokens)

            elif Agent.is_call_tools_node(node):
                spinner.stop()
                # Stream tool call and result events. Flush any buffered
                # model text as an Answer panel on the FIRST tool call in
                # this node — the text that arrives before bookkeeping
                # tools (update_todo_status, ask_user) is substantive prose
                # that the user needs to see prominently, not dim reasoning.
                # Pydantic-ai always visits a call_tools node after every
                # model_request node, even when the model produced a final
                # answer with zero tool calls; in that case no tool event
                # fires, the buffer survives to the end-of-stream branch,
                # and _show_answer renders it there instead.
                subagent_spinner: AskSpinner | None = None
                tool_spinner: AskSpinner | None = None
                saw_tool_call = False
                async with node.stream(run.ctx) as handle_stream:
                    async for event in handle_stream:
                        if isinstance(event, FunctionToolCallEvent):
                            if not saw_tool_call and text_buffer:
                                flushed = "".join(text_buffer).strip()
                                if flushed:
                                    _show_answer(console, flushed)
                                text_buffer.clear()
                            saw_tool_call = True
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
                            output = _extract_tool_result_text(event.result)
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

    # Tell the user why the agent is running again — otherwise the silent
    # second pass looks like random churn on screen.
    console.print(
        f"\n[dim]--- Quality gate: {escape(reason)} — requesting a richer answer... ---[/dim]\n"
    )

    # One quality retry with guidance
    retry_answer, updated_history = await _stream_one_pass(
        agent, deps, message_history, reason, console
    )
    message_history.clear()
    message_history.extend(updated_history)

    return retry_answer if retry_answer else answer


def _save_chat_error_log(e: Exception) -> Optional[Path]:
    """Save a full traceback for a chat-loop exception to ~/.seeknal/last-chat-error.log.

    Best-effort: returns the path on success, None if the log itself failed
    (we never want a logging failure to crash the chat loop). Appends rather
    than overwrites so multiple errors in one session are all captured.
    """
    import os
    import traceback
    from datetime import datetime

    try:
        log_dir = Path(os.path.expanduser("~/.seeknal"))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "last-chat-error.log"

        ts = datetime.now().isoformat(timespec="seconds")
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"\n=== {ts} {type(e).__name__}: {e} ===\n")
            f.write(traceback.format_exc())
            f.write("\n")
        return log_path
    except Exception:  # noqa: BLE001
        return None


def _format_chat_loop_error(
    e: Exception, log_path: Optional[Path] = None
) -> str:
    """Format a chat-loop exception into a structured user-facing message.

    Detects common categories (network/DNS, connection, timeout, auth) and
    emits actionable hints. Falls back to the raw `str(e)` for unrecognized
    errors. The opaque `[Errno 8] nodename nor servname provided, or not
    known` from socket.gaierror — which previously dumped through the chat
    loop with no context — now becomes a multi-line breakdown of likely
    causes.
    """
    import socket

    err_type = type(e).__name__
    err_msg = str(e)

    # DNS / name-resolution errors. The macOS "nodename nor servname" wording
    # and the Linux "Name or service not known" / "Temporary failure in name
    # resolution" all map to socket.gaierror but can also appear wrapped in
    # OSError or httpx ConnectError, so we string-match too.
    is_dns = (
        isinstance(e, socket.gaierror)
        or "[Errno 8]" in err_msg
        or "nodename nor servname" in err_msg
        or "Name or service not known" in err_msg
        or "Temporary failure in name resolution" in err_msg
    )

    # Pull the failing request URL from httpx exceptions when available —
    # this is the single most useful diagnostic for "which call broke".
    httpx_url: Optional[str] = None
    is_connect_error = False
    is_timeout = False
    try:
        import httpx
        if isinstance(e, httpx.HTTPError):
            req = getattr(e, "request", None)
            if req is not None:
                try:
                    httpx_url = str(req.url)
                except Exception:  # noqa: BLE001
                    httpx_url = None
        if isinstance(e, (httpx.ConnectError, getattr(httpx, "RemoteProtocolError", type(None)))):
            is_connect_error = True
        if isinstance(e, (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.WriteTimeout, httpx.PoolTimeout)):
            is_timeout = True
    except ImportError:
        pass

    # Generic connection error fallback (non-httpx, non-DNS)
    if not is_connect_error and not is_dns and isinstance(e, (ConnectionError, OSError)):
        is_connect_error = True

    parts: list[str] = []

    if is_dns:
        parts.append("[status.error]Network/DNS error during agent call.[/]")
        parts.append("")
        parts.append("[text.dim]Most common causes:[/]")
        parts.append("  • LLM provider host unreachable (Gemini / OpenAI / Ollama) — check internet")
        parts.append("  • A tool's HTTP backend is unreachable — Proof Editor, Seeknal Report Server")
        parts.append("  • Empty/malformed env var: PROOF_BASE_URL, SEEKNAL_PUBLISH_SERVER, HTTP_PROXY, HTTPS_PROXY")
        parts.append("  • Custom model endpoint configured but invalid (check SEEKNAL_ASK_MODEL)")
    elif is_timeout:
        parts.append("[status.error]Timeout during agent call.[/]")
        parts.append("[text.dim]The remote host accepted the connection but didn't respond in time.[/]")
    elif is_connect_error:
        parts.append("[status.error]Connection error during agent call.[/]")
        parts.append("[text.dim]Could not establish a network connection — host unreachable, port closed, or interrupted.[/]")
    else:
        parts.append(f"[status.error]Error: {err_msg}[/]")

    if (is_dns or is_timeout or is_connect_error) and httpx_url:
        parts.append(f"\n[text.dim]Failing request URL: {httpx_url}[/]")
    if is_dns or is_timeout or is_connect_error:
        parts.append(f"[text.dim]Raw error ({err_type}): {err_msg}[/]")

    if log_path:
        parts.append(f"[text.dim]Full traceback saved to: {log_path}[/]")

    return "\n".join(parts)


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

        question = _sanitize_user_input(question).strip()
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
            log_path = _save_chat_error_log(e)
            console.print(_format_chat_loop_error(e, log_path) + "\n")

"""Seeknal Ask CLI — AI-powered data analysis.

Provides `seeknal ask` commands for natural language querying
of seeknal project artifacts.

Usage:
    seeknal ask "how many customers?"        One-shot question
    seeknal ask chat                         Interactive multi-turn chat
    seeknal ask report "customer analysis"   Generate interactive HTML report
"""

import click
import platform
import subprocess
import sys
import typer
import typer.core
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Optional

from seeknal.ask.project import find_project_path


class _AskGroup(typer.core.TyperGroup):
    """Custom TyperGroup that treats unrecognised commands as a question.

    When the first positional arg isn't a known subcommand (like 'chat'),
    moves all args to ctx.args so the callback handles them as a question.
    """

    def invoke(self, ctx):
        if ctx._protected_args:
            first_arg = ctx._protected_args[0]
            if first_arg not in self.commands:
                ctx.args = [*ctx._protected_args, *ctx.args]
                ctx._protected_args = []
        return super().invoke(ctx)


ask_app = typer.Typer(
    name="ask",
    help="AI-powered natural language data analysis.",
    cls=_AskGroup,
    invoke_without_command=True,
    context_settings={"allow_extra_args": True},
)


@ask_app.callback()
def ask_callback(
    ctx: typer.Context,
    provider: Optional[str] = typer.Option(
        None, "--provider", "-p", help="LLM provider: google, ollama"
    ),
    model: Optional[str] = typer.Option(
        None, "--model", "-m", help="Model name override"
    ),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path (auto-detected if not set)"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress step-by-step output, show only final answer"
    ),
    web: bool = typer.Option(
        False, "--web", help="Enable web search tools"
    ),
):
    """Ask questions about your seeknal project data using natural language.

    Usage:
        seeknal ask "How many customers?"              One-shot question
        seeknal ask --project path "How many?"         With explicit project
        seeknal ask chat                               Interactive chat
        seeknal ask chat --project path                Chat with project
        seeknal ask -q "How many?"                     Quiet mode (no steps)
        seeknal ask --web "Compare AOV to benchmarks"  With web search
    """
    if ctx.invoked_subcommand is not None:
        return

    if not ctx.args:
        typer.echo(ctx.get_help())
        return

    # One-shot mode — question comes from extra args
    question = " ".join(ctx.args)
    _run_oneshot(question, provider=provider, model=model, project=project, quiet=quiet, include_web=web)


def _run_oneshot(
    question: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    project: Optional[Path] = None,
    quiet: bool = False,
    include_web: bool = False,
):
    """Execute a one-shot question and print the answer."""
    project_path = project or find_project_path()

    try:
        from seeknal.ask.agents.agent import create_agent, ask as agent_ask
    except ImportError:
        typer.echo(typer.style(
            "Seeknal Ask dependencies not installed.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install seeknal[ask]", fg=typer.colors.CYAN
        ))
        raise typer.Exit(1)

    from seeknal.ui.console import get_console

    console = get_console()
    console.print(f"\n[text.dim]Project: {project_path}[/]")
    console.print(f"[text.dim]Question: {question}[/]\n")

    # Create agent (always with spinner -- this part is not streaming)
    with console.status("[spinner.active]Loading agent..."):
        agent, deps, message_history, _cost_info = create_agent(
            project_path,
            provider=provider,
            model=model,
            include_web=include_web,
        )

    # Stream the answer with step-by-step visibility
    import asyncio
    from seeknal.ask.streaming import stream_ask

    try:
        answer = asyncio.run(
            stream_ask(agent, deps, message_history, question, console, quiet=quiet)
        )
    except KeyboardInterrupt:
        console.print("\n[text.dim]Cancelled.[/]")
        raise typer.Exit(0)

    console.print()


@ask_app.command("chat")
def chat_command(
    provider: Optional[str] = typer.Option(
        None, "--provider", "-p", help="LLM provider: google, ollama"
    ),
    model: Optional[str] = typer.Option(
        None, "--model", "-m", help="Model name override"
    ),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress step-by-step output, show only final answer"
    ),
    style: Optional[str] = typer.Option(
        None, "--style", "-s", help="Output style: concise, explanatory, formal, conversational"
    ),
    budget: Optional[float] = typer.Option(
        None, "--budget", help="Max USD budget for this session"
    ),
    web: bool = typer.Option(
        False, "--web", help="Enable web search/fetch tools"
    ),
    session: Optional[str] = typer.Option(
        None, "--session", help="Resume an existing named session"
    ),
    name: Optional[str] = typer.Option(
        None, "--name", help="Create a session with this name"
    ),
):
    """Start an interactive multi-turn chat session."""
    project_path = project or find_project_path()

    try:
        from seeknal.ask.agents.agent import create_agent, ask as agent_ask
    except ImportError:
        typer.echo(typer.style(
            "Seeknal Ask dependencies not installed.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install seeknal[ask]", fg=typer.colors.CYAN
        ))
        raise typer.Exit(1)

    from seeknal.ask.sessions import SessionStore
    from seeknal.ui.console import get_console

    console = get_console()

    # Session management: resume or create
    store = SessionStore(project_path)
    session_name: str

    if session:
        # Resume existing session
        if store.get(session) is None:
            console.print(f"[status.error]Session '{session}' not found.[/]")
            console.print("[text.dim]List sessions with: seeknal session list[/]")
            raise typer.Exit(1)
        session_name = session
    else:
        session_name = store.create(name=name)

    # Load existing message history (empty for new sessions)
    message_history = store.load_messages(session_name)

    # Create agent with spinner
    with console.status("[spinner.active]Loading agent..."):
        agent, deps, _, _cost_info = create_agent(
            project_path,
            provider=provider,
            model=model,
            style=style,
            budget=budget,
            include_web=web,
        )

    # Branded animated header with bird mascot
    from rich.text import Text

    right = Text()
    right.append("Seeknal Ask", style="brand.primary")
    right.append(" — ", style="text.dim")
    right.append("All-in-One Data Agent", style="text.muted")
    right.append(f"\nProject: {project_path}", style="text.dim")
    right.append(f"\nSession: {session_name}", style="text.dim")
    right.append("\nType 'exit' or 'quit' to end the session.", style="text.dim")

    if console.is_terminal:
        from seeknal.ui.animate import animated_header
        from seeknal.ui.fox import render_animated_fox

        animated_header(render_animated_fox(), right)
    else:
        console.print("Seeknal Ask — All-in-One Data Agent")
        console.print(f"Project: {project_path}")
        console.print(f"Session: {session_name}")
        console.print("Type 'exit' or 'quit' to end the session.\n")

    if session and message_history:
        msg_count = len([m for m in message_history if hasattr(m, 'parts')])
        console.print(f"[text.dim]Resumed session with {msg_count} messages.[/]\n")

    # Use async chat session with streaming visibility
    import asyncio
    from seeknal.ask.streaming import chat_session

    try:
        asyncio.run(chat_session(
            agent, deps, message_history, console,
            quiet=quiet, cost_info=_cost_info,
            session_store=store, session_name=session_name,
        ))
    except KeyboardInterrupt:
        # Save on interrupt
        store.save_messages(session_name, message_history)
        store.update(session_name, status="paused")
        console.print(f"\nSession saved: [brand.primary]{session_name}[/]")
        console.print(f"[text.dim]Resume with: seeknal ask chat --session {session_name}[/]")


# ---------------------------------------------------------------------------
# Report sub-app
# ---------------------------------------------------------------------------

report_app = typer.Typer(
    name="report",
    help="Generate interactive HTML reports from your data.",
    invoke_without_command=True,
    context_settings={"allow_extra_args": True},
)
ask_app.add_typer(report_app, name="report")


@report_app.callback()
def report_callback(
    ctx: typer.Context,
    provider: Optional[str] = typer.Option(
        None, "--provider", "-p", help="LLM provider: google, ollama"
    ),
    model: Optional[str] = typer.Option(
        None, "--model", "-m", help="Model name override"
    ),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path (auto-detected if not set)"
    ),
    exposure: Optional[str] = typer.Option(
        None, "--exposure", "-e",
        help="Run a predefined report exposure by name",
    ),
):
    """Generate reports via AI-guided analysis.

    Usage:
        seeknal ask report "customer analysis"            Interactive report
        seeknal ask report --exposure monthly_report      Run YAML spec
        seeknal ask report serve my-report                Live-preview
        seeknal ask report list                           List reports
    """
    if ctx.invoked_subcommand is not None:
        return

    # Mode 2: YAML spec execution
    if exposure:
        _run_exposure(exposure, provider=provider, model=model, project=project)
        return

    if not ctx.args:
        typer.echo(ctx.get_help())
        return

    topic = " ".join(ctx.args)
    _run_report(topic, provider=provider, model=model, project=project)


def _run_report(
    topic: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    project: Optional[Path] = None,
):
    """Run the interactive report generation workflow."""
    project_path = project or find_project_path()

    # Check for data
    has_data = (
        (project_path / "target" / "intermediate").exists()
        or (project_path / "target" / "cache").exists()
    )
    if not has_data:
        typer.echo(typer.style(
            "No data found. Run your seeknal pipeline first.",
            fg=typer.colors.RED,
        ))
        raise typer.Exit(1)

    try:
        from seeknal.ask.agents.agent import create_agent
    except ImportError:
        typer.echo(typer.style(
            "Seeknal Ask dependencies not installed.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install seeknal[ask]", fg=typer.colors.CYAN
        ))
        raise typer.Exit(1)

    from seeknal.ui.console import get_console

    console = get_console()

    report_prompt = (
        f"Generate an interactive report about: {topic}\n\n"
        "Follow this workflow:\n"
        "1. Ask the user 2-3 brief scoping questions about what they want\n"
        "2. Run analyses using execute_sql and execute_python\n"
        "3. Call generate_report with a title and well-structured Evidence "
        "markdown pages containing SQL queries and chart components"
    )

    console.print(f"\n[brand.primary]Seeknal Report Generator[/]")
    console.print(f"[text.dim]Project: {project_path}[/]")
    console.print(f"[text.dim]Topic: {topic}[/]\n")

    with console.status("[spinner.active]Loading agent..."):
        agent, deps, message_history, _cost_info = create_agent(
            project_path,
            provider=provider,
            model=model,
        )

    import asyncio
    from seeknal.ask.streaming import chat_session, stream_ask

    try:
        answer = asyncio.run(stream_ask(
            agent, deps, message_history, report_prompt, console
        ))
        # Save rendered markdown
        if answer and answer.strip():
            _save_report_markdown(project_path, topic, answer, console)
        # Continue in chat mode for follow-up
        asyncio.run(chat_session(agent, deps, message_history, console))
    except KeyboardInterrupt:
        console.print("\n[text.dim]Cancelled.[/]")


def _save_report_markdown(
    project_path: Path,
    topic: str,
    content: str,
    console=None,
):
    """Save rendered markdown from a report analysis."""
    from seeknal.ask.report.exposure import save_rendered_markdown

    try:
        output_path = save_rendered_markdown(project_path, topic, content)
        if console:
            console.print(f"\n[status.success]Report saved:[/] {output_path}")
        else:
            typer.echo(f"\nReport saved: {output_path}")
    except ValueError as e:
        if console:
            console.print(f"[status.error]Failed to save report: {e}[/]")
        else:
            typer.echo(f"Failed to save report: {e}")


def _run_exposure(
    name: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    project: Optional[Path] = None,
):
    """Execute a predefined report exposure by name (Mode 2)."""
    project_path = project or find_project_path()

    try:
        from seeknal.ask.report.exposure import load_report_exposure, resolve_prompt
    except ImportError:
        typer.echo(typer.style(
            "Seeknal Ask dependencies not installed.", fg=typer.colors.RED
        ))
        typer.echo("Install with: " + typer.style(
            "pip install seeknal[ask]", fg=typer.colors.CYAN
        ))
        raise typer.Exit(1)

    # Load and validate exposure YAML
    try:
        exposure = load_report_exposure(project_path, name)
    except FileNotFoundError as e:
        typer.echo(typer.style(str(e), fg=typer.colors.RED))
        raise typer.Exit(1)
    except ValueError as e:
        typer.echo(typer.style(f"Invalid exposure: {e}", fg=typer.colors.RED))
        raise typer.Exit(1)

    # Branch: deterministic (sections) vs agent-driven (prompt-only)
    if exposure.get("sections"):
        _run_deterministic_exposure(
            name, exposure, project_path, provider=provider, model=model
        )
        return

    params = exposure.get("params", {})
    inputs = exposure.get("inputs", [])
    prompt_template = params.get("prompt", "")

    # Resolve Jinja2 template variables
    try:
        prompt = resolve_prompt(prompt_template, project_path, inputs, params)
    except Exception as e:
        typer.echo(typer.style(
            f"Failed to resolve prompt template: {e}", fg=typer.colors.RED
        ))
        raise typer.Exit(1)

    try:
        from seeknal.ask.agents.agent import create_agent
    except ImportError:
        typer.echo(typer.style(
            "Seeknal Ask dependencies not installed.", fg=typer.colors.RED
        ))
        raise typer.Exit(1)

    from seeknal.ui.console import get_console

    console = get_console()

    console.print(f"\n[brand.primary]Running exposure:[/] {name}")
    console.print(f"[text.dim]Project: {project_path}[/]")
    console.print(f"[text.dim]Prompt: {prompt[:100]}{'...' if len(prompt) > 100 else ''}[/]\n")

    with console.status("[spinner.active]Loading agent..."):
        agent, deps, message_history, _cost_info = create_agent(
            project_path, provider=provider, model=model,
        )

    import asyncio
    from seeknal.ask.streaming import stream_ask

    try:
        answer = asyncio.run(stream_ask(
            agent, deps, message_history, prompt, console
        ))
        if answer and answer.strip():
            _save_report_markdown(project_path, name, answer, console)
    except KeyboardInterrupt:
        console.print("\n[text.dim]Cancelled.[/]")


def _run_deterministic_exposure(
    name: str,
    exposure: dict,
    project_path: Path,
    provider: Optional[str] = None,
    model: Optional[str] = None,
):
    """Run the deterministic report path for exposures with sections."""
    from seeknal.ui.console import get_console

    console = get_console()

    console.print(f"\n[brand.primary]Running exposure:[/] {name}")
    console.print(f"[text.dim]Project: {project_path}[/]")
    section_count = len(exposure["sections"])
    console.print(f"[text.dim]Sections: {section_count}[/]\n")

    try:
        from seeknal.ask.report.deterministic import render_deterministic_report

        with console.status("") as status:
            def _on_progress(msg: str) -> None:
                status.update(f"[spinner.active]{msg}")

            html_path, markdown = render_deterministic_report(
                exposure, project_path,
                provider=provider, model=model,
                on_progress=_on_progress,
            )

        # Save rendered markdown
        if markdown and markdown.strip():
            _save_report_markdown(project_path, name, markdown, console)

        # Report success
        if html_path.endswith(".html"):
            console.print(
                f"\n[status.success]Report built:[/] {html_path}"
            )
        else:
            console.print(f"\n[status.warning]{html_path}[/]")

    except ValueError as e:
        msg = f"Deterministic report failed: {e}"
        console.print(f"[status.error]{msg}[/]")
        raise typer.Exit(1)
    except Exception as e:
        msg = f"Unexpected error: {e}"
        console.print(f"[status.error]{msg}[/]")
        raise typer.Exit(1)


@report_app.command("serve")
def report_serve_command(
    name: str = typer.Argument(..., help="Report name (slug)"),
    port: int = typer.Option(3000, "--port", help="Dev server port"),
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
):
    """Live-preview an existing report with Evidence dev server."""
    project_path = project or find_project_path()
    report_dir = project_path / "target" / "reports" / name

    if not report_dir.exists():
        typer.echo(typer.style(
            f"Report '{name}' not found at {report_dir}",
            fg=typer.colors.RED,
        ))
        typer.echo("List available reports with: " + typer.style(
            "seeknal ask report list", fg=typer.colors.CYAN,
        ))
        raise typer.Exit(1)

    typer.echo(f"Starting Evidence dev server for '{name}' on port {port}...")

    # Generate source data before starting dev server
    typer.echo("Generating source data...")
    try:
        src_result = subprocess.run(
            ["npx", "evidence", "sources"],
            cwd=str(report_dir),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if src_result.returncode != 0:
            typer.echo(typer.style(
                f"Warning: source generation had issues: {src_result.stderr.strip()[-500:]}",
                fg=typer.colors.YELLOW,
            ))
    except subprocess.TimeoutExpired:
        typer.echo(typer.style(
            "Warning: source generation timed out, continuing anyway...",
            fg=typer.colors.YELLOW,
        ))
    except FileNotFoundError:
        pass  # npm not found will be caught below

    typer.echo("Press Ctrl-C to stop.\n")

    webbrowser.open(f"http://localhost:{port}")

    try:
        subprocess.run(
            ["npx", "evidence", "dev", "--port", str(port)],
            cwd=str(report_dir),
        )
    except KeyboardInterrupt:
        typer.echo("\nServer stopped.")
    except FileNotFoundError:
        typer.echo(typer.style(
            "npx not found. Install Node.js 18+ to use report serve.",
            fg=typer.colors.RED,
        ))
        raise typer.Exit(1)


@report_app.command("list")
def report_list_command(
    project: Optional[Path] = typer.Option(
        None, "--project", help="Project path"
    ),
):
    """List existing reports."""
    project_path = project or find_project_path()
    reports_dir = project_path / "target" / "reports"

    if not reports_dir.exists():
        typer.echo("No reports found. Generate one with:")
        typer.echo(typer.style(
            "  seeknal ask report 'topic'", fg=typer.colors.CYAN,
        ))
        return

    reports = []
    for d in sorted(reports_dir.iterdir()):
        if d.is_dir() and (d / "pages").exists():
            pages = list((d / "pages").glob("*.md"))
            mtime = datetime.fromtimestamp(d.stat().st_mtime)
            build_exists = (d / "build" / "index.html").exists()
            reports.append((d.name, len(pages), mtime, build_exists))

    if not reports:
        typer.echo("No reports found. Generate one with:")
        typer.echo(typer.style(
            "  seeknal ask report 'topic'", fg=typer.colors.CYAN,
        ))
        return

    typer.echo(f"{'Name':<30} {'Pages':<8} {'Built':<8} {'Last Modified'}")
    typer.echo("-" * 70)
    for name, page_count, mtime, built in reports:
        built_str = "yes" if built else "no"
        typer.echo(
            f"{name:<30} {page_count:<8} {built_str:<8} "
            f"{mtime.strftime('%Y-%m-%d %H:%M')}"
        )

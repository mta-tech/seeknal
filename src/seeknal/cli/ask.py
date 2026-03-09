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
):
    """Ask questions about your seeknal project data using natural language.

    Usage:
        seeknal ask "How many customers?"              One-shot question
        seeknal ask --project path "How many?"         With explicit project
        seeknal ask chat                               Interactive chat
        seeknal ask chat --project path                Chat with project
        seeknal ask -q "How many?"                     Quiet mode (no steps)
    """
    if ctx.invoked_subcommand is not None:
        return

    if not ctx.args:
        typer.echo(ctx.get_help())
        return

    # One-shot mode — question comes from extra args
    question = " ".join(ctx.args)
    _run_oneshot(question, provider=provider, model=model, project=project, quiet=quiet)


def _run_oneshot(
    question: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    project: Optional[Path] = None,
    quiet: bool = False,
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

    try:
        from rich.console import Console

        console = Console()
        console.print(f"\n[dim]Project: {project_path}[/dim]")
        console.print(f"[dim]Question: {question}[/dim]\n")

        # Create agent (always with spinner -- this part is not streaming)
        with console.status("[bold green]Loading agent..."):
            agent, config = create_agent(
                project_path,
                provider=provider,
                model=model,
            )

        # Stream the answer with step-by-step visibility
        import asyncio
        from seeknal.ask.streaming import stream_ask

        try:
            answer = asyncio.run(
                stream_ask(agent, config, question, console, quiet=quiet)
            )
        except KeyboardInterrupt:
            console.print("\n[dim]Cancelled.[/dim]")
            raise typer.Exit(0)

        console.print()

    except ImportError:
        # Fallback without rich -- use sync ask()
        typer.echo(f"Project: {project_path}")
        typer.echo(f"Question: {question}\n")

        agent, config = create_agent(
            project_path,
            provider=provider,
            model=model,
        )
        answer = agent_ask(agent, config, question)
        typer.echo(answer)


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

    try:
        from rich.console import Console

        console = Console()
    except ImportError:
        console = None

    # Create agent (with spinner if Rich available)
    if console:
        with console.status("[bold green]Loading agent..."):
            agent, config = create_agent(
                project_path,
                provider=provider,
                model=model,
            )
    else:
        agent, config = create_agent(
            project_path,
            provider=provider,
            model=model,
        )

    if console:
        console.print("[bold]Seeknal Ask[/bold] -- Interactive Data Analysis")
        console.print(f"[dim]Project: {project_path}[/dim]")
        console.print("[dim]Type 'exit' or 'quit' to end the session.[/dim]\n")

        # Use async chat session with streaming visibility
        import asyncio
        from seeknal.ask.streaming import chat_session

        try:
            asyncio.run(chat_session(agent, config, console, quiet=quiet))
        except KeyboardInterrupt:
            console.print("\nGoodbye!")
    else:
        # Fallback without Rich -- use sync ask()
        typer.echo("Seeknal Ask -- Interactive Data Analysis")
        typer.echo(f"Project: {project_path}")
        typer.echo("Type 'exit' or 'quit' to end the session.\n")

        while True:
            try:
                question = input("You: ").strip()
            except (EOFError, KeyboardInterrupt):
                typer.echo("\nGoodbye!")
                break

            if not question:
                continue
            if question.lower() in ("exit", "quit", "q"):
                typer.echo("Goodbye!")
                break

            try:
                answer = agent_ask(agent, config, question)
                typer.echo(f"\n{answer}\n")
            except Exception as e:
                typer.echo(f"Error: {e}\n")


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
):
    """Generate an interactive HTML report via AI-guided analysis.

    Usage:
        seeknal ask report "customer analysis"   Generate report interactively
        seeknal ask report serve my-report        Live-preview an existing report
        seeknal ask report list                   List existing reports
    """
    if ctx.invoked_subcommand is not None:
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

    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None

    report_prompt = (
        f"Generate an interactive report about: {topic}\n\n"
        "Follow this workflow:\n"
        "1. Ask the user 2-3 brief scoping questions about what they want\n"
        "2. Run analyses using execute_sql and execute_python\n"
        "3. Call generate_report with a title and well-structured Evidence "
        "markdown pages containing SQL queries and chart components"
    )

    if console:
        console.print(f"\n[bold]Seeknal Report Generator[/bold]")
        console.print(f"[dim]Project: {project_path}[/dim]")
        console.print(f"[dim]Topic: {topic}[/dim]\n")

        with console.status("[bold green]Loading agent..."):
            agent, config = create_agent(
                project_path,
                provider=provider,
                model=model,
            )

        import asyncio
        from seeknal.ask.streaming import chat_session

        # Send initial report prompt, then enter chat for scoping questions
        from seeknal.ask.streaming import stream_ask
        try:
            asyncio.run(stream_ask(agent, config, report_prompt, console))
            # Continue in chat mode for follow-up
            asyncio.run(chat_session(agent, config, console))
        except KeyboardInterrupt:
            console.print("\n[dim]Cancelled.[/dim]")
    else:
        from seeknal.ask.agents.agent import ask as agent_ask
        agent, config = create_agent(
            project_path, provider=provider, model=model,
        )
        answer = agent_ask(agent, config, report_prompt)
        typer.echo(answer)


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

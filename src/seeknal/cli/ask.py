"""Seeknal Ask CLI — AI-powered data analysis.

Provides `seeknal ask` commands for natural language querying
of seeknal project artifacts.

Usage:
    seeknal ask "how many customers?"        One-shot question
    seeknal ask chat                         Interactive multi-turn chat
"""

import typer
from pathlib import Path
from typing import Optional

from seeknal.ask.project import find_project_path


ask_app = typer.Typer(
    name="ask",
    help="AI-powered natural language data analysis.",
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

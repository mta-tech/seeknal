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


ask_app = typer.Typer(
    name="ask",
    help="AI-powered natural language data analysis.",
    invoke_without_command=True,
)


def _find_project_path() -> Path:
    """Find seeknal project root from current directory."""
    cwd = Path.cwd()

    # Check for seeknal markers
    markers = ["seeknal.yml", "seeknal.yaml", "target"]
    for marker in markers:
        if (cwd / marker).exists():
            return cwd

    # Walk up directories
    for parent in cwd.parents:
        for marker in markers:
            if (parent / marker).exists():
                return parent

    # Default to cwd
    return cwd


@ask_app.callback()
def ask_callback(
    ctx: typer.Context,
    question: Optional[str] = typer.Argument(None, help="Question to ask about your data."),
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
    """Ask questions about your seeknal project data using natural language."""
    if ctx.invoked_subcommand is not None:
        return

    if question is None:
        # No question and no subcommand — show help
        typer.echo(ctx.get_help())
        return

    # One-shot mode
    _run_oneshot(question, provider=provider, model=model, project=project)


def _run_oneshot(
    question: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    project: Optional[Path] = None,
):
    """Execute a one-shot question and print the answer."""
    project_path = project or _find_project_path()

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
        from rich.markdown import Markdown

        console = Console()
        console.print(f"\n[dim]Project: {project_path}[/dim]")
        console.print(f"[dim]Question: {question}[/dim]\n")

        with console.status("[bold green]Thinking..."):
            agent, config = create_agent(
                project_path,
                provider=provider,
                model=model,
            )
            answer = agent_ask(agent, config, question)

        console.print(Markdown(answer))
        console.print()

    except ImportError:
        # Fallback without rich
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
):
    """Start an interactive multi-turn chat session."""
    project_path = project or _find_project_path()

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
        from rich.markdown import Markdown

        console = Console()
    except ImportError:
        console = None

    agent, config = create_agent(
        project_path,
        provider=provider,
        model=model,
    )

    if console:
        console.print("[bold]Seeknal Ask[/bold] — Interactive Data Analysis")
        console.print(f"[dim]Project: {project_path}[/dim]")
        console.print("[dim]Type 'exit' or 'quit' to end the session.[/dim]\n")
    else:
        typer.echo("Seeknal Ask — Interactive Data Analysis")
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
            if console:
                with console.status("[bold green]Thinking..."):
                    answer = agent_ask(agent, config, question)
                console.print()
                console.print(Markdown(answer))
                console.print()
            else:
                answer = agent_ask(agent, config, question)
                typer.echo(f"\n{answer}\n")
        except Exception as e:
            if console:
                console.print(f"[red]Error: {e}[/red]\n")
            else:
                typer.echo(f"Error: {e}\n")

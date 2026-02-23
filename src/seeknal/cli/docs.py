"""
Seeknal CLI - Documentation Search Command

A command for searching and browsing Seeknal CLI documentation.

Usage:
    seeknal docs init              # Show init command docs
    seeknal docs "feature group"   # Search for "feature group"
    seeknal docs --list            # List all available topics
    seeknal docs --json "version"  # JSON output for AI consumption
"""

import re
import json
import shutil
import subprocess
from typing import List, Optional, Dict, Any
from pathlib import Path
import typer


docs_app = typer.Typer(
    name="docs",
    help="Search Seeknal CLI documentation or show specific topics.",
)


def _echo_success(message: str):
    """Print success message in green."""
    typer.echo(typer.style(f"✓ {message}", fg=typer.colors.GREEN))


def _echo_error(message: str):
    """Print error message in red."""
    typer.echo(typer.style(f"✗ {message}", fg=typer.colors.RED))


def _echo_warning(message: str):
    """Print warning message in yellow."""
    typer.echo(typer.style(f"⚠ {message}", fg=typer.colors.YELLOW))


def _echo_info(message: str):
    """Print info message in blue."""
    typer.echo(typer.style(f"ℹ {message}", fg=typer.colors.BLUE))


def parse_frontmatter(content: str) -> dict:
    """Parse YAML frontmatter from markdown content.

    Args:
        content: Markdown content with optional frontmatter

    Returns:
        dict: Parsed frontmatter fields (summary, read_when, related)
    """
    frontmatter = {}

    # Check for frontmatter
    if not content.startswith("---"):
        return frontmatter

    # Extract frontmatter block
    match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
    if not match:
        return frontmatter

    yaml_content = match.group(1)

    # Simple YAML parsing for our format
    for line in yaml_content.split("\n"):
        if ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            frontmatter[key] = value
        elif line.strip().startswith("-"):
            # Handle list items (related)
            if "related" not in frontmatter:
                frontmatter["related"] = []
            item = line.strip().lstrip("-").strip().strip('"').strip("'")
            frontmatter["related"].append(item)

    return frontmatter


def list_available_topics(docs_path: Path) -> List[dict]:
    """List all available documentation topics.

    Args:
        docs_path: Path to the docs/cli/ directory

    Returns:
        List of dicts with name, summary, and path for each topic.
    """
    topics = []

    if not docs_path.exists():
        return topics

    for md_file in sorted(docs_path.glob("*.md")):
        if md_file.name == "index.md":
            continue  # Skip index file

        topic_name = md_file.stem
        summary = ""

        try:
            content = md_file.read_text(encoding="utf-8")
            frontmatter = parse_frontmatter(content)
            summary = frontmatter.get("summary", "")
        except Exception:
            pass

        topics.append({
            "name": topic_name,
            "summary": summary,
            "path": str(md_file.relative_to(docs_path.parent.parent))
        })

    return topics


def format_topics_list(topics: List[dict], use_json: bool = False) -> str:
    """Format topics list for output.

    Args:
        topics: List of topic dicts
        use_json: Whether to output as JSON

    Returns:
        Formatted string output.
    """
    if use_json:
        return json.dumps({
            "topics": topics,
            "count": len(topics)
        }, indent=2)

    lines = [f"Available CLI documentation ({len(topics)} topics):\n"]
    for topic in topics:
        name = topic["name"].ljust(18)
        summary = topic["summary"][:60] if topic["summary"] else ""
        lines.append(f"  {name} {summary}")

    return "\n".join(lines)


def check_rg_installed() -> bool:
    """Check if ripgrep (rg) is available.

    Returns:
        bool: True if rg is installed, False otherwise.
    """
    return shutil.which("rg") is not None


def get_rg_install_instructions() -> str:
    """Get platform-specific ripgrep installation instructions.

    Returns:
        str: Installation instructions for the current platform.
    """
    import platform
    system = platform.system().lower()

    instructions = {
        "darwin": "  brew install ripgrep",
        "linux": "  # Debian/Ubuntu:\n  sudo apt install ripgrep\n\n  # Arch:\n  sudo pacman -S ripgrep\n\n  # Fedora:\n  sudo dnf install ripgrep",
        "windows": "  # Using scoop:\n  scoop install ripgrep\n\n  # Using chocolatey:\n  choco install ripgrep",
    }

    return instructions.get(system, "  # Install ripgrep from: https://github.com/BurntSushi/ripgrep#installation")


# Ripgrep flags for search
RG_FLAGS = [
    "--with-filename",    # Show file names
    "--line-number",      # Show line numbers
    "--ignore-case",      # Case-insensitive search
    "--fixed-strings",    # Treat as literal, not regex (for security)
    "--glob", "*.md",     # Only search .md files
    "--json",             # JSON output for parsing
]


def run_rg_search(query: str, docs_path: Path, max_results: int = 50) -> List[dict]:
    """Execute ripgrep search and return structured results.

    Args:
        query: Search query string
        docs_path: Path to docs/cli/ directory
        max_results: Maximum number of results to return

    Returns:
        List of result dicts with file_path, line_number, snippet, matched_text

    Raises:
        RuntimeError: If ripgrep execution fails
    """
    if not check_rg_installed():
        raise RuntimeError("ripgrep (rg) is not installed")

    if not docs_path.exists():
        return []

    # Sanitize query - ripgrep with --fixed-strings handles this but be safe
    # Remove null bytes and control characters
    safe_query = query.replace('\x00', '').strip()
    if not safe_query:
        return []

    cmd = ["rg"] + RG_FLAGS + ["--", safe_query, str(docs_path)]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30  # 30 second timeout
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Search timed out after 30 seconds")
    except FileNotFoundError:
        raise RuntimeError("ripgrep (rg) executable not found")

    # Parse JSON output from ripgrep
    results = parse_rg_json_output(result.stdout, max_results)

    return results


def parse_rg_json_output(rg_stdout: str, max_results: int = 50) -> List[dict]:
    """Parse ripgrep's JSON output format.

    Ripgrep outputs one JSON object per line for each match.

    Args:
        rg_stdout: Raw stdout from ripgrep --json
        max_results: Maximum results to return

    Returns:
        List of result dicts
    """
    results = []

    for line in rg_stdout.strip().split('\n'):
        if not line:
            continue

        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        # ripgrep outputs different types of messages
        # We only care about "match" type
        if data.get("type") != "match":
            continue

        match_data = data.get("data", {})

        # Extract file path
        file_path = match_data.get("path", {}).get("text", "")

        # Extract line number
        line_number = match_data.get("line_number", 0)

        # Extract matched text (the full line)
        lines = match_data.get("lines", {})
        matched_text = lines.get("text", "")

        # Generate snippet with truncation
        snippet = generate_snippet(matched_text, 200)

        results.append({
            "file_path": file_path,
            "line_number": line_number,
            "snippet": snippet,
            "matched_text": matched_text.strip()
        })

        if len(results) >= max_results:
            break

    return results


def generate_snippet(text: str, max_length: int = 200) -> str:
    """Generate a truncated snippet from text.

    Truncates at word boundaries when possible.

    Args:
        text: Full text to truncate
        max_length: Maximum length

    Returns:
        Truncated snippet with ellipsis if needed
    """
    text = text.strip()

    if len(text) <= max_length:
        return text

    # Truncate to max_length
    truncated = text[:max_length]

    # Try to truncate at word boundary
    last_space = truncated.rfind(' ')
    if last_space > max_length // 2:
        truncated = truncated[:last_space]

    return truncated + "..."


def get_docs_cli_path() -> Path:
    """Get the path to the docs/cli/ directory.

    Returns:
        Path to docs/cli/ directory.
    """
    # Get the seeknal package directory and go up to project root
    # __file__ is src/seeknal/cli/docs.py
    # .parent = cli, .parent.parent = seeknal, .parent.parent.parent = src, .parent.parent.parent.parent = project root
    package_dir = Path(__file__).parent.parent.parent.parent
    docs_path = package_dir / "docs" / "cli"

    # Fallback: check if running from project root via cwd
    if not docs_path.exists():
        cwd_docs = Path.cwd() / "docs" / "cli"
        if cwd_docs.exists():
            return cwd_docs

    return docs_path


def find_exact_topic_match(query: str, docs_path: Path) -> Optional[Path]:
    """Find an exact topic match for the query.

    Args:
        query: Topic name to search for
        docs_path: Path to docs/cli/ directory

    Returns:
        Path to matching file, or None if not found.
    """
    if not docs_path.exists():
        return None

    # Normalize query: replace spaces and hyphens with underscores, lowercase
    normalized_query = query.lower().replace(" ", "_").replace("-", "_")

    # Try exact matches with .md extension
    for md_file in docs_path.glob("*.md"):
        if md_file.name == "index.md":
            continue
        # Normalize file stem the same way for comparison
        file_stem = md_file.stem.lower().replace("-", "_").replace(" ", "_")
        if file_stem == normalized_query:
            return md_file

    return None


def read_doc_file(file_path: Path) -> str:
    """Read a documentation file.

    Args:
        file_path: Path to the markdown file

    Returns:
        File contents as string.
    """
    return file_path.read_text(encoding="utf-8")


def format_search_results(results: List[dict], query: str, use_json: bool = False) -> str:
    """Format search results for output.

    Args:
        results: List of search result dicts
        query: Original search query
        use_json: Whether to output as JSON

    Returns:
        Formatted string output
    """
    if use_json:
        return format_search_results_json(results, query)

    return format_search_results_human(results, query)


def format_search_results_human(results: List[dict], query: str) -> str:
    """Format search results for human reading.

    Args:
        results: List of search result dicts
        query: Original search query

    Returns:
        Human-readable formatted string
    """
    if not results:
        return f"No documentation found for '{query}'.\n\nTry: seeknal docs --list to see available topics."

    lines = [f"Found {len(results)} result{'s' if len(results) != 1 else ''} for \"{query}\":\n"]

    current_file = None
    for result in results:
        file_path = result.get("file_path", "")
        line_number = result.get("line_number", 0)
        snippet = result.get("snippet", "")

        # Extract just the filename from the path for cleaner display
        import os
        display_path = os.path.relpath(file_path) if file_path else ""

        # Show file header if it's a new file
        if display_path != current_file:
            if current_file is not None:
                lines.append("")  # Blank line between files
            lines.append(f"{typer.style(display_path, fg=typer.colors.CYAN, bold=True)}:{line_number}")
            current_file = display_path
        else:
            lines.append(f"  line {line_number}")

        # Show snippet with indentation
        lines.append(f"  {typer.style('...', fg=typer.colors.BRIGHT_BLACK)}{snippet}")

    return "\n".join(lines)


def format_search_results_json(results: List[dict], query: str) -> str:
    """Format search results as JSON.

    Args:
        results: List of search result dicts
        query: Original search query

    Returns:
        JSON string
    """
    output = {
        "query": query,
        "results": results,
        "total_count": len(results),
        "truncated": False  # Will be updated by caller if needed
    }
    return json.dumps(output, indent=2)


@docs_app.callback(invoke_without_command=True)
def docs_command(
    ctx: typer.Context,
    query: List[str] = typer.Argument(None, help="Search query or topic name"),
    list_topics: bool = typer.Option(False, "--list", "-l", help="List all available topics"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
    max_results: int = typer.Option(50, "--max-results", "-n", help="Maximum results to return"),
):
    """Search Seeknal CLI documentation or show specific topics."""

    if ctx.invoked_subcommand is not None:
        return

    docs_path = get_docs_cli_path()

    # Handle --list
    if list_topics:
        topics = list_available_topics(docs_path)
        typer.echo(format_topics_list(topics, json_output))
        return

    # Handle no query - show help
    if not query:
        typer.echo(ctx.get_help())
        return

    # Join query arguments into a single string
    query_str = " ".join(query)

    # Check for exact file match first
    exact_match = find_exact_topic_match(query_str, docs_path)
    if exact_match:
        content = read_doc_file(exact_match)
        if json_output:
            typer.echo(json.dumps({
                "query": query_str,
                "match_type": "exact",
                "file": str(exact_match.name),
                "content": content
            }, indent=2))
        else:
            typer.echo(content)
        return

    # Check if ripgrep is installed
    if not check_rg_installed():
        _echo_error("ripgrep (rg) is required for searching documentation")
        typer.echo("\nInstall ripgrep:")
        typer.echo(get_rg_install_instructions())
        raise typer.Exit(3)

    # Perform search
    try:
        results = run_rg_search(query_str, docs_path, max_results)
    except RuntimeError as e:
        _echo_error(str(e))
        raise typer.Exit(4)

    output = format_search_results(results, query_str, json_output)
    typer.echo(output)

    # Exit code 1 if no results
    if not results:
        raise typer.Exit(1)

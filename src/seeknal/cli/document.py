"""Auto-documentation CLI for seeknal pipelines.

Generates LLM-powered descriptions and column documentation for
pipeline YAML files, enriching them in-place while preserving
formatting and comments via ruamel.yaml round-trip mode.
"""

import json
import warnings
from io import StringIO
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

console = Console()

# Documentation generation prompt template
_DOC_PROMPT = """\
You are a data documentation specialist. Generate clear, concise documentation
for this data pipeline artifact.

## Pipeline Information
- **Kind**: {kind}
- **Name**: {name}

## Pipeline Code
```
{code}
```

{schema_section}

{sample_section}

## Instructions

Generate documentation in this exact JSON format:
```json
{{
  "description": "One or two sentences describing WHAT this pipeline does and its business purpose. Do not describe HOW it works — the code shows that.",
  "column_descriptions": {{
    "column_name": "Business meaning of this column in one sentence."
  }}
}}
```

Only include columns you can identify from the code or schema. Keep descriptions concise.
Respond with ONLY the JSON block, no other text.
"""


def _find_project_path(project: Optional[str] = None) -> Path:
    """Find the seeknal project path."""
    if project:
        p = Path(project)
        if p.exists():
            return p
        raise typer.BadParameter(f"Project path not found: {project}")

    # Check current directory
    cwd = Path.cwd()
    if (cwd / "seeknal").is_dir():
        return cwd

    raise typer.BadParameter(
        "Not in a seeknal project directory. "
        "Use --project to specify the path."
    )


def _gather_pipeline_context(
    pipeline: dict,
    project_path: Path,
    conn=None,
) -> dict:
    """Gather context for documentation generation."""
    file_path = project_path / pipeline["file_path"]
    try:
        code = file_path.read_text(encoding="utf-8")
    except OSError:
        code = "(could not read file)"

    # Try to get schema from DuckDB
    schema_section = ""
    sample_section = ""
    name = pipeline["name"]

    if conn is not None:
        try:
            result = conn.execute(f"DESCRIBE {name}").fetchall()
            if result:
                cols = "\n".join(f"  - {row[0]}: {row[1]}" for row in result)
                schema_section = f"## Output Schema\n{cols}"
        except Exception:
            pass

        try:
            result = conn.execute(f"SELECT * FROM {name} LIMIT 3").fetchall()
            if result:
                desc = conn.execute(f"DESCRIBE {name}").fetchall()
                col_names = [row[0] for row in desc]
                rows = []
                for row in result:
                    rows.append(", ".join(f"{c}={v!r}" for c, v in zip(col_names, row)))
                sample_section = f"## Sample Data (first 3 rows)\n" + "\n".join(rows)
        except Exception:
            pass

    return {
        "kind": pipeline["kind"],
        "name": name,
        "code": code,
        "schema_section": schema_section,
        "sample_section": sample_section,
    }


def _parse_llm_response(response: str) -> Optional[dict]:
    """Parse LLM response to extract description and column_descriptions."""
    # Try to extract JSON from response
    text = response.strip()

    # Strip markdown code fences if present
    if "```json" in text:
        start = text.index("```json") + 7
        end = text.index("```", start)
        text = text[start:end].strip()
    elif "```" in text:
        start = text.index("```") + 3
        end = text.index("```", start)
        text = text[start:end].strip()

    try:
        data = json.loads(text)
        if isinstance(data, dict) and "description" in data:
            return data
    except json.JSONDecodeError:
        pass

    return None


def _update_yaml_file(
    file_path: Path,
    description: str,
    column_descriptions: dict,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[bool, str]:
    """Update a pipeline YAML file with documentation.

    Returns (changed, message).
    """
    try:
        from ruamel.yaml import YAML
    except ImportError:
        return False, "ruamel.yaml not installed (pip install ruamel.yaml)"

    yaml = YAML()
    yaml.preserve_quotes = True

    try:
        with open(file_path) as f:
            doc = yaml.load(f)
    except Exception as e:
        return False, f"Failed to parse YAML: {e}"

    if not isinstance(doc, dict):
        return False, "YAML root is not a mapping"

    changed = False
    changes = []

    # Update description
    if "description" not in doc or not doc["description"] or force:
        if description:
            doc["description"] = description
            changed = True
            changes.append(f"  description: {description}")

    # Update column_descriptions
    if "column_descriptions" not in doc or not doc["column_descriptions"] or force:
        if column_descriptions:
            doc["column_descriptions"] = column_descriptions
            changed = True
            changes.append(f"  column_descriptions: {len(column_descriptions)} columns")

    if not changed:
        return False, "Already documented (use --force to overwrite)"

    if dry_run:
        return True, "\n".join(changes)

    try:
        with open(file_path, "w") as f:
            yaml.dump(doc, f)
    except Exception as e:
        return False, f"Failed to write YAML: {e}"

    return True, "\n".join(changes)


def generate_docs(
    project: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """Generate documentation for pipeline YAML files."""
    project_path = _find_project_path(project)

    # Import ask dependencies
    try:
        from seeknal.ask.agents.providers import get_llm
        from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery
    except ImportError:
        console.print("[red]seeknal[ask] dependencies not installed.[/red]")
        console.print("Install with: pip install seeknal[ask]")
        raise typer.Exit(1)

    # Discover pipelines
    discovery = ArtifactDiscovery(project_path)
    pipelines = discovery.get_pipelines_summary()

    if not pipelines:
        console.print("[yellow]No pipeline files found.[/yellow]")
        return

    # Filter by name if specified
    if pipeline_name:
        pipelines = [p for p in pipelines if p["name"] == pipeline_name]
        if not pipelines:
            console.print(f"[red]Pipeline '{pipeline_name}' not found.[/red]")
            raise typer.Exit(1)

    # Only process YAML pipelines (not Python)
    yaml_pipelines = [p for p in pipelines if p["file_path"].endswith((".yml", ".yaml"))]
    if not yaml_pipelines:
        console.print("[yellow]No YAML pipeline files to document.[/yellow]")
        return

    # Create LLM
    llm = get_llm(provider=provider, model=model)

    # Try to get DuckDB connection for schema info
    conn = None
    try:
        from seeknal.cli.repl import REPL
        repl = REPL(project_path=project_path, skip_history=True)
        conn = repl.conn
    except Exception:
        pass

    # Process each pipeline
    total = len(yaml_pipelines)
    documented = 0
    skipped = 0
    failed = 0

    for i, pipeline in enumerate(yaml_pipelines, 1):
        name = pipeline["name"]
        file_path = project_path / pipeline["file_path"]
        console.print(f"[dim]Documenting [{i}/{total}] {name}...[/dim]")

        try:
            context = _gather_pipeline_context(pipeline, project_path, conn)
            prompt = _DOC_PROMPT.format(**context)

            response = llm.invoke(prompt)
            response_text = response.content if hasattr(response, "content") else str(response)

            parsed = _parse_llm_response(response_text)
            if not parsed:
                console.print(f"  [yellow]Could not parse LLM response for {name}[/yellow]")
                failed += 1
                continue

            description = parsed.get("description", "")
            column_descriptions = parsed.get("column_descriptions", {})

            changed, message = _update_yaml_file(
                file_path, description, column_descriptions,
                force=force, dry_run=dry_run,
            )

            if changed:
                prefix = "[DRY RUN] " if dry_run else ""
                console.print(f"  [green]{prefix}Updated {name}[/green]")
                if message:
                    console.print(f"  [dim]{message}[/dim]")
                documented += 1
            else:
                console.print(f"  [dim]{message}[/dim]")
                skipped += 1

        except Exception as e:
            console.print(f"  [red]Error: {e}[/red]")
            failed += 1

    # Summary
    console.print()
    console.print(f"[bold]Documentation complete:[/bold]")
    console.print(f"  Documented: {documented}")
    console.print(f"  Skipped: {skipped}")
    console.print(f"  Failed: {failed}")

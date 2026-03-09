"""Report exposure utilities for Seeknal Ask.

Consolidated module providing:
- save_rendered_markdown(): Save agent's analysis to target/reported/{name}/{date}.md
- load_report_exposure(): Load and validate report exposure YAML specs
- resolve_prompt(): Resolve Jinja2 template variables in exposure prompts
"""

import re
import warnings
from datetime import date
from pathlib import Path
from typing import Any

import yaml


def save_rendered_markdown(project_path: Path, name: str, content: str) -> Path:
    """Save rendered markdown analysis to target/reported/{slug}/{date}.md.

    Args:
        project_path: Path to the seeknal project root.
        name: Report name (will be slugified).
        content: Markdown content to save.

    Returns:
        Path to the written file.

    Raises:
        ValueError: If content is empty or path traversal detected.
    """
    if not content or not content.strip():
        raise ValueError("Cannot save empty report content.")

    slug = _slugify(name)
    today = date.today().isoformat()
    output_dir = project_path / "target" / "reported" / slug
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{today}.md"

    # Validate path is under target/
    resolved = output_path.resolve()
    target_dir = (project_path / "target").resolve()
    if not str(resolved).startswith(str(target_dir)):
        raise ValueError(f"Path traversal detected: {output_path}")

    output_path.write_text(content, encoding="utf-8")
    return output_path


def load_report_exposure(project_path: Path, name: str) -> dict[str, Any]:
    """Load a report exposure YAML by name.

    Searches seeknal/exposures/{name}.yml first, then scans seeknal/**/*.yml
    for a matching kind=exposure, type=report node.

    Args:
        project_path: Path to the seeknal project root.
        name: Exposure name to find.

    Returns:
        Validated config dict.

    Raises:
        FileNotFoundError: If no matching exposure found.
        ValueError: If YAML is invalid or fails schema validation.
    """
    # Direct path first
    direct = project_path / "seeknal" / "exposures" / f"{name}.yml"
    if direct.exists():
        return _load_and_validate(direct)

    # Scan all YAML files under seeknal/
    seeknal_dir = project_path / "seeknal"
    if seeknal_dir.exists():
        for yml_path in seeknal_dir.rglob("*.yml"):
            try:
                with open(yml_path, encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                if (
                    isinstance(data, dict)
                    and data.get("kind") == "exposure"
                    and data.get("type") == "report"
                    and data.get("name") == name
                ):
                    return _validate_report_exposure(data, yml_path)
            except (yaml.YAMLError, OSError):
                continue

    raise FileNotFoundError(
        f"Report exposure '{name}' not found. "
        f"Looked in seeknal/exposures/{name}.yml and seeknal/**/*.yml."
    )


def resolve_prompt(
    template: str,
    project_path: Path,
    inputs: list[dict[str, str]],
    params: dict[str, Any],
) -> str:
    """Resolve Jinja2 template variables in a report exposure prompt.

    Uses SandboxedEnvironment to prevent SSTI attacks.

    Supported variables:
        {{ run_date }}                     → current date (YYYY-MM-DD)
        {{ inputs.table_name.columns }}    → comma-separated column names
        {{ inputs.table_name.row_count }}  → integer row count
        {{ params.custom_key }}            → user-defined params from YAML

    Args:
        template: Jinja2 template string.
        project_path: Path to the seeknal project root.
        inputs: List of input ref dicts, e.g. [{"ref": "transform.revenue"}].
        params: User-defined parameters from YAML.

    Returns:
        Resolved prompt string.
    """
    import jinja2
    from jinja2.sandbox import SandboxedEnvironment

    env = SandboxedEnvironment(undefined=jinja2.StrictUndefined)
    tmpl = env.from_string(template)

    context = {
        "run_date": date.today().isoformat(),
        "inputs": _build_input_context(project_path, inputs),
        "params": params,
    }
    return tmpl.render(context)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _slugify(name: str) -> str:
    """Convert name to filesystem-safe slug.

    Lowercases, replaces non-alphanumeric with hyphens,
    collapses consecutive hyphens, strips leading/trailing hyphens.
    """
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug[:60] if slug else "report"


_REPORT_NAME_RE = re.compile(r"^[a-z0-9_]+$")

_VALID_FORMATS = frozenset({"markdown", "html", "both"})


def _load_and_validate(path: Path) -> dict[str, Any]:
    """Load YAML file and validate as report exposure."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return _validate_report_exposure(data, path)


def _validate_report_exposure(data: Any, file_path: Path) -> dict[str, Any]:
    """Validate report exposure YAML schema."""
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML in {file_path}: expected a mapping.")
    if data.get("kind") != "exposure":
        raise ValueError(
            f"{file_path}: kind must be 'exposure', got '{data.get('kind')}'"
        )
    if data.get("type") != "report":
        raise ValueError(
            f"{file_path}: type must be 'report', got '{data.get('type')}'"
        )

    params = data.get("params", {})
    if not isinstance(params, dict):
        params = {}

    has_sections = bool(data.get("sections"))

    # params.prompt is required unless sections are defined
    if not has_sections and not params.get("prompt"):
        raise ValueError(
            f"{file_path}: params.prompt is required for report exposures "
            "without sections."
        )

    output_format = params.get("format", "markdown")
    if output_format not in _VALID_FORMATS:
        raise ValueError(
            f"{file_path}: params.format must be one of "
            f"{sorted(_VALID_FORMATS)}, got '{output_format}'"
        )

    data["_file_path"] = str(file_path)
    return data


def _build_input_context(
    project_path: Path, inputs: list[dict[str, str]]
) -> dict[str, Any]:
    """Build template context for input refs.

    For each input ref, provides .columns (comma-separated) and .row_count.
    Opens a temporary in-memory DuckDB connection for schema inspection.
    """
    context: dict[str, Any] = {}
    if not inputs:
        return context

    try:
        import duckdb
    except ImportError:
        warnings.warn("duckdb not available; input template variables unresolved.")
        for inp in inputs:
            ref = inp.get("ref", "")
            if "." in ref:
                key = ref.split(".", 1)[1]
                context[key] = {"columns": "[duckdb not available]", "row_count": 0}
        return context

    for inp in inputs:
        ref = inp.get("ref", "")
        if "." not in ref:
            continue
        kind, ref_name = ref.split(".", 1)
        parquet_path = (
            project_path / "target" / "intermediate" / f"{kind}_{ref_name}.parquet"
        )

        if parquet_path.exists():
            try:
                con = duckdb.connect(":memory:")
                pq = str(parquet_path)
                desc = con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{pq}')"
                ).fetchall()
                columns = ", ".join(row[0] for row in desc)
                count = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{pq}')"
                ).fetchone()[0]
                con.close()
                context[ref_name] = {"columns": columns, "row_count": count}
            except Exception as e:
                warnings.warn(f"Failed to inspect input '{ref}': {e}")
                context[ref_name] = {
                    "columns": "[error reading table]",
                    "row_count": 0,
                }
        else:
            warnings.warn(f"Input table not found: {parquet_path}")
            context[ref_name] = {"columns": "[table not found]", "row_count": 0}

    return context

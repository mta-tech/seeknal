"""Agent tool for saving analysis as a repeatable report exposure YAML.

The agent calls this tool after a successful interactive analysis when the
user wants to codify it as a repeatable report spec.  The tool writes a
``kind: exposure, type: report`` YAML file to ``seeknal/exposures/{name}.yml``.
"""

import json
import re
from pathlib import Path

import yaml
from langchain_core.tools import tool

_NAME_RE = re.compile(r"^[a-z0-9_]+$")
_VALID_FORMATS = {"markdown", "html", "both"}


@tool
def save_report_exposure(
    name: str,
    prompt: str,
    inputs: str,
    output_format: str = "markdown",
    schedule: str = "",
) -> str:
    """Save the current analysis as a repeatable report exposure YAML.

    Call this when the user wants to save an analysis as a regular scheduled
    report.  The YAML file is written to seeknal/exposures/{name}.yml and can
    be re-run with: seeknal ask report --exposure {name}

    Args:
        name: Report name in snake_case (e.g. "monthly_revenue_report").
        prompt: The analysis prompt distilled from the conversation.
        inputs: JSON array of input refs, e.g. '["transform.monthly_revenue"]'.
        output_format: Output format — "markdown", "html", or "both".
        schedule: Optional cron schedule (e.g. "0 8 * * MON" for every Monday 8am).
            When set, the exposure can be deployed to Prefect with:
            seeknal prefect deploy --exposure {name} --work-pool <pool>
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    return _do_save(name, prompt, inputs, output_format, ctx.project_path, schedule)


def _do_save(
    name: str,
    prompt: str,
    inputs_json: str,
    output_format: str,
    project_path: Path,
    schedule: str = "",
) -> str:
    """Core logic for saving report exposure YAML.  Separated for testability."""
    # Validate name (snake_case only, no path traversal)
    if not name or not _NAME_RE.match(name):
        return (
            f"Error: Invalid report name '{name}'. "
            "Use snake_case: lowercase letters, numbers, underscores only."
        )

    # Validate format
    if output_format not in _VALID_FORMATS:
        return (
            f"Error: output_format must be one of {sorted(_VALID_FORMATS)}, "
            f"got '{output_format}'"
        )

    # Validate prompt
    if not prompt or not prompt.strip():
        return "Error: prompt cannot be empty."

    # Parse inputs
    try:
        input_refs = json.loads(inputs_json)
        if not isinstance(input_refs, list):
            return "Error: inputs must be a JSON array of ref strings."
    except json.JSONDecodeError as e:
        return f"Error: invalid JSON in inputs: {e}"

    # Build YAML structure
    exposure_data = {
        "kind": "exposure",
        "name": name,
        "type": "report",
        "description": f"Auto-generated report exposure: {name}",
        "params": {
            "prompt": prompt,
            "format": output_format,
        },
        "inputs": [{"ref": ref} for ref in input_refs],
    }

    # Add schedule if provided
    if schedule and schedule.strip():
        cron_str = schedule.strip()
        cron_parts = cron_str.split()
        if len(cron_parts) != 5:
            return (
                f"Error: schedule must be a 5-field cron string "
                f"(minute hour day month weekday), got '{cron_str}'"
            )
        exposure_data["schedule"] = {"cron": cron_str}

    # Write YAML
    exposures_dir = project_path / "seeknal" / "exposures"
    exposures_dir.mkdir(parents=True, exist_ok=True)
    output_path = exposures_dir / f"{name}.yml"

    # Validate path is under project root
    resolved = output_path.resolve()
    project_resolved = project_path.resolve()
    if not str(resolved).startswith(str(project_resolved)):
        return "Error: path traversal detected in report name."

    overwrite_note = ""
    if output_path.exists():
        overwrite_note = " (overwriting existing file)"

    output_path.write_text(
        yaml.safe_dump(exposure_data, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )

    return (
        f"Report exposure saved to {output_path}{overwrite_note}\n"
        f"Run it with: seeknal ask report --exposure {name}"
    )

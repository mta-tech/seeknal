"""Save metric tool — persists an ad-hoc metric query as a YAML definition."""

import re

from langchain_core.tools import tool

_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _build_metric_dict(
    name: str,
    metric_type: str,
    measure: str,
    description: str,
    numerator: str,
    denominator: str,
) -> dict:
    """Build the metric YAML dict for the given type."""
    data: dict = {
        "kind": "metric",
        "name": name,
        "type": metric_type,
    }
    if description:
        data["description"] = description

    if metric_type == "simple":
        data["measure"] = measure
    elif metric_type == "ratio":
        data["numerator"] = numerator
        data["denominator"] = denominator

    return data


@tool
def save_metric(
    name: str,
    measure: str = "",
    description: str = "",
    metric_type: str = "simple",
    numerator: str = "",
    denominator: str = "",
    confirmed: bool = False,
) -> str:
    """Save an ad-hoc metric query as a permanent YAML metric definition.

    Creates a metric YAML file in seeknal/metrics/ that can be queried
    via query_metric in future sessions.

    IMPORTANT: You must set confirmed=True to proceed. Show the user
    what will be saved and get their confirmation first.

    Args:
        name: Metric name (alphanumeric and underscores only).
        measure: Measure name for simple metrics (e.g. 'total_revenue').
        description: Human-readable description of the metric.
        metric_type: Metric type: 'simple' or 'ratio'. Default 'simple'.
        numerator: Numerator measure name (required for ratio metrics).
        denominator: Denominator measure name (required for ratio metrics).
        confirmed: Must be True to actually write. Set to False to preview.
    """
    import yaml

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # ── Validate name ────────────────────────────────────────────────
    if not name or not _NAME_PATTERN.match(name):
        return (
            f"Invalid metric name '{name}'. "
            "Names must start with a letter or underscore and contain "
            "only alphanumeric characters and underscores."
        )

    # ── Validate type ────────────────────────────────────────────────
    if metric_type not in ("simple", "ratio"):
        return (
            f"Unsupported metric_type '{metric_type}'. "
            "Supported types: simple, ratio."
        )

    # ── Validate type-specific fields ────────────────────────────────
    if metric_type == "simple" and not measure:
        return "Simple metrics require a 'measure' parameter."

    if metric_type == "ratio":
        if not numerator:
            return "Ratio metrics require a 'numerator' parameter."
        if not denominator:
            return "Ratio metrics require a 'denominator' parameter."

    # ── Build YAML content ───────────────────────────────────────────
    metric_dict = _build_metric_dict(
        name=name,
        metric_type=metric_type,
        measure=measure,
        description=description,
        numerator=numerator,
        denominator=denominator,
    )
    yaml_content = yaml.safe_dump(metric_dict, default_flow_style=False, sort_keys=False)

    # ── Preview mode ─────────────────────────────────────────────────
    if not confirmed:
        return (
            f"Preview of metric YAML to be saved as "
            f"seeknal/metrics/{name}.yml:\n\n"
            f"```yaml\n{yaml_content}```\n\n"
            f"Call save_metric with confirmed=True to write this file."
        )

    # ── Check for duplicate names ────────────────────────────────────
    from seeknal.workflow.semantic.loader import load_semantic_layer

    _, existing_metrics = load_semantic_layer(ctx.project_path)
    existing_names = {m.name for m in existing_metrics}
    if name in existing_names:
        return (
            f"A metric named '{name}' already exists. "
            f"Choose a different name or remove the existing metric first."
        )

    # ── Write the file ───────────────────────────────────────────────
    metrics_dir = ctx.project_path / "seeknal" / "metrics"
    target_path = metrics_dir / f"{name}.yml"

    try:
        with ctx.fs_lock:
            metrics_dir.mkdir(parents=True, exist_ok=True)
            target_path.write_text(yaml_content)
    except OSError as e:
        return f"Failed to write metric file: {e}"

    return (
        f"Saved metric '{name}' to seeknal/metrics/{name}.yml\n\n"
        f"The metric is now available for query_metric in this and future sessions."
    )

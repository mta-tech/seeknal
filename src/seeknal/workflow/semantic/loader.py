"""Semantic layer loader — shared YAML loading for CLI and agent tools.

Loads SemanticModel and Metric definitions from a seeknal project directory.
Three sources in priority order:
1. Inline metrics from semantic model YAML (seeknal/semantic_models/*.yml)
2. Separate metric YAML files (seeknal/metrics/*.yml)
3. Auto-generated simple metrics from measures (Cube.js-style fallback)
"""

import warnings
from pathlib import Path

from seeknal.workflow.semantic.models import (
    Metric,
    MetricType,
    SemanticModel,
)


def load_semantic_layer(
    project_path: Path,
) -> tuple[list[SemanticModel], list[Metric]]:
    """Load all semantic models and metrics from a project.

    Args:
        project_path: Path to the seeknal project root.

    Returns:
        Tuple of (semantic_models, metrics). Both lists may be empty
        if no semantic YAML files exist.
    """
    import yaml

    # Load semantic models from seeknal/semantic_models/*.yml
    sm_dir = project_path / "seeknal" / "semantic_models"
    semantic_models: list[SemanticModel] = []
    if sm_dir.exists():
        for yml_path in sorted(sm_dir.glob("*.yml")):
            try:
                with open(yml_path, "r") as f:
                    data = yaml.safe_load(f) or {}
                if data.get("kind") == "semantic_model":
                    semantic_models.append(SemanticModel.from_dict(data))
            except Exception as e:
                warnings.warn(f"Skipping malformed semantic model {yml_path.name}: {e}")

    # Collect metrics from three sources (priority order)
    metric_list: list[Metric] = []
    seen_names: set[str] = set()

    # Source 1: Inline metrics from semantic models
    for sm in semantic_models:
        for m in sm.metrics:
            if m.name not in seen_names:
                metric_list.append(m)
                seen_names.add(m.name)

    # Source 2: Separate metric YAML files
    metrics_dir = project_path / "seeknal" / "metrics"
    if metrics_dir.exists():
        for yml_path in sorted(metrics_dir.glob("*.yml")):
            try:
                with open(yml_path, "r") as f:
                    for doc in yaml.safe_load_all(f):
                        if doc and doc.get("kind") == "metric":
                            m = Metric.from_dict(doc)
                            if m.name not in seen_names:
                                metric_list.append(m)
                                seen_names.add(m.name)
            except Exception as e:
                warnings.warn(f"Skipping malformed metric file {yml_path.name}: {e}")

    # Source 3: Auto-generate simple metrics from measures (Cube.js-style)
    for sm in semantic_models:
        for measure in sm.measures:
            if measure.name not in seen_names:
                metric_list.append(Metric(
                    name=measure.name,
                    type=MetricType.SIMPLE,
                    description=measure.description,
                    measure=measure.name,
                    aliases=list(measure.aliases),
                ))
                seen_names.add(measure.name)

    return semantic_models, metric_list

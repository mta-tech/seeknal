"""Loader for common configuration YAML files.

Reads seeknal/common/sources.yml, rules.yml, and transformations.yml,
validates them via Pydantic models, and returns a flat dictionary suitable
for template resolution in ParameterResolver.

Flat-key conventions:
  - sources:  ``{source_id}.{param_name}`` -> param value
  - rules:    ``rules.{rule_id}``          -> rule value expression
  - transforms: ``transforms.{transform_id}`` -> SQL snippet
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import yaml

from .models import RulesConfig, SourcesConfig, TransformationsConfig

logger = logging.getLogger(__name__)


def load_common_config(
    common_dir: Path,
    *,
    strict: bool = False,
) -> Optional[Dict[str, str]]:
    """Load and flatten all common configuration files.

    Args:
        common_dir: Path to the ``seeknal/common/`` directory.
        strict: If True, raise on validation errors instead of logging warnings.

    Returns:
        Flat dictionary mapping dotted keys to string values,
        or None if the common directory does not exist.

    Raises:
        ValueError: If *strict* is True and a YAML file fails validation.
    """
    if not common_dir.is_dir():
        return None

    flat: Dict[str, str] = {}

    # --- sources.yml ---
    sources_path = common_dir / "sources.yml"
    if sources_path.is_file():
        _load_sources(sources_path, flat, strict)

    # --- rules.yml ---
    rules_path = common_dir / "rules.yml"
    if rules_path.is_file():
        _load_rules(rules_path, flat, strict)

    # --- transformations.yml ---
    transforms_path = common_dir / "transformations.yml"
    if transforms_path.is_file():
        _load_transformations(transforms_path, flat, strict)

    if not flat:
        return None

    return flat


def _load_sources(
    path: Path, flat: Dict[str, str], strict: bool
) -> None:
    """Parse sources.yml and merge into *flat*."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not raw or not isinstance(raw, dict):
            return
        config = SourcesConfig(**raw)
    except Exception as exc:
        msg = f"Failed to parse {path}: {exc}"
        if strict:
            raise ValueError(msg) from exc
        logger.warning(msg)
        return

    for source in config.sources:
        # Each param becomes ``{source_id}.{param_name}``
        for param_name, param_value in source.params.items():
            flat[f"{source.id}.{param_name}"] = param_value
        # Also store the ref so users can look it up: ``{source_id}.ref``
        flat[f"{source.id}.ref"] = source.ref


def _load_rules(
    path: Path, flat: Dict[str, str], strict: bool
) -> None:
    """Parse rules.yml and merge into *flat*."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not raw or not isinstance(raw, dict):
            return
        config = RulesConfig(**raw)
    except Exception as exc:
        msg = f"Failed to parse {path}: {exc}"
        if strict:
            raise ValueError(msg) from exc
        logger.warning(msg)
        return

    for rule in config.rules:
        flat[f"rules.{rule.id}"] = rule.value


def _load_transformations(
    path: Path, flat: Dict[str, str], strict: bool
) -> None:
    """Parse transformations.yml and merge into *flat*."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not raw or not isinstance(raw, dict):
            return
        config = TransformationsConfig(**raw)
    except Exception as exc:
        msg = f"Failed to parse {path}: {exc}"
        if strict:
            raise ValueError(msg) from exc
        logger.warning(msg)
        return

    for t in config.transformations:
        flat[f"transforms.{t.id}"] = t.sql

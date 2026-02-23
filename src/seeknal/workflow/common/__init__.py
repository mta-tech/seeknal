"""Common configuration layer for reusable definitions.

Provides shared sources, rules, and transformations that can be
referenced in transform SQL via ``{{ dotted.key }}`` syntax.
"""

from .loader import load_common_config
from .models import (
    RuleDef,
    RulesConfig,
    SourceDef,
    SourcesConfig,
    TransformationDef,
    TransformationsConfig,
)

__all__ = [
    "load_common_config",
    "RuleDef",
    "RulesConfig",
    "SourceDef",
    "SourcesConfig",
    "TransformationDef",
    "TransformationsConfig",
]

"""Parameter resolution for YAML pipelines and Python scripts.

This module provides parameter substitution for dynamic values in YAML
configurations and Python scripts.

Basic Usage (YAML):
    ```yaml
    kind: source
    name: daily_events
    params:
      path: "data/{{today}}/*.parquet"
      api_key: "{{env:API_KEY}}"
      run_id: "{{run_id}}"
    ```
"""

from .resolver import ParameterResolver
from .helpers import get_param, list_params, has_param
from .functions import today, yesterday, month_start, year_start, env_var

__all__ = [
    "ParameterResolver",
    "get_param",
    "list_params",
    "has_param",
    "today",
    "yesterday",
    "month_start",
    "year_start",
    "env_var",
]

"""Helper functions for accessing resolved parameters in Python scripts.

Provides a clean, explicit API for parameter access with type conversion.
"""

import os
import re
import warnings
from typing import Any, Dict, Optional, Type, TypeVar

from .type_conversion import convert_to_bool, convert_to_type

T = TypeVar('T')

# Pattern for valid parameter names: alphanumeric with underscores, must start with letter or underscore
PARAM_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def get_param(
    name: str,
    default: Optional[Any] = None,
    param_type: Optional[Type[T]] = None,
) -> Any:
    """Get a resolved parameter value.

    Parameters are resolved from YAML and made available via environment
    variables with a SEEKNAL_PARAM_ prefix.

    Args:
        name: Parameter name (without prefix). Must be alphanumeric with
            underscores only, and must start with a letter or underscore.
        default: Default value if parameter not found
        param_type: Expected type for conversion (int, float, bool, str)

    Returns:
        Parameter value, converted to specified type if provided

    Raises:
        KeyError: If parameter not found and no default provided
        ValueError: If parameter name is invalid

    Examples:
        Get a string parameter:
        >>> run_date = get_param("run_date")

        Get with type conversion:
        >>> batch_size = get_param("batch_size", param_type=int)
        >>> ratio = get_param("ratio", param_type=float)

        Get with default:
        >>> region = get_param("region", default="us-east-1")

        Boolean conversion:
        >>> enabled = get_param("enabled", param_type=bool)
    """
    # Validate parameter name format
    if not PARAM_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid parameter name '{name}'. "
            f"Parameter names must be alphanumeric with underscores only, "
            f"and must start with a letter or underscore."
        )

    # Build environment variable name
    env_name = f"SEEKNAL_PARAM_{name.upper()}"

    # Check for system environment collision
    # Warn if parameter name conflicts with existing system environment variable
    # that is not a Seeknal parameter
    if env_name in os.environ:
        # This is expected - it's a Seeknal parameter
        pass
    else:
        # Check if the uppercase name might conflict with a system env var
        system_env_name = name.upper()
        if system_env_name in os.environ:
            warnings.warn(
                f"Parameter '{name}' may conflict with system environment variable "
                f"'{system_env_name}'. Consider using a different name to avoid confusion."
            )

    value = os.environ.get(env_name, default)

    if value is None:
        if default is not None:
            return default
        raise KeyError(
            f"Parameter '{name}' not found and no default provided. "
            f"Looking for environment variable: {env_name}"
        )

    # Type conversion
    if param_type is not None:
        return convert_to_type(value, param_type)

    return value


def list_params() -> Dict[str, str]:
    """List all available Seeknal parameters.

    Returns:
        Dictionary of parameter names to values (without prefix)

    Examples:
        >>> list_params()
        {'run_date': '2025-02-10', 'run_id': 'abc-123', 'batch_size': '100'}
    """
    params = {}
    prefix = "SEEKNAL_PARAM_"
    for key, value in os.environ.items():
        if key.startswith(prefix):
            param_name = key[len(prefix):].lower()
            params[param_name] = value
    return params


def has_param(name: str) -> bool:
    """Check if a parameter exists.

    Args:
        name: Parameter name (without prefix). Must be alphanumeric with
            underscores only, and must start with a letter or underscore.

    Returns:
        True if parameter exists, False otherwise

    Raises:
        ValueError: If parameter name is invalid

    Examples:
        >>> has_param("run_date")
        True
        >>> has_param("nonexistent")
        False
    """
    # Validate parameter name format
    if not PARAM_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid parameter name '{name}'. "
            f"Parameter names must be alphanumeric with underscores only, "
            f"and must start with a letter or underscore."
        )

    env_name = f"SEEKNAL_PARAM_{name.upper()}"
    return env_name in os.environ

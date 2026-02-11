"""Parameter resolution for YAML pipeline configuration.

Parses and resolves {{ }} parameter expressions in YAML node configs.
Supports CLI overrides and context-aware resolution.
"""

import re
import uuid
import warnings
from datetime import datetime
from typing import Any, Dict, Optional

from .functions import get_function, env_var
from .type_conversion import convert_to_bool

# Reserved parameter names that cannot be overridden by user parameters
RESERVED_PARAM_NAMES = {"run_id", "run_date", "project_id", "workspace_path"}


class ParameterResolver:
    """Resolve {{ }} parameters in YAML configurations.

    Supports:
    - Date/time functions: {{today}}, {{month_start}}, {{today(-1)}}
    - Environment variables: {{env:VAR|default}}
    - Runtime context: {{run_id}}, {{run_date}}, {{project_id}}
    - CLI overrides (passed via kwargs)

    Examples:
        >>> resolver = ParameterResolver(cli_overrides={"date": "2025-01-15"})
        >>> result = resolver.resolve({"path": "data/{{today}}/*.parquet"})
        >>> # result = {"path": "data/2025-01-15/*.parquet"}

        >>> resolver = ParameterResolver(run_id="custom-123")
        >>> result = resolver.resolve({"run_id": "{{run_id}}"})
        >>> # result = {"run_id": "custom-123"}
    """

    # Pattern to match {{ variable }} or {{ function(args) }}
    PARAM_PATTERN = re.compile(r'\{\{\s*([^}]+)\s*\}\}')

    def __init__(
        self,
        cli_overrides: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ):
        """Initialize resolver.

        Args:
            cli_overrides: Parameter values from CLI flags (take precedence)
            run_id: Custom run ID (or auto-generated UUID)
        """
        self.cli_overrides = cli_overrides or {}
        self.run_id = run_id or str(uuid.uuid4())
        self.run_date = datetime.now().isoformat()

        # Build context values (runtime parameters)
        self._context_values: Dict[str, str] = {
            "run_id": self.run_id,
            "run_date": self.run_date,
        }

        # Add Seeknal context values if available
        # Lazy import to avoid circular dependency
        try:
            import importlib
            ctx_module = importlib.import_module('seeknal.context')
            context_obj = getattr(ctx_module, 'context', None)
            if context_obj and hasattr(context_obj, 'project_id'):
                self._context_values["project_id"] = str(context_obj.project_id)
            if context_obj and hasattr(context_obj, 'workspace_path'):
                self._context_values["workspace_path"] = str(context_obj.workspace_path)
        except Exception:
            # Context may not be initialized yet
            pass

    def resolve(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve all parameters in a dictionary.

        Args:
            params: Dictionary with potential {{ }} expressions

        Returns:
            Dictionary with all {{ }} expressions resolved

        Examples:
            >>> resolver = ParameterResolver()
            >>> resolver.resolve({"date": "{{today}}"})
            {'date': '2025-02-10'}

            >>> resolver.resolve({"nested": {"value": "{{today}})"}})
            {'nested': {'value': '2025-02-10'}}

            >>> resolver.resolve({"items": ["{{today}}", "{{yesterday}})"})
            {'items': ['2025-02-10', '2025-02-09']}
        """
        if not params:
            return {}

        resolved = {}
        for key, value in params.items():
            # Check for parameter name collision with reserved system names
            if key in RESERVED_PARAM_NAMES:
                warnings.warn(
                    f"Parameter '{key}' collides with reserved system name. "
                    f"System value will take precedence. Use a different name."
                )

            # Check CLI override first (highest precedence)
            if key in self.cli_overrides:
                resolved[key] = self._type_convert(self.cli_overrides[key])
                continue

            # Resolve {{ }} expressions
            if isinstance(value, str):
                resolved[key] = self._resolve_string(value)
            elif isinstance(value, dict):
                resolved[key] = self.resolve(value)
            elif isinstance(value, list):
                resolved[key] = [self._resolve_item(v) for v in value]
            else:
                resolved[key] = value

        return resolved

    def _resolve_string(self, value: str) -> Any:
        """Resolve {{ }} expressions in a string.

        Args:
            value: String potentially containing {{ }} expressions

        Returns:
            Resolved value with type conversion applied

        Examples:
            >>> resolver._resolve_string("{{today}}")
            '2025-02-10'

            >>> resolver._resolve_string("prefix-{{today}}")
            'prefix-2025-02-10'

            >>> resolver._resolve_string("100")
            100  # Type converted to int
        """
        def replace_match(match):
            expr = match.group(1).strip()

            # Check for context values first (run_id, run_date, etc.)
            if expr in self._context_values:
                return self._context_values[expr]

            # Check for function call with args: func(arg1, arg2)
            if '(' in expr and expr.endswith(')'):
                return self._resolve_function(expr)

            # Check for simple function name
            func_info = get_function(expr)
            if func_info:
                func, _ = func_info
                return str(func())

            # Check for env:VAR|default syntax
            if expr.startswith('env:'):
                return self._resolve_env_var(expr[4:])

            # Return original if not resolved
            return match.group(0)

        result = self.PARAM_PATTERN.sub(replace_match, value)

        # Apply type conversion
        return self._type_convert(result)

    def _resolve_function(self, expr: str) -> str:
        """Resolve function call like today(-1) or env(VAR|default).

        Args:
            expr: Function expression like "today(-1)" or "month_start()"

        Returns:
            Resolved value as string

        Examples:
            >>> resolver._resolve_function("today(-1)")
            '2025-02-09'

            >>> resolver._resolve_function("month_start()")
            '2025-02-01'
        """
        # Parse: func_name(arg1, arg2, ...)
        func_match = re.match(r'(\w+)\((.*)\)', expr)
        if not func_match:
            return expr

        func_name = func_match.group(1)
        args_str = func_match.group(2).strip()

        # Parse arguments
        args = self._parse_args(args_str) if args_str else []

        # Call built-in function
        func_info = get_function(func_name)
        if func_info:
            func, _ = func_info
            return str(func(*args))

        return expr

    def _resolve_env_var(self, expr: str) -> str:
        """Resolve env:VAR|default syntax.

        Args:
            expr: Expression like "API_KEY" or "API_KEY|default_value"

        Returns:
            Environment variable value or default

        Examples:
            >>> resolver._resolve_env_var("HOME")
            '/home/user'

            >>> resolver._resolve_env_var("MISSING|default")
            'default'
        """
        parts = expr.split('|', 1)
        var_name = parts[0] if parts else None
        default = parts[1] if len(parts) > 1 else None

        if not var_name:
            return ""

        return env_var(var_name, default)

    def _parse_args(self, args_str: str) -> list:
        """Parse function arguments, handling numbers and negatives.

        Args:
            args_str: Comma-separated argument string

        Returns:
            List of parsed arguments with type conversion

        Examples:
            >>> resolver._parse_args("-1, 10, test")
            [-1, 10, 'test']

            >>> resolver._parse_args("")
            []
        """
        if not args_str:
            return []

        # Simple split by comma
        # Note: This doesn't handle quoted strings with commas
        # For MVP, we assume simple comma-separated values
        args = [a.strip() for a in args_str.split(',') if a.strip()]

        # Convert numeric arguments
        converted = []
        for arg in args:
            # Try integer first (handles negative numbers)
            try:
                converted.append(int(arg))
            except ValueError:
                try:
                    # Try float
                    converted.append(float(arg))
                except ValueError:
                    # Keep as string (remove quotes if present)
                    if (arg.startswith('"') and arg.endswith('"')) or \
                       (arg.startswith("'") and arg.endswith("'")):
                        converted.append(arg[1:-1])
                    else:
                        converted.append(arg)

        return converted

    def _resolve_item(self, item: Any) -> Any:
        """Resolve item in a list.

        Handles strings, dicts, and nested lists.
        """
        if isinstance(item, str):
            return self._resolve_string(item)
        elif isinstance(item, dict):
            return self.resolve(item)
        elif isinstance(item, list):
            return [self._resolve_item(i) for i in item]
        return item

    def _type_convert(self, value: str) -> Any:
        """Convert string to appropriate type.

        Handles boolean, integer, float, and keeps as string otherwise.

        Args:
            value: String value to convert

        Returns:
            Converted value (bool, int, float, or str)

        Examples:
            >>> resolver._type_convert("true")
            True

            >>> resolver._type_convert("100")
            100

            >>> resolver._type_convert("3.14")
            3.14

            >>> resolver._type_convert("hello")
            'hello'
        """
        # Boolean - use shared utility for consistency
        if value.lower() in ('true', 'false', '1', '0', 'yes', 'no', 'on', 'off'):
            return convert_to_bool(value)

        # Integer (handle negative numbers)
        try:
            if '.' not in value and 'e' not in value.lower():
                return int(value)
        except ValueError:
            pass

        # Float
        try:
            return float(value)
        except ValueError:
            pass

        # Keep as string
        return value

    def get_resolved_params(self) -> Dict[str, str]:
        """Get all runtime context parameters.

        Returns:
            Dictionary of runtime parameter names to values

        Examples:
            >>> resolver.get_resolved_params()
            {'run_id': 'abc-123', 'run_date': '2025-02-10T10:30:00'}
        """
        return self._context_values.copy()

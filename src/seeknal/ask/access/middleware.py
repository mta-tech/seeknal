"""Role-gated tool wrapping.

Wrap each tool function at ``create_ask_toolset()`` time. The wrapped tool
preserves the original signature so pydantic-ai sees the same parameter
schema as the unwrapped function.

Exception hierarchy:
- ``AccessError``           — base; **NOT** a ``RuntimeError`` subclass, so a
  broad ``except RuntimeError`` elsewhere can't silently swallow access
  failures.
- ``MissingToolRoleError``  — raised at import / toolset-construction time
  for any tool missing from the registry (fail-closed).
- ``InsufficientRoleError`` — raised at call time when the caller's role is
  below the tool's ``min_role``.
"""

from __future__ import annotations

import inspect
import logging
from functools import wraps
from typing import Any, Awaitable, Callable, Optional

from seeknal.ask.access.registry import tool_min_role
from seeknal.ask.access.roles import Role


access_logger = logging.getLogger("seeknal.access")


class AccessError(Exception):
    """Base class for access-control failures. Not a RuntimeError."""


class MissingToolRoleError(AccessError):
    """A tool was wrapped without a min_role registration."""

    def __init__(self, tool: str):
        super().__init__(
            f"Tool {tool!r} has no min_role registration. "
            "Refusing to wrap (fail closed)."
        )
        self.tool = tool


class InsufficientRoleError(AccessError):
    """Caller role rank < required tool role rank."""

    def __init__(self, tool: str, required: Role, actual: Optional[Role]):
        actual_label = actual.label() if actual is not None else "none"
        super().__init__(
            f"Tool {tool!r} requires role {required.label()!r}; "
            f"caller has role {actual_label!r}."
        )
        self.tool = tool
        self.required = required
        self.actual = actual


def _resolve_role(actual: Optional[Role]) -> Optional[Role]:
    """Lookup helper: tolerate string-typed role values for forward compat."""
    if actual is None:
        return None
    if isinstance(actual, Role):
        return actual
    if isinstance(actual, str):
        try:
            return Role.from_string(actual)
        except ValueError:
            return None
    if isinstance(actual, int):
        try:
            return Role(actual)
        except ValueError:
            return None
    return None


def _emit_denial(
    tool: str,
    required: Role,
    actual: Optional[Role],
    user_id: Optional[int],
) -> None:
    """Structured log entry for AC15. Keys are the contract."""
    access_logger.info(
        "role_denied",
        extra={
            "event": "role_denied",
            "user_id": user_id,
            "tool": tool,
            "required": required.label(),
            "actual": actual.label() if actual is not None else None,
        },
    )


def make_role_gated_tool(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a tool function so its declared ``min_role`` is enforced.

    Reads ``requestor_role`` from the active ``ToolContext`` (per-task
    ContextVar) at call time. If the role is missing or insufficient, raises
    :class:`InsufficientRoleError`. The original tool signature is preserved
    so pydantic-ai's tool-schema introspection still works.
    """
    name = fn.__name__
    required = tool_min_role.get(name)
    if required is None:
        raise MissingToolRoleError(name)

    is_coroutine = inspect.iscoroutinefunction(fn)

    if is_coroutine:

        @wraps(fn)
        async def async_wrapper(*args, **kwargs):
            _enforce_role(name, required)
            return await fn(*args, **kwargs)

        async_wrapper.__min_role__ = required  # type: ignore[attr-defined]
        return async_wrapper

    @wraps(fn)
    def sync_wrapper(*args, **kwargs):
        _enforce_role(name, required)
        return fn(*args, **kwargs)

    sync_wrapper.__min_role__ = required  # type: ignore[attr-defined]
    return sync_wrapper


def _enforce_role(tool_name: str, required: Role) -> None:
    """Look up caller role; raise InsufficientRoleError on mismatch."""
    from seeknal.ask.agents.tools._context import (  # local import — break cycle
        get_tool_context,
    )

    try:
        ctx = get_tool_context()
    except Exception:
        ctx = None

    actual_raw = getattr(ctx, "requestor_role", None) if ctx is not None else None
    actual = _resolve_role(actual_raw)
    user_id = None
    if ctx is not None:
        for attr in ("telegram_user_id", "user_id", "_user_id"):
            value = getattr(ctx, attr, None)
            if isinstance(value, int):
                user_id = value
                break

    if actual is None or not actual.can(required):
        _emit_denial(tool_name, required, actual, user_id)
        raise InsufficientRoleError(
            tool=tool_name, required=required, actual=actual
        )


__all__ = [
    "AccessError",
    "InsufficientRoleError",
    "MissingToolRoleError",
    "make_role_gated_tool",
]

"""Tool-role registry contract tests."""

from __future__ import annotations

import inspect

import pytest

from seeknal.ask.access.registry import (
    register_tool_role,
    tool_min_role,
)
from seeknal.ask.access.roles import Role


def test_registry_has_entries():
    assert len(tool_min_role) > 0


def test_every_role_has_at_least_one_tool():
    """Sanity: each role should map to at least one tool."""
    roles_used = set(tool_min_role.values())
    # VIEWER, ANALYST, OPERATOR, ENGINEER must each appear.
    assert Role.VIEWER in roles_used
    assert Role.ANALYST in roles_used
    assert Role.OPERATOR in roles_used
    assert Role.ENGINEER in roles_used


def test_registry_keys_are_strings():
    for key, role in tool_min_role.items():
        assert isinstance(key, str)
        assert isinstance(role, Role)


def test_register_overwrite_guard():
    with pytest.raises(ValueError, match="already registered"):
        register_tool_role("list_tables", Role.ADMIN)


def test_register_overwrite_allowed():
    original = tool_min_role.get("execute_sql", Role.VIEWER)
    register_tool_role("execute_sql", Role.ADMIN, overwrite=True)
    assert tool_min_role["execute_sql"] == Role.ADMIN
    # restore
    register_tool_role("execute_sql", original, overwrite=True)


def test_register_new_tool():
    name = "_test_tool_synthetic_x"
    register_tool_role(name, Role.OPERATOR)
    assert tool_min_role[name] == Role.OPERATOR


def test_all_tools_have_role():
    """Every tool importable from seeknal.ask.agents.tools.toolset (in the
    aggregated tool lists) must have an entry in ``tool_min_role`` —
    fail-closed contract per AC15.
    """
    from seeknal.ask.agents.tools import toolset as toolset_module

    seen: set[str] = set()
    # toolset.py defines several _CONSTANT lists of tool functions.
    for attr_name in dir(toolset_module):
        if not attr_name.startswith("_"):
            continue
        attr = getattr(toolset_module, attr_name, None)
        if not isinstance(attr, list):
            continue
        for item in attr:
            if callable(item):
                seen.add(item.__name__)

    # ask_user is also added in the wrapper.
    seen.add("ask_user")

    missing = sorted(seen - set(tool_min_role.keys()))
    assert not missing, (
        f"Tools missing role registration: {missing}. "
        "Add them to src/seeknal/ask/access/registry.py."
    )


def test_iter_returns_sorted():
    from seeknal.ask.access.registry import iter_tool_min_roles

    items = list(iter_tool_min_roles())
    names = [n for n, _ in items]
    assert names == sorted(names)

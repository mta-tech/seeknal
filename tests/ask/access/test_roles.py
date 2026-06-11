"""Role IntEnum hierarchy tests (AC18)."""

from __future__ import annotations

import pytest

from seeknal.ask.access.roles import Role


def test_role_ordering():
    assert Role.VIEWER < Role.ANALYST < Role.OPERATOR < Role.ENGINEER < Role.ADMIN


def test_inheritance():
    """A higher role can do everything a lower role can (AC18)."""
    assert Role.ADMIN.can(Role.VIEWER)
    assert Role.ADMIN.can(Role.OPERATOR)
    assert Role.ENGINEER.can(Role.OPERATOR)
    assert Role.OPERATOR.can(Role.ANALYST)
    assert Role.ANALYST.can(Role.VIEWER)
    # Reverse should fail
    assert not Role.VIEWER.can(Role.ANALYST)
    assert not Role.OPERATOR.can(Role.ENGINEER)


def test_role_can_self():
    assert Role.OPERATOR.can(Role.OPERATOR)


def test_from_string_case_insensitive():
    assert Role.from_string("viewer") == Role.VIEWER
    assert Role.from_string("ADMIN") == Role.ADMIN
    assert Role.from_string("Operator") == Role.OPERATOR


def test_from_string_unknown():
    with pytest.raises(ValueError, match="Unknown role"):
        Role.from_string("god_mode")


def test_label_lowercase():
    assert Role.VIEWER.label() == "viewer"
    assert Role.ADMIN.label() == "admin"

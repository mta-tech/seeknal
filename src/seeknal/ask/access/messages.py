"""Canonical user-facing strings for the role-gated access surface.

Kept in one module so the prose can be tweaked / localized centrally.
Assertion contracts in tests check structured log fields, NOT this prose.
"""

from __future__ import annotations

from seeknal.ask.access.roles import Role


def role_denied_reply(tool: str, required: Role, actual: Role | None) -> str:
    actual_label = actual.label() if actual is not None else "none"
    return (
        f"I can't run `{tool}` with your current role (`{actual_label}`). "
        f"This requires `{required.label()}`. "
        "Ask the admin to upgrade your access."
    )


__all__ = ["role_denied_reply"]

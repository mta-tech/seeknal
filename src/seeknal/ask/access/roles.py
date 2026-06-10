"""Role hierarchy — integer-rank IntEnum.

Hierarchy: ``viewer < analyst < operator < engineer < admin``. Higher roles
inherit every lower role's capabilities (integer comparison, not duplicated
capability lists).
"""

from __future__ import annotations

from enum import IntEnum


class Role(IntEnum):
    VIEWER = 1
    ANALYST = 2
    OPERATOR = 3
    ENGINEER = 4
    ADMIN = 5

    def can(self, required: "Role") -> bool:
        """True iff this role rank ≥ ``required`` rank."""
        return self.value >= required.value

    @classmethod
    def from_string(cls, name: str) -> "Role":
        """Parse a role name (case-insensitive). Raises ValueError on unknown."""
        try:
            return cls[name.upper()]
        except KeyError as exc:
            raise ValueError(
                f"Unknown role {name!r}. "
                "Valid: viewer, analyst, operator, engineer, admin"
            ) from exc

    def label(self) -> str:
        return self.name.lower()


__all__ = ["Role"]

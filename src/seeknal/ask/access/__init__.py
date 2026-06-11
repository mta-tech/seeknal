"""Role-gated access control for the Ask agent (v0.5).

Pieces:
- ``roles``     — ``Role`` IntEnum + rank comparison
- ``registry``  — ``tool_min_role`` dict (every Ask tool → its required role)
- ``messages``  — canonical user-facing reply strings (localizable)
- ``middleware``— exception types + ``make_role_gated_tool`` decorator
- ``policy``    — ``.seeknal/access.yml`` loader / resolver
- ``pending``   — ``.seeknal/access_pending.json`` queue (Step 15)

Imports here are kept lightweight so the module is safe to import at startup.
"""

from seeknal.ask.access.roles import Role
from seeknal.ask.access.registry import (
    tool_min_role,
    iter_tool_min_roles,
    register_tool_role,
)
from seeknal.ask.access.messages import role_denied_reply

__all__ = [
    "Role",
    "tool_min_role",
    "iter_tool_min_roles",
    "register_tool_role",
    "role_denied_reply",
    "default_local_role",
]


def default_local_role() -> "Role":
    """Default role for non-Telegram callers (CLI, programmatic Ask sessions).

    Local CLI implies filesystem ownership of the project → ADMIN. The HTTP
    gateway path must NOT rely on this default; it must supply the role
    explicitly.
    """
    return Role.ADMIN

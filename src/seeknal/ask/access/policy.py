"""Access policy loader for ``.seeknal/access.yml``.

Schema (validated at load time):

    schema_version: 1
    default_role: none            # or viewer/analyst/operator/engineer/admin
    allowlist:
      - telegram_user_id: 123456
        telegram_username: alice  # display only; not authoritative
        role: viewer
        notes: "CEO"
        added_at: 2026-05-15T10:00:00Z
        added_by: founder
      - telegram_chat_id: -100123456
        role: viewer
        notes: "Leadership group"
    deny_list:
      - telegram_user_id: 987654
        reason: "removed 2026-04-01"

``user_id`` is the security primary key. ``username`` is display only and
must never be used for security decisions.

``AccessPolicy.resolve(user_id, username, chat_id) -> Role | None``:
    1. ``deny_list[user_id]`` → ``None`` (no access).
    2. ``allowlist[user_id]`` → role.
    3. ``allowlist[chat_id]`` → group role.
    4. ``default_role`` (returns ``None`` for ``"none"``).

Atomic save via tempfile + os.replace. Path security validated via
``is_insecure_path``.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from seeknal.ask.access.roles import Role
from seeknal.utils.path_security import is_insecure_path

logger = logging.getLogger("seeknal.access.policy")


@dataclass
class AllowlistEntry:
    telegram_user_id: Optional[int] = None
    telegram_chat_id: Optional[int] = None
    telegram_username: Optional[str] = None
    role: Optional[Role] = None
    notes: str = ""
    added_at: str = ""
    added_by: str = ""

    def to_yaml(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.telegram_user_id is not None:
            data["telegram_user_id"] = self.telegram_user_id
        if self.telegram_chat_id is not None:
            data["telegram_chat_id"] = self.telegram_chat_id
        if self.telegram_username:
            data["telegram_username"] = self.telegram_username
        if self.role is not None:
            data["role"] = self.role.label()
        if self.notes:
            data["notes"] = self.notes
        if self.added_at:
            data["added_at"] = self.added_at
        if self.added_by:
            data["added_by"] = self.added_by
        return data


@dataclass
class DenyEntry:
    telegram_user_id: int
    reason: str = ""
    added_at: str = ""

    def to_yaml(self) -> dict[str, Any]:
        data: dict[str, Any] = {"telegram_user_id": self.telegram_user_id}
        if self.reason:
            data["reason"] = self.reason
        if self.added_at:
            data["added_at"] = self.added_at
        return data


@dataclass
class AccessPolicy:
    schema_version: int = 1
    default_role: Optional[Role] = None
    allowlist: list[AllowlistEntry] = field(default_factory=list)
    deny_list: list[DenyEntry] = field(default_factory=list)
    source_path: Optional[Path] = None

    @classmethod
    def empty(cls, source_path: Optional[Path] = None) -> "AccessPolicy":
        return cls(source_path=source_path)

    @classmethod
    def load(cls, path: Path) -> "AccessPolicy":
        path = Path(path).expanduser()
        if is_insecure_path(str(path)):
            raise ValueError(
                f"Insecure access policy path: {path!s}. "
                "Use .seeknal/access.yml inside the project."
            )
        if not path.exists():
            return cls.empty(source_path=path)
        with open(path, "r") as fh:
            raw = yaml.safe_load(fh) or {}
        if not isinstance(raw, dict):
            raise ValueError(
                f"access.yml at {path} did not parse to a mapping."
            )

        policy = cls(source_path=path)
        policy.schema_version = int(raw.get("schema_version", 1))

        default = raw.get("default_role")
        if default and str(default).lower() != "none":
            policy.default_role = Role.from_string(str(default))

        for entry in raw.get("allowlist", []) or []:
            if not isinstance(entry, dict):
                continue
            role_raw = entry.get("role")
            role = Role.from_string(str(role_raw)) if role_raw else None
            policy.allowlist.append(
                AllowlistEntry(
                    telegram_user_id=entry.get("telegram_user_id"),
                    telegram_chat_id=entry.get("telegram_chat_id"),
                    telegram_username=entry.get("telegram_username"),
                    role=role,
                    notes=entry.get("notes", "") or "",
                    added_at=entry.get("added_at", "") or "",
                    added_by=entry.get("added_by", "") or "",
                )
            )
        for entry in raw.get("deny_list", []) or []:
            if not isinstance(entry, dict):
                continue
            user_id = entry.get("telegram_user_id")
            if not isinstance(user_id, int):
                continue
            policy.deny_list.append(
                DenyEntry(
                    telegram_user_id=user_id,
                    reason=entry.get("reason", "") or "",
                    added_at=entry.get("added_at", "") or "",
                )
            )
        return policy

    def save(self, path: Optional[Path] = None) -> Path:
        target = Path(path or self.source_path)
        if is_insecure_path(str(target)):
            raise ValueError(
                f"Insecure save path: {target!s}. "
                "Use a project-local .seeknal/ path."
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "default_role": self.default_role.label() if self.default_role else "none",
            "allowlist": [entry.to_yaml() for entry in self.allowlist],
            "deny_list": [entry.to_yaml() for entry in self.deny_list],
        }
        fd, tmp_path = tempfile.mkstemp(
            prefix=".access.", suffix=".yml.tmp", dir=str(target.parent)
        )
        try:
            with os.fdopen(fd, "w") as fh:
                yaml.safe_dump(payload, fh, sort_keys=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, target)
        except Exception:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise
        self.source_path = target
        return target

    def resolve(
        self,
        user_id: int,
        username: Optional[str] = None,
        chat_id: Optional[int] = None,
    ) -> Optional[Role]:
        """Resolve a caller's effective role. ``None`` means no access."""
        del username  # display-only; never used for security decisions.

        for entry in self.deny_list:
            if entry.telegram_user_id == user_id:
                return None

        for entry in self.allowlist:
            if entry.role is None:
                continue
            if entry.telegram_user_id is not None and entry.telegram_user_id == user_id:
                return entry.role

        if chat_id is not None:
            for entry in self.allowlist:
                if entry.role is None:
                    continue
                if (
                    entry.telegram_chat_id is not None
                    and entry.telegram_chat_id == chat_id
                ):
                    return entry.role

        return self.default_role

    def grant(
        self,
        *,
        telegram_user_id: Optional[int] = None,
        telegram_chat_id: Optional[int] = None,
        role: Role,
        username: Optional[str] = None,
        notes: str = "",
        added_by: str = "",
        added_at: str = "",
    ) -> AllowlistEntry:
        """Add or replace an allowlist entry."""
        if telegram_user_id is None and telegram_chat_id is None:
            raise ValueError(
                "grant() requires telegram_user_id or telegram_chat_id."
            )
        # Remove any prior entry on the same primary key.
        self.allowlist = [
            e
            for e in self.allowlist
            if e.telegram_user_id != telegram_user_id
            or e.telegram_chat_id != telegram_chat_id
        ]
        entry = AllowlistEntry(
            telegram_user_id=telegram_user_id,
            telegram_chat_id=telegram_chat_id,
            telegram_username=username,
            role=role,
            notes=notes,
            added_at=added_at,
            added_by=added_by,
        )
        self.allowlist.append(entry)
        return entry

    def revoke(self, telegram_user_id: int, *, deny_reason: Optional[str] = None) -> bool:
        """Remove a user from the allowlist; optionally add to deny_list."""
        before = len(self.allowlist)
        self.allowlist = [
            e for e in self.allowlist if e.telegram_user_id != telegram_user_id
        ]
        removed = len(self.allowlist) < before
        if deny_reason is not None:
            self.deny_list = [
                e for e in self.deny_list if e.telegram_user_id != telegram_user_id
            ]
            self.deny_list.append(
                DenyEntry(telegram_user_id=telegram_user_id, reason=deny_reason)
            )
        return removed


__all__ = ["AccessPolicy", "AllowlistEntry", "DenyEntry"]

"""Pending approval queue for unlisted users (Step 15).

Stored at ``.seeknal/access_pending.json``. TTL default 30 days. Best-effort
cleanup of expired entries runs at start of each tick (orchestrated by the
caller, not enforced here).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from seeknal.utils.path_security import is_insecure_path

logger = logging.getLogger("seeknal.access.pending")

DEFAULT_TTL_DAYS = 30


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@dataclass
class PendingRequest:
    telegram_user_id: int
    telegram_username: Optional[str] = None
    intro_message: str = ""
    requested_at: str = ""
    ttl_until: str = ""
    notified_admins: list[int] = field(default_factory=list)


@dataclass
class PendingQueue:
    requests: list[PendingRequest] = field(default_factory=list)
    source_path: Optional[Path] = None

    @classmethod
    def load(cls, path: Path) -> "PendingQueue":
        path = Path(path).expanduser()
        if is_insecure_path(str(path)):
            raise ValueError(
                f"Insecure pending-queue path: {path!s}. "
                "Use .seeknal/access_pending.json inside the project."
            )
        if not path.exists():
            return cls(source_path=path)
        try:
            raw = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Pending queue at %s is corrupt (%s); starting empty.", path, exc)
            return cls(source_path=path)
        if not isinstance(raw, list):
            return cls(source_path=path)
        queue = cls(source_path=path)
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            user_id = entry.get("telegram_user_id")
            if not isinstance(user_id, int):
                continue
            queue.requests.append(
                PendingRequest(
                    telegram_user_id=user_id,
                    telegram_username=entry.get("telegram_username"),
                    intro_message=entry.get("intro_message", "") or "",
                    requested_at=entry.get("requested_at", "") or "",
                    ttl_until=entry.get("ttl_until", "") or "",
                    notified_admins=list(entry.get("notified_admins") or []),
                )
            )
        return queue

    def save(self, path: Optional[Path] = None) -> Path:
        target = Path(path or self.source_path)
        if is_insecure_path(str(target)):
            raise ValueError(f"Insecure pending-queue save path: {target!s}.")
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "telegram_user_id": req.telegram_user_id,
                "telegram_username": req.telegram_username,
                "intro_message": req.intro_message,
                "requested_at": req.requested_at,
                "ttl_until": req.ttl_until,
                "notified_admins": list(req.notified_admins),
            }
            for req in self.requests
        ]
        fd, tmp = tempfile.mkstemp(
            prefix=".access_pending.",
            suffix=".json.tmp",
            dir=str(target.parent),
        )
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except Exception:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        self.source_path = target
        return target

    def add(
        self,
        *,
        telegram_user_id: int,
        intro_message: str,
        telegram_username: Optional[str] = None,
        ttl_days: int = DEFAULT_TTL_DAYS,
    ) -> PendingRequest:
        # If already present, just refresh intro_message.
        for req in self.requests:
            if req.telegram_user_id == telegram_user_id:
                req.intro_message = intro_message
                req.telegram_username = telegram_username or req.telegram_username
                return req
        now = _utcnow()
        req = PendingRequest(
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
            intro_message=intro_message[:500],
            requested_at=_iso(now),
            ttl_until=_iso(now + timedelta(days=ttl_days)),
        )
        self.requests.append(req)
        return req

    def get(self, telegram_user_id: int) -> Optional[PendingRequest]:
        for req in self.requests:
            if req.telegram_user_id == telegram_user_id:
                return req
        return None

    def remove(self, telegram_user_id: int) -> bool:
        before = len(self.requests)
        self.requests = [
            r for r in self.requests if r.telegram_user_id != telegram_user_id
        ]
        return len(self.requests) < before

    def mark_notified(self, telegram_user_id: int, admin_id: int) -> None:
        req = self.get(telegram_user_id)
        if req is None:
            return
        if admin_id not in req.notified_admins:
            req.notified_admins.append(admin_id)

    def cleanup_expired(self, now: Optional[datetime] = None) -> int:
        now = now or _utcnow()
        keep: list[PendingRequest] = []
        dropped = 0
        for req in self.requests:
            ttl = _parse_iso(req.ttl_until)
            if ttl is None or ttl > now:
                keep.append(req)
            else:
                dropped += 1
        self.requests = keep
        return dropped


__all__ = ["PendingQueue", "PendingRequest", "DEFAULT_TTL_DAYS"]

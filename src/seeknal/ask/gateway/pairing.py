"""Pairing helpers for linking Telegram chats to gateway sessions."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from seeknal.ask.gateway.tenant import DEFAULT_TENANT
from seeknal.ask.sessions import generate_session_name

_CODE_RE = re.compile(r"[a-z0-9]+")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_pair_code(code: str) -> str:
    """Normalize user-entered pair codes."""
    parts = _CODE_RE.findall((code or "").lower())
    return "-".join(parts)


def generate_pair_code() -> str:
    """Generate a human-readable pairing code with enough entropy for invites."""
    return generate_session_name()


@dataclass(frozen=True)
class PairingRecord:
    code: str
    session_id: str
    tenant_id: str
    created_at: datetime
    expires_at: datetime
    used_at: datetime | None = None


class PairingError(Exception):
    """Base class for pairing failures."""


class PairCodeInvalidError(PairingError):
    """Pair code was not found."""


class PairCodeExpiredError(PairingError):
    """Pair code has expired."""


class PairCodeUsedError(PairingError):
    """Pair code has already been redeemed."""


class FilePairingStore:
    """Filesystem-backed pairing store shared between CLI and gateway."""

    def __init__(
        self,
        project_path: Path | None = None,
        *,
        base_dir: Path | None = None,
    ) -> None:
        if base_dir is not None:
            self._base = base_dir / "pairing"
        elif project_path is not None:
            self._base = project_path / ".seeknal" / "pairing"
        else:
            raise ValueError("FilePairingStore requires project_path or base_dir")
        self._base.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    def _tenant_dir(self, tenant_id: str) -> Path:
        tenant_dir = self._base / tenant_id
        tenant_dir.mkdir(parents=True, exist_ok=True)
        return tenant_dir

    def _record_path(self, tenant_id: str, code: str) -> Path:
        return self._tenant_dir(tenant_id) / f"{normalize_pair_code(code)}.json"

    async def create_pair_code(
        self,
        session_id: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
        ttl_seconds: int = 600,
    ) -> PairingRecord:
        async with self._lock:
            await self.cleanup_expired_codes()
            for _ in range(64):
                code = generate_pair_code()
                path = self._record_path(tenant_id, code)
                if not path.exists():
                    now = _utcnow()
                    record = PairingRecord(
                        code=code,
                        session_id=session_id,
                        tenant_id=tenant_id,
                        created_at=now,
                        expires_at=now + timedelta(seconds=ttl_seconds),
                    )
                    path.write_text(self._serialize(record), encoding="utf-8")
                    return record
            raise RuntimeError("Failed to allocate a unique file pair code after 64 attempts")

    async def redeem_pair_code(
        self,
        code: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> PairingRecord:
        async with self._lock:
            path = self._record_path(tenant_id, code)
            if not path.exists():
                raise PairCodeInvalidError("Pair code is invalid.")

            record = self._deserialize(path.read_text(encoding="utf-8"))
            if self._is_expired(record):
                path.unlink(missing_ok=True)
                raise PairCodeExpiredError("Pair code has expired.")
            if record.used_at is not None:
                raise PairCodeUsedError("Pair code has already been used.")

            updated = replace(record, used_at=_utcnow())
            path.write_text(self._serialize(updated), encoding="utf-8")
            return updated

    async def cleanup_expired_codes(self) -> int:
        removed = 0
        for tenant_dir in self._base.glob("*"):
            if not tenant_dir.is_dir():
                continue
            for path in tenant_dir.glob("*.json"):
                try:
                    record = self._deserialize(path.read_text(encoding="utf-8"))
                except Exception:
                    path.unlink(missing_ok=True)
                    removed += 1
                    continue
                if self._is_expired(record):
                    path.unlink(missing_ok=True)
                    removed += 1
        return removed

    @staticmethod
    def _is_expired(record: PairingRecord) -> bool:
        return record.expires_at <= _utcnow()

    @staticmethod
    def _serialize(record: PairingRecord) -> str:
        payload = asdict(record)
        for key in ("created_at", "expires_at", "used_at"):
            value = payload[key]
            payload[key] = value.isoformat() if value is not None else None
        return json.dumps(payload)

    @staticmethod
    def _deserialize(raw: str) -> PairingRecord:
        data = json.loads(raw)
        for key in ("created_at", "expires_at", "used_at"):
            if data.get(key):
                data[key] = datetime.fromisoformat(data[key])
            else:
                data[key] = None
        return PairingRecord(**data)


class InMemoryPairingStore:
    """Simple in-process pairing store for single-replica gateways."""

    def __init__(self) -> None:
        self._codes: dict[str, dict[str, PairingRecord]] = {}
        self._lock = asyncio.Lock()

    async def create_pair_code(
        self,
        session_id: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
        ttl_seconds: int = 600,
    ) -> PairingRecord:
        async with self._lock:
            self._cleanup_expired_locked()
            tenant_codes = self._codes.setdefault(tenant_id, {})

            for _ in range(64):
                code = generate_pair_code()
                record = tenant_codes.get(code)
                if record is None or self._is_expired(record):
                    now = _utcnow()
                    created = PairingRecord(
                        code=code,
                        session_id=session_id,
                        tenant_id=tenant_id,
                        created_at=now,
                        expires_at=now + timedelta(seconds=ttl_seconds),
                    )
                    tenant_codes[code] = created
                    return created

            raise RuntimeError("Failed to allocate a unique pair code after 64 attempts")

    async def redeem_pair_code(
        self,
        code: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> PairingRecord:
        normalized = normalize_pair_code(code)
        async with self._lock:
            tenant_codes = self._codes.setdefault(tenant_id, {})
            record = tenant_codes.get(normalized)
            if record is None:
                raise PairCodeInvalidError("Pair code is invalid.")
            if self._is_expired(record):
                del tenant_codes[normalized]
                raise PairCodeExpiredError("Pair code has expired.")
            if record.used_at is not None:
                raise PairCodeUsedError("Pair code has already been used.")

            updated = replace(record, used_at=_utcnow())
            tenant_codes[normalized] = updated
            return updated

    async def cleanup_expired_codes(self) -> int:
        async with self._lock:
            return self._cleanup_expired_locked()

    def _cleanup_expired_locked(self) -> int:
        removed = 0
        for tenant_id, codes in list(self._codes.items()):
            stale = [code for code, record in codes.items() if self._is_expired(record)]
            for code in stale:
                del codes[code]
                removed += 1
            if not codes:
                del self._codes[tenant_id]
        return removed

    @staticmethod
    def _is_expired(record: PairingRecord) -> bool:
        return record.expires_at <= _utcnow()


class TelegramLinkStore:
    """Filesystem-backed Telegram chat -> session mapping."""

    def __init__(
        self,
        project_path: Path | None = None,
        *,
        base_dir: Path | None = None,
    ) -> None:
        if base_dir is not None:
            self._base = base_dir / "telegram-links"
        elif project_path is not None:
            self._base = project_path / ".seeknal" / "telegram-links"
        else:
            raise ValueError("TelegramLinkStore requires project_path or base_dir")
        self._base.mkdir(parents=True, exist_ok=True)

    def _tenant_dir(self, tenant_id: str) -> Path:
        tenant_dir = self._base / tenant_id
        tenant_dir.mkdir(parents=True, exist_ok=True)
        return tenant_dir

    def _chat_path(self, tenant_id: str, chat_id: str) -> Path:
        return self._tenant_dir(tenant_id) / f"{chat_id}.json"

    def link_chat(
        self,
        chat_id: str,
        session_id: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> None:
        payload = {
            "chat_id": chat_id,
            "session_id": session_id,
            "tenant_id": tenant_id,
            "linked_at": _utcnow().isoformat(),
        }
        self._chat_path(tenant_id, chat_id).write_text(
            json.dumps(payload),
            encoding="utf-8",
        )

    def get_session_id(
        self,
        chat_id: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> str | None:
        path = self._chat_path(tenant_id, chat_id)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("session_id")

    def list_links(
        self,
        *,
        tenant_id: str = DEFAULT_TENANT,
        session_id: str | None = None,
    ) -> list[dict[str, str]]:
        """List paired Telegram chat/device ids for a tenant."""
        links: list[dict[str, str]] = []
        for path in sorted(self._tenant_dir(tenant_id).glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if session_id and data.get("session_id") != session_id:
                continue
            links.append(data)
        links.sort(key=lambda item: item.get("linked_at", ""), reverse=True)
        return links


class PublicSessionStore:
    """Filesystem-backed public session config for Telegram access."""

    def __init__(
        self,
        project_path: Path | None = None,
        *,
        base_dir: Path | None = None,
    ) -> None:
        if base_dir is not None:
            self._base = base_dir / "telegram-public"
        elif project_path is not None:
            self._base = project_path / ".seeknal" / "telegram-public"
        else:
            raise ValueError("PublicSessionStore requires project_path or base_dir")
        self._base.mkdir(parents=True, exist_ok=True)

    def _tenant_dir(self, tenant_id: str) -> Path:
        tenant_dir = self._base / tenant_id
        tenant_dir.mkdir(parents=True, exist_ok=True)
        return tenant_dir

    def _config_path(self, tenant_id: str) -> Path:
        return self._tenant_dir(tenant_id) / "public-session.json"

    def set_session(
        self,
        session_id: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> None:
        payload = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "updated_at": _utcnow().isoformat(),
        }
        self._config_path(tenant_id).write_text(
            json.dumps(payload),
            encoding="utf-8",
        )

    def get_session_id(
        self,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> str | None:
        path = self._config_path(tenant_id)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("session_id")

    def clear(
        self,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> bool:
        path = self._config_path(tenant_id)
        if not path.exists():
            return False
        path.unlink()
        return True


class RedisPairingStore:
    """Redis-backed pairing store for multi-replica gateways."""

    KEY_PREFIX = "seeknal:pair:"

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    def _key(self, tenant_id: str, code: str) -> str:
        if tenant_id == DEFAULT_TENANT:
            return f"{self.KEY_PREFIX}{code}"
        return f"{self.KEY_PREFIX}{tenant_id}:{code}"

    async def create_pair_code(
        self,
        session_id: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
        ttl_seconds: int = 600,
    ) -> PairingRecord:
        for _ in range(64):
            code = generate_pair_code()
            now = _utcnow()
            record = PairingRecord(
                code=code,
                session_id=session_id,
                tenant_id=tenant_id,
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
            )
            created = await self._redis.set(
                self._key(tenant_id, code),
                self._serialize(record),
                ex=ttl_seconds,
                nx=True,
            )
            if created:
                return record
        raise RuntimeError("Failed to allocate a unique Redis pair code after 64 attempts")

    async def redeem_pair_code(
        self,
        code: str,
        *,
        tenant_id: str = DEFAULT_TENANT,
    ) -> PairingRecord:
        normalized = normalize_pair_code(code)
        key = self._key(tenant_id, normalized)
        raw = await self._redis.get(key)
        if raw is None:
            raise PairCodeInvalidError("Pair code is invalid.")

        record = self._deserialize(raw)
        if record.expires_at <= _utcnow():
            await self._redis.delete(key)
            raise PairCodeExpiredError("Pair code has expired.")
        if record.used_at is not None:
            raise PairCodeUsedError("Pair code has already been used.")

        ttl = await self._redis.ttl(key)
        updated = replace(record, used_at=_utcnow())
        if ttl is None or ttl <= 0:
            ttl = max(1, int((record.expires_at - _utcnow()).total_seconds()))
        await self._redis.set(key, self._serialize(updated), ex=ttl)
        return updated

    async def cleanup_expired_codes(self) -> int:
        # Redis expiry handles cleanup for us.
        return 0

    @staticmethod
    def _serialize(record: PairingRecord) -> str:
        payload = asdict(record)
        for key in ("created_at", "expires_at", "used_at"):
            value = payload[key]
            payload[key] = value.isoformat() if value is not None else None
        return json.dumps(payload)

    @staticmethod
    def _deserialize(raw: Any) -> PairingRecord:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        for key in ("created_at", "expires_at", "used_at"):
            if data.get(key):
                data[key] = datetime.fromisoformat(data[key])
            else:
                data[key] = None
        return PairingRecord(**data)

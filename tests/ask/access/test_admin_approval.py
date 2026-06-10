"""Tests for the admin approval queue (Step 15, AC16)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from seeknal.ask.access.pending import (
    DEFAULT_TTL_DAYS,
    PendingQueue,
    PendingRequest,
)
from seeknal.ask.access.policy import AccessPolicy
from seeknal.ask.access.roles import Role


def test_load_empty(tmp_path: Path):
    queue = PendingQueue.load(tmp_path / ".seeknal/access_pending.json")
    assert queue.requests == []


def test_insecure_path():
    with pytest.raises(ValueError, match="Insecure"):
        PendingQueue.load(Path("/tmp/access_pending.json"))


def test_pending_flow(tmp_path: Path):
    """AC16: unlisted → pending → admin approval → policy updated."""
    path = tmp_path / ".seeknal/access_pending.json"
    queue = PendingQueue.load(path)
    queue.add(
        telegram_user_id=555,
        telegram_username="new_person",
        intro_message="Hi, I'm Carol from analytics — Bob said to message you.",
    )
    queue.save()

    loaded = PendingQueue.load(path)
    assert len(loaded.requests) == 1
    assert loaded.requests[0].telegram_user_id == 555
    assert "Carol" in loaded.requests[0].intro_message


def test_add_idempotent_for_same_user(tmp_path: Path):
    queue = PendingQueue.load(tmp_path / "p.json")
    queue.add(telegram_user_id=1, intro_message="first")
    queue.add(telegram_user_id=1, intro_message="second")
    assert len(queue.requests) == 1
    assert queue.requests[0].intro_message == "second"


def test_remove(tmp_path: Path):
    queue = PendingQueue.load(tmp_path / "p.json")
    queue.add(telegram_user_id=1, intro_message="x")
    assert queue.remove(1) is True
    assert queue.remove(1) is False
    assert queue.requests == []


def test_mark_notified(tmp_path: Path):
    queue = PendingQueue.load(tmp_path / "p.json")
    queue.add(telegram_user_id=1, intro_message="x")
    queue.mark_notified(1, admin_id=42)
    queue.mark_notified(1, admin_id=42)  # idempotent
    assert queue.requests[0].notified_admins == [42]


def test_ttl_cleanup(tmp_path: Path):
    queue = PendingQueue.load(tmp_path / "p.json")
    queue.add(telegram_user_id=1, intro_message="old")
    # Force ttl into the past
    queue.requests[0].ttl_until = (
        datetime.now(tz=timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    dropped = queue.cleanup_expired()
    assert dropped == 1
    assert queue.requests == []


def test_ttl_default_30_days(tmp_path: Path):
    queue = PendingQueue.load(tmp_path / "p.json")
    queue.add(telegram_user_id=1, intro_message="x")
    ttl = queue.requests[0].ttl_until
    parsed = datetime.strptime(ttl, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    delta = parsed - datetime.now(tz=timezone.utc)
    assert timedelta(days=DEFAULT_TTL_DAYS - 1) <= delta <= timedelta(days=DEFAULT_TTL_DAYS + 1)


def test_intro_message_truncated(tmp_path: Path):
    queue = PendingQueue.load(tmp_path / "p.json")
    long_message = "x" * 1000
    req = queue.add(telegram_user_id=1, intro_message=long_message)
    assert len(req.intro_message) == 500


def test_approval_updates_policy(tmp_path: Path):
    """End-to-end: admin approves → policy.grant + queue.remove + persistence."""
    policy_path = tmp_path / ".seeknal/access.yml"
    queue_path = tmp_path / ".seeknal/access_pending.json"

    policy = AccessPolicy.empty(source_path=policy_path)
    queue = PendingQueue.load(queue_path)

    queue.add(
        telegram_user_id=999,
        telegram_username="newcomer",
        intro_message="Please add me",
    )
    queue.save()

    # Admin approves as viewer
    policy.grant(telegram_user_id=999, role=Role.VIEWER, username="newcomer")
    policy.save()
    queue.remove(999)
    queue.save()

    loaded_policy = AccessPolicy.load(policy_path)
    loaded_queue = PendingQueue.load(queue_path)
    assert loaded_policy.resolve(user_id=999) == Role.VIEWER
    assert loaded_queue.requests == []


def test_two_admin_race(tmp_path: Path):
    """If two admins approve, second sees the first's already-approved state."""
    policy_path = tmp_path / "access.yml"
    policy = AccessPolicy.empty(source_path=policy_path)
    policy.grant(telegram_user_id=1, role=Role.VIEWER)
    policy.save()

    # Second admin's flow: try to grant — should overwrite without error.
    reloaded = AccessPolicy.load(policy_path)
    reloaded.grant(telegram_user_id=1, role=Role.OPERATOR)
    reloaded.save()
    final = AccessPolicy.load(policy_path)
    assert final.resolve(user_id=1) == Role.OPERATOR


def test_corrupt_queue_starts_empty(tmp_path: Path):
    path = tmp_path / ".seeknal/access_pending.json"
    path.parent.mkdir()
    path.write_text("{not json")
    queue = PendingQueue.load(path)
    assert queue.requests == []

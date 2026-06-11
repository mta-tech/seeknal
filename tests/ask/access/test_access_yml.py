"""Tests for AccessPolicy YAML loader (AC19)."""

from __future__ import annotations

from pathlib import Path

import pytest

from seeknal.ask.access.policy import AccessPolicy, AllowlistEntry, DenyEntry
from seeknal.ask.access.roles import Role


def _write(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return path


def test_load_missing_file_returns_empty(tmp_path: Path):
    policy = AccessPolicy.load(tmp_path / ".seeknal" / "access.yml")
    assert policy.allowlist == []
    assert policy.deny_list == []
    assert policy.default_role is None


def test_load_basic(tmp_path: Path):
    body = """
schema_version: 1
default_role: viewer
allowlist:
  - telegram_user_id: 1
    role: operator
    notes: "ops"
  - telegram_chat_id: -100123
    role: viewer
deny_list:
  - telegram_user_id: 9
    reason: "removed"
"""
    path = _write(tmp_path / ".seeknal" / "access.yml", body)
    policy = AccessPolicy.load(path)
    assert policy.default_role == Role.VIEWER
    assert len(policy.allowlist) == 2
    assert len(policy.deny_list) == 1


def test_insecure_path_rejected():
    with pytest.raises(ValueError, match="Insecure access policy"):
        AccessPolicy.load(Path("/tmp/access.yml"))


def test_resolve_user_id_keyed(tmp_path: Path):
    body = """
default_role: none
allowlist:
  - telegram_user_id: 42
    telegram_username: alice
    role: engineer
"""
    path = _write(tmp_path / ".seeknal/access.yml", body)
    policy = AccessPolicy.load(path)
    assert policy.resolve(user_id=42) == Role.ENGINEER


def test_username_change_does_not_grant_access(tmp_path: Path):
    """username is display-only; only user_id grants access."""
    body = """
default_role: none
allowlist:
  - telegram_user_id: 42
    telegram_username: alice
    role: engineer
"""
    path = _write(tmp_path / ".seeknal/access.yml", body)
    policy = AccessPolicy.load(path)
    # different user_id, same username → no access
    assert policy.resolve(user_id=99, username="alice") is None


def test_resolve_deny_list_overrides_allowlist(tmp_path: Path):
    body = """
default_role: viewer
allowlist:
  - telegram_user_id: 7
    role: admin
deny_list:
  - telegram_user_id: 7
    reason: removed
"""
    path = _write(tmp_path / ".seeknal/access.yml", body)
    policy = AccessPolicy.load(path)
    assert policy.resolve(user_id=7) is None


def test_resolve_chat_id_fallback(tmp_path: Path):
    body = """
default_role: none
allowlist:
  - telegram_chat_id: -100777
    role: viewer
"""
    path = _write(tmp_path / ".seeknal/access.yml", body)
    policy = AccessPolicy.load(path)
    assert policy.resolve(user_id=12345, chat_id=-100777) == Role.VIEWER
    # Without chat_id → default_role (none → None)
    assert policy.resolve(user_id=12345) is None


def test_resolve_default_role(tmp_path: Path):
    body = """
default_role: viewer
"""
    path = _write(tmp_path / ".seeknal/access.yml", body)
    policy = AccessPolicy.load(path)
    assert policy.resolve(user_id=9999) == Role.VIEWER


def test_unknown_role_raises(tmp_path: Path):
    body = """
allowlist:
  - telegram_user_id: 1
    role: god
"""
    path = _write(tmp_path / ".seeknal/access.yml", body)
    with pytest.raises(ValueError, match="Unknown role"):
        AccessPolicy.load(path)


def test_save_and_round_trip(tmp_path: Path):
    target = tmp_path / ".seeknal/access.yml"
    policy = AccessPolicy.empty(source_path=target)
    policy.default_role = Role.VIEWER
    policy.grant(
        telegram_user_id=1, role=Role.OPERATOR, username="alice", added_by="bob"
    )
    policy.save()

    loaded = AccessPolicy.load(target)
    assert loaded.default_role == Role.VIEWER
    assert len(loaded.allowlist) == 1
    assert loaded.allowlist[0].telegram_user_id == 1
    assert loaded.allowlist[0].role == Role.OPERATOR


def test_grant_overwrites_existing(tmp_path: Path):
    policy = AccessPolicy.empty(source_path=tmp_path / "access.yml")
    policy.grant(telegram_user_id=1, role=Role.VIEWER)
    policy.grant(telegram_user_id=1, role=Role.ADMIN)
    assert len(policy.allowlist) == 1
    assert policy.allowlist[0].role == Role.ADMIN


def test_revoke_removes_entry(tmp_path: Path):
    policy = AccessPolicy.empty(source_path=tmp_path / "access.yml")
    policy.grant(telegram_user_id=1, role=Role.VIEWER)
    assert policy.revoke(1) is True
    assert policy.allowlist == []


def test_revoke_with_deny_reason(tmp_path: Path):
    policy = AccessPolicy.empty(source_path=tmp_path / "access.yml")
    policy.grant(telegram_user_id=1, role=Role.VIEWER)
    policy.revoke(1, deny_reason="left team")
    assert any(e.telegram_user_id == 1 for e in policy.deny_list)
    assert policy.resolve(user_id=1) is None

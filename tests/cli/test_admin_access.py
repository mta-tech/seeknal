"""Tests for `seeknal admin access` CLI (Step 16)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from seeknal.ask.access.pending import PendingQueue
from seeknal.ask.access.policy import AccessPolicy
from seeknal.ask.access.roles import Role
from seeknal.cli.admin import app


runner = CliRunner()


def test_grant_and_list(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["access", "grant", "100", "viewer", "-u", "alice"])
    assert r.exit_code == 0, r.output
    assert "viewer" in r.output.lower()

    r = runner.invoke(app, ["access", "list"])
    assert r.exit_code == 0
    assert "user=100" in r.output
    assert "alice" in r.output


def test_grant_unknown_role(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["access", "grant", "100", "wizard"])
    assert r.exit_code != 0
    assert "Unknown role" in r.output


def test_revoke(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["access", "grant", "100", "operator"])
    r = runner.invoke(app, ["access", "revoke", "100"])
    assert r.exit_code == 0
    policy = AccessPolicy.load(tmp_path / ".seeknal/access.yml")
    assert all(e.telegram_user_id != 100 for e in policy.allowlist)


def test_revoke_with_deny(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["access", "grant", "200", "viewer"])
    r = runner.invoke(app, ["access", "revoke", "200", "--deny", "--reason", "left"])
    assert r.exit_code == 0
    policy = AccessPolicy.load(tmp_path / ".seeknal/access.yml")
    assert policy.resolve(user_id=200) is None
    assert any(e.telegram_user_id == 200 for e in policy.deny_list)


def test_set_default(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["access", "set-default", "viewer"])
    assert r.exit_code == 0
    policy = AccessPolicy.load(tmp_path / ".seeknal/access.yml")
    assert policy.default_role == Role.VIEWER


def test_pending_list_empty(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["access", "pending"])
    assert r.exit_code == 0
    assert "No pending" in r.output


def test_pending_approve_flow(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    queue = PendingQueue.load(tmp_path / ".seeknal/access_pending.json")
    queue.add(telegram_user_id=555, intro_message="please")
    queue.save()
    r = runner.invoke(app, ["access", "pending", "--approve", "555", "--role", "viewer"])
    assert r.exit_code == 0, r.output
    policy = AccessPolicy.load(tmp_path / ".seeknal/access.yml")
    assert policy.resolve(user_id=555) == Role.VIEWER
    queue2 = PendingQueue.load(tmp_path / ".seeknal/access_pending.json")
    assert queue2.requests == []


def test_pending_approve_requires_role(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    queue = PendingQueue.load(tmp_path / ".seeknal/access_pending.json")
    queue.add(telegram_user_id=555, intro_message="please")
    queue.save()
    r = runner.invoke(app, ["access", "pending", "--approve", "555"])
    assert r.exit_code != 0
    assert "--role" in r.output


def test_pending_deny(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    queue = PendingQueue.load(tmp_path / ".seeknal/access_pending.json")
    queue.add(telegram_user_id=555, intro_message="x")
    queue.save()
    r = runner.invoke(app, ["access", "pending", "--deny", "555", "--reason", "no"])
    assert r.exit_code == 0
    queue2 = PendingQueue.load(tmp_path / ".seeknal/access_pending.json")
    assert queue2.requests == []
    policy = AccessPolicy.load(tmp_path / ".seeknal/access.yml")
    assert any(e.telegram_user_id == 555 for e in policy.deny_list)


def test_export_import_round_trip(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["access", "grant", "100", "viewer"])
    runner.invoke(app, ["access", "grant", "200", "operator"])

    target = tmp_path / "backup.json"
    r = runner.invoke(app, ["access", "export", str(target)])
    assert r.exit_code == 0
    payload = json.loads(target.read_text())
    assert len(payload["allowlist"]) == 2

    # Wipe and re-import
    (tmp_path / ".seeknal/access.yml").unlink()
    r = runner.invoke(app, ["access", "import", str(target)])
    assert r.exit_code == 0
    policy = AccessPolicy.load(tmp_path / ".seeknal/access.yml")
    assert policy.resolve(user_id=100) == Role.VIEWER
    assert policy.resolve(user_id=200) == Role.OPERATOR


def test_import_requires_overwrite(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner.invoke(app, ["access", "grant", "100", "viewer"])
    target = tmp_path / "b.json"
    runner.invoke(app, ["access", "export", str(target)])
    r = runner.invoke(app, ["access", "import", str(target)])
    assert r.exit_code != 0
    assert "--overwrite" in r.output


def test_top_level_admin_registered(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    from seeknal.cli.main import app as main_app
    r = runner.invoke(main_app, ["admin", "--help"])
    assert r.exit_code == 0
    assert "access" in r.output

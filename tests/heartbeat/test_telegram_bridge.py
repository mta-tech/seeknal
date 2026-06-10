"""Tests for the Telegram → inbox bridge (Step 7) and the lint contract that
forbids heartbeat code from importing telegram code (Principle 5)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch


def test_no_telegram_imports_in_heartbeat():
    """Heartbeat code must not import from the telegram channel module
    (Principle 1: deterministic core; Principle 5: file boundary).
    """
    heartbeat_root = (
        Path(__file__).resolve().parents[2] / "src" / "seeknal" / "heartbeat"
    )
    bad: list[str] = []
    for py in heartbeat_root.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        if "seeknal.ask.gateway.channels" in text or "from telegram" in text:
            bad.append(str(py))
    assert not bad, (
        "Heartbeat must not import telegram. Offending files: "
        + ", ".join(bad)
    )


def test_inbox_drop_disabled_no_copy(tmp_path: Path, monkeypatch):
    """When inbox_drop is disabled, _copy_to_inbox is a no-op."""
    from seeknal.ask.gateway.channels.telegram import TelegramChannel

    project = tmp_path / "proj"
    project.mkdir()

    staged = project / "target/ask_ingest/_staging/sales.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text("a,b\n1,2\n")

    channel = TelegramChannel(project, token="t")
    with patch.object(
        TelegramChannel,
        "_inbox_drop_settings",
        return_value={"enabled": False, "folder": None},
    ):
        dest = channel._copy_to_inbox(staged)
    assert dest is None


def test_inbox_drop_enabled_copies_into_default_inbox(tmp_path: Path):
    from seeknal.ask.gateway.channels.telegram import TelegramChannel

    project = tmp_path / "proj"
    project.mkdir()

    staged = project / "target/ask_ingest/_staging/sales.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text("a,b\n1,2\n")

    channel = TelegramChannel(project, token="t")
    with patch.object(
        TelegramChannel,
        "_inbox_drop_settings",
        return_value={"enabled": True, "folder": None},
    ):
        dest = channel._copy_to_inbox(staged)
    assert dest is not None
    assert dest.exists()
    assert dest.parent == project / "inbox"
    assert dest.read_text() == staged.read_text()


def test_inbox_drop_filename_collision_appends_timestamp(tmp_path: Path):
    from seeknal.ask.gateway.channels.telegram import TelegramChannel

    project = tmp_path / "proj"
    project.mkdir()
    (project / "inbox").mkdir()
    (project / "inbox" / "sales.csv").write_text("x")

    staged = project / "target/ask_ingest/_staging/sales.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text("a,b\n1,2\n")

    channel = TelegramChannel(project, token="t")
    with patch.object(
        TelegramChannel,
        "_inbox_drop_settings",
        return_value={"enabled": True, "folder": "inbox"},
    ):
        dest = channel._copy_to_inbox(staged)
    assert dest is not None
    assert dest.name != "sales.csv"
    assert dest.name.startswith("sales.") and dest.suffix == ".csv"


def test_inbox_drop_resolves_inbox_from_heartbeat_md(tmp_path: Path):
    from seeknal.ask.gateway.channels.telegram import TelegramChannel

    project = tmp_path / "proj"
    project.mkdir()
    (project / "HEARTBEAT.md").write_text(
        """```yaml
interval: 30s
inbox_folders:
  - my_drop_zone
```
"""
    )
    staged = project / "target/ask_ingest/_staging/x.csv"
    staged.parent.mkdir(parents=True)
    staged.write_text("a\n1\n")

    channel = TelegramChannel(project, token="t")
    with patch.object(
        TelegramChannel,
        "_inbox_drop_settings",
        return_value={"enabled": True, "folder": None},
    ):
        dest = channel._copy_to_inbox(staged)
    assert dest is not None
    assert dest.parent.name == "my_drop_zone"


def test_inbox_drop_settings_reader_defaults(tmp_path: Path):
    from seeknal.ask.gateway.channels.telegram import TelegramChannel

    project = tmp_path / "proj"
    project.mkdir()
    channel = TelegramChannel(project, token="t")
    settings = channel._inbox_drop_settings()
    assert settings["enabled"] is False

"""Test that `seeknal init` scaffolds a HEARTBEAT.md."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner


def test_init_creates_heartbeat_md(tmp_path: Path, monkeypatch):
    """`seeknal init --name demo` writes a HEARTBEAT.md at the project root."""
    from seeknal.cli.main import app

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SEEKNAL_BASE_CONFIG_PATH", str(tmp_path / ".seeknal"))

    runner = CliRunner()
    result = runner.invoke(app, ["init", "--name", "demo", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output

    heartbeat_path = tmp_path / "HEARTBEAT.md"
    assert heartbeat_path.exists()
    text = heartbeat_path.read_text()
    assert "interval:" in text
    assert "inbox_folders" in text


def test_init_scaffolds_inbox_folder(tmp_path: Path, monkeypatch):
    from seeknal.cli.main import app

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SEEKNAL_BASE_CONFIG_PATH", str(tmp_path / ".seeknal"))

    runner = CliRunner()
    result = runner.invoke(app, ["init", "--name", "demo", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "inbox").exists() and (tmp_path / "inbox").is_dir()


def test_heartbeat_md_template_parses(tmp_path: Path, monkeypatch):
    from seeknal.cli.main import app
    from seeknal.heartbeat.config import HeartbeatConfig

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SEEKNAL_BASE_CONFIG_PATH", str(tmp_path / ".seeknal"))

    runner = CliRunner()
    result = runner.invoke(app, ["init", "--name", "demo", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output

    cfg = HeartbeatConfig.load(tmp_path)
    assert cfg.interval_seconds == 60
    assert cfg.enabled is True
    assert cfg.inbox_folders == [Path("inbox")]

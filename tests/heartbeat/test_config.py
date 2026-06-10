"""Tests for HeartbeatConfig parsing + fallback resolution."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from seeknal.heartbeat.config import (
    DEFAULT_INTERVAL_SECONDS,
    HeartbeatConfig,
    _parse_interval,
    _parse_heartbeat_body,
)


def test_parse_interval_seconds():
    assert _parse_interval("30s") == 30
    assert _parse_interval("2m") == 120
    assert _parse_interval("1h") == 3600
    assert _parse_interval("0s") == 0
    assert _parse_interval(45) == 45
    assert _parse_interval("90") == 90


def test_parse_interval_invalid():
    with pytest.raises(ValueError):
        _parse_interval(-1)
    with pytest.raises(ValueError):
        _parse_interval("nope")
    with pytest.raises(ValueError):
        _parse_interval(True)


def test_parse_body_pure_yaml():
    body = "interval: 30s\nenabled: true"
    assert _parse_heartbeat_body(body) == {"interval": "30s", "enabled": True}


def test_parse_body_fenced_yaml():
    body = """# Heartbeat config
Documentation goes here.

```yaml
interval: 60s
inbox_folders:
  - inbox
```

More comments below.
"""
    parsed = _parse_heartbeat_body(body)
    assert parsed["interval"] == "60s"
    assert parsed["inbox_folders"] == ["inbox"]


def test_parse_body_empty():
    assert _parse_heartbeat_body("") == {}
    assert _parse_heartbeat_body("   \n  ") == {}


def test_parse_body_invalid_yaml():
    body = "interval: [unclosed"
    with pytest.raises(yaml.YAMLError):
        _parse_heartbeat_body(body)


def test_parse_body_non_mapping():
    body = "- just\n- a\n- list"
    with pytest.raises(ValueError, match="must parse to a mapping"):
        _parse_heartbeat_body(body)


def test_load_missing_file_uses_defaults(tmp_path: Path, caplog):
    caplog.set_level(logging.WARNING, logger="seeknal.heartbeat.config")
    config = HeartbeatConfig.load(tmp_path)
    assert config.interval_seconds == DEFAULT_INTERVAL_SECONDS
    assert config.enabled is True
    assert config.ingested_target == "parquet"
    assert config.project_name == tmp_path.name
    assert any("not found" in rec.message for rec in caplog.records)


def test_load_explicit_values(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text(
        """```yaml
interval: 30s
enabled: true
inbox_folders:
  - inbox
  - drop_zone
ingested_target: parquet
quarantine_dir: target/heartbeat/quarantine
```
"""
    )
    config = HeartbeatConfig.load(tmp_path)
    assert config.interval_seconds == 30
    assert [p.name for p in config.inbox_folders] == ["inbox", "drop_zone"]
    assert config.ingested_target == "parquet"


def test_load_interval_zero_disables(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("interval: 0s\n")
    config = HeartbeatConfig.load(tmp_path)
    assert config.interval_seconds == 0


def test_load_insecure_inbox_rejected(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text(
        "inbox_folders:\n  - /tmp/bad_inbox\n"
    )
    with pytest.raises(ValueError, match="Insecure inbox folder"):
        HeartbeatConfig.load(tmp_path)


def test_load_unknown_target_raises(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("ingested_target: cassandra\n")
    with pytest.raises(ValueError, match="Unknown ingested_target"):
        HeartbeatConfig.load(tmp_path)


def test_load_yaml_parse_error(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("interval: [unclosed\n")
    with pytest.raises(yaml.YAMLError, match="HEARTBEAT.md parse error"):
        HeartbeatConfig.load(tmp_path)


def test_profile_fallback_for_ingested_target(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("interval: 30s\n")
    loader = MagicMock()
    loader.load_source_defaults.return_value = {"target": "iceberg"}
    config = HeartbeatConfig.load(tmp_path, profile_loader=loader)
    assert config.ingested_target == "iceberg"
    loader.load_source_defaults.assert_called_once_with("ingested")


def test_profile_fallback_unknown_target_raises(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("")
    loader = MagicMock()
    loader.load_source_defaults.return_value = {"target": "redis"}
    with pytest.raises(ValueError, match="Unknown ingested_target"):
        HeartbeatConfig.load(tmp_path, profile_loader=loader)


def test_project_name_explicit(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("project_name: my_demo\n")
    config = HeartbeatConfig.load(tmp_path)
    assert config.project_name == "my_demo"


def test_project_name_from_seeknal_project_yml(tmp_path: Path):
    (tmp_path / "seeknal_project.yml").write_text("name: from_yml\n")
    (tmp_path / "HEARTBEAT.md").write_text("")
    config = HeartbeatConfig.load(tmp_path)
    assert config.project_name == "from_yml"


def test_project_name_directory_fallback(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("")
    config = HeartbeatConfig.load(tmp_path)
    assert config.project_name == tmp_path.name


def test_inbox_folders_default_when_missing(tmp_path: Path):
    (tmp_path / "HEARTBEAT.md").write_text("interval: 30s\n")
    config = HeartbeatConfig.load(tmp_path)
    assert config.inbox_folders == [Path("inbox")]

"""HeartbeatConfig — loads HEARTBEAT.md project-level daemon config.

The config file lives at ``<project_root>/HEARTBEAT.md``. The body is either
pure YAML, or contains exactly one fenced ``` ```yaml ... ``` `` block.
Front-matter is not supported (keeps the parser simple).

Resolution rules:
- Missing file → defaults + log warning.
- ``ingested_target`` falls back to ``profiles.yml`` source_defaults.ingested.target,
  then ``"parquet"``.
- ``project_name`` priority: explicit key → ``seeknal_project.yml`` name → directory name.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from seeknal.utils.path_security import is_insecure_path

logger = logging.getLogger("seeknal.heartbeat.config")

DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_INBOX_FOLDERS = ("inbox",)
DEFAULT_QUARANTINE_DIR = "target/heartbeat/quarantine"
SUPPORTED_INGESTED_TARGETS = ("parquet", "iceberg")

_INTERVAL_RE = re.compile(r"^\s*(\d+)\s*([smh]?)\s*$", re.IGNORECASE)
_FENCED_YAML_RE = re.compile(r"```yaml\s*\n(.*?)\n```", re.DOTALL)


def _parse_interval(value: Any) -> int:
    """Parse "30s"/"2m"/"1h"/"0s" or a raw int → seconds. Reject negatives."""
    if isinstance(value, bool):
        raise ValueError(f"Invalid interval (bool): {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"Interval must be >= 0, got {value}")
        return value
    if isinstance(value, str):
        match = _INTERVAL_RE.match(value)
        if not match:
            raise ValueError(
                f"Invalid interval '{value}'. Expected e.g. '30s', '2m', '1h', '0s'."
            )
        num = int(match.group(1))
        suffix = match.group(2).lower()
        if suffix in ("", "s"):
            return num
        if suffix == "m":
            return num * 60
        if suffix == "h":
            return num * 3600
        raise ValueError(f"Invalid interval suffix '{suffix}'")
    raise ValueError(f"Interval must be int or str, got {type(value).__name__}")


def _parse_heartbeat_body(text: str) -> dict[str, Any]:
    """Parse HEARTBEAT.md body. Pure YAML, or single fenced ```yaml block."""
    stripped = text.strip()
    if not stripped:
        return {}
    fenced_match = _FENCED_YAML_RE.search(stripped)
    if fenced_match:
        yaml_text = fenced_match.group(1)
    else:
        yaml_text = stripped
    try:
        data = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        raise yaml.YAMLError(f"HEARTBEAT.md parse error: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f"HEARTBEAT.md must parse to a mapping, got {type(data).__name__}"
        )
    return data


def _resolve_project_name(
    project_root: Path,
    explicit: Optional[str],
) -> str:
    if explicit:
        return str(explicit)
    project_yml = project_root / "seeknal_project.yml"
    if project_yml.exists():
        try:
            with open(project_yml, "r") as fh:
                pdata = yaml.safe_load(fh) or {}
            name = pdata.get("name") if isinstance(pdata, dict) else None
            if name:
                return str(name)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to read project name from %s: %s", project_yml, exc
            )
    return project_root.name


@dataclass
class HeartbeatConfig:
    """Parsed HEARTBEAT.md daemon config.

    Attributes:
        interval_seconds: Loop interval. ``0`` disables ``start`` (tick still works).
        inbox_folders: List of folders (relative to project root or absolute).
        ingested_target: ``"parquet"`` | ``"iceberg"`` | ``None`` (use profile fallback).
        enabled: Master toggle. ``False`` → ``start`` exits with a message.
        quarantine_dir: Where bad files go (Path).
        project_name: Resolved at load time.
        profile_path: Path to profiles.yml override (None → ProfileLoader default).
    """

    interval_seconds: int = DEFAULT_INTERVAL_SECONDS
    inbox_folders: list[Path] = field(
        default_factory=lambda: [Path(p) for p in DEFAULT_INBOX_FOLDERS]
    )
    ingested_target: Optional[str] = None
    enabled: bool = True
    quarantine_dir: Path = field(default_factory=lambda: Path(DEFAULT_QUARANTINE_DIR))
    project_name: str = ""
    profile_path: Optional[Path] = None
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(
        cls,
        project_root: Path,
        profile_loader: Optional[Any] = None,
    ) -> "HeartbeatConfig":
        """Load HEARTBEAT.md from ``project_root`` and resolve fallbacks.

        Args:
            project_root: Directory containing HEARTBEAT.md.
            profile_loader: ProfileLoader instance used for ``ingested_target``
                fallback via ``load_source_defaults("ingested")``.
        """
        project_root = Path(project_root).expanduser().resolve()
        config_path = project_root / "HEARTBEAT.md"
        data: dict[str, Any]
        if not config_path.exists():
            logger.warning(
                "HEARTBEAT.md not found at %s; using defaults", config_path
            )
            data = {}
        else:
            with open(config_path, "r") as fh:
                data = _parse_heartbeat_body(fh.read())

        interval_raw = data.get("interval", DEFAULT_INTERVAL_SECONDS)
        interval_seconds = _parse_interval(interval_raw)

        enabled = bool(data.get("enabled", True))

        inbox_raw = data.get("inbox_folders") or list(DEFAULT_INBOX_FOLDERS)
        if isinstance(inbox_raw, str):
            inbox_raw = [inbox_raw]
        if not isinstance(inbox_raw, list):
            raise ValueError("`inbox_folders` must be a list of paths")
        inbox_folders = [Path(p) for p in inbox_raw]
        # Security check on each folder
        for folder in inbox_folders:
            folder_abs = folder if folder.is_absolute() else (project_root / folder)
            if is_insecure_path(str(folder_abs)):
                raise ValueError(
                    f"Insecure inbox folder: {folder_abs!s}. "
                    "Use a secure location (e.g. project-local inbox/)."
                )

        target = data.get("ingested_target")
        if target is not None and target not in SUPPORTED_INGESTED_TARGETS:
            raise ValueError(
                f"Unknown ingested_target '{target}'. "
                f"Accepted: {', '.join(SUPPORTED_INGESTED_TARGETS)}"
            )

        if target is None and profile_loader is not None:
            try:
                defaults = profile_loader.load_source_defaults("ingested") or {}
                target = defaults.get("target")
                if target is not None and target not in SUPPORTED_INGESTED_TARGETS:
                    raise ValueError(
                        f"Unknown ingested_target '{target}' in profile. "
                        f"Accepted: {', '.join(SUPPORTED_INGESTED_TARGETS)}"
                    )
            except ValueError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Could not read source_defaults.ingested.target from profile: %s",
                    exc,
                )
                target = None

        if target is None:
            target = "parquet"

        quarantine_raw = data.get("quarantine_dir", DEFAULT_QUARANTINE_DIR)
        quarantine_dir = Path(quarantine_raw)
        quarantine_abs = (
            quarantine_dir
            if quarantine_dir.is_absolute()
            else (project_root / quarantine_dir)
        )
        if is_insecure_path(str(quarantine_abs)):
            raise ValueError(
                f"Insecure quarantine_dir: {quarantine_abs!s}. "
                "Use a project-local path (e.g. target/heartbeat/quarantine)."
            )

        project_name = _resolve_project_name(project_root, data.get("project_name"))

        profile_raw = data.get("profile_path")
        profile_path = Path(profile_raw) if profile_raw else None

        return cls(
            interval_seconds=interval_seconds,
            inbox_folders=inbox_folders,
            ingested_target=target,
            enabled=enabled,
            quarantine_dir=quarantine_dir,
            project_name=project_name,
            profile_path=profile_path,
            raw=data,
        )

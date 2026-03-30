"""Heartbeat runner for seeknal ask.

Periodically runs agent turns driven by HEARTBEAT.md content, delivering
findings to configured channels. Responses containing HEARTBEAT_OK are
suppressed (everything is fine, no alert needed).

Configuration in seeknal_agent.yml:
    heartbeat:
      every: 30m
      target:
        channel: telegram
        chat_id: -1001234567890
      active_hours:
        start: "08:00"
        end: "22:00"
        timezone: Asia/Jakarta
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_HEARTBEAT_OK_PATTERN = re.compile(r"HEARTBEAT_OK", re.IGNORECASE)

_STATE_FILE = "heartbeat_state.json"
_HEARTBEAT_FILE = "HEARTBEAT.md"

# Agent execution timeout (seconds)
_AGENT_TIMEOUT = 300


def _parse_interval(interval_str: str) -> int:
    """Parse an interval string like '30m', '1h', '2h' to seconds.

    Supported suffixes: s (seconds), m (minutes), h (hours).

    Args:
        interval_str: Interval string (e.g., '30m', '1h', '2h', '90s').

    Returns:
        Interval in seconds.

    Raises:
        ValueError: If format is invalid.
    """
    interval_str = interval_str.strip().lower()
    match = re.match(r"^(\d+)\s*([smh])$", interval_str)
    if not match:
        raise ValueError(
            f"Invalid interval format: '{interval_str}'. "
            "Expected format like '30m', '1h', '2h', '90s'."
        )
    value = int(match.group(1))
    unit = match.group(2)
    multiplier = {"s": 1, "m": 60, "h": 3600}
    return value * multiplier[unit]


def _is_active_hours(config: dict) -> bool:
    """Check if current time is within the configured active hours window.

    Args:
        config: The heartbeat config dict. Should contain an 'active_hours'
                key with 'start', 'end', and optionally 'timezone'.

    Returns:
        True if within active hours or if active_hours is not configured.
    """
    active = config.get("active_hours")
    if not active or not isinstance(active, dict):
        return True

    start_str = active.get("start", "00:00")
    end_str = active.get("end", "23:59")
    tz_name = active.get("timezone")

    from zoneinfo import ZoneInfo

    tz = ZoneInfo(tz_name) if tz_name else None
    now = datetime.now(tz=tz)
    current_time = now.strftime("%H:%M")

    if start_str <= end_str:
        return start_str <= current_time <= end_str
    else:
        # Overnight window (e.g., 22:00 → 06:00)
        return current_time >= start_str or current_time <= end_str


def _is_heartbeat_ok(response: str) -> bool:
    """Check if the response indicates everything is OK (suppress delivery).

    The HEARTBEAT_OK protocol: if the stripped response starts with or ends
    with 'HEARTBEAT_OK', the message is suppressed.

    Args:
        response: Agent response text.

    Returns:
        True if the response should be suppressed.
    """
    stripped = response.strip()
    return stripped.startswith("HEARTBEAT_OK") or stripped.endswith("HEARTBEAT_OK")


def _save_state(project_path: Path, state: dict) -> None:
    """Persist heartbeat state to .seeknal/heartbeat_state.json.

    Args:
        project_path: Path to the seeknal project root.
        state: State dict with last_run, last_result, next_due, etc.
    """
    seeknal_dir = project_path / ".seeknal"
    seeknal_dir.mkdir(parents=True, exist_ok=True)
    state_path = seeknal_dir / _STATE_FILE
    tmp_path = state_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, default=str))
    os.replace(str(tmp_path), str(state_path))


def _load_state(project_path: Path) -> dict:
    """Load heartbeat state from .seeknal/heartbeat_state.json.

    Args:
        project_path: Path to the seeknal project root.

    Returns:
        State dict. Empty dict if no state file exists.
    """
    state_path = project_path / ".seeknal" / _STATE_FILE
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


class HeartbeatRunner:
    """Periodic heartbeat runner that uses the agent to check project health.

    Reads HEARTBEAT.md from the project's .seeknal/ directory, runs an
    isolated agent turn with the content as the question, and delivers
    findings (non-OK responses) to the configured channel.

    Args:
        config: Heartbeat section from seeknal_agent.yml.
        channels: Dict of channel name -> Channel instance.
    """

    def __init__(
        self,
        config: dict,
        channels: Optional[dict[str, Any]] = None,
    ) -> None:
        self._config = config
        self._channels = channels or {}
        self._interval = _parse_interval(config.get("every", "30m"))

    async def run(self, project_path: Path) -> None:
        """Main heartbeat loop. Runs until cancelled.

        Sleeps for the configured interval between heartbeat runs.
        Respects active_hours — skips execution outside the window.

        Args:
            project_path: Path to the seeknal project root.
        """
        logger.info(
            "Heartbeat runner started (interval=%ds, project=%s)",
            self._interval,
            project_path,
        )
        while True:
            try:
                if _is_active_hours(self._config):
                    response = await self.run_once(project_path)
                    logger.info("Heartbeat completed: %s", response[:100])
                else:
                    logger.debug("Heartbeat skipped: outside active hours")
            except asyncio.CancelledError:
                logger.info("Heartbeat runner cancelled")
                raise
            except Exception:
                logger.exception("Heartbeat run failed")

            await asyncio.sleep(self._interval)

    async def run_once(self, project_path: Path) -> str:
        """Execute a single heartbeat check.

        Reads HEARTBEAT.md, runs an isolated agent turn, and delivers
        findings to the configured channel if the response is not OK.

        Args:
            project_path: Path to the seeknal project root.

        Returns:
            The agent response text.
        """
        # Read HEARTBEAT.md
        heartbeat_path = project_path / ".seeknal" / _HEARTBEAT_FILE
        if not heartbeat_path.exists():
            msg = (
                f"HEARTBEAT.md not found at {heartbeat_path}. "
                "Create .seeknal/HEARTBEAT.md with monitoring instructions."
            )
            logger.warning(msg)
            _save_state(project_path, {
                "last_run": datetime.now().isoformat(),
                "last_result": "skipped",
                "last_message": msg,
            })
            return msg

        heartbeat_content = heartbeat_path.read_text().strip()
        if not heartbeat_content:
            msg = "HEARTBEAT.md is empty. Add monitoring instructions."
            logger.warning(msg)
            _save_state(project_path, {
                "last_run": datetime.now().isoformat(),
                "last_result": "skipped",
                "last_message": msg,
            })
            return msg

        # Create fresh agent with analysis profile, isolated session
        session_name = f"heartbeat-{int(time.time())}"
        try:
            response = await self._run_agent(
                project_path, heartbeat_content, session_name
            )
        except asyncio.TimeoutError:
            response = (
                f"Heartbeat agent timed out after {_AGENT_TIMEOUT}s. "
                "The check may be too complex or the agent is unresponsive."
            )
            logger.error("Heartbeat agent timed out after %ds", _AGENT_TIMEOUT)

        # Parse response for HEARTBEAT_OK
        is_ok = _is_heartbeat_ok(response)
        result = "ok" if is_ok else "findings"

        # Save state
        _save_state(project_path, {
            "last_run": datetime.now().isoformat(),
            "last_result": result,
            "last_message": response[:500],
            "next_due": (
                datetime.now() + timedelta(seconds=self._interval)
            ).isoformat(),
        })

        # Deliver findings if not OK
        if not is_ok:
            await self._deliver(response)
        else:
            logger.info("Heartbeat OK — suppressing delivery")

        return response

    async def _run_agent(
        self,
        project_path: Path,
        question: str,
        session_name: str,
    ) -> str:
        """Run an isolated agent turn for the heartbeat check.

        Args:
            project_path: Project root path.
            question: HEARTBEAT.md content to use as the agent prompt.
            session_name: Unique session name for isolation.

        Returns:
            Agent response text.
        """
        from seeknal.ask.agents.agent import ask as sync_ask, create_agent

        agent, config = create_agent(
            project_path=project_path,
            question=question,
            profile="analysis",
            session_name=session_name,
        )

        return await asyncio.wait_for(
            asyncio.to_thread(sync_ask, agent, config, question),
            timeout=_AGENT_TIMEOUT,
        )

    async def _deliver(self, text: str) -> None:
        """Deliver findings to the configured target channel.

        Args:
            text: The findings text to deliver.
        """
        target = self._config.get("target", {})
        if not isinstance(target, dict):
            logger.warning("Heartbeat target not configured, skipping delivery")
            return

        channel_name = target.get("channel")
        chat_id = target.get("chat_id")

        if not channel_name or not chat_id:
            logger.warning(
                "Heartbeat target incomplete (channel=%s, chat_id=%s)",
                channel_name,
                chat_id,
            )
            return

        channel = self._channels.get(channel_name)
        if channel is None:
            logger.warning(
                "Channel '%s' not found in channels dict. Available: %s",
                channel_name,
                list(self._channels.keys()),
            )
            return

        session_id = f"{channel_name}:{chat_id}"
        try:
            await channel.deliver(session_id, text)
            logger.info("Heartbeat findings delivered to %s", session_id)
        except Exception:
            logger.exception("Failed to deliver heartbeat to %s", session_id)

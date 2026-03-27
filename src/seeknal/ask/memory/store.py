"""Memory file store for seeknal ask agent.

Manages `.seeknal/memory/` directory with:
- MEMORY.md — curated index of important memories
- YYYY-MM-DD.md — daily append-only logs

Inspired by openclaw's file-based memory architecture where Markdown files
are the source of truth (human-readable, git-friendly, inspectable).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from filelock import FileLock

# Memory categories
CATEGORIES = {"project_knowledge", "user_preferences", "operational_history"}

# How many recent daily logs to include in search
_RECENT_DAYS = 7


class MemoryStore:
    """Manages persistent agent memory as Markdown files.

    Args:
        project_path: Root of the seeknal project.
    """

    def __init__(self, project_path: Path) -> None:
        self._dir = project_path / ".seeknal" / "memory"
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = FileLock(str(self._dir / ".lock"), timeout=10)
        self._memory_md = self._dir / "MEMORY.md"

    # -- Write ---------------------------------------------------------------

    def write(self, content: str, category: str = "project_knowledge") -> str:
        """Append a memory entry to today's daily log.

        Args:
            content: The memory text to store.
            category: One of project_knowledge, user_preferences, operational_history.

        Returns:
            Confirmation message.
        """
        if category not in CATEGORIES:
            category = "project_knowledge"

        now = datetime.now(timezone.utc)
        daily_file = self._dir / f"{now.strftime('%Y-%m-%d')}.md"
        timestamp = now.strftime("%H:%M")

        entry = f"## {timestamp} — {category}\n\n{content.strip()}\n\n"

        with self._lock:
            # Append to daily log
            with open(daily_file, "a", encoding="utf-8") as f:
                f.write(entry)

            # Update MEMORY.md index
            self._update_index(content, category, now)

        return f"Memory saved ({category}): {content[:80]}..."

    def _update_index(
        self, content: str, category: str, when: datetime,
    ) -> None:
        """Append a one-line summary to MEMORY.md index."""
        # Take first line or first 120 chars as summary
        summary = content.strip().split("\n")[0][:120]
        date_str = when.strftime("%Y-%m-%d")
        line = f"- [{date_str}] **{category}**: {summary}\n"

        if not self._memory_md.exists():
            self._memory_md.write_text(
                "# Agent Memory\n\nPersistent memories across sessions.\n\n",
                encoding="utf-8",
            )

        with open(self._memory_md, "a", encoding="utf-8") as f:
            f.write(line)

    # -- Search --------------------------------------------------------------

    def search(self, query: str, max_results: int = 10) -> list[str]:
        """Search memories by keyword matching.

        Scans MEMORY.md and recent daily logs for lines containing
        any of the query keywords. Returns matching entries.

        Args:
            query: Space-separated keywords to search for.
            max_results: Maximum entries to return.

        Returns:
            List of matching memory entries.
        """
        keywords = [w.lower() for w in query.split() if len(w) >= 2]
        if not keywords:
            return []

        matches: list[str] = []

        # Search MEMORY.md index
        if self._memory_md.exists():
            for line in self._memory_md.read_text(encoding="utf-8").splitlines():
                if any(k in line.lower() for k in keywords):
                    matches.append(line.strip())

        # Search recent daily logs
        for daily_file in self._recent_daily_files():
            text = daily_file.read_text(encoding="utf-8")
            for entry in self._parse_entries(text):
                if any(k in entry.lower() for k in keywords):
                    matches.append(entry.strip())

        return matches[:max_results]

    # -- Context loading -----------------------------------------------------

    def load_context(self) -> str:
        """Load memory content for injection into agent system prompt.

        Returns MEMORY.md content + today's daily log, concatenated.
        """
        parts: list[str] = []

        if self._memory_md.exists():
            content = self._memory_md.read_text(encoding="utf-8").strip()
            if content:
                parts.append(content)

        today_file = self._dir / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.md"
        if today_file.exists():
            content = today_file.read_text(encoding="utf-8").strip()
            if content:
                parts.append(f"## Today's Session Notes\n\n{content}")

        return "\n\n".join(parts) if parts else ""

    # -- Helpers -------------------------------------------------------------

    def _recent_daily_files(self) -> list[Path]:
        """Return the most recent daily log files, newest first."""
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}\.md$")
        files = sorted(
            (f for f in self._dir.iterdir() if pattern.match(f.name)),
            reverse=True,
        )
        return files[:_RECENT_DAYS]

    @staticmethod
    def _parse_entries(text: str) -> list[str]:
        """Split a daily log into individual entries by ## headings."""
        entries: list[str] = []
        current: list[str] = []
        for line in text.splitlines():
            if line.startswith("## ") and current:
                entries.append("\n".join(current))
                current = [line]
            else:
                current.append(line)
        if current:
            entries.append("\n".join(current))
        return entries

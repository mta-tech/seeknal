"""Session management for seeknal ask chat.

Provides named, persistent chat sessions backed by JSON files.
Each session stores pydantic-ai message history and lightweight metadata
in a per-project directory at `.seeknal/sessions/<name>/`.

Sessions are identified by human-readable names like `calm-river-315`.
"""

from __future__ import annotations

import json
import random
import shutil
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Human-readable session name generation
# ---------------------------------------------------------------------------

ADJECTIVES = [
    "amber", "azure", "bold", "brave", "bright", "calm", "clear", "cool",
    "coral", "crisp", "dawn", "deep", "eager", "early", "fair", "fast",
    "fine", "fond", "free", "fresh", "gold", "good", "grand", "green",
    "happy", "keen", "kind", "late", "light", "live", "lunar", "mild",
    "neat", "noble", "pale", "pure", "quick", "quiet", "rapid", "rare",
    "rich", "royal", "safe", "sharp", "silver", "sleek", "smart", "smooth",
    "solar", "solid", "sonic", "steady", "still", "strong", "sunny", "super",
    "sweet", "swift", "teal", "true", "vivid", "warm", "wild", "wise",
]

NOUNS = [
    "anchor", "arrow", "atlas", "aurora", "badge", "beacon", "bird", "blaze",
    "bloom", "bolt", "bridge", "brook", "castle", "cedar", "cliff", "cloud",
    "comet", "coral", "crane", "creek", "crown", "crystal", "delta", "dune",
    "eagle", "ember", "falcon", "fern", "flame", "forest", "frost", "garden",
    "glacier", "grove", "harbor", "hawk", "horizon", "island", "jasmine", "lake",
    "leaf", "lion", "lotus", "maple", "meadow", "moon", "mountain", "oak",
    "ocean", "olive", "orchid", "palm", "peak", "pearl", "phoenix", "pine",
    "planet", "pond", "prairie", "rain", "raven", "reef", "ridge", "river",
]


def generate_session_name() -> str:
    """Generate a unique human-readable session name like 'calm-river-315'."""
    adjective = random.choice(ADJECTIVES)
    noun = random.choice(NOUNS)
    suffix = random.randint(100, 999)
    return f"{adjective}-{noun}-{suffix}"


# ---------------------------------------------------------------------------
# SessionStore — JSON-file-based session persistence
# ---------------------------------------------------------------------------


class SessionStore:
    """Manages session metadata and conversation history.

    Each session is a directory under `<project>/.seeknal/sessions/`
    containing:
    - ``metadata.json`` — name, status, timestamps, message count
    - ``messages.json`` — pydantic-ai conversation history
    """

    def __init__(self, project_path: Path) -> None:
        self._base = project_path / ".seeknal" / "sessions"
        self._base.mkdir(parents=True, exist_ok=True)

    def _session_dir(self, name: str) -> Path:
        return self._base / name

    def _metadata_path(self, name: str) -> Path:
        return self._session_dir(name) / "metadata.json"

    def _messages_path(self, name: str) -> Path:
        return self._session_dir(name) / "messages.json"

    # -- CRUD ---------------------------------------------------------------

    def create(self, name: str | None = None) -> str:
        """Create a new session and return its name."""
        if name is None:
            name = generate_session_name()
            while self.get(name) is not None:
                name = generate_session_name()

        session_dir = self._session_dir(name)
        session_dir.mkdir(parents=True, exist_ok=True)

        now = datetime.now(timezone.utc).isoformat()
        metadata = {
            "name": name,
            "status": "active",
            "created_at": now,
            "updated_at": now,
            "message_count": 0,
            "last_question": None,
        }
        self._metadata_path(name).write_text(
            json.dumps(metadata, indent=2), encoding="utf-8"
        )
        return name

    def get(self, name: str) -> dict | None:
        """Fetch a session by name. Returns dict or None."""
        meta_path = self._metadata_path(name)
        if not meta_path.exists():
            return None
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def list(self) -> list[dict]:
        """List all sessions, most recently updated first."""
        sessions = []
        if not self._base.exists():
            return sessions
        for d in self._base.iterdir():
            if d.is_dir():
                meta = self.get(d.name)
                if meta:
                    sessions.append(meta)
        sessions.sort(key=lambda s: s.get("updated_at", ""), reverse=True)
        return sessions

    def update(self, name: str, **kwargs) -> None:
        """Update session metadata fields."""
        meta = self.get(name)
        if meta is None:
            return
        allowed = {"status", "message_count", "last_question", "updated_at"}
        for k, v in kwargs.items():
            if k in allowed:
                meta[k] = v
        if "updated_at" not in kwargs:
            meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._metadata_path(name).write_text(
            json.dumps(meta, indent=2), encoding="utf-8"
        )

    def delete(self, name: str) -> bool:
        """Delete a session and all its data."""
        session_dir = self._session_dir(name)
        if not session_dir.exists():
            return False
        shutil.rmtree(session_dir)
        return True

    # -- Message persistence ------------------------------------------------

    def save_messages(self, name: str, messages: list) -> None:
        """Save conversation history using pydantic-ai serialization."""
        from pydantic_ai.messages import ModelMessagesTypeAdapter

        data = ModelMessagesTypeAdapter.dump_json(messages)
        self._messages_path(name).write_bytes(data)

    def load_messages(self, name: str) -> list:
        """Load conversation history. Returns empty list if not found."""
        msg_path = self._messages_path(name)
        if not msg_path.exists():
            return []
        from pydantic_ai.messages import ModelMessagesTypeAdapter

        return list(ModelMessagesTypeAdapter.validate_json(msg_path.read_bytes()))

    # -- Context manager ----------------------------------------------------

    def close(self) -> None:
        """No-op (no connections to close with JSON files)."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

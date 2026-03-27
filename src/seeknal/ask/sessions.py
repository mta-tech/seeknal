"""Session management for seeknal ask chat.

Provides named, persistent chat sessions backed by SQLite. Each session
stores LangGraph checkpoints (via SqliteSaver) and lightweight metadata
(name, status, timestamps, message count) in a per-project database at
`.seeknal/sessions.db`.

Sessions are identified by human-readable names like `calm-river-315`.
"""

import random
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Human-readable session name generation (KAI-inspired)
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
# Session metadata schema
# ---------------------------------------------------------------------------

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS sessions (
    name TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    last_question TEXT
);
"""


# ---------------------------------------------------------------------------
# SessionStore — metadata CRUD + SqliteSaver bridge
# ---------------------------------------------------------------------------


class SessionStore:
    """Manages session metadata and provides a LangGraph SqliteSaver.

    Both the session metadata table and LangGraph checkpoint tables live
    in the same SQLite database at ``<project>/.seeknal/sessions.db``.
    """

    def __init__(self, project_path: Path) -> None:
        db_dir = project_path / ".seeknal"
        db_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = db_dir / "sessions.db"
        self._conn = sqlite3.connect(
            str(self._db_path), check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)

    # -- CRUD ---------------------------------------------------------------

    def create(self, name: str | None = None) -> str:
        """Create a new session and return its name."""
        if name is None:
            name = generate_session_name()
            # Ensure uniqueness (extremely unlikely collision)
            while self.get(name) is not None:
                name = generate_session_name()
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            "INSERT INTO sessions (name, status, created_at, updated_at) "
            "VALUES (?, 'active', ?, ?)",
            (name, now, now),
        )
        self._conn.commit()
        return name

    def get(self, name: str) -> dict | None:
        """Fetch a session by name. Returns dict or None."""
        row = self._conn.execute(
            "SELECT * FROM sessions WHERE name = ?", (name,),
        ).fetchone()
        return dict(row) if row else None

    def list(self) -> list[dict]:
        """List all sessions, most recently updated first."""
        rows = self._conn.execute(
            "SELECT * FROM sessions ORDER BY updated_at DESC",
        ).fetchall()
        return [dict(r) for r in rows]

    def update(self, name: str, **kwargs) -> None:
        """Update session metadata fields."""
        if not kwargs:
            return
        allowed = {"status", "message_count", "last_question", "updated_at"}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return
        # Always bump updated_at
        if "updated_at" not in fields:
            fields["updated_at"] = datetime.now(timezone.utc).isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [name]
        self._conn.execute(
            f"UPDATE sessions SET {set_clause} WHERE name = ?",  # noqa: S608
            values,
        )
        self._conn.commit()

    def delete(self, name: str) -> bool:
        """Delete a session and its LangGraph checkpoints."""
        session = self.get(name)
        if session is None:
            return False
        # Delete LangGraph checkpoint data (tables may not exist yet)
        for table in ("checkpoints", "writes"):
            try:
                self._conn.execute(
                    f"DELETE FROM {table} WHERE thread_id = ?", (name,),  # noqa: S608
                )
            except sqlite3.OperationalError:
                pass  # Table doesn't exist yet — no checkpoints to clean
        self._conn.execute("DELETE FROM sessions WHERE name = ?", (name,))
        self._conn.commit()
        return True

    # -- Checkpointer -------------------------------------------------------

    def get_checkpointer(self):
        """Return a SqliteSaver sharing this store's DB connection."""
        from langgraph.checkpoint.sqlite import SqliteSaver

        return SqliteSaver(self._conn)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

"""Conftest for E2E tests — project fixtures and skip logic."""

import json
import os
import re

import pandas as pd
import pytest

# Load .env so skip logic can see LLM credentials at collection time.
# The CLI loads .env at runtime, but pytest needs it at import time.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Spark autouse override (E2E tests don't use Spark)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_spark_state_between_tests():
    """No-op override: E2E tests don't use Spark."""
    yield


# ---------------------------------------------------------------------------
# Skip logic for tests requiring a real LLM backend
# ---------------------------------------------------------------------------

def _has_llm_credentials() -> bool:
    """Return True if LLM credentials are available in the environment."""
    if os.environ.get("GOOGLE_API_KEY"):
        return True
    if os.environ.get("SEEKNAL_ASK_LLM_PROVIDER") == "ollama":
        return True
    return False


requires_llm = pytest.mark.skipif(
    not _has_llm_credentials(),
    reason="LLM credentials not available (set GOOGLE_API_KEY or SEEKNAL_ASK_LLM_PROVIDER=ollama)",
)


# ---------------------------------------------------------------------------
# ANSI stripping helper
# ---------------------------------------------------------------------------

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from terminal output."""
    return _ANSI_RE.sub("", text)


# ---------------------------------------------------------------------------
# Minimal project fixture for ask chat E2E tests
# ---------------------------------------------------------------------------

@pytest.fixture
def chat_project(tmp_path):
    """Create a minimal seeknal project with one data source.

    Structure::

        <tmp_path>/
        ├── seeknal/
        │   └── sources/
        │       └── test_data.yml
        ├── target/
        │   └── intermediate/
        │       └── source_raw_test_data.parquet
        └── seeknal_project.yml
    """
    # Project config
    (tmp_path / "seeknal_project.yml").write_text(
        "name: test_chat_project\nengine: duckdb\n"
    )

    # Source definition
    sources_dir = tmp_path / "seeknal" / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "test_data.yml").write_text(
        "kind: source\nname: raw_test_data\nsource: parquet\npath: data/test_data.parquet\n"
    )

    # Intermediate parquet (simulates a pipeline that has been run)
    intermediate_dir = tmp_path / "target" / "intermediate"
    intermediate_dir.mkdir(parents=True)

    df = pd.DataFrame(
        {
            "id": range(1, 11),
            "name": [f"item_{i}" for i in range(1, 11)],
            "value": [i * 10.0 for i in range(1, 11)],
        }
    )
    df.to_parquet(intermediate_dir / "source_raw_test_data.parquet", index=False)

    # Manifest (so the agent knows about available data)
    manifest = {
        "nodes": {
            "source_raw_test_data": {
                "name": "raw_test_data",
                "type": "source",
                "output_path": str(intermediate_dir / "source_raw_test_data.parquet"),
            }
        }
    }
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(manifest))

    return tmp_path


# ---------------------------------------------------------------------------
# pyte terminal emulator for visual verification
# ---------------------------------------------------------------------------


def feed_to_screen(raw_output: str, width: int = 80, height: int = 50):
    """Feed raw pexpect output into a pyte virtual terminal screen.

    Returns a ``pyte.Screen`` with the rendered terminal state, allowing
    assertions on character styling (fg color, bold, etc.).
    """
    import pyte

    screen = pyte.Screen(width, height)
    stream = pyte.Stream(screen)
    stream.feed(raw_output)
    return screen

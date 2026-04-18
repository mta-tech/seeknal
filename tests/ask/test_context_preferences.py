"""Tests for list_context_files, write_project_file, save_preference, and
the session-startup preferences loader."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.list_context_files import list_context_files
from seeknal.ask.agents.tools.save_preference import (
    load_preferences,
    save_preference,
)
from seeknal.ask.agents.tools.write_project_file import write_project_file


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")


@pytest.fixture
def ctx(tmp_path: Path) -> ToolContext:
    context = ToolContext(
        repl=_REPLStub(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(context)
    return context


# ---------------------------------------------------------------------------
# list_context_files
# ---------------------------------------------------------------------------


def test_list_context_files_missing_directory(tmp_path, ctx):
    out = list_context_files()
    assert "No `context/` directory" in out


def test_list_context_files_empty(tmp_path, ctx):
    (tmp_path / "context").mkdir()
    out = list_context_files()
    assert "is empty" in out


def test_list_context_files_returns_entries_and_hints(tmp_path, ctx):
    ctx_dir = tmp_path / "context"
    ctx_dir.mkdir()
    (ctx_dir / "glossary.md").write_text("# Business Glossary\n\nRevenue = gmv.\n")
    (ctx_dir / "data_quality.yml").write_text("caveats:\n  - orders table lags 24h\n")
    (ctx_dir / "metric_definitions").mkdir()
    (ctx_dir / "metric_definitions" / "revenue.md").write_text(
        "## Revenue\n\nDefined as SUM(gmv) not SUM(revenue_column).\n"
    )

    out = list_context_files()
    assert "3 available" in out
    assert "context/glossary.md" in out
    # First-line hint stripped of '#' markers
    assert "Business Glossary" in out
    assert "context/data_quality.yml" in out
    assert "context/metric_definitions/revenue.md" in out


def test_list_context_files_skips_unknown_suffixes(tmp_path, ctx):
    ctx_dir = tmp_path / "context"
    ctx_dir.mkdir()
    (ctx_dir / "ok.md").write_text("# ok\n")
    (ctx_dir / "skip.pdf").write_bytes(b"%PDF")
    (ctx_dir / "secret.env").write_text("API_KEY=...\n")
    out = list_context_files()
    assert "context/ok.md" in out
    assert "skip.pdf" not in out
    assert "secret.env" not in out


# ---------------------------------------------------------------------------
# write_project_file
# ---------------------------------------------------------------------------


def test_write_project_file_happy_path(tmp_path, ctx):
    out = write_project_file("glossary.md", "# Glossary\n\nRevenue = gmv.\n")
    assert "Wrote `context/glossary.md`" in out
    assert (tmp_path / "context" / "glossary.md").exists()
    assert (
        "Revenue = gmv"
        in (tmp_path / "context" / "glossary.md").read_text()
    )


def test_write_project_file_nested_path(tmp_path, ctx):
    out = write_project_file(
        "metric_definitions/revenue.md", "## Revenue\n\nSUM(gmv).\n"
    )
    assert "Wrote" in out
    assert (tmp_path / "context" / "metric_definitions" / "revenue.md").exists()


def test_write_project_file_rejects_path_traversal(tmp_path, ctx):
    for bad in (
        "../secret.md",
        "subdir/../../etc/passwd.md",
        "..\\winpath.md",
    ):
        out = write_project_file(bad, "x")
        assert out.startswith("Error:")
        assert "traversal" in out.lower() or "invalid path" in out.lower()


def test_write_project_file_rejects_absolute_path(tmp_path, ctx):
    out = write_project_file("/etc/passwd.md", "x")
    assert out.startswith("Error:")


def test_write_project_file_rejects_unsupported_extension(tmp_path, ctx):
    out = write_project_file("payload.sh", "echo hi")
    assert out.startswith("Error:")
    assert "unsupported extension" in out.lower()


def test_write_project_file_rejects_oversized_content(tmp_path, ctx, monkeypatch):
    monkeypatch.setattr(
        "seeknal.ask.agents.tools.write_project_file._MAX_CONTENT_BYTES", 100
    )
    out = write_project_file("big.md", "x" * 200)
    assert out.startswith("Error:")
    assert "exceeds" in out.lower()


def test_write_project_file_rejects_empty_content(tmp_path, ctx):
    assert write_project_file("x.md", "").startswith("Error:")
    assert write_project_file("x.md", "   \n  \n").startswith("Error:")


# ---------------------------------------------------------------------------
# save_preference
# ---------------------------------------------------------------------------


def test_save_preference_creates_yaml(tmp_path, ctx):
    out = save_preference("Revenue means gmv column, not revenue_column.")
    assert "Saved to `preferences.yml`" in out
    pf = tmp_path / "preferences.yml"
    assert pf.exists()
    content = pf.read_text()
    assert "preferences:" in content
    assert "Revenue means gmv" in content


def test_save_preference_appends_subsequent_entries(tmp_path, ctx):
    save_preference("First rule.")
    save_preference("Second rule.")
    prefs = load_preferences(tmp_path)
    assert prefs == ["First rule.", "Second rule."]


def test_save_preference_deduplicates(tmp_path, ctx):
    save_preference("Same rule.")
    out = save_preference("Same rule.")
    assert "already present" in out
    prefs = load_preferences(tmp_path)
    assert prefs == ["Same rule."]


def test_save_preference_rejects_too_short(ctx):
    assert save_preference("ok").startswith("Error:")


def test_save_preference_rejects_empty(ctx):
    assert save_preference("").startswith("Error:")
    assert save_preference("   ").startswith("Error:")


def test_save_preference_rejects_oversized(ctx, monkeypatch):
    monkeypatch.setattr(
        "seeknal.ask.agents.tools.save_preference._PREF_MAX_LEN", 20
    )
    out = save_preference("This is a very long preference that exceeds the limit")
    assert out.startswith("Error:")


def test_save_preference_escapes_yaml_specials(tmp_path, ctx):
    save_preference('Use path "C:\\logs\\app" for Windows.')
    prefs = load_preferences(tmp_path)
    # Round-trip unescaped
    assert prefs == ['Use path "C:\\logs\\app" for Windows.']


# ---------------------------------------------------------------------------
# load_preferences (used by agent.py at session init)
# ---------------------------------------------------------------------------


def test_load_preferences_returns_empty_when_no_file(tmp_path):
    assert load_preferences(tmp_path) == []


def test_load_preferences_tolerates_malformed_file(tmp_path):
    (tmp_path / "preferences.yml").write_text("not actually yaml: [unclosed\n")
    # Must not raise — returns [] instead of crashing the agent
    result = load_preferences(tmp_path)
    assert isinstance(result, list)


def test_load_preferences_handles_unquoted_entries(tmp_path):
    (tmp_path / "preferences.yml").write_text(
        "preferences:\n  - Plain unquoted rule\n  - Another rule\n"
    )
    assert load_preferences(tmp_path) == ["Plain unquoted rule", "Another rule"]

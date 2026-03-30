"""Tests for MemoryStore — file-based agent memory."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from seeknal.ask.memory.store import MemoryStore


@pytest.fixture
def memory_store(tmp_path: Path) -> MemoryStore:
    return MemoryStore(tmp_path)


class TestWrite:
    def test_creates_daily_log(self, memory_store: MemoryStore, tmp_path: Path):
        memory_store.write("schema uses customer_id as PK", "project_knowledge")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily = tmp_path / ".seeknal" / "memory" / f"{today}.md"
        assert daily.exists()
        content = daily.read_text()
        assert "project_knowledge" in content
        assert "customer_id" in content

    def test_creates_memory_md_index(self, memory_store: MemoryStore, tmp_path: Path):
        memory_store.write("user prefers snake_case", "user_preferences")
        md = tmp_path / ".seeknal" / "memory" / "MEMORY.md"
        assert md.exists()
        content = md.read_text()
        assert "user_preferences" in content
        assert "snake_case" in content

    def test_appends_multiple_entries(self, memory_store: MemoryStore, tmp_path: Path):
        memory_store.write("first entry", "project_knowledge")
        memory_store.write("second entry", "operational_history")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily = tmp_path / ".seeknal" / "memory" / f"{today}.md"
        content = daily.read_text()
        assert "first entry" in content
        assert "second entry" in content
        assert "project_knowledge" in content
        assert "operational_history" in content

    def test_invalid_category_defaults_to_project_knowledge(
        self, memory_store: MemoryStore, tmp_path: Path,
    ):
        memory_store.write("test content", "invalid_category")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily = tmp_path / ".seeknal" / "memory" / f"{today}.md"
        content = daily.read_text()
        assert "project_knowledge" in content

    def test_returns_confirmation(self, memory_store: MemoryStore):
        result = memory_store.write("some memory", "project_knowledge")
        assert "Memory saved" in result
        assert "project_knowledge" in result


class TestSearch:
    def test_finds_by_keyword(self, memory_store: MemoryStore):
        memory_store.write("customer_id is the primary key", "project_knowledge")
        results = memory_store.search("customer_id")
        assert len(results) > 0
        assert any("customer_id" in r for r in results)

    def test_returns_empty_for_no_match(self, memory_store: MemoryStore):
        memory_store.write("something unrelated", "project_knowledge")
        results = memory_store.search("nonexistent_term_xyz")
        assert results == []

    def test_respects_max_results(self, memory_store: MemoryStore):
        for i in range(20):
            memory_store.write(f"entry about topic {i}", "project_knowledge")
        results = memory_store.search("topic", max_results=5)
        assert len(results) <= 5

    def test_empty_query_returns_empty(self, memory_store: MemoryStore):
        memory_store.write("some content", "project_knowledge")
        results = memory_store.search("")
        assert results == []


class TestLoadContext:
    def test_returns_empty_for_new_store(self, memory_store: MemoryStore):
        assert memory_store.load_context() == ""

    def test_includes_memory_md(self, memory_store: MemoryStore):
        memory_store.write("important fact", "project_knowledge")
        context = memory_store.load_context()
        assert "Agent Memory" in context
        assert "important fact" in context

    def test_includes_today_daily_log(self, memory_store: MemoryStore):
        memory_store.write("today's learning", "project_knowledge")
        context = memory_store.load_context()
        assert "today's learning" in context


class TestDirectoryCreation:
    def test_auto_creates_memory_directory(self, tmp_path: Path):
        store = MemoryStore(tmp_path)
        assert (tmp_path / ".seeknal" / "memory").is_dir()
        # Can write without error
        store.write("test", "project_knowledge")

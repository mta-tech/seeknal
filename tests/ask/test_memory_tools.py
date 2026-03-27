"""Tests for memory_write and memory_search agent tools."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from seeknal.ask.memory.store import MemoryStore

_CTX_PATH = "seeknal.ask.agents.tools._context.get_tool_context"


@pytest.fixture
def memory_store(tmp_path: Path) -> MemoryStore:
    return MemoryStore(tmp_path)


@pytest.fixture
def mock_ctx(memory_store: MemoryStore):
    ctx = MagicMock()
    ctx.memory_store = memory_store
    return ctx


class TestMemoryWrite:
    def test_writes_and_returns_confirmation(self, mock_ctx):
        with patch(_CTX_PATH, return_value=mock_ctx):
            from seeknal.ask.agents.tools.memory_write import memory_write

            result = memory_write.invoke(
                {"content": "customer_id is the primary key", "category": "project_knowledge"},
            )
            assert "Memory saved" in result
            assert "project_knowledge" in result

    def test_no_memory_store_returns_message(self):
        ctx = MagicMock()
        ctx.memory_store = None
        with patch(_CTX_PATH, return_value=ctx):
            from seeknal.ask.agents.tools.memory_write import memory_write

            result = memory_write.invoke(
                {"content": "test", "category": "project_knowledge"},
            )
            assert "not available" in result


class TestMemorySearch:
    def test_finds_matching_memories(self, mock_ctx, memory_store):
        memory_store.write("customer_id is primary key", "project_knowledge")
        with patch(_CTX_PATH, return_value=mock_ctx):
            from seeknal.ask.agents.tools.memory_search import memory_search

            result = memory_search.invoke({"query": "customer_id"})
            assert "customer_id" in result
            assert "Found" in result

    def test_no_match_returns_message(self, mock_ctx):
        with patch(_CTX_PATH, return_value=mock_ctx):
            from seeknal.ask.agents.tools.memory_search import memory_search

            result = memory_search.invoke({"query": "nonexistent_xyz"})
            assert "No memories found" in result

    def test_no_memory_store_returns_message(self):
        ctx = MagicMock()
        ctx.memory_store = None
        with patch(_CTX_PATH, return_value=ctx):
            from seeknal.ask.agents.tools.memory_search import memory_search

            result = memory_search.invoke({"query": "test"})
            assert "not available" in result


class TestProfileRegistration:
    def test_memory_tools_in_analysis_profile(self):
        from seeknal.ask.agents.profiles import get_tools_for_profile

        tools = get_tools_for_profile("analysis")
        names = {t.name for t in tools}
        assert "memory_write" in names
        assert "memory_search" in names

    def test_memory_tools_in_build_profile(self):
        from seeknal.ask.agents.profiles import get_tools_for_profile

        tools = get_tools_for_profile("build")
        names = {t.name for t in tools}
        assert "memory_write" in names
        assert "memory_search" in names

    def test_memory_tools_in_full_profile(self):
        from seeknal.ask.agents.profiles import get_tools_for_profile

        tools = get_tools_for_profile("full")
        names = {t.name for t in tools}
        assert "memory_write" in names
        assert "memory_search" in names

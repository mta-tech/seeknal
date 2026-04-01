"""Tests for SeeknaContextToolset."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from seeknal.ask.agents.context_toolset import SeeknaContextToolset


@pytest.fixture
def mock_discovery():
    """Create a mock ArtifactDiscovery."""
    discovery = MagicMock()
    discovery.get_context_for_prompt.return_value = (
        "### Entities\n- user (user_id)\n\n### Tables\n- transactions"
    )
    return discovery


@pytest.fixture
def empty_discovery():
    """Create a mock ArtifactDiscovery that returns empty context."""
    discovery = MagicMock()
    discovery.get_context_for_prompt.return_value = ""
    return discovery


@pytest.fixture
def mock_ctx():
    """Create a mock RunContext."""
    ctx = MagicMock()
    return ctx


class TestSeeknaContextToolset:
    def test_get_instructions_returns_context(self, mock_discovery, mock_ctx):
        toolset = SeeknaContextToolset(mock_discovery)
        result = toolset.get_instructions(mock_ctx)

        assert result is not None
        assert "## Data Context" in result
        assert "Entities" in result
        assert "user" in result
        assert "transactions" in result

    def test_get_instructions_calls_discovery(self, mock_discovery, mock_ctx):
        toolset = SeeknaContextToolset(mock_discovery)
        toolset.get_instructions(mock_ctx)

        mock_discovery.get_context_for_prompt.assert_called_once()

    def test_get_instructions_returns_none_for_empty_context(self, empty_discovery, mock_ctx):
        toolset = SeeknaContextToolset(empty_discovery)
        result = toolset.get_instructions(mock_ctx)

        assert result is None

    def test_get_instructions_returns_none_for_whitespace_context(self, mock_ctx):
        discovery = MagicMock()
        discovery.get_context_for_prompt.return_value = "   \n  "
        toolset = SeeknaContextToolset(discovery)
        result = toolset.get_instructions(mock_ctx)

        assert result is None

    def test_toolset_id(self, mock_discovery):
        toolset = SeeknaContextToolset(mock_discovery)
        assert toolset.id == "seeknal-context"

    def test_toolset_has_no_tools(self, mock_discovery):
        toolset = SeeknaContextToolset(mock_discovery)
        # FunctionToolset with no tools registered should have empty tools list
        assert len(list(toolset.tools)) == 0

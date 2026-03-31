"""Tests for project memory integration in seeknal ask agent."""

import pytest

from seeknal.ask.agents.agent import SYSTEM_PROMPT


class TestMemorySystemPromptGuidance:
    """Verify the system prompt includes memory guidance."""

    def test_has_memory_instruction(self):
        assert "memory" in SYSTEM_PROMPT.lower()

    def test_has_save_to_memory_guidance(self):
        assert "save them to memory" in SYSTEM_PROMPT


class TestMemoryAgentConfiguration:
    """Verify create_agent enables memory with correct parameters."""

    def test_create_agent_passes_include_memory_true(self):
        """Verify create_deep_agent is called with include_memory=True."""
        from unittest.mock import MagicMock, patch

        with (
            patch("seeknal.cli.repl.REPL") as mock_repl_cls,
            patch("seeknal.ask.security.configure_safe_connection"),
            patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
            patch("seeknal.ask.agents.tools._context.set_tool_context"),
            patch("seeknal.ask.agents.providers.get_model_string", return_value="test:model"),
            patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
            patch("seeknal.ask.agents.hooks.get_security_hooks", return_value=[]),
            patch("seeknal.ask.agents.skills.get_ask_skills", return_value=[]),
            patch("seeknal.ask.agents.tools.toolset.create_ask_toolset"),
            patch("seeknal.ask.processors.MicrocompactProcessor"),
            patch("seeknal.ask.processors.SqlResultCompactor"),
            patch("pydantic_deep.create_deep_agent") as mock_create,
            patch("pydantic_deep.DeepAgentDeps") as mock_deps,
            patch("pathlib.Path.mkdir"),
        ):
            mock_repl_cls.return_value = MagicMock()
            mock_create.return_value = MagicMock()
            mock_deps.return_value = MagicMock()

            from pathlib import Path
            from seeknal.ask.agents.agent import create_agent

            create_agent(project_path=Path("/tmp/test_project"))

            # Verify create_deep_agent was called with memory enabled
            call_kwargs = mock_create.call_args[1]
            assert call_kwargs["include_memory"] is True
            assert call_kwargs["memory_dir"] == ".seeknal/ask_memory"

    def test_create_agent_passes_history_processors(self):
        """Verify create_deep_agent receives history_processors."""
        from unittest.mock import MagicMock, patch

        with (
            patch("seeknal.cli.repl.REPL") as mock_repl_cls,
            patch("seeknal.ask.security.configure_safe_connection"),
            patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
            patch("seeknal.ask.agents.tools._context.set_tool_context"),
            patch("seeknal.ask.agents.providers.get_model_string", return_value="test:model"),
            patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
            patch("seeknal.ask.agents.hooks.get_security_hooks", return_value=[]),
            patch("seeknal.ask.agents.skills.get_ask_skills", return_value=[]),
            patch("seeknal.ask.agents.tools.toolset.create_ask_toolset"),
            patch("seeknal.ask.processors.MicrocompactProcessor") as mock_micro,
            patch("seeknal.ask.processors.SqlResultCompactor") as mock_sql,
            patch("pydantic_deep.create_deep_agent") as mock_create,
            patch("pydantic_deep.DeepAgentDeps") as mock_deps,
            patch("pathlib.Path.mkdir"),
        ):
            mock_repl_cls.return_value = MagicMock()
            mock_create.return_value = MagicMock()
            mock_deps.return_value = MagicMock()

            from pathlib import Path
            from seeknal.ask.agents.agent import create_agent

            create_agent(project_path=Path("/tmp/test_project"))

            call_kwargs = mock_create.call_args[1]
            processors = call_kwargs["history_processors"]
            assert len(processors) == 2

    def test_seeknal_directory_created(self, tmp_path):
        """Verify .seeknal/ directory is created."""
        from unittest.mock import MagicMock, patch

        project_dir = tmp_path / "my_project"
        project_dir.mkdir()

        with (
            patch("seeknal.cli.repl.REPL") as mock_repl_cls,
            patch("seeknal.ask.security.configure_safe_connection"),
            patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
            patch("seeknal.ask.agents.tools._context.set_tool_context"),
            patch("seeknal.ask.agents.providers.get_model_string", return_value="test:model"),
            patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
            patch("seeknal.ask.agents.hooks.get_security_hooks", return_value=[]),
            patch("seeknal.ask.agents.skills.get_ask_skills", return_value=[]),
            patch("seeknal.ask.agents.tools.toolset.create_ask_toolset"),
            patch("seeknal.ask.processors.MicrocompactProcessor"),
            patch("seeknal.ask.processors.SqlResultCompactor"),
            patch("pydantic_deep.create_deep_agent") as mock_create,
            patch("pydantic_deep.DeepAgentDeps") as mock_deps,
        ):
            mock_repl_cls.return_value = MagicMock()
            mock_create.return_value = MagicMock()
            mock_deps.return_value = MagicMock()

            from seeknal.ask.agents.agent import create_agent

            create_agent(project_path=project_dir)

            # Verify .seeknal directory was created
            assert (project_dir / ".seeknal").exists()
            assert (project_dir / ".seeknal").is_dir()

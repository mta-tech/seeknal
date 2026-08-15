"""Tests for Ask toolset routing surfaces."""

from __future__ import annotations

from pathlib import Path
import textwrap

from seeknal.ask.config import get_ask_toolset_mode, load_agent_config
from seeknal.ask.agents.agent import _build_connected_source_context
from seeknal.ask.agents.tools.toolset import create_ask_toolset


def _tool_names(toolset) -> set[str]:
    return set(toolset.tools.keys())


class _SourceContextRepl:
    attached = {"wh"}

    def execute_oneshot(self, sql: str, limit=None):
        normalized = " ".join(sql.split()).lower()
        if '"wh".information_schema.tables' in normalized:
            return ["table_schema", "table_name", "table_type"], [
                ("analytics", "orders", "BASE TABLE"),
                ("analytics", "monthly_revenue", "VIEW"),
            ]
        if '"wh".information_schema.columns' in normalized and "orders" in normalized:
            return ["column_name", "data_type"], [
                ("order_id", "INTEGER"),
                ("revenue", "DECIMAL(12,2)"),
            ]
        if (
            '"wh".information_schema.columns' in normalized
            and "monthly_revenue" in normalized
        ):
            return ["column_name", "data_type"], [
                ("month", "DATE"),
                ("segment", "VARCHAR"),
                ("revenue", "DECIMAL(12,2)"),
            ]
        raise AssertionError(f"unexpected SQL: {sql}")


def test_analysis_toolset_keeps_database_read_only_but_allows_project_memory():
    names = _tool_names(
        create_ask_toolset(
            mode="analysis",
            include_ask_user=False,
            include_intel_knowledge=True,
        )
    )

    assert {"execute_sql", "list_tables", "describe_table", "execute_python"} <= names
    assert {
        "list_source_context",
        "read_source_context",
        "list_sql_pairs",
        "execute_sql_pair",
        "read_sql_pair",
    } <= names
    assert {
        "intel_knowledge_list",
        "intel_knowledge_search",
        "intel_knowledge_read",
    } <= names
    assert {"list_ask_tests", "read_ask_test", "run_ask_test"} <= names
    assert {"list_ask_test_results", "read_ask_test_result"} <= names
    assert {
        "list_context_files",
        "read_project_file",
        "write_project_file",
        "save_preference",
    } <= names
    assert "ask_user" not in names
    assert "read_pipeline" not in names
    assert "search_pipelines" not in names
    assert "get_entities" not in names
    assert "get_entity_schema" not in names
    assert "run_pipeline" not in names
    assert "draft_node" not in names
    assert "write_ingested_table" not in names
    assert "generate_report" not in names
    assert "publish_to_seeknal_report" not in names


def test_connected_source_context_includes_attached_tables_and_columns():
    context = _build_connected_source_context(_SourceContextRepl())

    assert context is not None
    assert "wh.analytics.orders" in context
    assert "order_id INTEGER" in context
    assert "wh.analytics.monthly_revenue" in context
    assert "segment VARCHAR" in context


def test_full_toolset_keeps_legacy_tools_and_can_omit_ask_user():
    names = _tool_names(
        create_ask_toolset(
            mode="full",
            include_ask_user=False,
            include_intel_knowledge=True,
        )
    )

    assert "execute_sql" in names
    assert "run_pipeline" in names
    assert "write_project_file" in names
    assert "generate_report" in names
    assert "intel_knowledge_list" in names
    assert "intel_knowledge_search" in names
    assert "intel_knowledge_read" in names
    assert "ask_user" not in names


def test_toolset_secure_default_omits_laptop_intel_credential_tools():
    names = _tool_names(create_ask_toolset(mode="full", include_ask_user=False))

    assert "intel_knowledge_list" not in names
    assert "intel_knowledge_search" not in names
    assert "intel_knowledge_read" not in names


def test_connected_auto_config_routes_to_analysis_toolset(tmp_path: Path):
    (tmp_path / "seeknal_agent.yml").write_text(
        textwrap.dedent(
            """\
            mode:
              default: auto
            sources:
              warehouse:
                source_kind: connected
                source_type: database
                connector: postgresql
                namespace: warehouse
                access: read_only
                role: business_source_of_truth
            """
        )
    )

    assert get_ask_toolset_mode(load_agent_config(tmp_path)) == "analysis"


def test_legacy_project_without_source_registry_keeps_full_toolset():
    assert get_ask_toolset_mode({}) == "full"


def test_create_agent_omits_ask_user_in_gateway_and_uses_analysis_for_connected_source(
    tmp_path: Path,
):
    from unittest.mock import MagicMock, patch

    (tmp_path / "seeknal_agent.yml").write_text(
        textwrap.dedent(
            """\
            mode:
              default: auto
            sources:
              warehouse:
                source_kind: connected
                source_type: database
                connector: postgresql
                namespace: warehouse
                access: read_only
                role: business_source_of_truth
            """
        )
    )

    with (
        patch("seeknal.cli.repl.REPL") as mock_repl_cls,
        patch("seeknal.ask.security.configure_safe_connection"),
        patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
        patch(
            "seeknal.ask.agents.providers.get_model_string", return_value="test:model"
        ),
        patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
        patch("seeknal.ask.agents.tools.toolset.create_ask_toolset") as mock_toolset,
        patch("seeknal.ask.processors.MicrocompactProcessor"),
        patch("seeknal.ask.processors.SqlResultCompactor"),
        patch("pydantic_deep.create_deep_agent") as mock_create,
        patch("pydantic_deep.DeepAgentDeps") as mock_deps,
    ):
        mock_repl_cls.return_value = MagicMock(conn=MagicMock())
        mock_toolset.return_value = MagicMock()
        mock_create.return_value = MagicMock()
        mock_deps.return_value = MagicMock()

        from seeknal.ask.agents.agent import create_agent

        create_agent(project_path=tmp_path, environment="gateway")

        assert mock_toolset.call_count == 1
        assert mock_toolset.call_args.kwargs["mode"] == "analysis"
        assert mock_toolset.call_args.kwargs["include_ask_user"] is False
        assert mock_toolset.call_args.kwargs["include_intel_knowledge"] is False
        assert mock_deps.call_args[1]["ask_user"] is None


def test_create_agent_includes_ask_user_in_interactive_analysis_mode(tmp_path: Path):
    from unittest.mock import MagicMock, patch

    (tmp_path / "seeknal_agent.yml").write_text(
        textwrap.dedent(
            """\
            mode:
              default: auto
            sources:
              warehouse:
                source_kind: connected
                source_type: database
                connector: postgresql
                namespace: warehouse
                access: read_only
                role: business_source_of_truth
            """
        )
    )

    with (
        patch("seeknal.cli.repl.REPL") as mock_repl_cls,
        patch("seeknal.ask.security.configure_safe_connection"),
        patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
        patch(
            "seeknal.ask.agents.providers.get_model_string", return_value="test:model"
        ),
        patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
        patch("seeknal.ask.agents.tools.toolset.create_ask_toolset") as mock_toolset,
        patch("seeknal.ask.processors.MicrocompactProcessor") as mock_micro,
        patch("seeknal.ask.processors.SqlResultCompactor") as mock_sql,
        patch("pydantic_deep.create_deep_agent") as mock_create,
        patch("pydantic_deep.DeepAgentDeps") as mock_deps,
    ):
        mock_repl_cls.return_value = MagicMock(conn=MagicMock())
        mock_toolset.return_value = MagicMock()
        mock_create.return_value = MagicMock()
        mock_deps.return_value = MagicMock()

        from seeknal.ask.agents.agent import create_agent

        create_agent(project_path=tmp_path, environment="interactive")

        assert mock_toolset.call_count == 1
        assert mock_toolset.call_args.kwargs["mode"] == "analysis"
        assert mock_toolset.call_args.kwargs["include_ask_user"] is True
        assert mock_toolset.call_args.kwargs["include_intel_knowledge"] is True
        assert mock_deps.call_args[1]["ask_user"] is not None
        assert mock_create.call_args[1]["include_memory"] is False
        assert mock_create.call_args[1]["include_todo"] is False
        assert mock_create.call_args[1]["include_subagents"] is False
        instructions = mock_create.call_args[1]["instructions"]
        assert "Teach mode" in instructions
        assert "save_preference" in instructions
        assert "context/sql_pairs/<slug>.yml" in instructions
        # auto_summarization defaults OFF, so the compaction processors are not
        # wired; only the trailing-ModelRequest guard (always registered last) is.
        from seeknal.ask.processors import ensure_trailing_model_request

        assert mock_create.call_args[1]["history_processors"] == [
            ensure_trailing_model_request,
        ]
        mock_micro.assert_not_called()
        mock_sql.assert_not_called()


def test_create_agent_applies_agent_harness_config(tmp_path: Path):
    from unittest.mock import MagicMock, patch

    (tmp_path / "seeknal_agent.yml").write_text(
        textwrap.dedent(
            """\
            agent_harness:
              auto_summarization:
                enabled: true
                context_manager: false
                context_manager_max_tokens: 128000
                summarization_model: openai:gpt-4.1-mini
                eviction_token_limit: 50000
                patch_tool_calls: false
                microcompact:
                  keep_recent_turns_full: 5
                sql_result_compactor:
                  min_chars_full: 750
              cost_tracking:
                enabled: true
                budget_usd: 2.5
              hooks:
                sql_self_correction: false
              plan:
                enabled: false
                plans_dir: .seeknal/custom-plans
              stuck_loop_detection:
                enabled: false
              subagents:
                enabled: false
              teams:
                enabled: true
            """
        )
    )

    with (
        patch("seeknal.cli.repl.REPL") as mock_repl_cls,
        patch("seeknal.ask.security.configure_safe_connection"),
        patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
        patch(
            "seeknal.ask.agents.providers.get_model_string", return_value="test:model"
        ),
        patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
        patch("seeknal.ask.agents.tools.toolset.create_ask_toolset") as mock_toolset,
        patch("seeknal.ask.processors.MicrocompactProcessor") as mock_micro,
        patch("seeknal.ask.processors.SqlResultCompactor") as mock_sql,
        patch("pydantic_deep.create_deep_agent") as mock_create,
        patch("pydantic_deep.DeepAgentDeps") as mock_deps,
    ):
        mock_repl_cls.return_value = MagicMock(conn=MagicMock())
        mock_toolset.return_value = MagicMock()
        mock_create.return_value = MagicMock()
        mock_deps.return_value = MagicMock()

        from seeknal.ask.agents.agent import create_agent

        create_agent(project_path=tmp_path, environment="interactive")

        kwargs = mock_create.call_args[1]
        assert kwargs["context_manager"] is False
        assert kwargs["context_manager_max_tokens"] == 128000
        assert kwargs["summarization_model"] == "openai:gpt-4.1-mini"
        assert kwargs["eviction_token_limit"] == 50000
        assert kwargs["patch_tool_calls"] is False
        assert kwargs["include_plan"] is False
        assert kwargs["plans_dir"] == ".seeknal/custom-plans"
        assert kwargs["stuck_loop_detection"] is False
        assert kwargs["include_subagents"] is False
        assert kwargs["include_teams"] is False
        assert kwargs["include_builtin_subagents"] is False
        assert kwargs["cost_tracking"] is True
        assert kwargs["cost_budget_usd"] == 2.5
        # sql_self_correction disabled above; the csv_upload_reminder hook
        # was retired entirely — only sql_security remains.
        assert len(kwargs["hooks"]) == 1
        assert kwargs["hooks"][0].event.value == "pre_tool_use"
        # The trailing-ModelRequest guard is always appended LAST, after the
        # configured compaction processors.
        from seeknal.ask.processors import ensure_trailing_model_request

        assert kwargs["history_processors"] == [
            mock_micro.return_value,
            mock_sql.return_value,
            ensure_trailing_model_request,
        ]
        mock_micro.assert_called_once_with(keep_recent_turns=5)
        mock_sql.assert_called_once_with(min_chars=750)


def test_create_agent_intel_work_is_prompt_free_and_intel_only(tmp_path: Path):
    from unittest.mock import MagicMock, patch

    with (
        patch("seeknal.cli.repl.REPL") as mock_repl_cls,
        patch("seeknal.ask.security.configure_safe_connection"),
        patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
        patch(
            "seeknal.ask.agents.providers.get_model_string", return_value="test:model"
        ),
        patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
        patch("seeknal.ask.agents.tools.toolset.create_ask_toolset") as mock_toolset,
        patch("seeknal.ask.processors.MicrocompactProcessor"),
        patch("seeknal.ask.processors.SqlResultCompactor"),
        patch("pydantic_deep.create_deep_agent") as mock_create,
        patch("pydantic_deep.DeepAgentDeps") as mock_deps,
    ):
        mock_repl_cls.return_value = MagicMock(conn=MagicMock())
        mock_toolset.return_value = MagicMock()
        mock_create.return_value = MagicMock()
        mock_deps.return_value = MagicMock()

        from seeknal.ask.agents.agent import create_agent

        create_agent(project_path=tmp_path, environment="intel_work")

    toolset_kwargs = mock_toolset.call_args.kwargs
    assert toolset_kwargs["mode"] == "intel_work"
    assert toolset_kwargs["include_ask_user"] is False
    assert toolset_kwargs["include_request_clarification"] is False
    assert toolset_kwargs["include_intel_knowledge"] is True
    assert mock_deps.call_args.kwargs["ask_user"] is None
    agent_kwargs = mock_create.call_args.kwargs
    assert agent_kwargs["include_skills"] is False
    assert agent_kwargs["include_todo"] is False
    assert agent_kwargs["include_plan"] is False
    assert agent_kwargs["include_memory"] is False
    assert agent_kwargs["include_checkpoints"] is False
    assert agent_kwargs["include_subagents"] is False
    assert agent_kwargs["context_files"] == []
    assert agent_kwargs["context_manager"] is False
    assert "Never ask a question" in agent_kwargs["instructions"]

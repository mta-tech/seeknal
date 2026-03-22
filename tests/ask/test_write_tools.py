"""Tests for Ask agent write tools."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context


@pytest.fixture
def project_dir(tmp_path):
    """Create a temporary project with seeknal structure."""
    (tmp_path / "seeknal" / "sources").mkdir(parents=True)
    (tmp_path / "seeknal" / "transforms").mkdir(parents=True)
    (tmp_path / "seeknal_project.yml").write_text("name: test_project")
    return tmp_path


@pytest.fixture
def mock_ctx(project_dir):
    """Set up a mock ToolContext."""
    import threading

    repl = MagicMock()
    discovery = MagicMock()
    ctx = ToolContext(
        repl=repl,
        artifact_discovery=discovery,
        project_path=project_dir,
    )
    set_tool_context(ctx)
    return ctx


class TestDraftNode:
    def test_invalid_node_type(self, mock_ctx):
        from seeknal.ask.agents.tools.draft_node import draft_node

        result = draft_node.invoke({"node_type": "invalid", "name": "test"})
        assert "Invalid node type" in result

    def test_invalid_name(self, mock_ctx):
        from seeknal.ask.agents.tools.draft_node import draft_node

        result = draft_node.invoke({"node_type": "source", "name": "../escape"})
        assert "Invalid name" in result

    def test_creates_draft_file(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.draft_node import draft_node

        result = draft_node.invoke({"node_type": "source", "name": "customers"})
        assert "Created draft" in result
        assert (project_dir / "draft_source_customers.yml").exists()

    def test_refuses_duplicate(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.draft_node import draft_node

        (project_dir / "draft_source_customers.yml").write_text("existing")
        result = draft_node.invoke({"node_type": "source", "name": "customers"})
        assert "already exists" in result


class TestDryRunDraft:
    def test_file_not_found(self, mock_ctx):
        from seeknal.ask.agents.tools.dry_run_draft import dry_run_draft

        result = dry_run_draft.invoke({"file_path": "draft_source_missing.yml"})
        assert "not found" in result

    def test_invalid_draft_path(self, mock_ctx):
        from seeknal.ask.agents.tools.dry_run_draft import dry_run_draft

        result = dry_run_draft.invoke({"file_path": "not_a_draft.txt"})
        assert "Invalid draft filename" in result

    @patch("subprocess.run")
    def test_successful_dry_run(self, mock_run, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.dry_run_draft import dry_run_draft

        draft = project_dir / "draft_source_test.yml"
        draft.write_text("kind: source\nname: test")

        mock_run.return_value = MagicMock(
            returncode=0, stdout="OK", stderr=""
        )

        result = dry_run_draft.invoke({"file_path": "draft_source_test.yml"})
        assert "PASSED" in result

    @patch("subprocess.run")
    def test_failed_dry_run(self, mock_run, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.dry_run_draft import dry_run_draft

        draft = project_dir / "draft_source_test.yml"
        draft.write_text("invalid yaml")

        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Schema error"
        )

        result = dry_run_draft.invoke({"file_path": "draft_source_test.yml"})
        assert "FAILED" in result


class TestApplyDraft:
    def test_requires_confirmation(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.apply_draft import apply_draft

        draft = project_dir / "draft_source_test.yml"
        draft.write_text("kind: source\nname: test")

        result = apply_draft.invoke({
            "file_path": "draft_source_test.yml",
            "confirmed": False,
        })
        assert "confirmed=True" in result
        assert "Ready to apply" in result

    @patch("subprocess.run")
    def test_apply_with_confirmation(self, mock_run, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.apply_draft import apply_draft

        draft = project_dir / "draft_source_test.yml"
        draft.write_text("kind: source\nname: test")

        mock_run.return_value = MagicMock(
            returncode=0, stdout="Applied successfully", stderr=""
        )

        result = apply_draft.invoke({
            "file_path": "draft_source_test.yml",
            "confirmed": True,
        })
        assert "successfully" in result


class TestEditNode:
    def test_file_not_found(self, mock_ctx):
        from seeknal.ask.agents.tools.edit_node import edit_node

        result = edit_node.invoke({
            "node_path": "seeknal/transforms/missing.yml",
            "new_content": "test",
        })
        assert "not found" in result

    def test_shows_diff_without_confirmation(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.edit_node import edit_node

        node_file = project_dir / "seeknal" / "transforms" / "test.yml"
        node_file.write_text("old content")

        result = edit_node.invoke({
            "node_path": "seeknal/transforms/test.yml",
            "new_content": "new content",
            "confirmed": False,
        })
        assert "diff" in result.lower()
        assert "confirmed=True" in result

    def test_no_changes_detected(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.edit_node import edit_node

        node_file = project_dir / "seeknal" / "transforms" / "test.yml"
        node_file.write_text("same content")

        result = edit_node.invoke({
            "node_path": "seeknal/transforms/test.yml",
            "new_content": "same content",
            "confirmed": False,
        })
        assert "No changes" in result

    def test_applies_with_confirmation(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.edit_node import edit_node

        node_file = project_dir / "seeknal" / "transforms" / "test.yml"
        node_file.write_text("old content")

        result = edit_node.invoke({
            "node_path": "seeknal/transforms/test.yml",
            "new_content": "new content",
            "confirmed": True,
        })
        assert "Updated" in result
        assert node_file.read_text() == "new content"


class TestPlanPipeline:
    @patch("subprocess.run")
    def test_returns_plan(self, mock_run, mock_ctx):
        from seeknal.ask.agents.tools.plan_pipeline import plan_pipeline

        mock_run.return_value = MagicMock(
            returncode=0, stdout="1. source.a\n2. transform.b", stderr=""
        )

        result = plan_pipeline.invoke({})
        assert "Execution Plan" in result


class TestShowLineage:
    @patch("subprocess.run")
    def test_full_lineage(self, mock_run, mock_ctx):
        from seeknal.ask.agents.tools.show_lineage import show_lineage

        mock_run.return_value = MagicMock(
            returncode=0, stdout="source.a -> transform.b", stderr=""
        )

        result = show_lineage.invoke({})
        assert "Full Pipeline Lineage" in result

    @patch("subprocess.run")
    def test_node_focused_lineage(self, mock_run, mock_ctx):
        from seeknal.ask.agents.tools.show_lineage import show_lineage

        mock_run.return_value = MagicMock(
            returncode=0, stdout="source.a -> transform.b", stderr=""
        )

        result = show_lineage.invoke({"node_id": "transform.b"})
        assert "transform.b" in result


class TestRunPipeline:
    def test_requires_confirmation(self, mock_ctx):
        from seeknal.ask.agents.tools.run_pipeline import run_pipeline

        result = run_pipeline.invoke({"confirmed": False})
        assert "confirmed=True" in result
        assert "Ready to execute" in result

    @patch("subprocess.run")
    def test_runs_with_confirmation(self, mock_run, mock_ctx):
        from seeknal.ask.agents.tools.run_pipeline import run_pipeline

        mock_run.return_value = MagicMock(
            returncode=0, stdout="All nodes executed", stderr=""
        )

        result = run_pipeline.invoke({"confirmed": True})
        assert "successfully" in result

"""Tests for project-setup asset writer."""

from pathlib import Path
from unittest.mock import MagicMock

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.write_seeknal_project_asset import (
    write_seeknal_project_asset,
)


def _set_context(project_path: Path) -> None:
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=project_path,
        )
    )


def test_write_seeknal_project_asset_allows_sql_pair(tmp_path: Path):
    _set_context(tmp_path)

    result = write_seeknal_project_asset(
        "seeknal/sql_pairs/revenue.yml",
        """name: revenue\nprompt: Total revenue\nsql: |\n  SELECT 1 AS revenue\n""",
    )

    assert result.startswith("Wrote `seeknal/sql_pairs/revenue.yml`")
    assert (tmp_path / "seeknal/sql_pairs/revenue.yml").exists()


def test_write_seeknal_project_asset_requires_overwrite(tmp_path: Path):
    _set_context(tmp_path)
    target = tmp_path / "SEEKNAL_ASK.md"
    target.write_text("old")

    result = write_seeknal_project_asset("SEEKNAL_ASK.md", "new")

    assert result.startswith("Error:")
    assert target.read_text() == "old"

    result = write_seeknal_project_asset("SEEKNAL_ASK.md", "new", overwrite=True)

    assert result.startswith("Wrote `SEEKNAL_ASK.md`")
    assert target.read_text() == "new"


def test_write_seeknal_project_asset_rejects_unsupported_and_secret_paths(tmp_path: Path):
    _set_context(tmp_path)

    assert write_seeknal_project_asset("../x.yml", "content").startswith("Error:")
    assert write_seeknal_project_asset("seeknal/sources/orders.yml", "kind: source").startswith("Error:")
    assert write_seeknal_project_asset(
        "context/note.md",
        "database_url: postgresql://user:pass@example/db",
    ).startswith("Error:")

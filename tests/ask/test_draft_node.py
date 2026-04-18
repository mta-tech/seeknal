"""Tests for ask draft_node tool."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import seeknal.ask.agents.tools.draft_node as draft_node_module
from seeknal.workflow.draft import normalize_python_deps


def test_normalize_python_deps_preserves_order_and_includes_seeknal():
    assert normalize_python_deps(["httpx", "pandas"]) == [
        "seeknal",
        "httpx",
        "pandas",
    ]


def test_normalize_python_deps_deduplicates_and_strips_blanks():
    assert normalize_python_deps(["", "seeknal", "httpx", "httpx", " pandas "]) == [
        "seeknal",
        "httpx",
        "pandas",
    ]


def test_draft_node_python_renders_pep723_deps(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    ctx = SimpleNamespace(project_path=tmp_path, fs_lock=SimpleNamespace(__enter__=lambda self: None, __exit__=lambda self, exc_type, exc, tb: False))

    class _Lock:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    ctx.fs_lock = _Lock()

    monkeypatch.setattr(
        "seeknal.ask.agents.tools._context.get_tool_context",
        lambda: ctx,
    )
    monkeypatch.setattr(
        "seeknal.ask.agents.tools._write_security.get_drafts_dir",
        lambda project_path: (project_path / ".seeknal" / "drafts"),
    )

    (tmp_path / ".seeknal" / "drafts").mkdir(parents=True, exist_ok=True)

    result = asyncio.run(
        draft_node_module.draft_node(
            "transform",
            "fetch_pages",
            python=True,
            deps=["httpx", "pandas", "httpx"],
        )
    )

    draft_path = tmp_path / ".seeknal" / "drafts" / "draft_transform_fetch_pages.py"
    assert draft_path.exists()
    content = draft_path.read_text()

    assert "Created draft: draft_transform_fetch_pages.py" in result
    assert '#     "seeknal",' in content
    assert '#     "httpx",' in content
    assert '#     "pandas",' in content
    assert content.count('"httpx"') == 1

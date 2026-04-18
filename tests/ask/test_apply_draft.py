"""Tests for apply_draft tool."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from seeknal.ask.agents.tools.apply_draft import apply_draft


class _Lock:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False


def test_apply_draft_semantic_model_moves_to_semantic_models(tmp_path, monkeypatch):
    draft_dir = tmp_path / ".seeknal" / "drafts"
    draft_dir.mkdir(parents=True)
    draft_path = draft_dir / "draft_semantic_model_competitive_signals.yml"
    draft_path.write_text(
        "kind: semantic_model\n"
        "name: competitive_signals\n"
        "description: Test semantic model\n"
        "model: ref('transform.signals_enriched')\n"
    )

    ctx = SimpleNamespace(
        project_path=tmp_path,
        fs_lock=_Lock(),
        artifact_discovery=SimpleNamespace(refresh=lambda: None),
    )
    monkeypatch.setattr(
        "seeknal.ask.agents.tools._context.get_tool_context",
        lambda: ctx,
    )
    monkeypatch.setattr(
        "seeknal.ask.agents.tools._write_security.validate_draft_path",
        lambda file_path, project_path: draft_path,
    )

    result = asyncio.run(
        apply_draft(str(draft_path), confirmed=True)
    )

    target = tmp_path / "seeknal" / "semantic_models" / "competitive_signals.yml"
    assert target.exists()
    assert "seeknal/semantic_models/competitive_signals.yml" in result


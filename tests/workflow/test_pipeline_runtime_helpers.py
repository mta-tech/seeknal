"""Tests for PipelineContext runtime helpers."""

from __future__ import annotations

import json

import pytest

from seeknal.pipeline.context import PipelineContext
from seeknal.pipeline.llm import PipelineLLM


def test_context_get_param_and_metadata(tmp_path):
    ctx = PipelineContext(
        project_path=tmp_path,
        target_dir=tmp_path / "target",
        config={},
        params={"timeout": 30},
        node_id="transform.fetch_pages",
        node_kind="transform",
        node_meta={"params": {"timeout": 30}},
    )

    assert ctx.get_param("timeout") == 30
    assert ctx.get_param("missing", "fallback") == "fallback"
    assert ctx.node_id == "transform.fetch_pages"
    assert ctx.node_kind == "transform"
    assert ctx.node_meta["params"]["timeout"] == 30


def test_pipeline_state_roundtrip(tmp_path):
    ctx = PipelineContext(
        project_path=tmp_path,
        target_dir=tmp_path / "target",
        config={},
        node_id="transform.fetch_pages",
    )

    assert ctx.state.get("etag") is None
    ctx.state.set("etag", "abc123")
    assert ctx.state.get("etag") == "abc123"

    state_path = tmp_path / "target" / "pipeline_state" / "transform_fetch_pages.json"
    assert state_path.exists()
    assert json.loads(state_path.read_text())["etag"] == "abc123"


def test_pipeline_llm_from_context_google(monkeypatch, tmp_path):
    monkeypatch.setenv("SEEKNAL_ASK_LLM_PROVIDER", "google")
    monkeypatch.setenv("SEEKNAL_ASK_MODEL", "gemini-2.5-pro")
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")

    ctx = PipelineContext(project_path=tmp_path, target_dir=tmp_path / "target", config={})
    llm = PipelineLLM.from_context(ctx)

    assert llm.provider == "google"
    assert llm.model == "gemini-2.5-pro"
    assert llm.api_key == "test-key"


def test_pipeline_llm_from_context_prefers_params(monkeypatch, tmp_path):
    monkeypatch.setenv("SEEKNAL_ASK_LLM_PROVIDER", "google")
    monkeypatch.setenv("SEEKNAL_ASK_MODEL", "gemini-2.5-flash")

    ctx = PipelineContext(
        project_path=tmp_path,
        target_dir=tmp_path / "target",
        config={},
        params={
            "llm_provider": "ollama",
            "llm_model": "llama3.2",
            "llm_base_url": "http://ollama.internal:11434",
        },
    )
    llm = PipelineLLM.from_context(ctx)

    assert llm.provider == "ollama"
    assert llm.model == "llama3.2"
    assert llm.base_url == "http://ollama.internal:11434"


def test_pipeline_llm_generate_json_strips_code_fences(monkeypatch):
    llm = PipelineLLM(provider="ollama", model="llama3.1")
    monkeypatch.setattr(
        llm,
        "generate_text",
        lambda prompt: '```json\n{"summary":"ok","confidence":0.9}\n```',
    )

    result = llm.generate_json("hello", {"type": "object"})
    assert result == {"summary": "ok", "confidence": 0.9}


def test_pipeline_llm_google_requires_key(monkeypatch, tmp_path):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    ctx = PipelineContext(
        project_path=tmp_path,
        target_dir=tmp_path / "target",
        config={},
        params={"llm_provider": "google", "llm_model": "gemini-2.5-flash"},
    )
    llm = PipelineLLM.from_context(ctx)

    with pytest.raises(ValueError, match="Google API key required"):
        llm.generate_text("hello")

"""Regression tests for the F1 + F3 Ask harness robustness fixes.

Covers:
- F1a: ensure_trailing_model_request guards pydantic-ai's "history must end with a
       ModelRequest" invariant (the auto_summarization crash).
- F1c: the Ask FunctionToolset gets a real retry budget (max_retries >= 3).
- F1d: auto_summarization defaults OFF, but explicit settings are honored.
- F3 : testing.run_agent_answer seeds the per-turn governor so grounding /
       anti-fabrication guards are active in test mode (the vip_churn hallucination).
"""
from __future__ import annotations

from pathlib import Path

from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

from seeknal.ask.processors import ensure_trailing_model_request


def _req(text: str = "q") -> ModelRequest:
    return ModelRequest(parts=[UserPromptPart(content=text)])


def _resp(text: str = "a") -> ModelResponse:
    return ModelResponse(parts=[TextPart(content=text)])


# --- F1a -------------------------------------------------------------------

def test_guard_trims_trailing_model_response():
    req, resp = _req(), _resp()
    assert ensure_trailing_model_request([req, resp]) == [req]


def test_guard_trims_multiple_trailing_responses():
    req, r1, r2 = _req(), _resp("1"), _resp("2")
    assert ensure_trailing_model_request([req, r1, r2]) == [req]


def test_guard_noop_on_well_formed_history():
    msgs = [_req("a"), _resp("b"), _req("c")]
    assert ensure_trailing_model_request(msgs) == msgs


def test_guard_handles_empty():
    assert ensure_trailing_model_request([]) == []


def test_guard_never_returns_empty():
    # All-response history must not be emptied (would break the run differently).
    only_resp = [_resp()]
    assert ensure_trailing_model_request(only_resp) == only_resp


def test_guard_registered_last_in_history_processors():
    # The guard must run AFTER the compaction processors / summarizer.
    import inspect

    import seeknal.ask.agents.agent as agent_mod

    src = inspect.getsource(agent_mod.create_agent)
    assert "history_processors.append(ensure_trailing_model_request)" in src
    # ...and after the SqlResultCompactor append (so it is the last processor).
    assert src.index("SqlResultCompactor(") < src.index(
        "history_processors.append(ensure_trailing_model_request)"
    )


# --- F1c -------------------------------------------------------------------

def test_ask_toolset_has_retry_budget():
    from seeknal.ask.agents.tools.toolset import create_ask_toolset

    for mode in ("full", "analysis"):
        ts = create_ask_toolset(mode=mode)
        assert ts.max_retries is not None and ts.max_retries >= 3, mode


# --- F1d -------------------------------------------------------------------

def test_auto_summarization_defaults_off_when_omitted():
    from seeknal.ask.config import get_auto_summarization_config

    assert get_auto_summarization_config({})["enabled"] is False


def test_auto_summarization_explicit_settings_honored():
    from seeknal.ask.config import get_auto_summarization_config

    on = {"agent_harness": {"auto_summarization": {"enabled": True}}}
    off = {"agent_harness": {"auto_summarization": {"enabled": False}}}
    assert get_auto_summarization_config(on)["enabled"] is True
    assert get_auto_summarization_config(off)["enabled"] is False


def test_init_template_defaults_auto_summarization_off():
    # The generated seeknal_agent.yml must not ship the crash-prone default ON.
    import inspect

    import seeknal.cli.main as main_mod

    src = inspect.getsource(main_mod)
    block = src[src.index("auto_summarization:"):src.index("context_manager: auto")]
    assert "enabled: false" in block
    assert "enabled: true" not in block


# --- F3 --------------------------------------------------------------------

def test_run_agent_answer_seeds_turn_governor(monkeypatch):
    import seeknal.ask.agents.agent as agent_mod
    import seeknal.ask.agents.tools._context as ctx_mod
    import seeknal.ask.testing as testing

    calls: dict = {}
    monkeypatch.setattr(
        ctx_mod, "reset_turn_governor",
        lambda q=None: calls.__setitem__("governor_question", q),
    )
    monkeypatch.setattr(
        ctx_mod, "reset_report_approval",
        lambda: calls.__setitem__("report_reset", True),
    )
    # Stub agent construction + the agent call; echo the prompt back so we can
    # assert the call order didn't drop the prompt.
    monkeypatch.setattr(
        agent_mod, "create_agent",
        lambda *a, **k: ("AGENT", "DEPS", [], None),
    )
    monkeypatch.setattr(
        agent_mod, "ask",
        lambda agent, deps, history, prompt: f"ANSWER::{prompt}",
    )

    out = testing.run_agent_answer(Path("/tmp/does-not-matter"), "untung hari ini?")

    assert out == "ANSWER::untung hari ini?"
    assert calls.get("governor_question") == "untung hari ini?"
    assert calls.get("report_reset") is True

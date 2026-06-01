"""Tests for the verbatim re-ask short-circuit gate.

When the user asks the IDENTICAL question twice in a row, the agent harness
must short-circuit BEFORE any LLM/tool call and return the prior answer
verbatim — a silent cache, no banner or meta-commentary.
See ``.omc/specs/autopilot-verbatim-restate-gate.md``.
"""
from __future__ import annotations

import asyncio
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from seeknal.ask.agents.tools._context import (
    ToolContext,
    _normalize_question,
    build_verbatim_restate_response,
    lookup_prior_turn_answer,
    record_prior_turn_answer,
    reset_turn_governor,
    seed_prior_turn_from_history,
    set_tool_context,
)


class _Repl:
    """Minimal REPL stand-in matching the _seeknal_* attribute convention."""

    attached: set[str] = set()


def _make_ctx(tmp_path: Path) -> ToolContext:
    repl = _Repl()
    ctx = ToolContext(
        repl=repl,
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(ctx)
    return ctx


def _make_console():
    from rich.console import Console
    from seeknal.ui.theme import DARK_THEME

    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=120, theme=DARK_THEME), buf


# ---------------------------------------------------------------------------
# AC-1..AC-3: _normalize_question
# ---------------------------------------------------------------------------


def test_ac1_normalize_question_lowers_and_strips_trailing_punctuation():
    """AC-1: punctuation/case insensitivity for verbatim comparison."""
    a = _normalize_question("Berapa NIE yang terbit di tahun 2024?")
    b = _normalize_question("berapa nie yang terbit di tahun 2024")
    assert a == b
    assert a == "berapa nie yang terbit di tahun 2024"


def test_ac2_normalize_question_handles_none():
    """AC-2: None input becomes empty string."""
    assert _normalize_question(None) == ""
    assert _normalize_question("") == ""


def test_ac3_normalize_question_collapses_whitespace():
    """AC-3: runs of whitespace collapse to single spaces."""
    assert _normalize_question("  hello  world  ") == "hello world"
    assert _normalize_question("hello\t\nworld") == "hello world"


# ---------------------------------------------------------------------------
# AC-4..AC-8: record_prior_turn_answer / lookup_prior_turn_answer
# ---------------------------------------------------------------------------


def test_ac4_record_prior_turn_answer_stores_on_repl(tmp_path: Path):
    """AC-4: record stores normalized question and verbatim answer on ctx.repl."""
    ctx = _make_ctx(tmp_path)
    record_prior_turn_answer(question="Q1", answer="A1", ctx=ctx)
    assert getattr(ctx.repl, "_seeknal_prior_turn_question") == "q1"
    assert getattr(ctx.repl, "_seeknal_prior_turn_answer") == "A1"


def test_ac5_lookup_returns_prior_answer_on_match(tmp_path: Path):
    """AC-5: lookup returns the prior answer when normalized question matches."""
    ctx = _make_ctx(tmp_path)
    record_prior_turn_answer(question="Q1", answer="A1", ctx=ctx)
    assert lookup_prior_turn_answer("Q1", ctx=ctx) == "A1"
    # Punctuation/case-insensitive
    assert lookup_prior_turn_answer("q1?", ctx=ctx) == "A1"


def test_ac6_lookup_returns_none_when_no_prior(tmp_path: Path):
    """AC-6: lookup returns None when no prior pair was recorded."""
    ctx = _make_ctx(tmp_path)
    assert lookup_prior_turn_answer("Q1", ctx=ctx) is None


def test_ac7_lookup_returns_none_on_different_question(tmp_path: Path):
    """AC-7: lookup returns None when normalized forms differ."""
    ctx = _make_ctx(tmp_path)
    record_prior_turn_answer(question="Q1", answer="A1", ctx=ctx)
    assert lookup_prior_turn_answer("Q2", ctx=ctx) is None


def test_ac8_lookup_returns_none_when_prior_answer_empty(tmp_path: Path):
    """AC-8: lookup returns None when the stored prior answer is empty/whitespace."""
    ctx = _make_ctx(tmp_path)
    # record_prior_turn_answer should refuse to store empty/whitespace.
    record_prior_turn_answer(question="Q1", answer="   ", ctx=ctx)
    assert getattr(ctx.repl, "_seeknal_prior_turn_answer", None) is None
    assert lookup_prior_turn_answer("Q1", ctx=ctx) is None

    # And even if a whitespace answer was somehow set, lookup must not return it.
    setattr(ctx.repl, "_seeknal_prior_turn_question", _normalize_question("Q1"))
    setattr(ctx.repl, "_seeknal_prior_turn_answer", "   ")
    assert lookup_prior_turn_answer("Q1", ctx=ctx) is None


# ---------------------------------------------------------------------------
# AC-13: restate is a silent passthrough (no banner)
# ---------------------------------------------------------------------------


def test_ac13_restate_response_returns_prior_answer_unchanged():
    """The restate response is the prior answer verbatim — no banner, no
    meta-commentary. A verbatim re-ask behaves as a silent cache."""
    restate = build_verbatim_restate_response("Total = 42")
    assert restate == "Total = 42"
    # The old bilingual notice must not reappear.
    assert "giliran sebelumnya" not in restate
    assert "the same as the prior turn" not in restate


# ---------------------------------------------------------------------------
# Cross-cutting: reset_turn_governor must not wipe the prior-turn pair
# ---------------------------------------------------------------------------


def test_reset_turn_governor_preserves_prior_turn_pair(tmp_path: Path):
    """The gate lives on ctx.repl, not on in-turn fields — reset must not clear it."""
    ctx = _make_ctx(tmp_path)
    record_prior_turn_answer(question="Q1", answer="A1", ctx=ctx)
    reset_turn_governor("Q2")
    assert getattr(ctx.repl, "_seeknal_prior_turn_question") == "q1"
    assert getattr(ctx.repl, "_seeknal_prior_turn_answer") == "A1"


# ---------------------------------------------------------------------------
# seed_prior_turn_from_history — gateway REPL-rebuild fix
# ---------------------------------------------------------------------------
# Regression for the gateway bug: create_agent rebuilds the REPL every turn,
# so record_prior_turn_answer's in-memory pair never survives. The verbatim
# gate was permanently dormant on gateway/Telegram sessions. seed_prior_turn_
# from_history rehydrates the pair from the persisted message_history.


def _history_pair(question: str, answer: str) -> list:
    """Build a [ModelRequest, ModelResponse] message_history slice."""
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )

    return [
        ModelRequest(parts=[UserPromptPart(content=question)]),
        ModelResponse(parts=[TextPart(content=answer)]),
    ]


def test_seed_populates_prior_turn_from_history(tmp_path: Path):
    """A fresh REPL with no prior pair gets one seeded from message_history."""
    ctx = _make_ctx(tmp_path)
    history = _history_pair("Berapa NIE 2024?", "Total NIE 2024: 50")

    # Fresh REPL — nothing recorded yet (simulates the per-turn rebuild).
    assert lookup_prior_turn_answer("Berapa NIE 2024?") is None

    seed_prior_turn_from_history(history)

    # After seeding, the verbatim gate's lookup now resolves.
    assert lookup_prior_turn_answer("Berapa NIE 2024?") == "Total NIE 2024: 50"


def test_seed_empty_history_is_noop(tmp_path: Path):
    """No history → nothing seeded, lookup stays None, no crash."""
    _make_ctx(tmp_path)
    seed_prior_turn_from_history([])
    seed_prior_turn_from_history(None)
    assert lookup_prior_turn_answer("anything") is None


def test_seed_triple_ask_does_not_compound(tmp_path: Path):
    """A restate returns the prior answer verbatim (no banner), so seeding
    from a history whose last answer was itself a restate stores the exact
    same canonical text — a triple-ask can never compound."""
    _make_ctx(tmp_path)
    canonical = "Total NIE 2024: 50"
    # A restate of `canonical` is byte-identical to `canonical`.
    restated = build_verbatim_restate_response(canonical)
    assert restated == canonical
    history = _history_pair("Berapa NIE 2024?", restated)

    seed_prior_turn_from_history(history)

    stored = lookup_prior_turn_answer("Berapa NIE 2024?")
    assert stored == canonical


def test_seed_takes_the_most_recent_turn(tmp_path: Path):
    """With multiple turns in history, seeding uses the latest (question, answer)."""
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )

    _make_ctx(tmp_path)
    history = [
        ModelRequest(parts=[UserPromptPart(content="old question")]),
        ModelResponse(parts=[TextPart(content="old answer")]),
        ModelRequest(parts=[UserPromptPart(content="new question")]),
        ModelResponse(parts=[TextPart(content="new answer")]),
    ]
    seed_prior_turn_from_history(history)

    assert lookup_prior_turn_answer("new question") == "new answer"
    assert lookup_prior_turn_answer("old question") is None


# ---------------------------------------------------------------------------
# AC-9..AC-12: integration via stream_ask
# ---------------------------------------------------------------------------


class _StubAgent:
    """Minimal pydantic-ai Agent stand-in that pretends to be the LLM loop."""

    def __init__(self, answer: str = "fresh answer"):
        self.answer = answer
        self.iter_calls = 0
        self.run_calls = 0

    def iter(self, *args, **kwargs):  # pragma: no cover - not used here
        raise AssertionError(
            "_StubAgent.iter() should not be reached when the gate fires"
        )

    async def run(self, *args, **kwargs):
        self.run_calls += 1
        result = MagicMock()
        result.output = self.answer
        result.all_messages = lambda: []
        return result


async def _stream_ask_with_stub_one_pass(
    *,
    ctx: ToolContext,
    questions: list[str],
    one_pass_answer: str = "fresh streamed answer",
    execute_sql_mock: MagicMock | None = None,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[str], int]:
    """Drive stream_ask multiple times with _stream_one_pass mocked out.

    Returns the list of returned answers and the number of times the mocked
    LLM/tool path was entered.
    """
    from seeknal.ask import streaming as streaming_module

    one_pass_calls = {"n": 0}

    async def _fake_stream_one_pass(agent, deps, message_history, question, console,
                                    *, tool_calls_limit_override=None):
        one_pass_calls["n"] += 1
        # Simulate the agent path reaching execute_sql — this is the call we
        # want to assert is NOT reached on a verbatim re-ask short-circuit.
        if execute_sql_mock is not None:
            execute_sql_mock(question)
        return one_pass_answer, []

    async def _fake_quality_gate(agent, deps, message_history, answer, console):
        return answer

    monkeypatch.setattr(streaming_module, "_stream_one_pass", _fake_stream_one_pass)
    monkeypatch.setattr(streaming_module, "_stream_quality_gate", _fake_quality_gate)

    set_tool_context(ctx)

    agent = _StubAgent(answer=one_pass_answer)
    deps = MagicMock()
    message_history: list = []
    console, _buf = _make_console()
    answers: list[str] = []
    for q in questions:
        a = await streaming_module.stream_ask(
            agent, deps, message_history, q, console
        )
        answers.append(a)
    return answers, one_pass_calls["n"]


def test_ac9_verbatim_reask_short_circuits_without_calling_execute_sql(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-9: identical question on turn 2 returns the prior answer verbatim,
    no tool calls — a silent cache, no banner."""
    ctx = _make_ctx(tmp_path)
    execute_sql = MagicMock()

    async def _run():
        return await _stream_ask_with_stub_one_pass(
            ctx=ctx,
            questions=["Berapa NIE yang terbit?", "Berapa NIE yang terbit?"],
            one_pass_answer="A1: 42 NIE",
            execute_sql_mock=execute_sql,
            monkeypatch=monkeypatch,
        )

    answers, one_pass_calls = asyncio.run(_run())

    assert one_pass_calls == 1, (
        "Second turn must NOT enter the LLM/tool loop "
        f"(saw {one_pass_calls} pass calls)"
    )
    assert execute_sql.call_count == 1, (
        "execute_sql must only be called for the FIRST turn"
    )
    # Turn 1 returns the fresh answer; turn 2 returns the SAME answer
    # verbatim — silent cache, no banner / meta-commentary.
    assert "42 NIE" in answers[0]
    assert answers[1] == answers[0]
    assert "Pertanyaan ini sama" not in answers[1]


def test_ac10_different_questions_both_execute_normally(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-10: two distinct questions never short-circuit each other."""
    ctx = _make_ctx(tmp_path)

    async def _run():
        return await _stream_ask_with_stub_one_pass(
            ctx=ctx,
            questions=["Q1?", "Q2?"],
            one_pass_answer="A",
            monkeypatch=monkeypatch,
        )

    answers, one_pass_calls = asyncio.run(_run())

    assert one_pass_calls == 2
    assert "Pertanyaan ini sama" not in answers[0]
    assert "Pertanyaan ini sama" not in answers[1]


def test_ac11_triple_identical_question_short_circuits_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-11: three identical turns -> turn 2 and turn 3 short-circuit."""
    ctx = _make_ctx(tmp_path)
    execute_sql = MagicMock()

    async def _run():
        return await _stream_ask_with_stub_one_pass(
            ctx=ctx,
            questions=["Q1?", "Q1?", "Q1?"],
            one_pass_answer="A1",
            execute_sql_mock=execute_sql,
            monkeypatch=monkeypatch,
        )

    answers, one_pass_calls = asyncio.run(_run())

    assert one_pass_calls == 1, "Only the first turn invokes the agent loop"
    assert execute_sql.call_count == 1
    # Every gated turn returns the prior answer verbatim — no banner.
    assert answers[0] == answers[1] == answers[2] == "A1"


def test_ac12_tool_error_payload_does_not_lock_in_bad_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-12: if the prior answer was a tool-error JSON, the gate must NOT fire."""
    ctx = _make_ctx(tmp_path)

    tool_error_payload = (
        '{"category": "terminal_pg_connect", "retryable": false, '
        '"message": "could not connect to server"}'
    )

    async def _run():
        return await _stream_ask_with_stub_one_pass(
            ctx=ctx,
            questions=["Q1?", "Q1?"],
            one_pass_answer=tool_error_payload,
            monkeypatch=monkeypatch,
        )

    answers, one_pass_calls = asyncio.run(_run())

    # Both turns must hit the agent loop — gate refused to record the error
    # payload, so the second turn has no prior to short-circuit to.
    assert one_pass_calls == 2
    assert "Pertanyaan ini sama" not in answers[1]


def test_ac12b_empty_prior_answer_does_not_lock_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-12 (empty answer variant): empty prior answer is not stored either."""
    ctx = _make_ctx(tmp_path)

    async def _run():
        return await _stream_ask_with_stub_one_pass(
            ctx=ctx,
            questions=["Q1?", "Q1?"],
            one_pass_answer="",  # empty answer from the agent
            monkeypatch=monkeypatch,
        )

    answers, one_pass_calls = asyncio.run(_run())

    # An empty answer means _stream_one_pass falls through to Ralph retries
    # — we just want to assert the gate did NOT short-circuit turn 2 with an
    # empty/whitespace prior.
    assert "Pertanyaan ini sama" not in answers[1]
    assert one_pass_calls > 1


# ---------------------------------------------------------------------------
# AC-14: streaming path appends synthesized request/response to message_history
# ---------------------------------------------------------------------------


def test_ac14_streaming_gate_appends_synthesized_history_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-14: gated turn in stream_ask grows message_history by exactly 2 items.

    The synthesized pair is one ModelRequest (the user's re-ask) and one
    ModelResponse (the bilingual restate). Without this append the next
    turn's persisted history would miss the exchange entirely.
    """
    from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, UserPromptPart

    from seeknal.ask import streaming as streaming_module

    ctx = _make_ctx(tmp_path)

    async def _fake_stream_one_pass(agent, deps, message_history, question, console,
                                    *, tool_calls_limit_override=None):
        # Simulate the real path appending its own request/response.
        message_history.append(ModelRequest(parts=[UserPromptPart(content=question)]))
        message_history.append(ModelResponse(parts=[TextPart(content="A1")]))
        return "A1", []

    async def _fake_quality_gate(agent, deps, message_history, answer, console):
        return answer

    monkeypatch.setattr(streaming_module, "_stream_one_pass", _fake_stream_one_pass)
    monkeypatch.setattr(streaming_module, "_stream_quality_gate", _fake_quality_gate)

    set_tool_context(ctx)
    agent = _StubAgent(answer="A1")
    deps = MagicMock()
    message_history: list = []
    console, _buf = _make_console()

    async def _run():
        # Turn 1 — populates the prior pair.
        await streaming_module.stream_ask(
            agent, deps, message_history, "Q1?", console
        )
        len_after_turn1 = len(message_history)
        # Turn 2 — must short-circuit AND append exactly 2 items.
        await streaming_module.stream_ask(
            agent, deps, message_history, "Q1?", console
        )
        return len_after_turn1

    len_after_turn1 = asyncio.run(_run())
    # Turn 2 grew the list by exactly 2 — one ModelRequest + one ModelResponse.
    assert len(message_history) - len_after_turn1 == 2
    appended_request, appended_response = message_history[-2], message_history[-1]
    assert isinstance(appended_request, ModelRequest)
    assert isinstance(appended_response, ModelResponse)
    # The synthesized request carries the user's re-ask question.
    user_parts = [
        p for p in appended_request.parts if isinstance(p, UserPromptPart)
    ]
    assert user_parts and user_parts[0].content == "Q1?"
    # The synthesized response carries the prior answer verbatim.
    text_parts = [p for p in appended_response.parts if isinstance(p, TextPart)]
    assert text_parts
    text_content = text_parts[0].content
    assert text_content == "A1"


# ---------------------------------------------------------------------------
# AC-15: gateway gate persists synthesized pair via store.save_messages
# ---------------------------------------------------------------------------


def test_ac15_gateway_gate_persists_synthesized_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-15: _run_agent_inner verbatim re-ask persists request/response pair.

    After a gated turn, ``store.load_messages(session_id)`` must include the
    synthesized request/response at the tail so the next turn's LLM context
    sees that the user re-asked and got the restate.
    """
    from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, UserPromptPart

    from seeknal.ask.gateway import server as gateway_server
    from seeknal.ask.sessions import SessionStore

    ctx = _make_ctx(tmp_path)

    # Seed the prior-turn pair so the gate fires on the first _run_agent_inner
    # call. record_prior_turn_answer stores on ctx.repl which is bound via the
    # contextvar set in _make_ctx.
    record_prior_turn_answer(question="Q1?", answer="A1: 42 NIE", ctx=ctx)

    # Stub create_agent so _run_agent_inner doesn't try to spin up real LLM
    # plumbing. We never reach the agent loop on a gated turn anyway, but the
    # function unconditionally calls create_agent before the gate check.
    def _fake_create_agent(project_path, **kwargs):
        return MagicMock(), MagicMock(), None, None

    monkeypatch.setattr(
        "seeknal.ask.agents.agent.create_agent", _fake_create_agent
    )

    # The gate calls reset_turn_governor → get_tool_context() which depends on
    # the contextvar. _make_ctx already called set_tool_context, but the test
    # event loop may not propagate the contextvar into the coroutine, so set
    # it again right before run.
    set_tool_context(ctx)

    session_id = "test-session-ac15"
    store = SessionStore(tmp_path, tenant_id="default")
    store.create(name=session_id)

    async def _drive():
        events = []
        async for event in gateway_server._run_agent_inner(
            project_path=tmp_path,
            session_id=session_id,
            question="Q1?",
        ):
            events.append(event)
        return events

    events = asyncio.run(_drive())

    # Sanity — the gate did emit the restate answer event (prior answer
    # verbatim, no banner).
    answer_events = [e for e in events if e.get("type") == "answer"]
    assert answer_events
    assert answer_events[0]["data"] == "A1: 42 NIE"

    # The synthesized pair must be on disk via store.load_messages.
    loaded = store.load_messages(session_id)
    assert len(loaded) >= 2
    appended_request, appended_response = loaded[-2], loaded[-1]
    assert isinstance(appended_request, ModelRequest)
    assert isinstance(appended_response, ModelResponse)
    user_parts = [
        p for p in appended_request.parts if isinstance(p, UserPromptPart)
    ]
    assert user_parts and user_parts[0].content == "Q1?"
    text_parts = [p for p in appended_response.parts if isinstance(p, TextPart)]
    assert text_parts and text_parts[0].content == "A1: 42 NIE"


# ---------------------------------------------------------------------------
# AC-16: triple-ask stays canonical — every gated turn returns the same answer
# ---------------------------------------------------------------------------


def test_ac16_triple_ask_stores_canonical_prior_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """AC-16: the answer stays canonical across multiple gated turns.

    Turn 1 records "A1". Turns 2 and 3 both short-circuit. Because a
    restate returns the prior answer verbatim (no banner), every turn's
    answer is exactly "A1" and ``_seeknal_prior_turn_answer`` after turn
    3 is still exactly "A1" — no compounding is even possible.
    """
    ctx = _make_ctx(tmp_path)

    async def _run():
        return await _stream_ask_with_stub_one_pass(
            ctx=ctx,
            questions=["Q1?", "Q1?", "Q1?"],
            one_pass_answer="A1",
            monkeypatch=monkeypatch,
        )

    answers, one_pass_calls = asyncio.run(_run())

    # Sanity: only the first turn invoked the agent loop.
    assert one_pass_calls == 1
    # Every turn returns the identical canonical answer — no banner.
    assert answers[0] == answers[1] == answers[2] == "A1"
    # The stored prior answer is still the canonical value.
    stored = getattr(ctx.repl, "_seeknal_prior_turn_answer", None)
    assert stored == "A1"
    assert "Pertanyaan ini sama" not in stored

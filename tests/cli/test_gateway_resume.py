"""Regression tests for P2-1: the worker must read and use ``resume_turns``.

Security review 2026-09-01, Part 2 §2.7: ``_process_http_work_item`` read
``work_id``, ``session_id``, ``tenant_id``, ``question``, ``provider`` and
``model`` off the claimed work item and never ``resume_turns`` -- so IBA's
HMAC-sealed trajectory travelled all the way to the claim payload and was
dropped on the floor. These tests pin:

  1. a work item with ``resume_turns`` reaches the agent's message history
     (asserted at the real ``agent.iter`` call boundary, not by mocking the
     validating function itself);
  2. a work item with no ``resume_turns`` is byte-identical to before this
     change;
  3. every invalid shape is refused, logged exactly once, and the run
     proceeds -- with NO history at all, which is deliberately distinct from
     the "absent" case (which keeps the session-store history);
  4. a broker-supplied ``system`` turn can never lead or become part of the
     seeded history;
  5. the resume-vs-stored-history precedence rule from
     ``_run_agent_inner``'s docstring.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.gateway.server import _resume_turns_to_message_history, _run_agent_inner
from seeknal.ask.sessions import SessionStore
from seeknal.cli import gateway as gateway_module
from tests.cli.test_gateway import _PostOnlyClient


# ---------------------------------------------------------------------------
# _resolve_worker_resume_turns -- shape/bounds validation at the broker
# boundary (mirrors _resolve_worker_model_choice's own test style).
# ---------------------------------------------------------------------------


def test_resolve_worker_resume_turns_absent_is_a_silent_noop():
    assert gateway_module._resolve_worker_resume_turns(None) == (None, False)


def test_resolve_worker_resume_turns_empty_list_is_a_silent_noop():
    """ADR-0013: "Absence and an empty list say the same thing" -- IBA never
    sends an empty list on a real claim, so this is harmless, not malformed."""
    assert gateway_module._resolve_worker_resume_turns([]) == (None, False)


def test_resolve_worker_resume_turns_valid_turns_pass_through():
    turns = [
        {"role": "user", "content": "q1"},
        {"role": "assistant", "content": "a1"},
    ]
    assert gateway_module._resolve_worker_resume_turns(turns) == (turns, False)


@pytest.mark.parametrize(
    "raw_resume_turns",
    [
        pytest.param("not-a-list", id="not-a-list"),
        pytest.param({"role": "user", "content": "q"}, id="dict-not-list"),
        pytest.param([1, 2, 3], id="turns-not-objects"),
        pytest.param(["nope"], id="turn-not-object"),
        pytest.param([{"content": "q"}], id="turn-missing-role"),
        pytest.param([{"role": "root", "content": "q"}], id="turn-invalid-role"),
        pytest.param([{"role": "user"}], id="turn-missing-content"),
        # The exact shape IBA's own build_pause_trajectory emits for an
        # "interact" pause turn (trajectory.py: assistant content is a dict,
        # not a string). Worker-side validation refuses it rather than
        # guessing how to stringify an IBA-internal form structure.
        pytest.param(
            [{"role": "assistant", "content": {"type": "interact", "form": {}}}],
            id="turn-non-string-content",
        ),
        pytest.param(
            [{"role": "user", "content": "q"}] * 201,
            id="too-many-turns",
        ),
        pytest.param(
            [{"role": "user", "content": "x" * 1_000_001}],
            id="too-many-bytes",
        ),
    ],
)
def test_resolve_worker_resume_turns_refuses_every_invalid_shape(raw_resume_turns):
    turns, refused = gateway_module._resolve_worker_resume_turns(raw_resume_turns)
    assert turns is None
    assert refused is True


@pytest.mark.parametrize(
    "raw_resume_turns",
    [
        "not-a-list",
        ["nope"],
        [{"role": "user"}],
        [{"role": "assistant", "content": {"type": "interact", "form": {}}}],
        [{"role": "user", "content": "q"}] * 201,
    ],
)
def test_resolve_worker_resume_turns_logs_exactly_once_and_never_content(
    raw_resume_turns, capsys
):
    gateway_module._resolve_worker_resume_turns(raw_resume_turns)

    out = capsys.readouterr().out
    assert out.count("refusing broker-supplied resume_turns") == 1
    # The secret sauce: never print turn content, even when it IS the reason
    # for refusal (the too-long-content case is deliberately not included
    # here since the content itself is astronomically long; the interact-form
    # case is what actually proves this).
    assert "interact" not in out
    assert "form" not in out


def test_resolve_worker_resume_turns_seeded_log_reports_count_and_bytes_only():
    turns = [{"role": "user", "content": "SECRET-QUESTION-TEXT"}]

    import io
    import contextlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        gateway_module._resolve_worker_resume_turns(turns)
    out = buf.getvalue()

    assert "seeding history from resume_turns" in out
    assert "turns=1" in out
    assert "bytes=" in out
    assert "SECRET-QUESTION-TEXT" not in out


# ---------------------------------------------------------------------------
# _process_http_work_item -> _run_agent_streaming: the shallow call boundary
# (mirrors the existing _resolve_worker_model_choice integration tests).
# ---------------------------------------------------------------------------


async def _run_work_item_and_capture_streaming_kwargs(work: dict) -> dict:
    received: dict = {}

    async def fake_streaming(_project, _session, _question, **kwargs):
        received.update(kwargs)
        yield {"type": "answer", "data": "ok"}

    from unittest.mock import patch

    client = _PostOnlyClient()
    with patch("seeknal.ask.gateway.server._run_agent_streaming", fake_streaming):
        await gateway_module._process_http_work_item(
            work,
            client=client,
            base_url="http://example.invalid",
            headers={},
            project_path=Path("/tmp/does-not-matter"),
            semaphore=asyncio.Semaphore(1),
        )
    return received


@pytest.mark.asyncio
async def test_process_http_work_item_no_resume_turns_is_byte_identical_to_today():
    """Today's IBA gateway omits resume_turns on a first-run claim (ADR-0013).
    Every kwarg _run_agent_streaming receives must be exactly what it was
    before this change -- the two new kwargs resolve to their inert defaults.
    """
    received = await _run_work_item_and_capture_streaming_kwargs({
        "work_id": "w-deadbeef",
        "session_id": "s1",
        "question": "q1",
    })

    assert received == {
        "provider": None,
        "model": None,
        "tenant_id": "default",
        "resume_turns": None,
        "resume_turns_refused": False,
    }


@pytest.mark.asyncio
async def test_process_http_work_item_forwards_valid_resume_turns():
    turns = [
        {"role": "user", "content": "q1"},
        {"role": "assistant", "content": "a1"},
    ]
    received = await _run_work_item_and_capture_streaming_kwargs({
        "work_id": "w-deadbeef",
        "session_id": "s1",
        "question": "q2",
        "resume_turns": turns,
    })

    assert received["resume_turns"] == turns
    assert received["resume_turns_refused"] is False


@pytest.mark.asyncio
async def test_process_http_work_item_refuses_malformed_resume_turns():
    received = await _run_work_item_and_capture_streaming_kwargs({
        "work_id": "w-deadbeef",
        "session_id": "s1",
        "question": "q1",
        "resume_turns": "not-a-list",
    })

    assert received["resume_turns"] is None
    assert received["resume_turns_refused"] is True


# ---------------------------------------------------------------------------
# _resume_turns_to_message_history -- turn conversion, and the system-turn
# drop rule.
# ---------------------------------------------------------------------------


def _flatten_contents(messages: list) -> list[str]:
    from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, UserPromptPart

    out: list[str] = []
    for msg in messages:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, UserPromptPart):
                    out.append(part.content)
        elif isinstance(msg, ModelResponse):
            for part in msg.parts:
                if isinstance(part, TextPart):
                    out.append(part.content)
    return out


def test_resume_turns_to_message_history_converts_user_and_assistant_in_order():
    turns = [
        {"role": "user", "content": "q1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "content": "q2"},
    ]

    history = _resume_turns_to_message_history(turns)

    assert _flatten_contents(history) == ["q1", "a1", "q2"]


def test_resume_turns_to_message_history_drops_system_turns_entirely():
    turns = [
        {"role": "system", "content": "SYSTEM-INJECTION-MARKER"},
        {"role": "user", "content": "q1"},
    ]

    history = _resume_turns_to_message_history(turns)

    contents = _flatten_contents(history)
    assert "SYSTEM-INJECTION-MARKER" not in contents
    # The dropped system turn cannot even occupy the leading position: the
    # first (and only) produced message is the user turn.
    assert contents == ["q1"]
    assert len(history) == 1


# ---------------------------------------------------------------------------
# _run_agent_inner -- the real call boundary the review asked for: what
# `agent.iter` actually receives as `message_history`. Also pins the
# resume-vs-stored-history precedence rule end to end.
# ---------------------------------------------------------------------------


class _FakeRun:
    """Ends pydantic-ai's node loop immediately.

    `_run_agent_inner`'s `async for node in run:` loop simply completes with
    zero iterations, so control falls through to `result = run.result`
    without ever touching pydantic-ai's internal graph/node types -- this
    test cares about what `message_history` was, not about driving a real
    model turn.
    """

    def __init__(self, output: str = "stub answer") -> None:
        # `all_messages` mirrors real pydantic-ai Run.result enough for
        # `_run_agent_inner`'s `finally` block (which always tries to
        # persist `result.all_messages()`) to succeed instead of logging.
        self.result = SimpleNamespace(output=output, all_messages=lambda: [])

    async def __aenter__(self) -> "_FakeRun":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def __aiter__(self) -> "_FakeRun":
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class _CapturingAgent:
    """Fake pydantic-ai Agent recording every `.iter(...)` call's kwargs."""

    def __init__(self) -> None:
        self.iter_calls: list[dict] = []

    def iter(self, question, *, deps, message_history, usage_limits):
        self.iter_calls.append({
            "question": question,
            "message_history": list(message_history),
        })
        return _FakeRun()


def _install_capturing_agent(
    monkeypatch: pytest.MonkeyPatch, project_path: Path, capturing_agent: _CapturingAgent
) -> None:
    def fake_create_agent(*args, **kwargs):
        set_tool_context(
            ToolContext(
                repl=MagicMock(),
                artifact_discovery=MagicMock(),
                project_path=project_path,
            )
        )
        return capturing_agent, MagicMock(), [], {}

    monkeypatch.setattr("seeknal.ask.agents.agent.create_agent", fake_create_agent)


async def _collect_events(*args, **kwargs) -> list[dict]:
    return [event async for event in _run_agent_inner(*args, **kwargs)]


def _seed_stored_history(tmp_path: Path, session_id: str) -> None:
    from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, UserPromptPart

    store = SessionStore(tmp_path, tenant_id="default")
    store.create(name=session_id)
    store.save_messages(session_id, [
        ModelRequest(parts=[UserPromptPart(content="STORED-HISTORY-MARKER-Q")]),
        ModelResponse(parts=[TextPart(content="STORED-HISTORY-MARKER-A")]),
    ])


@pytest.mark.asyncio
async def test_run_agent_inner_no_resume_turns_uses_stored_history_unchanged(
    tmp_path, monkeypatch
):
    """The rule from step 1: neither resume_turns nor a refusal present ->
    the session-store history is used, exactly as before this feature."""
    session_id = "resume-absent"
    _seed_stored_history(tmp_path, session_id)
    agent = _CapturingAgent()
    _install_capturing_agent(monkeypatch, tmp_path, agent)

    await _collect_events(tmp_path, session_id, "new question")

    assert len(agent.iter_calls) == 1
    seeded = agent.iter_calls[0]["message_history"]
    assert _flatten_contents(seeded) == [
        "STORED-HISTORY-MARKER-Q",
        "STORED-HISTORY-MARKER-A",
    ]


@pytest.mark.asyncio
async def test_run_agent_inner_valid_resume_turns_replace_stored_history(
    tmp_path, monkeypatch
):
    """The rule from step 1: a valid, verified resume_turns REPLACES the
    session-store history for this run rather than being appended to it --
    the stored marker must not survive into what the agent sees."""
    session_id = "resume-valid"
    _seed_stored_history(tmp_path, session_id)
    resume_turns = [
        {"role": "user", "content": "RESUME-TURN-Q"},
        {"role": "assistant", "content": "RESUME-TURN-A"},
    ]
    agent = _CapturingAgent()
    _install_capturing_agent(monkeypatch, tmp_path, agent)

    events = await _collect_events(
        tmp_path,
        session_id,
        "new question",
        resume_turns=resume_turns,
        resume_turns_refused=False,
    )

    assert len(agent.iter_calls) == 1
    call = agent.iter_calls[0]
    seeded_contents = _flatten_contents(call["message_history"])
    assert seeded_contents == ["RESUME-TURN-Q", "RESUME-TURN-A"]
    assert "STORED-HISTORY-MARKER-Q" not in seeded_contents
    assert "STORED-HISTORY-MARKER-A" not in seeded_contents
    # The resumed turns are ahead of (precede) the new question in the
    # sequence the model is given: they are message_history, and the new
    # `question` string is the prompt appended after it by pydantic-ai.
    assert call["question"] == "new question"
    assert any(e["type"] == "answer" for e in events)


@pytest.mark.asyncio
async def test_run_agent_inner_refused_resume_turns_runs_with_no_history_at_all(
    tmp_path, monkeypatch
):
    """A malformed resume_turns is NOT the same as an absent one: this run
    gets no history at all, not a silent fall-back to the session store --
    the store's marker must be as absent as the (rejected) resume turns."""
    session_id = "resume-refused"
    _seed_stored_history(tmp_path, session_id)
    agent = _CapturingAgent()
    _install_capturing_agent(monkeypatch, tmp_path, agent)

    await _collect_events(
        tmp_path,
        session_id,
        "new question",
        resume_turns=None,
        resume_turns_refused=True,
    )

    assert len(agent.iter_calls) == 1
    seeded = agent.iter_calls[0]["message_history"]
    assert seeded == []


@pytest.mark.asyncio
async def test_run_agent_inner_broker_system_turn_cannot_lead_the_prompt(
    tmp_path, monkeypatch
):
    """A `system` turn inside resume_turns must never reach the model as
    part of history -- it cannot lead the prompt, and it cannot appear at
    all, since `create_agent` supplies the real system prompt out of band
    via `instructions=`."""
    session_id = "resume-system-turn"
    resume_turns = [
        {"role": "system", "content": "SYSTEM-INJECTION-MARKER"},
        {"role": "user", "content": "RESUME-TURN-Q"},
        {"role": "assistant", "content": "RESUME-TURN-A"},
    ]
    agent = _CapturingAgent()
    _install_capturing_agent(monkeypatch, tmp_path, agent)

    await _collect_events(
        tmp_path,
        session_id,
        "new question",
        resume_turns=resume_turns,
        resume_turns_refused=False,
    )

    seeded = agent.iter_calls[0]["message_history"]
    contents = _flatten_contents(seeded)
    assert "SYSTEM-INJECTION-MARKER" not in contents
    assert contents == ["RESUME-TURN-Q", "RESUME-TURN-A"]
    assert contents[0] != "SYSTEM-INJECTION-MARKER"

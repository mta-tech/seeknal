"""Tests for non-interactive Intel work delivery and outcome reporting."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    ToolCallPart,
    ToolReturnPart,
)

from seeknal.ask.intel_work import (
    ASSIGNMENT_PROTOCOL,
    OUTCOME_PROTOCOL,
    QUEUE_PROTOCOL,
    IntelWorkError,
    IntelWorkAgentOutput,
    IntelWorkOutcomeRejected,
    MAX_ARTIFACT_DECODED_BYTES,
    WorkExecutionResult,
    execute_work_queue,
    _parse_assignment_output,
    load_work_queue,
    post_outcome,
    results_as_json,
    run_assignment_with_ask,
    validate_assertion,
)


AGENT_ID = "1f9e7ac9-d30a-4d4f-9a75-b2ebfd148f9b"
INSTANCE_ID = "4da9fe9b-7b04-422c-9dbe-c4fbdd1a24c4"
WORK_ID = "b9567c36-272b-4e40-8147-083a91c07d38"
INSTRUCTION = "Reconcile the margin-floor controls and cite each document."


def _write_context_pack(project_path: Path) -> None:
    project_path.joinpath("SEEKNAL_ASK.md").write_text(
        f"""\
<!-- intel:managed:start -->
- List: `intel knowledge list --agent {AGENT_ID} --instance {INSTANCE_ID}`
- Search: `intel knowledge search \"<query>\" --agent {AGENT_ID} --instance {INSTANCE_ID}`
- Read: `intel knowledge read <resource-id> --agent {AGENT_ID} --instance {INSTANCE_ID}`
<!-- intel:managed:end -->
""",
        encoding="utf-8",
    )


def _item(*, protocol: str = ASSIGNMENT_PROTOCOL, instruction: str = INSTRUCTION):
    return {
        "protocol_version": protocol,
        "id": WORK_ID,
        "instruction": {"kind": "text", "text": instruction},
        "outcome_owner": "fitra",
        "outcome_channel": {
            "protocol_version": OUTCOME_PROTOCOL,
            "method": "POST",
            "path": f"/v1/external-agents/me/work/{WORK_ID}/outcome",
        },
    }


def _write_policy(
    policy_path: Path,
    *,
    items=None,
    queue_protocol: str = QUEUE_PROTOCOL,
    delivery: str = "at_least_once",
) -> None:
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        json.dumps(
            {
                "etag": "test",
                "bundle": {
                    "work_queue": {
                        "protocol_version": queue_protocol,
                        "delivery": delivery,
                        "items": [_item()] if items is None else items,
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _successful_submitter(_binding, _item, _request_id, assertion):
    return {"state_after": assertion["disposition"]}


@pytest.fixture
def work_project(tmp_path: Path):
    _write_context_pack(tmp_path)
    policy_path = tmp_path / "policy.json"
    state_path = tmp_path / ".seeknal" / "intel_work_state.json"
    _write_policy(policy_path)
    return tmp_path, policy_path, state_path


def test_protocol_constants_are_exact():
    assert ASSIGNMENT_PROTOCOL == "intel.external_agent_work_assignment.v1"
    assert QUEUE_PROTOCOL == "intel.external_agent_work_queue.v1"
    assert OUTCOME_PROTOCOL == "intel.external_agent_work_outcome.v1"


def test_load_queue_uses_bound_instance_and_exact_contract(work_project):
    project, policy_path, _ = work_project

    binding, items = load_work_queue(project, policy_path=policy_path)

    assert binding.agent_id == AGENT_ID
    assert binding.instance_id == INSTANCE_ID
    assert len(items) == 1
    assert items[0].work_item_id == WORK_ID
    assert items[0].instruction == INSTRUCTION


def test_load_queue_accepts_live_legacy_item_envelope(work_project):
    project, policy_path, _ = work_project
    _write_policy(
        policy_path,
        items=[_item(protocol="intel.external_agent_work_item.v1")],
    )

    _, items = load_work_queue(project, policy_path=policy_path)

    assert items[0].work_item_id == WORK_ID


@pytest.mark.parametrize(
    ("queue_protocol", "delivery"),
    [("wrong.queue.v1", "at_least_once"), (QUEUE_PROTOCOL, "at_most_once")],
)
def test_wrong_queue_contract_fails_closed(
    work_project, queue_protocol: str, delivery: str
):
    project, policy_path, _ = work_project
    _write_policy(
        policy_path,
        queue_protocol=queue_protocol,
        delivery=delivery,
    )

    with pytest.raises(IntelWorkError):
        load_work_queue(project, policy_path=policy_path)


def test_wrong_assignment_protocol_fails_closed(work_project):
    project, policy_path, _ = work_project
    _write_policy(policy_path, items=[_item(protocol="wrong.assignment.v1")])

    with pytest.raises(IntelWorkError, match="assignment protocol"):
        load_work_queue(project, policy_path=policy_path)


def test_conflicting_duplicate_item_fails_closed(work_project):
    project, policy_path, _ = work_project
    _write_policy(
        policy_path,
        items=[_item(), _item(instruction="Different instruction")],
    )

    with pytest.raises(IntelWorkError, match="conflicting duplicate"):
        load_work_queue(project, policy_path=policy_path)


@pytest.mark.parametrize(
    "assertion",
    [
        {"disposition": "done", "reason": "Completed with evidence."},
        {"disposition": "failed", "reason": "Known execution failure."},
        {
            "disposition": "in_progress",
            "reason": "Work continues.",
            "continuation": {"kind": "retry", "detail": "Resume after sync."},
        },
        {
            "disposition": "outcome_unknown",
            "reason": "Completion could not be verified.",
            "continuation": {"kind": "review", "detail": "Inspect the run."},
        },
        {
            "disposition": "blocked",
            "reason": "Requires operator action.",
            "blocker": {"owner": "fitra", "action": "Restore access."},
        },
    ],
)
def test_valid_outcome_shapes(assertion):
    assert validate_assertion(assertion) == assertion


def _artifact(content: bytes = b"# Exact memo\n") -> dict[str, str]:
    return {
        "filename": "mta-20-analysis.md",
        "content_type": "text/markdown",
        "title": "MTA-20 analysis",
        "summary": "A complete evidence-backed analysis.",
        "content_base64": base64.b64encode(content).decode("ascii"),
    }


def test_done_accepts_exact_canonical_artifact():
    assertion = {
        "disposition": "done",
        "reason": "The complete memo is attached.",
        "artifact": _artifact(),
    }

    assert validate_assertion(assertion) == assertion


@pytest.mark.parametrize(
    "disposition_shape",
    [
        {
            "disposition": "in_progress",
            "reason": "Working.",
            "continuation": {"kind": "work", "detail": "Continue."},
        },
        {
            "disposition": "outcome_unknown",
            "reason": "Unknown.",
            "continuation": {"kind": "review", "detail": "Review."},
        },
        {
            "disposition": "blocked",
            "reason": "Blocked.",
            "blocker": {"owner": "operator", "action": "Restore access."},
        },
        {"disposition": "failed", "reason": "Failed."},
    ],
)
def test_artifact_is_forbidden_unless_done_mutation_biter(disposition_shape):
    disposition_shape["artifact"] = _artifact()

    with pytest.raises(IntelWorkError, match="forbids artifact"):
        validate_assertion(disposition_shape)


def test_artifact_decoded_size_limit_mutation_biter():
    assertion = {
        "disposition": "done",
        "reason": "The complete memo is attached.",
        "artifact": _artifact(b"x" * (MAX_ARTIFACT_DECODED_BYTES + 1)),
    }

    with pytest.raises(IntelWorkError, match="ARTIFACT_TOO_LARGE"):
        validate_assertion(assertion)


@pytest.mark.parametrize(
    "encoded",
    [
        "not base64!",
        base64.b64encode(b"padding").decode("ascii").rstrip("="),
        "ZE==",
    ],
)
def test_artifact_requires_canonical_padded_base64(encoded):
    artifact = _artifact()
    artifact["content_base64"] = encoded

    with pytest.raises(IntelWorkError, match="canonical padded Base64"):
        validate_assertion(
            {
                "disposition": "done",
                "reason": "The complete memo is attached.",
                "artifact": artifact,
            }
        )


@pytest.mark.parametrize(
    "assertion",
    [
        {
            "disposition": "done",
            "reason": "ok",
            "continuation": {"kind": "x", "detail": "y"},
        },
        {
            "disposition": "failed",
            "reason": "ok",
            "blocker": {"owner": "x", "action": "y"},
        },
        {"disposition": "outcome_unknown", "reason": "unknown"},
        {"disposition": "in_progress", "reason": "progress"},
        {"disposition": "blocked", "reason": "blocked"},
        {
            "disposition": "blocked",
            "reason": "blocked",
            "blocker": {"owner": "x", "action": "y"},
            "continuation": {"kind": "x", "detail": "y"},
        },
        {"disposition": "done", "reason": "   "},
    ],
)
def test_invalid_outcome_shapes_fail_before_post(assertion):
    with pytest.raises(IntelWorkError):
        validate_assertion(assertion)


@pytest.mark.parametrize(
    "assertion",
    [
        {"disposition": "done", "reason": "ok", "continuation": None},
        {"disposition": "done", "reason": "ok", "blocker": None},
        {
            "disposition": "blocked",
            "reason": "blocked",
            "blocker": {"owner": "x", "action": "y"},
            "continuation": None,
        },
        {
            "disposition": "outcome_unknown",
            "reason": "unknown",
            "continuation": {"kind": "x", "detail": "y"},
            "blocker": None,
        },
    ],
)
def test_forbidden_null_fields_are_rejected(assertion):
    with pytest.raises(IntelWorkError, match="forbids"):
        validate_assertion(assertion)


def test_outcome_unknown_requires_continuation_mutation_biter():
    with pytest.raises(IntelWorkError, match="continuation"):
        validate_assertion(
            {"disposition": "outcome_unknown", "reason": "Cannot verify."}
        )


def _read_history(
    resource_id: str,
    content: str = "fetched evidence",
    filename: str = "nusalintas-margin-floor-addendum.md",
) -> list[Any]:
    list_call_id = "intel-list-1"
    call_id = "intel-read-1"
    return [
        ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="intel_knowledge_list",
                    args={},
                    tool_call_id=list_call_id,
                )
            ]
        ),
        ModelRequest(
            parts=[
                ToolReturnPart(
                    tool_name="intel_knowledge_list",
                    content=json.dumps(
                        {
                            "resources": [
                                {
                                    "id": resource_id,
                                    "filename": filename,
                                }
                            ]
                        }
                    ),
                    tool_call_id=list_call_id,
                )
            ]
        ),
        ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="intel_knowledge_read",
                    args={"resource_id": resource_id},
                    tool_call_id=call_id,
                )
            ]
        ),
        ModelRequest(
            parts=[
                ToolReturnPart(
                    tool_name="intel_knowledge_read",
                    content=content,
                    tool_call_id=call_id,
                )
            ]
        ),
    ]


def test_done_requires_typed_assertion_and_read_source_attestation():
    resource_id = "resource-margin-floor"
    assertion = _parse_assignment_output(
        {
            "assertion": {
                "disposition": "done",
                "reason": "Finding from nusalintas-margin-floor-addendum.md.",
            },
            "sources": [
                {
                    "resource_id": resource_id,
                    "filename": "nusalintas-margin-floor-addendum.md",
                }
            ],
        },
        _read_history(resource_id),
    )

    assert assertion["disposition"] == "done"


def test_done_memo_is_encoded_byte_exactly_and_reason_stays_short():
    resource_id = "resource-margin-floor"
    memo = "# MTA-20\n\nCafé evidence.\n"
    assertion = _parse_assignment_output(
        {
            "assertion": {
                "disposition": "done",
                "reason": "Finding from nusalintas-margin-floor-addendum.md; memo attached.",
            },
            "sources": [
                {
                    "resource_id": resource_id,
                    "filename": "nusalintas-margin-floor-addendum.md",
                }
            ],
            "deliverable": {
                "filename": "mta-20-analysis.md",
                "content_type": "text/markdown",
                "title": "MTA-20 analysis",
                "summary": "Evidence-backed MTA-20 analysis.",
                "content": memo,
            },
        },
        _read_history(resource_id),
    )

    assert assertion["disposition"] == "done"
    assert len(assertion["reason"]) < 4096
    decoded = base64.b64decode(assertion["artifact"]["content_base64"], validate=True)
    assert decoded == memo.encode("utf-8")
    assert hashlib.sha256(decoded).hexdigest() == hashlib.sha256(
        memo.encode("utf-8")
    ).hexdigest()


def test_oversized_deliverable_fails_closed_without_truncation():
    resource_id = "resource-margin-floor"
    content = "x" * (MAX_ARTIFACT_DECODED_BYTES + 1)
    assertion = _parse_assignment_output(
        {
            "assertion": {
                "disposition": "done",
                "reason": "Finding from nusalintas-margin-floor-addendum.md.",
            },
            "sources": [
                {
                    "resource_id": resource_id,
                    "filename": "nusalintas-margin-floor-addendum.md",
                }
            ],
            "deliverable": {
                "filename": "mta-20-analysis.md",
                "content_type": "text/markdown",
                "title": "MTA-20 analysis",
                "summary": "Evidence-backed MTA-20 analysis.",
                "content": content,
            },
        },
        _read_history(resource_id),
    )

    assert assertion == {
        "disposition": "failed",
        "reason": (
            "ARTIFACT_TOO_LARGE: deliverable exceeds the 256 KiB decoded limit "
            f"({MAX_ARTIFACT_DECODED_BYTES + 1} bytes)."
        ),
    }


@pytest.mark.parametrize(
    ("answer", "history"),
    [
        ("I could not reconcile the requested controls.", _read_history("resource-1")),
        (
            {
                "assertion": {"disposition": "done", "reason": "Unsupported."},
                "sources": [],
            },
            _read_history("resource-1"),
        ),
        (
            {
                "assertion": {
                    "disposition": "done",
                    "reason": "Claim cites nusalintas-margin-floor-addendum.md.",
                },
                "sources": [
                    {
                        "resource_id": "resource-not-read",
                        "filename": "nusalintas-margin-floor-addendum.md",
                    }
                ],
            },
            _read_history("resource-actually-read"),
        ),
    ],
)
def test_unverified_agent_answer_never_becomes_done(answer, history):
    assertion = _parse_assignment_output(answer, history)

    assert assertion["disposition"] == "outcome_unknown"
    assert "continuation" in assertion


def test_assignment_runner_requests_typed_output_and_requires_real_read(work_project):
    project, policy_path, _ = work_project
    _, items = load_work_queue(project, policy_path=policy_path)
    resource_id = "resource-margin-floor"
    output = IntelWorkAgentOutput.model_validate(
        {
            "assertion": {
                "disposition": "done",
                "reason": "Finding from nusalintas-margin-floor-addendum.md.",
            },
            "sources": [
                {
                    "resource_id": resource_id,
                    "filename": "nusalintas-margin-floor-addendum.md",
                }
            ],
        }
    )
    run_result = MagicMock(output=output)
    run_result.all_messages.return_value = _read_history(resource_id)
    agent = MagicMock()
    agent.run_sync.return_value = run_result
    deps = MagicMock()
    with (
        patch(
            "seeknal.ask.agents.agent.create_agent",
            return_value=(agent, deps, [], {}),
        ) as create_agent,
        patch(
            "seeknal.ask.agents.tools._context.get_tool_context",
            return_value=MagicMock(request_limit=12),
        ),
    ):
        assertion = run_assignment_with_ask(items[0], project)

    assert assertion["disposition"] == "done"
    assert create_agent.call_args.kwargs["environment"] == "intel_work"
    assert create_agent.call_args.kwargs["output_type"] is IntelWorkAgentOutput
    assert create_agent.call_args.kwargs["model"] == "gemini-2.5-flash"
    assert "reconcile" in agent.run_sync.call_args.args[0].lower()


def test_assignment_runner_preserves_explicit_model_override(work_project):
    project, policy_path, _ = work_project
    _, items = load_work_queue(project, policy_path=policy_path)
    output = IntelWorkAgentOutput.model_validate(
        {
            "assertion": {"disposition": "failed", "reason": "Known failure."},
            "sources": [],
            "deliverable": None,
        }
    )
    agent = MagicMock()
    agent.run_sync.return_value = MagicMock(
        output=output,
        all_messages=MagicMock(return_value=[]),
    )
    with (
        patch(
            "seeknal.ask.agents.agent.create_agent",
            return_value=(agent, MagicMock(), [], {}),
        ) as create_agent,
        patch(
            "seeknal.ask.agents.tools._context.get_tool_context",
            return_value=MagicMock(request_limit=12),
        ),
    ):
        run_assignment_with_ask(
            items[0], project, provider="google", model="gemini-2.5-pro"
        )

    assert create_agent.call_args.kwargs["model"] == "gemini-2.5-pro"


def test_redelivery_after_restart_does_not_execute_or_post_twice(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock(
        return_value={
            "disposition": "done",
            "reason": "Actual margin-floor finding from all three documents.",
        }
    )
    poster = MagicMock(side_effect=_successful_submitter)

    first = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )
    second = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )

    assert first[0].status == "acknowledged"
    assert second[0].status == "already_acknowledged"
    runner.assert_called_once()
    assert poster.call_count == 2
    assert [call.args[3]["disposition"] for call in poster.call_args_list] == [
        "in_progress",
        "done",
    ]
    assert poster.call_args_list[0].args[2] != poster.call_args_list[1].args[2]
    state = json.loads(state_path.read_text())
    assert state["items"][WORK_ID]["phase"] == "acknowledged"


def test_fresh_delivered_item_is_claimed_before_terminal_outcome(work_project):
    project, policy_path, state_path = work_project
    server = {"state": "delivered", "claimed_at": None}
    posted: list[tuple[str, str]] = []
    runner_states: list[str] = []

    def submitter(_binding, _item, request_id, assertion):
        disposition = assertion["disposition"]
        posted.append((request_id, disposition))
        if (
            disposition in {"done", "failed", "blocked"}
            and server["state"] == "delivered"
        ):
            raise IntelWorkOutcomeRejected(
                409,
                "terminal outcome requires a prior claim",
                code="WORK_CLAIM_REQUIRED",
            )
        if disposition == "in_progress":
            assert server["state"] == "delivered"
            server["state"] = "in_progress"
            server["claimed_at"] = "2026-08-15T16:10:00+00:00"
            return {"state_after": "in_progress", "receipt_id": "claim-receipt"}
        assert server["state"] == "in_progress"
        server["state"] = disposition
        return {"state_after": disposition, "receipt_id": "terminal-receipt"}

    def runner(*_args, **_kwargs):
        runner_states.append(str(server["state"]))
        return {"disposition": "done", "reason": "Verified result."}

    result = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=submitter,
    )

    assert [disposition for _, disposition in posted] == ["in_progress", "done"]
    assert posted[0][0] != posted[1][0]
    assert runner_states == ["in_progress"]
    assert server == {
        "state": "done",
        "claimed_at": "2026-08-15T16:10:00+00:00",
    }
    assert result[0].claim_response == {
        "state_after": "in_progress",
        "receipt_id": "claim-receipt",
    }


def test_legacy_unclaimed_409_is_claimed_then_replayed_without_rerun(work_project):
    project, policy_path, state_path = work_project
    binding, items = load_work_queue(project, policy_path=policy_path)
    item = items[0]
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "items": {
                    WORK_ID: {
                        "phase": "rejected",
                        "client_request_id": "legacy-terminal-request",
                        "instance_id": binding.instance_id,
                        "agent_id": binding.agent_id,
                        "instruction": item.instruction,
                        "protocol_version": item.protocol_version,
                        "payload_hash": item.payload_hash,
                        "assertion": {
                            "disposition": "done",
                            "reason": "Previously verified result.",
                        },
                        "server_response": {"http_status": 409},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    runner = MagicMock()
    server_state = "delivered"
    dispositions: list[str] = []

    def submitter(_binding, _item, _request_id, assertion):
        nonlocal server_state
        disposition = assertion["disposition"]
        dispositions.append(disposition)
        if disposition == "in_progress":
            assert server_state == "delivered"
            server_state = "in_progress"
            return {"state_after": "in_progress"}
        assert server_state == "in_progress"
        server_state = disposition
        return {"state_after": disposition}

    result = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=submitter,
    )

    assert dispositions == ["in_progress", "done"]
    assert server_state == "done"
    runner.assert_not_called()
    assert result[0].status == "acknowledged"


def test_redelivery_fails_closed_on_changed_payload(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock(
        return_value={"disposition": "done", "reason": "Verified result."}
    )
    poster = MagicMock(side_effect=_successful_submitter)
    execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )
    _write_policy(policy_path, items=[_item(instruction="Changed assignment")])

    with pytest.raises(IntelWorkError, match="payload conflicts"):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )

    runner.assert_called_once()
    assert poster.call_count == 2


def test_redelivery_fails_closed_on_changed_binding(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock(
        return_value={"disposition": "done", "reason": "Verified result."}
    )
    poster = MagicMock(side_effect=_successful_submitter)
    execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )
    project.joinpath("SEEKNAL_ASK.md").write_text(
        "<!-- intel:managed:start -->\n"
        f"`intel knowledge list --agent {AGENT_ID} --instance instance-new`\n"
        "<!-- intel:managed:end -->\n",
        encoding="utf-8",
    )

    with pytest.raises(IntelWorkError, match="binding does not match"):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )

    runner.assert_called_once()
    assert poster.call_count == 2


def test_prepared_outcome_retries_post_without_rerunning(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock(
        return_value={"disposition": "done", "reason": "Verified result."}
    )
    poster = MagicMock(
        side_effect=[
            {"state_after": "in_progress"},
            IntelWorkError("temporary post failure"),
            {"state_after": "done"},
        ]
    )

    with pytest.raises(IntelWorkError, match="temporary post failure"):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )
    result = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )

    runner.assert_called_once()
    assert poster.call_count == 3
    assert poster.call_args_list[1].args[2] == poster.call_args_list[2].args[2]
    assert poster.call_args_list[0].args[2] != poster.call_args_list[1].args[2]
    assert result[0].status == "acknowledged"


def test_prepared_artifact_retry_is_byte_identical_and_not_republished(work_project):
    project, policy_path, state_path = work_project
    terminal = {
        "disposition": "done",
        "reason": "The complete memo is attached.",
        "artifact": _artifact(b"# Durable memo\n\nExact bytes.\n"),
    }
    runner = MagicMock(return_value=terminal)
    poster = MagicMock(
        side_effect=[
            {"state_after": "in_progress"},
            IntelWorkError("terminal response lost"),
            {"state_after": "done"},
        ]
    )

    with pytest.raises(IntelWorkError, match="terminal response lost"):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )
    completed = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )
    replay = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )

    runner.assert_called_once()
    assert poster.call_count == 3
    first_terminal = poster.call_args_list[1]
    retried_terminal = poster.call_args_list[2]
    assert first_terminal.args[2] == retried_terminal.args[2]
    assert first_terminal.args[3] == retried_terminal.args[3] == terminal
    assert completed[0].status == "acknowledged"
    assert replay[0].status == "already_acknowledged"


def test_results_json_reports_artifact_digest_without_base64():
    content = b"# Exact memo\n"
    rendered = results_as_json(
        [
            WorkExecutionResult(
                work_item_id=WORK_ID,
                status="acknowledged",
                client_request_id="stable",
                assertion={
                    "disposition": "done",
                    "reason": "Memo attached.",
                    "artifact": _artifact(content),
                },
            )
        ]
    )
    payload = json.loads(rendered)

    assert "content_base64" not in rendered
    assert payload[0]["assertion"]["artifact"]["decoded_bytes"] == len(content)
    assert payload[0]["assertion"]["artifact"]["sha256"] == hashlib.sha256(
        content
    ).hexdigest()


def test_claim_retry_uses_same_request_id_before_execution(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock(
        return_value={"disposition": "done", "reason": "Verified result."}
    )
    poster = MagicMock(
        side_effect=[
            IntelWorkError("claim request timed out"),
            {"state_after": "in_progress"},
            {"state_after": "done"},
        ]
    )

    with pytest.raises(IntelWorkError, match="claim request timed out"):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )
    result = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=runner,
        submit_outcome=poster,
    )

    assert poster.call_args_list[0].args[2] == poster.call_args_list[1].args[2]
    assert poster.call_args_list[1].args[3]["disposition"] == "in_progress"
    runner.assert_called_once()
    assert result[0].status == "acknowledged"


def test_claim_rejection_never_runs_assignment(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock()
    poster = MagicMock(
        side_effect=IntelWorkOutcomeRejected(
            403,
            "instance is offline",
            code="INSTANCE_OFFLINE",
        )
    )

    with pytest.raises(IntelWorkOutcomeRejected, match="INSTANCE_OFFLINE"):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )

    runner.assert_not_called()
    poster.assert_called_once()
    state = json.loads(state_path.read_text())
    assert state["items"][WORK_ID]["phase"] == "claim_rejected"


def test_executor_never_prompts(work_project):
    project, policy_path, state_path = work_project

    with patch("builtins.input", side_effect=AssertionError("prompted")):
        result = execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=lambda *_args, **_kwargs: {
                "disposition": "done",
                "reason": "Verified without prompting.",
            },
            submit_outcome=_successful_submitter,
        )

    assert result[0].status == "acknowledged"


def test_known_runner_exception_posts_failed_not_outcome_unknown(work_project):
    project, policy_path, state_path = work_project
    posted: list[dict[str, Any]] = []

    def submitter(_binding, _item, _request_id, assertion):
        posted.append(assertion)
        return {"state_after": assertion["disposition"]}

    result = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        run_assignment=MagicMock(side_effect=RuntimeError("secret detail")),
        submit_outcome=submitter,
    )

    assert [assertion["disposition"] for assertion in posted] == [
        "in_progress",
        "failed",
    ]
    assert result[0].assertion == {
        "disposition": "failed",
        "reason": (
            "SEEKNAL_ANALYSIS_EXECUTION_FAILED: Ask did not produce a "
            "deliverable (RuntimeError)."
        ),
    }
    assert "secret detail" not in results_as_json(result)


def test_403_is_durable_and_not_retried(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock(
        return_value={"disposition": "done", "reason": "Verified result."}
    )
    poster = MagicMock(
        side_effect=[
            {"state_after": "in_progress"},
            IntelWorkOutcomeRejected(403, "instance unavailable"),
        ]
    )

    with pytest.raises(IntelWorkOutcomeRejected):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )
    with pytest.raises(IntelWorkOutcomeRejected):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )

    runner.assert_called_once()
    assert poster.call_count == 2


def test_rejected_outcome_can_be_explicitly_retried_without_rerunning(work_project):
    project, policy_path, state_path = work_project
    runner = MagicMock(
        return_value={"disposition": "done", "reason": "Verified result."}
    )
    poster = MagicMock(
        side_effect=[
            {"state_after": "in_progress"},
            IntelWorkOutcomeRejected(403, "instance unavailable"),
            {"state_after": "done"},
        ]
    )
    with pytest.raises(IntelWorkOutcomeRejected):
        execute_work_queue(
            project,
            item_id=WORK_ID,
            policy_path=policy_path,
            state_path=state_path,
            run_assignment=runner,
            submit_outcome=poster,
        )

    result = execute_work_queue(
        project,
        item_id=WORK_ID,
        policy_path=policy_path,
        state_path=state_path,
        retry_rejected=True,
        run_assignment=runner,
        submit_outcome=poster,
    )

    assert result[0].status == "acknowledged"
    runner.assert_called_once()
    assert poster.call_count == 3


def test_post_outcome_uses_exact_endpoint_body_and_secure_helper(work_project):
    project, _, _ = work_project
    binding, items = load_work_queue(project, policy_path=work_project[1])
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"work_item_id": WORK_ID, "state": "done"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    credential = subprocess.CompletedProcess(
        ["intel"],
        0,
        stdout=json.dumps({"Authorization": "Bearer test-only-credential"}),
        stderr="",
    )
    assertion = {"disposition": "done", "reason": "Actual finding."}
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            return_value=credential,
        ) as helper,
        patch("seeknal.ask.intel_work.httpx.Client", return_value=client),
    ):
        response = post_outcome(binding, items[0], "stable-request-id", assertion)

    assert response["state"] == "done"
    assert captured[0].method == "POST"
    assert captured[0].url.path == (
        f"/api/v1/external-agents/me/work/{WORK_ID}/outcome"
    )
    assert json.loads(captured[0].content) == {
        "protocol_version": OUTCOME_PROTOCOL,
        "client_request_id": "stable-request-id",
        "work_item_id": WORK_ID,
        "assertion": assertion,
    }
    assert helper.call_args.args[0] == [
        "intel",
        "connect",
        "credential-helper",
        "--instance",
        INSTANCE_ID,
    ]


def test_post_outcome_surfaces_safe_server_error_code(work_project):
    project, policy_path, _ = work_project
    binding, items = load_work_queue(project, policy_path=policy_path)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "detail": {
                    "code": "WORK_CLAIM_REQUIRED",
                    "message": "terminal outcome requires a prior claim",
                }
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    credential = subprocess.CompletedProcess(
        ["intel"],
        0,
        stdout=json.dumps({"Authorization": "Bearer test-only-credential"}),
        stderr="",
    )
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            return_value=credential,
        ),
        patch("seeknal.ask.intel_work.httpx.Client", return_value=client),
        pytest.raises(IntelWorkOutcomeRejected) as rejected,
    ):
        post_outcome(
            binding,
            items[0],
            "terminal-request-id",
            {"disposition": "done", "reason": "Verified result."},
        )

    assert rejected.value.code == "WORK_CLAIM_REQUIRED"
    assert "[WORK_CLAIM_REQUIRED]" in str(rejected.value)
    assert "terminal outcome requires a prior claim" in str(rejected.value)


def test_intel_work_toolset_has_knowledge_tools_and_no_prompt_tools():
    from seeknal.ask.agents.tools.toolset import create_ask_toolset

    names = set(
        create_ask_toolset(mode="intel_work", include_intel_knowledge=True).tools.keys()
    )

    assert names == {
        "intel_knowledge_list",
        "intel_knowledge_search",
        "intel_knowledge_read",
    }


def test_work_cli_defaults_to_google_and_never_reads_stdin(tmp_path: Path):
    from typer.testing import CliRunner

    from seeknal.cli.ask import ask_app

    completed = WorkExecutionResult(
        work_item_id=WORK_ID,
        status="acknowledged",
        client_request_id="stable-request-id",
        assertion={"disposition": "done", "reason": "Verified result."},
        server_response={"state": "done"},
    )
    with (
        patch("builtins.input", side_effect=AssertionError("prompted")),
        patch(
            "seeknal.ask.intel_work.execute_work_queue",
            return_value=[completed],
        ) as execute,
    ):
        result = CliRunner().invoke(
            ask_app,
            ["work", "--project", str(tmp_path), "--item", WORK_ID],
        )

    assert result.exit_code == 0
    assert '"status": "acknowledged"' in result.stdout
    assert execute.call_args.kwargs["provider"] == "google"
    assert execute.call_args.kwargs["item_id"] == WORK_ID
    assert execute.call_args.kwargs["retry_rejected"] is False


def test_work_cli_surfaces_server_rejection_code(tmp_path: Path):
    from typer.testing import CliRunner

    from seeknal.cli.ask import ask_app

    with patch(
        "seeknal.ask.intel_work.execute_work_queue",
        side_effect=IntelWorkOutcomeRejected(
            409,
            "terminal outcome requires a prior claim",
            code="WORK_CLAIM_REQUIRED",
        ),
    ):
        result = CliRunner().invoke(
            ask_app,
            ["work", "--project", str(tmp_path), "--item", WORK_ID],
        )

    assert result.exit_code == 1
    assert "[WORK_CLAIM_REQUIRED]" in result.output
    assert "terminal outcome requires a prior claim" in result.output

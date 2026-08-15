"""Execute Intel-delivered work with durable at-least-once semantics.

Intel delivers assignments inside the instance policy bundle.  This module
loads that queue, runs each instruction through a prompt-free Seeknal Ask
environment, validates the outcome contract, and reports it with the same
secure instance credential used by Intel knowledge tools.
"""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from typing import Annotated, Any, Callable, Literal, cast
import uuid

import httpx
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from seeknal.ask.agents.tools.intel_knowledge import (
    _GATEWAY_URL,
    _IntelBinding,
    _authorization_header,
    _resolve_binding,
)


ASSIGNMENT_PROTOCOL = "intel.external_agent_work_assignment.v1"
QUEUE_PROTOCOL = "intel.external_agent_work_queue.v1"
OUTCOME_PROTOCOL = "intel.external_agent_work_outcome.v1"

# Live instances enrolled before the assignment protocol rename still deliver
# this item envelope inside an otherwise current v1 queue. It is accepted only
# as a compatibility envelope; outcomes always use OUTCOME_PROTOCOL.
_LEGACY_ASSIGNMENT_PROTOCOL = "intel.external_agent_work_item.v1"
_DELIVERY = "at_least_once"
_STATE_VERSION = 1
_HTTP_TIMEOUT_SECONDS = 30
_MAX_REASON_CHARS = 12_000
_CLIENT_REQUEST_NAMESPACE = uuid.UUID("61bd37f6-2627-4a5a-9c90-e935704ea723")
_VALID_DISPOSITIONS = {
    "in_progress",
    "done",
    "failed",
    "blocked",
    "outcome_unknown",
}

_NonBlankString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Continuation(_StrictModel):
    kind: _NonBlankString
    detail: _NonBlankString


class _Blocker(_StrictModel):
    owner: _NonBlankString
    action: _NonBlankString


class _DoneAssertion(_StrictModel):
    disposition: Literal["done"]
    reason: _NonBlankString


class _FailedAssertion(_StrictModel):
    disposition: Literal["failed"]
    reason: _NonBlankString


class _InProgressAssertion(_StrictModel):
    disposition: Literal["in_progress"]
    reason: _NonBlankString
    continuation: _Continuation


class _UnknownAssertion(_StrictModel):
    disposition: Literal["outcome_unknown"]
    reason: _NonBlankString
    continuation: _Continuation


class _BlockedAssertion(_StrictModel):
    disposition: Literal["blocked"]
    reason: _NonBlankString
    blocker: _Blocker


_AgentAssertion = Annotated[
    _DoneAssertion
    | _FailedAssertion
    | _InProgressAssertion
    | _UnknownAssertion
    | _BlockedAssertion,
    Field(discriminator="disposition"),
]


class _SourceAttestation(_StrictModel):
    resource_id: _NonBlankString
    filename: _NonBlankString


class IntelWorkAgentOutput(_StrictModel):
    """Typed output requested from the non-interactive work agent."""

    assertion: _AgentAssertion
    sources: list[_SourceAttestation]


class IntelWorkError(RuntimeError):
    """Safe Intel work failure suitable for CLI output."""


class IntelWorkOutcomeRejected(IntelWorkError):
    """Non-retryable server rejection, including revoked/offline instances."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(
            f"Intel work outcome rejected with HTTP {status_code}: {detail}"
        )


@dataclass(frozen=True)
class WorkItem:
    work_item_id: str
    instruction: str
    protocol_version: str
    outcome_path: str
    payload_hash: str


@dataclass(frozen=True)
class WorkExecutionResult:
    work_item_id: str
    status: str
    client_request_id: str
    assertion: dict[str, Any] | None = None
    server_response: dict[str, Any] | None = None


def _nonblank(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_pair(value: Any, *, name: str, fields: tuple[str, str]) -> None:
    if not isinstance(value, dict):
        raise IntelWorkError(
            f"{name} must be an object with {fields[0]} and {fields[1]}."
        )
    if set(value) != set(fields):
        raise IntelWorkError(
            f"{name} must contain exactly {fields[0]} and {fields[1]}."
        )
    for field in fields:
        if not _nonblank(value.get(field)):
            raise IntelWorkError(f"{name}.{field} must be a non-blank string.")


def validate_assertion(assertion: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the exact server-enforced disposition shape before POST."""
    if not isinstance(assertion, Mapping):
        raise IntelWorkError("Outcome assertion must be an object.")
    result = dict(assertion)
    allowed = {"disposition", "reason", "continuation", "blocker"}
    if set(result) - allowed:
        raise IntelWorkError("Outcome assertion contains unsupported fields.")

    disposition = result.get("disposition")
    if disposition not in _VALID_DISPOSITIONS:
        raise IntelWorkError("Outcome assertion has an invalid disposition.")
    if not _nonblank(result.get("reason")):
        raise IntelWorkError("Outcome assertion reason must be a non-blank string.")

    continuation_present = "continuation" in result
    blocker_present = "blocker" in result
    continuation = result.get("continuation")
    blocker = result.get("blocker")
    if disposition in {"in_progress", "outcome_unknown"}:
        _validate_pair(
            continuation,
            name="continuation",
            fields=("kind", "detail"),
        )
        if blocker_present:
            raise IntelWorkError(f"{disposition} forbids blocker.")
    elif disposition == "blocked":
        _validate_pair(blocker, name="blocker", fields=("owner", "action"))
        if continuation_present:
            raise IntelWorkError("blocked forbids continuation.")
    elif continuation_present or blocker_present:
        raise IntelWorkError(f"{disposition} forbids continuation and blocker.")

    return result


def _payload_hash(raw_item: Mapping[str, Any]) -> str:
    canonical = json.dumps(raw_item, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_work_item(raw: Any) -> WorkItem:
    if not isinstance(raw, dict):
        raise IntelWorkError("Intel work queue item must be an object.")
    protocol = raw.get("protocol_version")
    if protocol not in {ASSIGNMENT_PROTOCOL, _LEGACY_ASSIGNMENT_PROTOCOL}:
        raise IntelWorkError("Intel work item has an unsupported assignment protocol.")

    work_item_id = raw.get("work_item_id", raw.get("id"))
    if not isinstance(work_item_id, str) or not _nonblank(work_item_id):
        raise IntelWorkError("Intel work item ID must be a non-blank string.")
    if raw.get("work_item_id") and raw.get("id") and raw["work_item_id"] != raw["id"]:
        raise IntelWorkError("Intel work item contains conflicting IDs.")

    instruction = raw.get("instruction")
    if not isinstance(instruction, dict):
        raise IntelWorkError("Intel work instruction must be a text object.")
    instruction_text = instruction.get("text")
    if (
        instruction.get("kind") != "text"
        or not isinstance(instruction_text, str)
        or not _nonblank(instruction_text)
    ):
        raise IntelWorkError("Intel work instruction must contain non-blank text.")

    outcome_channel = raw.get("outcome_channel")
    expected_path = f"/v1/external-agents/me/work/{work_item_id}/outcome"
    if not isinstance(outcome_channel, dict):
        raise IntelWorkError("Intel work item is missing its outcome channel.")
    if (
        outcome_channel.get("protocol_version") != OUTCOME_PROTOCOL
        or outcome_channel.get("method") != "POST"
        or outcome_channel.get("path") != expected_path
    ):
        raise IntelWorkError("Intel work item has an invalid outcome channel.")

    return WorkItem(
        work_item_id=work_item_id.strip(),
        instruction=instruction_text.strip(),
        protocol_version=protocol,
        outcome_path=expected_path,
        payload_hash=_payload_hash(raw),
    )


def load_work_queue(
    project_path: Path,
    *,
    policy_path: Path | None = None,
) -> tuple[_IntelBinding, list[WorkItem]]:
    """Load and validate delivered work for the workspace-bound instance."""
    project_path = Path(project_path).resolve()
    binding = _resolve_binding(project_path)
    path = policy_path or (
        Path.home() / ".intel" / "instances" / f"{binding.instance_id}.policy.json"
    )
    try:
        policy = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise IntelWorkError("Intel instance policy bundle is unavailable.") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise IntelWorkError("Intel instance policy bundle could not be read.") from exc
    if not isinstance(policy, dict):
        raise IntelWorkError("Intel instance policy bundle must be an object.")

    bundle = policy.get("bundle")
    if not isinstance(bundle, dict):
        raise IntelWorkError("Intel instance policy bundle is missing bundle data.")
    queue = bundle.get("work_queue")
    if queue is None:
        return binding, []
    if not isinstance(queue, dict):
        raise IntelWorkError("Intel work queue must be an object.")
    if queue.get("protocol_version") != QUEUE_PROTOCOL:
        raise IntelWorkError(
            "Intel work queue protocol does not match the v1 contract."
        )
    if queue.get("delivery") != _DELIVERY:
        raise IntelWorkError("Intel work queue delivery must be at_least_once.")
    raw_items = queue.get("items", [])
    if not isinstance(raw_items, list):
        raise IntelWorkError("Intel work queue items must be a list.")

    deduplicated: dict[str, WorkItem] = {}
    for raw_item in raw_items:
        item = _parse_work_item(raw_item)
        existing = deduplicated.get(item.work_item_id)
        if existing is not None and existing.payload_hash != item.payload_hash:
            raise IntelWorkError(
                f"Intel work queue contains conflicting duplicate {item.work_item_id}."
            )
        deduplicated[item.work_item_id] = item
    return binding, list(deduplicated.values())


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _client_request_id(instance_id: str, work_item_id: str) -> str:
    return str(
        uuid.uuid5(
            _CLIENT_REQUEST_NAMESPACE,
            f"{instance_id}:{work_item_id}",
        )
    )


def _empty_state() -> dict[str, Any]:
    return {"version": _STATE_VERSION, "items": {}}


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _empty_state()
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise IntelWorkError(
            "Intel work state is unreadable; refusing redelivery."
        ) from exc
    if (
        not isinstance(state, dict)
        or state.get("version") != _STATE_VERSION
        or not isinstance(state.get("items"), dict)
    ):
        raise IntelWorkError("Intel work state is invalid; refusing redelivery.")
    return state


def _validated_state_record(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise IntelWorkError("Intel work state record is invalid.")
    record = cast(dict[str, Any], raw)
    if record.get("phase") not in {"claimed", "prepared", "acknowledged", "rejected"}:
        raise IntelWorkError("Intel work state record has an invalid phase.")
    for field in (
        "client_request_id",
        "instance_id",
        "agent_id",
        "instruction",
        "protocol_version",
        "payload_hash",
    ):
        if not _nonblank(record.get(field)):
            raise IntelWorkError(f"Intel work state record has an invalid {field}.")
    if record["phase"] in {"prepared", "acknowledged", "rejected"}:
        assertion = record.get("assertion")
        if not isinstance(assertion, Mapping):
            raise IntelWorkError("Intel work state record has an invalid assertion.")
        validate_assertion(assertion)
    return record


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=".intel_work_state.",
        suffix=".json.tmp",
        dir=str(path.parent),
    )
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


@contextmanager
def _state_lock(state_path: Path) -> Iterator[None]:
    import fcntl

    lock_path = state_path.with_suffix(state_path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise IntelWorkError("Intel work state lock is unavailable.") from exc
    with os.fdopen(fd, "a+", encoding="utf-8") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise IntelWorkError("Intel work state lock is not a regular file.")
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _safe_json_response(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except (json.JSONDecodeError, UnicodeError):
        return {"http_status": response.status_code}
    return (
        payload if isinstance(payload, dict) else {"http_status": response.status_code}
    )


def post_outcome(
    binding: _IntelBinding,
    item: WorkItem,
    client_request_id: str,
    assertion: Mapping[str, Any],
) -> dict[str, Any]:
    """POST one validated outcome using the secure instance credential."""
    validated = validate_assertion(assertion)
    if not _nonblank(client_request_id):
        raise IntelWorkError("client_request_id must be a non-blank string.")
    body = {
        "protocol_version": OUTCOME_PROTOCOL,
        "client_request_id": client_request_id,
        "work_item_id": item.work_item_id,
        "assertion": validated,
    }
    authorization = _authorization_header(binding.instance_id)
    try:
        with httpx.Client(
            timeout=_HTTP_TIMEOUT_SECONDS,
            follow_redirects=False,
        ) as client:
            response = client.post(
                f"{_GATEWAY_URL}{item.outcome_path}",
                headers={
                    "Authorization": authorization,
                    "Content-Type": "application/json",
                },
                json=body,
            )
    except httpx.TimeoutException as exc:
        raise IntelWorkError("Intel work outcome request timed out.") from exc
    except httpx.RequestError as exc:
        raise IntelWorkError("Intel work outcome service is unreachable.") from exc

    if response.status_code >= 500:
        raise IntelWorkError(
            f"Intel work outcome service failed with HTTP {response.status_code}."
        )
    if response.status_code >= 300:
        detail = "instance authorization or outcome contract was rejected"
        raise IntelWorkOutcomeRejected(response.status_code, detail)
    return _safe_json_response(response)


def _successful_read_ids(messages: list[Any]) -> set[str]:
    from pydantic_ai.messages import ToolCallPart, ToolReturnPart

    read_calls: dict[str, str] = {}
    successful: set[str] = set()
    for message in messages:
        for part in getattr(message, "parts", []):
            if (
                isinstance(part, ToolCallPart)
                and part.tool_name == "intel_knowledge_read"
            ):
                try:
                    args = part.args_as_dict()
                except (TypeError, ValueError):
                    continue
                resource_id = args.get("resource_id")
                if isinstance(resource_id, str) and _nonblank(resource_id):
                    read_calls[part.tool_call_id] = resource_id.strip()
            elif isinstance(part, ToolReturnPart):
                resource_id = read_calls.get(part.tool_call_id)
                content = str(part.content or "")
                if resource_id and not content.startswith(
                    "Intel knowledge unavailable:"
                ):
                    successful.add(resource_id)
    return successful


def _resource_filename_map(messages: list[Any]) -> dict[str, set[str]]:
    from pydantic_ai.messages import ToolReturnPart

    mapped: dict[str, set[str]] = {}

    def collect(value: Any) -> None:
        if isinstance(value, list):
            for entry in value:
                collect(entry)
            return
        if not isinstance(value, dict):
            return
        identifiers = {
            candidate.strip()
            for key in ("id", "resource_id", "uri", "ov_uri")
            if isinstance((candidate := value.get(key)), str) and candidate.strip()
        }
        filenames = {
            candidate.strip()
            for key in ("filename", "display_name", "name")
            if isinstance((candidate := value.get(key)), str)
            and candidate.strip().endswith(".md")
        }
        for key in ("uri", "ov_uri"):
            uri = value.get(key)
            if isinstance(uri, str):
                basename = uri.rstrip("/").rsplit("/", 1)[-1]
                if basename.endswith(".md"):
                    filenames.add(basename)
        for identifier in identifiers:
            mapped.setdefault(identifier, set()).update(filenames)
        for child in value.values():
            collect(child)

    for message in messages:
        for part in getattr(message, "parts", []):
            if not isinstance(part, ToolReturnPart) or part.tool_name not in {
                "intel_knowledge_list",
                "intel_knowledge_search",
            }:
                continue
            try:
                collect(json.loads(str(part.content or "")))
            except (json.JSONDecodeError, TypeError):
                continue
    return mapped


def _unknown_assertion(reason: str, detail: str) -> dict[str, Any]:
    return {
        "disposition": "outcome_unknown",
        "reason": reason,
        "continuation": {
            "kind": "operator_review",
            "detail": detail,
        },
    }


def _parse_assignment_output(
    output: Any,
    message_history: list[Any],
) -> dict[str, Any]:
    """Accept a typed assertion only when a done claim is evidence-backed."""
    if isinstance(output, IntelWorkAgentOutput):
        payload = output.model_dump(mode="json")
    elif isinstance(output, dict):
        payload = output
    else:
        return _unknown_assertion(
            "Seeknal did not return a valid typed outcome assertion.",
            "Review the non-interactive Ask output and retry the assignment.",
        )
    if not isinstance(payload, dict) or set(payload) != {"assertion", "sources"}:
        return _unknown_assertion(
            "Seeknal returned an invalid typed outcome envelope.",
            "Review the non-interactive Ask output and retry the assignment.",
        )
    raw_assertion = payload.get("assertion")
    if not isinstance(raw_assertion, Mapping):
        return _unknown_assertion(
            "Seeknal returned an invalid outcome assertion.",
            "Review the non-interactive Ask output and retry the assignment.",
        )
    try:
        assertion = validate_assertion(raw_assertion)
    except IntelWorkError:
        return _unknown_assertion(
            "Seeknal returned an outcome with an invalid disposition shape.",
            "Review the non-interactive Ask output and retry the assignment.",
        )
    if assertion["disposition"] != "done":
        return assertion
    if len(assertion["reason"]) > _MAX_REASON_CHARS:
        return _unknown_assertion(
            "Seeknal's done reason exceeded the safe outcome size limit.",
            "Condense the evidence-backed finding and retry the assignment.",
        )

    sources = payload.get("sources")
    successful_reads = _successful_read_ids(message_history)
    known_filenames = _resource_filename_map(message_history)
    if not isinstance(sources, list) or not sources or not successful_reads:
        return _unknown_assertion(
            "Seeknal could not verify a done outcome from fetched Intel knowledge.",
            "Retry after confirming that the required Intel resources can be read.",
        )
    reason = assertion["reason"]
    attested_ids: set[str] = set()
    for source in sources:
        if not isinstance(source, dict) or set(source) != {"resource_id", "filename"}:
            return _unknown_assertion(
                "Seeknal returned an invalid source attestation for a done outcome.",
                "Review the cited Intel resources and retry the assignment.",
            )
        resource_id = source.get("resource_id")
        filename = source.get("filename")
        if (
            not isinstance(resource_id, str)
            or not isinstance(filename, str)
            or not _nonblank(resource_id)
            or not _nonblank(filename)
        ):
            return _unknown_assertion(
                "Seeknal returned a blank source attestation for a done outcome.",
                "Review the cited Intel resources and retry the assignment.",
            )
        normalized_id = resource_id.strip()
        normalized_filename = filename.strip()
        if (
            normalized_id not in successful_reads
            or normalized_filename not in known_filenames.get(normalized_id, set())
            or normalized_filename not in reason
        ):
            return _unknown_assertion(
                "Seeknal's done claim was not backed by its fetched and cited sources.",
                "Review the cited Intel resources and retry the assignment.",
            )
        attested_ids.add(normalized_id)
    if attested_ids != successful_reads:
        return _unknown_assertion(
            "Seeknal did not cite every Intel resource it used for the done claim.",
            "Review all fetched Intel resources and retry the assignment.",
        )
    return assertion


def run_assignment_with_ask(
    item: WorkItem,
    project_path: Path,
    *,
    provider: str = "google",
    model: str | None = None,
) -> dict[str, Any]:
    """Execute one assignment through the prompt-free Intel work agent."""
    from pydantic_ai.usage import UsageLimits

    from seeknal.ask.agents.agent import create_agent
    from seeknal.ask.agents.tools._context import get_tool_context

    agent, deps, message_history, _ = create_agent(
        project_path,
        provider=provider,
        model=model,
        environment="intel_work",
        output_type=IntelWorkAgentOutput,
    )
    prompt = (
        "Execute this Intel-assigned work non-interactively. Use the Intel "
        "knowledge tools to retrieve every relevant document, reconcile the "
        "evidence, and cite each source by filename in the outcome reason. "
        "Do not ask a question, request confirmation, or claim completion "
        "without fetched evidence. Return the structured result with exactly "
        "assertion and sources. assertion must follow the Intel "
        "outcome contract. sources must be a list of objects with exactly "
        "resource_id (the exact value passed to intel_knowledge_read) and "
        "filename. For done, include every successfully read resource and the "
        "actual finding in assertion.reason. For any uncertain result, use "
        "outcome_unknown with a continuation.\n\nAssignment:\n"
        f"{item.instruction}"
    )
    result = agent.run_sync(
        prompt,
        deps=deps,
        message_history=message_history,
        usage_limits=UsageLimits(request_limit=get_tool_context().request_limit),
    )
    message_history.clear()
    message_history.extend(result.all_messages())
    return _parse_assignment_output(result.output, message_history)


def _interrupted_assertion() -> dict[str, Any]:
    return {
        "disposition": "outcome_unknown",
        "reason": (
            "A previous Seeknal execution was interrupted before its result "
            "could be durably recorded."
        ),
        "continuation": {
            "kind": "operator_review",
            "detail": (
                "Review the interrupted run before explicitly authorizing any "
                "new execution."
            ),
        },
    }


def execute_work_queue(
    project_path: Path,
    *,
    item_id: str | None = None,
    provider: str = "google",
    model: str | None = None,
    retry_rejected: bool = False,
    policy_path: Path | None = None,
    state_path: Path | None = None,
    run_assignment: Callable[..., Mapping[str, Any]] | None = None,
    submit_outcome: Callable[..., dict[str, Any]] | None = None,
) -> list[WorkExecutionResult]:
    """Execute delivered work once and durably suppress redelivery duplicates."""
    project_path = Path(project_path).resolve()
    state_path = state_path or (project_path / ".seeknal" / "intel_work_state.json")
    runner = run_assignment or run_assignment_with_ask
    submitter = submit_outcome or post_outcome

    with _state_lock(state_path):
        state = _load_state(state_path)
        records = state["items"]
        binding, queue_items = load_work_queue(
            project_path,
            policy_path=policy_path,
        )
        delivered = {item.work_item_id: item for item in queue_items}
        if item_id:
            if item_id not in delivered and item_id not in records:
                raise IntelWorkError(f"Intel work item {item_id} is not delivered.")
            selected_ids = [item_id]
        else:
            selected_ids = list(delivered)
            selected_ids.extend(
                stored_id
                for stored_id, stored in records.items()
                if stored_id not in delivered
                and isinstance(stored, dict)
                and stored.get("phase") in {"claimed", "prepared"}
            )

        results: list[WorkExecutionResult] = []
        for selected_id in selected_ids:
            raw_record = records.get(selected_id)
            record: dict[str, Any] | None = (
                _validated_state_record(raw_record) if raw_record is not None else None
            )
            if record is not None:
                if (
                    record.get("instance_id") != binding.instance_id
                    or record.get("agent_id") != binding.agent_id
                ):
                    raise IntelWorkError(
                        "Intel work state binding does not match the current workspace."
                    )
                delivered_item = delivered.get(selected_id)
                if (
                    delivered_item is not None
                    and record.get("payload_hash") != delivered_item.payload_hash
                ):
                    raise IntelWorkError(
                        "Intel work redelivery payload conflicts with durable state."
                    )
            if record is not None and record.get("phase") == "acknowledged":
                results.append(
                    WorkExecutionResult(
                        work_item_id=selected_id,
                        status="already_acknowledged",
                        client_request_id=record["client_request_id"],
                        assertion=record.get("assertion"),
                        server_response=record.get("server_response"),
                    )
                )
                continue
            if record is not None and record.get("phase") == "rejected":
                if not retry_rejected:
                    response = record.get("server_response")
                    status_code = (
                        response.get("http_status")
                        if isinstance(response, dict)
                        else 403
                    )
                    if not isinstance(status_code, int):
                        status_code = 403
                    raise IntelWorkOutcomeRejected(
                        status_code,
                        "previous outcome rejection requires explicit post-only retry",
                    )
                record["phase"] = "prepared"
                record["retry_requested_at"] = _utcnow()
                _save_state(state_path, state)

            item = delivered.get(selected_id)
            if item is None:
                if record is None:
                    raise IntelWorkError(
                        f"Intel work item {selected_id} is not delivered."
                    )
                item = WorkItem(
                    work_item_id=selected_id,
                    instruction=record["instruction"],
                    protocol_version=record["protocol_version"],
                    outcome_path=(f"/v1/external-agents/me/work/{selected_id}/outcome"),
                    payload_hash=record["payload_hash"],
                )

            if record is None:
                record = {
                    "phase": "claimed",
                    "client_request_id": _client_request_id(
                        binding.instance_id,
                        selected_id,
                    ),
                    "instance_id": binding.instance_id,
                    "agent_id": binding.agent_id,
                    "instruction": item.instruction,
                    "protocol_version": item.protocol_version,
                    "payload_hash": item.payload_hash,
                    "claimed_at": _utcnow(),
                }
                records[selected_id] = record
                _save_state(state_path, state)

                try:
                    assertion = dict(
                        runner(
                            item,
                            project_path,
                            provider=provider,
                            model=model,
                        )
                    )
                except Exception:
                    assertion = _interrupted_assertion()
                assertion = validate_assertion(assertion)
                record["assertion"] = assertion
                record["phase"] = "prepared"
                record["prepared_at"] = _utcnow()
                _save_state(state_path, state)
            elif record.get("phase") == "claimed":
                record["assertion"] = _interrupted_assertion()
                record["phase"] = "prepared"
                record["prepared_at"] = _utcnow()
                _save_state(state_path, state)

            stored_assertion = record.get("assertion")
            if not isinstance(stored_assertion, Mapping):
                raise IntelWorkError(
                    "Intel work state has an invalid prepared assertion."
                )
            assertion = validate_assertion(stored_assertion)
            try:
                server_response = submitter(
                    binding,
                    item,
                    record["client_request_id"],
                    assertion,
                )
            except IntelWorkOutcomeRejected as exc:
                record["phase"] = "rejected"
                record["rejected_at"] = _utcnow()
                record["server_response"] = {"http_status": exc.status_code}
                _save_state(state_path, state)
                raise

            record["phase"] = "acknowledged"
            record["acknowledged_at"] = _utcnow()
            record["server_response"] = server_response
            _save_state(state_path, state)
            results.append(
                WorkExecutionResult(
                    work_item_id=selected_id,
                    status="acknowledged",
                    client_request_id=record["client_request_id"],
                    assertion=assertion,
                    server_response=server_response,
                )
            )
        return results


def results_as_json(results: list[WorkExecutionResult]) -> str:
    """Return secret-free CLI JSON for executor results."""
    return json.dumps([asdict(result) for result in results], indent=2)


__all__ = [
    "ASSIGNMENT_PROTOCOL",
    "QUEUE_PROTOCOL",
    "OUTCOME_PROTOCOL",
    "IntelWorkError",
    "IntelWorkOutcomeRejected",
    "WorkExecutionResult",
    "WorkItem",
    "execute_work_queue",
    "load_work_queue",
    "post_outcome",
    "results_as_json",
    "run_assignment_with_ask",
    "validate_assertion",
]

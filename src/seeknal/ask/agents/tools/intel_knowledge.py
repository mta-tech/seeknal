"""Read-only access to Intel knowledge granted to this Seeknal workspace.

The tools deliberately expose no agent, instance, or scope arguments to the
model.  Their binding comes from explicit project configuration or the
``intel connect sync`` managed block in ``SEEKNAL_ASK.md``.  Authentication is
resolved at call time through Intel's secure credential helper; credentials
are never read from disk or included in tool output.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
import shlex
import subprocess
from typing import Any
from urllib.parse import quote

import httpx
import yaml

from seeknal.ask.config import load_agent_config


_GATEWAY_URL = "https://intel-platform.exe.xyz/api"
_MANAGED_START = "<!-- intel:managed:start -->"
_MANAGED_END = "<!-- intel:managed:end -->"
_CREDENTIAL_TIMEOUT_SECONDS = 10
_HTTP_TIMEOUT_SECONDS = 30
_MAX_JSON_BYTES = 1_000_000
_MAX_CONTENT_BYTES = 262_144
_INTEL_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")


@dataclass(frozen=True)
class _IntelBinding:
    agent_id: str
    instance_id: str


@dataclass(frozen=True)
class _ResponseBody:
    content: bytes
    truncated: bool = False


class _IntelKnowledgeError(RuntimeError):
    """Safe, user-facing Intel knowledge failure without credential details."""


def _valid_intel_id(value: str) -> bool:
    return _INTEL_ID_PATTERN.fullmatch(value) is not None


def _configured_binding(project_path: Path) -> _IntelBinding | None:
    try:
        config = load_agent_config(project_path)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise _IntelKnowledgeError("Intel project config could not be read.") from exc

    intel = config.get("intel", {})
    if not isinstance(intel, dict):
        if "intel" in config:
            raise _IntelKnowledgeError("Intel config must be a mapping.")
        return None

    raw_agent_id = intel.get("agent_id")
    raw_instance_id = intel.get("instance_id")
    if raw_agent_id is None and raw_instance_id is None:
        return None
    if not isinstance(raw_agent_id, str) or not isinstance(raw_instance_id, str):
        raise _IntelKnowledgeError(
            "Intel config IDs must be non-empty strings."
        )
    agent_id = raw_agent_id.strip()
    instance_id = raw_instance_id.strip()
    if (
        not agent_id
        or not instance_id
        or not _valid_intel_id(agent_id)
        or not _valid_intel_id(instance_id)
    ):
        raise _IntelKnowledgeError("Intel config IDs must be non-empty strings.")
    return _IntelBinding(agent_id=agent_id, instance_id=instance_id)


def _managed_region(pack: str) -> str | None:
    if pack.count(_MANAGED_START) > 1 or pack.count(_MANAGED_END) > 1:
        raise _IntelKnowledgeError(
            "Intel context pack contains duplicate managed regions; run "
            "`intel connect sync`."
        )
    start = pack.find(_MANAGED_START)
    if start < 0:
        return None
    start += len(_MANAGED_START)
    end = pack.find(_MANAGED_END, start)
    if end < 0:
        raise _IntelKnowledgeError(
            "Intel context pack has an incomplete managed region; run "
            "`intel connect sync`."
        )
    return pack[start:end]


def _command_binding(command: str) -> _IntelBinding | None:
    try:
        tokens = shlex.split(command)
    except ValueError as exc:
        if command.lstrip().startswith("intel knowledge"):
            raise _IntelKnowledgeError(
                "Intel context pack contains a malformed knowledge command."
            ) from exc
        return None

    if tokens[:2] != ["intel", "knowledge"]:
        return None
    if len(tokens) < 3 or tokens[2] not in {"list", "search", "read"}:
        raise _IntelKnowledgeError(
            "Intel context pack contains an unsupported knowledge command."
        )
    if tokens.count("--agent") != 1 or tokens.count("--instance") != 1:
        raise _IntelKnowledgeError(
            "Intel context pack commands must contain exactly one --agent and "
            "one --instance value."
        )

    try:
        agent_id = tokens[tokens.index("--agent") + 1].strip()
        instance_id = tokens[tokens.index("--instance") + 1].strip()
    except IndexError as exc:
        raise _IntelKnowledgeError(
            "Intel context pack contains a knowledge command with a missing ID."
        ) from exc

    if (
        not agent_id
        or not instance_id
        or not _valid_intel_id(agent_id)
        or not _valid_intel_id(instance_id)
    ):
        raise _IntelKnowledgeError(
            "Intel context pack contains a knowledge command with an invalid ID."
        )
    return _IntelBinding(agent_id=agent_id, instance_id=instance_id)


def _context_pack_binding(project_path: Path) -> _IntelBinding | None:
    pack_path = project_path / "SEEKNAL_ASK.md"
    try:
        pack = pack_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError) as exc:
        raise _IntelKnowledgeError("Intel context pack could not be read.") from exc

    managed = _managed_region(pack)
    if managed is None:
        return None

    bindings: set[_IntelBinding] = set()
    for fragment in managed.split("`")[1::2]:
        binding = _command_binding(fragment.strip())
        if binding is not None:
            bindings.add(binding)

    if len(bindings) > 1:
        raise _IntelKnowledgeError(
            "Intel context pack contains conflicting agent/instance bindings; "
            "run `intel connect sync` or set explicit intel.agent_id and "
            "intel.instance_id config."
        )
    return next(iter(bindings), None)


def _resolve_binding(project_path: Path) -> _IntelBinding:
    binding = _configured_binding(project_path)
    if binding is None:
        binding = _context_pack_binding(project_path)
    if binding is None:
        raise _IntelKnowledgeError(
            "Intel agent_id and instance_id are unavailable. Run `intel connect "
            "sync` to refresh SEEKNAL_ASK.md or set both values under `intel` "
            "in seeknal_agent.yml."
        )
    return binding


def _authorization_header(instance_id: str) -> str:
    argv = [
        "intel",
        "connect",
        "credential-helper",
        "--instance",
        instance_id,
    ]
    try:
        result = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=_CREDENTIAL_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise _IntelKnowledgeError(
            "Intel credential helper is unavailable; install the `intel` CLI."
        ) from exc
    except (OSError, ValueError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise _IntelKnowledgeError("Intel credential helper failed.") from exc

    if result.returncode != 0:
        raise _IntelKnowledgeError("Intel credential helper failed.")

    try:
        headers = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise _IntelKnowledgeError(
            "Intel credential helper returned an invalid response."
        ) from exc

    authorization = headers.get("Authorization") if isinstance(headers, dict) else None
    if not isinstance(authorization, str) or not authorization.startswith("Bearer "):
        raise _IntelKnowledgeError(
            "Intel credential helper did not return an Authorization header."
        )
    return authorization


def _request(
    method: str,
    path: str,
    *,
    binding: _IntelBinding,
    body: dict[str, Any] | None = None,
    max_bytes: int = _MAX_JSON_BYTES,
) -> _ResponseBody:
    authorization = _authorization_header(binding.instance_id)
    headers = {"Authorization": authorization}
    if body is not None:
        headers["Content-Type"] = "application/json"

    try:
        with httpx.Client(
            timeout=_HTTP_TIMEOUT_SECONDS,
            follow_redirects=False,
        ) as client:
            with client.stream(
                method,
                f"{_GATEWAY_URL}{path}",
                headers=headers,
                json=body,
            ) as response:
                if response.status_code >= 300:
                    raise _IntelKnowledgeError(
                        "Intel knowledge request failed with HTTP "
                        f"{response.status_code}."
                    )
                content = bytearray()
                truncated = False
                for chunk in response.iter_bytes():
                    remaining = max_bytes - len(content)
                    if len(chunk) > remaining:
                        content.extend(chunk[:remaining])
                        truncated = True
                        break
                    content.extend(chunk)
    except httpx.TimeoutException as exc:
        raise _IntelKnowledgeError("Intel knowledge request timed out.") from exc
    except httpx.RequestError as exc:
        raise _IntelKnowledgeError("Intel knowledge service is unreachable.") from exc
    return _ResponseBody(content=bytes(content), truncated=truncated)


def _binding_and_agent_path() -> tuple[_IntelBinding, str]:
    from seeknal.ask.agents.tools._context import get_tool_context

    binding = _resolve_binding(get_tool_context().project_path)
    encoded_agent = quote(binding.agent_id, safe="")
    return binding, f"/v1/agents/{encoded_agent}/knowledge"


def _json_payload(response: _ResponseBody) -> Any:
    if response.truncated:
        raise _IntelKnowledgeError("Intel knowledge JSON response was too large.")
    try:
        return json.loads(response.content)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise _IntelKnowledgeError(
            "Intel knowledge service returned invalid JSON."
        ) from exc


def _json_result(response: _ResponseBody) -> str:
    payload = _json_payload(response)
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _resource_id_from_uri(
    resource_uri: str,
    *,
    binding: _IntelBinding,
    base_path: str,
) -> str:
    payload = _json_payload(_request("GET", base_path, binding=binding))
    resources = payload.get("resources") if isinstance(payload, dict) else None
    if not isinstance(resources, list):
        raise _IntelKnowledgeError(
            "Intel knowledge list did not contain resource metadata."
        )
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        if resource_uri not in {resource.get("uri"), resource.get("ov_uri")}:
            continue
        resource_id = resource.get("id")
        if isinstance(resource_id, str) and resource_id.strip():
            return resource_id.strip()
    raise _IntelKnowledgeError(
        "The search-result URI is not present in the granted resource list."
    )


def _safe_tool_call(call) -> str:
    try:
        return call()
    except _IntelKnowledgeError as exc:
        return f"Intel knowledge unavailable: {exc}"


def intel_knowledge_list() -> str:
    """List Intel knowledge resources granted to this workspace.

    The agent and instance are bound from trusted project context. This tool is
    read-only and never asks the user to select or confirm a scope.
    """

    def call() -> str:
        binding, base_path = _binding_and_agent_path()
        return _json_result(_request("GET", base_path, binding=binding))

    return _safe_tool_call(call)


def intel_knowledge_search(query: str, limit: int = 5) -> str:
    """Search Intel knowledge granted to this workspace.

    Args:
        query: Terms describing the knowledge needed.
        limit: Maximum results to return, from 1 through 20.
    """

    def call() -> str:
        normalized_query = query.strip()
        if not normalized_query:
            raise _IntelKnowledgeError("A non-empty search query is required.")
        if not 1 <= limit <= 20:
            raise _IntelKnowledgeError("Search limit must be between 1 and 20.")
        binding, base_path = _binding_and_agent_path()
        response = _request(
            "POST",
            f"{base_path}/search",
            binding=binding,
            body={"query": normalized_query, "limit": limit},
        )
        return _json_result(response)

    return _safe_tool_call(call)


def intel_knowledge_read(resource_id: str) -> str:
    """Read a granted Intel knowledge resource by ID.

    Use ``intel_knowledge_list`` or ``intel_knowledge_search`` first. This tool
    accepts either a resource UUID from list results or an exact ``viking://``
    URI from search results. Search URIs are resolved against the granted list;
    the workspace-bound agent and instance are never inferred from a name or
    scope label.

    Args:
        resource_id: Exact resource UUID or ``viking://`` URI returned by Intel.
    """

    def call() -> str:
        normalized_id = resource_id.strip()
        if not normalized_id:
            raise _IntelKnowledgeError("A non-empty resource_id is required.")
        binding, base_path = _binding_and_agent_path()
        if normalized_id.startswith("viking://"):
            normalized_id = _resource_id_from_uri(
                normalized_id,
                binding=binding,
                base_path=base_path,
            )
        encoded_resource = quote(normalized_id, safe="")
        response = _request(
            "GET",
            f"{base_path}/{encoded_resource}/content",
            binding=binding,
            max_bytes=_MAX_CONTENT_BYTES,
        )
        text = response.content.decode("utf-8", errors="replace")
        if response.truncated:
            text += (
                f"\n\n[Content truncated at {_MAX_CONTENT_BYTES:,} bytes by "
                "Seeknal Ask.]"
            )
        return text

    return _safe_tool_call(call)


__all__ = [
    "intel_knowledge_list",
    "intel_knowledge_search",
    "intel_knowledge_read",
]

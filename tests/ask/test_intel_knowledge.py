"""Tests for the read-only Intel knowledge tools."""

from __future__ import annotations

from inspect import signature
import json
from pathlib import Path
import subprocess
from unittest.mock import MagicMock, patch

import httpx
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.intel_knowledge import (
    intel_knowledge_list,
    intel_knowledge_read,
    intel_knowledge_search,
)


AGENT_ID = "agent-from-pack-123"
INSTANCE_ID = "instance-from-pack-456"
RESOURCE_ID = "resource/with spaces"


def _write_context_pack(project_path: Path, *, conflicting: bool = False) -> None:
    read_agent = "other-agent" if conflicting else AGENT_ID
    project_path.joinpath("SEEKNAL_ASK.md").write_text(
        f"""\
Decoy outside the managed region:
`intel knowledge list --agent decoy-agent --instance decoy-instance`

<!-- intel:managed:start -->
- `kb-scope-do-not-derive` (read)
- List: `intel knowledge list --agent {AGENT_ID} --instance {INSTANCE_ID}`
- Search: `intel knowledge search \"<query>\" --agent {AGENT_ID} --instance {INSTANCE_ID}`
- Read: `intel knowledge read <resource-id> --agent {read_agent} --instance {INSTANCE_ID}`
<!-- intel:managed:end -->
""",
        encoding="utf-8",
    )


@pytest.fixture
def project_path(tmp_path: Path) -> Path:
    _write_context_pack(tmp_path)
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )
    return tmp_path


def _completed_credential_helper(argv, **kwargs):
    return subprocess.CompletedProcess(
        argv,
        0,
        stdout=json.dumps({"Authorization": "Bearer test-only-credential"}),
        stderr="",
    )


def _client_for(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_list_binds_request_to_agent_and_instance_from_managed_context_pack(
    project_path: Path,
):
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            json={
                "resources": [
                    {"id": "margin-floor", "name": "nusalintas-margin-floor-addendum.md"}
                ]
            },
        )

    client = _client_for(handler)
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ) as run,
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            return_value=client,
        ) as client_cls,
    ):
        result = intel_knowledge_list()

    assert len(captured) == 1
    assert captured[0].url.path == f"/api/v1/agents/{AGENT_ID}/knowledge"
    assert "nusalintas-margin-floor-addendum.md" in result
    assert run.call_args.args[0] == [
        "intel",
        "connect",
        "credential-helper",
        "--instance",
        INSTANCE_ID,
    ]
    assert run.call_args.kwargs["stdin"] is subprocess.DEVNULL
    assert run.call_args.kwargs.get("shell") is not True
    assert client_cls.call_args.kwargs["follow_redirects"] is False


def test_search_posts_query_to_bound_agent(project_path: Path):
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"results": [{"resource_id": "margin-floor"}]})

    client = _client_for(handler)
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            return_value=client,
        ),
    ):
        result = intel_knowledge_search("minimum gross margin", limit=7)

    assert captured[0].method == "POST"
    assert captured[0].url.path == f"/api/v1/agents/{AGENT_ID}/knowledge/search"
    assert json.loads(captured[0].content) == {
        "query": "minimum gross margin",
        "limit": 7,
    }
    assert "margin-floor" in result


def test_read_uses_bound_agent_and_url_encodes_resource_id(project_path: Path):
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, text="Confidential floor: 17.25 percent.")

    client = _client_for(handler)
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            return_value=client,
        ),
    ):
        result = intel_knowledge_read(RESOURCE_ID)

    assert captured[0].method == "GET"
    assert captured[0].url.raw_path.decode() == (
        f"/api/v1/agents/{AGENT_ID}/knowledge/resource%2Fwith%20spaces/content"
    )
    assert result == "Confidential floor: 17.25 percent."


def test_read_resolves_search_uri_against_granted_resource_list(project_path: Path):
    resource_uri = "viking://resources/scopes/kb-scope/info/margin-floor.md"
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        if request.url.path.endswith("/knowledge"):
            return httpx.Response(
                200,
                json={
                    "resources": [
                        {
                            "id": "resource-uuid-789",
                            "uri": resource_uri,
                            "name": "margin-floor.md",
                        }
                    ]
                },
            )
        return httpx.Response(200, text="Resolved restricted content.")

    clients = [_client_for(handler) for _ in range(2)]
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            side_effect=clients,
        ),
    ):
        result = intel_knowledge_read(resource_uri)

    assert [request.url.path for request in captured] == [
        f"/api/v1/agents/{AGENT_ID}/knowledge",
        f"/api/v1/agents/{AGENT_ID}/knowledge/resource-uuid-789/content",
    ]
    assert result == "Resolved restricted content."


def test_tools_have_no_scope_arguments_and_never_prompt(project_path: Path):
    assert set(signature(intel_knowledge_list).parameters) == set()
    assert set(signature(intel_knowledge_search).parameters) == {"query", "limit"}
    assert set(signature(intel_knowledge_read).parameters) == {"resource_id"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/content"):
            return httpx.Response(200, text="content")
        return httpx.Response(200, json={"resources": [], "results": []})

    clients = [_client_for(handler) for _ in range(3)]
    with (
        patch("builtins.input", side_effect=AssertionError("prompted")),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            side_effect=clients,
        ),
    ):
        intel_knowledge_list()
        intel_knowledge_search("margin")
        intel_knowledge_read("resource-id")


def test_missing_binding_stops_before_credentials_or_http(tmp_path: Path):
    tmp_path.joinpath("SEEKNAL_ASK.md").write_text(
        "<!-- intel:managed:start -->\nIntel knowledge availability: available.\n"
        "<!-- intel:managed:end -->\n",
        encoding="utf-8",
    )
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    with (
        patch("seeknal.ask.agents.tools.intel_knowledge.subprocess.run") as run,
        patch("seeknal.ask.agents.tools.intel_knowledge.httpx.Client") as client,
    ):
        result = intel_knowledge_list()

    assert "agent_id and instance_id" in result
    run.assert_not_called()
    client.assert_not_called()


def test_conflicting_managed_bindings_are_rejected(tmp_path: Path):
    _write_context_pack(tmp_path, conflicting=True)
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    with patch("seeknal.ask.agents.tools.intel_knowledge.subprocess.run") as run:
        result = intel_knowledge_list()

    assert "conflicting" in result.lower()
    run.assert_not_called()


@pytest.mark.parametrize(
    "command",
    [
        "intel knowledge bogus --agent agent-a --instance instance-a",
        "intel knowledge list --agent --instance instance-a",
        "intel knowledge list --agent agent-a --agent agent-b --instance instance-a",
        "intel knowledge list --agent agent-a --instance instance\x00bad",
    ],
)
def test_malformed_managed_commands_are_rejected(tmp_path: Path, command: str):
    tmp_path.joinpath("SEEKNAL_ASK.md").write_text(
        f"<!-- intel:managed:start -->\n`{command}`\n<!-- intel:managed:end -->\n",
        encoding="utf-8",
    )
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    with patch("seeknal.ask.agents.tools.intel_knowledge.subprocess.run") as run:
        result = intel_knowledge_list()

    assert "Intel knowledge unavailable" in result
    run.assert_not_called()


def test_duplicate_managed_regions_are_rejected(tmp_path: Path):
    block = (
        "<!-- intel:managed:start -->\n"
        f"`intel knowledge list --agent {AGENT_ID} --instance {INSTANCE_ID}`\n"
        "<!-- intel:managed:end -->\n"
    )
    tmp_path.joinpath("SEEKNAL_ASK.md").write_text(block + block, encoding="utf-8")
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    with patch("seeknal.ask.agents.tools.intel_knowledge.subprocess.run") as run:
        result = intel_knowledge_list()

    assert "duplicate managed regions" in result
    run.assert_not_called()


@pytest.mark.parametrize("invalid_value", [None, True, ["agent"]])
def test_non_string_config_ids_are_rejected(tmp_path: Path, invalid_value):
    tmp_path.joinpath("seeknal_agent.yml").write_text(
        "intel:\n"
        f"  agent_id: {json.dumps(invalid_value)}\n"
        f"  instance_id: {INSTANCE_ID}\n",
        encoding="utf-8",
    )
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    with patch("seeknal.ask.agents.tools.intel_knowledge.subprocess.run") as run:
        result = intel_knowledge_list()

    assert "IDs must be non-empty strings" in result
    run.assert_not_called()


def test_malformed_yaml_returns_safe_error(tmp_path: Path):
    tmp_path.joinpath("seeknal_agent.yml").write_text("intel: [", encoding="utf-8")
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )

    result = intel_knowledge_list()

    assert result == "Intel knowledge unavailable: Intel project config could not be read."


def test_redirect_is_not_followed(project_path: Path):
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(302, headers={"Location": "https://attacker.invalid/"})

    client = _client_for(handler)
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            return_value=client,
        ) as client_cls,
    ):
        result = intel_knowledge_list()

    assert len(captured) == 1
    assert captured[0].url.host == "intel-platform.exe.xyz"
    assert client_cls.call_args.kwargs["follow_redirects"] is False
    assert result == "Intel knowledge unavailable: Intel knowledge request failed with HTTP 302."


def test_credential_helper_failure_never_returns_credential(project_path: Path):
    sentinel = "credential-must-not-leak"
    failed = subprocess.CompletedProcess(
        ["intel"],
        1,
        stdout=json.dumps({"Authorization": f"Bearer {sentinel}"}),
        stderr=sentinel,
    )

    with patch(
        "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
        return_value=failed,
    ):
        result = intel_knowledge_list()

    assert result == "Intel knowledge unavailable: Intel credential helper failed."
    assert sentinel not in result


def test_credential_helper_argument_error_is_safe(project_path: Path):
    with patch(
        "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
        side_effect=ValueError("embedded null byte"),
    ):
        result = intel_knowledge_list()

    assert result == "Intel knowledge unavailable: Intel credential helper failed."
    assert "embedded null byte" not in result


def test_read_truncates_large_remote_content(project_path: Path):
    oversized = "x" * 300_000

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=oversized)

    client = _client_for(handler)
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            return_value=client,
        ),
    ):
        result = intel_knowledge_read("large-resource")

    assert result.startswith("x" * 100)
    assert "[Content truncated at 262,144 bytes by Seeknal Ask.]" in result
    assert len(result) < len(oversized)


def test_explicit_config_binding_works_without_context_pack(tmp_path: Path):
    tmp_path.joinpath("seeknal_agent.yml").write_text(
        f"""\
intel:
  agent_id: {AGENT_ID}
  instance_id: {INSTANCE_ID}
""",
        encoding="utf-8",
    )
    set_tool_context(
        ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        )
    )
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"resources": []})

    client = _client_for(handler)
    with (
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.subprocess.run",
            side_effect=_completed_credential_helper,
        ),
        patch(
            "seeknal.ask.agents.tools.intel_knowledge.httpx.Client",
            return_value=client,
        ),
    ):
        intel_knowledge_list()

    assert str(captured[0].url).startswith(
        f"https://intel-platform.exe.xyz/api/v1/agents/{AGENT_ID}/knowledge"
    )

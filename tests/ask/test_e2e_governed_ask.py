"""End-to-end integration tests for a governed Ask turn in Atlas Runtime."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest

from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery
from seeknal.ask.agents.tools._context import (
    ToolContext,
    get_tool_context,
    set_tool_context,
)
from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.governed import (
    PROHIBITED_CREDENTIAL_VARS,
    GovernedAskContext,
)


@pytest.fixture
def session_token_file(tmp_path: Path) -> Path:
    """Create a temporary session token file with permission mode 0600."""
    token_file = tmp_path / "ask_session_token.jwt"
    token_file.write_text("governed-session-token-e2e-secret\n", encoding="utf-8")
    token_file.chmod(0o600)
    return token_file


def _build_fake_catalog(
    recorded_requests: list[httpx.Request],
    token: str,
) -> httpx.MockTransport:
    """Create an httpx.MockTransport that serves /api/ask/v1/query and /api/ask/v1/schema."""

    def handle_request(request: httpx.Request) -> httpx.Response:
        recorded_requests.append(request)

        # Authorization header validation
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {token}":
            return httpx.Response(
                401,
                json={"error": {"code": "unauthorized", "message": "Invalid token"}},
            )

        if request.url.path == "/api/ask/v1/query":
            body = json.loads(request.content)
            sql = body.get("sql", "")

            # 403 Forbidden condition
            if "forbidden" in sql.lower():
                return httpx.Response(
                    403,
                    json={
                        "error": {
                            "code": "access_denied",
                            "message": "User is not authorized to query forbidden table",
                        }
                    },
                )

            # Normal successful query response with labelled columns
            return httpx.Response(
                200,
                json={
                    "columns": [
                        {"name": "city", "classification": None, "type": "VARCHAR"},
                        {"name": "email", "classification": "pii", "type": "VARCHAR"},
                        {"name": "dx", "classification": "phi", "type": "VARCHAR"},
                    ],
                    "rows": [
                        {"city": "Seattle", "email": "alice@hospital.org", "dx": "E11.9"},
                        {"city": "Portland", "email": "bob@clinic.com", "dx": "I10"},
                    ],
                    "rowCount": 2,
                    "elapsedMs": 10,
                    "truncated": False,
                    "queryId": "q-ask-e2e-1",
                },
            )

        if request.url.path == "/api/ask/v1/schema":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"name": "patients", "type": "BASE TABLE"},
                        {"name": "city", "type": "VARCHAR"},
                        {"name": "email", "type": "VARCHAR"},
                        {"name": "dx", "type": "VARCHAR"},
                    ]
                },
            )

        return httpx.Response(404, json={"error": "not found"})

    return httpx.MockTransport(handle_request)


def _setup_governed_ask_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    token_file: Path,
    tier: str | None = None,
) -> None:
    """Configure environment variables for governed Ask mode."""
    monkeypatch.setenv("ATLAS_ASK_MODE", "governed")
    monkeypatch.setenv("ATLAS_CATALOG_URL", "https://catalog.atlas.local")
    monkeypatch.setenv("ATLAS_ASK_SESSION_TOKEN_FILE", str(token_file))

    if tier is not None:
        monkeypatch.setenv("ATLAS_ASK_EGRESS_TIER", tier)
    else:
        monkeypatch.delenv("ATLAS_ASK_EGRESS_TIER", raising=False)

    for var in PROHIBITED_CREDENTIAL_VARS:
        monkeypatch.delenv(var, raising=False)


def _build_tool_context_as_agent(
    tmp_path: Path,
    transport: httpx.BaseTransport,
) -> ToolContext:
    """Build ToolContext the way agent.py does in governed mode."""
    gov_ctx = GovernedAskContext.from_env(transport=transport)
    discovery = ArtifactDiscovery(tmp_path)
    session_id = uuid.uuid4().hex[:8]

    ctx = ToolContext(
        repl=gov_ctx.repl,
        artifact_discovery=discovery,
        project_path=tmp_path,
        session_id=session_id,
        disable_quality_gate=True,
        is_governed=True,
    )
    set_tool_context(ctx)

    tool_ctx = get_tool_context()
    tool_ctx.pg_passthrough = False
    tool_ctx.iba_engine_url = None
    tool_ctx.iba_engine_api_key = ""
    tool_ctx.iba_storage_presign_url = None
    tool_ctx.iba_storage_api_key = ""
    tool_ctx.read_max_lines = 200

    return tool_ctx


def test_governed_ask_execute_sql_tier_e1_default(
    tmp_path: Path,
    session_token_file: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Default tier E1: city values present, email and dx withheld, session token carried, no duckdb.connect."""
    _setup_governed_ask_environment(tmp_path, monkeypatch, session_token_file, tier=None)

    recorded_requests: list[httpx.Request] = []
    transport = _build_fake_catalog(recorded_requests, "governed-session-token-e2e-secret")
    _build_tool_context_as_agent(tmp_path, transport)

    # Spy on duckdb.connect to verify no connection / attach occurs during execute_sql
    connect_calls: list[tuple[Any, ...]] = []
    real_connect = duckdb.connect

    def spy_connect(*args: Any, **kwargs: Any):
        connect_calls.append((args, kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", spy_connect)

    # Execute the SQL tool
    result = execute_sql("SELECT city, email, dx FROM patients")

    # Assert city values are present in output
    assert "Seattle" in result
    assert "Portland" in result

    # Assert sensitive values (email, dx) are NOT in output
    assert "alice@hospital.org" not in result
    assert "bob@clinic.com" not in result
    assert "E11.9" not in result
    assert "I10" not in result

    # Assert withheld columns are explicitly named
    assert "email" in result
    assert "dx" in result
    assert "withheld" in result
    assert "column `email` withheld: classified pii, egress tier E1" in result
    assert "column `dx` withheld: classified phi, egress tier E1" in result

    # Assert the request carried the session token
    assert len(recorded_requests) == 1
    req = recorded_requests[0]
    assert req.url.path == "/api/ask/v1/query"
    assert req.headers.get("Authorization") == "Bearer governed-session-token-e2e-secret"

    # Assert that no duckdb.connect attach happened during query execution
    assert len(connect_calls) == 0


def test_governed_ask_execute_sql_tier_e3_all_values(
    tmp_path: Path,
    session_token_file: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Tier E3: output contains all values and no withheld notices."""
    _setup_governed_ask_environment(tmp_path, monkeypatch, session_token_file, tier="E3")

    recorded_requests: list[httpx.Request] = []
    transport = _build_fake_catalog(recorded_requests, "governed-session-token-e2e-secret")
    _build_tool_context_as_agent(tmp_path, transport)

    result = execute_sql("SELECT city, email, dx FROM patients")

    # Assert all values are present in output
    assert "Seattle" in result
    assert "Portland" in result
    assert "alice@hospital.org" in result
    assert "bob@clinic.com" in result
    assert "E11.9" in result
    assert "I10" in result

    # Assert no withheld columns
    assert "withheld" not in result


def test_governed_ask_execute_sql_tier_e0_no_values(
    tmp_path: Path,
    session_token_file: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Tier E0: output contains schema headers but no row data values."""
    _setup_governed_ask_environment(tmp_path, monkeypatch, session_token_file, tier="E0")

    recorded_requests: list[httpx.Request] = []
    transport = _build_fake_catalog(recorded_requests, "governed-session-token-e2e-secret")
    _build_tool_context_as_agent(tmp_path, transport)

    result = execute_sql("SELECT city, email, dx FROM patients")

    # Assert headers are present but no data values are present
    assert "city" in result
    assert "email" in result
    assert "dx" in result

    assert "Seattle" not in result
    assert "Portland" not in result
    assert "alice@hospital.org" not in result
    assert "bob@clinic.com" not in result
    assert "E11.9" not in result
    assert "I10" not in result
    assert "(0 rows" in result


def test_governed_ask_execute_sql_403_access_denied(
    tmp_path: Path,
    session_token_file: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Catalog 403 Forbidden renders user-facing 'Access denied' message."""
    _setup_governed_ask_environment(tmp_path, monkeypatch, session_token_file, tier=None)

    recorded_requests: list[httpx.Request] = []
    transport = _build_fake_catalog(recorded_requests, "governed-session-token-e2e-secret")
    _build_tool_context_as_agent(tmp_path, transport)

    result = execute_sql("SELECT * FROM forbidden_patients")

    # Assert access denied is rendered
    assert "Access denied" in result
    assert "User is not authorized to query forbidden table" in result

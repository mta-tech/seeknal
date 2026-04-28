"""Token-derived tenant routing tests for gateway/Temporal worker mode."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.testclient import TestClient

from seeknal.ask.gateway.auth import TokenPrincipal, TokenRegistry, load_token_registry
from seeknal.ask.gateway.server import create_gateway_app
from seeknal.ask.gateway.temporal import TEMPORAL_AVAILABLE


@pytest.fixture
def registry(tmp_path):
    return TokenRegistry([
        TokenPrincipal(
            token="sk-acme-worker",
            tenant_id="acme",
            task_queue="seeknal-ask-acme-private",
            callback_token="cb-acme-worker",
            callback_url="https://gateway.example.com",
            project_path=str(tmp_path / "worker-project"),
            temporal_address="temporal.example.com:7233",
            temporal_namespace="seeknal-prod",
        )
    ])


def test_load_token_registry_from_mapping_env(monkeypatch):
    monkeypatch.setenv(
        "SEEKNAL_API_TOKENS",
        json.dumps({"sk-1": {"tenant_id": "tenant1", "task_queue": "queue1"}}),
    )

    principal = load_token_registry().resolve("sk-1")

    assert principal.tenant_id == "tenant1"
    assert principal.task_queue == "queue1"


def test_worker_config_derives_queue_and_callback_from_token(tmp_path, registry):
    app = create_gateway_app(
        project_path=None,
        sessions_dir=tmp_path,
        token_registry=registry,
        callback_base_url="https://fallback.example.com",
        temporal_address="localhost:7233",
        temporal_namespace="default",
    )

    with TestClient(app) as client:
        response = client.get(
            "/internal/worker/config",
            headers={"Authorization": "Bearer sk-acme-worker", "X-Tenant-ID": "evil"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["tenant_id"] == "acme"
    assert body["task_queue"] == "seeknal-ask-acme-private"
    assert body["callback_auth_token"] == "cb-acme-worker"
    assert body["callback_url"] == "https://gateway.example.com"
    assert body["temporal_address"] == "temporal.example.com:7233"
    assert body["temporal_namespace"] == "seeknal-prod"


def test_token_mode_requires_auth_for_tenant_scoped_routes(tmp_path, registry):
    app = create_gateway_app(project_path=None, sessions_dir=tmp_path, token_registry=registry)

    with TestClient(app) as client:
        response = client.get("/sessions", headers={"X-Tenant-ID": "acme"})

    assert response.status_code == 401
    assert "token" in response.json()["error"].lower()


def test_callback_uses_callback_token_tenant_not_header(tmp_path, registry):
    app = create_gateway_app(project_path=None, sessions_dir=tmp_path, token_registry=registry)

    with TestClient(app) as client:
        acme_queue = app.state.broadcaster.subscribe("sess1", tenant_id="acme")
        evil_queue = app.state.broadcaster.subscribe("sess1", tenant_id="evil")
        response = client.post(
            "/internal/events/sess1/publish",
            headers={"Authorization": "Bearer cb-acme-worker", "X-Tenant-ID": "evil"},
            json={"type": "token", "data": "hello"},
        )

    assert response.status_code == 200
    assert json.loads(acme_queue.get_nowait())["data"] == "hello"
    assert evil_queue.empty()


def test_callback_token_cannot_list_sessions(tmp_path, registry):
    app = create_gateway_app(project_path=None, sessions_dir=tmp_path, token_registry=registry)

    with TestClient(app) as client:
        response = client.get(
            "/sessions",
            headers={"Authorization": "Bearer cb-acme-worker"},
        )

    assert response.status_code == 401


@pytest.mark.skipif(not TEMPORAL_AVAILABLE, reason="temporalio not installed")
def test_temporal_start_derives_queue_from_token_and_rejects_overrides(tmp_path, registry):
    temporal_client = MagicMock()
    temporal_client.start_workflow = AsyncMock()
    temporal_client.start_workflow.return_value = MagicMock(id="wf-acme")
    app = create_gateway_app(
        project_path=None,
        sessions_dir=tmp_path,
        temporal_client=temporal_client,
        token_registry=registry,
        callback_base_url="https://fallback.example.com",
    )

    with TestClient(app) as client:
        rejected = client.post(
            "/temporal/start",
            headers={"Authorization": "Bearer sk-acme-worker"},
            json={
                "question": "hello",
                "session_id": "sess1",
                "task_queue": "seeknal-ask-evil",
            },
        )
        accepted = client.post(
            "/temporal/start",
            headers={"Authorization": "Bearer sk-acme-worker", "X-Tenant-ID": "evil"},
            json={"question": "hello", "session_id": "sess1"},
        )

    assert rejected.status_code == 403
    assert accepted.status_code == 200
    _, kwargs = temporal_client.start_workflow.call_args
    assert kwargs["task_queue"] == "seeknal-ask-acme-private"
    workflow_input = temporal_client.start_workflow.call_args.args[1]
    assert workflow_input.tenant_id == "acme"
    assert workflow_input.callback_auth_token == "cb-acme-worker"
    assert workflow_input.project_path == str(tmp_path / "worker-project")

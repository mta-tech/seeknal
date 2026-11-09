"""End-to-end integration tests for a governed job run in Atlas Runtime."""

from __future__ import annotations

import base64
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric import ec
import httpx
import pytest

from seeknal.runtime.catalog_io import (
    GovernedConfigError,
    set_catalog_io,
)
from seeknal.runtime.lineage import RUN_NAMESPACE
from seeknal.runtime.run_plan import build_run_plan, plan_sha256
from seeknal.serving.grant_token import sign_grant_token
from seeknal.workflow.dag import DAGBuilder
from seeknal.workflow.executors.base import ExecutionContext
from seeknal.workflow.manifest_builder import build_manifest_from_dag
from seeknal.workflow.runner import DAGRunner, ExecutionStatus


@pytest.fixture(autouse=True)
def clean_catalog_io_fixture():
    """Ensure clean global CatalogIO state before and after each test."""
    set_catalog_io(None)
    yield
    set_catalog_io(None)


def _setup_project(project_path: Path) -> None:
    """Create a minimal pipeline: CSV source -> transform step1 -> transform step2."""
    seeknal_dir = project_path / "seeknal"
    sources_dir = seeknal_dir / "sources"
    transforms_dir = seeknal_dir / "transforms"
    sources_dir.mkdir(parents=True, exist_ok=True)
    transforms_dir.mkdir(parents=True, exist_ok=True)

    data_csv = (project_path / "data.csv").resolve()
    data_csv.write_text("id,val\n1,10\n2,20\n3,30\n", encoding="utf-8")

    (sources_dir / "raw.yml").write_text(
        f"""\
kind: source
name: raw
source: csv
table: {data_csv}
""",
        encoding="utf-8",
    )

    (transforms_dir / "step1.yml").write_text(
        """\
kind: transform
name: step1
transform: SELECT id, val + 1 AS val FROM input_0
inputs:
  - ref: source.raw
""",
        encoding="utf-8",
    )

    (transforms_dir / "step2.yml").write_text(
        """\
kind: transform
name: step2
transform: SELECT id, val * 2 AS val FROM input_0
inputs:
  - ref: transform.step1
""",
        encoding="utf-8",
    )


def _build_and_sign_grant(
    project_path: Path,
    *,
    jti: str | None = None,
) -> tuple[str, str, str, str]:
    """Compute run plan for the project and sign an ES256 run grant.

    Returns:
        (grant_token, jwks_json, plan_hash, jti)
    """
    plan = build_run_plan(project_path)
    plan_hash = plan_sha256(plan)

    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    kid = "key-test-governed-job-1"

    numbers = pub.public_numbers()
    x_bytes = numbers.x.to_bytes(32, "big")
    y_bytes = numbers.y.to_bytes(32, "big")
    x_b64 = base64.urlsafe_b64encode(x_bytes).rstrip(b"=").decode("ascii")
    y_b64 = base64.urlsafe_b64encode(y_bytes).rstrip(b"=").decode("ascii")

    jwks = {
        "keys": [
            {
                "kty": "EC",
                "crv": "P-256",
                "kid": kid,
                "x": x_b64,
                "y": y_b64,
            }
        ]
    }

    now = int(time.time())
    grant_jti = jti or str(uuid.uuid4())
    claims = {
        "typ": "run",
        "iss": "atlas-catalog",
        "aud": "atlas-rt-run",
        "sub": "user:operator",
        "principal": "svc:governed-e2e",
        "ws": "ws-e2e",
        "jti": grant_jti,
        "release": "rel-2026",
        "plan_sha256": plan_hash,
        "trigger": {"type": "user", "id": "trig-e2e"},
        "iat": now,
        "nbf": now,
        "exp": now + 3600,
        "reads": [],
        "writes": [],
        "connections": [],
    }

    token = sign_grant_token(claims, priv, kid=kid)
    return token, json.dumps(jwks), plan_hash, grant_jti


def _write_governed_files_and_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    token: str,
    jwks_json: str,
) -> Path:
    """Write required token/key files with mode 0600 and configure environment."""
    grant_file = tmp_path / "run_grant.jwt"
    grant_file.write_text(token, encoding="utf-8")
    grant_file.chmod(0o600)

    jwks_file = tmp_path / "jwks.json"
    jwks_file.write_text(jwks_json, encoding="utf-8")
    jwks_file.chmod(0o600)

    lakekeeper_token_file = tmp_path / "lakekeeper_token.txt"
    lakekeeper_token_file.write_text("test-lakekeeper-token\n", encoding="utf-8")
    lakekeeper_token_file.chmod(0o600)

    conns_dir = tmp_path / "connections"
    conns_dir.mkdir(mode=0o700, exist_ok=True)

    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    monkeypatch.setenv("ATLAS_RUN_GRANT_FILE", str(grant_file))
    monkeypatch.setenv("ATLAS_RUN_GRANT_JWKS_FILE", str(jwks_file))
    monkeypatch.setenv("ATLAS_LAKEKEEPER_TOKEN_FILE", str(lakekeeper_token_file))
    monkeypatch.setenv("ATLAS_LAKEKEEPER_URI", "https://catalog.atlas.local/catalog")
    monkeypatch.setenv("ATLAS_LAKEKEEPER_WAREHOUSE", "wh")
    monkeypatch.setenv("ATLAS_RUN_CONTROLLER_URL", "https://catalog.atlas.local")
    monkeypatch.setenv("ATLAS_CONNECTIONS_DIR", str(conns_dir))

    for var in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "KEYCLOAK_CLIENT_SECRET",
        "LAKEKEEPER_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)

    monkeypatch.chdir(tmp_path)

    return conns_dir


def _install_narrow_httpx_mock(
    monkeypatch: pytest.MonkeyPatch,
    mock_transport: httpx.BaseTransport,
) -> None:
    """Narrowly monkeypatch httpx.Client so CatalogIO clients use mock_transport."""
    real_client_init = httpx.Client.__init__

    def patched_client_init(self, *args: Any, **kwargs: Any) -> None:
        if kwargs.get("transport") is None:
            kwargs["transport"] = mock_transport
        real_client_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.Client, "__init__", patched_client_init)


def test_governed_dag_run_e2e_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Real DAG run in governed mode: nodes start, lineage emitted, run completes."""
    _setup_project(tmp_path)
    token, jwks_json, _, grant_jti = _build_and_sign_grant(tmp_path)
    _write_governed_files_and_env(tmp_path, monkeypatch, token, jwks_json)

    start_requests: list[dict[str, Any]] = []
    lineage_requests: list[dict[str, Any]] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("Authorization", "")
        if "/nodes/start" in request.url.path:
            body = json.loads(request.content)
            start_requests.append({
                "path": request.url.path,
                "auth": auth,
                "body": body,
            })
            return httpx.Response(200, json={"allowed": True})

        if request.url.path == "/api/runtime/v1/lineage":
            body = json.loads(request.content)
            lineage_requests.append({
                "path": request.url.path,
                "auth": auth,
                "body": body,
            })
            return httpx.Response(200, json={"status": "ok"})

        return httpx.Response(404, json={"error": "not found"})

    mock_transport = httpx.MockTransport(handle_request)
    _install_narrow_httpx_mock(monkeypatch, mock_transport)

    # Run the DAG with DAGRunner exactly as `seeknal run` does
    builder = DAGBuilder(project_path=tmp_path)
    builder.build()
    manifest = build_manifest_from_dag(builder, tmp_path.name)

    target_path = tmp_path / "target"
    target_path.mkdir(parents=True, exist_ok=True)
    exec_context = ExecutionContext(
        project_name=tmp_path.name,
        workspace_path=tmp_path,
        target_path=target_path,
        dry_run=False,
        verbose=True,
    )

    runner = DAGRunner(manifest, target_path=target_path, exec_context=exec_context)
    summary = runner.run()

    # 1. Assert every node calls nodes/start once in topological order with grant as bearer
    expected_order = ["source.raw", "transform.step1", "transform.step2"]
    actual_order = [r["body"]["node_id"] for r in start_requests]
    assert actual_order == expected_order

    for req in start_requests:
        assert req["auth"] == f"Bearer {token}"
        assert f"/api/runtime/v1/runs/{grant_jti}/nodes/start" in req["path"]

    # 2. Assert START and COMPLETE lineage events are posted for each node with deterministic runId
    for node_id in expected_order:
        expected_run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{grant_jti}:{node_id}"))
        node_events = [
            req["body"]
            for req in lineage_requests
            if req["body"]["job"]["name"] == node_id
        ]
        assert len(node_events) == 2, f"Expected START and COMPLETE for {node_id}"

        start_ev = next(e for e in node_events if e["eventType"] == "START")
        complete_ev = next(e for e in node_events if e["eventType"] == "COMPLETE")

        assert start_ev["run"]["runId"] == expected_run_id
        assert complete_ev["run"]["runId"] == expected_run_id
        assert start_ev["run"]["facets"]["parent"]["run"]["runId"] == grant_jti
        assert complete_ev["run"]["facets"]["parent"]["run"]["runId"] == grant_jti

    for req in lineage_requests:
        assert req["auth"] == f"Bearer {token}"

    # 3. Assert the run completes successfully
    assert summary.successful_nodes == 3
    assert summary.failed_nodes == 0
    assert len(summary.results) == 3
    assert all(r.status == ExecutionStatus.SUCCESS for r in summary.results)


def test_governed_dag_run_revocation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Revocation scenario: fake Catalog answers 403 run_revoked for the second node."""
    _setup_project(tmp_path)
    token, jwks_json, _, grant_jti = _build_and_sign_grant(tmp_path)
    _write_governed_files_and_env(tmp_path, monkeypatch, token, jwks_json)

    start_requests: list[dict[str, Any]] = []
    lineage_requests: list[dict[str, Any]] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("Authorization", "")
        if "/nodes/start" in request.url.path:
            body = json.loads(request.content)
            start_requests.append({
                "path": request.url.path,
                "auth": auth,
                "body": body,
            })
            node_id = body.get("node_id")
            if node_id == "transform.step1":
                return httpx.Response(403, json={"error": "run_revoked"})
            return httpx.Response(200, json={"allowed": True})

        if request.url.path == "/api/runtime/v1/lineage":
            body = json.loads(request.content)
            lineage_requests.append({
                "path": request.url.path,
                "auth": auth,
                "body": body,
            })
            return httpx.Response(200, json={"status": "ok"})

        return httpx.Response(404, json={"error": "not found"})

    mock_transport = httpx.MockTransport(handle_request)
    _install_narrow_httpx_mock(monkeypatch, mock_transport)

    builder = DAGBuilder(project_path=tmp_path)
    builder.build()
    manifest = build_manifest_from_dag(builder, tmp_path.name)

    target_path = tmp_path / "target"
    target_path.mkdir(parents=True, exist_ok=True)
    exec_context = ExecutionContext(
        project_name=tmp_path.name,
        workspace_path=tmp_path,
        target_path=target_path,
        dry_run=False,
        verbose=True,
    )

    runner = DAGRunner(manifest, target_path=target_path, exec_context=exec_context)
    summary = runner.run(continue_on_error=True)

    # First node completes
    assert summary.results[0].node_id == "source.raw"
    assert summary.results[0].status == ExecutionStatus.SUCCESS

    # Second node fails with a governed stop
    assert summary.results[1].node_id == "transform.step1"
    assert summary.results[1].status == ExecutionStatus.FAILED
    assert summary.results[1].metadata.get("governed_stop") is True
    assert "run revoked" in (summary.results[1].error_message or "").lower()

    # Third node never starts
    assert not any(r.node_id == "transform.step2" for r in summary.results)
    assert not any(r["body"].get("node_id") == "transform.step2" for r in start_requests)

    # Lineage flush still happens: first node's lineage events exist
    raw_events = [
        req["body"]
        for req in lineage_requests
        if req["body"]["job"]["name"] == "source.raw"
    ]
    assert len(raw_events) == 2
    assert any(e["eventType"] == "START" for e in raw_events)
    assert any(e["eventType"] == "COMPLETE" for e in raw_events)

    # No START is sent for the third node (or step1)
    assert not any(r["body"]["job"]["name"] == "transform.step2" for r in lineage_requests)
    assert not any(r["body"]["job"]["name"] == "transform.step1" for r in lineage_requests)


def test_governed_dag_run_static_credential_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Static-credential refusal: setting AWS_ACCESS_KEY_ID makes governed startup fail."""
    _setup_project(tmp_path)
    token, jwks_json, _, _ = _build_and_sign_grant(tmp_path)
    _write_governed_files_and_env(tmp_path, monkeypatch, token, jwks_json)

    # Inject static credential
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA-STATIC-PROHIBITED")

    builder = DAGBuilder(project_path=tmp_path)
    builder.build()
    manifest = build_manifest_from_dag(builder, tmp_path.name)

    target_path = tmp_path / "target"
    target_path.mkdir(parents=True, exist_ok=True)
    exec_context = ExecutionContext(
        project_name=tmp_path.name,
        workspace_path=tmp_path,
        target_path=target_path,
        dry_run=False,
        verbose=True,
    )

    # Startup fails before any node runs
    with pytest.raises(GovernedConfigError, match="AWS_ACCESS_KEY_ID"):
        DAGRunner(manifest, target_path=target_path, exec_context=exec_context)

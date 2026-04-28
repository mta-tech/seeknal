from __future__ import annotations

import json
import shutil
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
import typer

from seeknal.workflow.apply import apply_command


class _AtlasStubServer(ThreadingHTTPServer):
    def __init__(self, responses: dict[str, list[tuple[int, dict[str, Any]]]]):
        super().__init__(("127.0.0.1", 0), _AtlasStubHandler)
        self.responses = {path: list(entries) for path, entries in responses.items()}
        self.requests: list[dict[str, Any]] = []


class _AtlasStubHandler(BaseHTTPRequestHandler):
    server: _AtlasStubServer

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length).decode("utf-8") if length else "{}"
        body = json.loads(raw_body or "{}")
        self.server.requests.append({"path": self.path, "body": body})

        status_code, payload = self.server.responses[self.path].pop(0)
        response = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


@contextmanager
def _atlas_stub(responses: dict[str, list[tuple[int, dict[str, Any]]]]):
    server = _AtlasStubServer(responses)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _write_draft(tmp_path: Path) -> Path:
    draft = tmp_path / "draft_transform_orders_enriched.yml"
    draft.write_text(
        "kind: transform\n"
        "name: orders_enriched\n"
        "description: Enriched orders\n"
        "tags:\n"
        "  - analytics\n"
        "inputs:\n"
        "  - ref: source.orders\n"
        "transform: SELECT * FROM source.orders\n",
        encoding="utf-8",
    )
    return draft


def _secure_project_dir(tmp_path: Path) -> Path:
    project_dir = Path.cwd() / ".seeknal" / "test-tmp" / tmp_path.name
    shutil.rmtree(project_dir, ignore_errors=True)
    project_dir.mkdir(parents=True, exist_ok=True)
    return project_dir


def test_apply_smoke_calls_atlas_contracts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_dir = _secure_project_dir(tmp_path)
    draft = _write_draft(project_dir)
    monkeypatch.chdir(project_dir)
    monkeypatch.setattr("seeknal.workflow.apply.update_manifest", lambda _: True)

    with _atlas_stub(
        {
            "/api/contracts/policy-check": [(200, {"allowed": True, "reason": "ok", "actor": "tester", "required_roles": []})],
            "/api/contracts/assets/register": [(200, {"asset": {"id": "asset-1", "metadata_json": {"canonical_asset_id": "atlas://dev/test/transform/orders_enriched"}}})],
            "/api/contracts/lineage/publish": [(200, {"asset_id": "asset-1", "edges_created": 1})],
            "/api/contracts/runs/report": [(200, {"report": {"run_id": "run-1", "status": "completed"}})],
        }
    ) as atlas:
        monkeypatch.setenv("ATLAS_API_URL", f"http://127.0.0.1:{atlas.server_port}")
        monkeypatch.setenv("SEEKNAL_PROJECT_NAME", "test-project")
        monkeypatch.setenv("ATLAS_ENVIRONMENT", "dev")

        apply_command(str(draft), force=False, no_parse=False)

    assert [request["path"] for request in atlas.requests] == [
        "/api/contracts/policy-check",
        "/api/contracts/assets/register",
        "/api/contracts/lineage/publish",
        "/api/contracts/runs/report",
    ]
    expected_target = project_dir / "seeknal" / "transforms" / "orders_enriched.yml"
    target = Path(atlas.requests[0]["body"]["asset"]["metadata"]["path"])
    assert target.exists()
    assert target == expected_target
    assert atlas.requests[2]["body"]["upstreams"][0]["source_id"] == "source:orders"


def test_apply_stops_when_atlas_policy_denies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_dir = _secure_project_dir(tmp_path)
    draft = _write_draft(project_dir)
    monkeypatch.chdir(project_dir)
    monkeypatch.setattr("seeknal.workflow.apply.update_manifest", lambda _: True)

    with _atlas_stub(
        {
            "/api/contracts/policy-check": [(200, {"allowed": False, "reason": "restricted asset requires platform_admin", "actor": "tester", "required_roles": ["platform_admin"]})],
            "/api/contracts/runs/report": [(200, {"report": {"run_id": "run-1", "status": "denied"}})],
        }
    ) as atlas:
        monkeypatch.setenv("ATLAS_API_URL", f"http://127.0.0.1:{atlas.server_port}")

        with pytest.raises(typer.Exit) as exc_info:
            apply_command(str(draft), force=False, no_parse=False)

    assert exc_info.value.exit_code == 1
    assert not (project_dir / "seeknal" / "transforms" / "orders_enriched.yml").exists()
    assert [request["path"] for request in atlas.requests] == [
        "/api/contracts/policy-check",
        "/api/contracts/runs/report",
    ]
    assert atlas.requests[1]["body"]["status"] == "denied"


def test_apply_reports_failure_when_atlas_sync_breaks_after_local_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir = _secure_project_dir(tmp_path)
    draft = _write_draft(project_dir)
    monkeypatch.chdir(project_dir)
    monkeypatch.setattr("seeknal.workflow.apply.update_manifest", lambda _: True)

    with _atlas_stub(
        {
            "/api/contracts/policy-check": [(200, {"allowed": True, "reason": "ok", "actor": "tester", "required_roles": []})],
            "/api/contracts/assets/register": [(500, {"error": "catalog unavailable"})],
            "/api/contracts/runs/report": [(200, {"report": {"run_id": "run-1", "status": "failed"}})],
        }
    ) as atlas:
        monkeypatch.setenv("ATLAS_API_URL", f"http://127.0.0.1:{atlas.server_port}")

        with pytest.raises(typer.Exit) as exc_info:
            apply_command(str(draft), force=False, no_parse=False)

    assert exc_info.value.exit_code == 1
    assert [request["path"] for request in atlas.requests] == [
        "/api/contracts/policy-check",
        "/api/contracts/assets/register",
        "/api/contracts/runs/report",
    ]
    target = Path(atlas.requests[0]["body"]["asset"]["metadata"]["path"])
    assert target.exists()
    assert atlas.requests[-1]["body"]["status"] == "failed"


def test_apply_reports_local_failure_after_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir = _secure_project_dir(tmp_path)
    draft = _write_draft(project_dir)
    monkeypatch.chdir(project_dir)
    monkeypatch.setattr("seeknal.workflow.apply.update_manifest", lambda _: True)

    def fail_move(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("seeknal.workflow.apply.move_draft_file", fail_move)

    with _atlas_stub(
        {
            "/api/contracts/policy-check": [(200, {"allowed": True, "reason": "ok", "actor": "tester", "required_roles": []})],
            "/api/contracts/runs/report": [(200, {"report": {"run_id": "run-1", "status": "failed"}})],
        }
    ) as atlas:
        monkeypatch.setenv("ATLAS_API_URL", f"http://127.0.0.1:{atlas.server_port}")

        with pytest.raises(typer.Exit) as exc_info:
            apply_command(str(draft), force=False, no_parse=False)

    assert exc_info.value.exit_code == 1
    assert [request["path"] for request in atlas.requests] == [
        "/api/contracts/policy-check",
        "/api/contracts/runs/report",
    ]
    assert atlas.requests[-1]["body"]["status"] == "failed"
    assert atlas.requests[-1]["body"]["details"]["phase"] == "local-apply"
    assert atlas.requests[-1]["body"]["details"]["local_apply_completed"] is False

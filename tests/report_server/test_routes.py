from __future__ import annotations

import tarfile
from pathlib import Path
from unittest.mock import patch

from starlette.testclient import TestClient


def _publish(client: TestClient, tarball_path: Path, *, auth: bool = True) -> dict:
    headers = {"Content-Type": "application/gzip"}
    if auth:
        headers["Authorization"] = "Bearer test-key-123"
    with open(tarball_path, "rb") as f:
        body = f.read()
    return client.post("/publish", content=body, headers=headers)


def test_healthz_responds(test_client: TestClient) -> None:
    resp = test_client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "version": "0.1.0"}


def test_healthz_no_auth_required(test_client: TestClient) -> None:
    resp = test_client.get("/healthz")
    assert resp.status_code == 200, "healthz must be accessible without Authorization header"


def test_publish_happy_path(test_client: TestClient, honest_tarball: Path) -> None:
    resp = _publish(test_client, honest_tarball)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert "slug" in body
    assert "share_url" in body
    assert "owner_secret" in body
    assert body["share_url"] == f"/r/{body['slug']}"
    assert len(body["slug"]) == 6


def test_publish_unauthorized(test_client: TestClient, honest_tarball: Path) -> None:
    resp = _publish(test_client, honest_tarball, auth=False)
    assert resp.status_code == 401, resp.text


def test_publish_wrong_content_type(test_client: TestClient) -> None:
    resp = test_client.post(
        "/publish",
        content=b"not a tarball",
        headers={"Content-Type": "text/plain", "Authorization": "Bearer test-key-123"},
    )
    assert resp.status_code == 415, resp.text


def test_serve_index_and_assets(test_client: TestClient, honest_tarball: Path) -> None:
    pub_resp = _publish(test_client, honest_tarball)
    assert pub_resp.status_code == 201
    slug = pub_resp.json()["slug"]

    index_resp = test_client.get(f"/r/{slug}")
    assert index_resp.status_code == 200
    assert b"<html" in index_resp.content.lower()

    asset_resp = test_client.get(f"/r/{slug}/_app/asset.js")
    assert asset_resp.status_code == 200


def test_serve_returns_404_for_missing_slug(test_client: TestClient) -> None:
    resp = test_client.get("/r/xxxxxx")
    assert resp.status_code == 404


def test_revoke_then_410(test_client: TestClient, honest_tarball: Path) -> None:
    pub_resp = _publish(test_client, honest_tarball)
    assert pub_resp.status_code == 201
    slug = pub_resp.json()["slug"]
    owner_secret = pub_resp.json()["owner_secret"]

    revoke_resp = test_client.post(
        f"/revoke/{slug}",
        headers={"X-Seeknal-Owner-Secret": owner_secret},
    )
    assert revoke_resp.status_code == 200, revoke_resp.text
    assert revoke_resp.json()["slug"] == slug

    serve_resp = test_client.get(f"/r/{slug}")
    assert serve_resp.status_code == 410, "Revoked report must return 410"


def test_revoke_wrong_secret_403(test_client: TestClient, honest_tarball: Path) -> None:
    pub_resp = _publish(test_client, honest_tarball)
    assert pub_resp.status_code == 201
    slug = pub_resp.json()["slug"]

    revoke_resp = test_client.post(
        f"/revoke/{slug}",
        headers={"X-Seeknal-Owner-Secret": "completely-wrong-secret"},
    )
    assert revoke_resp.status_code == 403, revoke_resp.text


def test_revoke_missing_slug_404(test_client: TestClient) -> None:
    revoke_resp = test_client.post(
        "/revoke/xxxxxx",
        headers={"X-Seeknal-Owner-Secret": "any-value"},
    )
    assert revoke_resp.status_code == 404, revoke_resp.text


def test_malicious_tarball_rejected(
    test_client: TestClient, malicious_tarball_path_traversal: Path
) -> None:
    resp = _publish(test_client, malicious_tarball_path_traversal)
    assert resp.status_code == 400, (
        f"Malicious tarball must be rejected with 400, got {resp.status_code}: {resp.text}"
    )
    error_msg = resp.json().get("error", "")
    assert "Tarball entry" in error_msg
    assert str(test_client.app.state.config.data_dir) not in error_msg


def test_malicious_absolute_tarball_rejected(
    test_client: TestClient, malicious_tarball_absolute: Path
) -> None:
    resp = _publish(test_client, malicious_tarball_absolute)
    assert resp.status_code == 400
    error_msg = resp.json().get("error", "")
    assert error_msg == "Tarball entry has absolute path"
    assert "/etc/passwd" not in error_msg
    assert str(test_client.app.state.config.data_dir) not in error_msg


def test_corrupt_non_gzip_upload_returns_400(test_client: TestClient) -> None:
    resp = test_client.post(
        "/publish",
        content=b"this is definitely not a valid gzip tarball archive",
        headers={
            "Content-Type": "application/gzip",
            "Authorization": "Bearer test-key-123",
        },
    )
    assert resp.status_code == 400, (
        f"Corrupt upload must return 400, got {resp.status_code}: {resp.text}"
    )
    error_msg = resp.json().get("error", "")
    assert "corrupt" in error_msg.lower() or "invalid" in error_msg.lower()
    assert str(test_client.app.state.config.data_dir) not in error_msg


def test_filter_error_mapped_to_typed_400_without_server_paths(
    test_client: TestClient, honest_tarball: Path
) -> None:
    filter_err_cls = getattr(tarfile, "OutsideDestinationError", tarfile.TarError)

    def _raise_filter_err(*args, **kwargs):
        raise filter_err_cls(
            tarfile.TarInfo(name="../../etc/passwd"),
            str(test_client.app.state.config.data_dir),
        )

    with patch.object(test_client.app.state.storage, "extract_tarball", side_effect=_raise_filter_err):
        resp = _publish(test_client, honest_tarball)

    assert resp.status_code == 400
    error_msg = resp.json().get("error", "")
    assert error_msg
    assert str(test_client.app.state.config.data_dir) not in error_msg
    assert "/etc/passwd" not in error_msg


def test_get_r_path_works_in_api_key_mode_without_bearer(
    test_client: TestClient, honest_tarball: Path
) -> None:
    pub_resp = _publish(test_client, honest_tarball, auth=True)
    assert pub_resp.status_code == 201
    slug = pub_resp.json()["slug"]

    serve_resp = test_client.get(f"/r/{slug}")
    assert serve_resp.status_code == 200, (
        "Viewer must be able to access /r/{slug} without Authorization header"
    )

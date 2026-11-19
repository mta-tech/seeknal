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


def test_bad_content_length_returns_400(test_client: TestClient) -> None:
    for bad_cl in ("not-a-number", "-1", "12.34", "+10", ""):
        resp = test_client.post(
            "/publish",
            content=b"abc",
            headers={
                "Content-Type": "application/gzip",
                "Authorization": "Bearer test-key-123",
                "Content-Length": bad_cl,
            },
        )
        assert resp.status_code == 400, f"Content-Length={bad_cl!r} should return 400, got {resp.status_code}"
        assert "error" in resp.json()


def test_chunked_oversized_body_returns_413_without_reading_all_chunks(
    test_app,
) -> None:
    import asyncio
    from starlette.requests import Request
    from seeknal.report_server.routes import publish

    test_app.state.config.max_upload_bytes = 100
    chunk_size = 40
    total_chunks = 10
    chunks_yielded = 0

    async def _run():
        nonlocal chunks_yielded
        sent_chunks = 0

        async def receive():
            nonlocal chunks_yielded, sent_chunks
            sent_chunks += 1
            chunks_yielded += 1
            more = sent_chunks < total_chunks
            return {
                "type": "http.request",
                "body": b"x" * chunk_size,
                "more_body": more,
            }

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/publish",
            "raw_path": b"/publish",
            "query_string": b"",
            "headers": [
                (b"content-type", b"application/gzip"),
                (b"authorization", b"Bearer test-key-123"),
                (b"transfer-encoding", b"chunked"),
            ],
            "app": test_app,
        }
        req = Request(scope, receive)
        return await publish(req)

    resp = asyncio.run(_run())
    assert resp.status_code == 413
    # Cap is 100 bytes, chunk size is 40 bytes -> 3rd chunk reaches 120 bytes > 100 and aborts immediately
    assert chunks_yielded == 3, f"Expected to read at most 3 chunks (120B <= cap + 1 chunk), read {chunks_yielded}"


def test_healthz_responsive_while_extraction_runs(
    test_app,
    honest_tarball: Path,
) -> None:
    import asyncio
    import threading
    import time
    import httpx

    extraction_started = threading.Event()
    allow_extraction_finish = threading.Event()
    orig_extract = test_app.state.storage.extract_tarball

    def _slow_extract(slug, fileobj):
        extraction_started.set()
        allow_extraction_finish.wait(timeout=5.0)
        return orig_extract(slug, fileobj)

    tarball_bytes = honest_tarball.read_bytes()

    async def _run():
        transport = httpx.ASGITransport(app=test_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            pub_task = asyncio.create_task(
                client.post(
                    "/publish",
                    content=tarball_bytes,
                    headers={
                        "Content-Type": "application/gzip",
                        "Authorization": "Bearer test-key-123",
                    },
                )
            )
            # Wait for extraction thread to start without blocking the event loop
            for _ in range(100):
                if extraction_started.is_set():
                    break
                await asyncio.sleep(0.01)
            assert extraction_started.is_set(), "Extraction did not start in worker thread"

            t0 = time.monotonic()
            health_resp = await client.get("/healthz")
            elapsed = time.monotonic() - t0
            assert health_resp.status_code == 200
            assert health_resp.json()["status"] == "ok"
            assert elapsed < 0.5, f"/healthz took {elapsed:.3f}s while extraction was blocked"

            allow_extraction_finish.set()
            pub_resp = await pub_task
            assert pub_resp.status_code == 201

    with patch.object(test_app.state.storage, "extract_tarball", side_effect=_slow_extract):
        asyncio.run(_run())


def test_nul_in_pax_path_returns_typed_400_without_paths_in_response_or_logs(
    test_client: TestClient,
    tmp_path: Path,
    caplog,
) -> None:
    import io
    import logging

    nul_tar = tmp_path / "route_pax_nul.tar.gz"
    with tarfile.open(nul_tar, "w:gz", format=tarfile.PAX_FORMAT) as tf:
        info = tarfile.TarInfo(name="placeholder.txt")
        info.pax_headers["path"] = "evil\x00path.txt"
        data = b"payload"
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))

    caplog.set_level(logging.WARNING, logger="seeknal.report_server")
    resp = _publish(test_client, nul_tar)
    assert resp.status_code == 400
    assert resp.json() == {"error": "Tarball entry name contains a NUL byte"}

    data_dir_str = str(test_client.app.state.config.data_dir)
    assert data_dir_str not in resp.text
    for rec in caplog.records:
        msg = rec.getMessage()
        assert data_dir_str not in msg
        assert "evil" not in msg
        assert "embedded null" not in msg


def test_overlong_name_and_file_dir_conflict_return_typed_400_without_paths_in_logs(
    test_client: TestClient,
    tmp_path: Path,
    caplog,
) -> None:
    import logging
    from tests.report_server.conftest import _make_tarball

    data_dir_str = str(test_client.app.state.config.data_dir)

    # 1. Overlong name (> 1024 chars)
    overlong_name = ("segment_" * 15) + "/" + ("x" * 950)
    assert len(overlong_name) > 1024
    overlong_tar = _make_tarball(
        tmp_path,
        [(overlong_name, b"data", "file")],
        filename="overlong_route.tar.gz",
    )
    caplog.clear()
    caplog.set_level(logging.WARNING, logger="seeknal.report_server")
    resp_long = _publish(test_client, overlong_tar)
    assert resp_long.status_code == 400
    assert resp_long.json() == {"error": "Tarball entry name too long"}
    assert data_dir_str not in resp_long.text
    for rec in caplog.records:
        msg = rec.getMessage()
        assert data_dir_str not in msg
        assert "segment_" not in msg

    # 2. File "a" followed by "a/b" (raises OSError during extraction with absolute dest path in exc text)
    conflict_tar = _make_tarball(
        tmp_path,
        [("a", b"file_content", "file"), ("a/b", b"nested_under_file", "file")],
        filename="file_then_dir_conflict.tar.gz",
    )
    caplog.clear()
    caplog.set_level(logging.WARNING, logger="seeknal.report_server")
    resp_conflict = _publish(test_client, conflict_tar)
    assert resp_conflict.status_code == 400
    assert resp_conflict.json() == {"error": "Invalid or corrupt tarball archive"}
    assert data_dir_str not in resp_conflict.text
    assert any("exc_class=" in rec.getMessage() for rec in caplog.records)
    for rec in caplog.records:
        msg = rec.getMessage()
        assert data_dir_str not in msg
        assert "/assets/" not in msg

    # 3. Single component over OS NAME_MAX (e.g. 300 chars <= 1024, raises ENAMETOOLONG OSError on extraction)
    enametoolong_tar = _make_tarball(
        tmp_path,
        [("z" * 300, b"too_long_for_fs", "file")],
        filename="enametoolong.tar.gz",
    )
    caplog.clear()
    caplog.set_level(logging.WARNING, logger="seeknal.report_server")
    resp_nametoolong = _publish(test_client, enametoolong_tar)
    assert resp_nametoolong.status_code == 400
    assert resp_nametoolong.json() == {"error": "Invalid or corrupt tarball archive"}
    assert data_dir_str not in resp_nametoolong.text
    for rec in caplog.records:
        msg = rec.getMessage()
        assert data_dir_str not in msg
        assert "zzzzzzzz" not in msg



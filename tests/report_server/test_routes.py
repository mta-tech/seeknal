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


def test_server_config_defaults_to_api_key(monkeypatch) -> None:
    from seeknal.report_server.config import ServerConfig

    assert ServerConfig().auth_mode == "api_key"
    monkeypatch.delenv("SEEKNAL_REPORT_SERVER_AUTH_MODE", raising=False)
    assert ServerConfig.from_env().auth_mode == "api_key"


def test_default_auth_mode_app_rejects_unauthenticated_publish_without_writing_assets(
    tmp_path: Path,
    honest_tarball: Path,
    monkeypatch,
) -> None:
    from seeknal.report_server.app import create_app
    from seeknal.report_server.config import ServerConfig

    monkeypatch.setattr("seeknal.report_server.config.is_insecure_path", lambda _p: False)
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)
    data_dir = tmp_path / "default-auth-data"
    cfg = ServerConfig(data_dir=data_dir, api_keys=["k"])
    assert cfg.auth_mode == "api_key"
    app = create_app(cfg)
    client = TestClient(app, raise_server_exceptions=True)

    resp = _publish(client, honest_tarball, auth=False)
    assert resp.status_code == 401
    assets_dir = data_dir / "assets"
    assert list(assets_dir.iterdir()) == []


def test_api_key_mode_without_keys_fails_closed_in_validate_and_create_app(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import pytest
    from seeknal.report_server.app import create_app
    from seeknal.report_server.config import ServerConfig

    monkeypatch.setattr("seeknal.report_server.config.is_insecure_path", lambda _p: False)
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)
    cfg = ServerConfig(data_dir=tmp_path / "no-keys-data", api_keys=[])
    with pytest.raises(ValueError) as exc_info:
        cfg.validate()
    err = str(exc_info.value)
    assert "SEEKNAL_REPORT_SERVER_KEYS" in err
    assert "SEEKNAL_REPORT_SERVER_AUTH_MODE=open" in err

    with pytest.raises(ValueError) as app_exc_info:
        create_app(cfg)
    app_err = str(app_exc_info.value)
    assert "SEEKNAL_REPORT_SERVER_KEYS" in app_err
    assert "SEEKNAL_REPORT_SERVER_AUTH_MODE=open" in app_err


def test_explicit_open_mode_allows_unauthenticated_publish_and_logs_warning(
    tmp_path: Path,
    honest_tarball: Path,
    monkeypatch,
    caplog,
) -> None:
    import logging
    from seeknal.report_server.app import create_app
    from seeknal.report_server.config import ServerConfig

    monkeypatch.setattr("seeknal.report_server.config.is_insecure_path", lambda _p: False)
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)

    # 1. Explicit auth_mode="open" emits WARNING on seeknal.report_server and allows unauth publish
    caplog.clear()
    caplog.set_level(logging.WARNING, logger="seeknal.report_server")
    open_cfg = ServerConfig(data_dir=tmp_path / "open-mode-data", auth_mode="open")
    open_app = create_app(open_cfg)
    open_warnings = [
        rec for rec in caplog.records
        if rec.name == "seeknal.report_server"
        and rec.levelno == logging.WARNING
        and "auth_mode=open" in rec.getMessage()
    ]
    assert len(open_warnings) == 1

    open_client = TestClient(open_app, raise_server_exceptions=True)
    resp = _publish(open_client, honest_tarball, auth=False)
    assert resp.status_code == 201

    # 2. api_key mode emits no such warning
    caplog.clear()
    caplog.set_level(logging.WARNING, logger="seeknal.report_server")
    key_cfg = ServerConfig(
        data_dir=tmp_path / "key-mode-data",
        auth_mode="api_key",
        api_keys=["secret-key-not-logged"],
    )
    create_app(key_cfg)
    key_warnings = [
        rec for rec in caplog.records
        if rec.name == "seeknal.report_server"
        and rec.levelno == logging.WARNING
        and "auth_mode=open" in rec.getMessage()
    ]
    assert key_warnings == []
    for rec in caplog.records:
        assert "secret-key-not-logged" not in rec.getMessage()


def test_serve_asset_blocks_path_traversal_and_unsafe_paths(
    test_app,
    test_client: TestClient,
    honest_tarball: Path,
    tmp_path: Path,
) -> None:
    import asyncio

    marker = "OUTSIDE_SECRET_MARKER_99182"
    outside_secret = tmp_path / "secret.txt"
    outside_secret.write_text(marker, encoding="utf-8")
    data_dir_secret = test_app.state.config.data_dir / "secret.txt"
    data_dir_secret.write_text(marker, encoding="utf-8")

    pub_resp = _publish(test_client, honest_tarball)
    assert pub_resp.status_code == 201
    slug = pub_resp.json()["slug"]

    # Place a symlink inside the slug directory pointing outside to verify resolved-path containment
    mount = test_app.state.storage.local_mount_path(slug)
    assert mount is not None
    (mount / "escaped_symlink.txt").symlink_to(outside_secret)

    # Honest nested asset and index still return 200
    honest_nested = test_client.get(f"/r/{slug}/_app/asset.js")
    assert honest_nested.status_code == 200
    assert b"console.log('hello');" in honest_nested.content

    # Pre-encoded URLs via TestClient (so the HTTP client does not normalise dot segments)
    encoded_cases = [
        f"/r/{slug}/..%2Fsecret.txt",
        f"/r/{slug}/..%2F..%2Fsecret.txt",
        f"/r/{slug}/..%2F..%2F..%2Fsecret.txt",
        f"/r/{slug}/..%2F..%2Fmetadata.db",
        f"/r/{slug}/%2e%2e/secret.txt",
        f"/r/{slug}/%2e%2e/%2e%2e/secret.txt",
        f"/r/{slug}/%2e%2e%2f%2e%2e%2fsecret.txt",
        f"/r/{slug}/%2F{outside_secret.as_posix().lstrip('/')}",
        f"/r/{slug}/%2Fetc%2Fpasswd",
        f"/r/{slug}/index.html%00.txt",
        f"/r/{slug}/_app%5Casset.js",
        f"/r/{slug}/..%5C..%5Csecret.txt",
        f"/r/{slug}/escaped_symlink.txt",
    ]
    for url in encoded_cases:
        resp = test_client.get(url)
        assert resp.status_code == 404, f"Expected 404 for {url!r}, got {resp.status_code}"
        assert marker.encode() not in resp.content
        assert b"SQLite format" not in resp.content
        assert "secret.txt" not in resp.text
        assert str(outside_secret) not in resp.text

    # Raw ASGI scope requests (matching uvicorn behaviour without dot-segment normalisation)
    raw_asgi_paths = [
        f"/r/{slug}/../../../secret.txt",
        f"/r/{slug}/../../secret.txt",
        f"/r/{slug}/../../metadata.db",
        f"/r/{slug}/_app/../../../secret.txt",
        f"/r/{slug}//{outside_secret.as_posix().lstrip('/')}",
        f"/r/{slug}//etc/passwd",
        f"/r/{slug}/a\x00b.txt",
        f"/r/{slug}/_app\\asset.js",
        f"/r/{slug}/..\\..\\secret.txt",
        f"/r/{slug}/escaped_symlink.txt",
    ]

    async def _call_raw_asgi(req_path: str) -> tuple[int, bytes]:
        status_code = 0
        body_parts: list[bytes] = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            elif message["type"] == "http.response.body":
                body_parts.append(message.get("body", b""))

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": req_path,
            "raw_path": req_path.encode("utf-8", errors="surrogateescape"),
            "query_string": b"",
            "headers": [],
            "app": test_app,
        }
        await test_app(scope, receive, send)
        return status_code, b"".join(body_parts)

    for raw_path in raw_asgi_paths:
        status, body = asyncio.run(_call_raw_asgi(raw_path))
        assert status == 404, f"Expected 404 for raw ASGI {raw_path!r}, got {status}"
        assert marker.encode() not in body
        assert b"SQLite format" not in body
        assert b"secret.txt" not in body

    # Also verify fallback to storage.open_asset when local_mount_path returns None
    with patch.object(test_app.state.storage, "local_mount_path", return_value=None):
        honest_fallback = test_client.get(f"/r/{slug}/_app/asset.js")
        assert honest_fallback.status_code == 200
        assert b"console.log('hello');" in honest_fallback.content

        for raw_path in raw_asgi_paths:
            status, body = asyncio.run(_call_raw_asgi(raw_path))
            assert status == 404, f"Expected 404 on open_asset branch for {raw_path!r}, got {status}"
            assert marker.encode() not in body
            assert b"secret.txt" not in body





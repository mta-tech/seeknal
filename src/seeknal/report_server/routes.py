from __future__ import annotations

import io
import mimetypes
from datetime import datetime, timezone

from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, RedirectResponse, Response, StreamingResponse

from seeknal.report_server import owner, slugs
from seeknal.report_server.auth import AuthError, require_publish_auth
from seeknal.report_server.models import Publish, get_session
from seeknal.report_server.storage import TarSafetyError


async def publish(request: Request) -> JSONResponse:
    content_type = request.headers.get("content-type", "")
    if content_type not in ("application/gzip", "application/x-gzip"):
        return JSONResponse({"error": "Content-Type must be application/gzip"}, status_code=415)

    content_length = request.headers.get("content-length")
    config = request.app.state.config
    if content_length is not None and int(content_length) > config.max_upload_bytes:
        return JSONResponse({"error": "Upload exceeds maximum allowed size"}, status_code=413)

    try:
        await require_publish_auth(request, config)
    except AuthError as exc:
        return JSONResponse({"error": str(exc)}, status_code=401)

    body_bytes = await request.body()
    if len(body_bytes) > config.max_upload_bytes:
        return JSONResponse({"error": "Upload exceeds maximum allowed size"}, status_code=413)

    engine = request.app.state.engine
    storage = request.app.state.storage

    with get_session(engine) as session:
        slug = slugs.generate_unique(session)
        plain_secret, hash_hex, salt_hex = owner.mint()

        report_name = request.headers.get("X-Seeknal-Report-Name", slug)
        report_title = request.headers.get("X-Seeknal-Report-Title") or None

        try:
            asset_relpath = storage.extract_tarball(slug, io.BytesIO(body_bytes))
        except TarSafetyError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)

        created_at = datetime.now(timezone.utc)
        pub = Publish(
            slug=slug,
            report_name=report_name,
            report_title=report_title,
            owner_secret_hash=hash_hex,
            owner_secret_salt=salt_hex,
            created_at=created_at,
            asset_relpath=asset_relpath,
            byte_size=len(body_bytes),
        )
        session.add(pub)
        session.commit()

    return JSONResponse(
        {
            "slug": slug,
            "share_url": f"/r/{slug}",
            "owner_secret": plain_secret,
            "created_at": created_at.isoformat(),
        },
        status_code=201,
    )


async def revoke(request: Request) -> JSONResponse:
    slug = request.path_params["slug"]
    secret = request.headers.get("X-Seeknal-Owner-Secret")
    if not secret:
        return JSONResponse({"error": "X-Seeknal-Owner-Secret header required"}, status_code=400)

    engine = request.app.state.engine
    with get_session(engine) as session:
        pub = session.get(Publish, slug)
        if pub is None:
            return JSONResponse({"error": "Report not found"}, status_code=404)

        if not owner.verify(secret, pub.owner_secret_hash, pub.owner_secret_salt):
            return JSONResponse({"error": "Invalid owner secret"}, status_code=403)

        pub.revoked_at = datetime.now(timezone.utc)
        session.add(pub)
        session.commit()
        revoked_at = pub.revoked_at

    return JSONResponse({"slug": slug, "revoked_at": revoked_at.isoformat()})


async def serve_asset(request: Request) -> Response:
    slug = request.path_params["slug"]
    has_path_param = "path" in request.path_params
    path = request.path_params.get("path", "").lstrip("/")

    # Redirect /r/{slug} (no trailing slash) to /r/{slug}/ so relative URLs
    # in served HTML resolve correctly. Without this, ./data/report.duckdb
    # resolves to /r/data/report.duckdb instead of /r/{slug}/data/report.duckdb.
    if not has_path_param:
        return RedirectResponse(url=f"/r/{slug}/", status_code=308)

    engine = request.app.state.engine
    storage = request.app.state.storage

    with get_session(engine) as session:
        pub = session.get(Publish, slug)

    if pub is None:
        return JSONResponse({"error": "Report not found"}, status_code=404)

    if pub.revoked_at is not None:
        return JSONResponse({"error": "Report has been revoked"}, status_code=410)

    if not path:
        path = "index.html"

    mount = storage.local_mount_path(slug)
    if mount is not None:
        full_path = mount / path
        if not full_path.exists():
            return JSONResponse({"error": "Not found"}, status_code=404)
        if full_path.is_dir():
            return JSONResponse({"error": "Not found"}, status_code=404)
        mime, _ = mimetypes.guess_type(str(full_path))
        return FileResponse(str(full_path), media_type=mime or "application/octet-stream")

    try:
        fileobj = storage.open_asset(slug, path)
    except (FileNotFoundError, IsADirectoryError):
        return JSONResponse({"error": "Not found"}, status_code=404)

    mime, _ = mimetypes.guess_type(path)

    def _iter():
        try:
            while True:
                chunk = fileobj.read(65536)
                if not chunk:
                    break
                yield chunk
        finally:
            fileobj.close()

    return StreamingResponse(_iter(), media_type=mime or "application/octet-stream")


async def healthz(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "version": "0.1.0"})

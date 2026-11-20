from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import tarfile
import tempfile
from datetime import datetime, timezone

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, RedirectResponse, Response, StreamingResponse

from seeknal.report_server import owner, slugs
from seeknal.report_server.auth import AuthError, require_publish_auth
from seeknal.report_server.models import Publish, get_session
from seeknal.report_server.storage import TarSafetyError

_log = logging.getLogger("seeknal.report_server")

SPOOL_MAX_SIZE = 8 * 1024 * 1024  # 8 MiB in-memory threshold before rolling to disk inside data_dir


def _get_publish_semaphore(app, config) -> asyncio.Semaphore:
    sem = getattr(app.state, "publish_semaphore", None)
    cap = getattr(app.state, "publish_semaphore_capacity", None)
    if sem is None or cap != config.max_concurrent_publishes:
        sem = asyncio.Semaphore(config.max_concurrent_publishes)
        app.state.publish_semaphore = sem
        app.state.publish_semaphore_capacity = config.max_concurrent_publishes
    return sem


_FILTER_ERROR_TYPE = getattr(tarfile, "FilterError", TarSafetyError)
_OUTSIDE_DEST_ERROR_TYPE = getattr(tarfile, "OutsideDestinationError", ())
_LINK_OUTSIDE_ERROR_TYPE = getattr(tarfile, "LinkOutsideDestinationError", ())
_ABSOLUTE_PATH_ERROR_TYPE = getattr(tarfile, "AbsolutePathError", ())
_ABSOLUTE_LINK_ERROR_TYPE = getattr(tarfile, "AbsoluteLinkError", ())
_SPECIAL_FILE_ERROR_TYPE = getattr(tarfile, "SpecialFileError", ())


def _classify_tar_error(exc: Exception) -> str:
    if isinstance(exc, MemoryError):
        return "Tarball uncompressed size exceeds limit"
    if isinstance(exc, TarSafetyError):
        msg = str(exc)
        if ":" in msg:
            return msg.split(":", 1)[0].strip()
        return msg or "Tarball failed safety check"
    if _ABSOLUTE_PATH_ERROR_TYPE and isinstance(exc, _ABSOLUTE_PATH_ERROR_TYPE):
        return "Tarball entry has absolute path"
    if _ABSOLUTE_LINK_ERROR_TYPE and isinstance(exc, _ABSOLUTE_LINK_ERROR_TYPE):
        return "Tarball entry is a symlink"
    if _LINK_OUTSIDE_ERROR_TYPE and isinstance(exc, _LINK_OUTSIDE_ERROR_TYPE):
        return "Tarball entry is a symlink"
    if _OUTSIDE_DEST_ERROR_TYPE and isinstance(exc, _OUTSIDE_DEST_ERROR_TYPE):
        return "Tarball entry escapes destination"
    if _SPECIAL_FILE_ERROR_TYPE and isinstance(exc, _SPECIAL_FILE_ERROR_TYPE):
        return "Tarball entry is a special or device file"
    if isinstance(exc, _FILTER_ERROR_TYPE):
        return "Tarball entry rejected by extraction filter"
    return "Invalid or corrupt tarball archive"


async def publish(request: Request) -> JSONResponse:
    content_type = request.headers.get("content-type", "")
    if content_type not in ("application/gzip", "application/x-gzip"):
        return JSONResponse({"error": "Content-Type must be application/gzip"}, status_code=415)

    content_length = request.headers.get("content-length")
    config = request.app.state.config
    if content_length is not None:
        try:
            parsed_content_length = int(content_length.strip())
        except ValueError:
            return JSONResponse({"error": "Invalid Content-Length header"}, status_code=400)
        if parsed_content_length < 0:
            return JSONResponse({"error": "Invalid Content-Length header"}, status_code=400)
        if not content_length.strip().isdigit():
            return JSONResponse({"error": "Invalid Content-Length header"}, status_code=400)
        if parsed_content_length > config.max_upload_bytes:
            return JSONResponse({"error": "Upload exceeds maximum allowed size"}, status_code=413)

    try:
        await require_publish_auth(request, config)
    except AuthError as exc:
        return JSONResponse({"error": str(exc)}, status_code=401)

    semaphore = _get_publish_semaphore(request.app, config)
    if semaphore.locked():
        return JSONResponse(
            {"error": "Too many concurrent publish requests"},
            status_code=503,
            headers={"Retry-After": "1"},
        )
    await semaphore.acquire()

    spool_dir = config.data_dir / "tmp"
    spool_dir.mkdir(parents=True, exist_ok=True)
    spool = tempfile.SpooledTemporaryFile(
        max_size=SPOOL_MAX_SIZE,
        mode="w+b",
        dir=str(spool_dir),
    )
    try:
        running_bytes = 0
        async for chunk in request.stream():
            if not chunk:
                continue
            running_bytes += len(chunk)
            if running_bytes > config.max_upload_bytes:
                return JSONResponse({"error": "Upload exceeds maximum allowed size"}, status_code=413)
            spool.write(chunk)
        spool.seek(0)

        engine = request.app.state.engine
        storage = request.app.state.storage

        with get_session(engine) as session:
            slug = slugs.generate_unique(session)
            plain_secret, hash_hex, salt_hex = owner.mint()

            report_name = request.headers.get("X-Seeknal-Report-Name", slug)
            report_title = request.headers.get("X-Seeknal-Report-Title") or None

            try:
                asset_relpath = await run_in_threadpool(
                    storage.extract_tarball, slug, spool
                )
            except (
                TarSafetyError,
                _FILTER_ERROR_TYPE,
                tarfile.TarError,
                EOFError,
                OSError,
                ValueError,
                MemoryError,
            ) as exc:
                _log.warning(
                    "publish tarball rejected: slug=%s exc_class=%s",
                    slug,
                    type(exc).__name__,
                )
                return JSONResponse({"error": _classify_tar_error(exc)}, status_code=400)

            created_at = datetime.now(timezone.utc)
            pub = Publish(
                slug=slug,
                report_name=report_name,
                report_title=report_title,
                owner_secret_hash=hash_hex,
                owner_secret_salt=salt_hex,
                created_at=created_at,
                asset_relpath=asset_relpath,
                byte_size=running_bytes,
            )
            try:
                session.add(pub)
                session.commit()
            except Exception as exc:
                try:
                    session.rollback()
                except Exception:
                    pass
                storage.delete(slug)
                _log.error(
                    "publish database commit failed: slug=%s exc_class=%s",
                    slug,
                    type(exc).__name__,
                )
                return JSONResponse({"error": "Publish failed"}, status_code=500)
    finally:
        spool.close()
        semaphore.release()

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
    raw_path = request.path_params.get("path", "")

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

    path = raw_path if raw_path != "" else "index.html"
    if (
        "\x00" in path
        or "\\" in path
        or os.path.isabs(path)
        or path.startswith("/")
        or (len(path) >= 2 and path[0].isalpha() and path[1] == ":")
        or ".." in path.split("/")
    ):
        return JSONResponse({"error": "Not found"}, status_code=404)

    mount = storage.local_mount_path(slug)
    if mount is not None:
        try:
            mount_resolved = mount.resolve()
            full_path = (mount_resolved / path).resolve()
        except (OSError, ValueError):
            return JSONResponse({"error": "Not found"}, status_code=404)
        if not full_path.is_relative_to(mount_resolved) or full_path == mount_resolved:
            return JSONResponse({"error": "Not found"}, status_code=404)
        if not full_path.exists() or full_path.is_dir() or not full_path.is_file():
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

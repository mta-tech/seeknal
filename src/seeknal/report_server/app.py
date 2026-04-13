from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Route

from seeknal.report_server.config import ServerConfig
from seeknal.report_server.models import get_engine
from seeknal.report_server.routes import healthz, publish, revoke, serve_asset
from seeknal.report_server.storage import LocalFilesystemStorage

_log = logging.getLogger("seeknal.report_server")


def create_app(config: Optional[ServerConfig] = None) -> Starlette:
    if config is None:
        config = ServerConfig.from_env()

    config.validate()
    config.data_dir.mkdir(parents=True, exist_ok=True)

    storage = LocalFilesystemStorage(config.data_dir)
    engine = get_engine(config.data_dir)

    @asynccontextmanager
    async def lifespan(app: Starlette):
        _log.info(
            '{"event": "startup", "service": "seeknal-report-server", "version": "0.1.0", '
            '"data_dir": "%s", "auth_mode": "%s"}',
            config.data_dir,
            config.auth_mode,
        )
        yield
        _log.info('{"event": "shutdown", "service": "seeknal-report-server"}')

    routes = [
        Route("/publish", publish, methods=["POST"]),
        Route("/revoke/{slug}", revoke, methods=["POST"]),
        Route("/healthz", healthz, methods=["GET"]),
        Route("/r/{slug}/{path:path}", serve_asset, methods=["GET"]),
        Route("/r/{slug}", serve_asset, methods=["GET"]),
    ]

    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.config = config
    app.state.storage = storage
    app.state.engine = engine

    if config.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.cors_origins,
            allow_methods=["GET", "POST"],
            allow_headers=["*"],
        )

    return app

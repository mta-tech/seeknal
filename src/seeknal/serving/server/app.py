"""Application assembly for Seeknal Serving."""

from __future__ import annotations

import datetime
import json
import logging
import os
import sys
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import duckdb
import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from seeknal.connections.starrocks import create_starrocks_connection_from_url
from seeknal.serving.adapters import (
    CubeAdapter,
    CubeSecurityConfig,
    DBAPIAdapter,
    DuckDBAdapter,
    EngineAdapter,
    build_adapter,
    build_workload_adapters,
)
from seeknal.serving.discovery import PlacementRefresher, discover
from seeknal.serving.discovery.build import DiscoveryResult
from seeknal.serving.http import create_app
from seeknal.serving.placement import Placement
from seeknal.serving.server.config import (
    CubeConnectionConfig,
    DuckDBConnectionConfig,
    LakebaseConnectionConfig,
    LoggingConfig,
    ServerConfig,
    ServerConfigError,
    StarRocksAccountConfig,
    StarRocksConnectionConfig,
    read_secret_file,
)
from seeknal.serving.service import ServingPipeline

logger = logging.getLogger(__name__)


class JsonFormatter(logging.Formatter):
    """Formatter emitting JSON objects per log line."""

    def format(self, record: logging.LogRecord) -> str:
        data = {
            "timestamp": datetime.datetime.fromtimestamp(
                record.created, tz=datetime.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data)


def configure_logging(cfg: LoggingConfig) -> None:
    """Configure standard logging from LoggingConfig before server startup."""
    level = getattr(logging, cfg.level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        root.removeHandler(h)

    if cfg.file:
        handler = logging.FileHandler(cfg.file, encoding="utf-8")
    else:
        handler = logging.StreamHandler(sys.stderr)

    if cfg.format == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

    root.addHandler(handler)


@dataclass(frozen=True)
class ServerHandle:
    """Handle to a built serving server."""

    app: Starlette
    pipeline: ServingPipeline
    refresher: PlacementRefresher | None
    adapters: dict[str, EngineAdapter] = field(default_factory=dict)

    def __iter__(self):
        yield self.app
        yield self.pipeline
        yield self.refresher


def build_server(
    config: ServerConfig | Mapping | str | os.PathLike,
    *,
    env: Mapping[str, str] = os.environ,
    connect_factories: Mapping[str, Any] | None = None,
    discover_fn: Callable[[], DiscoveryResult] | None = None,
) -> ServerHandle:
    """Build a complete Seeknal Serving server handle from configuration."""
    # 1. Resolve configuration
    if isinstance(config, (str, os.PathLike)):
        cfg = ServerConfig.from_yaml(config, env=env)
    elif isinstance(config, Mapping) and not isinstance(config, ServerConfig):
        cfg = ServerConfig.from_dict(config, env=env)
    elif isinstance(config, ServerConfig):
        cfg = config
    else:
        raise TypeError(f"Unsupported config type: {type(config).__name__}")

    # 2. Check that all referenced environment variables exist at startup
    cfg.check_env(env=env)

    # 3. Configure logging
    configure_logging(cfg.logging)

    # 4. Load grant keys
    public_keys = cfg.load_grant_keys()

    # 5. Build ServingPipeline and PlacementRefresher
    refresher: PlacementRefresher | None = None
    if cfg.placement.is_static:
        static_cfg = cfg.placement.static
        assert static_cfg is not None
        placement = Placement.from_yaml(static_cfg.placement_file)
        try:
            with open(static_cfg.schemas_file, "r", encoding="utf-8") as f:
                schemas = json.load(f)
        except Exception as exc:
            raise ServerConfigError(
                f"Failed to load schemas file '{static_cfg.schemas_file}': {exc}"
            ) from exc

        pipeline = ServingPipeline(
            placement=placement,
            schemas=schemas,
            public_keys=public_keys,
            default_catalog=cfg.default_catalog,
            cube_mode=cfg.cube_mode,
            require_grant_typ=cfg.require_grant_typ,
        )
    else:
        empty_placement = Placement(engines={}, tables={}, cubes={})
        pipeline = ServingPipeline(
            placement=empty_placement,
            schemas={},
            public_keys=public_keys,
            default_catalog=cfg.default_catalog,
            cube_mode=cfg.cube_mode,
            require_grant_typ=cfg.require_grant_typ,
        )

        disc_cfg = cfg.placement.discovery_config
        assert disc_cfg is not None

        if discover_fn is not None:
            disc_call = discover_fn
        else:
            lb_connect = None
            if disc_cfg.lakebase is not None:
                lb_eng = disc_cfg.lakebase.engine
                if connect_factories and lb_eng in connect_factories:
                    lb_connect = connect_factories[lb_eng]
                elif lb_eng in cfg.connections and isinstance(
                    cfg.connections[lb_eng], LakebaseConnectionConfig
                ):
                    lb_conn_cfg = cfg.connections[lb_eng]
                    lb_connect = lambda: _make_lakebase_connect(
                        lb_conn_cfg.dsn_env, cfg, env
                    )()

            effective_env = dict(env)
            if cfg.secrets_env:
                effective_env.update(cfg.secrets_env)

            disc_call = lambda: _run_discovery(
                cfg.placement.discovery, client=None, lb_connect=lb_connect, env=effective_env
            )

        refresher = PlacementRefresher(
            discover_fn=disc_call,
            pipeline=pipeline,
            interval_seconds=disc_cfg.refresh_interval_seconds,
        )

    # 6. Build adapters from connections
    adapters: dict[str, EngineAdapter] = {}
    for eng_name, conn_cfg in cfg.connections.items():
        if isinstance(conn_cfg, StarRocksConnectionConfig):
            connects: dict[str, Callable[[], Any]] = {}
            for acc_name, acc in conn_cfg.accounts.items():
                connect_factory = None
                if connect_factories:
                    if eng_name in connect_factories:
                        cf = connect_factories[eng_name]
                        if isinstance(cf, Mapping) and acc_name in cf:
                            connect_factory = cf[acc_name]
                        elif callable(cf):
                            connect_factory = cf
                    if (
                        connect_factory is None
                        and f"{eng_name}.{acc_name}" in connect_factories
                    ):
                        connect_factory = connect_factories[f"{eng_name}.{acc_name}"]

                if connect_factory is not None:
                    connects[acc_name] = connect_factory
                elif acc.url_env:
                    connects[acc_name] = _make_starrocks_url_connect(
                        acc.url_env, cfg, env
                    )
                else:
                    connects[acc_name] = _make_starrocks_fields_connect(acc, cfg, env)

            adapters[eng_name] = build_workload_adapters("starrocks", connects)

        elif isinstance(conn_cfg, LakebaseConnectionConfig):
            if connect_factories and eng_name in connect_factories:
                conn_fn = connect_factories[eng_name]
            else:
                conn_fn = _make_lakebase_connect(conn_cfg.dsn_env, cfg, env)
            adapters[eng_name] = build_adapter("lakebase", conn_fn)

        elif isinstance(conn_cfg, CubeConnectionConfig):
            if connect_factories and eng_name in connect_factories:
                conn_fn = connect_factories[eng_name]
            else:
                conn_fn = _make_cube_connect(
                    host=conn_cfg.host,
                    port=conn_cfg.port,
                    dbname=conn_cfg.dbname,
                    user=conn_cfg.sql_user,
                    connect_timeout=conn_cfg.connect_timeout_seconds,
                )

            if cfg.cube_mode == "jwt":
                secret_str = cfg.get_env(conn_cfg.secret_env, env)
                security = CubeSecurityConfig(
                    secret=secret_str.encode("utf-8"),
                    sql_user=conn_cfg.sql_user,
                )
                adapters[eng_name] = CubeAdapter(
                    conn_fn,
                    security=security,
                    connect_timeout_seconds=conn_cfg.connect_timeout_seconds,
                )
            else:
                adapters[eng_name] = DBAPIAdapter(conn_fn, kind="cube")

        elif isinstance(conn_cfg, DuckDBConnectionConfig):
            if connect_factories and eng_name in connect_factories:
                conn_fn = connect_factories[eng_name]
            else:
                p = conn_cfg.path
                conn_fn = lambda: duckdb.connect(p, read_only=True)
            adapters[eng_name] = DuckDBAdapter(conn_fn)

    # 7. Add /readyz route and Lifespan
    async def readyz(request: Request) -> Response:
        is_ready = False
        if cfg.placement.is_static:
            is_ready = True
        elif refresher is not None and refresher.last_success_at is not None:
            is_ready = True

        if is_ready:
            return JSONResponse({"ready": True}, status_code=200)
        return JSONResponse(
            {"ready": False, "reason": "catalog_not_loaded"}, status_code=503
        )

    @asynccontextmanager
    async def lifespan(app: Starlette):
        if refresher is not None:
            refresher.refresh_once()
            refresher.start()
        try:
            yield
        finally:
            if refresher is not None:
                refresher.stop()

    extra_routes = [
        Route("/readyz", readyz, methods=["GET"]),
    ]

    app = create_app(
        pipeline=pipeline,
        adapters=adapters,
        timeout_seconds=cfg.limits.timeout_seconds,
        max_body_bytes=cfg.limits.max_body_bytes,
        max_result_bytes=cfg.limits.max_result_bytes,
        extra_routes=extra_routes,
        lifespan=lifespan,
    )

    return ServerHandle(
        app=app, pipeline=pipeline, refresher=refresher, adapters=adapters
    )


def _run_discovery(
    disc_data: dict,
    *,
    client: httpx.Client | None = None,
    lb_connect: Callable[[], Any] | None = None,
    env: Mapping[str, str] = os.environ,
) -> DiscoveryResult:
    if client is not None:
        return discover(
            disc_data, http_client=client, lakebase_connect=lb_connect, env=env
        )
    with httpx.Client() as c:
        return discover(disc_data, http_client=c, lakebase_connect=lb_connect, env=env)


def _make_starrocks_url_connect(
    url_env: str, cfg: ServerConfig, env: Mapping[str, str]
) -> Callable[[], Any]:
    def connect() -> Any:
        url = cfg.get_env(url_env, env)
        return create_starrocks_connection_from_url(url)

    return connect


def _make_starrocks_fields_connect(
    acc: StarRocksAccountConfig, cfg: ServerConfig, env: Mapping[str, str]
) -> Callable[[], Any]:
    def connect() -> Any:
        if acc.password_env:
            password = cfg.get_env(acc.password_env, env)
        elif acc.password_file:
            password = read_secret_file(acc.password_file)
        else:
            password = ""
        import pymysql

        kwargs = {
            "host": acc.host,
            "port": acc.port,
            "user": acc.user,
            "password": password,
            "charset": "utf8mb4",
            "connect_timeout": 10,
        }
        if acc.database:
            kwargs["database"] = acc.database
        return pymysql.connect(**kwargs)

    return connect


def _make_lakebase_connect(
    dsn_env: str, cfg: ServerConfig, env: Mapping[str, str]
) -> Callable[[], Any]:
    def connect() -> Any:
        dsn = cfg.get_env(dsn_env, env)
        import psycopg2

        return psycopg2.connect(dsn)

    return connect


def _make_cube_connect(
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    connect_timeout: float,
) -> Callable[..., Any]:
    def connect(**kwargs: Any) -> Any:
        import psycopg2

        u = kwargs.get("user", user)
        p = kwargs.get("password", "")
        to = kwargs.get("connect_timeout", connect_timeout)
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=u,
            password=p,
            connect_timeout=to,
        )

    return connect

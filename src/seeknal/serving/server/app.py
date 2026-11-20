"""Application assembly for Seeknal Serving."""

from __future__ import annotations

import datetime
import inspect
import json
import logging
import math
import os
import sys
import time
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
    EngineConfigError,
    LakebasePool,
    PostgresPool,
    build_adapter,
    build_workload_adapters,
)
from seeknal.serving.audit import (
    AuditSink,
    AuditWriteError,
    FileAuditSink,
    _safe_diag_target,
    build_audit_sink,
)
from seeknal.serving.discovery import PlacementRefresher, discover
from seeknal.serving.discovery.build import DiscoveryResult
from seeknal.serving.duckdb_policy import DuckDBHolder, connect_hardened
from seeknal.serving.errors import register_config_secrets
from seeknal.serving.http import GrantReplayCache, create_app
from seeknal.serving.metrics import MetricsRegistry
from seeknal.serving.ops import (
    ReadinessProbe,
    build_public_extra_routes,
    create_ops_app,
)
from seeknal.serving.placement import Placement
from seeknal.serving.postgres_session import (
    build_connect_kwargs,
    verify_and_harden_session,
)
from seeknal.serving.server.config import (
    AuditStartupError,
    CubeConnectionConfig,
    DuckDBConnectionConfig,
    LakebaseConnectionConfig,
    LoggingConfig,
    PostgresConnectionConfig,
    ServerConfig,
    ServerConfigError,
    StarRocksAccountConfig,
    StarRocksConnectionConfig,
    read_secret_file,
)
from seeknal.serving.service import ServingPipeline
from seeknal.serving.startup_validation import (
    _collect_secret_values,
    _scrub,
    resolve_startup_cursor_key,
    run_initial_discovery_refresh_or_raise,
)

try:
    from seeknal.serving.lakebase_endpoint import (
        ColdStartConnector,
        EngineStartingError,
        effective_storage_class,
    )
except ImportError:
    from seeknal.serving.postgres_session import (
        ColdStartConnector,
        EngineStartingError,
        effective_storage_class,
    )

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
    audit_sink: AuditSink | None = None
    ops_app: Starlette | None = None
    metrics: MetricsRegistry | None = None

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
    audit_failure_callback: Callable[[], Any] | None = None,
    readiness_clock: Callable[[], float] | None = None,
    run_initial_refresh: bool = False,
    cursor_key: bytes | str | None = None,
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

    # 2. Run fail-closed startup validation & offline preflight (env, secret files,
    # static placement/schemas/columns/roles/PII rules, engine-connection wiring)
    # before configuring a file logger or constructing adapters/refresher
    secret_values = _collect_secret_values(env, cfg=cfg)
    register_config_secrets(*secret_values)
    preflight = cfg.validate_runtime_wiring(
        env=env,
        check_cursor_key=True,
        cursor_key=cursor_key,
    )
    resolved_cursor_key = resolve_startup_cursor_key(
        cfg,
        cursor_key=cursor_key,
        env=env,
    )
    secret_values = _collect_secret_values(env, cfg=cfg)
    register_config_secrets(*secret_values)

    # 3. Load grant keys before configuring file logging
    public_keys = cfg.load_grant_keys()

    # 3b. Construct fail-closed audit sink before file logger / refresher / adapters
    audit_sink: AuditSink | None = None
    if cfg.audit is not None and cfg.audit.on_failure == "refuse":
        try:
            audit_sink = build_audit_sink(
                cfg.audit,
                on_write_failure=audit_failure_callback,
            )
        except AuditWriteError as exc:
            raise AuditStartupError(str(exc)) from exc

    # 4. Configure logging
    configure_logging(cfg.logging)

    if isinstance(audit_sink, FileAuditSink) and (
        audit_sink._max_bytes is None
        and audit_sink._max_files is None
        and audit_sink._max_age_seconds is None
    ):
        logger.warning(
            "Audit file sink '%s' has no max_bytes rotation and no retention "
            "(max_files/max_age_seconds) configured; audit log may grow unbounded",
            _safe_diag_target(audit_sink.path),
        )

    if cfg.audit is not None and audit_sink is None:
        audit_sink = build_audit_sink(
            cfg.audit,
            on_write_failure=audit_failure_callback,
        )

    # Pre-create MetricsRegistry and optional cold-start HTTP client so Lakebase
    # ColdStartConnectors and discovery connect callables share the same registry
    # and single-flight ColdStartConnector keyed by endpoint_id.
    metrics = MetricsRegistry(allowed_engines=tuple(cfg.connections.keys()))
    cold_start_http_clients: list[httpx.Client] = []
    connectors_by_endpoint: dict[str, Any] = {}

    def _get_cold_start_http_client() -> httpx.Client:
        if not cold_start_http_clients:
            cold_start_http_clients.append(httpx.Client())
        return cold_start_http_clients[0]

    # 5. Build ServingPipeline and PlacementRefresher
    last_refresh_error: list[Exception | None] = [None]
    last_refresh_error_scrubbed: list[str | None] = [None]
    refresher: PlacementRefresher | None = None
    if cfg.placement.is_static:
        assert preflight.placement is not None
        assert preflight.schemas is not None

        pipeline = ServingPipeline(
            placement=preflight.placement,
            schemas=preflight.schemas,
            public_keys=public_keys,
            default_catalog=cfg.default_catalog,
            cube_mode=cfg.cube_mode,
            require_grant_typ=cfg.require_grant_typ,
            max_grant_lifetime_seconds=cfg.grant_max_lifetime_seconds,
            compilation_cache_max_entries=cfg.compilation_cache_max_entries,
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
            max_grant_lifetime_seconds=cfg.grant_max_lifetime_seconds,
            compilation_cache_max_entries=cfg.compilation_cache_max_entries,
        )

        disc_cfg = cfg.placement.discovery_config
        assert disc_cfg is not None

        if discover_fn is not None:
            raw_disc_call = discover_fn
        else:
            lb_connect: Callable[[], Any] | None = None
            if disc_cfg.lakebase is not None:
                lb_eng = disc_cfg.lakebase.engine
                lb_custom = _resolve_custom_factory(
                    connect_factories, lb_eng, "interactive"
                )
                if lb_eng in cfg.connections and isinstance(
                    cfg.connections[lb_eng],
                    (PostgresConnectionConfig, LakebaseConnectionConfig),
                ):
                    lb_conn_cfg = cfg.connections[lb_eng]
                    lb_connect = _make_discovery_source_connect(
                        eng_name=lb_eng,
                        conn_cfg=lb_conn_cfg,
                        account_name="interactive",
                        cfg=cfg,
                        env=env,
                        connect_factories=connect_factories,
                        metrics=metrics,
                        get_http_client=_get_cold_start_http_client,
                        connectors_by_endpoint=connectors_by_endpoint,
                    )
                elif lb_custom is not None:
                    lb_connect = lb_custom

            postgres_connects = _build_discovery_postgres_connects(
                cfg=cfg,
                disc_cfg=disc_cfg,
                env=env,
                connect_factories=connect_factories,
                metrics=metrics,
                get_http_client=_get_cold_start_http_client,
                connectors_by_endpoint=connectors_by_endpoint,
            )

            effective_env = dict(env)
            if cfg.secrets_env:
                effective_env.update(cfg.secrets_env)

            def _call_run_discovery() -> DiscoveryResult:
                if _run_discovery is _ORIG_RUN_DISCOVERY:
                    return _run_discovery(
                        cfg.placement.discovery,
                        client=None,
                        lb_connect=lb_connect,
                        postgres_connects=postgres_connects,
                        connections=cfg.connections,
                        env=effective_env,
                    )
                try:
                    return _run_discovery(
                        cfg.placement.discovery,
                        client=None,
                        lb_connect=lb_connect,
                        postgres_connects=postgres_connects,
                        connections=cfg.connections,
                        env=effective_env,
                    )
                except TypeError as exc:
                    if (
                        exc.__traceback__ is not None
                        and exc.__traceback__.tb_next is None
                        and "unexpected keyword argument" in str(exc)
                    ):
                        return _run_discovery(
                            cfg.placement.discovery,
                            client=None,
                            lb_connect=lb_connect,
                            env=effective_env,
                        )
                    raise

            raw_disc_call = _call_run_discovery

        def _tracked_disc_call() -> DiscoveryResult:
            last_refresh_error[0] = None
            last_refresh_error_scrubbed[0] = None
            try:
                res = raw_disc_call()
                Placement.from_dict(res.placement)
                return res
            except Exception as exc:
                scrubbed = _scrub(str(exc), secret_values)
                last_refresh_error[0] = exc
                last_refresh_error_scrubbed[0] = scrubbed
                logger.error(
                    "Catalog discovery refresh failed (%s): %s",
                    type(exc).__name__,
                    scrubbed,
                )
                raise

        refresher = PlacementRefresher(
            discover_fn=_tracked_disc_call,
            pipeline=pipeline,
            interval_seconds=disc_cfg.refresh_interval_seconds,
        )
        setattr(refresher, "last_refresh_error", last_refresh_error)
        setattr(refresher, "last_refresh_error_scrubbed", last_refresh_error_scrubbed)

        if run_initial_refresh:
            run_initial_discovery_refresh_or_raise(
                refresher,
                cfg=cfg,
                env=env,
                secret_values=secret_values,
            )

    # 6. Build adapters from connections
    adapters: dict[str, EngineAdapter] = {}
    for eng_name, conn_cfg in cfg.connections.items():
        eff_sc = effective_storage_class(conn_cfg)
        if isinstance(conn_cfg, StarRocksConnectionConfig):
            connects: dict[str, Callable[[], Any]] = {}
            for acc_name, acc in conn_cfg.accounts.items():
                connect_factory = _resolve_custom_factory(
                    connect_factories, eng_name, acc_name
                )
                if connect_factory is not None:
                    connects[acc_name] = connect_factory
                elif acc.url_env:
                    connects[acc_name] = _make_starrocks_url_connect(
                        acc.url_env, cfg, env
                    )
                else:
                    connects[acc_name] = _make_starrocks_fields_connect(acc, cfg, env)

            sr_adapter = build_workload_adapters(
                "starrocks",
                connects,
                storage_class=eff_sc,
            )
            sr_adapter.storage_class = eff_sc
            adapters[eng_name] = sr_adapter

        elif isinstance(
            conn_cfg, (PostgresConnectionConfig, LakebaseConnectionConfig)
        ):
            pg_adapter = _build_postgres_wire_adapter(
                eng_name=eng_name,
                conn_cfg=conn_cfg,
                cfg=cfg,
                env=env,
                connect_factories=connect_factories,
                metrics=metrics,
                get_http_client=_get_cold_start_http_client,
                storage_class=eff_sc,
                connectors_by_endpoint=connectors_by_endpoint,
            )
            adapters[eng_name] = pg_adapter

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
                cube_ad: EngineAdapter = CubeAdapter(
                    conn_fn,
                    security=security,
                    connect_timeout_seconds=conn_cfg.connect_timeout_seconds,
                )
            else:
                cube_ad = DBAPIAdapter(
                    conn_fn,
                    kind="cube",
                    account="default",
                    storage_class=eff_sc,
                )
            setattr(cube_ad, "storage_class", eff_sc)
            setattr(cube_ad, "account", "default")
            adapters[eng_name] = cube_ad

        elif isinstance(conn_cfg, DuckDBConnectionConfig):
            if connect_factories and eng_name in connect_factories:
                conn_fn = connect_factories[eng_name]
                duck_ad = DuckDBAdapter(conn_fn)
            else:
                duck_ad = DuckDBAdapter(config=conn_cfg, env=env)
            duck_ad.storage_class = eff_sc
            duck_ad.account = "default"
            adapters[eng_name] = duck_ad

    # 7. Register built adapters on MetricsRegistry, build shared ReadinessProbe, public extra routes, and Lifespan
    metrics.register_adapters(adapters)
    effective_audit_on_failure = (
        cfg.audit.on_failure if cfg.audit is not None else "refuse"
    )
    readiness = ReadinessProbe(
        adapters,
        is_static=cfg.placement.is_static,
        refresher=refresher,
        audit_sink=audit_sink,
        audit_on_failure=effective_audit_on_failure,
        timeout_seconds=(
            cfg.ops.readyz_timeout_seconds if cfg.ops is not None else 2.0
        ),
        cache_seconds=(
            cfg.ops.readyz_cache_seconds if cfg.ops is not None else 5.0
        ),
        clock=readiness_clock if readiness_clock is not None else time.monotonic,
    )

    @asynccontextmanager
    async def lifespan(app: Starlette):
        if refresher is not None:
            if refresher.last_success_at is None:
                ok = refresher.refresh_once()
                if not ok:
                    reason = (
                        last_refresh_error_scrubbed[0]
                        or refresher.last_error
                        or "unknown_error"
                    )
                    logger.error(
                        "Initial catalog discovery refresh failed on lifespan startup: %s",
                        reason,
                    )
            refresher.start()
        try:
            yield
        finally:
            if refresher is not None:
                refresher.stop()
            for adapter in adapters.values():
                if hasattr(adapter, "close"):
                    try:
                        adapter.close()
                    except Exception:
                        pass
            for hc in cold_start_http_clients:
                try:
                    hc.close()
                except Exception:
                    pass
            DuckDBHolder.get_holder().close_all()
            if audit_sink is not None and hasattr(audit_sink, "close"):
                try:
                    audit_sink.close()
                except Exception:
                    pass

    extra_routes = build_public_extra_routes(
        cfg.ops,
        readiness=readiness,
        metrics=metrics,
    )

    app = create_app(
        pipeline=pipeline,
        adapters=adapters,
        timeout_seconds=cfg.limits.timeout_seconds,
        max_body_bytes=cfg.limits.max_body_bytes,
        max_result_bytes=cfg.limits.max_result_bytes,
        extra_routes=extra_routes,
        lifespan=lifespan,
        replay_cache=GrantReplayCache(max_entries=cfg.replay_cache_max_entries),
        audit_sink=audit_sink,
        audit_on_failure=effective_audit_on_failure,
        audit_timeout_seconds=(
            cfg.audit.timeout_seconds if cfg.audit is not None else None
        ),
        audit_failure_callback=audit_failure_callback,
        metrics=metrics,
        cursor_key=resolved_cursor_key,
    )

    ops_app: Starlette | None = None
    if cfg.ops is not None and cfg.ops.enabled:
        ops_env = dict(env)
        if cfg.secrets_env:
            ops_env.update(cfg.secrets_env)
        if cfg.ops.auth_token_env:
            ops_env[cfg.ops.auth_token_env] = cfg.get_env(
                cfg.ops.auth_token_env, env
            )
        ops_app = create_ops_app(
            ops_config=cfg.ops,
            readiness=readiness,
            metrics=metrics,
            adapters=adapters,
            is_static=cfg.placement.is_static,
            refresher=refresher,
            audit_sink=audit_sink,
            audit_on_failure=effective_audit_on_failure,
            env=ops_env,
        )

    return ServerHandle(
        app=app,
        pipeline=pipeline,
        refresher=refresher,
        adapters=adapters,
        audit_sink=audit_sink,
        ops_app=ops_app,
        metrics=metrics,
    )


def _run_discovery(
    disc_data: dict,
    *,
    client: httpx.Client | None = None,
    lb_connect: Callable[[], Any] | None = None,
    postgres_connects: Mapping[str, Callable[[], Any]] | None = None,
    connections: Mapping[str, Any] | None = None,
    env: Mapping[str, str] = os.environ,
) -> DiscoveryResult:
    if client is not None:
        return discover(
            disc_data,
            http_client=client,
            lakebase_connect=lb_connect,
            postgres_connects=postgres_connects,
            connections=connections,
            env=env,
        )
    with httpx.Client() as c:
        return discover(
            disc_data,
            http_client=c,
            lakebase_connect=lb_connect,
            postgres_connects=postgres_connects,
            connections=connections,
            env=env,
        )


_ORIG_RUN_DISCOVERY = _run_discovery


def _resolve_custom_factory(
    connect_factories: Mapping[str, Any] | None,
    eng_name: str,
    acc_name: str | None = None,
) -> Callable[..., Any] | None:
    if not connect_factories:
        return None
    if eng_name in connect_factories:
        cf = connect_factories[eng_name]
        if isinstance(cf, Mapping):
            if acc_name is not None and acc_name in cf and callable(cf[acc_name]):
                return cf[acc_name]
            if acc_name is None and "interactive" in cf and callable(cf["interactive"]):
                return cf["interactive"]
        elif callable(cf):
            return cf
    if acc_name is not None and f"{eng_name}.{acc_name}" in connect_factories:
        cf_dot = connect_factories[f"{eng_name}.{acc_name}"]
        if callable(cf_dot):
            return cf_dot
    return None


def _resolve_dsn(
    *,
    dsn_env: str | None,
    dsn_file: str | None,
    cfg: ServerConfig,
    env: Mapping[str, str],
) -> str:
    if dsn_env:
        return cfg.get_env(dsn_env, env)
    if dsn_file:
        return read_secret_file(dsn_file)
    raise ServerConfigError("PostgreSQL-wire connection has neither dsn_env nor dsn_file")


def _invoke_custom_postgres_factory(
    custom_factory: Callable[..., Any],
    *,
    dsn_env: str | None,
    dsn_file: str | None,
    cfg: ServerConfig,
    env: Mapping[str, str],
    ckw: dict[str, Any],
) -> Any:
    try:
        sig = inspect.signature(custom_factory)
        params = list(sig.parameters.values())
    except (ValueError, TypeError):
        try:
            return custom_factory(**ckw)
        except TypeError:
            return custom_factory()

    if not params:
        return custom_factory()

    has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
    has_varpos = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params)
    pos_params = [
        p
        for p in params
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]

    # When a test spy defines ``def fake_connect(dsn, **kwargs)`` (explicit positional DSN
    # without *args, unlike MagicMock(*args, **kwargs)), supply the resolved DSN positionally.
    if pos_params and not has_varpos and pos_params[0].name in ("dsn", "conninfo"):
        dsn_val = "postgresql://placeholder"
        if dsn_env or dsn_file:
            try:
                dsn_val = _resolve_dsn(
                    dsn_env=dsn_env, dsn_file=dsn_file, cfg=cfg, env=env
                )
            except Exception:
                pass
        if has_varkw:
            return custom_factory(dsn_val, **ckw)
        accepted = {
            k: v for k, v in ckw.items() if k in sig.parameters and k != pos_params[0].name
        }
        return custom_factory(dsn_val, **accepted)

    if has_varkw:
        try:
            return custom_factory(**ckw)
        except TypeError:
            return custom_factory()

    accepted_kw = {k: v for k, v in ckw.items() if k in sig.parameters}
    if accepted_kw:
        return custom_factory(**accepted_kw)
    return custom_factory()


def _make_postgres_wire_connect(
    *,
    eng_name: str,
    conn_cfg: PostgresConnectionConfig | LakebaseConnectionConfig,
    dsn_env: str | None,
    dsn_file: str | None,
    cfg: ServerConfig,
    env: Mapping[str, str],
    custom_factory: Callable[..., Any] | None = None,
    metrics: MetricsRegistry | None = None,
    get_http_client: Callable[[], httpx.Client] | None = None,
    connectors_by_endpoint: dict[str, Any] | None = None,
    account_name: str | None = None,
) -> Callable[..., Any]:
    kind = conn_cfg.kind
    read_only = conn_cfg.read_only
    is_legacy = isinstance(conn_cfg, LakebaseConnectionConfig) and conn_cfg.is_legacy
    default_timeout = conn_cfg.connect_timeout_seconds

    def base_connect(**kwargs: Any) -> Any:
        to = kwargs.get("connect_timeout", default_timeout)
        if custom_factory is not None:
            ckw = build_connect_kwargs(
                "postgresql://placeholder",
                connect_timeout=to,
                session=conn_cfg.session,
                tls=conn_cfg.tls,
                read_only=read_only,
                kind=kind,
            )
            conn = _invoke_custom_postgres_factory(
                custom_factory,
                dsn_env=dsn_env,
                dsn_file=dsn_file,
                cfg=cfg,
                env=env,
                ckw=ckw,
            )
        else:
            dsn = _resolve_dsn(
                dsn_env=dsn_env, dsn_file=dsn_file, cfg=cfg, env=env
            )
            ckw = build_connect_kwargs(
                dsn,
                connect_timeout=to,
                session=conn_cfg.session,
                tls=conn_cfg.tls,
                read_only=read_only,
                kind=kind,
            )
            import psycopg2

            conn = psycopg2.connect(dsn, **ckw)

        if conn is None:
            return None

        if conn_cfg.role_guard or kind == "lakebase" or conn_cfg.session is not None:
            verify_and_harden_session(
                conn,
                kind=kind,
                read_only=read_only,
                session=conn_cfg.session,
                role_guard=conn_cfg.role_guard,
                is_legacy_lakebase=is_legacy,
            )
        return conn

    lb_block = getattr(conn_cfg, "lakebase", None)
    cs_cfg = getattr(lb_block, "cold_start", None) if lb_block is not None else None
    if isinstance(conn_cfg, LakebaseConnectionConfig) and lb_block is not None and cs_cfg is not None:
        effective_env = dict(env)
        if cfg.secrets_env:
            effective_env.update(cfg.secrets_env)

        http_client = (
            get_http_client()
            if get_http_client is not None and getattr(cs_cfg, "wake", "none") == "api"
            else None
        )
        cs_init_kw: dict[str, Any] = {}
        try:
            cs_sig = inspect.signature(ColdStartConnector)
            cs_params = cs_sig.parameters
            cs_varkw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in cs_params.values()
            )
            if "metrics" in cs_params or cs_varkw:
                cs_init_kw["metrics"] = metrics
            if "http_client" in cs_params or cs_varkw:
                cs_init_kw["http_client"] = http_client
            if "engine" in cs_params or cs_varkw:
                cs_init_kw["engine"] = eng_name
            if "engine_name" in cs_params:
                cs_init_kw["engine_name"] = eng_name
            if "env" in cs_params or cs_varkw:
                cs_init_kw["env"] = effective_env
        except (ValueError, TypeError):
            cs_init_kw = {"metrics": metrics, "http_client": http_client}

        ep_key = str(getattr(lb_block, "endpoint_id", "") or eng_name)
        if (
            connectors_by_endpoint is not None
            and ep_key in connectors_by_endpoint
        ):
            connector = connectors_by_endpoint[ep_key]
            reg_fn = getattr(connector, "register_connect_fn", None)
            if callable(reg_fn):
                reg_fn(base_connect, preferred=(account_name == "interactive"))
        else:
            connector = ColdStartConnector(base_connect, lb_block, **cs_init_kw)
            if connectors_by_endpoint is not None:
                connectors_by_endpoint[ep_key] = connector

        def cold_start_connect(
            *,
            connect_timeout: float | None = None,
            deadline: float | None = None,
        ) -> Any:
            if deadline is not None:
                eff_deadline = deadline
            elif connect_timeout is not None:
                eff_deadline = time.monotonic() + float(connect_timeout)
            else:
                wait_s = (
                    getattr(cs_cfg, "max_query_wait_seconds", None)
                    or default_timeout
                )
                eff_deadline = time.monotonic() + float(wait_s)

            call_kw: dict[str, Any] = {}
            try:
                c_sig = inspect.signature(connector.connect)
                c_params = c_sig.parameters
                c_varkw = any(
                    p.kind == inspect.Parameter.VAR_KEYWORD
                    for p in c_params.values()
                )
                if "connect_fn" in c_params or c_varkw:
                    call_kw["connect_fn"] = base_connect
                if ("connect_timeout" in c_params or c_varkw) and connect_timeout is not None:
                    call_kw["connect_timeout"] = connect_timeout
            except (ValueError, TypeError):
                call_kw["connect_fn"] = base_connect
            return connector.connect(eff_deadline, **call_kw)

        setattr(cold_start_connect, "cold_start_connector", connector)
        setattr(cold_start_connect, "base_connect", base_connect)
        return cold_start_connect

    return base_connect


def _build_postgres_wire_adapter(
    *,
    eng_name: str,
    conn_cfg: PostgresConnectionConfig | LakebaseConnectionConfig,
    cfg: ServerConfig,
    env: Mapping[str, str],
    connect_factories: Mapping[str, Any] | None,
    metrics: MetricsRegistry | None,
    get_http_client: Callable[[], httpx.Client] | None,
    storage_class: str | None = None,
    connectors_by_endpoint: dict[str, Any] | None = None,
) -> EngineAdapter:
    lb_block = getattr(conn_cfg, "lakebase", None)
    eff_sc = (
        storage_class
        if storage_class is not None
        else effective_storage_class(conn_cfg)
    )
    lb_ep_meta: dict[str, str] | None = None
    if (
        isinstance(conn_cfg, LakebaseConnectionConfig)
        and lb_block is not None
        and hasattr(lb_block, "project_id")
        and hasattr(lb_block, "branch_id")
        and hasattr(lb_block, "endpoint_id")
        and hasattr(lb_block, "endpoint_type")
    ):
        lb_ep_meta = {
            "project_id": str(lb_block.project_id),
            "branch_id": str(lb_block.branch_id),
            "endpoint_id": str(lb_block.endpoint_id),
            "endpoint_type": str(lb_block.endpoint_type),
        }

    if conn_cfg.accounts:
        pools_by_cred: dict[tuple[str, Any], tuple[PostgresPool, Callable[..., Any]]] = {}
        connects_by_workload: dict[str, Callable[..., Any]] = {}
        pools_by_workload: dict[str, PostgresPool] = {}

        for acc_name, acc in conn_cfg.accounts.items():
            custom_fn = _resolve_custom_factory(
                connect_factories, eng_name, acc_name
            )
            if custom_fn is not None:
                cred_key: tuple[str, Any] = ("factory", id(custom_fn))
            elif acc.dsn_env:
                cred_key = ("dsn_env", acc.dsn_env)
            else:
                assert acc.dsn_file is not None
                cred_key = ("dsn_file", os.path.abspath(acc.dsn_file))

            if cred_key not in pools_by_cred:
                conn_fn = _make_postgres_wire_connect(
                    eng_name=eng_name,
                    conn_cfg=conn_cfg,
                    dsn_env=acc.dsn_env,
                    dsn_file=acc.dsn_file,
                    cfg=cfg,
                    env=env,
                    custom_factory=custom_fn,
                    metrics=metrics,
                    get_http_client=get_http_client,
                    connectors_by_endpoint=connectors_by_endpoint,
                    account_name=acc_name,
                )
                pool = PostgresPool(
                    conn_fn,
                    pool_max=conn_cfg.pool_max,
                    pool_min=conn_cfg.pool_min,
                    connect_timeout_seconds=conn_cfg.connect_timeout_seconds,
                    max_idle_seconds=conn_cfg.max_idle_seconds,
                    max_lifetime_seconds=conn_cfg.max_lifetime_seconds,
                    acquire_timeout_seconds=conn_cfg.acquire_timeout_seconds,
                )
                pools_by_cred[cred_key] = (pool, conn_fn)
            else:
                pool, conn_fn = pools_by_cred[cred_key]

            connects_by_workload[acc_name] = conn_fn
            pools_by_workload[acc_name] = pool

        wl_adapter = build_workload_adapters(
            conn_cfg.kind,
            connects_by_workload,
            pools=pools_by_workload,
            read_only=conn_cfg.read_only,
            session_config=conn_cfg.session,
            lakebase_config=lb_block,
            storage_class=eff_sc,
            lakebase_endpoint=lb_ep_meta,
        )
        wl_adapter.storage_class = eff_sc
        wl_adapter.lakebase_endpoint = lb_ep_meta
        return wl_adapter

    custom_fn = _resolve_custom_factory(connect_factories, eng_name, None)
    conn_fn = _make_postgres_wire_connect(
        eng_name=eng_name,
        conn_cfg=conn_cfg,
        dsn_env=conn_cfg.dsn_env or None,
        dsn_file=conn_cfg.dsn_file,
        cfg=cfg,
        env=env,
        custom_factory=custom_fn,
        metrics=metrics,
        get_http_client=get_http_client,
        connectors_by_endpoint=connectors_by_endpoint,
    )
    pool = PostgresPool(
        conn_fn,
        pool_max=conn_cfg.pool_max,
        pool_min=conn_cfg.pool_min,
        connect_timeout_seconds=conn_cfg.connect_timeout_seconds,
        max_idle_seconds=conn_cfg.max_idle_seconds,
        max_lifetime_seconds=conn_cfg.max_lifetime_seconds,
        acquire_timeout_seconds=conn_cfg.acquire_timeout_seconds,
    )
    return build_adapter(
        conn_cfg.kind,
        conn_fn,
        pool=pool,
        read_only=conn_cfg.read_only,
        session_config=conn_cfg.session,
        account="default",
        lakebase_config=lb_block,
        storage_class=eff_sc,
        lakebase_endpoint=lb_ep_meta,
    )


def _make_discovery_source_connect(
    *,
    eng_name: str,
    conn_cfg: PostgresConnectionConfig | LakebaseConnectionConfig,
    account_name: str,
    cfg: ServerConfig,
    env: Mapping[str, str],
    connect_factories: Mapping[str, Any] | None,
    metrics: MetricsRegistry | None,
    get_http_client: Callable[[], httpx.Client] | None,
    connectors_by_endpoint: dict[str, Any] | None = None,
) -> Callable[[], Any]:
    if conn_cfg.accounts:
        if account_name not in conn_cfg.accounts:
            def _missing_account() -> Any:
                raise EngineConfigError(
                    f"no engine account configured for workload '{account_name}'"
                )

            return _missing_account

        acc = conn_cfg.accounts[account_name]
        custom_fn = _resolve_custom_factory(
            connect_factories, eng_name, account_name
        )
        conn_fn = _make_postgres_wire_connect(
            eng_name=eng_name,
            conn_cfg=conn_cfg,
            dsn_env=acc.dsn_env,
            dsn_file=acc.dsn_file,
            cfg=cfg,
            env=env,
            custom_factory=custom_fn,
            metrics=metrics,
            get_http_client=get_http_client,
            connectors_by_endpoint=connectors_by_endpoint,
            account_name=account_name,
        )
        return lambda: conn_fn()

    custom_fn = _resolve_custom_factory(connect_factories, eng_name, account_name)
    conn_fn = _make_postgres_wire_connect(
        eng_name=eng_name,
        conn_cfg=conn_cfg,
        dsn_env=conn_cfg.dsn_env or None,
        dsn_file=conn_cfg.dsn_file,
        cfg=cfg,
        env=env,
        custom_factory=custom_fn,
        metrics=metrics,
        get_http_client=get_http_client,
        connectors_by_endpoint=connectors_by_endpoint,
    )
    return lambda: conn_fn()


def _build_discovery_postgres_connects(
    *,
    cfg: ServerConfig,
    disc_cfg: Any,
    env: Mapping[str, str],
    connect_factories: Mapping[str, Any] | None,
    metrics: MetricsRegistry | None,
    get_http_client: Callable[[], httpx.Client] | None,
    connectors_by_endpoint: dict[str, Any] | None = None,
) -> dict[str, Callable[[], Any]]:
    introspect_accounts: dict[str, str] = {}
    parsed_pgs = getattr(disc_cfg, "postgres_sources", None)
    if isinstance(parsed_pgs, Mapping):
        for src_name, src_cfg in parsed_pgs.items():
            acc = getattr(src_cfg, "account", None)
            introspect_accounts[str(src_name)] = (
                acc.strip() if isinstance(acc, str) and acc.strip() else "interactive"
            )
    elif isinstance(cfg.placement.discovery, Mapping):
        raw_pgs = cfg.placement.discovery.get("postgres_sources")
        if isinstance(raw_pgs, Mapping):
            for src_name, src_spec in raw_pgs.items():
                acc_val = (
                    src_spec.get("account")
                    if isinstance(src_spec, Mapping)
                    else None
                )
                introspect_accounts[str(src_name)] = (
                    acc_val.strip()
                    if isinstance(acc_val, str) and acc_val.strip()
                    else "interactive"
                )

    postgres_connects: dict[str, Callable[[], Any]] = {}
    for eng_name, conn_cfg in cfg.connections.items():
        if isinstance(
            conn_cfg, (PostgresConnectionConfig, LakebaseConnectionConfig)
        ):
            acc_name = introspect_accounts.get(eng_name, "interactive")
            postgres_connects[eng_name] = _make_discovery_source_connect(
                eng_name=eng_name,
                conn_cfg=conn_cfg,
                account_name=acc_name,
                cfg=cfg,
                env=env,
                connect_factories=connect_factories,
                metrics=metrics,
                get_http_client=get_http_client,
                connectors_by_endpoint=connectors_by_endpoint,
            )

    for src_name, acc_name in introspect_accounts.items():
        if src_name not in postgres_connects:
            custom_fn = _resolve_custom_factory(
                connect_factories, src_name, acc_name
            )
            if custom_fn is not None:
                postgres_connects[src_name] = lambda fn=custom_fn: fn()

    return postgres_connects


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
    dsn_env: str,
    cfg: ServerConfig,
    env: Mapping[str, str],
    connect_timeout: float = 10.0,
) -> Callable[..., Any]:
    def connect(**kwargs: Any) -> Any:
        dsn = cfg.get_env(dsn_env, env)
        import psycopg2

        to = kwargs.get("connect_timeout", connect_timeout)
        to_int = max(1, math.ceil(float(to)))
        return psycopg2.connect(dsn, connect_timeout=to_int)

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
        to_int = max(1, math.ceil(float(to)))
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=u,
            password=p,
            connect_timeout=to_int,
        )

    return connect

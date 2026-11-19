"""Operations configuration, engine readiness probes, and dedicated ops ASGI app for Seeknal Serving."""

from __future__ import annotations

import hmac
import math
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import duckdb
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from seeknal.serving.adapters import (
    CubeAdapter,
    DBAPIAdapter,
    DuckDBAdapter,
    WorkloadRoutedAdapter,
    make_cube_context_token,
)
from seeknal.serving.metrics import PROMETHEUS_CONTENT_TYPE, MetricsRegistry
from seeknal.serving.service import PreparedQuery

_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_SAFE_KEY_DIAG_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_SAFE_ENGINE_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")

_LITERAL_TOKEN_KEYS = frozenset(
    {
        "auth_token",
        "token",
        "bearer_token",
        "secret",
        "password",
        "api_key",
        "apikey",
    }
)

_ALLOWED_OPS_KEYS = frozenset(
    {
        "enabled",
        "host",
        "port",
        "auth_token_env",
        "readyz_timeout_seconds",
        "readyz_cache_seconds",
        "public_metrics",
        "public_buildinfo",
        "public_readyz",
    }
)


def _validate_ops_fields(
    *,
    enabled: Any,
    host: Any,
    port: Any,
    auth_token_env: Any,
    readyz_timeout_seconds: Any,
    readyz_cache_seconds: Any,
    public_metrics: Any,
    public_buildinfo: Any,
    public_readyz: Any,
    error_cls: type[Exception] = ValueError,
) -> tuple[bool, str, int, str | None, float, float, bool, bool, bool]:
    if not isinstance(enabled, bool):
        raise error_cls("ops.enabled must be a boolean")

    if not isinstance(host, str) or not host.strip():
        raise error_cls("ops.host must be a non-empty string")
    clean_host = host.strip()

    if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
        raise error_cls("ops.port must be an integer between 1 and 65535")

    clean_token_env: str | None
    if auth_token_env is None:
        clean_token_env = None
    elif isinstance(auth_token_env, str) and _ENV_VAR_NAME_RE.match(auth_token_env):
        clean_token_env = auth_token_env
    else:
        raise error_cls(
            "ops.auth_token_env must be None or a valid environment variable name "
            "matching ^[A-Za-z_][A-Za-z0-9_]{0,127}$"
        )

    if (
        isinstance(readyz_timeout_seconds, bool)
        or not isinstance(readyz_timeout_seconds, (int, float))
        or math.isnan(readyz_timeout_seconds)
        or math.isinf(readyz_timeout_seconds)
        or readyz_timeout_seconds <= 0
    ):
        raise error_cls("ops.readyz_timeout_seconds must be a positive finite number")
    clean_timeout = float(readyz_timeout_seconds)

    if (
        isinstance(readyz_cache_seconds, bool)
        or not isinstance(readyz_cache_seconds, (int, float))
        or math.isnan(readyz_cache_seconds)
        or math.isinf(readyz_cache_seconds)
        or readyz_cache_seconds < 0
    ):
        raise error_cls(
            "ops.readyz_cache_seconds must be a non-negative finite number"
        )
    clean_cache = float(readyz_cache_seconds)

    if not isinstance(public_metrics, bool):
        raise error_cls("ops.public_metrics must be a boolean")
    if not isinstance(public_buildinfo, bool):
        raise error_cls("ops.public_buildinfo must be a boolean")
    if not isinstance(public_readyz, bool):
        raise error_cls("ops.public_readyz must be a boolean")

    return (
        enabled,
        clean_host,
        port,
        clean_token_env,
        clean_timeout,
        clean_cache,
        public_metrics,
        public_buildinfo,
        public_readyz,
    )


@dataclass(frozen=True)
class OpsConfig:
    """Configuration for the Seeknal Serving operations listener and endpoints."""

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 9090
    auth_token_env: str | None = None
    readyz_timeout_seconds: float = 2.0
    readyz_cache_seconds: float = 5.0
    public_metrics: bool = False
    public_buildinfo: bool = False
    public_readyz: bool = True

    def __post_init__(self) -> None:
        (
            _,
            clean_host,
            _,
            _,
            clean_timeout,
            clean_cache,
            _,
            _,
            _,
        ) = _validate_ops_fields(
            enabled=self.enabled,
            host=self.host,
            port=self.port,
            auth_token_env=self.auth_token_env,
            readyz_timeout_seconds=self.readyz_timeout_seconds,
            readyz_cache_seconds=self.readyz_cache_seconds,
            public_metrics=self.public_metrics,
            public_buildinfo=self.public_buildinfo,
            public_readyz=self.public_readyz,
            error_cls=ValueError,
        )
        object.__setattr__(self, "host", clean_host)
        object.__setattr__(self, "readyz_timeout_seconds", clean_timeout)
        object.__setattr__(self, "readyz_cache_seconds", clean_cache)

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        error_cls: type[Exception] = ValueError,
    ) -> OpsConfig:
        """Parse and validate an 'ops' mapping (enabled defaults to True when section is present)."""
        if not isinstance(d, Mapping):
            raise error_cls(
                f"ops configuration must be a mapping, got {type(d).__name__}"
            )

        for key in d.keys():
            if isinstance(key, str) and key.lower() in _LITERAL_TOKEN_KEYS:
                raise error_cls(
                    f"Literal secret key 'ops.{key}' is not permitted; "
                    "use 'ops.auth_token_env' with an environment variable name instead"
                )

        unknown = set(d.keys()) - _ALLOWED_OPS_KEYS
        if unknown:
            safe_names = [
                k if isinstance(k, str) and _SAFE_KEY_DIAG_RE.match(k) else "<unknown>"
                for k in sorted(unknown, key=str)
            ]
            raise error_cls(
                f"Unknown key(s) in ops configuration: {', '.join(safe_names)}"
            )

        (
            enabled,
            host,
            port,
            auth_token_env,
            readyz_timeout_seconds,
            readyz_cache_seconds,
            public_metrics,
            public_buildinfo,
            public_readyz,
        ) = _validate_ops_fields(
            enabled=d.get("enabled", True),
            host=d.get("host", "127.0.0.1"),
            port=d.get("port", 9090),
            auth_token_env=d.get("auth_token_env", None),
            readyz_timeout_seconds=d.get("readyz_timeout_seconds", 2.0),
            readyz_cache_seconds=d.get("readyz_cache_seconds", 5.0),
            public_metrics=d.get("public_metrics", False),
            public_buildinfo=d.get("public_buildinfo", False),
            public_readyz=d.get("public_readyz", True),
            error_cls=error_cls,
        )

        return cls(
            enabled=enabled,
            host=host,
            port=port,
            auth_token_env=auth_token_env,
            readyz_timeout_seconds=readyz_timeout_seconds,
            readyz_cache_seconds=readyz_cache_seconds,
            public_metrics=public_metrics,
            public_buildinfo=public_buildinfo,
            public_readyz=public_readyz,
        )


def resolve_build_info() -> dict[str, str]:
    """Resolve build metadata from seeknal.serving.buildinfo returning only the 5 canonical keys."""
    from seeknal.serving.buildinfo import BUILD_INFO_KEYS, get_build_info

    info = get_build_info()
    return {key: str(info.get(key, "unknown")) for key in BUILD_INFO_KEYS}


class ReadinessProbe:
    """Bounded, cached per-engine readiness checker that never leaks internal errors or DSNs."""

    def __init__(
        self,
        adapters: Mapping[str, Any] | None = None,
        *,
        is_static: bool | None = None,
        refresher: Any = None,
        catalog_loaded_fn: Callable[[], bool] | None = None,
        timeout_seconds: float = 2.0,
        cache_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        custom_probes: Mapping[str, Callable[[], bool]] | None = None,
    ) -> None:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a positive finite number")
        if (
            isinstance(cache_seconds, bool)
            or not isinstance(cache_seconds, (int, float))
            or not math.isfinite(cache_seconds)
            or cache_seconds < 0
        ):
            raise ValueError("cache_seconds must be a non-negative finite number")

        self._adapters: dict[str, Any] = {}
        if adapters:
            for name, adapter in adapters.items():
                if isinstance(name, str) and _SAFE_ENGINE_KEY_RE.match(name):
                    self._adapters[name] = adapter

        self._is_static = is_static
        self._refresher = refresher
        self._catalog_loaded_fn = catalog_loaded_fn
        self._timeout_seconds = float(timeout_seconds)
        self._cache_seconds = float(cache_seconds)
        self._clock = clock
        self._custom_probes: dict[str, Callable[[], bool]] = (
            dict(custom_probes) if custom_probes else {}
        )

        self._cache_lock = threading.Lock()
        self._cache: dict[str, tuple[bool, float]] = {}
        self._engine_locks: dict[str, threading.Lock] = {
            name: threading.Lock() for name in self._adapters
        }

    def clear_cache(self) -> None:
        """Invalidate all cached per-engine readiness results."""
        with self._cache_lock:
            self._cache.clear()

    def _is_catalog_loaded(self) -> bool:
        if self._catalog_loaded_fn is not None:
            try:
                return bool(self._catalog_loaded_fn())
            except Exception:
                return False
        if self._is_static is True:
            return True
        if self._is_static is False:
            return (
                self._refresher is not None
                and getattr(self._refresher, "last_success_at", None) is not None
            )
        if self._refresher is not None:
            return getattr(self._refresher, "last_success_at", None) is not None
        return True

    def _probe_adapter_impl(self, eng_name: str, adapter: Any) -> bool:
        if eng_name in self._custom_probes:
            return bool(self._custom_probes[eng_name]())

        if isinstance(adapter, WorkloadRoutedAdapter):
            if not adapter.by_workload:
                return False
            sub = adapter.by_workload.get("interactive")
            if sub is None:
                sub = next(iter(adapter.by_workload.values()))
            return self._probe_adapter_impl(eng_name, sub)

        if isinstance(adapter, DuckDBAdapter):
            return adapter.check_health(timeout_seconds=self._timeout_seconds)

        if isinstance(adapter, DBAPIAdapter):
            before = getattr(adapter, "_before_connect", None)
            if callable(before):
                before()

            pool = adapter.pool
            pooled = pool is not None
            conn = pool.acquire() if pooled else adapter._connect()
            if conn is None:
                return False

            ok = False
            cur = None
            try:
                if hasattr(conn, "cursor") and callable(conn.cursor):
                    cur = conn.cursor()
                    if (
                        cur is not None
                        and hasattr(cur, "execute")
                        and callable(cur.execute)
                    ):
                        cur.execute("SELECT 1")
                ok = True
                if pooled and hasattr(conn, "rollback") and callable(conn.rollback):
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                return True
            finally:
                if cur is not None and hasattr(cur, "close") and callable(cur.close):
                    try:
                        cur.close()
                    except Exception:
                        pass
                if pooled:
                    try:
                        pool.release(conn, success=ok)
                    except Exception:
                        pass
                elif hasattr(conn, "close") and callable(conn.close):
                    try:
                        conn.close()
                    except Exception:
                        pass

        if isinstance(adapter, CubeAdapter):
            probe_prepared = PreparedQuery(
                sql="SELECT 1",
                params=(),
                paramstyle="format",
                engine=eng_name,
                engine_kind="cube",
                rule="probe",
                reason="readiness_probe",
                masked_columns=(),
                row_limit=1,
                audit_id="readyz-probe",
                subject="system:readyz",
                workspace="system",
                redacted_sql="SELECT 1",
                cube_security_context={"masks": {}, "row_filters": {}},
            )
            token = make_cube_context_token(probe_prepared, adapter._security)
            timeout_int = max(
                1,
                math.ceil(
                    min(
                        float(self._timeout_seconds),
                        float(getattr(adapter, "_connect_timeout_seconds", 10)),
                    )
                ),
            )
            try:
                conn = adapter._connect(
                    user=adapter._security.sql_user,
                    password=token,
                    connect_timeout=timeout_int,
                )
            except TypeError:
                conn = adapter._connect()

            if conn is None:
                return False

            cur = None
            try:
                if hasattr(conn, "cursor") and callable(conn.cursor):
                    cur = conn.cursor()
                    if (
                        cur is not None
                        and hasattr(cur, "execute")
                        and callable(cur.execute)
                    ):
                        cur.execute("SELECT 1")
                return True
            finally:
                if cur is not None and hasattr(cur, "close") and callable(cur.close):
                    try:
                        cur.close()
                    except Exception:
                        pass
                if hasattr(conn, "close") and callable(conn.close):
                    try:
                        conn.close()
                    except Exception:
                        pass

        if callable(getattr(adapter, "check_health", None)):
            return bool(adapter.check_health(timeout_seconds=self._timeout_seconds))

        if callable(getattr(adapter, "_connect", None)):
            conn = adapter._connect()
            if conn is None:
                return False
            cur = None
            try:
                if hasattr(conn, "cursor") and callable(conn.cursor):
                    cur = conn.cursor()
                    if (
                        cur is not None
                        and hasattr(cur, "execute")
                        and callable(cur.execute)
                    ):
                        cur.execute("SELECT 1")
                return True
            finally:
                if cur is not None and hasattr(cur, "close") and callable(cur.close):
                    try:
                        cur.close()
                    except Exception:
                        pass
                if hasattr(conn, "close") and callable(conn.close):
                    try:
                        conn.close()
                    except Exception:
                        pass

        if callable(getattr(adapter, "execute", None)):
            return True

        return False

    def _run_bounded_probe(self, eng_name: str, adapter: Any) -> bool:
        outcome: list[bool] = [False]

        def _worker() -> None:
            try:
                outcome[0] = bool(self._probe_adapter_impl(eng_name, adapter))
            except Exception:
                outcome[0] = False

        thread = threading.Thread(
            target=_worker,
            daemon=True,
            name=f"seeknal-readyz-{eng_name}",
        )
        thread.start()
        thread.join(timeout=self._timeout_seconds)
        if thread.is_alive():
            return False
        return outcome[0]

    def _get_engine_lock(self, eng_name: str) -> threading.Lock:
        with self._cache_lock:
            lock = self._engine_locks.get(eng_name)
            if lock is None:
                lock = threading.Lock()
                self._engine_locks[eng_name] = lock
            return lock

    def _check_engine_cached(self, eng_name: str, adapter: Any) -> bool:
        if self._cache_seconds > 0:
            now = float(self._clock())
            with self._cache_lock:
                cached = self._cache.get(eng_name)
                if cached is not None:
                    is_ok, checked_at = cached
                    if 0.0 <= (now - checked_at) < self._cache_seconds:
                        return is_ok

        eng_lock = self._get_engine_lock(eng_name)
        with eng_lock:
            if self._cache_seconds > 0:
                now = float(self._clock())
                with self._cache_lock:
                    cached = self._cache.get(eng_name)
                    if cached is not None:
                        is_ok, checked_at = cached
                        if 0.0 <= (now - checked_at) < self._cache_seconds:
                            return is_ok

            ok = self._run_bounded_probe(eng_name, adapter)
            if self._cache_seconds > 0:
                recorded_at = float(self._clock())
                with self._cache_lock:
                    self._cache[eng_name] = (ok, recorded_at)
            return ok

    def check(self) -> tuple[int, dict[str, Any]]:
        """Evaluate catalog and engine readiness, returning (http_status, safe_payload)."""
        catalog_loaded = self._is_catalog_loaded()
        engines_status: dict[str, str] = {}
        all_engines_ok = True

        for eng_name, adapter in self._adapters.items():
            ok = self._check_engine_cached(eng_name, adapter)
            engines_status[eng_name] = "ok" if ok else "unavailable"
            if not ok:
                all_engines_ok = False

        if not catalog_loaded:
            return (
                503,
                {
                    "ready": False,
                    "reason": "catalog_not_loaded",
                    "engines": engines_status,
                },
            )

        if all_engines_ok:
            return 200, {"ready": True, "engines": engines_status}

        return 503, {"ready": False, "engines": engines_status}

    async def handle_request(self, request: Request) -> JSONResponse:
        """Async Starlette endpoint handler for GET /readyz."""
        status_code, payload = await run_in_threadpool(self.check)
        return JSONResponse(payload, status_code=status_code)


def _verify_ops_bearer_token(
    request: Request,
    auth_token_env: str | None,
    env: Mapping[str, str] | None,
) -> JSONResponse | None:
    if not auth_token_env:
        return None

    source_env = env if env is not None else os.environ
    expected = source_env.get(auth_token_env, "")
    if not isinstance(expected, str) or not expected:
        return JSONResponse(
            {"error": "unauthorized", "message": "Unauthorized"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )

    auth_header = request.headers.get("authorization")
    if not auth_header:
        return JSONResponse(
            {"error": "unauthorized", "message": "Missing Authorization header"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )

    parts = auth_header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return JSONResponse(
            {"error": "unauthorized", "message": "Invalid Authorization header"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )

    candidate = parts[1].strip()
    if not candidate or any(ch.isspace() for ch in candidate):
        return JSONResponse(
            {"error": "unauthorized", "message": "Invalid Authorization header"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not hmac.compare_digest(
        candidate.encode("utf-8"),
        expected.encode("utf-8"),
    ):
        return JSONResponse(
            {"error": "unauthorized", "message": "Invalid bearer token"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )

    return None


def build_public_extra_routes(
    ops_config: OpsConfig | None = None,
    *,
    readiness: ReadinessProbe,
    metrics: MetricsRegistry | None = None,
    build_info_fn: Callable[[], Mapping[str, Any]] | None = None,
) -> list[Route]:
    """Build optional public-listener operational routes governed by OpsConfig flags."""
    cfg = ops_config if ops_config is not None else OpsConfig()
    info_fn = build_info_fn or resolve_build_info
    routes: list[Route] = []

    if cfg.public_readyz:
        async def public_readyz(request: Request) -> Response:
            start = time.perf_counter()
            status_code, payload = await run_in_threadpool(readiness.check)
            if metrics is not None:
                err_code = None
                if status_code != 200:
                    err_code = (
                        "catalog_not_loaded"
                        if payload.get("reason") == "catalog_not_loaded"
                        else "not_ready"
                    )
                metrics.record_request(
                    "/readyz",
                    status=status_code,
                    duration_seconds=time.perf_counter() - start,
                    error_code=err_code,
                )
            return JSONResponse(payload, status_code=status_code)

        routes.append(Route("/readyz", public_readyz, methods=["GET"]))

    if cfg.public_metrics and metrics is not None:
        async def public_metrics_endpoint(request: Request) -> Response:
            start = time.perf_counter()
            body = await run_in_threadpool(metrics.render)
            metrics.record_request(
                "/metrics",
                status=200,
                duration_seconds=time.perf_counter() - start,
            )
            return Response(content=body, media_type=PROMETHEUS_CONTENT_TYPE)

        routes.append(Route("/metrics", public_metrics_endpoint, methods=["GET"]))

    if cfg.public_buildinfo:
        async def public_buildinfo(request: Request) -> Response:
            start = time.perf_counter()
            payload = dict(info_fn())
            if metrics is not None:
                metrics.record_request(
                    "/buildinfo",
                    status=200,
                    duration_seconds=time.perf_counter() - start,
                )
            return JSONResponse(payload, status_code=200)

        routes.append(Route("/buildinfo", public_buildinfo, methods=["GET"]))

    return routes


def create_ops_app(
    *,
    ops_config: OpsConfig | None = None,
    readiness: ReadinessProbe | None = None,
    metrics: MetricsRegistry | None = None,
    adapters: Mapping[str, Any] | None = None,
    is_static: bool | None = None,
    refresher: Any = None,
    env: Mapping[str, str] | None = None,
    build_info_fn: Callable[[], Mapping[str, Any]] | None = None,
    lifespan: Any = None,
) -> Starlette:
    """Create the dedicated Starlette operations application (/healthz, /readyz, /metrics, /buildinfo)."""
    cfg = ops_config if ops_config is not None else OpsConfig(enabled=True)
    probe = (
        readiness
        if readiness is not None
        else ReadinessProbe(
            adapters,
            is_static=is_static,
            refresher=refresher,
            timeout_seconds=cfg.readyz_timeout_seconds,
            cache_seconds=cfg.readyz_cache_seconds,
        )
    )
    registry = (
        metrics if metrics is not None else MetricsRegistry(adapters=adapters)
    )
    if adapters is not None and metrics is not None:
        registry.register_adapters(adapters)

    info_fn = build_info_fn or resolve_build_info

    async def ops_healthz(request: Request) -> Response:
        start = time.perf_counter()
        auth_err = _verify_ops_bearer_token(request, cfg.auth_token_env, env)
        if auth_err is not None:
            registry.record_request(
                "/healthz",
                status=401,
                duration_seconds=time.perf_counter() - start,
                error_code="unauthorized",
            )
            return auth_err
        registry.record_request(
            "/healthz",
            status=200,
            duration_seconds=time.perf_counter() - start,
        )
        return JSONResponse({"status": "ok"})

    async def ops_readyz(request: Request) -> Response:
        start = time.perf_counter()
        auth_err = _verify_ops_bearer_token(request, cfg.auth_token_env, env)
        if auth_err is not None:
            registry.record_request(
                "/readyz",
                status=401,
                duration_seconds=time.perf_counter() - start,
                error_code="unauthorized",
            )
            return auth_err
        status_code, payload = await run_in_threadpool(probe.check)
        err_code = None
        if status_code != 200:
            err_code = (
                "catalog_not_loaded"
                if payload.get("reason") == "catalog_not_loaded"
                else "not_ready"
            )
        registry.record_request(
            "/readyz",
            status=status_code,
            duration_seconds=time.perf_counter() - start,
            error_code=err_code,
        )
        return JSONResponse(payload, status_code=status_code)

    async def ops_metrics(request: Request) -> Response:
        start = time.perf_counter()
        auth_err = _verify_ops_bearer_token(request, cfg.auth_token_env, env)
        if auth_err is not None:
            registry.record_request(
                "/metrics",
                status=401,
                duration_seconds=time.perf_counter() - start,
                error_code="unauthorized",
            )
            return auth_err
        registry.record_request(
            "/metrics",
            status=200,
            duration_seconds=time.perf_counter() - start,
        )
        body = await run_in_threadpool(registry.render)
        return Response(content=body, media_type=PROMETHEUS_CONTENT_TYPE)

    async def ops_buildinfo(request: Request) -> Response:
        start = time.perf_counter()
        auth_err = _verify_ops_bearer_token(request, cfg.auth_token_env, env)
        if auth_err is not None:
            registry.record_request(
                "/buildinfo",
                status=401,
                duration_seconds=time.perf_counter() - start,
                error_code="unauthorized",
            )
            return auth_err
        payload = dict(info_fn())
        registry.record_request(
            "/buildinfo",
            status=200,
            duration_seconds=time.perf_counter() - start,
        )
        return JSONResponse(payload, status_code=200)

    routes = [
        Route("/healthz", ops_healthz, methods=["GET"]),
        Route("/readyz", ops_readyz, methods=["GET"]),
        Route("/metrics", ops_metrics, methods=["GET"]),
        Route("/buildinfo", ops_buildinfo, methods=["GET"]),
    ]
    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.ops_config = cfg
    app.state.readiness = probe
    app.state.metrics = registry
    return app

"""Prometheus text exposition and bounded-cardinality metrics for Seeknal Serving."""

from __future__ import annotations

import math
import re
import threading
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from seeknal.serving.adapters import DBAPIAdapter, DuckDBAdapter, WorkloadRoutedAdapter
from seeknal.serving.errors import ERROR_CATALOGUE

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

DEFAULT_DURATION_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)

ALLOWED_ROUTES: tuple[str, ...] = (
    "/v1/query",
    "/v1/select",
    "/v1/explain",
    "/v1/cancel",
    "/healthz",
    "/readyz",
    "/metrics",
    "/buildinfo",
)
_ALLOWED_ROUTES_SET = frozenset(ALLOWED_ROUTES)

ALLOWED_ERROR_CODES: tuple[str, ...] = tuple(
    dict.fromkeys(
        (
            *ERROR_CATALOGUE.keys(),
            "bad_request",
            "unauthorized",
            "grant_token_error",
            "enforcement_error",
            "routing_error",
            "rate_limited",
            "too_many_requests",
            "client_disconnected",
            "engine_error",
            "resource_exhausted",
            "engine_busy",
            "pool_exhausted",
            "replay_cache_full",
            "workload_account_missing",
            "catalog_not_loaded",
            "not_ready",
            "audit_error",
        )
    )
)
_ALLOWED_ERROR_CODES_SET = frozenset(ALLOWED_ERROR_CODES)

ALLOWED_GRANT_REJECTION_REASONS: tuple[str, ...] = (
    "expired",
    "invalid_signature",
    "malformed",
    "replayed",
    "untrusted_kid",
    "invalid_claims",
    "lifetime_exceeded",
    "other",
)
_ALLOWED_GRANT_REASONS_SET = frozenset(ALLOWED_GRANT_REJECTION_REASONS)

ALLOWED_GUARD_REJECTION_CODES: tuple[str, ...] = (
    "filter_required",
    "sort_not_indexed",
    "offset_too_large",
    "other",
)
_ALLOWED_GUARD_CODES_SET = frozenset(ALLOWED_GUARD_REJECTION_CODES)

ALLOWED_POSTGRES_POOL_KINDS: tuple[str, ...] = (
    "postgres",
    "lakebase",
)
_ALLOWED_POSTGRES_POOL_KINDS_SET = frozenset(ALLOWED_POSTGRES_POOL_KINDS)

ALLOWED_POSTGRES_ACCOUNTS: tuple[str, ...] = (
    "interactive",
    "bi",
    "report",
    "ask",
    "job",
    "default",
)
_ALLOWED_POSTGRES_ACCOUNTS_SET = frozenset(ALLOWED_POSTGRES_ACCOUNTS)

ALLOWED_DISCOVERY_REFRESH_OUTCOMES: tuple[str, ...] = (
    "success",
    "error",
)
_ALLOWED_DISCOVERY_OUTCOMES_SET = frozenset(ALLOWED_DISCOVERY_REFRESH_OUTCOMES)

ALLOWED_LAKEBASE_WAKE_OUTCOMES: tuple[str, ...] = (
    "ok",
    "success",
    "error",
    "timeout",
)
_ALLOWED_LAKEBASE_WAKE_OUTCOMES_SET = frozenset(ALLOWED_LAKEBASE_WAKE_OUTCOMES)

ALLOWED_LAKEBASE_STATES: tuple[str, ...] = (
    "ok",
    "starting",
    "unavailable",
)
_ALLOWED_LAKEBASE_STATES_SET = frozenset(ALLOWED_LAKEBASE_STATES)

_SAFE_ENGINE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")
_SUSPICIOUS_ENGINE_NAME_RE = re.compile(
    r"(://|@|secret|password|passwd|token|bearer|select\b|from\b|where\b)",
    re.IGNORECASE,
)


def _is_safe_configured_engine(name: Any) -> bool:
    if not isinstance(name, str):
        return False
    if name == "none":
        return False
    if not _SAFE_ENGINE_NAME_RE.match(name):
        return False
    if _SUSPICIOUS_ENGINE_NAME_RE.search(name):
        return False
    return True


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def normalize_route(route: Any) -> str:
    """Normalize an HTTP route path against the strict route allowlist."""
    if isinstance(route, str) and route in _ALLOWED_ROUTES_SET:
        return route
    return "other"


def normalize_status(status: Any) -> str:
    """Normalize an HTTP status code to a 3-digit string '100'..'599' or 'unknown'."""
    if isinstance(status, bool) or status is None:
        return "unknown"
    if isinstance(status, int):
        if 100 <= status <= 599:
            return str(status)
        return "unknown"
    if isinstance(status, str) and len(status) == 3 and status.isdigit():
        code = int(status)
        if 100 <= code <= 599:
            return status
    return "unknown"


def normalize_error_code(code: Any) -> str:
    """Normalize an error category against the bounded typed error allowlist."""
    if isinstance(code, str) and code in _ALLOWED_ERROR_CODES_SET:
        return code
    return "internal_error"


def normalize_grant_reason(reason: Any) -> str:
    """Normalize a grant rejection reason against the bounded enum allowlist."""
    if isinstance(reason, str) and reason in _ALLOWED_GRANT_REASONS_SET:
        return reason
    return "other"


def normalize_guard_code(code: Any) -> str:
    """Normalize a guardrail rejection code against the bounded enum allowlist."""
    if isinstance(code, BaseException):
        code = getattr(code, "code", None)
    if isinstance(code, str) and code in _ALLOWED_GUARD_CODES_SET:
        return code
    return "other"


def _format_gauge_float(value: float) -> str:
    formatted = f"{value:.6f}".rstrip("0").rstrip(".")
    return formatted if formatted and formatted != "-0" else "0"


def classify_grant_rejection(exc_or_reason: BaseException | str | None) -> str:
    """Map a GrantTokenError / GrantReplayedError or reason code to a bounded reason enum."""
    if exc_or_reason is None:
        return "other"
    if isinstance(exc_or_reason, str):
        return normalize_grant_reason(exc_or_reason)

    exc_name = type(exc_or_reason).__name__
    if exc_name == "GrantReplayedError":
        return "replayed"
    if (
        exc_name == "GrantExpiredError"
        or getattr(exc_or_reason, "code", None) == "grant_expired"
    ):
        return "expired"

    msg = str(exc_or_reason)
    if "already been used" in msg:
        return "replayed"
    if "Token has expired" in msg:
        return "expired"
    if (
        "Invalid token signature" in msg
        or "Signature verification failed" in msg
        or "Signature must be exactly 64 bytes" in msg
        or "Invalid signature format" in msg
    ):
        return "invalid_signature"
    if (
        "Unknown key ID" in msg
        or "is not an EC public key" in msg
        or "Failed to load PEM public key" in msg
        or "Unsupported key type" in msg
        or "SECP256R1" in msg
    ):
        return "untrusted_kid"
    if "Token lifetime of" in msg or "exceeds maximum" in msg:
        return "lifetime_exceeded"
    if (
        "Token must be a string" in msg
        or "Expected 3 segments" in msg
        or "Invalid base64url" in msg
        or "Failed to decode base64url" in msg
        or "Invalid token header JSON" in msg
        or "Token header must be a JSON object" in msg
        or "Missing 'alg'" in msg
        or "Unsupported algorithm" in msg
        or "Unsupported token type" in msg
        or "Missing or invalid 'kid'" in msg
        or "Invalid token payload JSON" in msg
        or "Token claims must be a JSON object" in msg
        or "Missing Authorization header" in msg
        or "Malformed Authorization header" in msg
    ):
        return "malformed"
    if (
        "Invalid issuer" in msg
        or "Invalid audience" in msg
        or "Missing required 'iat'" in msg
        or "Missing required 'exp'" in msg
        or "Missing 'exp'" in msg
        or "claim must be" in msg
        or "in the future" in msg
        or "must not be before" in msg
        or "not yet valid" in msg
        or "Cancel token cannot be used" in msg
        or "Missing or invalid 'typ'" in msg
        or "Expected 'typ' claim" in msg
        or "Missing required 'jti'" in msg
        or "Missing required 'sub'" in msg
    ):
        return "invalid_claims"
    return "other"


class _HistogramSeries:
    """Single (route, engine) latency histogram series storing per-bucket non-cumulative counts."""

    __slots__ = ("bucket_counts", "count", "total")

    def __init__(self, num_buckets: int) -> None:
        self.bucket_counts: list[int] = [0] * num_buckets
        self.count: int = 0
        self.total: float = 0.0

    def observe(self, value: float, boundaries: tuple[float, ...]) -> None:
        self.count += 1
        self.total += value
        for idx, bound in enumerate(boundaries):
            if value <= bound:
                self.bucket_counts[idx] += 1
                break


class MetricsRegistry:
    """Thread-safe Prometheus metrics registry with strict label allowlists."""

    def __init__(
        self,
        *,
        engines: Iterable[str] = (),
        allowed_engines: Iterable[str] = (),
        adapters: Mapping[str, Any] | None = None,
        buckets: Sequence[float] = DEFAULT_DURATION_BUCKETS,
    ) -> None:
        cleaned_buckets: list[float] = []
        for b in buckets:
            if isinstance(b, bool) or not isinstance(b, (int, float)):
                raise ValueError("Histogram bucket boundary must be a finite positive number")
            fb = float(b)
            if not math.isfinite(fb) or fb <= 0:
                raise ValueError("Histogram bucket boundary must be a finite positive number")
            cleaned_buckets.append(fb)
        sorted_buckets = tuple(sorted(set(cleaned_buckets)))
        if not sorted_buckets:
            raise ValueError("Histogram buckets must not be empty")

        self._buckets: tuple[float, ...] = sorted_buckets
        self._lock = threading.Lock()
        self._allowed_engines: set[str] = set()
        self._adapters: dict[str, Any] = {}

        self._requests_total: dict[tuple[str, str, str], int] = {}
        self._durations: dict[tuple[str, str], _HistogramSeries] = {}
        self._errors_total: dict[tuple[str, str, str], int] = {}
        self._grants_rejected_total: dict[str, int] = {}
        self._audit_write_failures_total: int = 0
        self._guard_rejections_total: dict[str, int] = {}
        self._guard_warnings_total: dict[str, int] = {}
        self._sync_lag_seconds: dict[str, float] = {}
        self._discovery_refresh_total: dict[tuple[str, str], int] = {}
        self._lakebase_wake_total: dict[tuple[str, str], int] = {}
        self._lakebase_connect_retries_total: dict[str, int] = {}
        self._lakebase_cold_starts: dict[str, _HistogramSeries] = {}
        self._lakebase_states: dict[str, str] = {}

        self.add_engines(engines)
        self.add_engines(allowed_engines)
        if adapters is not None:
            self.register_adapters(adapters)

    @property
    def buckets(self) -> tuple[float, ...]:
        return self._buckets

    @property
    def allowed_engines(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._allowed_engines)

    def add_engines(self, engines: Iterable[str]) -> None:
        """Add safe configured engine identifiers to the engine label allowlist."""
        with self._lock:
            for name in engines:
                if _is_safe_configured_engine(name):
                    self._allowed_engines.add(name)

    def register_adapters(self, adapters: Mapping[str, Any]) -> None:
        """Register configured engine adapters for dynamic gauges and engine label allowlisting."""
        with self._lock:
            for name, adapter in adapters.items():
                if _is_safe_configured_engine(name):
                    self._allowed_engines.add(name)
                    self._adapters[name] = adapter

    def normalize_engine(self, engine: Any) -> str:
        """Normalize an engine name against the configured engine allowlist (or 'none')."""
        if not isinstance(engine, str):
            return "none"
        with self._lock:
            if engine in self._allowed_engines:
                return engine
        return "none"

    def _normalize_engine_locked(self, engine: Any) -> str:
        if isinstance(engine, str) and engine in self._allowed_engines:
            return engine
        return "none"

    def inc_request(self, route: Any, engine: Any, status: Any) -> None:
        """Increment seeknal_serving_requests_total with allowlisted labels."""
        safe_route = normalize_route(route)
        safe_status = normalize_status(status)
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            key = (safe_route, safe_engine, safe_status)
            self._requests_total[key] = self._requests_total.get(key, 0) + 1

    def observe_duration(
        self, route: Any, engine: Any, duration_seconds: float
    ) -> None:
        """Observe a request latency in seconds on seeknal_serving_request_duration_seconds."""
        if isinstance(duration_seconds, bool) or not isinstance(
            duration_seconds, (int, float)
        ):
            return
        val = float(duration_seconds)
        if math.isnan(val) or math.isinf(val):
            return
        if val < 0.0:
            val = 0.0
        safe_route = normalize_route(route)
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            key = (safe_route, safe_engine)
            series = self._durations.get(key)
            if series is None:
                series = _HistogramSeries(len(self._buckets))
                self._durations[key] = series
            series.observe(val, self._buckets)

    def record_error(self, route: Any, engine: Any, code: Any) -> None:
        """Increment seeknal_serving_errors_total with allowlisted labels."""
        safe_route = normalize_route(route)
        safe_code = normalize_error_code(code)
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            key = (safe_route, safe_engine, safe_code)
            self._errors_total[key] = self._errors_total.get(key, 0) + 1

    def record_request(
        self,
        route: Any,
        *,
        status: Any,
        duration_seconds: float | None = None,
        engine: Any = None,
        error_code: Any = None,
    ) -> None:
        """Atomically record request count, optional latency histogram, and optional error code."""
        safe_route = normalize_route(route)
        safe_status = normalize_status(status)
        safe_code = normalize_error_code(error_code) if error_code is not None else None

        valid_duration: float | None = None
        if (
            duration_seconds is not None
            and not isinstance(duration_seconds, bool)
            and isinstance(duration_seconds, (int, float))
        ):
            fd = float(duration_seconds)
            if math.isfinite(fd):
                valid_duration = max(0.0, fd)

        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            req_key = (safe_route, safe_engine, safe_status)
            self._requests_total[req_key] = self._requests_total.get(req_key, 0) + 1

            if valid_duration is not None:
                dur_key = (safe_route, safe_engine)
                series = self._durations.get(dur_key)
                if series is None:
                    series = _HistogramSeries(len(self._buckets))
                    self._durations[dur_key] = series
                series.observe(valid_duration, self._buckets)

            if safe_code is not None:
                err_key = (safe_route, safe_engine, safe_code)
                self._errors_total[err_key] = self._errors_total.get(err_key, 0) + 1

    def record_grant_rejected(self, reason: BaseException | str | None) -> None:
        """Increment seeknal_serving_grants_rejected_total by bounded reason enum."""
        if isinstance(reason, BaseException):
            safe_reason = classify_grant_rejection(reason)
        else:
            safe_reason = normalize_grant_reason(reason)
        with self._lock:
            self._grants_rejected_total[safe_reason] = (
                self._grants_rejected_total.get(safe_reason, 0) + 1
            )

    def record_audit_write_failure(self, count: int = 1) -> None:
        """Increment seeknal_serving_audit_write_failures_total."""
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            return
        with self._lock:
            self._audit_write_failures_total += count

    def record_guard_rejection(self, code: Any) -> None:
        """Increment seeknal_serving_guard_rejections_total by bounded guardrail code."""
        safe_code = normalize_guard_code(code)
        with self._lock:
            self._guard_rejections_total[safe_code] = (
                self._guard_rejections_total.get(safe_code, 0) + 1
            )

    def inc_guard_rejection(self, code: Any) -> None:
        """Alias for record_guard_rejection."""
        self.record_guard_rejection(code)

    def record_guard_warning(self, code: Any) -> None:
        """Increment seeknal_serving_guard_warnings_total by bounded guardrail code."""
        safe_code = normalize_guard_code(code)
        with self._lock:
            self._guard_warnings_total[safe_code] = (
                self._guard_warnings_total.get(safe_code, 0) + 1
            )

    def inc_guard_warning(self, code: Any) -> None:
        """Alias for record_guard_warning."""
        self.record_guard_warning(code)

    def set_sync_lag(self, engine: Any, lag_seconds: float | int | None) -> None:
        """Set or clear seeknal_serving_sync_lag_seconds{engine}."""
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            if lag_seconds is None:
                self._sync_lag_seconds.pop(safe_engine, None)
                return
            if isinstance(lag_seconds, bool) or not isinstance(
                lag_seconds, (int, float)
            ):
                return
            fval = float(lag_seconds)
            if not math.isfinite(fval):
                return
            self._sync_lag_seconds[safe_engine] = max(0.0, fval)

    def record_discovery_refresh(self, engine: Any, outcome: Any) -> None:
        """Increment seeknal_serving_discovery_refresh_total{engine,outcome}."""
        safe_outcome = (
            outcome
            if isinstance(outcome, str) and outcome in _ALLOWED_DISCOVERY_OUTCOMES_SET
            else "error"
        )
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            key = (safe_engine, safe_outcome)
            self._discovery_refresh_total[key] = (
                self._discovery_refresh_total.get(key, 0) + 1
            )

    def record_lakebase_wake(self, engine: Any, outcome: Any) -> None:
        """Increment seeknal_serving_lakebase_wake_total{engine,outcome}."""
        safe_outcome = (
            outcome
            if isinstance(outcome, str)
            and outcome in _ALLOWED_LAKEBASE_WAKE_OUTCOMES_SET
            else "error"
        )
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            key = (safe_engine, safe_outcome)
            self._lakebase_wake_total[key] = (
                self._lakebase_wake_total.get(key, 0) + 1
            )

    def record_lakebase_connect_retry(self, engine: Any, count: int = 1) -> None:
        """Increment seeknal_serving_lakebase_connect_retries_total{engine}."""
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            return
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            self._lakebase_connect_retries_total[safe_engine] = (
                self._lakebase_connect_retries_total.get(safe_engine, 0) + count
            )

    def observe_lakebase_cold_start(
        self, engine: Any, duration_seconds: float
    ) -> None:
        """Observe cold-start duration on seeknal_serving_lakebase_cold_start_seconds{engine}."""
        if isinstance(duration_seconds, bool) or not isinstance(
            duration_seconds, (int, float)
        ):
            return
        val = float(duration_seconds)
        if math.isnan(val) or math.isinf(val):
            return
        if val < 0.0:
            val = 0.0
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            series = self._lakebase_cold_starts.get(safe_engine)
            if series is None:
                series = _HistogramSeries(len(self._buckets))
                self._lakebase_cold_starts[safe_engine] = series
            series.observe(val, self._buckets)

    def set_lakebase_state(self, engine: Any, state: Any) -> None:
        """Set seeknal_serving_lakebase_state{engine,state} bounded to ok|starting|unavailable."""
        safe_state = (
            state
            if isinstance(state, str) and state in _ALLOWED_LAKEBASE_STATES_SET
            else "unavailable"
        )
        with self._lock:
            safe_engine = self._normalize_engine_locked(engine)
            self._lakebase_states[safe_engine] = safe_state

    def reset(self) -> None:
        """Clear recorded counter and histogram values (preserving configured engines/adapters)."""
        with self._lock:
            self._requests_total.clear()
            self._durations.clear()
            self._errors_total.clear()
            self._grants_rejected_total.clear()
            self._audit_write_failures_total = 0
            self._guard_rejections_total.clear()
            self._guard_warnings_total.clear()
            self._sync_lag_seconds.clear()
            self._discovery_refresh_total.clear()
            self._lakebase_wake_total.clear()
            self._lakebase_connect_retries_total.clear()
            self._lakebase_cold_starts.clear()
            self._lakebase_states.clear()

    @staticmethod
    def _collect_adapter_gauges(
        adapters_snapshot: list[tuple[str, Any]],
    ) -> tuple[
        list[tuple[str, dict[str, int]]],
        list[tuple[str, dict[str, int]]],
        list[tuple[str, str, str, dict[str, int]]],
    ]:
        duckdb_stats: list[tuple[str, dict[str, int]]] = []
        lakebase_stats: list[tuple[str, dict[str, int]]] = []
        postgres_pool_stats: list[tuple[str, str, str, dict[str, int]]] = []

        for eng_name, adapter in adapters_snapshot:
            if isinstance(adapter, WorkloadRoutedAdapter) and adapter.by_workload:
                target = adapter.by_workload.get("interactive") or next(
                    iter(adapter.by_workload.values())
                )
                if isinstance(target, DuckDBAdapter):
                    try:
                        duckdb_stats.append((eng_name, target.stats()))
                    except Exception:
                        pass

                seen_pool_ids: set[int] = set()
                agg_in_use = 0
                agg_idle = 0
                has_pooled_sub = False
                for w_key, sub in sorted(
                    adapter.by_workload.items(), key=lambda kv: str(kv[0])
                ):
                    if (
                        isinstance(sub, DBAPIAdapter)
                        and sub.kind in _ALLOWED_POSTGRES_POOL_KINDS_SET
                        and sub.pool is not None
                    ):
                        try:
                            pst = sub.pool_stats()
                        except Exception:
                            pst = None
                        if pst is not None:
                            acc_label = (
                                w_key
                                if isinstance(w_key, str)
                                and w_key in _ALLOWED_POSTGRES_ACCOUNTS_SET
                                else "default"
                            )
                            postgres_pool_stats.append(
                                (eng_name, sub.kind, acc_label, pst)
                            )
                            pid = id(sub.pool)
                            if pid not in seen_pool_ids:
                                seen_pool_ids.add(pid)
                                agg_in_use += int(pst.get("in_use", 0))
                                agg_idle += int(pst.get("idle", 0))
                                has_pooled_sub = True
                if has_pooled_sub:
                    lakebase_stats.append(
                        (eng_name, {"in_use": agg_in_use, "idle": agg_idle})
                    )
                continue

            if isinstance(adapter, DuckDBAdapter):
                try:
                    duckdb_stats.append((eng_name, adapter.stats()))
                except Exception:
                    pass
            elif isinstance(adapter, DBAPIAdapter) and adapter.pool is not None:
                try:
                    pst = adapter.pool_stats()
                    if pst is not None:
                        lakebase_stats.append((eng_name, pst))
                        if adapter.kind in _ALLOWED_POSTGRES_POOL_KINDS_SET:
                            raw_acc = adapter.account
                            acc_label = (
                                raw_acc
                                if isinstance(raw_acc, str)
                                and raw_acc in _ALLOWED_POSTGRES_ACCOUNTS_SET
                                else "default"
                            )
                            postgres_pool_stats.append(
                                (eng_name, adapter.kind, acc_label, pst)
                            )
                except Exception:
                    pass

        return duckdb_stats, lakebase_stats, postgres_pool_stats

    def render(self) -> str:
        """Render all metrics in Prometheus text exposition format 0.0.4."""
        with self._lock:
            requests_snap = sorted(self._requests_total.items())
            durations_snap = [
                (k, list(v.bucket_counts), v.count, v.total)
                for k, v in sorted(self._durations.items())
            ]
            errors_snap = sorted(self._errors_total.items())
            grants_snap = sorted(self._grants_rejected_total.items())
            audit_failures = self._audit_write_failures_total
            guard_snap = sorted(self._guard_rejections_total.items())
            guard_warn_snap = sorted(self._guard_warnings_total.items())
            sync_lag_snap = sorted(self._sync_lag_seconds.items())
            disc_refresh_snap = sorted(self._discovery_refresh_total.items())
            lb_wake_snap = sorted(self._lakebase_wake_total.items())
            lb_retries_snap = sorted(self._lakebase_connect_retries_total.items())
            lb_cold_starts_snap = [
                (k, list(v.bucket_counts), v.count, v.total)
                for k, v in sorted(self._lakebase_cold_starts.items())
            ]
            lb_states_snap = sorted(self._lakebase_states.items())
            adapters_snap = sorted(self._adapters.items(), key=lambda kv: kv[0])

        duckdb_stats, lakebase_stats, postgres_pool_stats = (
            self._collect_adapter_gauges(adapters_snap)
        )

        lines: list[str] = []

        # 1. Requests counter
        lines.append(
            "# HELP seeknal_serving_requests_total Total HTTP requests handled by Seeknal Serving."
        )
        lines.append("# TYPE seeknal_serving_requests_total counter")
        for (route, engine, status), val in requests_snap:
            r_esc = _escape_label_value(route)
            e_esc = _escape_label_value(engine)
            s_esc = _escape_label_value(status)
            lines.append(
                f'seeknal_serving_requests_total{{route="{r_esc}",engine="{e_esc}",status="{s_esc}"}} {val}'
            )

        # 2. Request duration histogram
        lines.append(
            "# HELP seeknal_serving_request_duration_seconds Request latency in seconds by route and engine."
        )
        lines.append("# TYPE seeknal_serving_request_duration_seconds histogram")
        for (route, engine), bucket_counts, count, total in durations_snap:
            r_esc = _escape_label_value(route)
            e_esc = _escape_label_value(engine)
            cumulative = 0
            for bound, b_count in zip(self._buckets, bucket_counts):
                cumulative += b_count
                lines.append(
                    f'seeknal_serving_request_duration_seconds_bucket{{route="{r_esc}",engine="{e_esc}",le="{bound}"}} {cumulative}'
                )
            lines.append(
                f'seeknal_serving_request_duration_seconds_bucket{{route="{r_esc}",engine="{e_esc}",le="+Inf"}} {count}'
            )
            lines.append(
                f'seeknal_serving_request_duration_seconds_sum{{route="{r_esc}",engine="{e_esc}"}} {total:.6f}'
            )
            lines.append(
                f'seeknal_serving_request_duration_seconds_count{{route="{r_esc}",engine="{e_esc}"}} {count}'
            )

        # 3. Errors counter
        lines.append(
            "# HELP seeknal_serving_errors_total Total errors by route, engine, and typed error code."
        )
        lines.append("# TYPE seeknal_serving_errors_total counter")
        for (route, engine, code), val in errors_snap:
            r_esc = _escape_label_value(route)
            e_esc = _escape_label_value(engine)
            c_esc = _escape_label_value(code)
            lines.append(
                f'seeknal_serving_errors_total{{route="{r_esc}",engine="{e_esc}",code="{c_esc}"}} {val}'
            )

        # 4. DuckDB gauges
        lines.append(
            "# HELP seeknal_serving_duckdb_queue_depth Requests waiting for a DuckDB cursor slot."
        )
        lines.append("# TYPE seeknal_serving_duckdb_queue_depth gauge")
        for eng_name, st in duckdb_stats:
            e_esc = _escape_label_value(eng_name)
            lines.append(
                f'seeknal_serving_duckdb_queue_depth{{engine="{e_esc}"}} {int(st.get("queue_depth", 0))}'
            )

        lines.append(
            "# HELP seeknal_serving_duckdb_in_flight_cursors Currently active DuckDB cursor slots."
        )
        lines.append("# TYPE seeknal_serving_duckdb_in_flight_cursors gauge")
        for eng_name, st in duckdb_stats:
            e_esc = _escape_label_value(eng_name)
            lines.append(
                f'seeknal_serving_duckdb_in_flight_cursors{{engine="{e_esc}"}} {int(st.get("in_flight_cursors", 0))}'
            )

        lines.append(
            "# HELP seeknal_serving_duckdb_generation Current DuckDB holder instance generation."
        )
        lines.append("# TYPE seeknal_serving_duckdb_generation gauge")
        for eng_name, st in duckdb_stats:
            e_esc = _escape_label_value(eng_name)
            lines.append(
                f'seeknal_serving_duckdb_generation{{engine="{e_esc}"}} {int(st.get("generation", 0))}'
            )

        # 5. Postgres-wire pool gauges (plus deprecated Lakebase pool gauges)
        lines.append(
            "# HELP seeknal_serving_postgres_pool_in_use Active borrowed connections in the Postgres-wire pool."
        )
        lines.append("# TYPE seeknal_serving_postgres_pool_in_use gauge")
        for eng_name, kind_str, acc_str, pst in postgres_pool_stats:
            e_esc = _escape_label_value(eng_name)
            k_esc = _escape_label_value(kind_str)
            a_esc = _escape_label_value(acc_str)
            lines.append(
                f'seeknal_serving_postgres_pool_in_use{{engine="{e_esc}",kind="{k_esc}",account="{a_esc}"}} {int(pst.get("in_use", 0))}'
            )

        lines.append(
            "# HELP seeknal_serving_postgres_pool_idle Idle available connections in the Postgres-wire pool."
        )
        lines.append("# TYPE seeknal_serving_postgres_pool_idle gauge")
        for eng_name, kind_str, acc_str, pst in postgres_pool_stats:
            e_esc = _escape_label_value(eng_name)
            k_esc = _escape_label_value(kind_str)
            a_esc = _escape_label_value(acc_str)
            lines.append(
                f'seeknal_serving_postgres_pool_idle{{engine="{e_esc}",kind="{k_esc}",account="{a_esc}"}} {int(pst.get("idle", 0))}'
            )

        lines.append(
            "# HELP seeknal_serving_lakebase_pool_in_use Deprecated duplicate of seeknal_serving_postgres_pool_in_use."
        )
        lines.append("# TYPE seeknal_serving_lakebase_pool_in_use gauge")
        for eng_name, pst in lakebase_stats:
            e_esc = _escape_label_value(eng_name)
            lines.append(
                f'seeknal_serving_lakebase_pool_in_use{{engine="{e_esc}"}} {int(pst.get("in_use", 0))}'
            )

        lines.append(
            "# HELP seeknal_serving_lakebase_pool_idle Deprecated duplicate of seeknal_serving_postgres_pool_idle."
        )
        lines.append("# TYPE seeknal_serving_lakebase_pool_idle gauge")
        for eng_name, pst in lakebase_stats:
            e_esc = _escape_label_value(eng_name)
            lines.append(
                f'seeknal_serving_lakebase_pool_idle{{engine="{e_esc}"}} {int(pst.get("idle", 0))}'
            )

        # 6. Grants rejected counter
        lines.append(
            "# HELP seeknal_serving_grants_rejected_total Total rejected grant or cancel tokens by bounded reason."
        )
        lines.append("# TYPE seeknal_serving_grants_rejected_total counter")
        for reason, val in grants_snap:
            rs_esc = _escape_label_value(reason)
            lines.append(
                f'seeknal_serving_grants_rejected_total{{reason="{rs_esc}"}} {val}'
            )

        # 7. Audit write failures counter
        lines.append(
            "# HELP seeknal_serving_audit_write_failures_total Total audit log write failures."
        )
        lines.append("# TYPE seeknal_serving_audit_write_failures_total counter")
        lines.append(f"seeknal_serving_audit_write_failures_total {audit_failures}")

        # 8. Guardrail rejections & warnings counters
        lines.append(
            "# HELP seeknal_serving_guard_rejections_total Total guardrail rejections by bounded code."
        )
        lines.append("# TYPE seeknal_serving_guard_rejections_total counter")
        for g_code, val in guard_snap:
            gc_esc = _escape_label_value(g_code)
            lines.append(
                f'seeknal_serving_guard_rejections_total{{code="{gc_esc}"}} {val}'
            )

        lines.append(
            "# HELP seeknal_serving_guard_warnings_total Total guardrail warnings by bounded code."
        )
        lines.append("# TYPE seeknal_serving_guard_warnings_total counter")
        for g_code, val in guard_warn_snap:
            gc_esc = _escape_label_value(g_code)
            lines.append(
                f'seeknal_serving_guard_warnings_total{{code="{gc_esc}"}} {val}'
            )

        # 9. Sync lag gauge
        lines.append(
            "# HELP seeknal_serving_sync_lag_seconds Observed source freshness lag in seconds by engine."
        )
        lines.append("# TYPE seeknal_serving_sync_lag_seconds gauge")
        for eng_name, lag_val in sync_lag_snap:
            e_esc = _escape_label_value(eng_name)
            lines.append(
                f'seeknal_serving_sync_lag_seconds{{engine="{e_esc}"}} {_format_gauge_float(lag_val)}'
            )

        # 10. Discovery refresh counter
        lines.append(
            "# HELP seeknal_serving_discovery_refresh_total Total discovery refresh cycles by engine and outcome."
        )
        lines.append("# TYPE seeknal_serving_discovery_refresh_total counter")
        for (eng_name, outcome), val in disc_refresh_snap:
            e_esc = _escape_label_value(eng_name)
            o_esc = _escape_label_value(outcome)
            lines.append(
                f'seeknal_serving_discovery_refresh_total{{engine="{e_esc}",outcome="{o_esc}"}} {val}'
            )

        # 11. Lakebase cold-start & wake series
        lines.append(
            "# HELP seeknal_serving_lakebase_wake_total Total Lakebase control-plane wake operations by engine and outcome."
        )
        lines.append("# TYPE seeknal_serving_lakebase_wake_total counter")
        for (eng_name, outcome), val in lb_wake_snap:
            e_esc = _escape_label_value(eng_name)
            o_esc = _escape_label_value(outcome)
            lines.append(
                f'seeknal_serving_lakebase_wake_total{{engine="{e_esc}",outcome="{o_esc}"}} {val}'
            )

        lines.append(
            "# HELP seeknal_serving_lakebase_connect_retries_total Total Lakebase bounded connection retries by engine."
        )
        lines.append("# TYPE seeknal_serving_lakebase_connect_retries_total counter")
        for eng_name, val in lb_retries_snap:
            e_esc = _escape_label_value(eng_name)
            lines.append(
                f'seeknal_serving_lakebase_connect_retries_total{{engine="{e_esc}"}} {val}'
            )

        lines.append(
            "# HELP seeknal_serving_lakebase_cold_start_seconds Lakebase cold-start resume duration in seconds by engine."
        )
        lines.append("# TYPE seeknal_serving_lakebase_cold_start_seconds histogram")
        for eng_name, bucket_counts, count, total in lb_cold_starts_snap:
            e_esc = _escape_label_value(eng_name)
            cumulative = 0
            for bound, b_count in zip(self._buckets, bucket_counts):
                cumulative += b_count
                lines.append(
                    f'seeknal_serving_lakebase_cold_start_seconds_bucket{{engine="{e_esc}",le="{bound}"}} {cumulative}'
                )
            lines.append(
                f'seeknal_serving_lakebase_cold_start_seconds_bucket{{engine="{e_esc}",le="+Inf"}} {count}'
            )
            lines.append(
                f'seeknal_serving_lakebase_cold_start_seconds_sum{{engine="{e_esc}"}} {total:.6f}'
            )
            lines.append(
                f'seeknal_serving_lakebase_cold_start_seconds_count{{engine="{e_esc}"}} {count}'
            )

        lines.append(
            "# HELP seeknal_serving_lakebase_state Current Lakebase endpoint state (1 for active state, 0 otherwise)."
        )
        lines.append("# TYPE seeknal_serving_lakebase_state gauge")
        for eng_name, cur_state in lb_states_snap:
            e_esc = _escape_label_value(eng_name)
            for st_name in ALLOWED_LAKEBASE_STATES:
                st_esc = _escape_label_value(st_name)
                flag = 1 if st_name == cur_state else 0
                lines.append(
                    f'seeknal_serving_lakebase_state{{engine="{e_esc}",state="{st_esc}"}} {flag}'
                )

        return "\n".join(lines) + "\n"

    format_prometheus = render


_DEFAULT_REGISTRY = MetricsRegistry()


def get_default_registry() -> MetricsRegistry:
    """Return the process-wide default MetricsRegistry instance."""
    return _DEFAULT_REGISTRY

"""Load testing harness for Seeknal Serving.

Sends concurrent POST /v1/query requests via HTTPX and asyncio, grouping results
by routing rule and reporting request counts, errors by status, and latency percentiles.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass
class RuleStats:
    """Statistics collected for queries under a specific routing rule."""

    rule: str
    request_count: int = 0
    errors_by_status: dict[int, int] = field(default_factory=dict)
    latencies_ms: list[float] = field(default_factory=list)
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0


@dataclass
class LoadReport:
    """Summary of a load test run across all routing rules."""

    total_requests: int
    total_errors: int
    by_rule: dict[str, RuleStats]
    duration_seconds: float

    def format_summary(self) -> str:
        """Format a human-readable summary of load test metrics."""
        lines = [
            f"Load Test Report ({self.duration_seconds:.2f}s, "
            f"{self.total_requests} requests, {self.total_errors} errors):",
        ]
        for rule_name, stats in sorted(self.by_rule.items()):
            err_str = f"errors={stats.errors_by_status}" if stats.errors_by_status else "errors=0"
            lines.append(
                f"  Rule: {rule_name:<16} count={stats.request_count:<6} "
                f"{err_str:<20} p50={stats.p50_ms:.2f}ms p95={stats.p95_ms:.2f}ms p99={stats.p99_ms:.2f}ms"
            )
        return "\n".join(lines)


def _percentile(values: Sequence[float], p: float) -> float:
    """Calculate the p-th percentile from an ordered sequence."""
    if not values:
        return 0.0
    k = (len(values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(values[int(k)])
    d0 = values[int(f)] * (c - k)
    d1 = values[int(c)] * (k - f)
    return float(d0 + d1)


def load_tokens(token_file: str) -> dict[str, str]:
    """Load grant tokens from a JSON or YAML file."""
    with open(token_file, "r", encoding="utf-8") as f:
        content = f.read().strip()
    try:
        data = json.loads(content)
    except Exception:
        import yaml
        data = yaml.safe_load(content)

    if isinstance(data, str):
        return {"default": data}
    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items()}
    if isinstance(data, list):
        return {str(i): str(t) for i, t in enumerate(data)}
    return {}


def load_cases(cases_file: str) -> list[dict[str, Any]]:
    """Load query cases from a JSON or YAML file."""
    with open(cases_file, "r", encoding="utf-8") as f:
        content = f.read().strip()
    try:
        import yaml
        data = yaml.safe_load(content)
    except Exception:
        data = json.loads(content)

    if isinstance(data, dict) and "cases" in data:
        data = data["cases"]
    if not isinstance(data, list):
        raise ValueError("cases file must contain a list of cases")
    return list(data)


async def run_load(
    url: str | None = None,
    *,
    cases: Sequence[dict[str, Any]],
    tokens: Mapping[str, str] | None = None,
    concurrency: int = 1,
    duration: float = 1.0,
    transport: httpx.AsyncBaseTransport | None = None,
    app: Any = None,
    client: httpx.AsyncClient | None = None,
) -> LoadReport:
    """Run concurrent load test queries against a serving endpoint."""
    if not cases:
        raise ValueError("cases sequence cannot be empty")

    tokens_dict = dict(tokens or {})

    # Prepare cases with resolved tokens
    prepared_cases: list[dict[str, Any]] = []
    for c in cases:
        c_dict = dict(c)
        tok = c_dict.get("token")
        if not tok:
            name = c_dict.get("name")
            key = c_dict.get("token_key") or name
            if key and key in tokens_dict:
                tok = tokens_dict[key]
            elif "default" in tokens_dict:
                tok = tokens_dict["default"]
            elif len(tokens_dict) == 1:
                tok = next(iter(tokens_dict.values()))
        if not tok:
            raise ValueError(f"No token found for case: {c_dict.get('name', 'unnamed')}")
        c_dict["token"] = tok
        prepared_cases.append(c_dict)

    by_rule: dict[str, RuleStats] = {}
    lock = asyncio.Lock()
    start_time = time.monotonic()
    end_time = start_time + max(0.01, float(duration))

    async def _execute_loop(cli: httpx.AsyncClient, worker_id: int) -> None:
        idx = worker_id
        while time.monotonic() < end_time:
            case_item = prepared_cases[idx % len(prepared_cases)]
            idx += 1

            sql = case_item["sql"]
            params = case_item.get("params", [])
            token = case_item["token"]

            req_start = time.perf_counter()
            status = 200
            rule = "unknown"
            try:
                resp = await cli.post(
                    "/v1/query",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"sql": sql, "params": params},
                    timeout=30.0,
                )
                latency_ms = (time.perf_counter() - req_start) * 1000.0
                status = resp.status_code
                try:
                    body = resp.json()
                    if isinstance(body, dict) and "rule" in body:
                        rule = body["rule"]
                except Exception:
                    pass
            except Exception:
                latency_ms = (time.perf_counter() - req_start) * 1000.0
                status = 500
                rule = "network_error"

            async with lock:
                if rule not in by_rule:
                    by_rule[rule] = RuleStats(rule=rule)
                stats = by_rule[rule]
                stats.request_count += 1
                stats.latencies_ms.append(latency_ms)
                if status != 200:
                    stats.errors_by_status[status] = stats.errors_by_status.get(status, 0) + 1

    async def _run_with_client(cli: httpx.AsyncClient) -> None:
        tasks = [
            asyncio.create_task(_execute_loop(cli, i))
            for i in range(max(1, concurrency))
        ]
        await asyncio.gather(*tasks)

    if client is not None:
        await _run_with_client(client)
    elif app is not None or transport is not None:
        effective_transport = transport or httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=effective_transport,
            base_url=url or "http://testserver",
        ) as cli:
            await _run_with_client(cli)
    else:
        async with httpx.AsyncClient(base_url=url) as cli:
            await _run_with_client(cli)

    actual_duration = time.monotonic() - start_time
    total_requests = 0
    total_errors = 0

    for stats in by_rule.values():
        total_requests += stats.request_count
        total_errors += sum(stats.errors_by_status.values())
        if stats.latencies_ms:
            sorted_lats = sorted(stats.latencies_ms)
            stats.p50_ms = _percentile(sorted_lats, 50)
            stats.p95_ms = _percentile(sorted_lats, 95)
            stats.p99_ms = _percentile(sorted_lats, 99)

    return LoadReport(
        total_requests=total_requests,
        total_errors=total_errors,
        by_rule=by_rule,
        duration_seconds=actual_duration,
    )


def main() -> None:
    """CLI entrypoint for running load test harness."""
    parser = argparse.ArgumentParser(description="Seeknal Serving load testing harness")
    parser.add_argument("--url", required=True, help="Base URL of the serving instance (e.g. http://localhost:8000)")
    parser.add_argument("--token-file", required=True, help="Path to token JSON or YAML file")
    parser.add_argument("--cases", required=True, help="Path to cases YAML or JSON file")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of concurrent workers")
    parser.add_argument("--duration", type=float, default=5.0, help="Test duration in seconds")

    args = parser.parse_args()

    tokens = load_tokens(args.token_file)
    cases = load_cases(args.cases)

    report = asyncio.run(
        run_load(
            url=args.url,
            cases=cases,
            tokens=tokens,
            concurrency=args.concurrency,
            duration=args.duration,
        )
    )

    print(report.format_summary())
    if report.total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

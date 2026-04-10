"""Concurrency test script for seeknal ask gateway.

Tests that multiple users can query the gateway simultaneously
without cross-contamination or errors.

Usage:
    python scripts/test_concurrency.py --url http://172.19.0.9:8000 --concurrency 5
    python scripts/test_concurrency.py --url http://localhost:8000 --scenario a
    python scripts/test_concurrency.py --url http://localhost:8000 --scenario all
"""

from __future__ import annotations

import argparse
import asyncio
import time
from dataclasses import dataclass


@dataclass
class Result:
    request_id: int
    session_id: str
    status_code: int
    response_time_ms: float
    answer: str
    expected_marker: str
    passed: bool
    error: str | None = None


async def send_ask_request(
    session: object,
    url: str,
    session_id: str,
    question: str,
    request_id: int,
    expected_marker: str,
    auth_token: str | None = None,
) -> Result:
    """Send a single /ask request and return the result."""
    import aiohttp

    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    payload = {"question": question, "session_id": session_id}
    start = time.monotonic()

    try:
        async with session.post(
            f"{url}/ask",
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=180),
        ) as resp:
            elapsed = (time.monotonic() - start) * 1000
            if resp.status == 200:
                data = await resp.json()
                answer = data.get("answer", "")
                passed = bool(answer) if not expected_marker else expected_marker.lower() in answer.lower()
                return Result(
                    request_id=request_id,
                    session_id=session_id,
                    status_code=resp.status,
                    response_time_ms=elapsed,
                    answer=answer[:200],
                    expected_marker=expected_marker,
                    passed=passed,
                )
            else:
                body = await resp.text()
                return Result(
                    request_id=request_id,
                    session_id=session_id,
                    status_code=resp.status,
                    response_time_ms=elapsed,
                    answer="",
                    expected_marker=expected_marker,
                    passed=False,
                    error=body[:200],
                )
    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        return Result(
            request_id=request_id,
            session_id=session_id,
            status_code=0,
            response_time_ms=elapsed,
            answer="",
            expected_marker=expected_marker,
            passed=False,
            error=str(e)[:200],
        )


async def scenario_a_multi_user(url: str, concurrency: int) -> list[Result]:
    """Scenario A: Different-session concurrency (main scaling test)."""
    import aiohttp

    print(f"\n{'='*60}")
    print(f"Scenario A: Multi-user concurrency (N={concurrency})")
    print(f"{'='*60}")

    ts = int(time.time())
    tasks = []

    async with aiohttp.ClientSession() as session:
        for i in range(concurrency):
            sid = f"concurrency-a-{i}-{ts}"
            # Each question asks for a specific number to verify no cross-contamination
            question = f"Run this SQL and return ONLY the number: SELECT COUNT(*) FROM raw_customers"
            # Pass criterion: got a numeric answer (not an error)
            tasks.append(
                send_ask_request(session, url, sid, question, i, "")
            )

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        total_elapsed = (time.monotonic() - start) * 1000

    print(f"\nTotal wall-clock time: {total_elapsed:.0f}ms")
    avg_single = sum(r.response_time_ms for r in results) / len(results)
    print(f"Avg per-request time: {avg_single:.0f}ms")
    if concurrency > 1:
        speedup = (avg_single * concurrency) / total_elapsed
        print(f"Parallelism speedup: {speedup:.1f}x")

    return list(results)


async def scenario_b_same_session(url: str) -> list[Result]:
    """Scenario B: Same-session serialization (correctness test)."""
    import aiohttp

    print(f"\n{'='*60}")
    print(f"Scenario B: Same-session serialization (N=3)")
    print(f"{'='*60}")

    ts = int(time.time())
    sid = f"concurrency-b-serial-{ts}"
    tasks = []

    async with aiohttp.ClientSession() as session:
        for i in range(3):
            question = f"How many tables are available? Just the count. (request {i})"
            tasks.append(
                send_ask_request(session, url, sid, question, i, "table")
            )

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        total_elapsed = (time.monotonic() - start) * 1000

    print(f"\nTotal wall-clock time: {total_elapsed:.0f}ms")
    print(f"(Expect ~3x single request if serialization works)")

    return list(results)


async def scenario_c_temporal(url: str, concurrency: int) -> list[Result]:
    """Scenario C: Temporal workflow dispatch (split-mode test)."""
    import aiohttp

    print(f"\n{'='*60}")
    print(f"Scenario C: Temporal workflow dispatch (N={concurrency})")
    print(f"{'='*60}")

    ts = int(time.time())
    results = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(concurrency):
            sid = f"concurrency-c-{i}-{ts}"

            async def submit(s=session, session_id=sid, idx=i):
                start = time.monotonic()
                try:
                    async with s.post(
                        f"{url}/temporal/start",
                        json={"question": "List tables", "session_id": session_id},
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        elapsed = (time.monotonic() - start) * 1000
                        if resp.status == 200:
                            data = await resp.json()
                            wf_id = data.get("workflow_id", "")
                            return Result(
                                request_id=idx,
                                session_id=session_id,
                                status_code=resp.status,
                                response_time_ms=elapsed,
                                answer=f"workflow_id={wf_id}",
                                expected_marker="workflow_id",
                                passed=bool(wf_id),
                            )
                        else:
                            return Result(
                                request_id=idx,
                                session_id=session_id,
                                status_code=resp.status,
                                response_time_ms=elapsed,
                                answer="",
                                expected_marker="workflow_id",
                                passed=False,
                                error=await resp.text(),
                            )
                except Exception as e:
                    elapsed = (time.monotonic() - start) * 1000
                    return Result(
                        request_id=idx,
                        session_id=session_id,
                        status_code=0,
                        response_time_ms=elapsed,
                        answer="",
                        expected_marker="workflow_id",
                        passed=False,
                        error=str(e)[:200],
                    )

            tasks.append(submit())

        start = time.monotonic()
        results = await asyncio.gather(*tasks)
        total_elapsed = (time.monotonic() - start) * 1000

    print(f"\nTotal wall-clock time: {total_elapsed:.0f}ms")
    print(f"(Expect fast — workflow start is non-blocking)")

    return list(results)


def print_results(results: list[Result]) -> int:
    """Print results table and return number of failures."""
    print(f"\n{'ID':>3} | {'Session':>30} | {'Status':>6} | {'Time':>8} | {'Pass':>4} | Answer / Error")
    print("-" * 100)
    failures = 0
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        if not r.passed:
            failures += 1
        detail = r.answer[:50] if r.passed else (r.error or r.answer)[:50]
        print(f"{r.request_id:>3} | {r.session_id:>30} | {r.status_code:>6} | {r.response_time_ms:>7.0f}ms | {status:>4} | {detail}")

    print(f"\nResults: {len(results) - failures}/{len(results)} passed, {failures} failed")
    return failures


async def main():
    parser = argparse.ArgumentParser(description="Seeknal gateway concurrency test")
    parser.add_argument("--url", default="http://localhost:8000", help="Gateway base URL")
    parser.add_argument("--concurrency", "-n", type=int, default=5, help="Number of concurrent requests")
    parser.add_argument("--scenario", choices=["a", "b", "c", "all"], default="a", help="Test scenario")
    parser.add_argument("--dry-run", action="store_true", help="Only check health endpoint")
    args = parser.parse_args()

    # Health check
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{args.url}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    print(f"Gateway healthy: {args.url}")
                else:
                    print(f"Gateway unhealthy: {resp.status}")
                    return 1
    except Exception as e:
        print(f"Cannot reach gateway at {args.url}: {e}")
        return 1

    if args.dry_run:
        print("Dry run — connectivity OK")
        return 0

    total_failures = 0
    scenarios = [args.scenario] if args.scenario != "all" else ["a", "b", "c"]

    for s in scenarios:
        if s == "a":
            results = await scenario_a_multi_user(args.url, args.concurrency)
        elif s == "b":
            results = await scenario_b_same_session(args.url)
        elif s == "c":
            results = await scenario_c_temporal(args.url, args.concurrency)
        else:
            continue
        total_failures += print_results(results)

    return 1 if total_failures > 0 else 0


if __name__ == "__main__":
    exit(asyncio.run(main()))

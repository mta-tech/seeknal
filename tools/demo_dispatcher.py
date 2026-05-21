"""Submit N work items concurrently and report which worker handled each."""
from __future__ import annotations

import asyncio
import sys
import time

import httpx


async def submit_one(client: httpx.AsyncClient, idx: int, gateway_url: str) -> tuple[int, dict, float]:
    started = time.monotonic()
    print(f"[DISP] → submit #{idx:02d}", flush=True)
    r = await client.post(
        f"{gateway_url}/demo/submit",
        json={"session_id": f"sess-{idx:02d}", "question": f"question-{idx:02d}"},
        timeout=120.0,
    )
    elapsed = time.monotonic() - started
    body = r.json()
    print(
        f"[DISP] ✓ #{idx:02d} done in {elapsed:5.1f}s → {body['answer']!r} "
        f"events={body['event_count']}",
        flush=True,
    )
    return idx, body, elapsed


async def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    gateway_url = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8765"

    print(f"[DISP] firing {n} concurrent submissions at {gateway_url}", flush=True)
    started = time.monotonic()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *[submit_one(client, i, gateway_url) for i in range(n)]
        )
    total_elapsed = time.monotonic() - started

    print("\n[DISP] ===== Summary =====", flush=True)
    print(f"[DISP] total wall time: {total_elapsed:.1f}s for {n} items", flush=True)

    from collections import Counter
    handler_counts = Counter(body["answer"] for _, body, _ in results)
    for handler, count in sorted(handler_counts.items()):
        print(f"[DISP]   {handler}: {count} items", flush=True)
    avg = sum(e for _, _, e in results) / len(results)
    print(f"[DISP] avg per-item latency: {avg:.1f}s", flush=True)


if __name__ == "__main__":
    asyncio.run(main())

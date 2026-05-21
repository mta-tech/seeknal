"""Demo HTTP worker — runs the REAL _run_http_only_worker code path
with a mocked agent (no LLM calls). Use for tmux multi-worker demos.

Usage: python tools/demo_worker.py <worker_id> <max_concurrency> [gateway_url]
"""
from __future__ import annotations

import asyncio
import random
import sys
from pathlib import Path
from unittest.mock import patch


async def main() -> None:
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    max_concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    gateway_url = sys.argv[3] if len(sys.argv) > 3 else "http://127.0.0.1:8765"

    rng = random.Random(int(worker_id) * 1000)

    async def fake_streaming(_project, session, question, **_kw):
        delay = rng.uniform(2.0, 5.0)
        print(
            f"[W{worker_id}] start session={session} q={question!r} (will take {delay:.1f}s)",
            flush=True,
        )
        yield {"type": "tool_call", "data": "SELECT 1 FROM fake_table"}
        await asyncio.sleep(delay / 2)
        yield {"type": "tool_result", "data": "fake_rows=42"}
        await asyncio.sleep(delay / 2)
        yield {"type": "answer", "data": f"answered-by-W{worker_id}"}

    print(
        f"[W{worker_id}] booting (N={max_concurrency}, gateway={gateway_url})",
        flush=True,
    )

    from seeknal.cli import gateway as gateway_module

    with patch("seeknal.ask.gateway.server._run_agent_streaming", fake_streaming):
        try:
            await gateway_module._run_http_only_worker(
                project_path=Path("/tmp/demo"),
                gateway_url=gateway_url,
                api_token="demo",
                poll_timeout=2.0,
                max_concurrency=max_concurrency,
                shutdown_timeout=10.0,
            )
        except KeyboardInterrupt:
            print(f"[W{worker_id}] interrupted", flush=True)


if __name__ == "__main__":
    asyncio.run(main())

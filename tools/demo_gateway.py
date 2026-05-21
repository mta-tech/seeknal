"""Mini gateway hosting the REAL HttpWorkerBroker over HTTP.

Demo-only. No auth, no LLM, no project — just exposes the broker
endpoints the same way the production gateway does, so workers using
the real `_run_http_only_worker` code path can fan out work.

Uses Starlette (same dep as the production gateway) — no extra deps.
"""
from __future__ import annotations

import sys

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from seeknal.ask.gateway.http_worker import (
    HttpWorkerResult,
    http_worker_broker,
)


async def submit(request: Request) -> JSONResponse:
    payload = await request.json()
    print(
        f"[GW] submit session={payload['session_id']} q={payload['question']!r}",
        flush=True,
    )
    result = await http_worker_broker.enqueue_and_wait(
        session_id=payload["session_id"],
        question=payload["question"],
        project_path="/tmp/demo",
        timeout=120.0,
    )
    return JSONResponse({
        "answer": result.answer,
        "event_count": result.event_count,
        "error": result.error,
    })


async def work_stream(request: Request) -> Response:
    try:
        timeout = float(request.query_params.get("timeout", "30"))
    except (TypeError, ValueError):
        timeout = 30.0
    item = await http_worker_broker.claim_next(tenant_id="default", timeout=timeout)
    if item is None:
        return Response(status_code=204)
    print(
        f"[GW] dispatched work={item.work_id[:8]} session={item.session_id}",
        flush=True,
    )
    return JSONResponse({
        "work_id": item.work_id,
        "session_id": item.session_id,
        "question": item.question,
        "tenant_id": item.tenant_id,
        "provider": item.provider,
        "model": item.model,
    })


async def post_event(request: Request) -> JSONResponse:
    work_id = request.path_params["work_id"]
    event = await request.json()
    if event.get("type") in {"answer", "error", "done"}:
        print(
            f"[GW] event work={work_id[:8]} type={event.get('type')}",
            flush=True,
        )
    return JSONResponse({"ok": True})


async def post_complete(request: Request) -> JSONResponse:
    work_id = request.path_params["work_id"]
    body = await request.json()
    print(
        f"[GW] complete work={work_id[:8]} events={body.get('event_count')} "
        f"err={body.get('error')}",
        flush=True,
    )
    await http_worker_broker.complete(
        work_id=work_id,
        tenant_id="default",
        result=HttpWorkerResult(
            answer=body.get("answer", ""),
            event_count=body.get("event_count", 0),
            error=body.get("error"),
        ),
    )
    return JSONResponse({"ok": True})


app = Starlette(routes=[
    Route("/demo/submit", submit, methods=["POST"]),
    Route("/internal/worker/work-stream", work_stream, methods=["GET"]),
    Route("/internal/worker/work/{work_id}/event", post_event, methods=["POST"]),
    Route("/internal/worker/work/{work_id}/complete", post_complete, methods=["POST"]),
])


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    print(f"[GW] starting on http://127.0.0.1:{port}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")

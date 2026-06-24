"""ask_user callback for off-process HTTP workers.

The HTTP worker uses this callback instead of interactive_ask_user.
Flow:
  1. Worker pushes ask_user event to gateway → gateway broadcasts to client via SSE
  2. Worker long-polls GET /internal/worker/work/{id}/ask_user_wait
  3. Client POSTs /sessions/{id}/ask_user_response → gateway resolves rendezvous
  4. Long-poll returns answer → callback returns → agent resumes

Usage::

    callback = await make_http_worker_ask_user(
        gateway_url="https://gateway.example.com",
        work_id=item.work_id,
        session_id=item.session_id,
        auth_token="...",
        timeout=None,  # production: wait forever
    )
    agent, deps, *_ = create_agent(
        project_path,
        environment="gateway",
        ask_user_callback=callback,
    )
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4


async def make_http_worker_ask_user(
    gateway_url: str,
    work_id: str,
    session_id: str,
    auth_token: str,
    timeout: float | None = None,
):
    """Build an async ask_user callback for an HTTP worker.

    Parameters
    ----------
    gateway_url:
        Base URL of the seeknal gateway (no trailing slash).
    work_id:
        Work item ID received from GET /internal/worker/work-stream.
    session_id:
        Session ID associated with the work item.
    auth_token:
        Bearer token for authenticating internal worker endpoints.
    timeout:
        Seconds to wait for client response before auto-selecting recommended.
        None (default) = wait forever (production).
        N = auto-select recommended after N seconds (experimentation).
    """
    import httpx

    async def http_worker_ask_user(question: str, options: list[dict[str, Any]]) -> str:
        prompt_id = uuid4().hex
        event = {
            "type": "ask_user",
            "prompt_id": prompt_id,
            "session_id": session_id,
            "question": question,
            "options": options,
            "timeout_seconds": timeout,
        }
        headers = {"Authorization": f"Bearer {auth_token}"}
        async with httpx.AsyncClient() as client:
            # Push event to gateway → SSE → client
            await client.post(
                f"{gateway_url}/internal/worker/work/{work_id}/event",
                json=event,
                headers=headers,
                timeout=10.0,
            )
            # Long-poll until client responds or timeout fires
            params: dict[str, str] = {"prompt_id": prompt_id}
            if timeout is not None:
                params["timeout"] = str(timeout)
            resp = await client.get(
                f"{gateway_url}/internal/worker/work/{work_id}/ask_user_wait",
                params=params,
                headers=headers,
                timeout=(timeout + 10.0) if timeout is not None else None,
            )
            resp.raise_for_status()
            return str(resp.json().get("answer", ""))

    return http_worker_ask_user

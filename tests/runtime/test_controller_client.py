"""Tests for RunControllerClient error mapping and request dispatch."""

import json
import base64
import httpx
import pytest

from seeknal.runtime.controller_client import (
    NodeNotAuthorized,
    RunControllerClient,
    RunControllerUnavailable,
    RunRevoked,
    extract_jti,
)
from seeknal.runtime.run_grant import RunGrantError
from seeknal.runtime.run_plan import PlanNode


def _make_dummy_token(jti: str = "run-uuid-1234") -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"ES256","typ":"JWT","kid":"k1"}').rstrip(b"=").decode("ascii")
    payload = base64.urlsafe_b64encode(json.dumps({"jti": jti, "typ": "run"}).encode()).rstrip(b"=").decode("ascii")
    sig = base64.urlsafe_b64encode(b"0" * 64).rstrip(b"=").decode("ascii")
    return f"{header}.{payload}.{sig}"


@pytest.fixture
def test_node():
    return PlanNode(
        id="transform.daily_orders",
        kind="transform",
        reads=("warehouse.raw.orders",),
        writes=("warehouse.analytics.daily_orders",),
        connections=(),
    )


def test_node_start_success_200(test_node):
    token = _make_dummy_token("run-uuid-1234")
    captured_requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(200, json={"allowed": True})

    transport = httpx.MockTransport(handler)
    client = RunControllerClient(
        base_url="https://controller.internal",
        grant_token=token,
        transport=transport,
    )

    client.node_start(test_node)

    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.method == "POST"
    assert req.url == "https://controller.internal/api/runtime/v1/runs/run-uuid-1234/nodes/start"
    assert req.headers["Authorization"] == f"Bearer {token}"
    body = json.loads(req.content.decode("utf-8"))
    assert body["node_id"] == "transform.daily_orders"
    assert body["reads"] == ["warehouse.raw.orders"]
    assert body["writes"] == ["warehouse.analytics.daily_orders"]


def test_node_start_403_run_revoked(test_node):
    token = _make_dummy_token("run-uuid-1234")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"error": "run_revoked", "table": "warehouse.analytics.daily_orders"},
        )

    transport = httpx.MockTransport(handler)
    client = RunControllerClient(
        base_url="https://controller.internal",
        grant_token=token,
        transport=transport,
    )

    with pytest.raises(RunRevoked) as exc_info:
        client.node_start(test_node)
    assert exc_info.value.code == "run_revoked"
    assert exc_info.value.table == "warehouse.analytics.daily_orders"
    assert "revoked" in str(exc_info.value).lower()


def test_node_start_403_not_authorized(test_node):
    token = _make_dummy_token("run-uuid-1234")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"error": "not_authorized", "table": "warehouse.analytics.daily_orders"},
        )

    transport = httpx.MockTransport(handler)
    client = RunControllerClient(
        base_url="https://controller.internal",
        grant_token=token,
        transport=transport,
    )

    with pytest.raises(NodeNotAuthorized) as exc_info:
        client.node_start(test_node)
    assert exc_info.value.code == "not_authorized"
    assert exc_info.value.table == "warehouse.analytics.daily_orders"
    assert "not authorized" in str(exc_info.value).lower()


def test_node_start_401_grant_error(test_node):
    token = _make_dummy_token("run-uuid-1234")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "invalid_grant"})

    transport = httpx.MockTransport(handler)
    client = RunControllerClient(
        base_url="https://controller.internal",
        grant_token=token,
        transport=transport,
    )

    with pytest.raises(RunGrantError) as exc_info:
        client.node_start(test_node)
    assert "401" in str(exc_info.value)


def test_node_start_500_unavailable(test_node):
    token = _make_dummy_token("run-uuid-1234")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(handler)
    client = RunControllerClient(
        base_url="https://controller.internal",
        grant_token=token,
        transport=transport,
    )

    with pytest.raises(RunControllerUnavailable) as exc_info:
        client.node_start(test_node)
    assert "500" in str(exc_info.value)


def test_node_start_timeout(test_node):
    token = _make_dummy_token("run-uuid-1234")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Read timed out", request=request)

    transport = httpx.MockTransport(handler)
    client = RunControllerClient(
        base_url="https://controller.internal",
        grant_token=token,
        transport=transport,
    )

    with pytest.raises(RunControllerUnavailable) as exc_info:
        client.node_start(test_node)
    assert "timed out" in str(exc_info.value).lower()


def test_node_start_connection_error(test_node):
    token = _make_dummy_token("run-uuid-1234")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Failed to establish a new connection", request=request)

    transport = httpx.MockTransport(handler)
    client = RunControllerClient(
        base_url="https://controller.internal",
        grant_token=token,
        transport=transport,
    )

    with pytest.raises(RunControllerUnavailable) as exc_info:
        client.node_start(test_node)
    assert "network error" in str(exc_info.value).lower()


def test_extract_jti_invalid():
    with pytest.raises(RunGrantError):
        extract_jti("bad.token")

    with pytest.raises(RunGrantError):
        extract_jti("not-a-token")

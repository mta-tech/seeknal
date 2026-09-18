"""Authenticated loopback Iceberg REST fixture for mutation integration tests.

This is intentionally a small test server. Catalog semantics and metadata commits
are delegated to PyIceberg's SqlCatalog; data and metadata stay under ``tmp_path``.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import requests
import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from pyiceberg.catalog.rest import ConfigResponse, CreateTableRequest, TableResponse
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table import CommitTableRequest
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER


@dataclass
class RestFixtureState:
    backend: SqlCatalog
    token: str
    create_table_attempts: int = 0
    commit_attempts: int = 0
    unauthorized_requests: int = 0
    forced_commit_status: int | None = None
    concurrent_append: pa.Table | None = None
    received_authorization: list[str | None] = field(default_factory=list)
    staged_tables: dict[tuple[str, ...], Any] = field(default_factory=dict)


@dataclass
class RunningRestFixture:
    uri: str
    warehouse: str
    token: str
    backend: SqlCatalog
    state: RestFixtureState


def _model_json(model: Any) -> Any:
    return json.loads(model.model_dump_json(by_alias=True, exclude_none=True))


def _error(status: int, message: str, error_type: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"message": message, "type": error_type, "code": status}},
    )


def _table_response(table: Any) -> Any:
    return _model_json(
        TableResponse(
            metadata_location=table.metadata_location,
            metadata=table.metadata,
            config={},
        )
    )


def _namespace_tuple(namespace: str) -> tuple[str, ...]:
    return tuple(namespace.split("\x1f"))


def _make_app(state: RestFixtureState, warehouse: str) -> Starlette:
    async def require_bearer(request: Request, call_next: Any) -> Any:
        authorization = request.headers.get("authorization")
        state.received_authorization.append(authorization)
        if authorization != f"Bearer {state.token}":
            state.unauthorized_requests += 1
            return _error(401, "missing or invalid test token", "Unauthorized")
        return await call_next(request)

    async def config(request: Request) -> Response:
        return JSONResponse(
            _model_json(ConfigResponse(defaults={"warehouse": warehouse}, overrides={}))
        )

    async def namespace_exists(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        status = (
            204
            if _namespace_tuple(namespace) in state.backend.list_namespaces()
            else 404
        )
        return Response(status_code=status)

    async def create_namespace(request: Request) -> Any:
        payload = await request.json()
        try:
            state.backend.create_namespace(
                tuple(payload["namespace"]), payload.get("properties", {})
            )
        except Exception as exc:
            return _error(409, str(exc), type(exc).__name__)
        return JSONResponse(None)

    async def table_exists(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        table_name = request.path_params["table_name"]
        identifier = (*_namespace_tuple(namespace), table_name)
        status = 204 if state.backend.table_exists(identifier) else 404
        return Response(status_code=status)

    async def create_table(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        state.create_table_attempts += 1
        request_payload = await request.json()
        request_payload.setdefault("location", None)
        payload = CreateTableRequest.model_validate(request_payload)
        try:
            create_kwargs = {
                "schema": payload.table_schema,
                "location": payload.location,
                "partition_spec": payload.partition_spec or PartitionSpec(),
                "sort_order": payload.write_order or UNSORTED_SORT_ORDER,
                "properties": payload.properties,
            }
            identifier = (*_namespace_tuple(namespace), payload.name)
            if payload.stage_create:
                transaction = state.backend.create_table_transaction(
                    identifier, **create_kwargs
                )
                table = transaction._table
                state.staged_tables[identifier] = table
            else:
                table = state.backend.create_table(identifier, **create_kwargs)
        except Exception as exc:
            return _error(409, str(exc), type(exc).__name__)
        return JSONResponse(_table_response(table))

    async def load_table(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        table_name = request.path_params["table_name"]
        try:
            return JSONResponse(
                _table_response(
                    state.backend.load_table((*_namespace_tuple(namespace), table_name))
                )
            )
        except Exception as exc:
            return _error(404, str(exc), type(exc).__name__)

    async def commit_table(request: Request) -> Response:
        namespace = request.path_params["namespace"]
        table_name = request.path_params["table_name"]
        state.commit_attempts += 1
        payload = CommitTableRequest.model_validate(await request.json())
        identifier = (*_namespace_tuple(namespace), table_name)

        if state.forced_commit_status is not None:
            status = state.forced_commit_status
            return _error(status, "forced commit response", "ForcedCommitError")

        try:
            try:
                stale_table = state.backend.load_table(identifier)
            except NoSuchTableError:
                stale_table = state.staged_tables[identifier]
            if state.concurrent_append is not None:
                competing_table = state.backend.load_table(identifier)
                competing_table.append(state.concurrent_append)
                state.concurrent_append = None
            response = state.backend.commit_table(
                stale_table,
                requirements=tuple(payload.requirements),
                updates=tuple(payload.updates),
            )
            state.staged_tables.pop(identifier, None)
        except CommitFailedException as exc:
            return _error(409, str(exc), type(exc).__name__)
        return JSONResponse(_model_json(response))

    return Starlette(
        routes=[
            Route("/v1/config", config, methods=["GET"]),
            Route("/v1/namespaces/{namespace}", namespace_exists, methods=["HEAD"]),
            Route("/v1/namespaces", create_namespace, methods=["POST"]),
            Route(
                "/v1/namespaces/{namespace}/tables/{table_name}",
                table_exists,
                methods=["HEAD"],
            ),
            Route("/v1/namespaces/{namespace}/tables", create_table, methods=["POST"]),
            Route(
                "/v1/namespaces/{namespace}/tables/{table_name}",
                load_table,
                methods=["GET"],
            ),
            Route(
                "/v1/namespaces/{namespace}/tables/{table_name}",
                commit_table,
                methods=["POST"],
            ),
        ],
        middleware=[Middleware(BaseHTTPMiddleware, dispatch=require_bearer)],
    )


def _listening_socket() -> tuple[socket.socket, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    return sock, sock.getsockname()[1]


@contextmanager
def run_rest_fixture(tmp_path: Path) -> Iterator[RunningRestFixture]:
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir()
    warehouse = f"file://{warehouse_path}"
    token = "local-integration-token"
    backend = SqlCatalog(
        "integration_backend",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=warehouse,
    )
    state = RestFixtureState(backend=backend, token=token)
    app = _make_app(state, warehouse)
    sock, port = _listening_socket()
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off"))
    thread = threading.Thread(
        target=server.run,
        kwargs={"sockets": [sock]},
        daemon=True,
    )
    thread.start()
    uri = f"http://127.0.0.1:{port}"

    for _ in range(100):
        try:
            response = requests.get(
                f"{uri}/v1/config",
                headers={"Authorization": f"Bearer {token}"},
                timeout=0.1,
            )
            if response.status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(0.01)
    else:
        server.should_exit = True
        thread.join(timeout=5)
        sock.close()
        raise RuntimeError("local Iceberg REST fixture did not start")

    try:
        yield RunningRestFixture(
            uri=uri,
            warehouse=warehouse,
            token=token,
            backend=backend,
            state=state,
        )
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        sock.close()
        if thread.is_alive():
            raise RuntimeError("local Iceberg REST fixture did not stop")

"""Opt-in public-data smoke test; raw data stays outside the repository.

Run with PYTHONPATH=src:. .venv/bin/python tools/validate_dependency_real_data.py
  --csv /tmp/penguins.csv --output /tmp/receipt.json

Requires a disposable S3 service on 127.0.0.1:19240 and Temporal dev server
on 127.0.0.1:19243. Uses test-only credentials, an isolated S3 prefix, the
repository's REST catalog fixture, and a deterministic local model boundary.
No external model provider is contacted. This is real observed penguin data;
batch splitting and version columns are test metadata, not observed revisions.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import tempfile
import uuid
from collections import Counter
from importlib.metadata import version
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pyarrow as pa
import s3fs
from aiohttp import web
import pydantic_ai.models
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from temporalio.client import Client
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from seeknal.ask.agents.agent import create_agent
from seeknal.ask.agents.tools._context import get_tool_context
from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.gateway.temporal import (
    AgentWorkflow,
    AgentWorkflowInput,
    run_agent_activity,
)
from seeknal.workflow.materialization.config import CatalogConfig
from seeknal.workflow.materialization.operations import write_to_iceberg
from tests.integration.iceberg_rest_fixture import run_rest_fixture

SOURCE = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv"
SHA256 = "e07636bd8af74260099ea2f8678e2eabbf35def579940cc76f67061ee16c06c1"
ENDPOINT = "http://127.0.0.1:19240"


async def agent_and_callback(project: Path, frame: pd.DataFrame) -> dict:
    target = project / "target" / "intermediate"
    target.mkdir(parents=True)
    frame.to_parquet(target / "source_penguins.parquet", index=False)
    pydantic_ai.models.ALLOW_MODEL_REQUESTS = False
    agent, deps, _, _ = create_agent(
        project,
        provider="openai",
        model="gpt-4o-mini",
        api_key="unused-local-test",
    )
    calls = []

    def model(messages, info):
        results = [
            part
            for message in messages
            for part in message.parts
            if isinstance(part, ToolReturnPart) and part.tool_name == "execute_sql"
        ]
        if results:
            return ModelResponse(parts=[TextPart(str(results[-1].content))])
        calls.append("execute_sql")
        return ModelResponse(
            parts=[
                ToolCallPart(
                    "execute_sql",
                    {
                        "sql": "SELECT species, COUNT(*) AS n FROM source_penguins GROUP BY species ORDER BY species"
                    },
                )
            ]
        )

    with agent.override(model=FunctionModel(model)):
        result = await agent.run("Count the penguins by species", deps=deps)
    answer = str(result.output)
    assert calls == ["execute_sql"], calls
    for species, count in Counter(frame.species).items():
        assert species in answer and str(count) in answer, answer
    # Check the same real SQL tool directly, independently of the local model.
    assert "344" in execute_sql("SELECT COUNT(*) AS n FROM source_penguins")
    get_tool_context().repl.conn.close()

    received = []

    async def callback(request):
        assert request.headers.get("Authorization") == "Bearer local-callback-test"
        assert request.headers.get("X-Tenant-ID") == "public-validation"
        received.append(await request.json())
        return web.Response(status=204)

    app = web.Application()
    app.router.add_post("/internal/events/public-data/publish", callback)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]

    async def local_stream(*args, **kwargs):
        # Only the model/stream boundary is deterministic. Workflow, activity,
        # aiohttp client, HTTP server and callback payloads run for real.
        yield {"type": "answer", "data": answer}
        yield {"type": "done"}

    try:
        client = await Client.connect("127.0.0.1:19243")
        queue = "public-validation-" + uuid.uuid4().hex
        with patch("seeknal.ask.gateway.server._run_agent_streaming", local_stream):
            async with Worker(
                client,
                task_queue=queue,
                workflows=[AgentWorkflow],
                activities=[run_agent_activity],
                workflow_runner=UnsandboxedWorkflowRunner(),
            ):
                output = await client.execute_workflow(
                    AgentWorkflow.run,
                    AgentWorkflowInput(
                        session_id="public-data",
                        question="public penguin counts",
                        project_path=str(project),
                        tenant_id="public-validation",
                        callback_url=f"http://127.0.0.1:{port}",
                        callback_auth_token="local-callback-test",
                    ),
                    id=queue,
                    task_queue=queue,
                )
        assert output.error is None and output.answer == answer and output.event_count == 2
        assert [event["type"] for event in received] == ["answer", "done"]
        assert received[0]["data"] == answer
        return {
            "species_counts": dict(Counter(frame.species)),
            "agent_sql_tool_calls": len(calls),
            "model": "local FunctionModel; no provider requests",
            "temporal": "real dev server, workflow and activity; deterministic stream boundary",
            "callback_events": len(received),
        }
    finally:
        await runner.cleanup()


def validate(csv_path: Path) -> dict:
    raw = csv_path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == SHA256, "Unexpected public dataset content"
    frame = pd.read_csv(csv_path)
    assert len(frame) == 344 and len(frame.columns) == 7
    prefix = "validation-" + uuid.uuid4().hex
    fs = s3fs.S3FileSystem(
        key="local",
        secret="local",
        client_kwargs={"endpoint_url": ENDPOINT, "region_name": "us-east-1"},
    )
    bucket = "seeknal-public-validation"
    fs.makedirs(bucket, exist_ok=True)
    root = f"{bucket}/{prefix}"
    try:
        fs.pipe(f"{root}/penguins.csv", raw)
        assert fs.cat(f"{root}/penguins.csv") == raw
        with fs.open(f"{root}/penguins.csv", "rb") as source:
            observed = pd.read_csv(source)
        assert observed.equals(frame)
        frame = observed
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            properties = {
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "s3.endpoint": ENDPOINT,
                "s3.region": "us-east-1",
                "s3.access-key-id": "local",
                "s3.secret-access-key": "local",
            }
            with (
                run_rest_fixture(
                    directory, warehouse=f"s3://{root}/warehouse", properties=properties
                ) as fixture,
                duckdb.connect() as con,
            ):
                config = CatalogConfig(
                    uri=fixture.uri, warehouse=fixture.warehouse, bearer_token=fixture.token
                )
                table_data = pa.Table.from_pandas(frame, preserve_index=False)
                table_data = table_data.append_column(
                    "observation_id", pa.array(range(344), pa.int64())
                )
                table_data = table_data.append_column(
                    "batch_version", pa.array([1] * 344, pa.int64())
                )

                def write(batch):
                    con.register("incoming", batch)
                    return write_to_iceberg(
                        con,
                        "local_rest",
                        "public.penguins",
                        "incoming",
                        "upsert",
                        unique_keys=["observation_id"],
                        catalog_config=config,
                        when_matched_update_if="source.batch_version > target.batch_version",
                        compare_columns=["batch_version"],
                    )

                initial = write(table_data.slice(0, 300))
                replay = write(table_data)
                assert initial.inserted_row_count == 300
                assert (replay.inserted_row_count, replay.skipped_by_condition) == (44, 300)
                newer = table_data.slice(0, 20).set_column(
                    table_data.schema.get_field_index("batch_version"),
                    "batch_version",
                    pa.array([2] * 20, pa.int64()),
                )
                updated = write(newer)
                assert updated.updated_row_count == 20
                table = fixture.backend.load_table(("public", "penguins"))
                before = table.current_snapshot().snapshot_id
                rejected = write(table_data.slice(0, 20))
                table.refresh()
                assert rejected.row_count == 0 and rejected.skipped_by_condition == 20
                assert table.current_snapshot().snapshot_id == before
                expected = pa.concat_tables([newer, table_data.slice(20)]).sort_by("observation_id")
                actual = table.scan().to_arrow().sort_by("observation_id")
                assert actual.cast(expected.schema).equals(expected)
                assert type(table.io).__name__ == "FsspecFileIO"
                # A separate engine reads actual Iceberg metadata and S3 Parquet.
                con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg")
                con.execute(
                    "SET s3_endpoint='127.0.0.1:19240'; SET s3_use_ssl=false; SET s3_url_style='path'"
                )
                con.execute("SET s3_access_key_id='local'; SET s3_secret_access_key='local'")
                independent = con.execute(
                    "SELECT * FROM iceberg_scan(?) ORDER BY observation_id",
                    [table.metadata_location],
                ).fetch_arrow_table()
                assert independent.cast(expected.schema).equals(expected)
                objects = len(fs.find(root))
            agent_receipt = asyncio.run(agent_and_callback(directory / "project", frame))
        return {
            "source": SOURCE,
            "sha256": SHA256,
            "bytes": len(raw),
            "rows": len(frame),
            "columns": len(frame.columns),
            "s3_objects": objects,
            "iceberg": {
                "initial_inserts": 300,
                "new_inserts": 44,
                "equal_skips": 300,
                "newer_updates": 20,
                "older_skips": 20,
                "noop_snapshot_unchanged": True,
                "readers": ["PyIceberg FsspecFileIO", "DuckDB iceberg_scan"],
                "exact_rows_equal": True,
            },
            "agent_and_callback": agent_receipt,
            "versions": {
                name: version(name)
                for name in [
                    "s3fs",
                    "fsspec",
                    "aiohttp",
                    "multidict",
                    "pydantic-ai-slim",
                    "Mako",
                    "apprise",
                    "duckdb",
                    "pyiceberg",
                ]
            },
        }
    finally:
        fs.rm(root, recursive=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Prevent ambient deployment credentials/endpoints from affecting a local test.
    test_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(
            ("AWS_", "S3_", "LAKEKEEPER_", "ICEBERG_", "TEMPORAL_", "SEEKNAL_", "IBA_")
        )
    }
    test_env.update(
        AWS_REQUEST_CHECKSUM_CALCULATION="when_required",
        AWS_RESPONSE_CHECKSUM_VALIDATION="when_required",
        NO_PROXY="127.0.0.1,localhost",
    )
    with patch.dict(os.environ, test_env, clear=True):
        receipt = validate(args.csv)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))

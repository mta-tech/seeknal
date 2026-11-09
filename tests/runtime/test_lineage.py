"""Tests for seeknal.runtime.lineage module."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import httpx
import pytest

from seeknal.runtime.lineage import (
    COLUMN_LINEAGE_DATASET_FACET_SCHEMA_URL,
    DATASET_VERSION_DATASET_FACET_SCHEMA_URL,
    ERROR_MESSAGE_RUN_FACET_SCHEMA_URL,
    JOB_TYPE_JOB_FACET_SCHEMA_URL,
    OPENLINEAGE_SCHEMA_URL,
    PARENT_RUN_FACET_SCHEMA_URL,
    PRODUCER,
    RUN_NAMESPACE,
    SCHEMA_DATASET_FACET_SCHEMA_URL,
    LineageEmitter,
    build_node_event,
    column_lineage_for_sql,
    dataset_for_table,
)
from seeknal.runtime.run_plan import PlanNode


class DummyGrant:
    def __init__(self, jti: str = "run-jti-123", ws: str = "ws-prod") -> None:
        self.jti = jti
        self.ws = ws


@pytest.fixture
def test_node() -> PlanNode:
    return PlanNode(
        id="transform.daily_orders",
        kind="transform",
        reads=("warehouse.raw.orders",),
        writes=("warehouse.analytics.daily_orders",),
        connections=(),
    )


def test_constants():
    assert OPENLINEAGE_SCHEMA_URL == "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
    assert PRODUCER == "https://github.com/mta-tech/seeknal/runtime"
    assert RUN_NAMESPACE == uuid.UUID("6f1d0c6e-6a63-5c9b-9a3e-2c0c5d3a7a10")
    assert "ParentRunFacet" in PARENT_RUN_FACET_SCHEMA_URL
    assert "JobTypeJobFacet" in JOB_TYPE_JOB_FACET_SCHEMA_URL
    assert "SchemaDatasetFacet" in SCHEMA_DATASET_FACET_SCHEMA_URL
    assert "DatasetVersionDatasetFacet" in DATASET_VERSION_DATASET_FACET_SCHEMA_URL
    assert "ColumnLineageDatasetFacet" in COLUMN_LINEAGE_DATASET_FACET_SCHEMA_URL
    assert "ErrorMessageRunFacet" in ERROR_MESSAGE_RUN_FACET_SCHEMA_URL


def test_deterministic_run_id_fixed_input():
    grant = DummyGrant(jti="run-1234")
    node = PlanNode(id="node-test", kind="transform", reads=(), writes=(), connections=())

    event1 = build_node_event(
        "START",
        grant=grant,
        node=node,
        project="proj",
        lakekeeper_uri="http://lakekeeper:8181",
    )
    # Exact known UUIDv5 value from (RUN_NAMESPACE, "run-1234:node-test")
    expected_uuid = str(uuid.uuid5(RUN_NAMESPACE, "run-1234:node-test"))
    assert expected_uuid == "2e7996e8-1395-59fc-b5a5-939282c64092"
    assert event1["run"]["runId"] == "2e7996e8-1395-59fc-b5a5-939282c64092"

    event2 = build_node_event(
        "COMPLETE",
        grant=grant,
        node=node,
        project="proj",
        lakekeeper_uri="http://lakekeeper:8181",
    )
    assert event2["run"]["runId"] == event1["run"]["runId"]


def test_dataset_for_table_deterministic():
    # URI with port
    res = dataset_for_table("warehouse.raw.orders", lakekeeper_uri="http://lakekeeper:8181")
    assert res == {
        "namespace": "iceberg://lakekeeper:8181/warehouse",
        "name": "raw.orders",
    }

    # URI with userinfo and path
    res2 = dataset_for_table("warehouse.analytics.summary", lakekeeper_uri="http://user:pass@LAKEKEEPER:8181/iceberg-catalog")
    assert res2 == {
        "namespace": "iceberg://lakekeeper:8181/warehouse",
        "name": "analytics.summary",
    }

    # URI without port
    res3 = dataset_for_table("lakekeeper.analytics.summary", lakekeeper_uri="https://LakeKeeper.Example.COM")
    assert res3 == {
        "namespace": "iceberg://lakekeeper.example.com/lakekeeper",
        "name": "analytics.summary",
    }

    # Bare host:port
    res4 = dataset_for_table("catalog_wh.ns1.tbl", lakekeeper_uri="lakekeeper:9000")
    assert res4 == {
        "namespace": "iceberg://lakekeeper:9000/catalog_wh",
        "name": "ns1.tbl",
    }


def test_dataset_for_table_invalid_raises():
    with pytest.raises(ValueError, match="3-part"):
        dataset_for_table("raw.orders", lakekeeper_uri="http://lakekeeper:8181")

    with pytest.raises(ValueError, match="3-part"):
        dataset_for_table("orders", lakekeeper_uri="http://lakekeeper:8181")

    with pytest.raises(ValueError, match="3-part"):
        dataset_for_table("a.b.c.d", lakekeeper_uri="http://lakekeeper:8181")

    with pytest.raises(ValueError, match="3-part"):
        dataset_for_table("", lakekeeper_uri="http://lakekeeper:8181")


def test_golden_start_and_complete_events_json_roundtrip(test_node):
    grant = DummyGrant(jti="018e6962-4f3b-7489-b2c8-89c0c1b48b9a", ws="ws-analytics")
    fixed_time = "2026-03-30T10:00:00.000000Z"

    event_start = build_node_event(
        "START",
        grant=grant,
        node=test_node,
        project="ecommerce",
        lakekeeper_uri="http://lakekeeper:8181",
        event_time=fixed_time,
    )

    # Validate JSON round-trip
    dumped = json.dumps(event_start)
    loaded = json.loads(dumped)
    assert loaded == event_start

    assert event_start["eventType"] == "START"
    assert event_start["eventTime"] == fixed_time
    assert event_start["producer"] == PRODUCER
    assert event_start["schemaURL"] == OPENLINEAGE_SCHEMA_URL

    # Job
    assert event_start["job"]["namespace"] == "atlas://ws-analytics/ecommerce"
    assert event_start["job"]["name"] == "transform.daily_orders"
    job_type_facet = event_start["job"]["facets"]["jobType"]
    assert job_type_facet["processingType"] == "BATCH"
    assert job_type_facet["integration"] == "SEEKNAL"
    assert job_type_facet["jobType"] == "transform"
    assert job_type_facet["_producer"] == PRODUCER
    assert job_type_facet["_schemaURL"] == JOB_TYPE_JOB_FACET_SCHEMA_URL

    # Parent facet
    parent_facet = event_start["run"]["facets"]["parent"]
    assert parent_facet["run"]["runId"] == "018e6962-4f3b-7489-b2c8-89c0c1b48b9a"
    assert parent_facet["job"]["namespace"] == "atlas://ws-analytics/ecommerce"
    assert parent_facet["job"]["name"] == "ecommerce"
    assert parent_facet["_producer"] == PRODUCER
    assert parent_facet["_schemaURL"] == PARENT_RUN_FACET_SCHEMA_URL

    # Inputs and outputs
    assert len(event_start["inputs"]) == 1
    assert event_start["inputs"][0]["namespace"] == "iceberg://lakekeeper:8181/warehouse"
    assert event_start["inputs"][0]["name"] == "raw.orders"

    assert len(event_start["outputs"]) == 1
    assert event_start["outputs"][0]["namespace"] == "iceberg://lakekeeper:8181/warehouse"
    assert event_start["outputs"][0]["name"] == "analytics.daily_orders"

    # COMPLETE event
    event_complete = build_node_event(
        "COMPLETE",
        grant=grant,
        node=test_node,
        project="ecommerce",
        lakekeeper_uri="http://lakekeeper:8181",
        event_time=fixed_time,
    )
    assert json.loads(json.dumps(event_complete)) == event_complete
    assert event_complete["eventType"] == "COMPLETE"
    assert event_complete["run"]["runId"] == event_start["run"]["runId"]


def test_schema_version_and_column_lineage_facets(test_node):
    grant = DummyGrant()
    schema = [
        {"name": "order_date", "type": "date"},
        {"name": "total_amount", "type": "double"},
    ]
    snapshot_ids = {
        "warehouse.raw.orders": 111122223333,
        "warehouse.analytics.daily_orders": 999988887777,
    }
    col_lineage = {
        "total_amount": [("warehouse.raw.orders", "amount")],
        "order_date": [("warehouse.raw.orders", "created_at")],
    }

    event = build_node_event(
        "COMPLETE",
        grant=grant,
        node=test_node,
        project="analytics",
        lakekeeper_uri="http://lakekeeper:8181",
        schema=schema,
        snapshot_ids=snapshot_ids,
        column_lineage=col_lineage,
    )

    # Input dataset version
    in_ds = event["inputs"][0]
    assert in_ds["facets"]["version"]["datasetVersion"] == "111122223333"
    assert in_ds["facets"]["version"]["_producer"] == PRODUCER
    assert in_ds["facets"]["version"]["_schemaURL"] == DATASET_VERSION_DATASET_FACET_SCHEMA_URL

    # Output dataset facets
    out_ds = event["outputs"][0]
    assert out_ds["facets"]["version"]["datasetVersion"] == "999988887777"
    assert out_ds["facets"]["version"]["_schemaURL"] == DATASET_VERSION_DATASET_FACET_SCHEMA_URL

    assert out_ds["facets"]["schema"]["fields"] == schema
    assert out_ds["facets"]["schema"]["_producer"] == PRODUCER
    assert out_ds["facets"]["schema"]["_schemaURL"] == SCHEMA_DATASET_FACET_SCHEMA_URL

    col_facet = out_ds["facets"]["columnLineage"]
    assert col_facet["_producer"] == PRODUCER
    assert col_facet["_schemaURL"] == COLUMN_LINEAGE_DATASET_FACET_SCHEMA_URL
    assert "total_amount" in col_facet["fields"]
    assert col_facet["fields"]["total_amount"]["inputFields"] == [
        {
            "namespace": "iceberg://lakekeeper:8181/warehouse",
            "name": "raw.orders",
            "field": "amount",
        }
    ]
    assert col_facet["fields"]["order_date"]["inputFields"] == [
        {
            "namespace": "iceberg://lakekeeper:8181/warehouse",
            "name": "raw.orders",
            "field": "created_at",
        }
    ]


def test_schema_normalization_forms_and_rejections(test_node):
    grant = DummyGrant()

    # 1. Tuple form schema: (name, type)
    tuple_schema = [("d", "date"), ("n", "bigint")]
    event_tuple = build_node_event(
        "COMPLETE",
        grant=grant,
        node=test_node,
        project="analytics",
        lakekeeper_uri="http://lakekeeper:8181",
        schema=tuple_schema,
    )
    assert event_tuple["outputs"][0]["facets"]["schema"]["fields"] == [
        {"name": "d", "type": "date"},
        {"name": "n", "type": "bigint"},
    ]

    # 2-list form schema: [name, type]
    list_schema = [["d", "date"], ["n", "bigint"]]
    event_list = build_node_event(
        "COMPLETE",
        grant=grant,
        node=test_node,
        project="analytics",
        lakekeeper_uri="http://lakekeeper:8181",
        schema=list_schema,
    )
    assert event_list["outputs"][0]["facets"]["schema"]["fields"] == [
        {"name": "d", "type": "date"},
        {"name": "n", "type": "bigint"},
    ]

    # 2. Dict-item form schema with extra fields and description
    dict_item_schema = [
        {"name": "d", "type": "date", "extra_attr": 42},
        {"name": "n", "type": "bigint", "description": "counter column", "ignored": "yes"},
    ]
    event_dict_items = build_node_event(
        "COMPLETE",
        grant=grant,
        node=test_node,
        project="analytics",
        lakekeeper_uri="http://lakekeeper:8181",
        schema=dict_item_schema,
    )
    assert event_dict_items["outputs"][0]["facets"]["schema"]["fields"] == [
        {"name": "d", "type": "date"},
        {"name": "n", "type": "bigint", "description": "counter column"},
    ]

    # 3. Dict schema: {"name": "type"}
    dict_schema = {"d": "date", "n": "bigint"}
    event_dict = build_node_event(
        "COMPLETE",
        grant=grant,
        node=test_node,
        project="analytics",
        lakekeeper_uri="http://lakekeeper:8181",
        schema=dict_schema,
    )
    assert event_dict["outputs"][0]["facets"]["schema"]["fields"] == [
        {"name": "d", "type": "date"},
        {"name": "n", "type": "bigint"},
    ]

    # 4. Rejected malformed items (naming bad item in ValueError)
    # 3-tuple
    with pytest.raises(ValueError, match=r"\('a', 'b', 'c'\)"):
        build_node_event(
            "COMPLETE",
            grant=grant,
            node=test_node,
            project="analytics",
            lakekeeper_uri="http://lakekeeper:8181",
            schema=[("a", "b", "c")],
        )

    # String item in list
    with pytest.raises(ValueError, match="not_a_tuple_or_dict"):
        build_node_event(
            "COMPLETE",
            grant=grant,
            node=test_node,
            project="analytics",
            lakekeeper_uri="http://lakekeeper:8181",
            schema=["not_a_tuple_or_dict"],
        )

    # Mapping with empty string name
    with pytest.raises(ValueError, match="name"):
        build_node_event(
            "COMPLETE",
            grant=grant,
            node=test_node,
            project="analytics",
            lakekeeper_uri="http://lakekeeper:8181",
            schema=[{"name": "", "type": "int"}],
        )

    # Mapping with non-string type
    with pytest.raises(ValueError, match="type"):
        build_node_event(
            "COMPLETE",
            grant=grant,
            node=test_node,
            project="analytics",
            lakekeeper_uri="http://lakekeeper:8181",
            schema=[{"name": "col1", "type": 123}],
        )

    # Tuple with non-string column name or type
    with pytest.raises(ValueError, match="123"):
        build_node_event(
            "COMPLETE",
            grant=grant,
            node=test_node,
            project="analytics",
            lakekeeper_uri="http://lakekeeper:8181",
            schema=[(123, "date")],
        )

    with pytest.raises(ValueError, match="123"):
        build_node_event(
            "COMPLETE",
            grant=grant,
            node=test_node,
            project="analytics",
            lakekeeper_uri="http://lakekeeper:8181",
            schema=[("col", 123)],
        )


def test_fail_redaction_and_truncation(test_node):
    grant = DummyGrant()

    # Redaction test
    sensitive_msg = (
        "Execution failed: password=superSecretPass123&other=val and "
        "TOKEN=mySecretBearerToken456 with secret=topSecretKey999."
    )
    event_fail = build_node_event(
        "FAIL",
        grant=grant,
        node=test_node,
        project="analytics",
        lakekeeper_uri="http://lakekeeper:8181",
        error=sensitive_msg,
    )

    err_facet = event_fail["run"]["facets"]["errorMessage"]
    assert err_facet["_producer"] == PRODUCER
    assert err_facet["_schemaURL"] == ERROR_MESSAGE_RUN_FACET_SCHEMA_URL
    assert err_facet["programmingLanguage"] == "python"

    msg = err_facet["message"]
    assert "superSecretPass123" not in msg
    assert "mySecretBearerToken456" not in msg
    assert "topSecretKey999" not in msg
    assert "password=[REDACTED]" in msg
    assert "TOKEN=[REDACTED]" in msg
    assert "secret=[REDACTED]" in msg

    # Truncation test (> 2000 chars)
    huge_msg = "Error: " + ("a" * 2500)
    event_fail_huge = build_node_event(
        "FAIL",
        grant=grant,
        node=test_node,
        project="analytics",
        lakekeeper_uri="http://lakekeeper:8181",
        error=huge_msg,
    )
    huge_err_msg = event_fail_huge["run"]["facets"]["errorMessage"]["message"]
    assert len(huge_err_msg) == 2000


def test_column_lineage_for_sql_valid_and_garbage():
    sql = "SELECT a.x AS y, b.z FROM t1 a JOIN t2 b ON a.id=b.id"

    # Input tables by base table names
    res1 = column_lineage_for_sql(
        sql,
        input_tables={"t1": "catalog.db.t1", "t2": "catalog.db.t2"},
    )
    assert res1 == {
        "y": [("catalog.db.t1", "x")],
        "z": [("catalog.db.t2", "z")],
    }

    # Input tables by aliases
    res2 = column_lineage_for_sql(
        sql,
        input_tables={"a": "catalog.db.t1", "b": "catalog.db.t2"},
    )
    assert res2 == {
        "y": [("catalog.db.t1", "x")],
        "z": [("catalog.db.t2", "z")],
    }

    # Single table query without table qualifier on column
    single_sql = "SELECT order_id AS id, amount FROM orders"
    res_single = column_lineage_for_sql(
        single_sql,
        input_tables={"orders": "catalog.raw.orders"},
    )
    assert res_single == {
        "id": [("catalog.raw.orders", "order_id")],
        "amount": [("catalog.raw.orders", "amount")],
    }

    # Garbage / unparseable SQL returns {} and does not raise
    assert column_lineage_for_sql("NOT A VALID SQL STATEMENT %$#@", input_tables={"t1": "catalog.db.t1"}) == {}
    assert column_lineage_for_sql("", input_tables={"t1": "catalog.db.t1"}) == {}
    assert column_lineage_for_sql("   ", input_tables={"t1": "catalog.db.t1"}) == {}


def test_emitter_success():
    captured_requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(202, json={"status": "accepted"})

    transport = httpx.MockTransport(handler)
    emitter = LineageEmitter(
        base_url="https://catalog.internal",
        grant_token="grant-token-xyz",
        transport=transport,
    )

    event = {"eventType": "START", "test": True}
    emitter.emit(event)

    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.method == "POST"
    assert req.url == "https://catalog.internal/api/runtime/v1/lineage"
    assert req.headers["Authorization"] == "Bearer grant-token-xyz"
    assert json.loads(req.content.decode("utf-8")) == event

    assert emitter.stats() == {
        "sent": 1,
        "buffered": 0,
        "dropped": 0,
        "failed_attempts": 0,
    }


def test_emitter_500_buffered_and_flush_success():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(500, text="Internal Server Error")
        return httpx.Response(200, json={"status": "ok"})

    transport = httpx.MockTransport(handler)
    emitter = LineageEmitter(
        base_url="https://catalog.internal",
        grant_token="grant-token-xyz",
        transport=transport,
    )

    event = {"eventType": "START", "node": "test"}
    emitter.emit(event)

    # Failed on emit -> buffered
    assert emitter.stats() == {
        "sent": 0,
        "buffered": 1,
        "dropped": 0,
        "failed_attempts": 1,
    }

    # Flush -> succeeds
    emitter.flush()
    assert emitter.stats() == {
        "sent": 1,
        "buffered": 0,
        "dropped": 0,
        "failed_attempts": 1,
    }


def test_emitter_buffer_overflow_dropped():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="Service Unavailable")

    transport = httpx.MockTransport(handler)
    emitter = LineageEmitter(
        base_url="https://catalog.internal",
        grant_token="grant-token-xyz",
        max_buffer=2,
        transport=transport,
    )

    emitter.emit({"id": 1})
    assert emitter.stats() == {"sent": 0, "buffered": 1, "dropped": 0, "failed_attempts": 1}

    emitter.emit({"id": 2})
    assert emitter.stats() == {"sent": 0, "buffered": 2, "dropped": 0, "failed_attempts": 2}

    # 3rd event exceeds max_buffer=2: oldest (id: 1) is dropped
    emitter.emit({"id": 3})
    assert emitter.stats() == {"sent": 0, "buffered": 2, "dropped": 1, "failed_attempts": 3}
    assert emitter._buffer == [{"id": 2}, {"id": 3}]


def test_emitter_network_error_buffered():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused", request=request)

    transport = httpx.MockTransport(handler)
    emitter = LineageEmitter(
        base_url="https://catalog.internal",
        grant_token="grant-token-xyz",
        transport=transport,
    )

    # Must not raise
    emitter.emit({"eventType": "START"})
    assert emitter.stats() == {
        "sent": 0,
        "buffered": 1,
        "dropped": 0,
        "failed_attempts": 1,
    }


def test_emitter_token_not_in_log_records(caplog):
    secret_token = "secret-super-sensitive-token-12345"

    def handler(request: httpx.Request) -> httpx.Response:
        # Simulate an error response that might try to reflect headers or fail
        return httpx.Response(500, text=f"Error processing Bearer {secret_token}")

    transport = httpx.MockTransport(handler)
    emitter = LineageEmitter(
        base_url="https://catalog.internal",
        grant_token=secret_token,
        transport=transport,
    )

    with caplog.at_level(logging.WARNING):
        emitter.emit({"eventType": "FAIL"})

    assert emitter.stats()["buffered"] == 1
    # Verify the secret token is never present in log records
    for record in caplog.records:
        assert secret_token not in record.message
        assert secret_token not in record.getMessage()
    assert secret_token not in caplog.text

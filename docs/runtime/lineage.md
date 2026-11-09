# Atlas Runtime OpenLineage Event Specification and Delivery

This document defines the OpenLineage 1.x / 2.0.2 event shape, naming conventions, facet schemas, delivery semantics, and Catalog endpoint contract for governed runs in Seeknal Atlas Runtime.

---

## 1. Context and Architecture

During governed execution in Atlas Runtime:
1. Each pipeline node transition produces OpenLineage `RunEvent` records (`START`, `COMPLETE`, or `FAIL`).
2. The runtime job pod emits lineage directly to the **Atlas Catalog** via `POST /api/runtime/v1/lineage`, authenticating with its assigned run grant token.
3. Atlas Catalog validates and forwards these lineage events to downstream metadata services (such as DataHub). The runtime job pod never communicates directly with DataHub and holds no DataHub credentials.
4. Lineage collection is best-effort: failure to transmit lineage never halts pipeline execution, but events are buffered in memory to prevent silent loss.

---

## 2. OpenLineage RunEvent Shape

Every emitted event conforms to the OpenLineage specification (`OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"`):

```json
{
  "eventType": "START | COMPLETE | FAIL",
  "eventTime": "2026-03-30T10:00:00.000000Z",
  "run": {
    "runId": "481a5391-f925-5e69-9556-96ea9d1b0d26",
    "facets": {
      "parent": {
        "_producer": "https://github.com/mta-tech/seeknal/runtime",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet",
        "run": {
          "runId": "018e6962-4f3b-7489-b2c8-89c0c1b48b9a"
        },
        "job": {
          "namespace": "atlas://ws-default/analytics_pipeline",
          "name": "analytics_pipeline"
        }
      }
    }
  },
  "job": {
    "namespace": "atlas://ws-default/analytics_pipeline",
    "name": "transform.daily_orders",
    "facets": {
      "jobType": {
        "_producer": "https://github.com/mta-tech/seeknal/runtime",
        "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
        "processingType": "BATCH",
        "integration": "SEEKNAL",
        "jobType": "transform"
      }
    }
  },
  "inputs": [
    {
      "namespace": "iceberg://lakekeeper:8181/warehouse",
      "name": "raw.orders"
    }
  ],
  "outputs": [
    {
      "namespace": "iceberg://lakekeeper:8181/warehouse",
      "name": "analytics.daily_orders"
    }
  ],
  "producer": "https://github.com/mta-tech/seeknal/runtime",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
}
```

### Core Top-Level Fields

| Field | Type | Description |
|---|---|---|
| `eventType` | string | Execution lifecycle transition: `"START"`, `"COMPLETE"`, or `"FAIL"`. |
| `eventTime` | string | ISO-8601 UTC timestamp ending in `Z`. |
| `run` | object | Run details including deterministic `runId` and run facets. |
| `job` | object | Job definition including namespace, name, and job facets. |
| `inputs` | list | List of input dataset objects referenced in `node.reads`. |
| `outputs` | list | List of output dataset objects declared in `node.writes`. |
| `producer` | string | Producer URI: `"https://github.com/mta-tech/seeknal/runtime"`. |
| `schemaURL` | string | Schema definition URI: `"https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"`. |

---

## 3. Namespaces, Run IDs, and Parent Facet

### Dataset Namespaces
Iceberg datasets are represented deterministically from 3-part table identifiers (`catalog.namespace.table`):
- **Namespace**: `iceberg://<host>/<catalog>`, where `<host>` is derived using `urllib.parse.urlsplit(...).netloc` with any userinfo stripped and host lowercased (preserving port when present, e.g. `http://lakekeeper:8181` yields `iceberg://lakekeeper:8181/<catalog>`), and `<catalog>` is the first part of the 3-part table name.
- **Name**: `<namespace>.<table>` representing the schema and table name.
- Non-3-part table names raise a `ValueError`.

### Job Namespaces
- **Job Namespace**: `atlas://<ws>/<project>`, where `<ws>` is the workspace ID from the run grant, and `<project>` is the Seeknal project name.
- **Job Name**: The DAG node identifier (e.g. `transform.daily_orders`). For parent job references, `<project>` is used as the job name.

### Run and Parent IDs
- **Parent Run ID**: The root run grant identifier (`grant.jti`).
- **Node Run ID**: A deterministic UUID v5 generated from module constant `RUN_NAMESPACE = uuid.UUID("6f1d0c6e-6a63-5c9b-9a3e-2c0c5d3a7a10")` and name string `f"{grant.jti}:{node.id}"` (`uuid5(RUN_NAMESPACE, f"{grant.jti}:{node.id}")`). `jti` is treated as an opaque string.
- **Parent Facet (`run.facets.parent`)**: Links the individual node execution to the overarching pipeline execution and project job.

---

## 4. Facets Used

All facets carry `_producer` and `_schemaURL` properties per the OpenLineage specification.

### 1. Parent Run Facet (`parent`)
Included in `run.facets`. Links the node execution to the run grant (`grant.jti`) and parent job (`atlas://<ws>/<project>`).
- Schema URL: `https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet`

### 2. Job Type Facet (`jobType`)
Included in `job.facets`. Describes the workload characteristics:
- `processingType`: `"BATCH"`
- `integration`: `"SEEKNAL"`
- `jobType`: `node.kind` (e.g. `"transform"`, `"source"`, `"python"`)
- Schema URL: `https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet`

### 3. Schema Facet (`schema`)
Included in `dataset.facets` on output datasets when schema metadata is provided.
- `fields`: `[{"name": "<column_name>", "type": "<column_type>"}, ...]`
- Schema URL: `https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet`

### 4. Dataset Version Facet (`version`)
Included in `dataset.facets` when Iceberg snapshot IDs are provided for matching tables (`{table: snapshot_id}`).
- `datasetVersion`: String representation of the snapshot ID (e.g. `"7283648291047192"`).
- Schema URL: `https://openlineage.io/spec/facets/1-0-1/DatasetVersionDatasetFacet.json#/$defs/DatasetVersionDatasetFacet`

### 5. Column Lineage Facet (`columnLineage`)
Included in `dataset.facets` on output datasets when column lineage is derived:
```json
{
  "_producer": "https://github.com/mta-tech/seeknal/runtime",
  "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
  "fields": {
    "daily_total": {
      "inputFields": [
        {
          "namespace": "iceberg://lakekeeper:8181/warehouse",
          "name": "raw.orders",
          "field": "amount"
        }
      ]
    }
  }
}
```
- Schema URL: `https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet`

### 6. Error Message Run Facet (`errorMessage`)
Included in `run.facets` when `eventType="FAIL"`:
- `message`: Sanitized error description. Credentials matching `(?i)(password|secret|token)=\S+` are redacted to `\1=[REDACTED]`, and message length is truncated to at most 2000 characters.
- `programmingLanguage`: `"python"`
- Schema URL: `https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet`

---

## 5. Column Lineage Derivation (`column_lineage_for_sql`)

The `column_lineage_for_sql` function extracts fine-grained column transformations from SQL statements:
- Parses SQL using SQLGlot AST analysis via `seeknal.dag.sql_parser.SQLParser`.
- Resolves table qualifiers and aliases (e.g. `SELECT a.x AS y FROM t1 a`) against plan tables provided in `input_tables` (`alias -> 3-part table`).
- Returns a mapping of `{output_column: [(input_table, input_column), ...]}`.
- Fails safe: never raises exceptions on invalid or unsupported SQL queries, returning `{}` instead.

---

## 6. Delivery Semantics and `LineageEmitter`

`LineageEmitter` provides reliable, non-blocking delivery of lineage events to the Atlas Catalog:

```python
from seeknal.runtime.lineage import LineageEmitter

emitter = LineageEmitter(
    base_url="https://catalog.internal",
    grant_token=run_grant_jwt,
    timeout_seconds=5,
    max_buffer=1000,
)

# Non-blocking emission
emitter.emit(event)

# Flush any buffered items at completion
emitter.flush()

# Check delivery status
stats = emitter.stats()
# {"sent": 5, "buffered": 0, "dropped": 0, "failed_attempts": 0}
```

### Reliability Model
1. **Best-Effort Delivery**: Emits asynchronously over HTTP POST. Failures never raise exceptions out of `emit()`.
2. **In-Memory Buffering**: On HTTP non-2xx responses or network errors (timeouts, connection drops), events are appended to an in-memory buffer.
3. **Bounded FIFO Queue**: When the buffer reaches `max_buffer` (default: 1000), the oldest buffered events are dropped first, and the `dropped` counter is incremented.
4. **Flush Semantics**: Calling `flush()` attempts to deliver buffered events sequentially in original order, stopping at the first encountered failure.
5. **Credential Safety**: Emitter logs warnings on transmission failures but redacts bearer tokens and secrets from log messages.

---

## 7. Catalog Endpoint Contract

- **Endpoint**: `POST /api/runtime/v1/lineage`
- **Headers**:
  - `Authorization`: `Bearer <grant_token>`
  - `Content-Type`: `application/json`
- **Request Body**: Exactly one OpenLineage `RunEvent` JSON object.
- **Expected Responses**:
  - `202 Accepted` (or `200 OK`): Event received and queued for ingestion.
  - `401 Unauthorized`: Missing or invalid run grant token.
  - `403 Forbidden`: Token expired or lacks permission.
  - `5xx / Network Error`: Event buffered in runtime memory for subsequent flush retry.

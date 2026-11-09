# Atlas Runtime Run Grant, Run Plan, and Controller Client

This document defines the contract, plan derivation rules, controller client semantics, and CLI commands for Atlas Runtime execution governance in Seeknal.

---

## 1. Context and Architecture

Atlas Runtime executes a Seeknal project as a governed job under a service principal identity.
Prior to job execution:
1. The Atlas Run Controller evaluates compiled Seeknal DAG dependencies and requests a **Run Grant** from the Atlas Catalog.
2. The Atlas Catalog validates permissions against OpenFGA and metadata in DataHub, issuing an ES256 compact JWS run grant signed by the catalog key.
3. During execution, at every node transition, the runtime job calls the run controller (`POST /api/runtime/v1/runs/{jti}/nodes/start`).
4. If a run has been revoked or permissions are violated, the controller halts execution before the node starts (fail-closed model).

---

## 2. Run Grant Contract v1

A run grant is a compact JWS signed with ES256. The header contains:
```json
{"alg": "ES256", "typ": "JWT", "kid": "<key-id>"}
```

### Claims Specification

| Claim | Type | Validation Rule |
|---|---|---|
| `typ` | string | Exactly `"run"`. |
| `iss` | string | Exactly `"atlas-catalog"`. |
| `aud` | string | Exactly `"atlas-rt-run"`. Serving grants (`aud="seeknal-serving"`) are strictly refused. |
| `sub` | string | Non-empty Keycloak subject of the service principal (OpenFGA subject `user:<sub>`). |
| `principal` | string | Non-empty display name formatted as `svc:<project>:<env>`. |
| `ws` | string | Non-empty workspace identifier. |
| `jti` | string | Non-empty run identifier (UUID v4 string). |
| `release` | string | Non-empty project release ID. |
| `plan_sha256` | string | Exactly 64 lowercase hexadecimal characters representing the SHA-256 of canonical plan JSON. |
| `trigger` | object | Object `{"type": "schedule" \| "user" \| "agent", "id": "<trigger-id>"}`. |
| `iat` | integer | Issued-at time in integer seconds; must not be in the future beyond leeway. |
| `nbf` | integer | Not-before time in integer seconds; must satisfy `nbf <= iat + 5` and not be in the future beyond leeway. |
| `exp` | integer | Expiration time in integer seconds; `exp > now - leeway`, `exp >= iat`, and `60 <= exp - iat <= 86400`. |
| `reads` | list | `[{"table": "<warehouse>.<namespace>.<table>", "actions": ["read"]}]` sorted by `table`, deduplicated. |
| `writes` | list | `[{"table": "<warehouse>.<namespace>.<table>", "actions": ["write"]}]` sorted by `table`, deduplicated. |
| `connections` | list | `[{"name": "<name>", "actions": ["read"] \| ["write"] \| ["read", "write"]}]` sorted by `name`, deduplicated. |

---

## 3. Run Plan Derivation Rules

A run plan captures all input tables, output tables, connections, and DAG execution topology.

### Structure of Canonical Plan JSON

```json
{
  "connections": [
    {"actions": ["read", "write"], "name": "pg_conn"}
  ],
  "nodes": [
    {
      "connections": ["pg_conn"],
      "id": "source.orders",
      "kind": "source",
      "reads": ["lakekeeper.raw.orders"],
      "writes": []
    }
  ],
  "reads": [
    {"actions": ["read"], "table": "lakekeeper.raw.orders"}
  ],
  "writes": [
    {"actions": ["write"], "table": "lakekeeper.analytics.daily_orders"}
  ]
}
```

### Derivation Algorithm

1. **Topological Ordering**: The DAG is built via `DAGBuilder(project_path, profile_path).build()` and traversed in Kahn topological sort order.
2. **Sources**:
   - **Iceberg sources**: Contribute a read access for their 3-part table name (`catalog.namespace.table`). Table names are normalized to lowercase. If a table name does not have 3 non-empty dot-separated segments, a `RunPlanError` is raised.
   - **Non-Iceberg sources**: Contribute a read connection using `params.connection` (or top-level `connection`). File-based sources (`csv`, `parquet`, `json`, etc.) without a connection profile contribute no connection, but record `kind="source"`.
3. **Materializations (Sinks)**:
   - Evaluated from `materializations` (or singular `materialization`).
   - Targets with `enabled: false` are skipped.
   - When `--env <env>` is supplied, environment namespace prefixing is applied via `MaterializationDispatcher._prefix_target`:
     - PostgreSQL: `schema.table` -> `{env}_{schema}.table`.
     - Iceberg: `catalog.namespace.table` -> `catalog.{env}_{namespace}.table`.
   - **Iceberg materializations**: Normalized to lowercase and validated as 3-part names; contribute a write access entry.
   - **PostgreSQL materializations**: Contribute a write connection using target `connection`.
4. **Python Nodes**:
   - Python pipeline nodes (from `.py` script files or node type `NodeType.PYTHON`) are included with `kind="python"`.
   - Their declared materializations are processed identically.
   - `RunPlan.has_python_nodes` returns `True` whenever any node has `kind="python"`, signalling the need for gVisor sandboxing.
5. **Deduplication and Sorting**:
   - All `reads` and `writes` are merged, deduplicated, and sorted by table name.
   - All `connections` are merged by name; actions are unioned and sorted (e.g. `["read", "write"]`).
   - Node-level access lists are deduplicated and sorted.
6. **Canonical Serialization**:
   - Serialized with UTF-8, keys sorted, separators `(",", ":")`, and `ensure_ascii=False`.
   - `plan_sha256` is the lowercase hex SHA-256 digest of these canonical bytes.

---

## 4. Run Controller Client and Error Handling

`RunControllerClient` handles communication with the run controller during execution:

```python
from seeknal.runtime import RunControllerClient

client = RunControllerClient(base_url="https://controller.local", grant_token=token)
client.node_start(node)
```

### Protocol: `POST /api/runtime/v1/runs/{jti}/nodes/start`
- **Headers**:
  - `Authorization: Bearer <grant_token>`
  - `Content-Type: application/json`
- **Body**:
  - `{"node_id": node.id, "reads": list(node.reads), "writes": list(node.writes)}`

### Error Mapping

| HTTP Status / Condition | Exception Raised | Details |
|---|---|---|
| `200` with `{"allowed": true}` | *None* | Success; node execution proceeds. |
| `401 Unauthorized` | `RunGrantError` | Token invalid, expired, or wrong signature. |
| `403 Forbidden` (`error="run_revoked"`) | `RunRevoked` | Subclass of `RunControllerError`. Carries `code="run_revoked"` and optional `table`. |
| `403 Forbidden` (other error) | `NodeNotAuthorized` | Subclass of `RunControllerError`. Carries `code` and optional `table`. |
| `500`, `503`, network error, timeout | `RunControllerUnavailable` | Fail closed immediately without retries. |

---

## 5. Command-Line Interface (CLI)

### `seeknal.runtime plan`

Builds the canonical run plan and prints its digest.

```bash
uv run -m seeknal.runtime plan --project <dir> [--env <env>] [--profile <path>] [--out <file>]
```

- **Arguments**:
  - `--project DIR`: Project directory containing `seeknal/`.
  - `--env ENV`: Optional environment prefix (e.g. `prod`, `staging`).
  - `--profile PATH`: Optional path to `profiles.yml`.
  - `--out FILE`: Optional output file path where canonical JSON is written.
- **Output**:
  - `stdout`: Canonical plan JSON string.
  - `stderr`: `plan_sha256=<hex-digest>`.
- **Exit codes**:
  - `0`: Success.
  - `2`: Compilation error (`RunPlanError`).

### `seeknal.runtime verify-grant`

Verifies a run grant against a JWKS public key set and checks plan coverage.

```bash
uv run -m seeknal.runtime verify-grant --grant <grant-file> --jwks <jwks-file> [--plan <plan-file>]
```

- **Arguments**:
  - `--grant FILE`: Path to file containing ES256 compact JWS grant token.
  - `--jwks FILE`: Path to JWKS JSON file.
  - `--plan FILE`: Optional path to canonical plan JSON file to verify admission coverage.
- **Output**:
  - `stdout`: `OK` on success.
  - `stderr`: Error details on failure.
- **Exit codes**:
  - `0`: Valid grant (and covered plan when `--plan` is provided).
  - `1`: Invalid grant or uncovered plan.

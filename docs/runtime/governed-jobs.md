# Atlas Runtime Governed Jobs

This document defines the runtime environment contract, security guarantees, architectural invariants, and unsupported features for Seeknal jobs running in **governed mode** under Atlas Runtime.

---

## 1. Context and Architecture

In a governed Atlas Runtime job, the execution pod holds only:
1. A verified **Run Grant** (compact JWS) specifying the exact authorized read tables, write tables, and connections for the execution plan.
2. A short-lived, **Lakekeeper-audience OAuth2 token** written to disk and refreshed atomically by the run controller.

Storage credentials are never statically baked into the pod or passed as static environment variables. Instead, storage credentials are provided per table load by Lakekeeper via DuckDB's Iceberg extension using `ACCESS_DELEGATION_MODE 'vended_credentials'`.

All I/O operations are centralized through `CatalogIO` (`seeknal.runtime.catalog_io`), which serves as the choke point governing Iceberg attaches, table read/write authorizations, connection resolution, and node execution lifecycle transitions.

---

## 2. Environment Contract

Governed mode is activated when the environment variable is set:
```bash
ATLAS_RUNTIME_MODE=governed
```

When `ATLAS_RUNTIME_MODE=governed`, **every** setting listed below is required. A missing setting causes an immediate startup failure (never a fallback to unauthenticated or legacy behavior).

| Environment Variable | Description | Security Requirements |
|---|---|---|
| `ATLAS_RUNTIME_MODE` | Runtime mode selector. Must be exactly `"governed"`. | Required. |
| `ATLAS_RUN_GRANT_FILE` | Absolute path to the run grant token (compact JWS). | Mode 0600 (no group/world read). |
| `ATLAS_RUN_GRANT_JWKS_FILE` | Absolute path to the JWKS JSON containing the catalog public key(s). | Mode 0600 (no group/world read). |
| `ATLAS_LAKEKEEPER_TOKEN_FILE` | Absolute path to the short-lived Lakekeeper bearer token. | Mode 0600 (re-read on each attach). |
| `ATLAS_LAKEKEEPER_URI` | Lakekeeper catalog base endpoint URI (e.g. `http://lakekeeper:8181`). | Required. |
| `ATLAS_LAKEKEEPER_WAREHOUSE` | Default Lakekeeper warehouse name. | Required. |
| `ATLAS_RUN_CONTROLLER_URL` | Base URL of the Atlas Run Controller service. | Required. |
| `ATLAS_CONNECTIONS_DIR` | Directory containing `<name>.json` connection descriptors. | Mode 0600 for all contained connection files. |

### Prohibition of Static Credentials

A governed pod must hold no static credentials. Startup validation strictly refuses to start if any of the following environment variables are set (naming the offending variable in the refusal error):
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `KEYCLOAK_CLIENT_SECRET`
- `LAKEKEEPER_TOKEN`

### Legacy and Ungoverned Execution

When `ATLAS_RUNTIME_MODE` is unset or not `"governed"`, execution behavior is **completely unchanged**, using standard S3 configuration, Keycloak client credentials, and `profiles.yml` profiles.

---

## 3. Core Guarantees

### 1. Fail-Fast Grant Authorization
- `CatalogIO.check_read(table)` and `CatalogIO.check_write(table)` normalize 3-part Iceberg table identifiers (`<warehouse>.<namespace>.<table>`) to lowercase and verify that the table is explicitly authorized in the run grant's `reads` or `writes` claims.
- Any access to an unauthorized table raises `NotInRunGrant` before DuckDB or the catalog is touched.

### 2. Vended Credentials Only
- `CatalogIO.attach_iceberg(con, alias)` loads `httpfs` and `iceberg` extensions and issues an `ATTACH` statement using:
  ```sql
  ATTACH '<warehouse>' AS <alias> (
      TYPE ICEBERG,
      ENDPOINT '<uri>/catalog',
      AUTHORIZATION_TYPE 'oauth2',
      TOKEN '<token>',
      ACCESS_DELEGATION_MODE 'vended_credentials'
  )
  ```
- It never executes `SET s3_access_key_id`, `SET s3_secret_access_key`, or `SET s3_use_ssl=false`.

### 3. Dynamic Token Re-attachment
- The Lakekeeper bearer token in `ATLAS_LAKEKEEPER_TOKEN_FILE` is refreshed periodically on disk by the controller.
- `CatalogIO` re-reads the token file on every attach call and compares it against the previously attached token for that connection alias. If the token file content has changed, `CatalogIO` cleanly `DETACH`es and re-attaches with the updated token.

### 4. Governed Connection Resolution
- External connections (e.g., PostgreSQL sources and materializations) are resolved strictly through `CatalogIO.connection(name, action)`.
- Connection JSON is loaded exclusively from `ATLAS_CONNECTIONS_DIR/<name>.json`, requiring mode 0600.
- `profiles.yml` is never read in governed mode.
- Accessing a connection not authorized for the requested action (`read` vs `write`) raises `NotInRunGrant`.

### 5. Node Start Lifecycle and Immediate Halt
- Before `pre_execute` begins for any node, `CatalogIO.node_start(node.qualified_name)` calls the run controller (`POST /api/runtime/v1/runs/{jti}/nodes/start`).
- If the run controller responds that the run is revoked (`RunRevoked`) or the node is unauthorized (`NodeNotAuthorized`), the executing node fails with status `FAILED`.
- Both `DAGRunner` and `ParallelDAGRunner` immediately halt scheduling of all subsequent nodes, ignoring `continue_on_error`.

---

## 4. Unsupported Features (v1)

The following features are intentionally not supported in Governed Mode v1:
1. **PyIceberg Advanced Mutation Modes**: Modes requiring PyIceberg (e.g. conditional upsert, merge, complex delete mutations) are refused with a clear error (`PyIceberg advanced mutation modes are not supported in governed mode (v1)`). Governed mode currently uses DuckDB's native Iceberg integration with vended credentials.
2. **Non-S3 Vended Storage**: Storage backends other than S3-compatible object storage via Lakekeeper credential vending are deferred until subsequent Lakekeeper releases.

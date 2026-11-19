# Seeknal Serving Governed Select API (`POST /v1/select`)

`POST /v1/select` provides a structured JSON query endpoint over the same ES256 grant model, placement router, guardrails, policy enforcement, and engine adapters used by `POST /v1/query`.

By restricting identifiers to catalog placement and schema allowlists and binding all filter and cursor values exclusively as positional parameters (`$1..$n`), SQL injection is impossible by construction.

---

## 1. Authentication & Grant Contract (`select.v1`)

Every `POST /v1/select` request requires an `Authorization: Bearer <grant_token>` header carrying an ES256 JWS signed by the Catalog.

Required grant claims for `POST /v1/select`:

| Claim | Requirement |
|---|---|
| `api` | Must equal `"select.v1"`. |
| `sql_sha256` | Lower-case 64-hex SHA-256 of the **exact raw HTTP request body bytes**. |
| `sql_dialect` | Must equal `"postgres"` (or be omitted, defaulting to `"postgres"`). |
| `params_sha256` | Must be absent (`null` / omitted). Parameter values live inside the signed request body. |
| `jti`, `sub`, `ws`, `exp`, `iat`, `max_rows`, `tables` | Standard single-use grant claims. Each paginated request requires a fresh grant (`jti`). |

Cross-endpoint isolation:
- `POST /v1/select` rejects grants without `api == "select.v1"`, grants with `sql_dialect != "postgres"`, grants carrying `params_sha256`, or requests where any byte of the body differs from `sql_sha256` (`401 grant_invalid`).
- `POST /v1/query` and `POST /v1/explain` reject any grant carrying an `api` claim (`401 grant_invalid`).

---

## 2. Request Payload

```json
{
  "table": "cbn.rpt.rpt_txn_detail",
  "columns": ["txn_id", "as_of_date", "branch_code", "amount"],
  "where": {
    "_and": [
      {"customer_id": {"_eq": "C-1001"}},
      {"as_of_date": {"_gte": "2026-10-01", "_lt": "2026-11-01"}},
      {"_not": {"branch_code": {"_in": ["X01", "X02"]}}}
    ]
  },
  "order_by": [{"as_of_date": "desc"}, {"txn_id": "desc"}],
  "limit": 100,
  "after": "<opaque HMAC cursor or omitted>"
}
```

### 2.1 Top-Level Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `table` | `str` | Yes | 1–3 identifier segments (`^[A-Za-z_][A-Za-z0-9_]{0,62}$`). 2-part names inherit `default_catalog` when configured. Must resolve to a placed table with a known schema. |
| `columns` | `list[str]` | Yes | Non-empty list of unique column identifiers present in the table schema. Masked columns may be projected (returning masked output), while ungranted columns return `403 access_denied`. |
| `where` | `object` | No | Boolean filter tree (depth $\le 5$, total operator nodes $\le 64$). |
| `order_by` | `list[object]` | No | List of single-key `{column: "asc"\|"desc"}` objects sharing a single sort direction. Visible primary key columns from `table.keys` not already listed are appended in the same direction (`"asc"` when `order_by` is omitted/empty). |
| `limit` | `int` | No | Positive safe integer capping page size. Effective row cap is `min(grant.max_rows, limit, table.access.max_rows)`. |
| `after` | `str` | No | Opaque HMAC-SHA256 keyset pagination token from a prior response's `next` field. |
| `hint` | `str` | No | Optional routing engine hint. |
| `max_staleness_seconds` | `number` | No | Maximum acceptable replication lag in seconds. |
| `timeout_s` | `number` | No | Positive finite execution timeout override (clamped to server timeout). |
| `max_result_bytes` | `int` | No | Positive integer cap on JSON-encoded `rows` bytes (clamped to server limit). |

Any unknown top-level key or malformed field returns `400 select_invalid`.

### 2.2 Filter Operators (`where`)

Logical combinators:
- `_and`: non-empty list of condition objects
- `_or`: non-empty list of condition objects
- `_not`: single non-empty condition object

Column operators:
- `_eq`, `_neq`, `_gt`, `_gte`, `_lt`, `_lte`: non-null `seeknal-params-v1` scalar (`bool`, safe `int` in `[-9007199254740991, 9007199254740991]`, or UTF-8 `str` $\le 16\text{ KiB}$ without unpaired surrogates; `float` is rejected)
- `_in`, `_nin`: non-empty list of non-null `seeknal-params-v1` scalars (length $\le$ `policy.max_in_list`, default `1000`)
- `_like`: prefix-only string matching `^([^%_\\]|\\[%_\\])+%$` (must have at least one prefix character or escaped wildcard before trailing `%`; bare `%`, leading `%`, and unescaped `_` are rejected)
- `_is_null`: `bool` (`true` $\rightarrow$ `IS NULL`, `false` $\rightarrow$ `NOT col IS NULL`)

Column governance:
- Referencing a masked or ungranted column in `where` or `order_by` returns `403 access_denied`.

---

## 3. Keyset Pagination

Serving uses deterministic keyset pagination rather than large `OFFSET` scans:

1. **Deterministic sort key**: `effective_order_spec` takes the single-direction `order_by` list and appends any visible primary key columns from `table.keys` not already present.
2. **Fetch `limit + 1`**: Enforcement sets `row_limit = effective_max_rows + 1`. When `limit + 1` rows exist (`truncated == true`), `effective_order_spec` is non-empty, and a cursor signing key is configured (`cursor_key` or `SEEKNAL_CURSOR_KEY`), Serving emits `next`. Any extra primary key or sort columns added internally for cursor construction are stripped from `columns`, `column_types`, and `rows` before returning the response.
3. **Opaque signed token**:
   - Payload JSON: `{"v": 1, "t": "<canonical_table_lower>", "o": [["col", "dir"], ...], "k": [...]}`
   - Format: `<base64url(payload)>.<base64url(hmac_sha256(payload))>` (unpadded canonical base64url)
4. **Seek predicate**:
   - Single sort column: `c1 > $n` (`asc`) or `c1 < $n` (`desc`)
   - Composite sort columns: `(c1, c2, ...) > ($n1, $n2, ...)` (`asc`) or `<` (`desc`)
5. **Verification**: Bit flips, signature mismatches, missing signing keys, foreign table cursors, or sort-order mismatches return `400 cursor_invalid`.

---

## 4. Response Payload (`200 OK`)

Returns all `POST /v1/query` fields plus `next`:

```json
{
  "columns": ["txn_id", "as_of_date", "branch_code", "amount"],
  "column_types": ["bigint", "date", "varchar", "decimal(18,2)"],
  "rows": [
    [9002, "2026-10-31", "JKT01", "150000.00"],
    [9001, "2026-10-31", "BDG02", "85000.00"]
  ],
  "truncated": true,
  "notices": ["row_cap_hit"],
  "next": "eyJ2IjoxLCJ0IjoiY2JuLnJwdC5ycHRfdHhuX2RldGFpbCIsIm8iOltbImFzX29mX2RhdGUiLCJkZXNjIl0sWyJ0eG5faWQiLCJkZXNjIl1dLCJrIjpbIjIwMjYtMTAtMzEiLDkwMDFdfQ.signature",
  "engine": "pg_report",
  "rule": "filter",
  "reason": "Selective filter on Postgres-wire table",
  "masked_columns": [],
  "audit_id": "jti-select-001",
  "redacted_sql": "SELECT txn_id, as_of_date, branch_code, amount FROM cbn.rpt.rpt_txn_detail WHERE customer_id = $1 AND as_of_date >= $2 AND as_of_date < $3 AND NOT branch_code IN ($4, $5) ORDER BY as_of_date DESC, txn_id DESC",
  "elapsed_ms": 4.12
}
```

---

## 5. Error Codes

| HTTP Status | `code` | Trigger |
|---|---|---|
| `400` | `select_invalid` | Malformed JSON, unknown field, hostile/unknown table or column identifier, unsupported operator, float/unsafe scalar, non-prefix `_like`, mixed `order_by` directions, or depth/node limit exceeded. |
| `400` | `cursor_invalid` | Tampered, bit-flipped, unconfigured-key, foreign-table, or mismatched-order `after` cursor. |
| `401` | `grant_invalid` | Missing/invalid `api: "select.v1"`, body SHA-256 mismatch, non-`postgres` `sql_dialect`, `params_sha256` present, or `api` grant sent to `/v1/query` or `/v1/explain`. |
| `401` | `grant_expired` | Expired grant token. |
| `401` | `grant_replayed` | Reused `jti` across requests or pages. |
| `403` | `access_denied` | Ungranted table/column, or masked/ungranted column referenced in `where` or `order_by`. |
| `413` | `payload_too_large` / `result_too_large` | Request body or serialized `rows` exceeds configured byte cap. |
| `422` | `filter_required` / `sort_not_indexed` / `offset_too_large` | Guardrail violation on target table. |
| `503` | `engine_starting` | Cold-starting endpoint waking up; includes `Retry-After` response header. |

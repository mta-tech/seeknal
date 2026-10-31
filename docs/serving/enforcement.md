# Seeknal Serving: Query Policy Enforcement

## 1. Overview & Architecture

Seeknal Serving operates as a secure SQL gateway situated directly behind the Atlas Data Catalog.

```text
Superset / Client
       │ (User SQL)
       ▼
Atlas Data Catalog
       │ (User SQL + Decoded Claims / Grant)
       ▼
Seeknal Serving (enforce.py)
       │ (Rewritten Enforced SQL / Cube Security Context)
       ▼
Engines (StarRocks, Cube SQL API, Lakebase PostgreSQL, DuckDB)
```

### Why Enforcement Lives in Serving
1. **Shared Service Accounts on Engines:** Engines execute queries using privileged service accounts. User credentials and roles do not reach the target databases directly.
2. **Single Choke-Point:** Seeknal Serving inspects every incoming query and rewrites it before any database engine parses or plans it. This makes Serving the single point where column masking, row filtering, table grants, and row limits can be guaranteed.
3. **AST-Based Rewriting:** All policy checks and SQL rewrites are performed via `sqlglot` Abstract Syntax Tree (AST) transformations. Textual matching or regex parsing is never used, eliminating bypasses caused by whitespace variations, comments, or nested subqueries.

---

## 2. Grant Shape

The Catalog evaluates user authorization and issues a grant (decoded from a Catalog JWT).

### JSON Claims Schema

```json
{
  "sub": "user:7f3c",
  "ws": "claims-jabar",
  "jti": "aud-0192",
  "sql_sha256": "4b9e017ec59a9a111c96fcd87536a0d641ba39b77f0d380652919eed3999ce3d",
  "max_rows": 10000,
  "exp": 1793430000,
  "tables": {
    "claims.claim": {
      "columns": ["claim_id", "province", "idrg_code", "amount", "admit_date"],
      "masks": {
        "patient_nik": "hash",
        "patient_name": "redact",
        "birth_date": "year"
      },
      "row_filters": [
        {"column": "province", "op": "in", "values": ["JB"]}
      ],
      "actions": ["select"]
    }
  }
}
```

### Dataclass Model (`seeknal.serving.enforce`)

- `RowFilter(column: str, op: str, values: tuple)`: `op` is one of `eq`, `in`, `gte`, `lte`.
- `TableGrant(table: str, columns: tuple[str, ...], masks: Mapping[str, str], row_filters: tuple[RowFilter, ...] = (), actions: tuple[str, ...] = ("select",))`.
- `Grant(subject: str, workspace: str, audit_id: str, sql_sha256: str, max_rows: int, expires_at: float, tables: Mapping[str, TableGrant])`.

---

## 3. Enforcement Rules & Healthcare Claims Examples

Throughout this section, examples use a healthcare claims analyst who is restricted to West Java (`province = 'JB'`), with `patient_nik` hashed, `patient_name` redacted, `birth_date` truncated to year, and `confidential_note` ungranted.

### Base Schema (`claims.claim`)
- `claim_id` (INT)
- `patient_nik` (VARCHAR)
- `patient_name` (VARCHAR)
- `province` (VARCHAR)
- `idrg_code` (VARCHAR)
- `amount` (INT)
- `admit_date` (DATE)
- `birth_date` (DATE)
- `confidential_note` (VARCHAR) — *Ungranted*

---

### Rule 1: Integrity
- **SHA-256 Check:** `hashlib.sha256(sql.encode()).hexdigest()` must match `grant.sql_sha256`. Any alteration of the SQL after Catalog issuance is refused.
- **Expiration:** `now` must precede `grant.expires_at`.
- **Single Statement:** SQL must parse into exactly one statement; multi-statement injections are refused.

---

### Rule 2: Tables
Every referenced table (excluding statement-defined CTEs) must be in `grant.tables` and in `schemas`.
- *Refusal Example:* Querying an ungranted table (`SELECT * FROM hospital.payroll`) raises `EnforcementError: table 'hospital.payroll' is not granted`.

---

### Rule 3: Actions
- `SELECT` queries require `select` on all referenced tables.
- `INSERT`, `UPDATE`, and `DELETE` require the respective write action on the target table, and `select` on any read tables.
- DDL (`CREATE`, `DROP`, `ALTER`), `MERGE`, `SET`, and `COPY` statements are refused.

---

### Rule 4: Columns & Star Expansion
- Every column reference must resolve to a granted column (visible or masked).
- Columns in schema that are neither visible nor masked are rejected.
  - *Refusal Example:* `SELECT confidential_note FROM claims.claim` raises `EnforcementError: column 'confidential_note' in table 'claims.claim' is neither visible nor masked`.
- `*` and `t.*` expand into that table's granted columns in schema order.

#### Example: Star Expansion
**Before SQL:**
```sql
SELECT * FROM claims.claim
```
**After AST Expansion:**
`*` expands strictly to granted columns, omitting `confidential_note`:
```sql
SELECT claim_id, patient_nik, patient_name, province, idrg_code, amount, admit_date, birth_date
FROM claims.claim
```

---

### Rule 5: Secure-View Rewrite (Engines: StarRocks, Lakebase, DuckDB)
Every table reference is replaced with a derived table (subquery) selecting only granted columns in schema order, with masks applied and row filters AND-ed into the subquery's `WHERE` clause.

#### Example: Secure-View Query Rewrite (DuckDB dialect)
**Before SQL:**
```sql
SELECT claim_id, province, patient_nik, amount
FROM claims.claim
WHERE amount > 5000
```

**Enforced SQL:**
```sql
SELECT claim_id, province, patient_nik, amount
FROM (
    SELECT
        claim_id,
        SHA256(CAST(patient_nik AS TEXT)) AS patient_nik,
        '****' AS patient_name,
        province,
        idrg_code,
        amount,
        admit_date,
        CAST(DATE_TRUNC('YEAR', birth_date) AS DATE) AS birth_date
    FROM claims.claim
    WHERE province IN ('JB')
) AS claim
WHERE amount > 5000
LIMIT 10001
```

---

### Rule 6: Writes
- Target tables of `UPDATE` and `DELETE` are **not** wrapped in derived tables.
- Row filters are AND-ed into the statement's `WHERE` clause.
- `UPDATE` cannot SET a masked column or any column outside `columns`.
- `INSERT` into a table with row filters is refused (fail-closed).
- `INSERT` naming a masked column is refused.

#### Example: UPDATE Rewrite
**Before SQL:**
```sql
UPDATE claims.claim SET amount = 6000 WHERE claim_id = 42
```
**Enforced SQL:**
```sql
UPDATE claims.claim SET amount = 6000 WHERE claim_id = 42 AND province IN ('JB')
```

---

### Rule 7: Row Cap
Outermost `LIMIT` is set to `min(existing_limit, grant.max_rows + 1)`. If `LIMIT` is absent, it is set to `grant.max_rows + 1`. The extra row enables clients to detect truncation without issuing an additional count query. Writes have `row_limit = None`.

---

### Rule 8: The Cube Path (`target_kind == "cube"`)
Cube SQL API operates on pre-aggregated cubes and does not support derived subquery tables.
When `target_kind == "cube"`:
1. Rules 1–4 and 7 run as normal.
2. Table secure-view rewrites are skipped.
3. `EnforcedQuery.cube_security_context` is populated:
```python
{
    "subject": "user:7f3c",
    "masks": {
        "claims.claim": {
            "patient_nik": "hash",
            "patient_name": "redact",
            "birth_date": "year"
        }
    },
    "row_filters": {
        "claims.claim": [
            {"column": "province", "op": "in", "values": ["JB"]}
        ]
    }
}
```
Cube's native access policies consume this context from a signed token to enforce masking and row filters during cube query generation.

---

### Rule 9: Mask Expressions by Target Dialect

| Method | PostgreSQL (`lakebase`) | StarRocks (`starrocks`) | DuckDB (`duckdb`) |
|---|---|---|---|
| `redact` | `'****'` | `'****'` | `'****'` |
| `null` | `CAST(NULL AS TEXT)` | `CAST(NULL AS VARCHAR)` | `CAST(NULL AS VARCHAR)` |
| `hash` | `encode(sha256(convert_to(CAST(c AS TEXT), 'UTF8')), 'hex')` | `sha2(CAST(c AS VARCHAR), 256)` | `sha256(CAST(c AS VARCHAR))` |
| `last4` | `'****' \|\| right(CAST(c AS TEXT), 4)` | `concat('****', right(CAST(c AS VARCHAR), 4))` | `'****' \|\| right(CAST(c AS VARCHAR), 4)` |
| `year` | `CAST(date_trunc('year', c) AS DATE)` | `CAST(date_trunc('year', c) AS DATE)` | `CAST(date_trunc('year', c) AS DATE)` |

---

### Rule 10: Row Filter Literals & Injection Protection
All filter values are injected as AST literal nodes (`exp.Literal`, `exp.Boolean`, `exp.Is`). String values containing quotes (e.g. `'JB'' OR ''1''=''1'`) are properly escaped by sqlglot and never concatenated directly as strings.

---

### Rule 11: Audit Log Redaction (`redacted_sql`)
`EnforcedQuery.redacted_sql` contains the original statement rendered in source `dialect` with all literals replaced by `?`.
- **Original:** `SELECT * FROM claims.claim WHERE patient_nik = '320123' AND amount > 500`
- **Redacted:** `SELECT * FROM claims.claim WHERE patient_nik = ? AND amount > ?`

---

## 4. Masked-Value Filtering Behaviour

In the secure-view rewrite architecture, masking happens inside the derived table.
Consequently, any `WHERE`, `GROUP BY`, `ORDER BY`, or `JOIN` conditions referencing a masked column in the outer query operate on the **masked** value:

```sql
SELECT claim_id FROM claims.claim WHERE patient_nik = '3201234567890001'
```
Rewrites to:
```sql
SELECT claim_id FROM (
    SELECT claim_id, SHA256(CAST(patient_nik AS TEXT)) AS patient_nik, ...
    FROM claims.claim WHERE province IN ('JB')
) AS claim
WHERE patient_nik = '3201234567890001'
```
In this query, `patient_nik` in the outer `WHERE` clause evaluates against the SHA-256 hex string, not the raw plaintext NIK.

### Why this is intended:
1. **Preventing Side-Channel Leakage:** If the outer query evaluated predicates against unmasked data before returning masked values, an attacker could deduce sensitive values through brute-force boolean probing (e.g. `WHERE patient_nik LIKE '3201%'`).
2. **Deterministic Computation:** Grouping and ordering on masked columns operate consistently across all queries without leaking plaintext ordering or bucket counts.

---

## 5. Scope Boundaries (Out of Scope)

The following responsibilities are intentionally handled outside `seeknal.serving.enforce`:
- **Cryptographic Signature Verification:** Verifying HMAC or asymmetric signatures on Catalog tokens is performed by gateway middleware before claims reach `enforce()`.
- **Grant Generation:** Policy decisions, role-to-mask mappings, and grant issuance are authored by the Atlas Data Catalog.
- **Engine Query Execution:** Database connection pooling, cursor execution, and result streaming are managed by engine drivers and downstream Seeknal runners.

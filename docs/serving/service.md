# Seeknal Serving: Pipeline from Signed Grant to Engine-Ready SQL

## 1. Overview & Architecture

Seeknal Serving connects the Atlas Data Catalog with underlying execution engines:

```text
Superset / BI / Client
       │
       │ (User SQL + $1..$n params)
       ▼
Atlas Data Catalog
       │
       │ (Signs ES256 grant token with claims: tables, masks, row filters, max_rows, sql_sha256)
       ▼
Seeknal Serving (service.py)
  ├── 1. verify_grant_token (validates ES256 signature, expiry, audience, issuer)
  ├── 2. router.decide (inspects original SQL AST shape; selects engine)
  ├── 3. enforce (applies masks, row filters, row cap, column checks)
  ├── 4. router.rewrite (maps logical table names to physical placement names)
  └── 5. parameter compilation (swaps markers to driver style: %s or ?, escapes % to %%)
       │
       │ (PreparedQuery: engine, engine_kind, sql, params, paramstyle, audit metadata)
       ▼
Target Engine (StarRocks, Lakebase PostgreSQL, Cube SQL API, or DuckDB)
```

The pipeline operates in-memory with zero network overhead, no active database connections, and strict security isolation.

---

## 2. Pipeline Execution Order

`ServingPipeline.prepare(sql, token, params=..., hint=..., max_staleness_seconds=..., now=...)` executes five distinct phases in strict sequential order:

1. **Token Verification (`verify_grant_token` & `Grant.from_claims`):**
   Decodes and cryptographically verifies the compact JWS token from the Catalog using ES256 public keys. Converts validated claims into an immutable `Grant` object.
2. **Engine Routing (`router.decide`):**
   Parses the client's *original* SQL statement and extracts structural query features (statement kind, table references, joins, aggregations, key equality lookups). Determines feasible engines and selects the target engine based on placement rules and hints.
3. **Policy Enforcement (`enforce`):**
   Validates query integrity against `grant.sql_sha256`, enforces column allowlists, injects column masking expressions, wraps tables with row filter predicates, limits result rows (`max_rows + 1`), and generates audit/redaction structures.
4. **Physical Placement Rewriting (`router.rewrite`):**
   Replaces logical table references within the enforced AST with the target engine's physical table paths (e.g., `claims.claim` → `lakekeeper.claims.claim AS claim`), while preserving table aliases and leaving semantic cubes untouched.
5. **Parameter Binding & Paramstyle Compilation:**
   Maps PostgreSQL-style placeholders (`$1..$n`) to the target engine's native parameter style (`%s` for format engines, `?` for qmark engines), escapes literal `%` characters to `%%` for format drivers, and constructs the final positional `params` tuple in exact appearance order.

### Why Routing Reads the Original SQL

Routing occurs **before** query enforcement transforms the AST. 

When `enforce()` runs, it replaces every direct table reference with a derived table subquery:
```sql
-- Original statement
SELECT * FROM claims.claim WHERE claim_id = 42

-- Enforced statement (derived table wrapper)
SELECT claim_id, amount FROM (
    SELECT claim_id, amount FROM claims.claim WHERE province = 'JB'
) AS claim WHERE claim_id = 42 LIMIT 101
```

If the router inspected the enforced AST, key query features would be obscured:
- Point-lookup detection requires single-table equality predicates on primary keys without subqueries or joins. In an enforced query, the presence of derived subqueries would disqualify the query from point-lookup routing to Lakebase.
- Table row counts and simple scan heuristics would be confused by nested derived projections.

Routing on the client's original SQL guarantees that routing heuristics accurately reflect user intent.

---

## 3. Grant Token Format & Cryptographic Checks

The Catalog signs each grant as a compact JWS (JWT) using **ES256 (ECDSA using P-256 and SHA-256) only**.

### Token Structure
A 3-part base64url-encoded string separated by dots:
```text
<base64url(header)>.<base64url(payload)>.<base64url(signature)>
```

### Cryptographic Requirements
- **Algorithm Enforcement:** The header `alg` must be exactly `"ES256"`. Any other algorithm (such as `"none"`, `"HS256"`, `"RS256"`, or a missing `alg` field) is rejected. If `typ` is specified, it must be `"JWT"`.
- **Key Lookup (`kid`):** The header must include a `kid` that maps directly to a trusted public key in the pipeline's `public_keys` dictionary. Keys may be supplied as PEM-encoded strings/bytes or `cryptography.hazmat.primitives.asymmetric.ec.EllipticCurvePublicKey` instances on the SECP256R1 (NIST P-256) curve.
- **Raw Signature Conversion:** JWS defines ES256 signatures as the 64-byte concatenation of raw big-endian integers $r$ (32 bytes) and $s$ (32 bytes). `verify_grant_token` converts $r||s$ into ASN.1 DER format using `cryptography.hazmat.primitives.asymmetric.utils.encode_dss_signature` and verifies the signature using `public_key.verify(..., ec.ECDSA(hashes.SHA256()))`.
- **Claims Validation:**
  - `iss`: Issuer must match the expected Catalog issuer (default `"atlas-catalog"`).
  - `aud`: Audience must match (either equal as a string or present within a list of audiences; default `"seeknal-serving"`).
  - `iat`: Issued-at timestamp is required and must not be in the future (allowing `leeway_seconds`, default 5 s).
  - `exp`: Expiration timestamp must be in the future (allowing `leeway_seconds`).
  - `nbf`: Not-before timestamp, if present, must be in the past (allowing `leeway_seconds`).
  - Lifetime check: `exp - iat` must not exceed `max_lifetime_seconds` (default 300 s).
- **Sanitized Errors:** Every failure raises `GrantTokenError` with a concise diagnostic message that never exposes raw tokens, keys, or credentials.

---

## 4. Parameter Styles and `%` Escaping

Seeknal Serving enables parameterized queries where client SQL uses standard PostgreSQL `$1..$n` notation.

### Paramstyle by Engine Kind

| Engine Kind | Driver Paramstyle | Placeholder Syntax | Target Dialect |
| :--- | :--- | :--- | :--- |
| `starrocks` | `format` | `%s` | `starrocks` |
| `lakebase`  | `format` | `%s` | `postgres` |
| `cube`      | `format` | `%s` | `postgres` |
| `duckdb`    | `qmark`  | `?`  | `duckdb` |

### Parameter Replacement Lifecycle

1. **AST Marker Substitution:**
   Before dialect transpilation, all `exp.Parameter` and `exp.Placeholder` AST nodes are replaced with unique variable identifiers (e.g., `__SEEKNAL_PARAM_0__`). This avoids dialect-specific quirks (such as StarRocks rendering `$1` as `@1`).
2. **Text-Order Marker Scanning:**
   After the AST is rendered in the target dialect, the rendered SQL text is scanned from left to right for markers. This ensures that parameters referenced multiple times or out of order (e.g., `WHERE x = $2 AND y = $1 AND z = $2`) produce a positional `params` tuple matching the exact appearance order in the final query: `(params[1], params[0], params[1])`.
3. **Escaping `%` for Format Drivers:**
   For engines using `paramstyle="format"` (`starrocks`, `lakebase`, `cube`), python database drivers (such as `pymysql` or `psycopg2`) interpret `%` as format specifiers. Every literal `%` in the SQL (e.g., in `LIKE 'JB%'` or date format patterns) is escaped to `%%` before the parameter markers are substituted with `%s`.
4. **Validation:**
   - Referencing a placeholder index greater than the number of supplied parameters raises `EnforcementError`.
   - Passing parameters that are never referenced in the SQL statement raises `EnforcementError`.

---

## 5. End-to-End Example: Provincial Analyst Query Routed to StarRocks

An analyst in West Java (`JB`) runs a parameterized summary query from Superset.

### 1. Configuration & Public Key Setup

```python
import time, hashlib
from cryptography.hazmat.primitives.asymmetric import ec
from seeknal.serving import ServingPipeline, sign_grant_token, Placement

# Generate Catalog ES256 key pair
catalog_private_key = ec.generate_private_key(ec.SECP256R1())
catalog_public_key = catalog_private_key.public_key()

placement = Placement.from_dict({
    "engines": {
        "sr": {"kind": "starrocks", "dialect": "starrocks"},
        "lb": {"kind": "lakebase", "dialect": "postgres"},
    },
    "tables": {
        "claims.claim": {
            "keys": ["claim_id"],
            "row_count": 50_000_000,
            "serve": {
                "sr": "lakekeeper.claims.claim",
                "lb": "public.claim",
            },
        }
    },
    "cubes": {},
})

schemas = {
    "claims.claim": ["claim_id", "province", "idrg_code", "amount", "patient_name", "admit_year"]
}

pipeline = ServingPipeline(
    placement=placement,
    schemas=schemas,
    public_keys={"catalog-2026": catalog_public_key},
)
```

### 2. Client Query & Signed Grant Token

```python
client_sql = """
SELECT idrg_code, count(*) AS total_claims, sum(amount) AS total_amount
FROM claims.claim
WHERE admit_year = $1 AND patient_name LIKE 'A%'
GROUP BY idrg_code
""".strip()

now = time.time()
claims = {
    "sub": "analyst@westjava.gov.id",
    "ws": "jabar-analytics",
    "jti": "grant-claim-0912",
    "iss": "atlas-catalog",
    "aud": "seeknal-serving",
    "iat": now,
    "exp": now + 120,
    "sql_sha256": hashlib.sha256(client_sql.encode("utf-8")).hexdigest(),
    "max_rows": 5000,
    "tables": {
        "claims.claim": {
            "columns": ["idrg_code", "amount", "admit_year", "patient_name"],
            "masks": {},
            "row_filters": [
                {"column": "province", "op": "eq", "values": ["JB"]}
            ],
            "actions": ["select"],
        }
    },
}

token = sign_grant_token(claims, catalog_private_key, kid="catalog-2026")
```

### 3. Pipeline Preparation

```python
prepared = pipeline.prepare(client_sql, token, params=(2025,))

print("Target Engine :", prepared.engine)        # sr
print("Engine Kind   :", prepared.engine_kind)   # starrocks
print("Rule / Reason :", prepared.rule)          # analytics
print("Paramstyle    :", prepared.paramstyle)    # format
print("Params        :", prepared.params)        # (2025,)
print("Prepared SQL  :\n", prepared.sql)
```

### 4. Output SQL Rendered for StarRocks

The resulting query has been safely transformed:
- Routed to StarRocks due to large row count (50M) and aggregation (`count(*)`, `sum(amount)`).
- Rewritten with derived table policy enforcement including row filter `province = 'JB'`.
- Table reference replaced with physical placement path `lakekeeper.claims.claim AS claim`.
- Parameter `$1` replaced with `%s`.
- Literal `%` in `LIKE 'A%'` escaped to `LIKE 'A%%'`.
- Row cap `LIMIT 5001` applied.

```sql
SELECT
  idrg_code,
  COUNT(*) AS total_claims,
  SUM(amount) AS total_amount
FROM (
  SELECT
    idrg_code,
    amount,
    patient_name,
    admit_year
  FROM lakekeeper.claims.claim AS claim
  WHERE
    province = 'JB'
) AS claim
WHERE
  admit_year = %s AND patient_name LIKE 'A%%'
GROUP BY
  idrg_code
LIMIT 5001
```

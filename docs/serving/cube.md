# Seeknal Serving: Cube SQL API Enforcement & Security Context

## 1. Overview & The Threat

Seeknal Serving routes semantic analytical queries to Cube's SQL API. For traditional database targets (StarRocks, Lakebase PostgreSQL, DuckDB), Serving enforces catalog access policies by rewriting tables into secure-view subqueries containing mask expressions and mandatory row filters directly in SQL.

However, Cube's SQL API operates on semantic cubes, dimensions, and measures, and does not accept nested derived table subqueries in place of cube identifiers. As a result, Seeknal Serving skips the table secure-view rewrite for Cube targets.

**The Threat:** If policy context (masks and row filters) is not delivered to Cube and applied during Cube's query compilation:
- Users can see raw values for columns that should be masked.
- Row-level isolation policies are bypassed, exposing rows outside the user's authorized scope.
- Without execution deadlines or cancel hooks, expensive queries can monopolize Cube workers or cause denial of service.

To eliminate this vulnerability, Seeknal Serving implements **fail-closed enforcement by default** and provides a secure, opt-in mechanism to deliver signed policy contexts to Cube via JSON Web Tokens (JWT).

---

## 2. Enforcement Modes (`cube_mode`)

Seeknal Serving provides two operational modes via `cube_mode`:

### `refuse` (Default)
In `refuse` mode, Seeknal Serving fails closed:
- If a client query targets Cube and any referenced cube's `TableGrant` contains non-empty `masks` or `row_filters`, Serving immediately raises `EnforcementError("cube target has masks or row filters; cube enforcement is not enabled")`.
- Queries referencing only clear granted columns without row filters proceed as normal.
- This ensures no query requiring masking or row filtering can reach Cube without cryptographic enforcement enabled.

### `jwt` (Opt-in)
In `jwt` mode, masks and row filters are permitted because Serving compiles and delivers a compact HS256 JWT security context to Cube with every connection:
- Masked members cannot be used to probe values: an `EnforcementError` is raised if a masked member appears anywhere in a `WHERE`, `HAVING`, `JOIN ... ON`, `GROUP BY`, or `ORDER BY` clause, or inside any function or expression.
- A masked member may **only** appear as a bare top-level projection, optionally aliased (e.g., `SELECT patient_nik FROM claims_cube` or `SELECT patient_nik AS p FROM claims_cube`).
- Mandatory row filters originate from the signed Catalog grant and are delivered inside the token's claims, so user SQL cannot circumvent them.

---

## 3. Security Context & Token Claims

When using `CubeAdapter`, Seeknal Serving signs a compact **HS256 JWT** using `CubeSecurityConfig(secret, issuer, audience, ttl_seconds, sql_user)`. The token is passed as the connection password to Cube's SQL API and is never logged, exposed in error messages, or returned to clients.

### JWT Header
```json
{
  "alg": "HS256",
  "typ": "JWT"
}
```

### JWT Claims Payload
```json
{
  "iss": "seeknal-serving",
  "aud": "cube-sql",
  "iat": 1700000000,
  "exp": 1700000060,
  "sub": "user:7f3c",
  "audit_id": "aud-0192",
  "masks": {
    "claims_cube": {
      "patient_nik": "hash"
    }
  },
  "row_filters": {
    "claims_cube": [
      {
        "column": "province",
        "op": "eq",
        "values": ["JB"]
      }
    ]
  }
}
```

- `iss`, `aud`: Verifiable issuer and audience.
- `iat`, `exp`: Token timestamps with TTL ≤ 300 seconds (default 60 seconds).
- `sub`: Authenticated user subject.
- `audit_id`: Unique request audit identifier (`jti`).
- `masks`: Map of cube names to member mask rules (`hash`, `redact`, `null`, `last4`, `year`).
- `row_filters`: Map of cube names to lists of filter predicates (`eq`, `in`, `gte`, `lte`).

---

## 4. Reference `cube.js` Configuration

The following configuration snippet demonstrates how to verify the incoming context token and enforce filters and masks in Cube:

```javascript
const jwt = require('jsonwebtoken');

const CUBE_SQL_SECRET = process.env.CUBE_SQL_SECRET;
const CUBE_ISSUER = 'seeknal-serving';
const CUBE_AUDIENCE = 'cube-sql';

module.exports = {
  // 1. Authenticate connection and extract securityContext
  checkSqlAuth: (req, user, password) => {
    if (!password) {
      throw new Error('Authentication required');
    }

    try {
      const decoded = jwt.verify(password, CUBE_SQL_SECRET, {
        algorithms: ['HS256'],
        issuer: CUBE_ISSUER,
        audience: CUBE_AUDIENCE,
      });

      return {
        password,
        securityContext: decoded,
      };
    } catch (err) {
      throw new Error(`Invalid or expired token: ${err.message}`);
    }
  },

  // 2. Rewrite query to inject mandatory row filters
  queryRewrite: (query, { securityContext }) => {
    if (!securityContext || !securityContext.row_filters) {
      return query;
    }

    const filters = query.filters || [];

    for (const [cubeName, cubeFilters] of Object.entries(securityContext.row_filters)) {
      for (const rf of cubeFilters) {
        let operator;
        let values = rf.values;

        if (rf.op === 'eq') {
          operator = 'equals';
        } else if (rf.op === 'in') {
          operator = 'equals';
        } else if (rf.op === 'gte') {
          operator = 'gte';
        } else if (rf.op === 'lte') {
          operator = 'lte';
        } else {
          throw new Error(`Unsupported filter operator: ${rf.op}`);
        }

        filters.push({
          member: `${cubeName}.${rf.column}`,
          operator: operator,
          values: values,
        });
      }
    }

    query.filters = filters;
    return query;
  },

  // 3. Member-level masking via access control or context variables
  // Cube data models inspect securityContext.masks to return masked dimensions:
  // e.g., in cube definition:
  // patient_nik: {
  //   sql: (CUBE, { securityContext }) => {
  //     const mask = securityContext?.masks?.[CUBE._name]?.patient_nik;
  //     if (mask === 'hash') return `SHA256(${CUBE}.patient_nik)`;
  //     if (mask === 'redact') return `'****'`;
  //     return `${CUBE}.patient_nik`;
  //   },
  //   type: 'string',
  // }
};
```

---

## 5. Verification Before Enabling `jwt` Mode

`jwt` mode must **not** be enabled in production until every item on this checklist passes against a real Cube deployment:

1. [ ] **Masking:** A masked member requested in a bare top-level projection comes back masked according to the granted mask rule.
2. [ ] **Row filtering:** Row filters always apply, including with `MEASURE()` aggregations, views, and pre-aggregations.
3. [ ] **Probe refusal:** Any attempt to filter, join, group, order, or wrap a masked member in an expression is refused by Seeknal Serving with `EnforcementError`.
4. [ ] **Token integrity:** An expired token, forged signature, wrong issuer, or wrong audience is rejected by Cube's `checkSqlAuth`.
5. [ ] **Watchdog deadline:** Queries exceeding the configured timeout are cancelled via `conn.cancel` and raise `EngineTimeout`.
6. [ ] **Client cancel:** Client cancellation invokes `conn.cancel` via the cancel handle and raises `EngineCancelled`.

Do not enable `cube_mode="jwt"` until every item above passes in staging validation.

# Atlas Runtime ACU Metering

This document defines the compute metering specification, ACU calculation formula, hourly rollup derivation, compact JWS signing, monthly licensing exports, and CLI operations for Atlas Runtime.

---

## 1. Context and ACU Unit Definition

Atlas bills compute in **Atlas Compute Units (ACU)**.

`1 ACU` represents one 4 vCPU / 16 GiB node-hour of plain job compute.
Serving compute is not metered; it is covered by the flat per-cell platform fee.

### Authoritative Formula

Compute for any pod execution is calculated as:

$$\text{ACU} = \frac{\text{seconds} \times \max\left(\text{vCPU}_{\text{requested}}, \frac{\text{GiB}_{\text{requested}}}{4}\right) \times \text{kind\_weight}}{14,400}$$

Where:
- $\text{seconds} = (\text{end} - \text{start}).\text{total\_seconds}()$
- $\text{kind\_weight}$ is versioned in the runtime release manifest (`acu_weights`):
  - `job`: `1.0`
  - `session`: `1.5`
  - `warehouse`: `2.0`
  - `sandbox`: `1.0`
- Decimal arithmetic is used throughout, and the resulting ACU is quantized to 6 decimal places using `ROUND_HALF_EVEN`.

---

## 2. Usage Record Contract

A raw pod execution produces a `UsageRecord`:

| Field | Type | Description |
|---|---|---|
| `pod_uid` | `str` | Unique pod identifier string. |
| `ws` | `str` | Atlas workspace identifier. |
| `kind` | `str` | Pod kind: `job`, `session`, `warehouse`, or `sandbox`. |
| `vcpu_requested` | `float` | Requested vCPUs ($\ge 0$, finite). |
| `memory_gib_requested` | `float` | Requested memory in GiB ($\ge 0$, finite). |
| `start` | `datetime` | Pod execution start (timezone-aware UTC). |
| `end` | `datetime` | Pod execution end (timezone-aware UTC, strictly $> \text{start}$). |
| `labels` | `Mapping[str, str]` | Metadata labels including `principal`, `user`, `release`, `run_jti`, `session_id`. |

### Validation Rules
- `end > start`: strictly monotonic.
- `vcpu_requested >= 0` and `memory_gib_requested >= 0`, with at least one $> 0$.
- `kind` must be one of the recognized kinds (`job`, `session`, `warehouse`, `sandbox`).
- Naive datetimes are strictly rejected; datetimes must be timezone-aware (and are normalized to UTC).

---

## 3. Hourly Splitting and Rollups

### Boundary Splitting (`split_by_hour`)

Records spanning UTC hour boundaries are split into contiguous piecewise records:
- Each piece inherits the record's identity, requests, and labels.
- The piece start and end align to the UTC `:00:00` boundary.
- The sum of ACU for the pieces equals the ACU of the whole within $10^{-6}$.

### Rollup Aggregation (`rollup`)

Pieces are grouped by the composite key `(ws, hour_start, kind, release)` and summed:
- `ws`: Workspace ID.
- `hour_start`: UTC datetime truncated to the beginning of the hour (`:00:00Z`).
- `kind`: Pod kind.
- `release`: Release identifier from labels (`labels.get("release", "")`).
- `acu`: Sum of ACU quantized to 6 decimals (`ROUND_HALF_EVEN`).
- `pod_seconds`: Sum of pod running seconds (`Decimal`).
- `record_count`: Number of piecewise usage records in the hour bucket.
- `by_label`: Dictionary mapping showback keys (`principal:<x>`, `user:<x>`) to accumulated ACU (`Decimal`).

Rollup results are deterministically sorted by `(ws, hour_start, kind, release)`.

---

## 4. Signed Totals (`sign_rollups`, `verify_rollups`)

In BYOC (Bring Your Own Cloud) deployments, only signed hourly totals leave the customer cell.

Totals are signed as a compact JWS (ES256) reusing `seeknal.serving.grant_token.sign_grant_token`.

### Token Header
```json
{
  "alg": "ES256",
  "typ": "JWT",
  "kid": "<key-id>"
}
```

### Token Claims
```json
{
  "typ": "acu_rollup",
  "iss": "<cell-id>",
  "iat": 1700000000,
  "rollups": [
    {
      "ws": "ws-prod-1",
      "hour_start": "2026-11-09T00:00:00Z",
      "kind": "job",
      "release": "atlas-rt/1.0.0",
      "acu": "1.000000",
      "pod_seconds": "3600",
      "record_count": 1,
      "by_label": {
        "principal:svc:billing:prod": "1.000000"
      }
    }
  ]
}
```

- JSON formatting is canonical: keys recursively sorted, `separators=(",", ":")`.
- Decimals are serialized as string representations.
- Hours are serialized as ISO-8601 with `Z`.

Verification (`verify_rollups`) checks the ES256 signature, validates `typ == "acu_rollup"`, and reconstructs `HourlyRollup` objects. Tampered tokens or incorrect `typ` are rejected with `MeteringError`.

---

## 5. Monthly Export (`export_monthly`)

For self-managed installs, licensing data is exported as monthly CSV without showback details:

```csv
ws,hour_start,kind,release,acu,pod_seconds,record_count
ws-prod-1,2026-11-09T00:00:00Z,job,atlas-rt/1.0.0,1.000000,3600,1
```

- Filters records matching the specified year and month.
- Deterministically sorted by `(ws, hour_start, kind, release)`.
- No showback columns (`by_label`) are exported in the licensing CSV.

---

## 6. CLI Usage

The runtime CLI provides the `meter` subcommand:

```bash
python -m seeknal.runtime meter \
  --records <path-to-records.jsonl> \
  --manifest <path-to-manifest.yaml> \
  [--sign-key <path-to-pem> --kid <key-id> --cell <cell-id>] \
  [--month <YYYY-MM>]
```

- Default: outputs aggregated hourly rollups as formatted JSON.
- With `--sign-key PEM --kid KID --cell CELL`: outputs signed compact JWS string.
- With `--month YYYY-MM`: outputs monthly license CSV.

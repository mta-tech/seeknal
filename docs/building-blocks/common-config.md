# Common Configuration

Define reusable column mappings, SQL filters, and SQL snippets in a shared location so every transform in your project stays consistent.

---

## Overview

As pipelines grow, the same column names, filter conditions, and JOIN patterns appear in many transforms. Common configuration centralises these values in `seeknal/common/` so you can:

- Change a column name in one place and have it propagate everywhere.
- Share business rules (e.g., "active users only") across transforms.
- Keep SQL snippets (e.g., region enrichment JOINs) DRY.

Values are referenced in transform SQL with `{{ dotted.key }}` syntax and resolved before execution.

---

## Directory Structure

```
seeknal/
  common/
    sources.yml           # Reusable source column mappings
    rules.yml             # Reusable SQL filter expressions
    transformations.yml   # Reusable SQL snippets
  sources/
  transforms/
  ...
```

The `seeknal/common/` directory is created automatically by `seeknal init`. Files inside it are **not** treated as pipeline nodes -- they are loaded separately and made available to all transforms.

---

## File Formats

### sources.yml

Maps logical names to source parameters (column names, identifiers, etc.):

```yaml
sources:
  - id: traffic
    ref: source.traffic_daily
    params:
      dateCol: date_id
      idCol: msisdn
      revenueCol: total_revenue

  - id: customers
    ref: source.customer_master
    params:
      idCol: customer_id
      segmentCol: segment_code
```

Each parameter becomes a flat key: `{source_id}.{param_name}`.

| Expression | Resolves to |
|---|---|
| `{{ traffic.dateCol }}` | `date_id` |
| `{{ traffic.idCol }}` | `msisdn` |
| `{{ customers.segmentCol }}` | `segment_code` |

The source `ref` is also available as `{{ traffic.ref }}` (resolves to `source.traffic_daily`).

### rules.yml

Defines reusable SQL filter expressions:

```yaml
rules:
  - id: activeOnly
    value: "status = 'active'"

  - id: callExpression
    value: "service_type = 'Voice'"

  - id: recentData
    value: "date_id >= '2025-01-01'"
```

Each rule becomes `rules.{rule_id}`.

| Expression | Resolves to |
|---|---|
| `{{ rules.activeOnly }}` | `status = 'active'` |
| `{{ rules.callExpression }}` | `service_type = 'Voice'` |

### transformations.yml

Defines reusable SQL snippets (JOINs, sub-selects, CTEs, etc.):

```yaml
transformations:
  - id: enrichWithRegion
    sql: "LEFT JOIN ref('source.regions') r ON t.region_id = r.id"

  - id: excludeTestAccounts
    sql: "WHERE account_type != 'test'"
```

Each transformation becomes `transforms.{transform_id}`.

| Expression | Resolves to |
|---|---|
| `{{ transforms.enrichWithRegion }}` | `LEFT JOIN ref('source.regions') r ON t.region_id = r.id` |
| `{{ transforms.excludeTestAccounts }}` | `WHERE account_type != 'test'` |

---

## Usage in Transforms

Reference common config values in any transform SQL using `{{ }}` syntax:

```yaml
name: daily_voice_revenue
kind: transform
inputs:
  - ref: source.traffic_daily
transform: |
  SELECT
    {{ traffic.idCol }},
    {{ traffic.dateCol }},
    SUM({{ traffic.revenueCol }}) AS revenue
  FROM input_0
  WHERE {{ rules.callExpression }}
    AND {{ rules.recentData }}
  GROUP BY {{ traffic.idCol }}, {{ traffic.dateCol }}
```

After resolution this becomes:

```sql
SELECT
  msisdn,
  date_id,
  SUM(total_revenue) AS revenue
FROM input_0
WHERE service_type = 'Voice'
  AND date_id >= '2025-01-01'
GROUP BY msisdn, date_id
```

---

## Before / After Comparison

**Without common config** -- column names and filters are duplicated:

```yaml
# transforms/voice_revenue.yml
transform: |
  SELECT msisdn, date_id, SUM(total_revenue) AS revenue
  FROM input_0
  WHERE service_type = 'Voice'
  GROUP BY msisdn, date_id

# transforms/data_usage.yml
transform: |
  SELECT msisdn, date_id, SUM(data_bytes) AS usage
  FROM input_0
  WHERE service_type = 'Data'
  GROUP BY msisdn, date_id
```

If the column `msisdn` is renamed to `subscriber_id` upstream, you must update every transform.

**With common config** -- single source of truth:

```yaml
# common/sources.yml
sources:
  - id: traffic
    ref: source.traffic_daily
    params:
      dateCol: date_id
      idCol: msisdn

# transforms/voice_revenue.yml
transform: |
  SELECT {{ traffic.idCol }}, {{ traffic.dateCol }}, SUM(total_revenue) AS revenue
  FROM input_0
  WHERE service_type = 'Voice'
  GROUP BY {{ traffic.idCol }}, {{ traffic.dateCol }}

# transforms/data_usage.yml
transform: |
  SELECT {{ traffic.idCol }}, {{ traffic.dateCol }}, SUM(data_bytes) AS usage
  FROM input_0
  WHERE service_type = 'Data'
  GROUP BY {{ traffic.idCol }}, {{ traffic.dateCol }}
```

Now renaming `msisdn` to `subscriber_id` requires changing only `common/sources.yml`.

---

## Resolution Priority

When Seeknal encounters a `{{ expression }}` it resolves using this priority cascade (highest wins):

| Priority | Source | Example |
|---|---|---|
| 1 | CLI overrides | `--param date=2025-01-01` |
| 2 | Context values | `{{ run_id }}`, `{{ run_date }}` |
| 3 | Built-in functions | `{{ today }}`, `{{ month_start(-1) }}` |
| 4 | Environment variables | `{{ env:API_KEY }}` |
| 5 | **Common config** | `{{ traffic.dateCol }}`, `{{ rules.activeOnly }}` |
| 6 | Pass-through | Unresolved expression left as-is |

Common config expressions are identified by containing a `.` (dot). Simple expressions like `{{ today }}` or `{{ run_id }}` are never looked up in common config.

---

## Error Handling

If common config is loaded and a dotted expression does not match any key, Seeknal raises an error with suggestions:

```
ValueError: Unknown common config key '{{ traffic.datCol }}'.
Available keys: traffic.dateCol, traffic.idCol, traffic.revenueCol.
Did you mean: traffic.dateCol?
```

If no `seeknal/common/` directory exists, dotted expressions pass through unchanged for backward compatibility.

---

## Interaction with ref() Syntax

Common config `{{ }}` expressions are resolved **before** `ref()` syntax. This means you can use common config inside `ref()` arguments or alongside them:

```yaml
transform: |
  SELECT t.{{ traffic.idCol }}, r.region_name
  FROM ref('source.traffic_daily') t
  {{ transforms.enrichWithRegion }}
```

Resolution order:
1. `{{ traffic.idCol }}` and `{{ transforms.enrichWithRegion }}` are resolved first
2. `ref('source.traffic_daily')` is resolved to `input_0`
3. `input_0` is bound to the actual data source

---

## Getting Started

To add common config to an existing project:

1. **Create the directory** (or run `seeknal init` on a new project):

   ```bash
   mkdir -p seeknal/common
   ```

2. **Add a sources file** with your most-used column mappings:

   ```bash
   cat > seeknal/common/sources.yml << 'EOF'
   sources:
     - id: traffic
       ref: source.traffic_daily
       params:
         dateCol: date_id
         idCol: msisdn
   EOF
   ```

3. **Replace hardcoded values** in your transforms:

   ```yaml
   # Before
   transform: |
     SELECT msisdn, date_id FROM input_0

   # After
   transform: |
     SELECT {{ traffic.idCol }}, {{ traffic.dateCol }} FROM input_0
   ```

4. **Run your pipeline** -- common config is picked up automatically:

   ```bash
   seeknal run --full
   ```

Add `rules.yml` and `transformations.yml` as needed. You only need the files that are useful for your project -- all three are optional.

---

## Tips

- Keep IDs short and descriptive: `traffic`, not `traffic_daily_source_v2`.
- Use `rules.` prefix for WHERE-clause conditions and `transforms.` for JOIN/snippet blocks.
- Common config works in both `seeknal run` and `seeknal dry-run` (preview).
- You can combine common config with built-in functions: `WHERE {{ traffic.dateCol }} = '{{ today }}'`.

---

**Next**: Learn about [Sources](sources.md) or return to [Building Blocks](index.md)

# Chapter 5: Named ref() Input References

> **Duration:** 15 minutes | **Difficulty:** Intermediate | **Format:** YAML & CLI

Learn to use named `ref()` syntax in transform SQL for self-documenting, reorder-safe pipeline definitions.

---

## What You'll Build

Refactor your existing transforms from Chapter 2 to use named references:

```
Before:  SELECT * FROM input_0 s JOIN input_1 p ON ...
After:   SELECT * FROM ref('source.products') p JOIN ref('transform.events_cleaned') e ON ...
```

**After this chapter, you'll have:**
- Transforms using named `ref()` syntax instead of positional `input_0`
- Understanding of when to use `ref()` vs `input_N`
- Mixed syntax working in the same SQL

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 2: Transformations](2-transformations.md) — Transforms created with positional `input_0` syntax

---

## Part 1: Understanding the Problem (3 minutes)

### Why Named References?

Look at the `sales_enriched` transform from Chapter 2:

```yaml
inputs:
  - ref: transform.events_cleaned
  - ref: source.products
transform: |
  SELECT e.event_id, p.name AS product_name, ...
  FROM input_0 e
  LEFT JOIN input_1 p ON e.product_id = p.product_id
```

Two problems:
1. **What is `input_0`?** You have to count the `inputs:` list to know it's `events_cleaned`
2. **Reorder breaks SQL** — If you swap the order of `inputs:`, the SQL silently uses wrong tables

Named `ref()` solves both:

```yaml
inputs:
  - ref: transform.events_cleaned
  - ref: source.products
transform: |
  SELECT e.event_id, p.name AS product_name, ...
  FROM ref('transform.events_cleaned') e
  LEFT JOIN ref('source.products') p ON e.product_id = p.product_id
```

Now the SQL is self-documenting and input order doesn't matter.

---

## Part 2: Refactor to Named Refs (7 minutes)

### Update events_cleaned

Open `seeknal/transforms/events_cleaned.yml` and update the SQL:

```yaml
kind: transform
name: events_cleaned
description: "Clean sales events: remove nulls and deduplicate"
inputs:
  - ref: source.sales_events
transform: |
  SELECT
    event_id,
    product_id,
    quantity,
    sale_date,
    region
  FROM ref('source.sales_events')
  WHERE quantity IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY sale_date DESC) = 1
```

!!! info "ref() Resolution"
    `ref('source.sales_events')` is resolved to `input_0` at execution time, based on the position in the `inputs:` list. The name must exactly match an entry in `inputs:`.

### Update sales_enriched

Open `seeknal/transforms/sales_enriched.yml`:

```yaml
kind: transform
name: sales_enriched
description: "Enrich sales events with product details"
inputs:
  - ref: transform.events_cleaned
  - ref: source.products
transform: |
  SELECT
    e.event_id,
    e.product_id,
    p.name AS product_name,
    p.category,
    e.quantity,
    p.price,
    e.quantity * p.price AS total_amount,
    e.sale_date,
    e.region
  FROM ref('transform.events_cleaned') e
  LEFT JOIN ref('source.products') p ON e.product_id = p.product_id
```

### Update sales_summary

Open `seeknal/transforms/sales_summary.yml`:

```yaml
kind: transform
name: sales_summary
description: "Revenue summary by product category"
inputs:
  - ref: transform.sales_enriched
transform: |
  SELECT
    category,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_units,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
  FROM ref('transform.sales_enriched')
  WHERE category IS NOT NULL
  GROUP BY category
  ORDER BY total_revenue DESC
```

### Verify Everything Works

```bash
# Plan should show the same 9-node DAG
seeknal plan

# Run the full pipeline
seeknal run
```

**Checkpoint:** All nodes should succeed. The output is identical to Chapter 2 — only the SQL syntax changed, not the behavior.

### Verify with Dry-Run

```bash
seeknal dry-run seeknal/transforms/sales_enriched.yml
```

The preview should show the resolved SQL with actual data, just like before.

---

## Part 3: Mixed Syntax & Edge Cases (5 minutes)

### Mixing ref() and input_N

You can use both in the same SQL:

```yaml
inputs:
  - ref: transform.events_cleaned
  - ref: source.products
transform: |
  SELECT *
  FROM ref('transform.events_cleaned') e
  JOIN input_1 p ON e.product_id = p.product_id
```

This works but isn't recommended — pick one style per transform for consistency.

### Quote Styles

Both single and double quotes work:

```sql
-- Both are valid
FROM ref('source.products')
FROM ref("source.products")
```

### Error Handling

Try referencing a non-existent input:

```yaml
transform: |
  SELECT * FROM ref('source.nonexistent')
```

Seeknal will give a clear error:

```
✗ Unknown ref('source.nonexistent'). Available inputs: source.products, transform.events_cleaned
```

### Security

`ref()` arguments are validated to prevent SQL injection. Only alphanumeric characters, underscores, dots, and hyphens are allowed:

```sql
-- This would be rejected:
FROM ref('source.x; DROP TABLE users')
-- Error: Invalid ref() argument. Must match pattern: ^[a-zA-Z_][a-zA-Z0-9_.\-]*$
```

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. ref() name doesn't match inputs list**

    - Symptom: `Unknown ref('source.X')` error
    - Fix: The `ref()` argument must exactly match an entry in `inputs:`. Check spelling and the `kind.name` format (e.g., `source.products`, not just `products`).

    **2. Forgot to include the input in `inputs:` list**

    - Symptom: `Unknown ref()` error even though the node exists
    - Fix: Every `ref()` in SQL must have a corresponding entry in `inputs:`. The `inputs:` list declares DAG dependencies — nodes not listed are not available.

    **3. Mixed syntax confusion**

    - Symptom: Wrong table used in query
    - Fix: If mixing `ref()` and `input_N`, remember that `input_0` is always the first entry in `inputs:`, regardless of any `ref()` calls. Stick to one style per transform.

---

## Summary

In this chapter, you learned:

- [x] **Named ref() Syntax** — `ref('source.products')` instead of `input_0`
- [x] **Self-Documenting SQL** — Readers know exactly which table each reference points to
- [x] **Reorder Safety** — Changing `inputs:` order doesn't break SQL
- [x] **Backward Compatible** — `input_0`/`input_1` still work, can mix with `ref()`
- [x] **Validation** — Invalid refs give clear errors with available inputs

**Before vs After:**

| Aspect | `input_0` | `ref('source.products')` |
|--------|-----------|--------------------------|
| Readability | Must count inputs list | Self-documenting |
| Reorder safe | No — changes break SQL | Yes — name-based |
| Error messages | "input_0 not found" | "source.products not found" |
| Backward compat | Always works | Always works |

---

## What's Next?

In **[Chapter 6: Common Configuration](6-common-config.md)**, you'll learn to centralize column mappings, business rules, and SQL snippets in a shared configuration layer.

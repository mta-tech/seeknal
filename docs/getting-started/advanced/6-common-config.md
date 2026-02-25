# Chapter 6: Common Configuration

> **Duration:** 20 minutes | **Difficulty:** Intermediate | **Format:** YAML & CLI

Learn to centralize column mappings, business rules, and SQL snippets in a shared configuration layer that keeps your transforms DRY and consistent.

---

## What You'll Build

A common configuration layer with reusable definitions:

```
seeknal/common/
├── sources.yml           →  {{products.idCol}}, {{products.priceCol}}
├── rules.yml             →  {{rules.validPrice}}, {{rules.hasQuantity}}
└── transformations.yml   →  {{transforms.priceCalc}}
```

**After this chapter, you'll have:**
- Source column mappings referenced with `{{ dotted.key }}` syntax
- Reusable SQL filter expressions shared across transforms
- Reusable SQL snippets for common calculations
- Understanding of resolution priority (context > env > common config)

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 2: Transformations](2-transformations.md) — Transforms created
- [ ] [Chapter 5: Named ref() References](5-named-refs.md) — Familiar with `ref()` syntax

---

## Part 1: Source Column Mappings (7 minutes)

### The Problem

Look at your transforms — the same column names appear repeatedly:

```yaml
# In events_cleaned.yml
WHERE quantity IS NOT NULL

# In sales_enriched.yml
e.quantity * p.price AS total_amount

# In sales_summary.yml
SUM(quantity) AS total_units
SUM(total_amount) AS total_revenue
```

If your data team renames `quantity` to `units_sold`, you'd need to update every transform. Common config solves this.

### Draft Source Entries

Use `seeknal draft common-source` to generate a template for each source entry, then apply it:

```bash
seeknal draft common-source events
```

This creates `draft_common_source_events.yml`. Edit it to map your event columns:

```yaml
kind: common-source
name: events
ref: "source.sales_events"
params:
  idCol: event_id
  productCol: product_id
  quantityCol: quantity
  dateCol: sale_date
  regionCol: region
```

Apply to merge into `seeknal/common/sources.yml` (the directory and file are created automatically):

```bash
seeknal apply draft_common_source_events.yml
```

Repeat for the products source:

```bash
seeknal draft common-source products
```

Edit `draft_common_source_products.yml`:

```yaml
kind: common-source
name: products
ref: "source.products"
params:
  idCol: product_id
  nameCol: name
  categoryCol: category
  priceCol: price
```

```bash
seeknal apply draft_common_source_products.yml
```

Your `seeknal/common/sources.yml` now contains both entries.

Each parameter becomes a dotted key: `{source_id}.{param_name}`.

| Expression | Resolves To |
|------------|-------------|
| `{{events.quantityCol}}` | `quantity` |
| `{{events.dateCol}}` | `sale_date` |
| `{{products.priceCol}}` | `price` |
| `{{products.categoryCol}}` | `category` |

### Use in a Transform

Update `seeknal/transforms/events_cleaned.yml`:

```yaml
kind: transform
name: events_cleaned
description: "Clean sales events: remove nulls and deduplicate"
inputs:
  - ref: source.sales_events
transform: |
  SELECT
    {{events.idCol}},
    {{events.productCol}},
    {{events.quantityCol}},
    {{events.dateCol}},
    {{events.regionCol}}
  FROM ref('source.sales_events')
  WHERE {{events.quantityCol}} IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {{events.idCol}}
    ORDER BY {{events.dateCol}} DESC
  ) = 1
```

### Verify

```bash
seeknal dry-run seeknal/transforms/events_cleaned.yml
```

The preview shows the resolved SQL — `{{events.quantityCol}}` becomes `quantity`, `{{events.idCol}}` becomes `event_id`, etc.

```bash
seeknal run
```

**Checkpoint:** Pipeline succeeds with identical results. The only change is that column names now come from common config.

---

## Part 2: Reusable Business Rules (7 minutes)

### Draft Rule Entries

Business rules appear in multiple transforms. Centralize them using `seeknal draft common-rule`:

```bash
seeknal draft common-rule hasQuantity
```

Edit `draft_common_rule_hasQuantity.yml`:

```yaml
kind: common-rule
name: hasQuantity
value: "quantity IS NOT NULL AND quantity > 0"
```

```bash
seeknal apply draft_common_rule_hasQuantity.yml
```

Repeat for the remaining rules:

```bash
seeknal draft common-rule validPrice
# Edit: value: "price >= 0 AND price < 10000"
seeknal apply draft_common_rule_validPrice.yml

seeknal draft common-rule completedOrder
# Edit: value: "category IS NOT NULL"
seeknal apply draft_common_rule_completedOrder.yml
```

Your `seeknal/common/rules.yml` now contains all three entries.

Each rule becomes a flat key: `rules.{rule_id}`.

| Expression | Resolves To |
|------------|-------------|
| `{{rules.hasQuantity}}` | `quantity IS NOT NULL AND quantity > 0` |
| `{{rules.validPrice}}` | `price >= 0 AND price < 10000` |

### Use in Transforms

Update `seeknal/transforms/sales_summary.yml` to use the rule:

```yaml
kind: transform
name: sales_summary
description: "Revenue summary by product category"
inputs:
  - ref: transform.sales_enriched
transform: |
  SELECT
    {{products.categoryCol}},
    COUNT(*) AS order_count,
    SUM({{events.quantityCol}}) AS total_units,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
  FROM ref('transform.sales_enriched')
  WHERE {{rules.completedOrder}}
  GROUP BY {{products.categoryCol}}
  ORDER BY total_revenue DESC
```

!!! info "Combining ref() and {{ }}"
    You can use both `ref()` (for input tables) and `{{ }}` (for common config values) in the same SQL. They are resolved at different stages: `{{ }}` first during parameter resolution, then `ref()` during execution.

### Verify

```bash
seeknal dry-run seeknal/transforms/sales_summary.yml
```

Check that the preview shows `category IS NOT NULL` in the WHERE clause (resolved from `{{rules.completedOrder}}`).

---

## Part 3: SQL Snippets & Resolution Priority (6 minutes)

### Draft Transformation Entries

For common SQL calculations, add reusable snippets using `seeknal draft common-transformation`:

```bash
seeknal draft common-transformation totalAmount
```

Edit `draft_common_transformation_totalAmount.yml`:

```yaml
kind: common-transformation
name: totalAmount
sql: "{{events.quantityCol}} * {{products.priceCol}}"
```

```bash
seeknal apply draft_common_transformation_totalAmount.yml
```

Add a second snippet:

```bash
seeknal draft common-transformation revenueShare
```

Edit `draft_common_transformation_revenueShare.yml`:

```yaml
kind: common-transformation
name: revenueShare
sql: "ROUND(total_revenue * 100.0 / SUM(total_revenue) OVER (), 2)"
```

```bash
seeknal apply draft_common_transformation_revenueShare.yml
```

| Expression | Resolves To |
|------------|-------------|
| `{{transforms.totalAmount}}` | `quantity * price` |
| `{{transforms.revenueShare}}` | `ROUND(total_revenue * 100.0 / SUM(...) OVER (), 2)` |

!!! tip "Nested Resolution"
    Notice that `totalAmount` references `{{events.quantityCol}}` and `{{products.priceCol}}`. Common config values are resolved in a single pass — the snippet contains the literal text `{{events.quantityCol}}`, which the parameter resolver then resolves to `quantity`. This means all `{{ }}` expressions are resolved together.

### Understanding Resolution Priority

When a `{{ }}` expression is encountered, Seeknal resolves it using this priority cascade:

| Priority | Source | Example |
|----------|--------|---------|
| 1 (highest) | Context values | CLI `--params`, runtime context |
| 2 | Built-in functions | `{{today}}`, `{{env:MY_VAR}}` |
| 3 | Environment variables | `{{env:DATABASE_URL}}` |
| 4 (lowest) | Common config | `{{traffic.dateCol}}`, `{{rules.X}}` |

Higher-priority sources always win. This means you can override a common config value at runtime:

```bash
# Override events.quantityCol for this run
seeknal run --params events.quantityCol=units_sold
```

### Typo Detection

If you mistype a common config key, Seeknal gives suggestions:

```yaml
# Typo: "event" instead of "events"
WHERE {{event.quantityCol}} IS NOT NULL
```

```
✗ Unresolved dotted expression: 'event.quantityCol'.
  Did you mean: events.quantityCol?
  Available keys: events.dateCol, events.idCol, events.productCol, ...
```

### Verify the Full Pipeline

```bash
seeknal plan
seeknal run
```

**Checkpoint:** All 9 nodes succeed. Transforms use `{{ }}` for column names and rules, `ref()` for table references.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Dotted key not found**

    - Symptom: `Unresolved dotted expression` error
    - Fix: Check the `seeknal/common/sources.yml` file. The key format is `{source_id}.{param_name}`. For rules, it's `rules.{rule_id}`.

    **2. No `seeknal/common/` directory**

    - Symptom: `{{ }}` expressions pass through unresolved
    - Fix: Create the directory and add your YAML files. Without the common directory, dotted expressions without dots (like `{{today}}`) still work as built-in functions.

    **3. YAML parse error in common config**

    - Symptom: `Failed to parse seeknal/common/sources.yml` error
    - Fix: Check YAML syntax. Common errors: missing quotes around values with special characters, wrong indentation.

    **4. Name collision with built-in functions**

    - Symptom: Unexpected value for `{{ }}` expression
    - Fix: Built-in functions (`today`, `env:X`) take precedence over common config. Use dotted keys (e.g., `dates.today`) for common config to avoid collisions.

---

## Summary

In this chapter, you learned:

- [x] **Source Column Mappings** — `{{source_id.param}}` references from `seeknal/common/sources.yml`
- [x] **Reusable Rules** — `{{rules.rule_id}}` for shared SQL filters from `seeknal/common/rules.yml`
- [x] **SQL Snippets** — `{{transforms.snippet_id}}` for reusable calculations from `seeknal/common/transformations.yml`
- [x] **Resolution Priority** — context > built-in > env > common config
- [x] **Typo Detection** — Misspelled keys get suggestions with available alternatives

**Common Config Files:**

| File | Key Format | Example |
|------|------------|---------|
| `sources.yml` | `{id}.{param}` | `{{events.dateCol}}` → `sale_date` |
| `rules.yml` | `rules.{id}` | `{{rules.hasQuantity}}` → SQL expression |
| `transformations.yml` | `transforms.{id}` | `{{transforms.totalAmount}}` → SQL snippet |

**Key Commands:**
```bash
seeknal draft common-source <name>          # Generate common source entry template
seeknal draft common-rule <name>            # Generate common rule entry template
seeknal draft common-transformation <name>  # Generate common transformation template
seeknal apply <draft>.yml                   # Merge entry into seeknal/common/*.yml
seeknal dry-run <transform>.yml             # Preview with resolved {{ }} and ref()
seeknal run                                 # Execute pipeline with common config
seeknal run --params key=value              # Override common config at runtime
```

---

## Advanced Guide Complete!

You've completed all chapters of the Advanced Guide:

```
Ch.1  File Sources       │  CSV, JSONL, Parquet
Ch.2  Transformations    │  Filter, JOIN, Aggregation
Ch.3  Data Rules         │  Null, Range, Severity levels
Ch.4  Lineage            │  DAG visualization, Column tracing, Inspect
Ch.5  Named ref()        │  Self-documenting, reorder-safe references
Ch.6  Common Config      │  Shared column mappings, rules, SQL snippets
```

### What's Next?

Explore other persona paths or dive into the reference docs:

- **[Data Engineer Path](../data-engineer-path/)** — Production ELT pipelines with incremental processing
- **[Analytics Engineer Path](../analytics-engineer-path/)** — Semantic models and business metrics
- **[ML Engineer Path](../ml-engineer-path/)** — Feature stores with point-in-time joins

---

## See Also

- **[Common Configuration Reference](../../building-blocks/common-config.md)** — Full reference for all common config features
- **[Transforms Reference](../../building-blocks/transforms.md)** — Complete transform syntax
- **[CLI Reference](../../reference/cli.md)** — All commands and flags

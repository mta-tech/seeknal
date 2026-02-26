---
summary: Inspect consolidated entity feature stores
read_when: You want to list or inspect consolidated entity views
related:
  - consolidate
  - repl
  - run
---

# seeknal entity

Manage and inspect consolidated entity feature stores. Feature groups that
share the same `entity` are merged into per-entity views with struct-namespaced
columns after `seeknal run`.

## Synopsis

```bash
seeknal entity list [OPTIONS]
seeknal entity show [OPTIONS] NAME
```

## Subcommands

### entity list

List all consolidated entities in the feature store.

```bash
seeknal entity list
seeknal entity list --path /my/project
```

**Options:**

| Option | Description |
|--------|-------------|
| `--project`, `-p` | Project name |
| `--path` | Project path (default: current directory) |

**Example output:**

```
  customer              2 FGs  7 features  [ready]  consolidated: 2026-02-26T10:30:00
  product               1 FGs  4 features  [ready]  consolidated: 2026-02-26T10:30:00
```

### entity show

Display detailed catalog for a consolidated entity, including feature groups,
features, row counts, and schema information.

```bash
seeknal entity show customer
seeknal entity show product --path /my/project
```

**Options:**

| Option | Description |
|--------|-------------|
| `NAME` | Entity name to inspect (required) |
| `--project`, `-p` | Project name |
| `--path` | Project path (default: current directory) |

**Example output:**

```
Entity: customer
Join keys: customer_id
Consolidated at: 2026-02-26T10:30:00
Schema version: 1
Feature groups: 2

  customer_features:
    Features: total_orders, total_revenue, avg_order_value, first_order_date, last_order_date, days_since_first_order
    Rows: 6
    Event time col: event_time

  product_preferences:
    Features: top_category, category_count, electronics_ratio
    Rows: 6
    Event time col: event_time

✓ Parquet: target/feature_store/customer/features.parquet
```

## Storage Layout

After consolidation, entities are stored at:

```
target/feature_store/
└── customer/
    ├── features.parquet           ← merged entity view with struct columns
    └── _entity_catalog.json       ← catalog metadata (FGs, features, schema)
```

## Querying in REPL

Consolidated entities are automatically registered as `entity_{name}` views:

```sql
-- View the consolidated entity with struct columns
SELECT * FROM entity_customer LIMIT 3;

-- Access features using dot notation
SELECT
    customer_id,
    customer_features.total_revenue,
    product_preferences.top_category
FROM entity_customer;
```

## See Also

- [seeknal consolidate](consolidate.md) - Manually trigger consolidation
- [seeknal repl](repl.md) - Query entity views interactively
- [seeknal run](run.md) - Execute pipeline (auto-consolidates)

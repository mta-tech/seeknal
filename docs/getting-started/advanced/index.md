# Advanced Guide

**Duration:** ~155 minutes | **Difficulty:** Intermediate | **Format:** YAML, Python & CLI

Go deeper with Seeknal's advanced capabilities: multi-format file sources, data quality rules, pipeline lineage visualization, named references, shared configuration, and Python pipelines.

---

## What You'll Learn

Take your Seeknal skills to the next level with advanced features that improve pipeline quality, maintainability, and observability:

1. **File Sources** - Load CSV, Parquet, and JSONL data into your pipeline
2. **Transformations** - Clean, join, and aggregate data with SQL
3. **Data Rules** - Validate data quality with automated checks
4. **Lineage & Inspection** - Visualize data flow and debug pipeline outputs
5. **Named ref() References** - Self-documenting, reorder-safe SQL references
6. **Common Configuration** - Shared column mappings, rules, and SQL snippets
7. **Data Profiling** - Compute statistics and validate with threshold checks
8. **Python Pipelines** - Build nodes with Python decorators and mix with YAML

---

## Prerequisites

Before starting, ensure you have:

- At least one **[Learning Path](../index.md)** completed (Data Engineer, Analytics Engineer, or ML Engineer)
- Familiarity with the `draft → dry-run → apply` workflow
- Seeknal installed and available on your PATH
- Basic SQL knowledge (SELECT, WHERE, JOIN, GROUP BY)

---

## Chapters

### Chapter 1: File Sources (~20 minutes)

Load data from CSV, JSONL, and Parquet files:

```
products.csv           →  source.products
sales_events.jsonl     →  source.sales_events
sales_snapshot.parquet →  source.sales_snapshot
```

**You'll learn:**
- Creating sources from different file formats
- The `draft → dry-run → apply` workflow for each
- Exploring data with `seeknal repl`
- How file formats differ in schema handling

**[Start Chapter 1 →](1-file-sources.md)**

---

### Chapter 2: Transformations (~20 minutes)

Clean, join, and aggregate your source data:

```
source.products ────────────┐
                            ├──→ sales_enriched (JOIN)
source.sales_events ────────┘
                                     └──→ sales_summary (aggregation)
```

**You'll learn:**
- Single-input transforms with `ref()` syntax
- Multi-input transforms (JOINs)
- Aggregation transforms (GROUP BY)
- Running a full pipeline with `seeknal plan` and `seeknal run`

**[Start Chapter 2 →](2-transformations.md)**

---

### Chapter 3: Data Rules (~25 minutes)

Validate data quality with automated rule checks:

```
transform.events_cleaned ──→ rule.not_null_quantity  (null check)
                         ──→ rule.positive_quantity  (range check)
source.products          ──→ rule.valid_prices       (range on source)
transform.events_cleaned ──→ rule.no_duplicate_events (sql_assertion)
```

**You'll learn:**
- Creating rule nodes for data validation
- Expression-based rules (null, range, freshness)
- SQL assertion rules (dbt-style custom SQL checks)
- Severity levels: `error` vs `warn`
- Integrating rules into your pipeline DAG

**[Start Chapter 3 →](3-data-rules.md)**

---

### Chapter 4: Lineage & Inspection (~17 minutes)

Visualize data lineage and inspect intermediate pipeline outputs:

```
seeknal lineage                              →  Full DAG (HTML)
seeknal lineage transform.sales_enriched     →  Focused node view
seeknal lineage transform.X --column total   →  Column-level trace
seeknal lineage --ascii                      →  ASCII tree to stdout
seeknal inspect transform.sales_enriched     →  Data preview
```

**You'll learn:**
- Interactive HTML lineage visualization with Cytoscape.js
- Focused node and column-level lineage tracing
- ASCII tree output for terminal use and AI agent consumption
- Inspecting intermediate node outputs for debugging
- Schema inspection for column types

**[Start Chapter 4 →](4-lineage.md)**

---

### Chapter 5: Named ref() References (~15 minutes)

Refactor transforms to use self-documenting named references:

```
Before:  SELECT * FROM input_0 s JOIN input_1 p ON ...
After:   SELECT * FROM ref('source.products') p JOIN ref('transform.events_cleaned') e ON ...
```

**You'll learn:**
- Named `ref()` syntax instead of positional `input_0`
- Self-documenting SQL that's safe to reorder
- Mixed syntax and error handling
- Security validation for ref() arguments

**[Start Chapter 5 →](5-named-refs.md)**

---

### Chapter 6: Common Configuration (~20 minutes)

Centralize column mappings, business rules, and SQL snippets:

```
seeknal/common/
├── sources.yml           →  {{products.idCol}}, {{products.priceCol}}
├── rules.yml             →  {{rules.validPrice}}, {{rules.hasQuantity}}
└── transformations.yml   →  {{transforms.priceCalc}}
```

**You'll learn:**
- Source column mappings with `{{ dotted.key }}` syntax
- Reusable SQL filter expressions and snippets
- Resolution priority (context > env > common config)
- Typo detection with suggestions

**[Start Chapter 6 →](6-common-config.md)**

---

### Chapter 7: Data Profiling & Validation (~20 minutes)

Compute statistical profiles and validate with threshold-based quality checks:

```
source.products ──→ profile.products_stats ──→ rule.products_quality
                         │
                         └── row_count, avg, stddev, null_percent,
                             distinct_count, top_values, freshness
```

**You'll learn:**
- Computing 14+ metrics per column with `kind: profile`
- Auto-detection of column types (numeric, timestamp, string)
- Threshold-based quality checks with `type: profile_check` rules
- Soda-style expressions: `"> 5"`, `"= 0"`, `"between 10 and 500"`

**[Start Chapter 7 →](7-data-profiling.md)**

---

### Chapter 8: Python Pipelines (~25 minutes)

Build pipeline nodes using Python decorators and mix them with existing YAML nodes:

```
source.products (YAML) ───────────┐
                                  ├──→ transform.customer_analytics (Python)
transform.sales_enriched (YAML) ──┘
source.exchange_rates (Python) ────────→ transform.category_insights (Python)
```

**You'll learn:**
- Creating Python sources and transforms with `@source` and `@transform`
- PEP 723 per-file dependency isolation
- Referencing YAML nodes from Python via `ctx.ref()`
- Running mixed YAML + Python pipelines

**[Start Chapter 8 →](8-python-pipelines.md)**

---

## Continue Learning

Explore other persona paths or dive into the reference documentation:

| Path | Focus | Time |
|------|-------|------|
| **[Data Engineer →](../data-engineer-path/)** | ELT pipelines, incremental processing, production environments | ~75 min |
| **[Analytics Engineer →](../analytics-engineer-path/)** | Semantic models, business metrics, BI deployment | ~75 min |
| **[ML Engineer →](../ml-engineer-path/)** | Feature stores, aggregations, training-serving parity | ~90 min |

---

## Key Commands You'll Learn

```bash
# Initialize a project
seeknal init --name my-project

# Draft, validate, and apply nodes
seeknal draft source my_source
seeknal draft transform my_transform
seeknal draft rule my_rule
seeknal draft profile my_profile
seeknal draft source my_source --python      # Python source template
seeknal draft transform my_transform --python  # Python transform template
seeknal dry-run draft_source_my_source.yml
seeknal apply draft_source_my_source.yml

# Build and run pipeline
seeknal plan
seeknal run

# Explore data interactively
seeknal repl

# Visualize data lineage
seeknal lineage
seeknal lineage transform.my_transform --column my_col
seeknal lineage --ascii                          # ASCII tree to stdout
seeknal lineage transform.my_transform --ascii   # Focused ASCII tree

# Inspect intermediate outputs
seeknal inspect transform.my_transform

# Preview resolved SQL (ref() and {{ }} expressions)
seeknal dry-run seeknal/transforms/my_transform.yml

# Override common config at runtime
seeknal run --params events.quantityCol=units_sold
```

---

*Last updated: February 2026 | Seeknal Documentation*

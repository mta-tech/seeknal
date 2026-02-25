# Data Engineer Path

**Duration:** ~75 minutes | **Format:** YAML & Python | **Prerequisites:** SQL, CLI experience

Build production-ready ELT pipelines with Seeknal, from data ingestion to production deployment.

---

## What You'll Learn

The Data Engineer path teaches you to build production-grade ELT pipelines with Seeknal. You'll learn to:

1. **Build ELT Pipelines** - Load data from CSV/Parquet sources, transform with DuckDB, save results
2. **Add Incremental Models** - Implement CDC, scheduling, and change data capture
3. **Deploy to Production** - Use virtual environments, promotion workflows, and rollback procedures

---

## Prerequisites

Before starting this path, ensure you have:

- **[Quick Start](../../quick-start/)** completed — Basic pipeline builder workflow
- Basic SQL knowledge (JOINs, window functions, aggregations)
- Familiarity with command line tools
- Understanding of data warehouse concepts

---

## Chapters

### Chapter 1: Build ELT Pipeline (~25 minutes)

Learn to build a complete e-commerce order processing pipeline:

```
CSV Source → Orders (Raw) → Transform → Orders (Clean) → Parquet
```

**You'll build:**
- CSV source that loads order data
- DuckDB transformation with data quality checks
- Clean output saved to Parquet format
- Data validation and deduplication logic

**[Start Chapter 1 →](1-elt-pipeline.md)**

---

### Chapter 2: Add Incremental Models (~30 minutes)

Optimize your pipeline with incremental processing:

```
Source → Incremental Load → Transform → Output
                  ↓
            Change Detection
```

**You'll build:**
- Incremental source that only processes new/changed data
- CDC (Change Data Capture) patterns for detecting updates
- Scheduled pipeline runs for automated processing
- Monitoring and validation for incremental updates

**[Start Chapter 2 →](2-incremental-models.md)**

---

### Chapter 3: Deploy to Production Environments (~35 minutes)

Deploy your pipeline safely using virtual environments:

```
dev → testing → staging → production
```

**You'll learn:**
- Virtual environments for safe development and testing
- Plan-apply-promote workflow for controlled deployments
- Change categorization (breaking vs non-breaking changes)
- Production promotion with approvals and rollback procedures

**[Start Chapter 3 →](3-production-environments.md)**

---

## What You'll Build

By the end of this path, you'll have a complete production-ready pipeline:

| Component | Technology | Purpose |
|-----------|------------|---------|
| **CSV/Parquet Source** | File-based | Load order data |
| **DuckDB Transform** | SQL | Clean, validate, deduplicate |
| **Parquet Output** | Columnar storage | Store processed data |
| **Incremental Loading** | CDC | Only process new data |
| **Scheduling** | Cron | Automated pipeline runs |
| **Virtual Environments** | Isolation | Safe dev → prod workflow |

---

## Key Commands You'll Learn

```bash
# Initialize a new project
seeknal init --name ecommerce-pipeline

# Draft a new source
seeknal draft source orders_data

# Validate and apply to project
seeknal dry-run seeknal/sources/orders_data.yml
seeknal apply seeknal/sources/orders_data.yml

# Run pipeline
seeknal run

# Plan changes for environment
seeknal plan dev

# Apply in isolated environment
seeknal env apply dev

# Promote to production
seeknal env promote dev prod
```

---

## Resources

### Reference
- [CLI Reference](../../reference/cli.md) — All commands and flags
- [YAML Schema Reference](../../reference/yaml-schema.md) — Pipeline YAML reference
- [Troubleshooting](../../reference/troubleshooting.md) — Debug common issues

### Concepts
- [Virtual Environments](../../concepts/virtual-environments.md) — Isolate dev and prod
- [Change Categorization](../../concepts/change-categorization.md) — Breaking vs non-breaking changes
- [Pipeline Builder](../../concepts/pipeline-builder.md) — Core workflow for all pipelines

### Related Paths
- [Analytics Engineer Path](../analytics-engineer-path/) — Build semantic layers and metrics
- [ML Engineer Path](../ml-engineer-path/) — Feature stores and ML pipelines

---

## Getting Help

If you get stuck:
1. Check the [Troubleshooting Guide](../../reference/troubleshooting.md)
2. Review the [CLI Reference](../../reference/cli.md)
3. Run `seeknal <command> --help` for command-specific help

---

*Last updated: February 2026 | Seeknal Documentation*

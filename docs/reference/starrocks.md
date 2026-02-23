# StarRocks Reference

**Coming Soon** - This reference is under development.

## Overview

StarRocks is a fast, real-time analytics database that powers high-concurrency queries and sub-second response times.

## Configuration

### Connection Setup

```yaml
warehouses:
  starrocks:
    type: starrocks
    host: ${STARROCKS_HOST}
    port: 9030
    user: ${STARROCKS_USER}
    password: ${STARROCKS_PASSWORD}
    database: analytics
```

## Commands

### Deploy Semantic Layer

```bash
seeknal deploy-semantic-layer --target starrocks
```

### Refresh Materialized Views

```bash
seeknal refresh-materialized-views
```

### Get Connection String

```bash
seeknal get-connection-string --target starrocks
```

## Resources

- [StarRocks Guide](../guides/starrocks.md) — Comprehensive guide
- [Analytics Engineer Path](../getting-started/analytics-engineer-path/) — Hands-on deployment

---

*Last updated: February 2026 | Seeknal Documentation*

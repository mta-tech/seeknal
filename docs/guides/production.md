# Production Deployment Guide

> **Status:** 📝 Documentation in progress

Best practices for deploying Seeknal pipelines to production environments.

---

## Overview

Deploying data pipelines to production requires careful planning, testing, and monitoring. This guide covers the end-to-end production deployment process for Seeknal pipelines.

---

## Pre-Deployment Checklist

### 1. Pipeline Validation

Before deploying, validate your pipeline:

```bash
# Parse and validate configuration
seeknal parse

# Plan deployment to see what will change
seeknal plan prod

# Dry-run to preview execution
seeknal run --dry-run
```

### 2. Data Quality Checks

```bash
# Run data quality audits
seeknal audit

# Validate feature data (for feature groups)
seeknal validate-features user_features --mode fail
```

### 3. Environment Setup

```bash
# List available environments
seeknal env list

# Create production environment configuration
seeknal plan prod
```

---

## Deployment Strategies

### Virtual Environments (Recommended)

Use virtual environments to isolate development and production:

```bash
# 1. Create production environment plan
seeknal plan prod

# 2. Review the plan
seeknal env plan prod

# 3. Apply in isolated environment
seeknal env apply prod --dry-run

# 4. Execute production deployment
seeknal env apply prod
```

**Benefits**:
- Safe testing before production deployment
- Rollback capability
- Change tracking and categorization

### Direct Deployment

For simpler deployments, you can deploy directly:

```bash
# Deploy to production
seeknal run --env prod
```

---

## Change Categorization

Seeknal automatically categorizes changes by impact:

| Category | Description | Example |
|----------|-------------|---------|
| **Breaking** | Requires manual intervention | Schema changes, dropped columns |
| **Non-Breaking** | Safe to deploy | New features, performance improvements |
| **Additive** | Expands functionality | New sources, transforms |

Review changes before deployment:

```bash
seeknal plan prod
```

---

## Production Best Practices

### 1. Use Source Control

```bash
# Version control your pipeline definitions
git add seeknal/
git commit -m "Deploy v2.0 to production"
git tag production-v2.0
```

### 2. Implement Monitoring

```bash
# Monitor pipeline runs
seeknal logs --pipeline orders_pipeline --tail 50

# Check recent executions
seeknal status
```

### 3. Set Up Alerts

Configure alerts for:
- Pipeline failures
- Data quality issues
- Performance degradation
- Data freshness checks

### 4. Schedule Regular Runs

```yaml
# pipelines/scheduled_orders.yaml
schedule:
  cron: "*/15 * * * *"  # Every 15 minutes
  timezone: UTC
```

```bash
# Deploy scheduled pipeline
seeknal apply pipelines/scheduled_orders.yaml
```

### 5. Implement Rollback Procedures

```bash
# List feature group versions
seeknal version list user_features

# Rollback to previous version
seeknal materialize user_features --start-date 2024-01-01 --version 1

# Rollback environment changes
seeknal env rollback prod
```

---

## Production Considerations

### Performance

| Aspect | Recommendation |
|--------|----------------|
| **Incremental Processing** | Use CDC to only process new data |
| **Parallel Execution** | Enable `--parallel` for independent nodes |
| **Resource Limits** | Set `--max-workers` based on available CPU |

### Security

| Aspect | Recommendation |
|--------|----------------|
| **Credentials** | Use environment variables, never commit secrets |
| **Storage Paths** | Use secure paths (not `/tmp`) |
| **Access Control** | Limit who can promote to production |

### Reliability

| Aspect | Recommendation |
|--------|----------------|
| **Idempotency** | Ensure operations can be safely retried |
| **Validation** | Run data quality checks before and after |
| **Monitoring** | Track pipeline health metrics |

---

## Troubleshooting Production Issues

### Pipeline Failures

```bash
# Check logs
seeknal logs --pipeline <name> --tail 100

# View recent status
seeknal status

# Validate configuration
seeknal validate
```

### Data Quality Issues

```bash
# Run diagnostics
seeknal audit <node>

# Validate features
seeknal validate-features <feature_group> --verbose
```

### Performance Issues

```bash
# Check execution plan
seeknal plan prod

# Enable parallel execution
seeknal env apply prod --parallel
```

---

## Related Topics

- **[Virtual Environments](../concepts/virtual-environments.md)** — Isolate dev and prod
- **[Change Categorization](../concepts/change-categorization.md)** — Understanding change impact
- **[Incident Management](../concepts/incident-management.md)** — Handling production issues

---

## CLI Commands Reference

```bash
# Environment Management
seeknal env plan <env>          # Preview changes
seeknal env apply <env>         # Execute deployment
seeknal env promote <from> <to> # Promote environments
seeknal env list               # List all environments
seeknal env delete <env>        # Delete environment

# Change Management
seeknal plan                    # Show changes since last run
seeknal promote                 # Promote changes to production

# Monitoring
seeknal logs                    # View pipeline logs
seeknal status                  # Check system status
seeknal audit                   # Run data quality audits
```

---

*Coming soon: Advanced deployment patterns, blue-green deployments, and canary release strategies.*
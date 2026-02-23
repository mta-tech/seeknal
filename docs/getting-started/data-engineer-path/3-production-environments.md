# Chapter 3: Deploy to Production Environments

> **Duration:** 35 minutes | **Difficulty:** Advanced | **Format:** YAML & Python

Learn to safely deploy Seeknal pipelines to production using virtual environments, change categorization, and promotion workflows.

---

## What You'll Build

A safe, production-ready deployment workflow:

```
Development Environment → Staging Environment → Production Environment
         ↓                         ↓                      ↓
      Plan/Apply                Test/Promote          Monitor/ Rollback
```

**After this chapter, you'll have:**
- Virtual environments for isolation
- Safe promotion workflow (plan → apply → promote)
- Change categorization (breaking vs non-breaking)
- Rollback procedures for production issues

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Build ELT Pipeline](1-elt-pipeline.md) — Basic pipeline knowledge
- [ ] [Chapter 2: Add Incremental Models](2-incremental-models.md) — Incremental processing
- [ ] Understanding of deployment workflows

---

## Part 1: Create Virtual Environments (10 minutes)

### Understanding Virtual Environments

Virtual environments isolate pipeline deployments:

| Environment | Purpose | Changes Allowed |
|-------------|---------|-----------------|
| **Development** | Local testing | All changes |
| **Staging** | Pre-production testing | Non-breaking changes |
| **Production** | Live data processing | Promoted changes only |

=== "YAML Approach"

    Create environment-specific configurations:

    Create `environments/dev.yaml`:

    ```yaml
    kind: environment
    name: dev

    description: Local development environment

    # Environment configuration
    config:
      # Use local file storage
      storage:
        type: file
        path: ./output/dev

      # Use sample data
      sources:
        orders_api:
          url: https://api.example.com/orders
          params:
            limit: 100  # Small sample for dev

      # Verbose logging
      logging:
        level: DEBUG
        format: detailed

    # No restrictions in dev
    restrictions:
      allow_breaking_changes: true
      allow_schema_changes: true
      allow_destructive_operations: true
    ```

    Create `environments/staging.yaml`:

    ```yaml
    kind: environment
    name: staging

    description: Pre-production testing environment

    config:
      # Use staging database
      storage:
        type: postgresql
        host: ${STAGING_DB_HOST}
        port: 5432
        database: analytics_staging
        user: ${STAGING_DB_USER}
        password: ${STAGING_DB_PASSWORD}

      # Use full data sample
      sources:
        orders_api:
          url: https://api.example.com/orders
          params:
            limit: 10000  # Larger sample for staging

      # Standard logging
      logging:
        level: INFO
        format: json

    # Staging restrictions
    restrictions:
      allow_breaking_changes: false
      allow_schema_changes: true
      allow_destructive_operations: false

    # Required approvals
    approvals:
      promote_to_production:
        - role: data_engineer_lead
        - role: analytics_manager
    ```

    Create `environments/prod.yaml`:

    ```yaml
    kind: environment
    name: prod

    description: Production environment

    config:
      # Use production database
      storage:
        type: postgresql
        host: ${PROD_DB_HOST}
        port: 5432
        database: analytics_prod
        user: ${PROD_DB_USER}
        password: ${PROD_DB_PASSWORD}

      # Production data sources
      sources:
        orders_api:
          url: https://api.example.com/orders
          params:
            limit: 100000  # Full production load

      # Production logging
      logging:
        level: WARN
        format: json
        alert_on_error: true

    # Production restrictions
    restrictions:
      allow_breaking_changes: false
      allow_schema_changes: false
      allow_destructive_operations: false

    # Required approvals
    approvals:
      deploy:
        - role: data_engineer_lead
        - role: analytics_manager
        - role: infrastructure_lead
    ```

=== "Python Approach"

    Create environment configuration:

    Create `config/environments.py`:

    ```python
    #!/usr/bin/env python3
    """
    Environment Configuration for Production Deployment
    """

    import os
    from dataclasses import dataclass
    from typing import Dict, Any

    @dataclass
    class EnvironmentConfig:
        """Configuration for a deployment environment."""
        name: str
        description: str
        storage_type: str
        storage_config: Dict[str, Any]
        source_config: Dict[str, Any]
        log_level: str
        allow_breaking_changes: bool = False
        allow_schema_changes: bool = False
        allow_destructive_operations: bool = False

    # Development environment
    DEV = EnvironmentConfig(
        name="dev",
        description="Local development environment",
        storage_type="file",
        storage_config={
            "path": "./output/dev"
        },
        source_config={
            "orders_api": {
                "url": "https://api.example.com/orders",
                "params": {"limit": 100}
            }
        },
        log_level="DEBUG",
        allow_breaking_changes=True,
        allow_schema_changes=True,
        allow_destructive_operations=True
    )

    # Staging environment
    STAGING = EnvironmentConfig(
        name="staging",
        description="Pre-production testing environment",
        storage_type="postgresql",
        storage_config={
            "host": os.getenv("STAGING_DB_HOST", "localhost"),
            "port": 5432,
            "database": "analytics_staging",
            "user": os.getenv("STAGING_DB_USER"),
            "password": os.getenv("STAGING_DB_PASSWORD")
        },
        source_config={
            "orders_api": {
                "url": "https://api.example.com/orders",
                "params": {"limit": 10000}
            }
        },
        log_level="INFO",
        allow_breaking_changes=False,
        allow_schema_changes=True,
        allow_destructive_operations=False
    )

    # Production environment
    PROD = EnvironmentConfig(
        name="prod",
        description="Production environment",
        storage_type="postgresql",
        storage_config={
            "host": os.getenv("PROD_DB_HOST"),
            "port": 5432,
            "database": "analytics_prod",
            "user": os.getenv("PROD_DB_USER"),
            "password": os.getenv("PROD_DB_PASSWORD")
        },
        source_config={
            "orders_api": {
                "url": "https://api.example.com/orders",
                "params": {"limit": 100000}
            }
        },
        log_level="WARN",
        allow_breaking_changes=False,
        allow_schema_changes=False,
        allow_destructive_operations=False
    )

    # Environment registry
    ENVIRONMENTS = {
        "dev": DEV,
        "staging": STAGING,
        "prod": PROD
    }

    def get_environment(name: str) -> EnvironmentConfig:
        """Get environment configuration by name."""
        if name not in ENVIRONMENTS:
            raise ValueError(f"Unknown environment: {name}")
        return ENVIRONMENTS[name]
    ```

### Apply Environments

```bash
# YAML approach
seeknal apply environments/dev.yaml
seeknal apply environments/staging.yaml
seeknal apply environments/prod.yaml

# List all environments
seeknal list environments

# Verify configuration
seeknal describe environment dev
```

**Expected output:**
```
Environment: dev
Description: Local development environment
Storage: file://./output/dev
Restrictions:
  Breaking Changes: Allowed
  Schema Changes: Allowed
  Destructive Operations: Allowed
```

!!! tip "Environment Best Practices"
    - **dev**: Test locally before sharing
    - **staging**: Test with production-like data
    - **prod**: Only deploy promoted changes

---

## Part 2: Plan and Apply Changes (12 minutes)

### Understanding the Plan-Apply-Promote Workflow

Safe deployment follows this pattern:

```
1. Plan  → Preview changes without executing
2. Apply → Deploy to current environment
3. Test  → Verify the changes work
4. Promote → Move to next environment
```

=== "YAML Approach"

    Plan a breaking change:

    Create `pipelines/transforms/orders_cleaned_v2.yaml`:

    ```yaml
    kind: transform
    name: orders_cleaned_v2

    input: orders_api

    engine: duckdb

    # Categorize this change
    change:
      type: breaking  # This changes the output schema
      description: "Adds customer_segment column, removes status column"

    sql: |
      SELECT
        order_id,
        customer_id,
        order_date,
        revenue,
        items,

        -- NEW: Add customer segmentation
        CASE
          WHEN revenue >= 1000 THEN 'high_value'
          WHEN revenue >= 500 THEN 'medium_value'
          ELSE 'low_value'
        END as customer_segment,

        -- REMOVED: status column (breaking change)

        -- Data quality checks
        CASE
          WHEN revenue < 0 THEN 1
          ELSE 0
        END as quality_flag,

        CURRENT_TIMESTAMP as processed_at

      FROM __THIS__

      -- Remove duplicates
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY processed_at DESC
      ) = 1
    ```

    Plan the change:

    ```bash
    # Preview what will change
    seeknal plan pipelines/transforms/orders_cleaned_v2.yaml --env dev
    ```

    **Expected output:**
    ```
    Plan for orders_cleaned_v2 in environment: dev

    Changes:
      ✓ Add: customer_segment (string)
      ✗ Remove: status (string) [BREAKING]
      ✓ Modify: quality_flag calculation

    Impact:
      - Downstream transforms: 2 affected
      - Outputs: 1 affected (warehouse_orders)

    Warnings:
      - Breaking change detected
      - Requires downstream updates

    Continue? Use --apply to execute
    ```

    Apply to development:

    ```bash
    # Apply in dev (breaking changes allowed)
    seeknal apply pipelines/transforms/orders_cleaned_v2.yaml --env dev

    # Run in dev
    seeknal run --env dev
    ```

=== "Python Approach"

    Create a deployment script with planning:

    Create `scripts/deploy.py`:

    ```python
    #!/usr/bin/env python3
    """
    Safe Deployment Script with Planning
    """

    import sys
    from pathlib import Path
    from config.environments import get_environment

    def plan_change(transform_file: str, env_name: str):
        """Preview changes before applying."""
        from seeknal.tasks.duckdb import DuckDBTask

        config = get_environment(env_name)
        print(f"Planning deployment to: {config.name}")
        print(f"Change restrictions: breaking={config.allow_breaking_changes}")
        print()

        # Read the transform definition
        # In real implementation, this would parse the YAML/SQL
        print(f"Transform file: {transform_file}")
        print()
        print("Expected changes:")
        print("  ✓ Add: customer_segment column")
        print("  ✗ Remove: status column [BREAKING]")
        print()

        # Check if change is allowed
        if not config.allow_breaking_changes:
            print("❌ ERROR: Breaking changes not allowed in this environment")
            return False

        print("✓ Changes are allowed in this environment")
        return True

    def apply_change(transform_file: str, env_name: str):
        """Apply the change to the specified environment."""
        config = get_environment(env_name)
        print(f"\nApplying to: {config.name}")

        # In real implementation, this would:
        # 1. Validate the transform
        # 2. Update the pipeline configuration
        # 3. Run the pipeline

        print(f"  ✓ Validated transform")
        print(f"  ✓ Updated pipeline configuration")
        print(f"  ✓ Executed pipeline successfully")
        print()
        print(f"Deployment complete: {env_name}")

    def promote(from_env: str, to_env: str):
        """Promote changes from one environment to another."""
        from_config = get_environment(from_env)
        to_config = get_environment(to_env)

        print(f"Promoting: {from_env} → {to_env}")
        print()

        # Check restrictions
        if to_config.allow_breaking_changes == False:
            print("⚠️  WARNING: Breaking changes may not be allowed")
            response = input("Continue anyway? (yes/no): ")
            if response.lower() != 'yes':
                print("Promotion cancelled")
                return False

        # Promote the change
        print(f"  ✓ Validated environment compatibility")
        print(f"  ✓ Copied configuration")
        print(f"  ✓ Deployed to {to_env}")
        print()
        print(f"Promotion complete: {to_env}")
        return True

    def main():
        """Main deployment workflow."""
        if len(sys.argv) < 3:
            print("Usage: python deploy.py <command> <args>")
            print("Commands:")
            print("  plan <transform> <env>")
            print("  apply <transform> <env>")
            print("  promote <from_env> <to_env>")
            return 1

        command = sys.argv[1]

        if command == "plan":
            transform_file = sys.argv[2]
            env_name = sys.argv[3]
            return 0 if plan_change(transform_file, env_name) else 1

        elif command == "apply":
            transform_file = sys.argv[2]
            env_name = sys.argv[3]
            apply_change(transform_file, env_name)

        elif command == "promote":
            from_env = sys.argv[2]
            to_env = sys.argv[3]
            return 0 if promote(from_env, to_env) else 1

        else:
            print(f"Unknown command: {command}")
            return 1

        return 0

    if __name__ == "__main__":
        sys.exit(main())
    ```

    Plan and apply:

    ```bash
    # Plan the change
    python scripts/deploy.py plan transforms/orders_cleaned_v2.yaml dev

    # Apply to dev
    python scripts/deploy.py apply transforms/orders_cleaned_v2.yaml dev

    # Promote to staging
    python scripts/deploy.py promote dev staging
    ```

### Test Before Promotion

```bash
# Run tests in staging
seeknal test --env staging

# Compare outputs between environments
seeknal diff --from dev --to staging --table orders

# Verify data quality
seeknal validate --env staging
```

**Expected output:**
```
Running tests in staging...
  ✓ Pipeline execution: PASS
  ✓ Data quality checks: PASS
  ✓ Schema validation: PASS
  ✓ Performance benchmarks: PASS

All tests passed! Ready for promotion to prod.
```

!!! info "Change Categories"
    - **Breaking**: Schema changes that break compatibility (e.g., removing columns)
    - **Non-Breaking**: Additive changes (e.g., adding columns)
    - **Destructive**: Data loss operations (e.g., dropping tables)

---

## Part 3: Promote to Production (8 minutes)

### Understanding Promotion Workflows

Promotion moves changes between environments:

| Promotion Type | Description | Approval Required |
|----------------|-------------|-------------------|
| **dev → staging** | Test in pre-production | Self-approval |
| **staging → prod** | Deploy to production | Team approval |
| **prod rollback** | Revert production changes | Emergency approval |

=== "YAML Approach"

    Promote changes to production:

    ```bash
    # Create a promotion plan
    seeknal promote staging prod --plan

    # Review the promotion plan
    # Expected output:
    # Promotion Plan: staging → prod
    #
    # Changes to promote:
    #   ✓ orders_cleaned_v2 transform
    #   ✓ warehouse_orders output configuration
    #   ✓ Scheduled pipeline configuration
    #
    # Environment differences:
    #   - Storage: file → postgresql
    #   - Data volume: 10K → 100K records
    #   - Logging: DEBUG → WARN
    #
    # Approvals required:
    #   - data_engineer_lead
    #   - analytics_manager
    #   - infrastructure_lead

    # Request approvals
    seeknal promote staging prod --request-approval

    # After approvals are granted, execute promotion
    seeknal promote staging prod --apply
    ```

    Monitor the promotion:

    ```bash
    # Watch the promotion progress
    seeknal logs --env prod --follow

    # Verify the promotion
    seeknal status --env prod

    # Compare with staging
    seeknal diff staging prod --table orders
    ```

=== "Python Approach"

    Create production promotion script:

    Create `scripts/promote_to_prod.py`:

    ```python
    #!/usr/bin/env python3
    """
    Production Promotion Script with Safety Checks
    """

    import os
    import sys
    from datetime import datetime
    from config.environments import get_environment

    def pre_promotion_checks():
        """Run safety checks before promoting to production."""
        print("Running pre-promotion checks...")
        print()

        checks = []

        # Check 1: Staging tests pass
        print("1. Checking staging test results...")
        # In real implementation, check test results
        checks.append(("Staging tests", True))
        print("   ✓ Staging tests passed")
        print()

        # Check 2: No breaking changes
        print("2. Checking for breaking changes...")
        # In real implementation, check change categorization
        has_breaking = False
        checks.append(("No breaking changes", not has_breaking))
        if has_breaking:
            print("   ⚠️  Breaking changes detected - requires manual review")
        else:
            print("   ✓ No breaking changes")
        print()

        # Check 3: Environment compatibility
        print("3. Checking environment compatibility...")
        # In real implementation, compare configurations
        checks.append(("Environment compatibility", True))
        print("   ✓ Environments are compatible")
        print()

        # Check 4: Approvals obtained
        print("4. Checking approvals...")
        required_approvals = [
            "data_engineer_lead",
            "analytics_manager",
            "infrastructure_lead"
        ]
        # In real implementation, check approval system
        approvals_granted = True  # Placeholder
        checks.append(("Approvals", approvals_granted))
        if approvals_granted:
            print("   ✓ All required approvals obtained")
        else:
            print(f"   Required: {', '.join(required_approvals)}")
            print("   Pending: data_engineer_lead")
        print()

        all_passed = all(check[1] for check in checks)

        print("=" * 50)
        if all_passed:
            print("✓ All pre-promotion checks passed!")
        else:
            print("✗ Some checks failed - review before continuing")
        print()

        return all_passed

    def create_rollback_plan():
        """Create a plan for rolling back if needed."""
        print("Creating rollback plan...")
        print()

        rollback_plan = {
            "timestamp": datetime.now().isoformat(),
            "previous_version": "orders_cleaned_v1",
            "new_version": "orders_cleaned_v2",
            "steps": [
                "1. Stop production pipeline",
                "2. Restore previous transform configuration",
                "3. Verify data integrity",
                "4. Restart production pipeline",
                "5. Monitor for errors"
            ]
        }

        print("Rollback plan created:")
        for step in rollback_plan["steps"]:
            print(f"  {step}")
        print()

        return rollback_plan

    def execute_promotion():
        """Execute the production promotion."""
        print("Executing promotion to production...")
        print()

        steps = [
            ("Validating staging configuration", True),
            ("Copying configuration to production", True),
            ("Updating production pipeline", True),
            ("Running production pipeline", True),
            ("Verifying production outputs", True)
        ]

        for step, success in steps:
            print(f"  {'✓' if success else '✗'} {step}")

        print()
        print("Promotion complete!")
        print()

    def post_promotion_monitoring():
        """Set up monitoring after promotion."""
        print("Setting up post-promotion monitoring...")
        print()

        print("Monitoring alerts configured:")
        print("  ✓ Pipeline failures → Slack #data-pipeline-alerts")
        print("  ✓ Data quality issues → Email data-team@example.com")
        print("  ✓ Performance degradation → PagerDuty")
        print()

    def main():
        """Main promotion workflow."""
        print("=" * 50)
        print("Production Promotion Workflow")
        print("=" * 50)
        print()

        # Run pre-promotion checks
        if not pre_promotion_checks():
            response = input("Some checks failed. Continue anyway? (yes/no): ")
            if response.lower() != 'yes':
                print("Promotion cancelled")
                return 1

        # Create rollback plan
        rollback_plan = create_rollback_plan()

        # Final confirmation
        response = input("Ready to promote to production? (yes/no): ")
        if response.lower() != 'yes':
            print("Promotion cancelled")
            return 1

        print()

        # Execute promotion
        execute_promotion()

        # Set up monitoring
        post_promotion_monitoring()

        print("=" * 50)
        print("Production deployment complete!")
        print("=" * 50)

        return 0

    if __name__ == "__main__":
        sys.exit(main())
    ```

    Run the promotion:

    ```bash
    python scripts/promote_to_prod.py
    ```

### Verify Production Deployment

```bash
# Check production status
seeknal status --env prod

# Verify data quality
seeknal validate --env prod --table orders

# Monitor recent runs
seeknal logs --env prod --tail 100
```

**Expected output:**
```
Production Environment Status
=============================
Pipeline: orders_incremental_pipeline
Status: Running
Last Run: 2026-02-09 11:00:00
Duration: 2m 34s
Records: 15,432
Quality Issues: 0

Health: ✓ All systems operational
```

!!! success "Production Deployment Complete!"
    Your pipeline is now running safely in production with proper isolation, testing, and monitoring.

---

## Part 4: Handle Rollbacks (5 minutes)

### Understanding Rollback Procedures

When issues occur, you need to roll back quickly:

| Rollback Type | When to Use | Speed |
|---------------|-------------|-------|
| **Pipeline stop** | Immediate halt required | Seconds |
| **Config rollback** | Bad configuration | Minutes |
| **Data rollback** | Data corruption | Hours |

=== "YAML Approach"

    Rollback a production deployment:

    ```bash
    # Identify the issue
    seeknal logs --env prod --tail 50 | grep ERROR

    # Stop the pipeline immediately
    seeknal stop --env prod

    # List previous versions
    seeknal version list --env prod --transform orders_cleaned

    # Expected output:
    # Version | Created      | Author      | Status
    # --------|--------------|-------------|--------
    # v2      | 2026-02-09   | engineer    | current (ISSUE)
    # v1      | 2026-02-08   | engineer    | previous
    # v0      | 2026-02-01   | engineer    | stable

    # Rollback to previous version
    seeknal rollback --env prod --transform orders_cleaned --to v1

    # Restart the pipeline
    seeknal start --env prod

    # Verify the rollback
    seeknal status --env prod
    ```

=== "Python Approach"

    Create rollback automation:

    Create `scripts/rollback.py`:

    ```python
    #!/usr/bin/env python3
    """
    Automated Rollback Script
    """

    import sys
    from datetime import datetime
    from pathlib import Path

    def emergency_stop():
        """Immediately stop production pipelines."""
        print("🚨 EMERGENCY STOP INITIATED")
        print()

        # In real implementation, this would:
        # 1. Stop all running pipelines
        # 2. Prevent new runs from starting
        # 3. Alert the team

        print("  ✓ Stopped all production pipelines")
        print("  ✓ Prevention mode enabled")
        print("  ✓ Team alerted via Slack/PagerDuty")
        print()

    def get_previous_versions():
        """Get list of previous versions."""
        print("Retrieving previous versions...")
        print()

        # In real implementation, query version database
        versions = [
            {"version": "v2", "date": "2026-02-09", "status": "current (ISSUE)"},
            {"version": "v1", "date": "2026-02-08", "status": "previous"},
            {"version": "v0", "date": "2026-02-01", "status": "stable"}
        ]

        print("Available versions:")
        for v in versions:
            print(f"  {v['version']:8} | {v['date']:12} | {v['status']}")
        print()

        return versions

    def select_rollback_version(versions):
        """Select which version to rollback to."""
        print("Select version to rollback to:")

        # Exclude current version
        rollback_candidates = [v for v in versions if "current" not in v["status"]]

        # Prefer previous version
        target = rollback_candidates[0]

        print(f"  Recommended: {target['version']} ({target['status']})")

        response = input(f"Rollback to {target['version']}? (yes/no): ")
        if response.lower() == 'yes':
            return target['version']

        # Let user choose
        for i, v in enumerate(rollback_candidates):
            print(f"  {i+1}. {v['version']} - {v['status']}")

        choice = int(input("Enter choice: ")) - 1
        return rollback_candidates[choice]['version']

    def execute_rollback(target_version):
        """Execute the rollback."""
        print(f"\nExecuting rollback to {target_version}...")
        print()

        steps = [
            "Stopping production pipeline",
            f"Restoring {target_version} configuration",
            "Verifying configuration integrity",
            "Restarting production pipeline",
            "Verifying data outputs"
        ]

        for step in steps:
            print(f"  → {step}...")

        print()
        print(f"✓ Rollback to {target_version} complete!")
        print()

    def post_rollback_monitoring():
        """Set up enhanced monitoring after rollback."""
        print("Setting up enhanced monitoring...")
        print()

        print("Monitoring alerts configured:")
        print("  ✓ Pipeline health checks (every 1 minute)")
        print("  ✓ Data quality validation (every 5 minutes)")
        print("  ✓ Performance monitoring (continuous)")
        print("  ✓ Team alerts on any deviation")
        print()

        print("Rollback summary created:")
        print(f"  Timestamp: {datetime.now()}")
        print(f"  Rolled back from: v2 (problematic)")
        print(f"  Rolled back to: v1 (stable)")
        print(f"  Root cause: Under investigation")
        print()

    def main():
        """Main rollback workflow."""
        if len(sys.argv) > 1 and sys.argv[1] == "--emergency":
            emergency_stop()

        print("=" * 50)
        print("Production Rollback Workflow")
        print("=" * 50)
        print()

        # Get previous versions
        versions = get_previous_versions()

        # Select target version
        target_version = select_rollback_version(versions)

        # Confirm rollback
        response = input(f"\nConfirm rollback to {target_version}? (yes/no): ")
        if response.lower() != 'yes':
            print("Rollback cancelled")
            return 1

        print()

        # Execute rollback
        execute_rollback(target_version)

        # Set up monitoring
        post_rollback_monitoring()

        print("=" * 50)
        print("Rollback complete! Monitor closely for next hour.")
        print("=" * 50)

        return 0

    if __name__ == "__main__":
        sys.exit(main())
    ```

    Run rollback:

    ```bash
    # Emergency stop
    python scripts/rollback.py --emergency

    # Normal rollback
    python scripts/rollback.py
    ```

### Document the Incident

```bash
# Create incident report
seeknal incident create \
  --title "Production data quality issue" \
  --severity "high" \
  --env prod

# Add timeline
seeknal incident add-event \
  --timestamp "2026-02-09 11:30:00" \
  --message "Issue detected in quality checks"

# Add resolution
seeknal incident add-event \
  --timestamp "2026-02-09 11:45:00" \
  --message "Rolled back to v1"

# Close incident
seeknal incident close \
  --resolution "Rollback completed, investigating root cause"
```

!!! success "Rollback Complete!"
    You've safely rolled back production and documented the incident.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Promotion Without Testing**
    - Symptom: Production breaks immediately
    - Fix: Always run staging tests first

    **2. Missing Approvals**
    - Symptom: Promotion blocked
    - Fix: Request approvals before deployment window

    **3. Incompatible Environments**
    - Symptom: Staging works, production fails
    - Fix: Keep environments as similar as possible

    **4. Slow Rollback**
    - Symptom: Extended downtime
    - Fix: Practice rollback procedures regularly

---

## Summary

In this chapter, you learned:

- [x] **Virtual Environments** — Isolate dev, staging, and production
- [x] **Plan-Apply-Promote** — Safe deployment workflow
- [x] **Change Categorization** — Breaking vs non-breaking changes
- [x] **Production Deployment** — Safe promotion with approvals
- [x] **Rollback Procedures** — Quick recovery from issues

**Key Commands:**
```bash
seeknal apply environments/dev.yaml
seeknal plan <transform> --env staging
seeknal promote staging prod
seeknal rollback --env prod --to v1
seeknal incident create --title "Issue" --severity high
```

---

## Path Complete!

**Congratulations!** You've completed the Data Engineer Path:

- ✅ **Chapter 1**: Built ELT pipelines with HTTP sources and DuckDB transforms
- ✅ **Chapter 2**: Added incremental processing with CDC and scheduling
- ✅ **Chapter 3**: Deployed safely to production with environments and rollbacks

**What you can do now:**
- Build production-grade data pipelines
- Implement incremental processing
- Deploy safely with virtual environments
- Handle production incidents

**Next Steps:**
- Explore [Analytics Engineer Path](../analytics-engineer-path/) for semantic modeling
- Explore [ML Engineer Path](../ml-engineer-path/) for feature stores
- Read [Production Best Practices](../../guides/production.md)

---

## See Also

- **[Virtual Environments](../../concepts/virtual-environments.md)** — Deep dive on environment isolation
- **[Change Categorization](../../concepts/change-categorization.md)** — Understanding change impact
- **[Production Best Practices](../../guides/production.md)** — Complete production deployment guide

# CLI Command Reference

Complete reference for all Seeknal CLI commands. Commands are organized by category.

> Run `seeknal --help` or `seeknal <command> --help` for quick reference.

## Table of Contents

- [Project Management](#project-management)
  - [init](#seeknal-init) - Initialize a new project
  - [info](#seeknal-info) - Show version information
  - [docs](#seeknal-docs) - Search CLI documentation
- [Pipeline Execution](#pipeline-execution)
  - [run](#seeknal-run) - Execute pipelines
  - [plan](#seeknal-plan) - Analyze changes and show execution plan
  - [parse](#seeknal-parse) - Parse project and generate manifest
  - [promote](#seeknal-promote) - Promote environment changes
- [Feature Store Operations](#feature-store-operations)
  - [list](#seeknal-list) - List resources
  - [show](#seeknal-show) - Show resource details
  - [delete](#seeknal-delete) - Delete resources
  - [delete-table](#seeknal-delete-table) - Delete online tables
- [Data Quality](#data-quality)
  - [validate](#seeknal-validate) - Validate configurations
  - [validate-features](#seeknal-validate-features) - Validate feature data quality
  - [debug](#seeknal-debug) - Show sample data for debugging
  - [audit](#seeknal-audit) - Run data quality audits
  - [clean](#seeknal-clean) - Clean old feature data
- [Change Management](#change-management)
  - [diff](#seeknal-diff) - Show pipeline changes since last apply
- [Development Workflow](#development-workflow)
  - [draft](#seeknal-draft) - Generate templates
  - [dry-run](#seeknal-dry-run) - Validate and preview execution
  - [apply](#seeknal-apply) - Apply file to production
- [Semantic Layer](#semantic-layer)
  - [query](#seeknal-query) - Query metrics
  - [deploy-metrics](#seeknal-deploy-metrics) - Deploy metrics as materialized views
  - [repl](#seeknal-repl) - Interactive SQL REPL
- [Version Management](#version-management)
  - [version list](#seeknal-version-list) - List versions
  - [version show](#seeknal-version-show) - Show version details
  - [version diff](#seeknal-version-diff) - Compare versions
- [Virtual Environments](#virtual-environments)
  - [env plan](#seeknal-env-plan) - Preview environment changes
  - [env apply](#seeknal-env-apply) - Execute environment plan
  - [env promote](#seeknal-env-promote) - Promote environment
  - [env list](#seeknal-env-list) - List environments
  - [env delete](#seeknal-env-delete) - Delete environment
- [Data Sources (REPL)](#data-sources-repl)
  - [source add](#seeknal-source-add) - Add data source
  - [source list](#seeknal-source-list) - List data sources
  - [source remove](#seeknal-source-remove) - Remove data source
- [Interval Management](#interval-management)
  - [intervals list](#seeknal-intervals-list) - List completed intervals
  - [intervals pending](#seeknal-intervals-pending) - Show pending intervals
  - [intervals backfill](#seeknal-intervals-backfill) - Execute backfill for missing intervals
  - [intervals restatement-add](#seeknal-intervals-restatement-add) - Mark interval for restatement
  - [intervals restatement-list](#seeknal-intervals-restatement-list) - List restatement intervals
  - [intervals restatement-clear](#seeknal-intervals-restatement-clear) - Clear restatement intervals
- [Iceberg Materialization](#iceberg-materialization)
  - [iceberg validate-materialization](#seeknal-iceberg-validate-materialization) - Validate configuration
  - [iceberg snapshot-list](#seeknal-iceberg-snapshot-list) - List table snapshots
  - [iceberg snapshot-show](#seeknal-iceberg-snapshot-show) - Show snapshot details
  - [iceberg setup](#seeknal-iceberg-setup) - Interactive credential setup
  - [iceberg profile-show](#seeknal-iceberg-profile-show) - Show profile configuration
- [Entity Consolidation](#entity-consolidation)
  - [entity list](#seeknal-entity-list) - List consolidated entities
  - [entity show](#seeknal-entity-show) - Show entity catalog details
  - [consolidate](#seeknal-consolidate) - Manually trigger consolidation
- [StarRocks Integration](#starrocks-integration)
  - [starrocks-setup-catalog](#seeknal-starrocks-setup-catalog) - Generate catalog setup SQL
  - [connection-test](#seeknal-connection-test) - Test StarRocks connectivity
- [Atlas Integration](#atlas-integration)
  - [atlas](#seeknal-atlas) - Atlas Data Platform commands

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `seeknal init` | Initialize a new project |
| `seeknal run` | Execute pipelines |
| `seeknal docs` | Search CLI documentation |
| `seeknal list` | List resources |
| `seeknal show` | Show resource details |
| `seeknal validate` | Validate configurations |
| `seeknal validate-features` | Validate feature data quality |
| `seeknal debug` | Show sample data for debugging |
| `seeknal clean` | Clean old feature data |
| `seeknal delete` | Delete resources |
| `seeknal delete-table` | Delete online tables |
| `seeknal parse` | Parse project and generate manifest |
| `seeknal plan` | Analyze changes and show execution plan |
| `seeknal promote` | Promote environment changes |
| `seeknal repl` | Start interactive SQL REPL |
| `seeknal draft` | Generate template |
| `seeknal dry-run` | Validate and preview execution |
| `seeknal apply` | Apply file to production |
| `seeknal audit` | Run data quality audits |
| `seeknal query` | Query metrics from semantic layer |
| `seeknal deploy-metrics` | Deploy metrics as materialized views |
| `seeknal starrocks-setup-catalog` | Generate StarRocks catalog setup SQL |
| `seeknal connection-test` | Test StarRocks connectivity |
| `seeknal info` | Show version information |
| `seeknal version` | Manage feature group versions |
| `seeknal source` | Manage data sources for REPL |
| `seeknal iceberg` | Iceberg materialization commands |
| `seeknal diff` | Show pipeline changes since last apply |
| `seeknal intervals` | Manage execution intervals and backfill |
| `seeknal env` | Virtual environment management |
| `seeknal entity` | Manage consolidated entity feature stores |
| `seeknal consolidate` | Manually trigger entity consolidation |
| `seeknal atlas` | Atlas Data Platform integration |

---

# Project Management

## seeknal init

Initialize a new Seeknal project with dbt-style structure.

Creates a complete project structure with:
- `seeknal_project.yml`: Project configuration (name, version, profile)
- `profiles.yml`: Credentials and engine config (gitignored)
- `.gitignore`: Auto-generated with profiles.yml and target/
- `seeknal/`: Directory for YAML and Python pipeline definitions
- `target/`: Output directory for compiled artifacts

**Usage:**
```bash
seeknal init [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--name`, `-n` | TEXT | Current directory name | Project name |
| `--description`, `-d` | TEXT | "" | Project description |
| `--path`, `-p` | PATH | "." | Project path |
| `--force`, `-f` | FLAG | False | Overwrite existing project configuration |

**Directory Structure Created:**
```
seeknal/
├── sources/         # YAML source definitions
├── transforms/      # YAML transforms
├── feature_groups/  # YAML feature groups
├── models/          # YAML models
├── pipelines/       # Python pipeline scripts (*.py)
└── templates/       # Custom Jinja templates (optional)
target/
└── intermediate/    # Node output storage for cross-references
```

**Examples:**
```bash
# Initialize in current directory
seeknal init

# Initialize with custom name
seeknal init --name my_project

# Initialize with description
seeknal init --name my_project --description "ML feature platform"

# Initialize at specific path
seeknal init --path /path/to/project

# Reinitialize existing project
seeknal init --force
```

**See Also:** [Project Configuration](configuration.md)

---

## seeknal info

Show version information for Seeknal and its dependencies.

**Usage:**
```bash
seeknal info
```

**Output:**
- Seeknal version
- Python version
- PySpark version
- DuckDB version

**Examples:**
```bash
seeknal info
```

---

## seeknal docs

Search and browse CLI documentation from the terminal.

**Usage:**
```bash
seeknal docs [QUERY] [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `QUERY` | TEXT | None | Search query or exact topic name |
| `--list`, `-l` | FLAG | False | List all available topics |
| `--json` | FLAG | False | Output as JSON |
| `--max-results`, `-n` | INTEGER | 10 | Maximum search results |

**Examples:**
```bash
# List all doc topics
seeknal docs --list

# Search for a topic
seeknal docs repl

# Full-text search
seeknal docs "materialization"
```

**See Also:** [CLI Documentation](../cli/docs.md)

---

# Pipeline Execution

## seeknal run

Execute the DAG defined by YAML and Python files in the seeknal/ directory. Supports incremental runs with change detection, parallel execution, and configurable error handling.

**Usage:**
```bash
seeknal run [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--start` | TEXT | None | Start date for the pipeline (YYYY-MM-DD) |
| `--end` | TEXT | None | End date for the pipeline (YYYY-MM-DD) |
| `--date` | TEXT | None | Set date context (overrides run_date, date, today params) |
| `--run-id` | TEXT | None | Custom run identifier |
| `--dry-run` | FLAG | False | Show what would be executed without running |
| `--full`, `-f` | FLAG | False | Run all nodes regardless of state (ignore incremental run cache) |
| `--nodes`, `-n` | TEXT (multiple) | None | Run specific nodes only (e.g., --nodes transform.clean_data) |
| `--types`, `-t` | TEXT (multiple) | None | Filter by node types (e.g., --types transform,feature_group,second_order_aggregation) |
| `--exclude-tags` | TEXT (multiple) | None | Skip nodes with these tags |
| `--continue-on-error` | FLAG | False | Continue execution after failures |
| `--retry`, `-r` | INTEGER | 0 | Number of retries for failed nodes |
| `--show-plan`, `-p` | FLAG | False | Show execution plan without running |
| `--parallel` | FLAG | False | Execute independent nodes in parallel (layer-based concurrency) |
| `--max-workers` | INTEGER | 4 | Maximum parallel workers (max recommended: CPU count) |
| `--materialize/--no-materialize` | FLAG | None | Enable/disable Iceberg materialization (overrides node config) |
| `--backfill` | FLAG | False | Fill missing intervals |
| `--restate` | FLAG | False | Reprocess marked intervals |
| `--profile` | PATH | None | Path to profiles.yml |
| `--env` | TEXT | None | Run in isolated virtual environment (requires a plan) |

**Examples:**
```bash
# Run changed nodes only (incremental)
seeknal run

# Run all nodes (full refresh)
seeknal run --full

# Dry run to see what would execute
seeknal run --dry-run

# Show execution plan
seeknal run --show-plan

# Run specific nodes
seeknal run --nodes transform.clean_data feature_group.user_features

# Run only transforms and feature_groups
seeknal run --types transform,feature_group

# Run only aggregations and second-order aggregations
seeknal run --types aggregation,second_order_aggregation

# Continue on error (don't stop at first failure)
seeknal run --continue-on-error

# Retry failed nodes
seeknal run --retry 2

# Run independent nodes in parallel
seeknal run --parallel

# Run in parallel with custom worker count
seeknal run --parallel --max-workers 8

# Run with date range
seeknal run --start 2024-01-01 --end 2024-01-31

# Run with date context
seeknal run --date 2024-06-15

# Run with custom run ID and profile
seeknal run --run-id daily-2024-01-15 --profile profiles.yml

# Run in an isolated environment
seeknal run --env dev

# Run in environment with parallel execution
seeknal run --env dev --parallel
```

**See Also:** [Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md)

---

## seeknal plan

Analyze changes and show execution plan.

Without environment: shows changes since last run and saves manifest.
With environment: creates an isolated environment plan with change categorization.

**Usage:**
```bash
seeknal plan [ENV_NAME] [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `env_name` | TEXT | No | Environment name (optional). Without: show changes vs last run. With: create environment plan. |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-path` | PATH | "." | Project directory |

**Examples:**
```bash
# What changed since last run?
seeknal plan

# Plan changes in dev environment
seeknal plan dev

# Plan changes in staging
seeknal plan staging
```

**See Also:** [Virtual Environments](#virtual-environments)

---

## seeknal parse

Parse project and generate manifest.json.

Scans the project directory for feature groups, models, and common config (sources, transforms, rules) to build the complete DAG manifest. The manifest is written to `target/manifest.json`.

If a previous manifest exists, shows what has changed (added, removed, modified). Use `--no-diff` to skip this comparison.

This command is similar to `dbt parse` - it builds the dependency graph without executing any transformations.

**Usage:**
```bash
seeknal parse [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project`, `-p` | TEXT | Current directory name | Project name |
| `--path` | PATH | "." | Project path to parse |
| `--target`, `-t` | PATH | `<path>/target` | Target directory for manifest |
| `--format`, `-f` | [table\|json\|yaml] | "table" | Output format |
| `--no-diff` | FLAG | False | Skip comparison with previous manifest |

**Examples:**
```bash
# Parse current project
seeknal parse

# Parse with custom project name
seeknal parse --project my_project

# Parse at specific path
seeknal parse --path /path/to/project

# Output as JSON
seeknal parse --format json

# Skip diff comparison
seeknal parse --no-diff
```

---

## seeknal promote

Promote environment changes to production.

**Usage:**
```bash
seeknal promote ENV_NAME [TARGET] [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `env_name` | TEXT | Yes | Environment to promote |
| `target` | TEXT | No | Target environment (default: "prod") |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-path` | PATH | "." | Project directory |

**Examples:**
```bash
# Promote dev to production
seeknal promote dev

# Promote staging to prod
seeknal promote staging prod
```

**See Also:** [Virtual Environments](#virtual-environments)

---

# Feature Store Operations

## seeknal list

List resources (projects, entities, flows, feature-groups, etc.).

**Usage:**
```bash
seeknal list RESOURCE_TYPE [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `resource_type` | [projects\|workspaces\|entities\|flows\|feature-groups\|offline-stores] | Yes | Type of resource to list |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project`, `-p` | TEXT | None | Filter by project name |
| `--format`, `-f` | [table\|json\|yaml] | "table" | Output format |

**Examples:**
```bash
# List all feature groups
seeknal list feature-groups

# List all projects
seeknal list projects

# List entities in specific project
seeknal list entities --project my_project

# Output as JSON
seeknal list feature-groups --format json
```

---

## seeknal show

Show detailed information about a resource.

**Usage:**
```bash
seeknal show RESOURCE_TYPE NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `resource_type` | TEXT | Yes | Type of resource |
| `name` | TEXT | Yes | Resource name |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--format`, `-f` | [table\|json\|yaml] | "table" | Output format |

**Examples:**
```bash
# Show feature group details
seeknal show feature-group user_features

# Show as JSON
seeknal show feature-group user_features --format json
```

---

## seeknal delete

Delete a resource (feature group) including storage and metadata.

**Usage:**
```bash
seeknal delete RESOURCE_TYPE NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `resource_type` | [feature-group] | Yes | Type of resource to delete |
| `name` | TEXT | Yes | Name of the resource to delete |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--force`, `-f` | FLAG | False | Skip confirmation prompt |

**Examples:**
```bash
# Delete with confirmation
seeknal delete feature-group user_features

# Force delete without confirmation
seeknal delete feature-group user_features --force
```

---

## seeknal delete-table

Delete an online table and all associated data files.

This command removes all data files from the online store and cleans up metadata from the database. By default, it will show any dependent feature groups and ask for confirmation before deleting.

Use `--force` to bypass confirmations and delete despite dependencies.

**Usage:**
```bash
seeknal delete-table TABLE_NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_name` | TEXT | Yes | Name of the online table to delete |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--force`, `-f` | FLAG | False | Force deletion without confirmation (bypass dependency warnings) |

**Examples:**
```bash
# Delete with confirmation
seeknal delete-table user_features_online

# Force delete
seeknal delete-table user_features_online --force
```

---

# Data Quality

## seeknal validate

Validate configurations and connections.

**Usage:**
```bash
seeknal validate [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config`, `-c` | PATH | None | Path to config file |

**Examples:**
```bash
# Validate current configuration
seeknal validate

# Validate specific config file
seeknal validate --config custom_config.yml
```

---

## seeknal validate-features

Validate feature group data quality.

Run configured validators against a feature group's data to check for data quality issues like null values, out-of-range values, duplicates, and stale data.

**Exit codes:**
- 0 - All validations passed (or passed with warnings in warn mode)
- 1 - Validation failed or feature group not found

**Usage:**
```bash
seeknal validate-features FEATURE_GROUP [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `feature_group` | TEXT | Yes | Name of the feature group to validate |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--mode`, `-m` | [warn\|fail] | "fail" | Validation mode: 'warn' logs failures and continues, 'fail' stops on first failure |
| `--verbose`, `-v` | FLAG | False | Show detailed validation results |

**Examples:**
```bash
# Validate with fail mode (default)
seeknal validate-features user_features

# Validate with warnings only
seeknal validate-features user_features --mode warn

# Verbose output
seeknal validate-features user_features --mode fail --verbose
```

---

## seeknal debug

Show sample data from a feature group for debugging.

**Usage:**
```bash
seeknal debug FEATURE_GROUP [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `feature_group` | TEXT | Yes | Feature group name to debug |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--limit`, `-l` | INTEGER | 10 | Number of rows to show |

**Examples:**
```bash
# Show 10 rows (default)
seeknal debug user_features

# Show 50 rows
seeknal debug user_features --limit 50
```

---

## seeknal audit

Run data quality audits on cached node outputs.

Executes all audit rules defined in node YAML configs against the last execution's cached outputs. No re-execution needed.

**Usage:**
```bash
seeknal audit [NODE] [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node` | TEXT | No | Specific node to audit (e.g., source.users) |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--target-path` | TEXT | "target" | Path to target directory |

**Examples:**
```bash
# Audit all nodes
seeknal audit

# Audit specific node
seeknal audit source.users

# Custom target path
seeknal audit --target-path /path/to/target
```

---

## seeknal clean

Clean old feature data based on TTL or date.

**Usage:**
```bash
seeknal clean FEATURE_GROUP [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `feature_group` | TEXT | Yes | Feature group name to clean |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--before`, `-b` | TEXT | None | Delete data before this date (YYYY-MM-DD) |
| `--ttl` | INTEGER | None | Delete data older than TTL days |
| `--dry-run` | FLAG | False | Show what would be deleted without deleting |

**Examples:**
```bash
# Clean data before specific date
seeknal clean user_features --before 2024-01-01

# Clean data older than 90 days
seeknal clean user_features --ttl 90

# Dry run to preview
seeknal clean user_features --before 2024-01-01 --dry-run
```

---

# Change Management

## seeknal diff

Show changes in pipeline files since last apply.

Like `git diff` for your Seeknal pipelines. Shows unified YAML diffs with semantic annotations (BREAKING/NON_BREAKING/METADATA).

**Usage:**
```bash
seeknal diff [NODE] [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node` | TEXT | No | Node to diff (e.g., `sources/orders`) |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--type`, `-t` | TEXT | None | Filter by node type (e.g., `transforms`, `sources`) |
| `--stat` | FLAG | False | Show summary statistics only |
| `--project-path`, `-p` | PATH | "." | Project directory |

**Examples:**
```bash
# Show all changes
seeknal diff

# Diff a specific node
seeknal diff sources/orders

# Filter by node type
seeknal diff --type transforms

# Show summary statistics only
seeknal diff --stat
```

**See Also:** [Change Categorization](../concepts/change-categorization.md)

---

# Development Workflow

## seeknal draft

Generate template from Jinja2 template.

Creates a draft file for a new node using Jinja2 templates. The draft file can be edited and then applied using `seeknal apply`.

Supports both YAML (`--default`) and Python (`--python`) output formats.

**Template discovery order:**
1. `project/seeknal/templates/*.j2` (project override)
2. Package templates (default)

**Usage:**
```bash
seeknal draft NODE_TYPE NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_type` | TEXT | Yes | Node type (source, transform, feature-group, model, aggregation, second-order-aggregation, rule, exposure) |
| `name` | TEXT | Yes | Node name |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--description`, `-d` | TEXT | None | Node description |
| `--force`, `-f` | FLAG | False | Overwrite existing draft file |
| `--python`, `-py` | FLAG | False | Generate Python file instead of YAML |
| `--deps` | TEXT | None | Comma-separated Python dependencies for PEP 723 header |

**Examples:**
```bash
# Create a YAML feature group draft
seeknal draft feature-group user_behavior

# Create a second-order aggregation draft
seeknal draft second-order-aggregation regional_totals

# Create a Python transform draft
seeknal draft transform clean_data --python

# Create Python source with dependencies
seeknal draft source raw_users --python --deps pandas,requests

# Create with description
seeknal draft source postgres_users --description "PostgreSQL users table"

# Overwrite existing draft
seeknal draft transform clean_data --force
```

---

## seeknal dry-run

Validate YAML/Python and preview execution.

Performs comprehensive validation:
1. YAML syntax validation with line numbers
2. Schema validation (required fields)
3. Dependency validation (refs to upstream nodes)
4. Preview execution with sample data

**Usage:**
```bash
seeknal dry-run FILE_PATH [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `file_path` | TEXT | Yes | Path to YAML or Python pipeline file |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--limit`, `-l` | INTEGER | 10 | Row limit for preview |
| `--timeout`, `-t` | INTEGER | 30 | Query timeout in seconds |
| `--schema-only`, `-s` | FLAG | False | Validate schema only, skip execution |

**Examples:**
```bash
# Validate and preview
seeknal dry-run draft_feature_group_user_behavior.yml

# Preview with 5 rows
seeknal dry-run draft_feature_group_user_behavior.yml --limit 5

# Schema validation only (no execution)
seeknal dry-run draft_source_postgres.yml --schema-only

# Longer timeout for slow queries
seeknal dry-run draft_transform.yml --timeout 60
```

---

## seeknal apply

Apply file to production.

- **YAML files:** Moves to `seeknal/<type>s/<name>.yml` and updates manifest
- **Python files:** Copies to `seeknal/pipelines/<name>.py` and validates

**Usage:**
```bash
seeknal apply FILE_PATH [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `file_path` | TEXT | Yes | Path to YAML or Python pipeline file |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--force`, `-f` | FLAG | False | Overwrite existing file without prompt |
| `--no-parse` | FLAG | False | Skip manifest regeneration |

**Examples:**
```bash
# Apply YAML draft
seeknal apply draft_feature_group_user_behavior.yml

# Apply Python file
seeknal apply seeknal/pipelines/enriched_sales.py

# Apply with overwrite
seeknal apply draft_transform.yml --force

# Apply without regenerating manifest
seeknal apply draft_source.yml --no-parse
```

---

# Semantic Layer

## seeknal query

Query metrics from the semantic layer.

Compiles metric definitions into SQL, executes against DuckDB, and returns formatted results.

**Usage:**
```bash
seeknal query [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--metrics` | TEXT | Required | Comma-separated metric names |
| `--dimensions` | TEXT | None | Comma-separated dimensions (e.g. region,ordered_at__month) |
| `--filter` | TEXT | None | SQL filter expression |
| `--order-by` | TEXT | None | Order by columns (prefix - for DESC) |
| `--limit` | INTEGER | 100 | Maximum rows to return |
| `--compile` | FLAG | False | Show generated SQL without executing |
| `--format` | TEXT | "table" | Output format: table, json, csv |
| `--project-path` | TEXT | "." | Path to project directory |

**Examples:**
```bash
# Query metrics
seeknal query --metrics total_revenue,order_count --dimensions region

# With filter
seeknal query --metrics total_revenue --dimensions region --filter "region='US'"

# Compile only (show SQL)
seeknal query --metrics total_revenue --compile

# JSON output
seeknal query --metrics total_revenue --format json
```

---

## seeknal deploy-metrics

Deploy semantic layer metrics as StarRocks materialized views.

Generates CREATE MATERIALIZED VIEW DDL for each metric definition and executes on the specified StarRocks connection.

**Usage:**
```bash
seeknal deploy-metrics [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--connection` | TEXT | Required | StarRocks connection name or URL |
| `--dimensions` | TEXT | None | Comma-separated dimensions to include in MVs |
| `--refresh-interval` | TEXT | "1 DAY" | MV refresh interval (e.g. '1 HOUR', '1 DAY') |
| `--drop-existing` | FLAG | False | Drop and recreate existing MVs |
| `--dry-run` | FLAG | False | Show DDL without executing |
| `--project-path` | TEXT | "." | Path to project directory |

**Examples:**
```bash
# Deploy metrics
seeknal deploy-metrics --connection starrocks://root@localhost:9030/analytics

# Dry run to see DDL
seeknal deploy-metrics --connection starrocks://root@localhost:9030/analytics --dry-run

# With custom refresh interval
seeknal deploy-metrics --connection starrocks://root@localhost:9030/analytics --refresh-interval "1 HOUR"

# Drop and recreate
seeknal deploy-metrics --connection starrocks://root@localhost:9030/analytics --drop-existing
```

---

## seeknal repl

Start interactive SQL REPL.

The SQL REPL allows you to explore data across multiple sources using DuckDB as a unified query engine. Connect to PostgreSQL, MySQL, SQLite databases, or query local parquet/csv files.

**Usage:**
```bash
seeknal repl [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--profile` | PATH | None | Path to profiles.yml for auto-registration |

**REPL Commands:**
- `.connect <source>` - Connect to saved source
- `.connect <url>` - Connect to database URL
- `.connect <path>` - Query local file (parquet/csv)
- `.tables` - List available tables
- `.quit` - Exit REPL

**Examples:**
```bash
# Start REPL
seeknal repl

# Inside REPL
seeknal> .connect mydb                   # (saved source)
seeknal> .connect postgres://user:pass@host/db
seeknal> .connect /path/to/data.parquet
seeknal> SELECT * FROM db0.users LIMIT 10
seeknal> .tables
seeknal> .quit
```

**Managing Sources:**
```bash
# Add source
seeknal source add mydb --url postgres://user:pass@localhost/mydb

# List sources
seeknal source list

# Remove source
seeknal source remove mydb
```

**See Also:** [Data Sources](#data-sources-repl)

---

# Version Management

## seeknal version list

List all versions of a feature group.

Displays version history with creation timestamps and feature counts, sorted by version number (most recent first). Use this command to review the evolution of a feature group over time.

**Usage:**
```bash
seeknal version list FEATURE_GROUP [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `feature_group` | TEXT | Yes | Name of the feature group to query versions for |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--format`, `-f` | [table\|json\|yaml] | "table" | Output format |
| `--limit`, `-l` | INTEGER | None | Maximum number of versions to display (most recent first) |

**Examples:**
```bash
# List all versions
seeknal version list user_features

# List last 5 versions
seeknal version list user_features --limit 5

# JSON output
seeknal version list user_features --format json
```

---

## seeknal version show

Show detailed information about a feature group version.

Displays comprehensive metadata for a specific version including:
- Version number and timestamps (created, updated)
- Feature count
- Complete Avro schema with field names and types

If no version is specified, shows the latest version.

**Usage:**
```bash
seeknal version show FEATURE_GROUP [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `feature_group` | TEXT | Yes | Name of the feature group to inspect |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--version`, `-v` | INTEGER | Latest | Specific version number to show |
| `--format`, `-f` | [table\|json\|yaml] | "table" | Output format |

**Examples:**
```bash
# Show latest version
seeknal version show user_features

# Show version 2
seeknal version show user_features --version 2

# JSON output
seeknal version show user_features --format json
```

---

## seeknal version diff

Compare two versions of a feature group to identify schema differences.

Analyzes the Avro schemas of both versions and displays:
- **Added features (+):** New fields in the target version
- **Removed features (-):** Fields removed from the base version
- **Modified features (~):** Fields with type changes

This is useful for understanding schema evolution and identifying potential breaking changes before rolling back or deploying a version.

**Usage:**
```bash
seeknal version diff FEATURE_GROUP [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `feature_group` | TEXT | Yes | Name of the feature group to compare versions for |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--from`, `-f` | INTEGER | Required | Base version number to compare from (older version) |
| `--to`, `-t` | INTEGER | Required | Target version number to compare to (newer version) |
| `--format` | [table\|json\|yaml] | "table" | Output format |

**Examples:**
```bash
# Compare versions 1 and 2
seeknal version diff user_features --from 1 --to 2

# Compare versions 1 and 3 as JSON
seeknal version diff user_features --from 1 --to 3 --format json
```

---

# Virtual Environments

## seeknal env plan

Preview changes in a virtual environment.

**Usage:**
```bash
seeknal env plan ENV_NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `env_name` | TEXT | Yes | Environment name (e.g., dev, staging) |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-path` | PATH | "." | Project directory |
| `--profile` | PATH | None | Path to profiles.yml (auto-discovered if omitted) |

**Examples:**
```bash
# Plan dev environment
seeknal env plan dev

# Plan staging environment
seeknal env plan staging
```

---

## seeknal env apply

Execute a plan in a virtual environment.

Validates the saved plan, then executes changed nodes using the environment-specific cache directory. Unchanged nodes reference production outputs (zero-copy).

**Usage:**
```bash
seeknal env apply ENV_NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `env_name` | TEXT | Yes | Environment name |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--force/--no-force` | FLAG | False | Apply even if plan is stale |
| `--parallel` | FLAG | False | Run nodes in parallel |
| `--max-workers` | INTEGER | 4 | Max parallel workers |
| `--continue-on-error` | FLAG | False | Continue past failures |
| `--dry-run` | FLAG | False | Show what would execute without running |
| `--profile` | PATH | None | Path to profiles.yml (overrides plan.json) |
| `--project-path` | PATH | "." | Project directory |

**Examples:**
```bash
# Apply dev environment plan
seeknal env apply dev

# Apply with parallel execution
seeknal env apply dev --parallel

# Force apply stale plan
seeknal env apply dev --force

# Dry run
seeknal env apply dev --dry-run
```

---

## seeknal env promote

Promote environment to production.

**Usage:**
```bash
seeknal env promote FROM_ENV [TO_ENV] [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `from_env` | TEXT | Yes | Source environment |
| `to_env` | TEXT | No | Target environment (default: "prod") |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-path` | PATH | "." | Project directory |
| `--profile` | PATH | None | Path to profiles YAML for target environment |
| `--rematerialize` | FLAG | False | Re-run materialization against target credentials |
| `--dry-run` | FLAG | False | Show what would be promoted without executing |

**Examples:**
```bash
# Promote dev to production
seeknal env promote dev

# Promote staging to prod
seeknal env promote staging prod
```

---

## seeknal env list

List all virtual environments.

**Usage:**
```bash
seeknal env list [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-path` | PATH | "." | Project directory |

**Examples:**
```bash
seeknal env list
```

---

## seeknal env delete

Delete a virtual environment.

**Usage:**
```bash
seeknal env delete ENV_NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `env_name` | TEXT | Yes | Environment to delete |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project-path` | PATH | "." | Project directory |

**Examples:**
```bash
seeknal env delete dev
```

---

# Data Sources (REPL)

## seeknal source add

Add a new data source with encrypted credentials.

**Usage:**
```bash
seeknal source add NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | TEXT | Yes | Unique name for this source |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--url`, `-u` | TEXT | Required | Connection URL |

**Examples:**
```bash
# Add PostgreSQL source
seeknal source add mydb --url postgres://user:pass@localhost/mydb

# Add MySQL source
seeknal source add mysqldb --url mysql://user:pass@localhost/mydb
```

---

## seeknal source list

List all saved data sources.

**Usage:**
```bash
seeknal source list
```

**Examples:**
```bash
seeknal source list
```

---

## seeknal source remove

Remove a saved data source.

**Usage:**
```bash
seeknal source remove NAME
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | TEXT | Yes | Name of source to remove |

**Examples:**
```bash
seeknal source remove mydb
```

---

# Interval Management

Manage execution intervals for incremental pipelines — list completed intervals, find gaps, execute backfills, and mark intervals for restatement.

## seeknal intervals list

List completed intervals for a node.

Shows all time intervals that have been successfully executed for the specified node.

**Usage:**
```bash
seeknal intervals list NODE_ID [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_id` | TEXT | Yes | Node identifier (e.g., `transform.clean_data`) |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--output`, `-o` | [table\|json] | "table" | Output format |

**Examples:**
```bash
# List completed intervals
seeknal intervals list transform.clean_data

# JSON output
seeknal intervals list feature_group.user_features --output json
```

---

## seeknal intervals pending

Show pending intervals for a node.

Calculates which intervals need to be executed based on the schedule and compares against completed intervals to find gaps.

**Usage:**
```bash
seeknal intervals pending NODE_ID [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_id` | TEXT | Yes | Node identifier |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--start`, `-s` | TEXT | Required | Start date (YYYY-MM-DD or ISO timestamp) |
| `--end`, `-e` | TEXT | None | End date (defaults to next scheduled run) |
| `--schedule` | TEXT | "daily" | Schedule preset (daily, hourly, weekly, monthly, yearly) |

**Examples:**
```bash
# Show pending daily intervals
seeknal intervals pending feature_group.user_features --start 2024-01-01

# Show pending intervals for a date range
seeknal intervals pending transform.clean_data --start 2024-01-01 --end 2024-01-31

# Hourly intervals
seeknal intervals pending feature_group.user_features --start 2024-01-01 --schedule hourly
```

---

## seeknal intervals backfill

Execute backfill for missing intervals.

Calculates pending intervals for the specified date range and executes the node for each missing interval.

**Usage:**
```bash
seeknal intervals backfill NODE_ID [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_id` | TEXT | Yes | Node identifier to backfill |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--start`, `-s` | TEXT | Required | Start date (YYYY-MM-DD) |
| `--end`, `-e` | TEXT | Required | End date (YYYY-MM-DD) |
| `--schedule` | TEXT | "daily" | Schedule preset |
| `--dry-run` | FLAG | False | Show what would be executed without running |

**Examples:**
```bash
# Backfill daily intervals
seeknal intervals backfill feature_group.user_features --start 2024-01-01 --end 2024-01-31

# Dry run to preview
seeknal intervals backfill transform.clean_data --start 2024-01-01 --end 2024-01-31 --dry-run

# Hourly backfill
seeknal intervals backfill feature_group.user_features --start 2024-01-01 --end 2024-01-31 --schedule hourly
```

---

## seeknal intervals restatement-add

Mark an interval for restatement. The next run will re-process data for this interval.

**Usage:**
```bash
seeknal intervals restatement-add NODE_ID [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_id` | TEXT | Yes | Node identifier |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--start`, `-s` | TEXT | Required | Start date (YYYY-MM-DD) |
| `--end`, `-e` | TEXT | Required | End date (YYYY-MM-DD) |

**Examples:**
```bash
seeknal intervals restatement-add feature_group.user_features --start 2024-01-01 --end 2024-01-31
```

---

## seeknal intervals restatement-list

List all intervals marked for restatement.

**Usage:**
```bash
seeknal intervals restatement-list NODE_ID
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_id` | TEXT | Yes | Node identifier |

**Examples:**
```bash
seeknal intervals restatement-list feature_group.user_features
```

---

## seeknal intervals restatement-clear

Clear all restatement intervals for a node.

**Usage:**
```bash
seeknal intervals restatement-clear NODE_ID
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_id` | TEXT | Yes | Node identifier |

**Examples:**
```bash
seeknal intervals restatement-clear feature_group.user_features
```

---

# Iceberg Materialization

## seeknal iceberg validate-materialization

Validate materialization configuration.

Checks that:
- Profile file exists and is valid YAML
- Catalog credentials are accessible
- Catalog connection succeeds
- Warehouse path is valid
- Schema evolution settings are valid

**Usage:**
```bash
seeknal iceberg validate-materialization [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--profile`, `-p` | TEXT | "~/.seeknal/profiles.yml" | Path to profiles.yml |

**Examples:**
```bash
# Validate default profile
seeknal iceberg validate-materialization

# Validate custom profile (recommended for local projects)
seeknal iceberg validate-materialization --profile profiles.yml
```

> **Known Issue:** This command currently crashes with a `RequestException` error due to a missing import in `materialization_cli.py`. This will be fixed in a future release.

**See Also:** [Iceberg Materialization](../iceberg-materialization.md)

---

## seeknal iceberg snapshot-list

List snapshots for an Iceberg table.

Shows the most recent snapshots for the specified table, including snapshot ID, timestamp, and schema version.

**Usage:**
```bash
seeknal iceberg snapshot-list TABLE_NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_name` | TEXT | Yes | Fully qualified table name (e.g., `atlas.production.orders`) |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--limit`, `-l` | INTEGER | 10 | Maximum number of snapshots to list |

**Examples:**
```bash
# List last 10 snapshots
seeknal iceberg snapshot-list atlas.production.orders

# List last 20 snapshots
seeknal iceberg snapshot-list atlas.production.orders --limit 20
```

> **Known Issue:** This command currently fails with "NameListToString NOT IMPLEMENTED" when using 3-part table names (`catalog.namespace.table`). This is a DuckDB/Iceberg extension compatibility issue.

---

## seeknal iceberg snapshot-show

Show details of a specific snapshot.

Displays detailed information about a snapshot including creation timestamp, schema version, row count, and expiration.

**Usage:**
```bash
seeknal iceberg snapshot-show TABLE_NAME SNAPSHOT_ID
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_name` | TEXT | Yes | Fully qualified table name (e.g., `atlas.production.orders`) |
| `snapshot_id` | TEXT | Yes | Snapshot ID (or first 8 characters) |

**Examples:**
```bash
# Show snapshot details
seeknal iceberg snapshot-show atlas.production.orders 12345678
```

---

## seeknal iceberg setup

Interactive setup for materialization credentials.

Prompts for Lakekeeper catalog credentials and stores them either in environment variables (displayed as export commands) or in the system keyring.

**Usage:**
```bash
seeknal iceberg setup [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--keyring`, `-k` | FLAG | False | Use system keyring for credentials (more secure) |

**Examples:**
```bash
# Environment variables (default)
seeknal iceberg setup

# System keyring (more secure)
seeknal iceberg setup --keyring
```

**See Also:** [Iceberg Materialization](../iceberg-materialization.md)

---

## seeknal iceberg profile-show

Show current materialization profile configuration.

Displays the active materialization configuration from profiles.yml, including catalog settings, schema evolution options, and defaults.

**Usage:**
```bash
seeknal iceberg profile-show [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--profile`, `-p` | TEXT | "~/.seeknal/profiles.yml" | Path to profiles.yml |

**Examples:**
```bash
# Show default profile (reads from ~/.seeknal/profiles.yml)
seeknal iceberg profile-show

# Show local project profile (recommended)
seeknal iceberg profile-show --profile profiles.yml
```

> **Known Issue:** The default path is `~/.seeknal/profiles.yml`, which may not be where your project's `profiles.yml` is located. For local projects, always pass `--profile profiles.yml` explicitly.

---

# StarRocks Integration

## seeknal starrocks-setup-catalog

Generate StarRocks Iceberg catalog setup SQL.

Outputs CREATE EXTERNAL CATALOG DDL for connecting StarRocks to an Iceberg catalog.

**Usage:**
```bash
seeknal starrocks-setup-catalog [OPTIONS]
```

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--catalog-name` | TEXT | "iceberg_catalog" | Catalog name |
| `--catalog-type` | TEXT | "iceberg" | Catalog type |
| `--warehouse` | TEXT | "s3://warehouse/" | Warehouse path |
| `--uri` | TEXT | None | Catalog URI (e.g., thrift://host:9083) |

**Examples:**
```bash
# Generate catalog setup
seeknal starrocks-setup-catalog --catalog-name my_catalog --uri thrift://hive:9083

# With S3 warehouse
seeknal starrocks-setup-catalog --warehouse s3://my-bucket/warehouse
```

---

## seeknal connection-test

Test connectivity to a StarRocks database.

Tests connection using profiles.yml config or a direct URL.

**Usage:**
```bash
seeknal connection-test NAME [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | TEXT | Yes | Connection profile name or starrocks:// URL |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--profile`, `-p` | TEXT | "default" | Profile name in profiles.yml |

**Examples:**
```bash
# Test using profile
seeknal connection-test default

# Test with direct URL
seeknal connection-test starrocks://user:pass@host:9030/db
```

---

# Atlas Integration

## seeknal atlas

Atlas Data Platform integration commands.

Provides access to Atlas governance, lineage, and API capabilities from the Seeknal CLI.

**Installation:**
```bash
pip install seeknal[atlas]
```

**Usage:**
```bash
seeknal atlas [OPTIONS] COMMAND [ARGS]...
```

**Subcommands:**
- `info` - Show Atlas integration information
- `api` - Manage the Atlas Seeknal API server
- `governance` - Data governance operations
- `lineage` - Lineage tracking and publishing

**Examples:**
```bash
# Show Atlas info
seeknal atlas info

# Start API server
seeknal atlas api start --port 8000

# Check API status
seeknal atlas api status

# Get governance statistics
seeknal atlas governance stats

# List governance policies
seeknal atlas governance policies

# Show lineage
seeknal atlas lineage show user_features

# Publish lineage
seeknal atlas lineage publish my_pipeline
```

---

## seeknal lineage

Generate interactive lineage visualization for your project's DAG.

**Usage:**
```bash
seeknal lineage [NODE_ID] [OPTIONS]
```

**Arguments:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `node_id` | TEXT | No | Node to focus on (e.g., `transform.clean_orders`) |

**Options:**
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--column`, `-c` | TEXT | None | Column to trace lineage for (requires node argument) |
| `--project`, `-p` | TEXT | None | Project name |
| `--path` | PATH | "." | Project path |
| `--output`, `-o` | PATH | `target/lineage.html` | Output HTML file path |
| `--no-open` | FLAG | False | Don't auto-open browser |

**Examples:**
```bash
# Full DAG visualization
seeknal lineage

# Focus on a specific node
seeknal lineage transform.clean_orders

# Trace column-level lineage
seeknal lineage transform.clean_orders --column total_amount

# Custom output path
seeknal lineage --output dag.html

# Generate without opening browser
seeknal lineage --no-open
```

---

# Entity Consolidation

## seeknal entity list

List all consolidated entities in the feature store.

```bash
seeknal entity list [--project PROJECT] [--path PATH]
```

| Option | Description | Default |
|--------|-------------|---------|
| `--project, -p` | Project name | Current directory name |
| `--path` | Project path | `.` |

**Output:**

Shows entity name, number of feature groups, total features, status (ready/stale), and consolidation timestamp.

```bash
# List all entities
seeknal entity list

# Output:
#   customer              2 FGs  15 features  [ready]  consolidated: 2026-02-26T10:30:00
#   product               3 FGs   8 features  [ready]  consolidated: 2026-02-26T10:30:00
```

---

## seeknal entity show

Display detailed catalog for a consolidated entity.

```bash
seeknal entity show <name> [--project PROJECT] [--path PATH]
```

| Argument/Option | Description | Default |
|-----------------|-------------|---------|
| `name` | Entity name to inspect | *(required)* |
| `--project, -p` | Project name | Current directory name |
| `--path` | Project path | `.` |

**Output:**

Shows entity metadata, join keys, and per-feature-group details (features, row counts, schema).

```bash
# Show entity details
seeknal entity show customer

# Output:
# Entity: customer
# Join keys: customer_id
# Consolidated at: 2026-02-26T10:30:00
# Schema version: 1
# Feature groups: 2
#
#   customer_features:
#     Features: revenue, orders, avg_spend
#     Rows: 50000
#     Event time col: event_time
#     Last updated: 2026-02-26T10:30:00
#
#   product_features:
#     Features: price, category
#     Rows: 45000
#     Event time col: event_time
#     Last updated: 2026-02-26T10:30:00
```

---

## seeknal consolidate

Manually trigger entity consolidation. Useful after running individual nodes with `seeknal run --nodes`.

```bash
seeknal consolidate [--project PROJECT] [--path PATH] [--prune]
```

| Option | Description | Default |
|--------|-------------|---------|
| `--project, -p` | Project name | Current directory name |
| `--path` | Project path | `.` |
| `--prune` | Remove stale FG columns not in current manifest | `false` |

**Examples:**

```bash
# Consolidate all entities
seeknal consolidate

# Re-consolidate and remove stale FG columns
seeknal consolidate --prune

# Consolidate from specific project path
seeknal consolidate --path /my/project
```

> **Note:** Consolidation runs automatically after `seeknal run`. Use this command when you need to manually trigger consolidation, for example after running individual nodes with `seeknal run --nodes feature_group.customer_features`.

---

## Additional Resources

- [Getting Started Guide](../getting-started-comprehensive.md)
- [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md)
- [Configuration Reference](configuration.md)
- [YAML Schema Reference](yaml-schema.md)
- [Migration Guides](migration.md) - Migrate from Spark, dbt, Feast, or Featuretools
- [Troubleshooting Guide](troubleshooting.md) - Debug common issues
- [API Reference](../api/index.md)
- [GitHub Repository](https://github.com/mta-tech/seeknal)

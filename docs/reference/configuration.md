# Configuration Reference

This reference documents all configuration options for Seeknal projects. Configuration is managed through YAML files, TOML application settings, and environment variables.

## Overview

Seeknal uses a multi-layered configuration system:

1. **Project configuration** (`seeknal_project.yml`) - Project metadata and settings
2. **Profiles** (`profiles.yml`) - Database connections and engine configuration
3. **Application config** (`config.toml`) - TOML-based runtime settings
4. **Environment variables** - Sensitive credentials and deployment overrides

## Project Configuration (`seeknal_project.yml`)

Created by `seeknal init`. Controls project-level settings and metadata.

### Location

```
<project_root>/seeknal_project.yml
```

### Schema

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Project name (converted to snake_case) |
| `version` | string | No | "1.0.0" | Project version (semantic versioning) |
| `profile` | string | No | "default" | Profile name to use from profiles.yml |
| `config-version` | integer | No | 1 | Configuration schema version |
| `description` | string | No | — | Project description |

### Example

```yaml
name: my_analytics_project
version: 1.0.0
profile: default
config-version: 1
description: Customer analytics and feature engineering
```

### Usage

The project file is automatically loaded when running CLI commands from the project directory. It determines which profile to use and provides project metadata for the feature store.

## Profiles (`profiles.yml`)

Connection settings for databases, data engines, and materialization targets.

### Location

```
<project_root>/profiles.yml
```

**Security Note**: This file is automatically added to `.gitignore` to prevent credential leakage. Never commit it to version control.

### Schema

#### Profile Structure

```yaml
<profile_name>:
  target: <output_name>
  outputs:
    <output_name>:
      type: <engine_type>
      # Engine-specific configuration
```

#### DuckDB Profile

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | Yes | — | Must be "duckdb" |
| `path` | string | No | "target/dev.duckdb" | Path to DuckDB database file |
| `threads` | integer | No | 4 | Number of threads for parallel execution |
| `memory_limit` | string | No | "1GB" | Memory limit (e.g., "1GB", "512MB") |

#### Spark Profile

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | Yes | — | Must be "spark" |
| `master` | string | No | "local[*]" | Spark master URL |
| `app_name` | string | No | "seeknal" | Spark application name |
| `config` | object | No | {} | Spark configuration properties |

#### PostgreSQL Profile

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | Yes | — | Must be "postgresql" |
| `host` | string | Yes | — | Database host |
| `port` | integer | No | 5432 | Database port |
| `database` | string | Yes | — | Database name |
| `user` | string | Yes | — | Database user |
| `password` | string | Yes | — | Database password (use env var) |
| `schema` | string | No | "public" | Default schema |

#### StarRocks Profile

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | Yes | — | Must be "starrocks" |
| `host` | string | Yes | — | StarRocks FE host |
| `port` | integer | No | 9030 | Query port |
| `http_port` | integer | No | 8030 | HTTP port |
| `user` | string | Yes | — | Database user |
| `password` | string | Yes | — | Database password (use env var) |
| `database` | string | Yes | — | Database name |

### Example

```yaml
# Development profile
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: target/dev.duckdb
      threads: 4
      memory_limit: 1GB

# Production profile with PostgreSQL
production:
  target: prod
  outputs:
    prod:
      type: postgresql
      host: ${POSTGRES_HOST}
      port: 5432
      database: seeknal_prod
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}
      schema: public

# Spark profile
spark_prod:
  target: prod
  outputs:
    prod:
      type: spark
      master: spark://spark-master:7077
      app_name: seeknal-prod
      config:
        spark.executor.memory: 4g
        spark.driver.memory: 2g
        spark.sql.shuffle.partitions: 200

# StarRocks profile
starrocks:
  default:
    type: starrocks
    host: ${STARROCKS_HOST}
    port: 9030
    user: ${STARROCKS_USER}
    password: ${STARROCKS_PASSWORD}
    database: analytics
```

## Materialization Configuration

Iceberg materialization settings for persisting pipeline outputs.

### Profile-Level Configuration

Add to `profiles.yml` for global defaults:

```yaml
materialization:
  enabled: true
  catalog:
    type: rest
    uri: ${LAKEKEEPER_URI}
    warehouse: s3://my-bucket/warehouse
    bearer_token: ${LAKEKEEPER_TOKEN}
    verify_tls: true
    connection_timeout: 10
    request_timeout: 30
  default_mode: append
  schema_evolution:
    mode: safe
    allow_column_add: false
    allow_column_type_change: false
  duckdb:
    iceberg_extension: true
    threads: 4
    memory_limit: 1GB
  partition_by: []
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | false | Enable Iceberg materialization globally |
| `catalog.type` | string | "rest" | Catalog type (currently only "rest" supported) |
| `catalog.uri` | string | — | REST catalog endpoint URL |
| `catalog.warehouse` | string | — | Warehouse path (S3/GCS/Azure/local) |
| `catalog.bearer_token` | string | — | Optional bearer token for authentication |
| `catalog.verify_tls` | boolean | true | Verify TLS certificates |
| `catalog.connection_timeout` | integer | 10 | Connection timeout in seconds |
| `catalog.request_timeout` | integer | 30 | Request timeout in seconds |
| `default_mode` | string | "append" | Default write mode ("append" or "overwrite") |
| `schema_evolution.mode` | string | "safe" | Schema evolution mode ("safe", "auto", "strict") |
| `schema_evolution.allow_column_add` | boolean | false | Allow adding new columns |
| `schema_evolution.allow_column_type_change` | boolean | false | Allow changing column types |
| `duckdb.iceberg_extension` | boolean | true | Load DuckDB Iceberg extension |
| `duckdb.threads` | integer | 4 | DuckDB thread count |
| `duckdb.memory_limit` | string | "1GB" | DuckDB memory limit |
| `partition_by` | list | [] | Default partition columns |

### Node-Level Configuration

Override materialization settings per node in YAML:

```yaml
kind: transform
name: clean_orders
materialization:
  enabled: true
  mode: append
  table: warehouse.prod.orders
  partition_by:
    - order_date
```

## Environment Variables

Environment variables provide flexible configuration, especially for sensitive values and deployment-specific settings.

### Project and Configuration Paths

| Variable | Description | Default |
|----------|-------------|---------|
| `SEEKNAL_BASE_CONFIG_PATH` | Base directory for Seeknal files | `~/.seeknal/` |
| `SEEKNAL_USER_CONFIG_PATH` | User config file path | `~/.seeknal/config.toml` |
| `SEEKNAL_BACKEND_CONFIG_PATH` | Backend config file path | `~/.seeknal/backend.toml` |
| `DEFAULT_SEEKNAL_DB_PATH` | SQLite database location | `~/.seeknal/seeknal.db` |

### Database Credentials

| Variable | Description | Usage |
|----------|-------------|-------|
| `TURSO_DATABASE_URL` | Turso database URL (production) | Cloud-hosted SQLite |
| `TURSO_AUTH_TOKEN` | Turso authentication token | Cloud-hosted SQLite |
| `POSTGRES_HOST` | PostgreSQL host | Reference in profiles.yml with `${POSTGRES_HOST}` |
| `POSTGRES_USER` | PostgreSQL user | Reference in profiles.yml with `${POSTGRES_USER}` |
| `POSTGRES_PASSWORD` | PostgreSQL password | Reference in profiles.yml with `${POSTGRES_PASSWORD}` |
| `STARROCKS_HOST` | StarRocks FE host | Reference in profiles.yml with `${STARROCKS_HOST}` |
| `STARROCKS_USER` | StarRocks user | Reference in profiles.yml with `${STARROCKS_USER}` |
| `STARROCKS_PASSWORD` | StarRocks password | Reference in profiles.yml with `${STARROCKS_PASSWORD}` |

### Iceberg Materialization

| Variable | Description | Usage |
|----------|-------------|-------|
| `LAKEKEEPER_URI` | Lakekeeper REST catalog endpoint | Reference in profiles.yml with `${LAKEKEEPER_URI}` |
| `LAKEKEEPER_TOKEN` | Lakekeeper bearer token (optional) | Reference in profiles.yml with `${LAKEKEEPER_TOKEN}` |
| `LAKEKEEPER_WAREHOUSE` | Warehouse path (alternative to profiles.yml) | Direct environment variable access |

### Application Configuration Overrides

Use the pattern `SEEKNAL__SECTION__KEY` to override TOML configuration:

```bash
# Override logging level
export SEEKNAL__LOGGING__LEVEL="DEBUG"

# Override database settings
export SEEKNAL__DATABASE__HOST="production-db.example.com"
export SEEKNAL__DATABASE__PORT="5432"

# Override storage paths
export SEEKNAL__STORAGE__BASE_PATH="/mnt/data/seeknal"
```

Type conversion is automatic:
- `"true"` / `"false"` → boolean
- `"42"` → integer
- `"3.14"` → float
- `'["a", "b"]'` → list

## Application Config (`config.toml`)

TOML-based runtime configuration for logging, tasks, and application behavior.

### Location

```
~/.seeknal/config.toml
```

Or custom path via `SEEKNAL_USER_CONFIG_PATH` environment variable.

### Schema

#### Logging Configuration

```toml
[logging]
level = "INFO"
format = "%(asctime)s - %(levelname)s - %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"
log_attributes = "[]"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | string | "INFO" | Log level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL") |
| `format` | string | — | Python logging format string |
| `datefmt` | string | "%Y-%m-%d %H:%M:%S" | Date format for log timestamps |
| `log_attributes` | string | "[]" | Additional log attributes (JSON array as string) |

#### Task Defaults

```toml
[tasks]
[tasks.defaults]
max_retries = 3
retry_delay = 60
timeout = 3600
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | integer | 0 | Maximum retry attempts for failed tasks |
| `retry_delay` | integer | — | Delay between retries in seconds (or `false` for none) |
| `timeout` | integer | — | Task timeout in seconds (or `false` for none) |

#### Storage Configuration

```toml
[storage]
base_path = "/data/seeknal"
offline_store = "${storage.base_path}/offline"
online_store = "${storage.base_path}/online"
```

Supports variable interpolation with `${section.key}` syntax and environment variables.

### Complete Example

```toml
# Seeknal Application Configuration

[logging]
level = "INFO"
format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"
log_attributes = "[]"

[tasks]
[tasks.defaults]
max_retries = 3
retry_delay = 60
timeout = 7200

[storage]
base_path = "$SEEKNAL_DATA_PATH"
offline_store = "${storage.base_path}/offline"
online_store = "${storage.base_path}/online"
archive_store = "${storage.base_path}/archive"

[database]
host = "$SEEKNAL_DB_HOST"
port = "$SEEKNAL_DB_PORT"
name = "$SEEKNAL_DB_NAME"
username = "$SEEKNAL_DB_USER"
password = "$SEEKNAL_DB_PASSWORD"
```

## Directory Structure

After running `seeknal init`, your project has this structure:

```
my-project/
├── seeknal_project.yml        # Project configuration
├── profiles.yml                # Credentials and connections (gitignored)
├── .gitignore                  # Auto-generated
├── seeknal/                    # YAML and Python pipelines
│   ├── sources/               # Source definitions (*.yml)
│   ├── transforms/            # Transform definitions (*.yml)
│   ├── feature_groups/        # Feature group definitions (*.yml)
│   ├── models/                # Model definitions (*.yml)
│   ├── pipelines/             # Python pipeline scripts (*.py)
│   └── templates/             # Custom Jinja templates (optional)
└── target/                     # Output directory (gitignored)
    ├── intermediate/          # Node output storage for cross-references
    ├── cache/                 # Cached node outputs
    │   ├── source/           # Source outputs
    │   ├── transform/        # Transform outputs
    │   └── ...
    ├── manifest.json          # Production manifest
    ├── run_state.json         # Production run state
    └── environments/          # Virtual environments
        └── <env_name>/       # Environment-specific outputs
```

### Directory Purposes

| Directory | Purpose | Gitignored |
|-----------|---------|-----------|
| `seeknal/` | YAML and Python pipeline definitions | No |
| `seeknal/sources/` | Source node definitions | No |
| `seeknal/transforms/` | Transform node definitions | No |
| `seeknal/feature_groups/` | Feature group definitions | No |
| `seeknal/models/` | Model definitions | No |
| `seeknal/pipelines/` | Python pipeline scripts | No |
| `seeknal/templates/` | Custom Jinja templates | No |
| `target/` | All build outputs | Yes |
| `target/intermediate/` | Cross-node references | Yes |
| `target/cache/` | Incremental execution cache | Yes |
| `target/environments/` | Virtual environment outputs | Yes |

## Variable Interpolation

Seeknal supports variable interpolation in TOML and YAML files.

### Environment Variable Syntax

Use standard shell syntax `$VAR_NAME` or `${VAR_NAME}`:

```yaml
database:
  host: $DB_HOST
  port: ${DB_PORT}
  username: $DB_USER
```

### Config Reference Syntax

Use `${section.key}` to reference other config values:

```toml
[storage]
base_path = "/data/seeknal"
offline_store = "${storage.base_path}/offline"
online_store = "${storage.base_path}/online"
```

### Nested Interpolation

Environment variables can reference other environment variables:

```bash
export BASE="/data"
export PROJECT="seeknal"
export SEEKNAL_PATH="$BASE/$PROJECT"  # Resolves to /data/seeknal
```

## Configuration Loading Order

Configuration is loaded and merged in this order (later overrides earlier):

1. **Default configuration** - Built-in defaults from `configuration.py`
2. **User config file** - `~/.seeknal/config.toml` or `SEEKNAL_USER_CONFIG_PATH`
3. **Backend config file** - `~/.seeknal/backend.toml` or `SEEKNAL_BACKEND_CONFIG_PATH`
4. **Environment variables** - `SEEKNAL__SECTION__KEY` pattern
5. **Profile configuration** - From `profiles.yml` based on active profile
6. **Node-level overrides** - Materialization settings in individual YAML files

## Best Practices

### Security

- **Never commit credentials** - Use environment variables and add `profiles.yml` to `.gitignore`
- **Use system keyring** - For local development, consider system keyring integration
- **Rotate credentials** - Regularly update passwords and tokens
- **Principle of least privilege** - Grant only necessary permissions to database users

### Organization

- **Environment-specific profiles** - Maintain separate profiles for dev/staging/prod
- **Variable interpolation** - Use `${var}` syntax to avoid duplication
- **Profile inheritance** - Create base profiles and extend them for environments
- **Document custom settings** - Comment your configuration files

### Development Workflow

```yaml
# profiles.yml - Development
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: target/dev.duckdb

# profiles.yml - Production (separate file or profile)
production:
  target: prod
  outputs:
    prod:
      type: postgresql
      host: ${POSTGRES_HOST}
      database: seeknal_prod
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}
```

### Validation

Use these commands to validate your configuration:

```bash
# Check project configuration
seeknal init --name test --path /tmp/test
cat /tmp/test/seeknal_project.yml

# Validate Iceberg materialization
seeknal iceberg validate-materialization

# Show active profile
seeknal iceberg profile-show

# Test database connection
seeknal run --dry-run
```

## CLI Configuration Commands

```bash
# Initialize project with configuration
seeknal init --name my_project --description "My project"

# Show version and configuration info
seeknal info

# Interactive Iceberg setup
seeknal iceberg setup

# Validate materialization config
seeknal iceberg validate-materialization

# Show active profile settings
seeknal iceberg profile-show
```

## Related Documentation

- [Initialization Guide](../examples/initialization.md) - Project setup
- [Iceberg Materialization](../iceberg-materialization.md) - Detailed Iceberg configuration
- [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md) - Using configuration in pipelines
- [Python Pipelines Tutorial](../tutorials/python-pipelines-tutorial.md) - Programmatic configuration

## Troubleshooting

### Configuration Not Found

```
Error: Profile file not found: ~/.seeknal/profiles.yml
```

**Solution**: Run `seeknal init` in your project directory to create default configuration files.

### Environment Variable Not Resolved

```
Error: Required environment variable 'POSTGRES_HOST' is not set
```

**Solution**: Set the environment variable before running Seeknal:

```bash
export POSTGRES_HOST=localhost
# Or add to ~/.bashrc, ~/.zshrc, or .env file
```

### Invalid Profile

```
Error: Profile 'production' not found. Available profiles: default
```

**Solution**: Check `profiles.yml` and ensure the profile exists:

```yaml
production:  # This must match the profile name in seeknal_project.yml
  target: prod
  outputs:
    prod:
      type: postgresql
      # ...
```

### Permission Denied

```
Error: Permission denied: /mnt/data/seeknal
```

**Solution**: Ensure the application has write permissions to the configured directories, or update the path in your configuration.


---

## Additional Resources

- [CLI Reference](cli.md) - Complete command documentation
- [YAML Schema Reference](yaml-schema.md) - Pipeline node definitions
- [Migration Guides](migration.md) - Migrate from other platforms
- [Troubleshooting Guide](troubleshooting.md) - Debug common issues
- [Getting Started Guide](../getting-started-comprehensive.md) - Tutorial


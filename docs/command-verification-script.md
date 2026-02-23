# Command Verification Script

This script documents the commands that need to be tested against the actual Seeknal software implementation.

## Quick Start Commands

### Installation Verification

```bash
# After installation from GitHub Releases wheel file
seeknal --version
# Expected: seeknal x.x.x

# Test Python import
python -c "from seeknal.tasks.duckdb import DuckDBTask; print('✓ Seeknal imported')"
# Expected: ✓ Seeknal imported
```

### Pipeline Builder Workflow

```bash
# 1. Initialize project
seeknal init test-project
# Expected: Creates project structure

# 2. Create source
seeknal draft source --name test_data --path data/test.csv
# Expected: Creates draft file

# 3. Apply source
seeknal apply pipelines/sources/test_data.yaml
# Expected: Source applied successfully

# 4. Run pipeline
seeknal run
# Expected: Pipeline executes
```

## Data Engineer Path Commands

### Chapter 1: ELT Pipeline

```bash
# HTTP source creation
seeknal draft source --name orders_api --type http
# Expected: Creates HTTP source template

# Apply HTTP source
seeknal apply pipelines/sources/orders_api.yaml
# Expected: HTTP source applied

# DuckDB transform
seeknal draft transform --name orders_cleaned --input orders_api
# Expected: Creates transform template

# Warehouse output
seeknal draft output --name warehouse_orders --input orders_cleaned --target warehouse
# Expected: Creates output template
```

### Chapter 2: Incremental Models

```bash
# Incremental source
seeknal apply pipelines/sources/orders_incremental.yaml
# Expected: Incremental source configured

# CDC transform
seeknal apply pipelines/transforms/orders_cdc.yaml
# Expected: CDC transform applied

# Scheduled pipeline
seeknal apply pipelines/scheduled_orders.yaml
# Expected: Scheduled pipeline created
```

### Chapter 3: Production Environments

```bash
# Environment planning
seeknal plan dev
# Expected: Shows changes for dev environment

# Environment application
seeknal env apply dev
# Expected: Executes in dev environment

# Environment promotion
seeknal env promote dev prod
# Expected: Promotes changes to production
```

## ML Engineer Path Commands

### Chapter 1: Feature Store

```bash
# Feature group versioning
seeknal version list <feature_group>
# Expected: Lists all versions

seeknal version show <feature_group> --version 1
# Expected: Shows version details

seeknal version diff <feature_group> --from 1 --to 2
# Expected: Shows schema differences
```

### Chapter 2: Second-Order Aggregations

```bash
# Feature validation
seeknal validate-features <feature_group> --mode fail
# Expected: Validates feature data quality
```

### Chapter 3: Training-to-Serving Parity

```bash
# Online serving
seeknal materialize <feature_group> --start-date 2024-01-01 --online-only
# Expected: Materializes to online store
```

## Analytics Engineer Path Commands

### StarRocks Integration

```bash
# StarRocks catalog setup
seeknal starrocks-setup-catalog --catalog-name iceberg_catalog --uri thrift://hive:9083
# Expected: Generates catalog setup SQL

# Connection testing
seeknal connection-test starrocks://user:pass@host:9030/analytics
# Expected: Tests connectivity
```

## Implementation Status Notes

### Commands Verified in Codebase

Based on code inspection, the following commands are implemented:

| Command | Status | Location |
|---------|--------|----------|
| `seeknal init` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal draft` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal apply` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal run` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal plan` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal parse` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal env plan` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal env apply` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal env promote` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal env list` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal version list` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal version show` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal version diff` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal materialize` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal validate-features` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal delete` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal audit` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal starrocks-setup-catalog` | ✅ Implemented | `src/seeknal/cli/main.py` |
| `seeknal connection-test` | ✅ Implemented | `src/seeknal/cli/main.py` |

### Commands Requiring Verification

The following commands are documented but need runtime verification:

| Command Category | Needs Verification |
|-----------------|-------------------|
| HTTP source polling | Actual API connectivity |
| DuckDB transformations | SQL execution on real data |
| Warehouse outputs | Database connection and write |
| Virtual environments | Environment isolation behavior |
| Materialization | Offline/online store write operations |

## Testing Recommendations

### Unit Testing

```python
# Test command availability
import subprocess
import sys

def test_command_exists(command):
    result = subprocess.run(['seeknal', command, '--help'],
                          capture_output=True)
    return result.returncode == 0

# Test core commands
commands = ['init', 'draft', 'apply', 'run', 'plan', 'parse']
for cmd in commands:
    assert test_command_exists(cmd), f"Command {cmd} not found"
```

### Integration Testing

```bash
# Create test project
seeknal init test-cli-verification

# Test basic workflow
seeknal draft source --name test --path test.csv
echo "id,value\n1,100" > test.csv
seeknal apply pipelines/sources/test.yaml
seeknal run

# Verify output
[ -f target/intermediate/test.parquet ] && echo "✓ Output created"
```

## Next Steps

1. **Set up test environment** with Seeknal installed from GitHub Releases
2. **Run verification script** against actual Seeknal installation
3. **Update documentation** with any discovered discrepancies
4. **Add known issues** section for commands that don't work as documented
5. **Create example datasets** for comprehensive testing

---

*Generated: 2026-02-10*
*Purpose: Document commands requiring runtime verification*
*Status: Pending execution*

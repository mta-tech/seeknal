# Troubleshooting Guide

Comprehensive troubleshooting for common Seeknal issues, errors, and problems.

> **Navigation:**
> - [Installation Issues](#installation-issues)
> - [Configuration Issues](#configuration-issues)
> - [CLI Issues](#cli-issues)
> - [Feature Store Issues](#feature-store-issues)
> - [Performance Issues](#performance-issues)
> - [DuckDB-Specific Issues](#duckdb-specific-issues)
> - [Spark-Specific Issues](#spark-specific-issues)
> - [Virtual Environment Issues](#virtual-environment-issues)
> - [Getting Help](#getting-help)

---

## Quick Diagnosis

Before diving into specific issues, try these quick diagnostic steps:

```bash
# 1. Check Seeknal version
seeknal info

# 2. Validate configuration
seeknal validate

# 3. Test database connection
python -c "from seeknal.request import get_db_session; print('DB OK')"

# 4. Check Python version (must be 3.11+)
python --version
```

---

## Installation Issues

### "ModuleNotFoundError: No module named 'seeknal'"

**Symptoms:**
```bash
$ python -c "import seeknal"
ModuleNotFoundError: No module named 'seeknal'
```

**Causes:**
1. Seeknal not installed in active Python environment
2. Virtual environment not activated
3. Wrong Python interpreter

**Solutions:**

```bash
# Check which Python you're using
which python
python --version

# Ensure virtual environment is activated
source .venv/bin/activate  # Linux/macOS
.\.venv\Scripts\activate   # Windows

# Verify Seeknal is installed
pip show seeknal

# If not installed, install it
pip install seeknal
```

### "ImportError: cannot import name 'DuckDBTask'"

**Symptoms:**
```python
from seeknal.tasks.duckdb import DuckDBTask
# ImportError: cannot import name 'DuckDBTask'
```

**Causes:**
1. Outdated Seeknal version
2. Corrupted installation

**Solutions:**

```bash
# Check Seeknal version
pip show seeknal

# Reinstall
pip uninstall seeknal
pip install seeknal

# Verify installation
python -c "from seeknal.tasks.duckdb import DuckDBTask; print('OK')"
```

### "SSLError: SSL: CERTIFICATE_VERIFY_FAILED"

**Symptoms:**
```
SSLError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed
```

**Causes:**
1. SSL certificate issues on macOS
2. Corporate proxy blocking downloads

**Solutions:**

```bash
# macOS: Install certificates
/Applications/Python\ 3.11/Install\ Certificates.command

# Or use pip with trusted host
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org seeknal

# Behind proxy: Set proxy variables
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080
```

---

## Configuration Issues

### "Config file not found"

**Symptoms:**
```bash
$ seeknal validate
Error: Config file not found at ~/.seeknal/config.toml
```

**Causes:**
1. Configuration directory doesn't exist
2. Config file missing

**Solutions:**

```bash
# Create config directory
mkdir -p ~/.seeknal

# Create minimal config file
cat > ~/.seeknal/config.toml << EOF
[default]
profile = "default"
EOF

# Verify config
seeknal validate
```

### "profiles.yml not found"

**Symptoms:**
```
Error: profiles.yml not found in project directory
```

**Causes:**
1. Project not initialized
2. profiles.yml deleted

**Solutions:**

```bash
# Re-initialize project
seeknal init --name my_project

# Or create profiles.yml manually
cat > profiles.yml << EOF
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: target/dev.duckdb
EOF
```

### "Database connection failed"

**Symptoms:**
```
Error: Database connection failed: disk I/O error
```

**Causes:**
1. Database file corrupted
2. Insufficient permissions
3. Disk full

**Solutions:**

```bash
# Check database file exists and is readable
ls -la ~/.seeknal/metadata.db

# Check disk space
df -h

# Recreate database if corrupted
rm ~/.seeknal/metadata.db
seeknal init --name my_project
```

---

## CLI Issues

### "No project found"

**Symptoms:**
```bash
$ seeknal run
Error: No project found. Initialize with: seeknal init
```

**Causes:**
1. Not in a project directory
2. seeknal_project.yml missing

**Solutions:**

```bash
# Check if project file exists
ls -la seeknal_project.yml

# Initialize project
seeknal init --name my_project

# Or navigate to existing project
cd /path/to/project
seeknal run
```

### "Command not found: seeknal"

**Symptoms:**
```bash
$ seeknal --help
bash: seeknal: command not found
```

**Causes:**
1. Seeknal installed but not in PATH
2. Virtual environment not activated

**Solutions:**

```bash
# Activate virtual environment
source .venv/bin/activate

# Check if seeknal is available
which seeknal

# If not, reinstall with pip
pip install seeknal

# Or use python -m seeknal
python -m seeknal.cli --help
```

### "YAML parse error"

**Symptoms:**
```
Error: YAML parse error in seeknal/sources/my_source.yml: line 5, column 3
```

**Causes:**
1. Invalid YAML syntax
2. Indentation errors
3. Special characters not quoted

**Solutions:**

```bash
# Validate YAML before applying
seeknal dry-run seeknal/sources/my_source.yml

# Check YAML syntax
python -c "import yaml; yaml.safe_load(open('seeknal/sources/my_source.yml'))"

# Common fixes:
# - Use spaces, not tabs for indentation
# - Quote strings with special characters
# - Ensure proper list formatting with dashes
```

---

## Feature Store Issues

### "Feature group not found"

**Symptoms:**
```
Error: Feature group 'user_features' not found
```

**Causes:**
1. Feature group not created
2. Wrong project context
3. Typo in name

**Solutions:**

```bash
# List available feature groups
seeknal list feature-groups

# Show specific feature group
seeknal show feature-group user_features

# Check if in correct project
seeknal info

# Create feature group if needed
# (Use Python API or YAML definition)
```

### "Entity mismatch"

**Symptoms:**
```
Error: Entity 'user' does not match feature group entity
```

**Causes:**
1. Wrong entity used for query
2. Entity keys don't match

**Solutions:**

```python
# Check entity definition
from seeknal.entity import Entity
entity = Entity(name="user", join_keys=["user_id"])

# Verify feature group entity matches
fg = FeatureGroup.load(name="user_features")
print(fg.entity.name)  # Should be "user"
print(fg.entity.join_keys)  # Should be ["user_id"]
```

### "Point-in-time join failed"

**Symptoms:**
```
Error: Point-in-time join failed: duplicate keys found
```

**Causes:**
1. Duplicate entity keys in source data
2. Event time column not properly configured

**Solutions:**

```python
# Check for duplicates
df[df.duplicated(subset=['user_id', 'event_time'], keep=False)]

# Configure event time column
from seeknal.featurestore.duckdbengine.feature_group import Materialization

materialization = Materialization(
    event_time_col="event_time",
    create_date_columns=True,
)

# Remove duplicates before writing
df = df.drop_duplicates(subset=['user_id', 'event_time'])
```

### "Online table not found"

**Symptoms:**
```
Error: Online table 'user_features_online' not found
```

**Causes:**
1. Online serving not enabled
2. Table not created with serve()

**Solutions:**

```python
# Create online table
from seeknal.featurestore.duckdbengine.feature_group import (
    HistoricalFeaturesDuckDB,
)

lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])

# Serve to online store
online_table = hist.serve(name="user_features_online")

# Verify table exists
seeknal show feature-group user_features
```

---

## Performance Issues

### "Transformation is slow"

**Symptoms:**
- DuckDB task taking >10 seconds for small datasets
- Memory usage spiking

**Diagnosis:**

```python
import time
start = time.time()
result = task.transform()
print(f"Duration: {time.time() - start:.2f}s")
```

**Solutions:**

1. **Use Parquet instead of CSV:**
   ```python
   # Before: Slow CSV reading
   df = pd.read_csv("data.csv")

   # After: Fast Parquet reading
   df = pd.read_parquet("data.parquet")
   ```

2. **Add filters early in SQL:**
   ```sql
   -- Before: Process all data then filter
   SELECT * FROM __THIS__
   WHERE event_time >= '2024-01-01'

   -- After: Filter first using source config
   ```

3. **Use appropriate engine:**
   ```python
   # For <100M rows: Use DuckDB
   from seeknal.tasks.duckdb import DuckDBTask

   # For >100M rows: Use Spark
   from seeknal.tasks.sparkengine import SparkEngineTask
   ```

### "Out of memory"

**Symptoms:**
```
MemoryError: Unable to allocate array
```

**Solutions:**

1. **Process data in chunks:**
   ```python
   chunk_size = 100000
   for i in range(0, len(df), chunk_size):
       chunk = df.iloc[i:i+chunk_size]
       fg.set_dataframe(chunk)
       fg.write(feature_start_time=datetime(2024, 1, 1))
   ```

2. **Use DuckDB instead of Pandas for large data:**
   ```python
   # DuckDB processes data on-disk, no need to load all into memory
   import duckdb

   result = duckdb.query("""
       SELECT user_id, COUNT(*) as count
       FROM 'data.parquet'
       GROUP BY user_id
   ").to_df()
   ```

3. **Reduce data types:**
   ```python
   # Use smaller data types
   df['user_id'] = df['user_id'].astype('int32')  # Instead of int64
   df['amount'] = df['amount'].astype('float32')  # Instead of float64
   ```

### "Virtual environment apply is slow"

**Symptoms:**
```
seeknal env apply dev takes >5 minutes
```

**Solutions:**

```bash
# Use parallel execution
seeknal env apply dev --parallel --max-workers 8

# Limit nodes to run
seeknal env apply dev --nodes transform.clean_data

# Show plan first to see what will run
seeknal env plan dev
```

---

## DuckDB-Specific Issues

### "Catalog Error"

**Symptoms:**
```
Catalog Error: Table with name "my_table" does not exist
```

**Causes:**
1. DuckDB not properly initialized
2. Table not registered

**Solutions:**

```python
import duckdb

# Create connection
con = duckdb.connect(database=':memory:')

# Register table explicitly
con.register('my_table', df)

# Or use SQL to create table
con.execute("CREATE TABLE my_table AS SELECT * FROM df")
```

### "Type conversion error"

**Symptoms:**
```
Error: Invalid type conversion for column 'amount'
```

**Solutions:**

```python
# Convert types before DuckDB operation
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Explicitly specify schema in DuckDB
con.execute("""
    CREATE TABLE my_table (
        user_id VARCHAR,
        amount DOUBLE,
        timestamp TIMESTAMP
    )
""")
```

---

## Spark-Specific Issues

### "Java gateway process exited"

**Symptoms:**
```
Py4JJavaError: An error occurred while calling o0.pyWriteDynamic.
: java.lang.OutOfMemoryError: Java heap space
```

**Causes:**
1. Insufficient memory for Spark
2. JVM not properly configured

**Solutions:**

```python
import os
from pyspark.sql import SparkSession

# Increase memory allocation
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 4g --executor-memory 4g pyspark-shell'

spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

### "Delta Lake not found"

**Symptoms:**
```
AnalysisException: Delta Lake not found
```

**Solutions:**

```bash
# Install delta-spark
pip install delta-spark

# Configure Spark to use Delta Lake
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()
```

---

## Virtual Environment Issues

### "Plan is stale"

**Symptoms:**
```
Error: Plan is stale. Run 'seeknal env plan dev' again.
```

**Causes:**
1. YAML files changed after plan was created
2. Environment state out of sync

**Solutions:**

```bash
# Re-create plan
seeknal env plan dev

# Force apply even if stale
seeknal env apply dev --force

# Or use latest plan with --no-stale-check
seeknal env apply dev
```

### "Environment not found"

**Symptoms:**
```
Error: Virtual environment 'dev' not found
```

**Solutions:**

```bash
# List available environments
seeknal env list

# Create environment first
seeknal env plan dev

# Then apply
seeknal env apply dev
```

### "Dependency cycle detected"

**Symptoms:**
```
Error: Cycle detected in DAG: source_a -> transform_b -> source_a
```

**Causes:**
1. Circular dependency in YAML definitions
2. Incorrect deps configuration

**Solutions:**

```yaml
# Incorrect: Circular dependency
# source_a.yml
deps:
  - transform.b

# transform_b.yml
deps:
  - source.source_a

# Correct: Remove cycle
# source_a.yml
deps: []  # No dependencies

# transform_b.yml
deps:
  - source.source_a  # Depends on source
```

---

## Validation and Debugging

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or for specific module
logging.getLogger('seeknal').setLevel(logging.DEBUG)
```

### Use Dry Run Mode

```bash
# Validate without executing
seeknal dry-run seeknal/transforms/my_transform.yml

# Show execution plan without running
seeknal run --show-plan
```

### Inspect Intermediate Results

```python
# Check intermediate data
from pathlib import Path

intermediate_dir = Path("target/intermediate")
for file in intermediate_dir.glob("*.parquet"):
    print(f"{file.name}: {pd.read_parquet(file).shape}")
```

---

## Getting Help

### Diagnostic Information Collection

Before seeking help, collect this information:

```bash
# System information
seeknal info
python --version
pip list | grep seeknal

# Configuration
cat seeknal_project.yml
cat profiles.yml

# Error reproduction
# 1. Copy the exact error message
# 2. Note the command that failed
# 3. Include relevant YAML/Python code
```

### Where to Get Help

1. **Documentation**: Check [Getting Started Guide](../getting-started-comprehensive.md)
2. **CLI Reference**: Review [CLI Command Reference](cli.md)
3. **GitHub Issues**: Search [github.com/mta-tech/seeknal/issues](https://github.com/mta-tech/seeknal/issues)
4. **Discussions**: Ask in [GitHub Discussions](https://github.com/mta-tech/seeknal/discussions)

### When Creating an Issue

Include:

1. **Seeknal version**: `seeknal info`
2. **Python version**: `python --version`
3. **Operating system**: `uname -a` (Linux/macOS) or systeminfo (Windows)
4. **Error message**: Full traceback
5. **Steps to reproduce**: Minimal code/example
6. **Expected behavior**: What you expected to happen
7. **Actual behavior**: What actually happened

### Common Debugging Commands

```bash
# Validate entire project
seeknal validate

# Check database state
python -c "from seeknal.request import get_db_session; from sqlmodel import select; from seeknal.models import FeatureGroupTable; print([fg.name for fg in get_db_session().exec(select(FeatureGroupTable)).all()])"

# Test feature group loading
python -c "from seeknal.featurestore.feature_group import FeatureGroup; fg = FeatureGroup.load(name='my_features'); print(f'Loaded: {fg.name}')"

# Inspect DAG
seeknal parse --format json > manifest.json
cat manifest.json | jq '.nodes | length'
```

---

*Last updated: February 2026 | Seeknal Documentation*

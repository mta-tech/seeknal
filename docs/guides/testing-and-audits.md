# Testing & Audits

Seeknal provides built-in data quality validation at two levels: **audits** for production-time quality checks on pipeline outputs, and **feature validation** for development-time quality checks on feature groups.

---

## Audits (`seeknal audit`)

### What Are Audits?

Audits run data quality checks against cached pipeline outputs (Parquet files in `target/cache/`) without re-executing the pipeline. They verify that your data meets quality expectations after transformation, enabling fast quality validation in CI/CD pipelines.

**Key characteristics:**
- Execute against cached Parquet files from the last pipeline run
- No re-execution needed - run instantly on cached data
- Defined inline in YAML node configurations
- Support severity levels (error vs. warn) for flexible failure handling
- Run via `seeknal audit` command

**When to use audits:**
- Production data quality checks after pipeline execution
- CI/CD quality gates before deploying transformations
- Post-processing validation of transform outputs
- Quick validation without re-running expensive transformations

---

### Defining Audits in YAML

Add audit rules to any node's YAML configuration under the `audits:` section. Audits are defined as a list of rules, each with a `type`, required parameters, and optional `severity`.

**Basic structure:**

```yaml
kind: transform
name: clean_users
transform: |
  SELECT * FROM {{ ref('raw_users') }}
  WHERE email IS NOT NULL
inputs:
  - ref: source.raw_users
audits:
  - type: <audit_type>
    # ... type-specific parameters
    severity: error  # or 'warn' (default: error)
```

---

### Built-in Audit Types

#### 1. `not_null` - Check for NULL values

Verifies that specified columns contain no NULL values.

```yaml
audits:
  - type: not_null
    columns: [user_id, email, name]
    severity: error
```

**Parameters:**
- `columns` (required): List of column names to check
- `severity` (optional): `error` or `warn` (default: `error`)

**Example output:**
```
PASS not_null [user_id, email, name]: All values non-null (45ms)
```

---

#### 2. `unique` - Check for duplicates

Verifies that specified column combinations are unique (no duplicate rows).

```yaml
audits:
  - type: unique
    columns: [user_id]
    severity: error
```

**Parameters:**
- `columns` (required): List of column names that should be unique together
- `severity` (optional): `error` or `warn` (default: `error`)

**Example with composite key:**
```yaml
audits:
  - type: unique
    columns: [user_id, event_date]
    severity: error
```

**Example output:**
```
PASS unique [user_id]: All values unique (72ms)
FAIL unique [user_id, event_date] (error): 5 duplicate groups (98ms)
```

---

#### 3. `accepted_values` - Validate against allowed values

Checks that column values are within an allowed set (enum validation).

```yaml
audits:
  - type: accepted_values
    columns: [status]
    values: [active, inactive, pending]
    severity: error
```

**Parameters:**
- `columns` (required): List of columns to check (typically one column)
- `values` (required): List of allowed values
- `severity` (optional): `error` or `warn` (default: `error`)

**Example with multiple columns:**
```yaml
audits:
  - type: accepted_values
    columns: [order_status]
    values: [pending, confirmed, shipped, delivered, cancelled]
    severity: warn
```

**Example output:**
```
PASS accepted_values [status]: All values accepted (34ms)
FAIL accepted_values [order_status] (warn): 12 rows with invalid values (56ms)
```

---

#### 4. `row_count` - Validate row count bounds

Checks that the output has a minimum and/or maximum number of rows.

```yaml
audits:
  - type: row_count
    min: 1
    max: 1000000
    severity: error
```

**Parameters:**
- `min` (optional): Minimum allowed row count
- `max` (optional): Maximum allowed row count
- `severity` (optional): `error` or `warn` (default: `error`)
- **Note:** At least one of `min` or `max` must be specified

**Example - minimum only:**
```yaml
audits:
  - type: row_count
    min: 100
    severity: warn
```

**Example output:**
```
PASS row_count: Row count 5432 within bounds (12ms)
FAIL row_count (error): count 50 < min 100 (8ms)
```

---

#### 5. `custom_sql` - Custom SQL validation

Execute custom SQL queries that return failing rows. The audit passes if the query returns zero rows.

```yaml
audits:
  - type: custom_sql
    sql: "SELECT * FROM __THIS__ WHERE age < 0"
    severity: error
```

**Parameters:**
- `sql` (required): SQL query that returns failing rows
- `severity` (optional): `error` or `warn` (default: `error`)
- **Special placeholder:** `__THIS__` is replaced with the current table name

**Example - check business logic:**
```yaml
audits:
  - type: custom_sql
    sql: |
      SELECT * FROM __THIS__
      WHERE order_total < 0
         OR discount_percent > 100
         OR quantity <= 0
    severity: error
```

**Example - referential integrity:**
```yaml
audits:
  - type: custom_sql
    sql: |
      SELECT o.*
      FROM __THIS__ o
      LEFT JOIN source.customers c ON o.customer_id = c.customer_id
      WHERE c.customer_id IS NULL
    severity: warn
```

**Example output:**
```
PASS custom_sql: Custom audit passed (145ms)
FAIL custom_sql (error): 3 failing rows (234ms)
```

---

### Complete Example

```yaml
kind: transform
name: clean_orders
description: "Cleaned and validated order data"
transform: |
  SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    order_total,
    CASE
      WHEN order_status IN ('shipped', 'delivered') THEN 'fulfilled'
      WHEN order_status = 'cancelled' THEN 'cancelled'
      ELSE 'pending'
    END as fulfillment_status
  FROM source.raw_orders
  WHERE order_date >= '2024-01-01'
inputs:
  - ref: source.raw_orders
audits:
  # Primary key check
  - type: not_null
    columns: [order_id]
    severity: error

  # Uniqueness check
  - type: unique
    columns: [order_id]
    severity: error

  # Required fields
  - type: not_null
    columns: [customer_id, order_date]
    severity: error

  # Enum validation
  - type: accepted_values
    columns: [order_status]
    values: [pending, confirmed, shipped, delivered, cancelled]
    severity: warn

  # Sanity checks on data volume
  - type: row_count
    min: 1
    severity: error

  # Business logic validation
  - type: custom_sql
    sql: |
      SELECT * FROM __THIS__
      WHERE order_total < 0
         OR order_date > CURRENT_DATE
    severity: error
```

---

### Running Audits

#### Audit all nodes

```bash
seeknal audit
```

Runs audits on all nodes that have `audits:` defined. Scans `target/cache/` for Parquet files and executes configured audit rules.

**Example output:**
```
Auditing source.users:
  PASS not_null [user_id, email]: All values non-null (45ms)
  PASS unique [user_id]: All values unique (72ms)

Auditing transform.clean_orders:
  PASS not_null [order_id]: All values non-null (23ms)
  PASS unique [order_id]: All values unique (67ms)
  FAIL accepted_values [order_status] (warn): 3 rows with invalid values (41ms)
  PASS row_count: Row count 1543 within bounds (8ms)

Audit Summary: 5 passed, 1 failed
```

---

#### Audit specific node

```bash
seeknal audit transform.clean_orders
```

Runs audits only on the specified node. Use the fully qualified node name: `<kind>.<name>`.

---

#### Common workflow

```bash
# 1. Run the pipeline
seeknal run

# 2. Run audits on cached outputs
seeknal audit

# 3. If audits fail, inspect the data
seeknal query "SELECT * FROM transform.clean_orders LIMIT 10"
```

---

### Audit Output Format

Audit results show:
- **Status**: `PASS` (green) or `FAIL` (red)
- **Audit type**: The audit rule type
- **Columns**: Affected columns (if applicable)
- **Severity**: `(error)` or `(warn)` shown only on failures
- **Message**: Details about the failure
- **Duration**: Execution time in milliseconds

**Exit codes:**
- `0` - All audits passed
- `1` - One or more audits failed with `severity: error`

**Note:** Audits with `severity: warn` do not cause exit code 1, allowing the pipeline to succeed with warnings.

---

### CI/CD Integration

Add audits to your CI/CD pipeline:

```yaml
# .github/workflows/data-quality.yml
name: Data Quality Checks

on: [push, pull_request]

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install Seeknal
        run: pip install seeknal

      - name: Run pipeline
        run: seeknal run

      - name: Run audits
        run: seeknal audit
```

---

## Feature Validation (`seeknal validate-features`)

### What Is Feature Validation?

Feature validation provides **development-time quality checks** for feature groups. Unlike audits (which run on pipeline outputs), feature validation runs on feature group DataFrames using configurable validators.

**Key characteristics:**
- Runs on PySpark DataFrames during feature group development
- Configured programmatically via `ValidationConfig`
- Supports complex validators (null, range, uniqueness, freshness, custom)
- Two modes: `warn` (log failures) or `fail` (raise exception)
- Run via `seeknal validate-features` command

**When to use feature validation:**
- Feature engineering data quality checks
- ML pipeline validation before model training
- Development-time debugging of data issues
- Ensuring feature group schema compliance

---

### Built-in Validators

Seeknal provides five built-in validators in `seeknal.feature_validation.validators`:

#### 1. `NullValidator` - Detect NULL values

Checks columns for null values with configurable threshold.

```python
from seeknal.feature_validation.validators import NullValidator

# No nulls allowed (default)
validator = NullValidator(columns=["user_id", "email"])

# Allow up to 5% nulls
validator = NullValidator(
    columns=["age", "phone"],
    max_null_percentage=0.05
)
```

**Parameters:**
- `columns` (List[str]): Columns to check
- `max_null_percentage` (float): Maximum allowed null percentage (0.0-1.0, default: 0.0)

---

#### 2. `RangeValidator` - Validate numeric bounds

Checks that numeric values fall within min/max bounds.

```python
from seeknal.feature_validation.validators import RangeValidator

# Age must be between 0 and 120
validator = RangeValidator(
    column="age",
    min_val=0,
    max_val=120
)

# Price must be non-negative (min only)
validator = RangeValidator(column="price", min_val=0)

# Percentage must not exceed 100 (max only)
validator = RangeValidator(column="discount_percent", max_val=100)
```

**Parameters:**
- `column` (str): Column to validate
- `min_val` (float, optional): Minimum value (inclusive)
- `max_val` (float, optional): Maximum value (inclusive)
- **Note:** At least one of `min_val` or `max_val` required

---

#### 3. `UniquenessValidator` - Detect duplicates

Checks for duplicate rows based on specified columns.

```python
from seeknal.feature_validation.validators import UniquenessValidator

# Strict uniqueness (no duplicates)
validator = UniquenessValidator(columns=["user_id"])

# Allow up to 1% duplicates
validator = UniquenessValidator(
    columns=["user_id", "event_date"],
    max_duplicate_percentage=0.01
)
```

**Parameters:**
- `columns` (List[str]): Columns defining uniqueness (composite key)
- `max_duplicate_percentage` (float): Maximum allowed duplicate percentage (0.0-1.0, default: 0.0)

---

#### 4. `FreshnessValidator` - Check timestamp recency

Validates that timestamps are within a maximum age from current time.

```python
from datetime import timedelta
from seeknal.feature_validation.validators import FreshnessValidator

# Timestamps must be within last 24 hours
validator = FreshnessValidator(
    column="event_time",
    max_age=timedelta(hours=24)
)

# With custom reference time
from datetime import datetime
validator = FreshnessValidator(
    column="updated_at",
    max_age=timedelta(days=7),
    reference_time=datetime(2024, 1, 1, 12, 0, 0)
)
```

**Parameters:**
- `column` (str): Timestamp column to check
- `max_age` (timedelta): Maximum allowed age
- `reference_time` (datetime, optional): Reference time (default: current time)

---

#### 5. `CustomValidator` - User-defined validation

Wrap custom validation functions for flexible validation logic.

```python
from seeknal.feature_validation.validators import CustomValidator
from pyspark.sql import DataFrame

# Simple boolean function
def check_positive_values(df: DataFrame) -> bool:
    return df.filter(F.col("value") < 0).count() == 0

validator = CustomValidator(
    func=check_positive_values,
    name="positive_values_check"
)

# Lambda function
validator = CustomValidator(
    func=lambda df: df.count() > 0,
    name="non_empty_check"
)

# Function returning ValidationResult for detailed control
from seeknal.feature_validation.models import ValidationResult

def check_with_details(df: DataFrame) -> ValidationResult:
    count = df.filter(F.col("status") == "error").count()
    total = df.count()
    return ValidationResult(
        validator_name="error_check",
        passed=count == 0,
        failure_count=count,
        total_count=total,
        message=f"Found {count} error records"
    )

validator = CustomValidator(
    func=check_with_details,
    name="error_check",
    description="Check for error status records"
)
```

**Parameters:**
- `func` (Callable): Validation function returning `bool` or `ValidationResult`
- `name` (str): Validator name (default: "CustomValidator")
- `description` (str, optional): Human-readable description

---

### Configuring Feature Validation

Feature validation is configured programmatically via `ValidationConfig`:

```python
from seeknal.featurestore.feature_group import FeatureGroup
from seeknal.feature_validation.models import ValidationConfig, ValidationMode
from seeknal.feature_validation.validators import (
    NullValidator,
    RangeValidator,
    UniquenessValidator
)

# Create feature group
fg = FeatureGroup.load("user_features")

# Configure validators
validation_config = ValidationConfig(
    mode=ValidationMode.FAIL,  # or ValidationMode.WARN
    validators=[
        NullValidator(columns=["user_id", "signup_date"]),
        UniquenessValidator(columns=["user_id"]),
        RangeValidator(column="age", min_val=0, max_val=120),
        RangeValidator(column="ltv_score", min_val=0, max_val=100)
    ],
    enabled=True
)

# Attach to feature group
fg.set_validation_config(validation_config)
fg.save()
```

---

### Validation Modes

Feature validation supports two modes:

| Mode | Behavior | Exit Code | Use Case |
|------|----------|-----------|----------|
| `WARN` | Log failures but continue execution | 0 | Development, exploratory analysis |
| `FAIL` | Raise exception on first failure | 1 | Production, CI/CD pipelines |

**Example - WARN mode:**
```python
from seeknal.feature_validation.models import ValidationMode

validation_config = ValidationConfig(
    mode=ValidationMode.WARN,
    validators=[...]
)
```

All validators execute even if some fail. Failures are logged but don't halt execution.

**Example - FAIL mode (default):**
```python
from seeknal.feature_validation.models import ValidationMode

validation_config = ValidationConfig(
    mode=ValidationMode.FAIL,  # Default
    validators=[...]
)
```

Execution stops at the first validator failure, raising `ValidationException`.

---

### Running Feature Validation

#### Via CLI

```bash
# Run in FAIL mode (default)
seeknal validate-features user_features

# Run in WARN mode
seeknal validate-features user_features --mode warn

# Verbose output
seeknal validate-features user_features --mode fail --verbose
```

**Example output:**
```
Validating feature group: user_features
  Mode: fail

  Validators to run: 4
    - NullValidator
    - UniquenessValidator
    - RangeValidator
    - RangeValidator

Running validators...
  PASS NullValidator - Null check passed for columns ['user_id', 'signup_date']
  PASS UniquenessValidator - All 5432 rows are unique
  PASS RangeValidator - All 5432 non-null values within range [0, 120]
  FAIL RangeValidator - 23 values (0.42%) outside range [0, 100]

Validation stopped: Range check failed for column 'ltv_score'
```

---

#### Programmatically

```python
from seeknal.featurestore.feature_group import FeatureGroup
from seeknal.feature_validation.models import ValidationMode
from seeknal.feature_validation.validators import (
    ValidationRunner,
    NullValidator,
    UniquenessValidator
)

# Load feature group
fg = FeatureGroup.load("user_features")

# Create validators
validators = [
    NullValidator(columns=["user_id"]),
    UniquenessValidator(columns=["user_id"])
]

# Run validation
try:
    summary = fg.validate(validators=validators, mode=ValidationMode.FAIL)
    print(f"Validation passed: {summary.passed_count}/{summary.total_validators}")
except ValidationException as e:
    print(f"Validation failed: {e.message}")
```

---

## YAML Tests Block

### Overview

In addition to inline `audits:`, Seeknal supports a simplified `tests:` block in YAML for basic data quality checks on feature groups. This provides a dbt-like experience.

**Supported in:**
- Feature groups
- Aggregations

**Syntax:**

```yaml
kind: feature_group
name: user_features
# ... other configuration
tests:
  - not_null: [user_id, signup_date]
  - unique: [user_id]
```

---

### Supported Test Types

#### `not_null` - Verify no NULL values

```yaml
tests:
  - not_null: [column1, column2]
```

Checks that the specified columns contain no NULL values.

---

#### `unique` - Verify uniqueness

```yaml
tests:
  - unique: [column1]
```

Checks that the specified columns are unique (no duplicates).

**Composite key:**
```yaml
tests:
  - unique: [user_id, event_date]
```

---

### Complete Example

```yaml
kind: feature_group
name: customer_lifetime_features
description: "Customer lifetime value features"
entity:
  name: customer
  join_keys: [customer_id]
materialization:
  event_time_col: last_order_date
  offline:
    enabled: true
transform: |
  SELECT
    customer_id,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date,
    COUNT(*) as order_count,
    SUM(order_total) as lifetime_value
  FROM source.raw_orders
  GROUP BY customer_id
inputs:
  - ref: source.raw_orders
features:
  first_order_date:
    description: "Date of first order"
    dtype: date
  last_order_date:
    description: "Date of most recent order"
    dtype: date
  order_count:
    description: "Total number of orders"
    dtype: int
  lifetime_value:
    description: "Total spend across all orders"
    dtype: float
tests:
  - not_null: [customer_id]
  - unique: [customer_id]
```

---

## Comparison to Other Tools

| Feature | Seeknal | dbt | SQLMesh |
|---------|---------|-----|---------|
| **Audit Definition** | `audits:` YAML block | `tests:` in schema.yml | `audits:` in model SQL |
| **Built-in Tests** | 5 types (not_null, unique, accepted_values, row_count, custom_sql) | 4 types (not_null, unique, accepted_values, relationships) | Similar to Seeknal |
| **Custom SQL Tests** | `custom_sql` audit type | Custom test macros via `dbt test` | `audit` with SQL expressions |
| **Runtime Checks** | `seeknal audit` | `dbt test` | `sqlmesh audit` |
| **Severity Levels** | `error`, `warn` | `error`, `warn` | `error` |
| **Execution** | Runs on cached Parquet files | Re-executes models | Runs on materialized tables |
| **Feature Validation** | Built-in validators for PySpark DataFrames | Not built-in | Not built-in |
| **Validation Modes** | `warn`, `fail` | N/A | N/A |
| **CLI Command** | `seeknal audit`, `seeknal validate-features` | `dbt test` | `sqlmesh audit` |

---

## Best Practices

### 1. Use Audits for Fast Quality Gates

Audits run instantly on cached data, making them ideal for CI/CD pipelines:

```yaml
audits:
  - type: not_null
    columns: [id, created_at]
    severity: error
  - type: row_count
    min: 1
    severity: error
```

### 2. Set Appropriate Severity Levels

Use `error` for critical issues, `warn` for informational checks:

```yaml
audits:
  # Critical - must pass
  - type: unique
    columns: [user_id]
    severity: error

  # Informational - log but don't fail
  - type: accepted_values
    columns: [country_code]
    values: [US, UK, CA, AU, DE, FR]
    severity: warn
```

### 3. Combine Multiple Audit Types

Layer audits for comprehensive validation:

```yaml
audits:
  # 1. Schema validation
  - type: not_null
    columns: [order_id, customer_id, order_date]
    severity: error

  # 2. Data integrity
  - type: unique
    columns: [order_id]
    severity: error

  # 3. Business logic
  - type: custom_sql
    sql: |
      SELECT * FROM __THIS__
      WHERE order_total < 0 OR quantity <= 0
    severity: error

  # 4. Volume sanity check
  - type: row_count
    min: 100
    severity: warn
```

### 4. Use Feature Validation for ML Pipelines

Validate feature groups before training:

```python
validators = [
    NullValidator(columns=["user_id", "features"]),
    RangeValidator(column="age", min_val=0, max_val=120),
    FreshnessValidator(column="updated_at", max_age=timedelta(days=1))
]

summary = fg.validate(validators=validators, mode=ValidationMode.FAIL)
if summary.passed:
    # Proceed with training
    train_model(fg)
```

### 5. Document Custom Audits

Add comments to custom SQL audits for maintainability:

```yaml
audits:
  # Check referential integrity: all orders must have valid customer_id
  - type: custom_sql
    sql: |
      SELECT o.*
      FROM __THIS__ o
      LEFT JOIN source.customers c ON o.customer_id = c.customer_id
      WHERE c.customer_id IS NULL
    severity: error
```

---

## Troubleshooting

### Audit fails: "Table does not exist"

**Cause:** No cached data for the node.

**Solution:** Run the pipeline first:
```bash
seeknal run
seeknal audit
```

---

### Feature validation: "No validators configured"

**Cause:** Feature group has no `ValidationConfig` set.

**Solution:** Configure validators:
```python
fg.set_validation_config(ValidationConfig(validators=[...]))
fg.save()
```

---

### Audit passes but data looks wrong

**Cause:** Audits only check what you tell them to check.

**Solution:** Add more specific custom SQL audits:
```yaml
audits:
  - type: custom_sql
    sql: |
      SELECT * FROM __THIS__
      WHERE your_specific_condition
    severity: error
```

---

### Slow audit execution

**Cause:** Large cached Parquet files.

**Solution:**
- Optimize transforms to output fewer rows
- Use `row_count` with `max` to catch runaway queries early
- Consider partitioning large outputs

---

## Summary

Seeknal's testing and audits system provides:

- **Audits**: Production-time quality checks on cached outputs via `seeknal audit`
- **Feature Validation**: Development-time quality checks on feature groups via `seeknal validate-features`
- **YAML Tests**: Simple dbt-like test syntax for basic checks
- **Five audit types**: not_null, unique, accepted_values, row_count, custom_sql
- **Five validators**: NullValidator, RangeValidator, UniquenessValidator, FreshnessValidator, CustomValidator
- **Flexible severity**: error vs. warn for different failure handling
- **CI/CD ready**: Fast execution on cached data with clear exit codes

Use audits for fast quality gates in production pipelines, and feature validation for comprehensive ML pipeline data quality checks.

## See Also

- **Concepts**: [Audit](../concepts/glossary.md#audit), [Validation](../concepts/glossary.md#validation), [Rule](../concepts/glossary.md#rule)
- **Reference**: [CLI Commands](../reference/cli.md#data-quality), [YAML Schema - Audits](../reference/yaml-schema.md)
- **Tutorials**: [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md)

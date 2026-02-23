---
summary: Validate feature group data quality
read_when: You want to run data quality checks on a feature group
related:
  - validate
  - audit
  - version
---

# seeknal validate-features

Run data quality validators against a feature group. Checks for issues like
null values, out-of-range values, duplicates, and stale data based on
configured validation rules.

## Synopsis

```bash
seeknal validate-features [OPTIONS] FEATURE_GROUP
```

## Description

The `validate-features` command executes configured validators against a feature
group's data to check for data quality issues. Validators are defined in the
feature group's validation configuration.

Supported validator types:
- **null**: Check for null values (with max percentage threshold)
- **range**: Check numeric values are within bounds
- **uniqueness**: Check for duplicate values
- **freshness**: Check data is not stale

## Options

| Option | Description |
|--------|-------------|
| `FEATURE_GROUP` | Name of the feature group to validate |
| `--mode`, `-m` | Validation mode: `warn` (log and continue) or `fail` (stop on error) |
| `--verbose`, `-v` | Show detailed validation results |

## Examples

### Validate with default mode (fail)

```bash
seeknal validate-features user_features
```

### Validate with warnings only

```bash
seeknal validate-features user_features --mode warn
```

### Validate with verbose output

```bash
seeknal validate-features user_features --mode fail --verbose
```

## Configuring Validators

Validators are configured when creating a feature group:

```python
from seeknal.featurestore.feature_group import FeatureGroup
from seeknal.feature_validation.models import ValidationConfig, ValidatorConfig

fg = FeatureGroup(name="user_features")

fg.set_validation_config(ValidationConfig(validators=[
    ValidatorConfig(
        validator_type="null",
        columns=["user_id", "email"],
        params={"max_null_percentage": 0.0}
    ),
    ValidatorConfig(
        validator_type="range",
        columns=["age"],
        params={"min_val": 0, "max_val": 120}
    ),
    ValidatorConfig(
        validator_type="uniqueness",
        columns=["user_id"],
        params={"max_duplicate_percentage": 0.0}
    ),
]))
```

## Output Example

```
Validating feature group: user_features
  Mode: fail

Loading feature group...
  Validators to run: 3

Running validators...

Validation Summary
==================================================
  Total validators: 3
  Passed:           2
  Failed:           1

Results:
  Validator    Status    Message
  ----------   --------  -------
  null         PASS      No null values found
  range        FAIL      15 values out of range (0.15%)
  uniqueness   PASS      All values unique

--------------------------------------------------
✗ Validation failed with 1 error(s) for 'user_features'
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | All validations passed (or passed with warnings in warn mode) |
| 1 | Validation failed or feature group not found |

## See Also

- [seeknal validate](validate.md) - Validate configurations
- [seeknal audit](audit.md) - Run data quality audits
- [seeknal version](version.md) - Manage feature group versions

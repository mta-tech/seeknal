# Example Workflow Pipelines

This directory contains example YAML pipeline definitions demonstrating all 7 node types in the Seeknal workflow system.

## Pipeline Overview

The examples show a complete customer analytics pipeline:

```
source.users → source.transactions
      ↓              ↓
transform.clean_users
      ↓
transform.user_transactions
      ↓
feature_group.user_features
      ↓
aggregation.user_transaction_history
      ↓
model.churn_predictor
      ↓
rule.validation → rule.freshness
      ↓
exposure.api → exposure.file
```

## Node Types

### 1. Source Nodes (`01_source_*.yml`)
Load data from various sources (CSV, database, API).

**Examples:**
- `01_source_users.yml` - Load user data from CSV
- `02_source_transactions.yml` - Load transaction data from CSV

### 2. Transform Nodes (`03_transform_*.yml`)
Execute SQL transformations using DuckDB.

**Examples:**
- `03_transform_clean_users.yml` - Clean and standardize user data
- `04_transform_user_transactions.yml` - Join users with transactions

### 3. Feature Group Nodes (`05_feature_group_*.yml`)
Create and materialize feature groups in the feature store.

**Examples:**
- `05_feature_group_user_features.yml` - User-level features for ML

### 4. Model Nodes (`07_model_*.yml`)
Train ML models using scikit-learn.

**Examples:**
- `07_model_churn.yml` - Predict customer churn probability

### 5. Aggregation Nodes (`06_aggregation_*.yml`)
Compute time-series aggregations with sliding windows.

**Examples:**
- `06_aggregation_user_history.yml` - User transaction history aggregates

### 6. Rule Nodes (`08_rule_*.yml`, `09_rule_*.yml`)
Validate data quality and business rules.

**Examples:**
- `08_rule_validation.yml` - Validate data quality
- `09_rule_freshness.yml` - Ensure data freshness

### 7. Exposure Nodes (`10_exposure_*.yml`, `11_exposure_*.yml`)
Publish results to various destinations.

**Examples:**
- `10_exposure_api.yml` - Expose via REST API
- `11_exposure_file.yml` - Export to file

## Usage

To run this workflow:

```bash
# Initialize the project
seeknal init --name customer_analytics

# Copy example YAML files to seeknal/ directory
cp examples/workflow/*.yml seeknal/

# Run the workflow
seeknal run customer_analytics

# Or run in dry-run mode to see the execution plan
seeknal run customer_analytics --dry-run

# Run only specific node types
seeknal run customer_analytics --types source,transform

# Run specific nodes
seeknal run customer_analytics --nodes clean_users,user_features

# Run with full execution (ignore state)
seeknal run customer_analytics --full
```

## File Organization

```
examples/workflow/
├── README.md (this file)
├── 01_source_users.yml
├── 02_source_transactions.yml
├── 03_transform_clean_users.yml
├── 04_transform_user_transactions.yml
├── 05_feature_group_user_features.yml
├── 06_aggregation_user_history.yml
├── 07_model_churn.yml
├── 08_rule_validation.yml
├── 09_rule_freshness.yml
├── 10_exposure_api.yml
└── 11_exposure_file.yml
```

## Customization

To adapt these examples for your use case:

1. **Update file paths** in `source` nodes to point to your data
2. **Modify SQL** in `transform` nodes for your business logic
3. **Define features** in `feature_group` nodes based on your domain
4. **Configure models** in `model` nodes with your algorithms
5. **Set up aggregations** with your time windows and metrics
6. **Add validation rules** to ensure data quality
7. **Configure exposures** to publish to your destinations

## Next Steps

- Create your own pipeline definitions
- Reference the executor documentation: `src/seeknal/workflow/executors/README.md`
- Check the design document: `docs/plans/2026-01-25-yaml-pipeline-executor-design.md`

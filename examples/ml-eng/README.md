# ML Engineering: Fraud Detection Pipeline

This example demonstrates parallel execution for feature engineering at scale. The fraud detection pipeline includes 8 independent data sources that execute simultaneously, achieving up to 3x speedup.

## Pipeline Architecture

```
Layer 0 (8 sources - parallel):
├── 01_source_transactions.yml
├── 02_source_user_profiles.yml
├── 03_source_device_fingerprints.yml
├── 04_source_ip_geolocation.yml
├── 05_source_merchant_data.yml
├── 06_source_card_data.yml
├── 07_source_fraud_labels.yml
└── 08_source_realtime_alerts.yml

Layer 1 (4 transforms - parallel):
├── 09_transform_txn_enriched.yml
├── 10_transform_merchant_risk.yml
├── 11_transform_card_velocity.yml
└── 12_transform_alert_features.yml

Layer 2 (2 feature groups - parallel):
├── 13_feature_group_txn_features.yml
└── 14_feature_group_user_risk.yml

Layer 3 (1 exposure):
└── 15_exposure_training_data.yml
```

## Total Pipeline
- **15 nodes** across **4 topological layers**
- **14 nodes** can run in parallel (all except the final exposure)
- **Theoretical speedup:** 3.75x (15 nodes / 4 layers)

## Usage

### Sequential Execution
```bash
cd examples/ml-eng
seeknal parse
seeknal run
```

### Parallel Execution
```bash
seeknal run --parallel
```

### Parallel with Custom Workers
```bash
# Use 4 workers (for 4-core machines)
seeknal run --parallel --max-workers 4

# Use 16 workers (for high-core machines)
seeknal run --parallel --max-workers 16
```

### Environment Experimentation
```bash
# Plan changes
seeknal env plan experiment

# Apply changes
seeknal env apply experiment --parallel

# Promote to production
seeknal env promote experiment prod
```

## Expected Performance

On an 8-core machine:
- **Sequential:** ~2.07s (15 nodes × 0.14s avg)
- **Parallel:** ~0.72s (4 layers × 0.18s avg)
- **Speedup:** 2.87x

## Tutorial

For a complete walkthrough, see:
`docs/tutorials/ml-eng-parallel.md`

## Data Requirements

This example assumes CSV data files in a `data/` subdirectory:
- `data/transactions.csv`
- `data/user_profiles.csv`
- `data/device_fingerprints.csv`
- `data/ip_geolocation.csv`
- `data/merchant_data.csv`
- `data/card_data.csv`
- `data/fraud_labels.csv`
- `data/realtime_alerts.csv`

See the YAML files for expected schema.

## Key Features Demonstrated

1. **Parallel Execution** - 8 sources run simultaneously
2. **Topological Layers** - Automatic dependency grouping
3. **Environment Workflow** - Plan/apply/promote cycle
4. **Breaking Change Detection** - Schema change awareness
5. **Max Workers Tuning** - Optimize for machine size

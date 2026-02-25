# Data Engineering Tutorial Examples

This directory contains 8 example YAML files for the data engineering tutorial on virtual environments and parallel execution.

## Files

- `01_source_orders.yml` - E-commerce order transactions
- `02_source_customers.yml` - Customer profile information
- `03_source_inventory.yml` - Product inventory levels
- `04_transform_clean_orders.yml` - Clean and validate order data
- `05_transform_customer_enriched.yml` - Enrich customers with order metrics
- `06_transform_inventory_status.yml` - Calculate inventory availability status
- `07_aggregation_daily_sales.yml` - Daily sales metrics aggregation
- `08_exposure_warehouse.yml` - Data warehouse export

## Pipeline Architecture

```
Sources (3)
├── orders ──────────┐
├── customers ───────┼───┐
└── inventory ───┐   │   │
                 │   │   │
Transforms (3)   │   │   │
├── clean_orders ◄───┘   │
├── customer_enriched ◄──┤
│   (depends on: customers, clean_orders)
└── inventory_status ◄───┘

Aggregations (1)
└── daily_sales
    (depends on: clean_orders)

Exposures (1)
└── warehouse_export
    (depends on: customer_enriched)
```

## Usage

Follow the tutorial at: `docs/tutorials/data-eng-environments.md`

### Quick Start

```bash
# Create project directory
mkdir -p ~/seeknal-data-eng-tutorial
cd ~/seeknal-data-eng-tutorial

# Copy example files
mkdir -p seeknal
cp /path/to/seeknal/examples/data-eng/*.yml seeknal/

# Create sample data (see tutorial for details)
mkdir -p data
# ... create CSV files (orders.csv, customers.csv, inventory.csv)

# Run the pipeline
seeknal parse
seeknal run

# Test virtual environments
seeknal env plan dev
seeknal env apply dev

# Test parallel execution
seeknal run --parallel
```

## Key Features Demonstrated

1. **Virtual Environments**
   - Plan changes before applying
   - Isolated testing in dev environment
   - Safe promotion to production

2. **Parallel Execution**
   - Independent nodes run concurrently
   - Configurable worker count
   - Faster pipeline execution

3. **Change Detection**
   - BREAKING changes (column removal, type change)
   - NON_BREAKING changes (column addition, SQL logic)
   - METADATA changes (description, tags)

4. **DAG Dependencies**
   - Automatic topological sorting
   - Upstream source execution
   - Downstream rebuild propagation

## Testing

Run the test suite:

```bash
pytest tests/tutorials/test_data_eng_tutorial.py -v
```

This validates:
- YAML files parse correctly
- DAG builds without cycles
- Change categorization works
- Dependencies are correct

## Learn More

- [Data Engineering Tutorial](../../docs/tutorials/data-eng-environments.md)
- [YAML Pipeline Tutorial](../../docs/tutorials/yaml-pipeline-tutorial.md)
- [API Reference](../../docs/api/yaml-schema.md)

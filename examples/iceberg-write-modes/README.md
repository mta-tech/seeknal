# Iceberg Write Modes

These examples show the two keyed/partition-aware Iceberg write modes. Configure
the catalog and credentials in `profiles.yml`, then place the YAML nodes under
the matching `seeknal/` project directories.

## SCD Type 1 dimension with `upsert`

[`dim_customer.yml`](dim_customer.yml) uses the singular `materialization:`
form. `customer_id` is a stable business key. Every incoming row is a complete
row image: existing customers are updated and new customers are inserted.
Customers absent from the batch remain unchanged.

Generate surrogate keys upstream when the warehouse model requires them. The
writer does not create surrogate keys or manage SCD Type 2 history.

## Complete daily partitions with `insert_overwrite`

[`mart_sales_daily.yml`](mart_sales_daily.yml) recomputes complete daily results
and replaces only the `sales_date` partitions present in that result. Older
partitions remain intact. Do not send a delta for a touched date: rows missing
from the incoming result will be removed from that date's partition.

Only identity partition columns are supported. The declared `partition_by`
must match the target table's current partition spec.

## Multiple targets

[`multi_target_customer.yml`](multi_target_customer.yml) shows plural
`materializations:`. Each Iceberg target has its own transaction; success is not
atomic across both tables.

## Python equivalents

Typed inline configuration:

```python
from seeknal.pipeline.decorators import transform
from seeknal.pipeline.materialization_config import MaterializationConfig


@transform(
    name="dim_customer",
    inputs=["source.customer_updates"],
    materialization=MaterializationConfig(
        enabled=True,
        table="atlas.analytics.dim_customer",
        mode="upsert",
        unique_keys=["customer_id"],
        create_table=True,
        max_batch_bytes=268435456,
    ),
)
def dim_customer(ctx):
    return ctx.ref("source.customer_updates")
```

Stackable target decorators:

```python
from seeknal.pipeline.decorators import materialize, transform


@transform(name="mart_sales_daily", inputs=["source.sales"])
@materialize(
    type="iceberg",
    table="atlas.analytics.mart_sales_daily",
    mode="insert_overwrite",
    partition_by=["sales_date"],
    create_table=True,
)
def mart_sales_daily(ctx):
    return ctx.ref("source.sales")
```

`max_batch_bytes` defaults to 256 MiB and measures the frozen Arrow table's
logical `nbytes`; it is not a process-memory limit. An empty batch never deletes
a partition. If the target is missing, `create_table: true` makes empty input a
no-op without creating the table, while `create_table: false` fails.

Commit conflicts and unknown commit outcomes are returned as failures without a
blind retry. Inspect the table's current snapshot and data before deciding to
rerun.

Useful local commands:

```bash
seeknal docs --list
seeknal docs run
seeknal run --dry-run
seeknal run
seeknal run --full --profile profiles.yml
```

Local catalog/FileIO tests do not validate the actual deployed Lakekeeper,
object storage, credentials, or TLS path. Smoke-test the selected deployment
before using these modes with production data.

from seeknal.pipeline import feature_group


@feature_group(
    name="customer_profile",
    entity={"name": "customer", "join_keys": ["customer_id"]},
    features={
        "customer_id": {
            "dtype": "string",
            "description": "Stable customer identifier",
        },
        "age": {"dtype": "integer", "description": "Customer age in years"},
        "lifetime_value": {
            "dtype": "float",
            "description": "Current customer lifetime value",
        },
        "observed_at": {
            "dtype": "timestamp",
            "description": "Source observation time",
        },
    },
    inputs=["source.customers"],
    materializations=[
        {
            "type": "atlas_online",
            "connection": "atlas_feature_store",
            "table": "customer_profile",
            "mode": "replace",
            "event_time_column": "observed_at",
            "ttl_seconds": 86400,
        }
    ],
    tags=["customer", "online-serving"],
)
def customer_profile(ctx):
    return ctx.duckdb.sql("""
        SELECT
            customer_id,
            age,
            lifetime_value,
            CAST(observed_at AS TIMESTAMP) AS observed_at
        FROM source.customers
        """).df()

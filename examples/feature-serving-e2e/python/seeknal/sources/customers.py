from seeknal.pipeline import source


@source(
    name="customers",
    source="csv",
    table="data/customers.csv",
    columns={
        "customer_id": {"dtype": "string"},
        "age": {"dtype": "integer"},
        "lifetime_value": {"dtype": "float"},
        "observed_at": {"dtype": "timestamp"},
    },
)
def customers():
    pass

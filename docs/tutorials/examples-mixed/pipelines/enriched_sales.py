# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""
Transform: Enrich sales data with product categories.

This Python node references a YAML source node.
"""

from seeknal.pipeline import transform


@transform(
    name="enriched_sales",
    description="Add product category and margin to sales data",
)
def enriched_sales(ctx):
    """Enrich sales data with product information."""
    # Reference YAML source
    sales = ctx.ref("source.raw_sales")

    return ctx.duckdb.sql("""
        SELECT
            s.*,
            -- Add product categories based on product_id
            CASE
                WHEN s.product_id LIKE 'ELE-%' THEN 'Electronics'
                WHEN s.product_id LIKE 'CLO-%' THEN 'Clothing'
                WHEN s.product_id LIKE 'FOO-%' THEN 'Food'
                ELSE 'Other'
            END as product_category,
            -- Calculate margin (30% profit margin)
            (s.unit_price * 0.7) as unit_cost,
            (s.unit_price - (s.unit_price * 0.7)) as margin
        FROM sales s
    """).df()

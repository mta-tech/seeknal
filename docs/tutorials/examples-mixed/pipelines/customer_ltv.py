# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""
Transform: Calculate customer lifetime value.

This Python node references multiple YAML source nodes.
"""

from seeknal.pipeline import transform


@transform(
    name="customer_ltv",
    description="Calculate customer lifetime value (LTV)",
)
def customer_ltv(ctx):
    """Calculate LTV from orders and products."""
    # Reference multiple YAML sources
    orders = ctx.ref("source.raw_orders")
    products = ctx.ref("source.product_catalog")

    # Register for SQL access
    ctx.duckdb.register("orders", orders)
    ctx.duckdb.register("products", products)

    return ctx.duckdb.sql("""
        WITH order_totals AS (
            SELECT
                o.customer_id,
                SUM(o.quantity * p.unit_price) as total_spend,
                COUNT(DISTINCT o.order_id) as order_count,
                MIN(o.order_date) as first_order,
                DATEDIFF('day', CURRENT_DATE, MIN(o.order_date)) as days_since_first_order
            FROM orders o
            LEFT JOIN products p ON o.product_id = p.product_id
            GROUP BY o.customer_id
        )
        SELECT
            customer_id,
            total_spend,
            order_count,
            first_order,
            -- Calculate average order value
            total_spend / NULLIF(order_count, 0) as avg_order_value,
            days_since_first_order,
            -- Simple LTV prediction (3x AOV * order_count)
            (total_spend / NULLIF(order_count, 0)) * 3 * order_count as predicted_ltv
        FROM order_totals
        ORDER BY predicted_ltv DESC
    """).df()

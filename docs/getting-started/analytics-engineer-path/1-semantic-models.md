# Chapter 1: Define Semantic Models

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** YAML & Python

Learn to create semantic models that provide business-friendly views of your data, enabling self-service analytics across your organization.

---

## What You'll Build

A semantic model for e-commerce analytics that defines:

```
Orders Semantic Model
├── Entities (Order, Customer, Product)
├── Dimensions (Status, Date, Category)
└── Measures (Revenue, Count, AOV)
```

**After this chapter, you'll have:**
- A complete semantic model with entities, dimensions, and measures
- Queryable semantic layer for business users
- Foundation for business metrics

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Quick Start](../../quick-start/) — Basic pipeline builder workflow
- [ ] [AE Path Overview](index.md) — Introduction to this path
- [ ] Comfortable with SQL JOINs and aggregations

---

## Part 1: Understand Semantic Models (7 minutes)

### What Are Semantic Models?

Semantic models provide **business-friendly views** of your data:

| Traditional SQL | Semantic Model |
|-----------------|----------------|
| `SELECT SUM(revenue) FROM orders WHERE status = 'completed'` | `orders.total_revenue` |
| `SELECT COUNT(*) FROM users WHERE created_at > '2024-01-01'` | `users.new_users` |
| `SELECT AVG(amount) FROM transactions` | `transactions.avg_amount` |

**Benefits:**
- **Business language**: Use terms stakeholders understand
- **Reusable**: Define once, query from anywhere
- **Governed**: Controlled definitions prevent inconsistencies
- **Self-serve**: Non-technical users can build queries

### Semantic Model Components

=== "Entities"

    **Entities** are the core business objects in your data:

    ```yaml
    entities:
      - name: customer
        id: customer_id
        type: profile  # Profile vs Transaction

      - name: order
        id: order_id
        type: transaction
    ```

    **Entity Types:**
    - **Profile**: Stable attributes (customer, product)
    - **Transaction**: Events over time (order, visit, click)

=== "Dimensions"

    **Dimensions** are attributes for filtering and grouping:

    ```yaml
    dimensions:
      # Categorical dimension
      - name: order_status
        type: categorical
        values: [pending, completed, cancelled]

      # Time dimension
      - name: order_date
        type: time
        granularity: [day, week, month]

      # Geographic dimension
      - name: country
        type: geographic
        hierarchy: [country, region, city]
    ```

=== "Measures"

    **Measures** are aggregatable metrics:

    ```yaml
    measures:
      - name: total_revenue
        expression: SUM(revenue)
        type: currency

      - name: order_count
        expression: COUNT(*)
        type: numeric

      - name: avg_order_value
        expression: AVG(revenue)
        type: currency
    ```

---

## Part 2: Create Semantic Model (10 minutes)

=== "YAML Approach"

    Create a new project:

    ```bash
    seeknal init ecommerce-analytics
    cd ecommerce-analytics
    ```

    Create the orders semantic model:

    ```bash
    seeknal draft semantic-model --name orders
    ```

    Edit `semantic-models/orders.yaml`:

    ```yaml
    kind: semantic_model
    name: orders
    description: "E-commerce order transactions"

    # Data source
    source: warehouse.orders

    # Entities
    entities:
      - name: order
        id: order_id
        type: transaction
        description: "Individual order transaction"

      - name: customer
        id: customer_id
        type: profile
        description: "Customer who placed the order"

      - name: product
        id: product_id
        type: profile
        description: "Product purchased"

    # Dimensions
    dimensions:
      # Categorical dimensions
      - name: status
        type: categorical
        column: order_status
        description: "Order status"
        values: [pending, completed, cancelled, refunded]

      - name: category
        type: categorical
        column: product_category
        description: "Product category"

      # Time dimensions
      - name: order_date
        type: time
        column: created_at
        description: "Date order was placed"
        granularity: [day, week, month, quarter, year]

      - name: order_hour
        type: time
        column: created_at
        description: "Hour of day order was placed"
        granularity: [hour]

      # Geographic dimensions
      - name: country
        type: geographic
        column: billing_country
        description: "Customer billing country"

      - name: region
        type: geographic
        column: billing_state
        description: "Customer billing state/region"

    # Measures
    measures:
      - name: total_revenue
        expression: SUM(revenue)
        type: currency
        description: "Total order revenue"

      - name: order_count
        expression: COUNT(*)
        type: numeric
        description: "Total number of orders"

      - name: avg_order_value
        expression: AVG(revenue)
        type: currency
        description: "Average order value"

      - name: total_items
        expression: SUM(quantity)
        type: numeric
        description: "Total items ordered"

      - name: unique_customers
        expression: COUNT(DISTINCT customer_id)
        type: numeric
        description: "Number of unique customers"

    # Relationships
    relationships:
      - from: customer
        to: order
        type: one_to_many
        description: "A customer can have many orders"

      - from: product
        to: order
        type: one_to_many
        description: "A product can be in many orders"
    ```

=== "Python Approach"

    Create `semantic_models.py`:

    ```python
    #!/usr/bin/env python3
    """
    Semantic Models - Chapter 1
    Define semantic models for e-commerce analytics
    """

    from seeknal.semantic_layer import SemanticModel, Entity, Dimension, Measure

    # Create orders semantic model
    orders_model = SemanticModel(
        name="orders",
        description="E-commerce order transactions",
        source="warehouse.orders"
    )

    # Add entities
    orders_model.add_entity(Entity(
        name="order",
        id="order_id",
        type="transaction",
        description="Individual order transaction"
    ))

    orders_model.add_entity(Entity(
        name="customer",
        id="customer_id",
        type="profile",
        description="Customer who placed the order"
    ))

    orders_model.add_entity(Entity(
        name="product",
        id="product_id",
        type="profile",
        description="Product purchased"
    ))

    # Add dimensions
    orders_model.add_dimension(Dimension(
        name="status",
        type="categorical",
        column="order_status",
        description="Order status",
        values=["pending", "completed", "cancelled", "refunded"]
    ))

    orders_model.add_dimension(Dimension(
        name="order_date",
        type="time",
        column="created_at",
        description="Date order was placed",
        granularity=["day", "week", "month"]
    ))

    orders_model.add_dimension(Dimension(
        name="country",
        type="geographic",
        column="billing_country",
        description="Customer billing country"
    ))

    # Add measures
    orders_model.add_measure(Measure(
        name="total_revenue",
        expression="SUM(revenue)",
        type="currency",
        description="Total order revenue"
    ))

    orders_model.add_measure(Measure(
        name="order_count",
        expression="COUNT(*)",
        type="numeric",
        description="Total number of orders"
    ))

    orders_model.add_measure(Measure(
        name="avg_order_value",
        expression="AVG(revenue)",
        type="currency",
        description="Average order value"
    ))

    # Add relationships
    orders_model.add_relationship(
        from_entity="customer",
        to_entity="order",
        type="one_to_many"
    )

    # Save the model
    orders_model.save("semantic-models/orders.yaml")

    print("Semantic model created successfully!")
    ```

### Apply the Semantic Model

```bash
# YAML approach
seeknal apply semantic-models/orders.yaml

# Python approach
python semantic_models.py
```

**Expected output:**
```
Applying semantic model 'orders'...
  ✓ Validated YAML schema
  ✓ Checked entity definitions
  ✓ Verified dimensions
  ✓ Registered measures
Semantic model applied successfully!
```

---

## Part 3: Query Semantic Models (8 minutes)

### Understanding Semantic Queries

Semantic models automatically generate business-friendly SQL:

=== "Query with seeknal query"

    ```bash
    # Query total revenue by status
    seeknal query --model orders --measure total_revenue --group-by status
    ```

    **Generated SQL:**
    ```sql
    SELECT
      order_status as status,
      SUM(revenue) as total_revenue
    FROM warehouse.orders
    GROUP BY order_status
    ORDER BY total_revenue DESC
    ```

    **Expected output:**
    ```
    status          | total_revenue
    ----------------|--------------
    completed       | $125,430.00
    pending         | $15,230.00
    cancelled       | $3,420.00
    ```

=== "Query with Python"

    ```python
    from seeknal.semantic_layer import Query

    # Create a query
    query = Query(model="orders")

    # Add measures and dimensions
    query.add_measure("total_revenue")
    query.add_dimension("status")

    # Add filters
    query.filter("order_date >= '2024-01-01'")

    # Execute
    results = query.execute()
    print(results)
    ```

    **Expected output:**
    ```
       status  total_revenue
    0  completed      125430.00
    1    pending       15230.00
    2  cancelled        3420.00
    ```

### Advanced Queries

=== "Time-based Analysis"

    ```bash
    # Daily revenue trend
    seeknal query --model orders \
      --measure total_revenue,order_count \
      --group-by order_date \
      --filter "order_date >= '2024-01-01'" \
      --order-by order_date
    ```

=== "Multi-dimensional Analysis"

    ```bash
    # Revenue by status and category
    seeknal query --model orders \
      --measure total_revenue \
      --group-by status,category \
      --filter "order_date >= '2024-01-01'"
    ```

=== "Comparison Analysis"

    ```python
    # Python: Month-over-month comparison
    query = Query(model="orders")

    query.add_measure("total_revenue")
    query.add_dimension("order_date")

    # Add time comparison
    query.compare_periods(
        period="month",
        comparison="previous_period"
    )

    results = query.execute()
    ```

---

## What Makes Semantic Models Powerful?

!!! success "Single Source of Truth"
    Before semantic models:
    ```sql
    -- Analyst A's definition
    SUM(CASE WHEN status = 'completed' THEN revenue END)

    -- Analyst B's definition
    SUM(CASE WHEN status IN ('completed', 'pending') THEN revenue END)
    ```

    After semantic models:
    ```yaml
    # Everyone uses the same definition
    measure: total_revenue
    expression: SUM(CASE WHEN status = 'completed' THEN revenue END)
    ```

!!! tip "Self-Serve Analytics"
    Business users can now ask:
    - "What's our revenue by month?"
    - "How many orders per category?"
    - "What's the average order value by region?"

    Without writing SQL!

---

## Summary

In this chapter, you learned:

- [x] **Semantic Model Components** — Entities, dimensions, measures
- [x] **Creating Semantic Models** — YAML and Python approaches
- [x] **Entity Relationships** — One-to-many, many-to-many
- [x] **Querying Semantic Models** — Business-friendly queries
- [x] **Governance Benefits** — Single source of truth

**Key Commands:**
```bash
seeknal draft semantic-model --name <name>
seeknal apply semantic-models/<name>.yaml
seeknal query --model <name> --measure <measure>
```

---

## What's Next?

[Chapter 2: Create Business Metrics →](2-business-metrics.md)

Build on your semantic models to create reusable business metrics and KPIs.

---

## See Also

- **[Semantic Layer Guide](../../guides/semantic-layer.md)** — Deep dive on semantic layer
- **[Building Blocks: Semantic Models](../../building-blocks/semantic-models.md)** — Reference documentation
- **[CLI Reference](../../reference/cli.md)** — All semantic model commands
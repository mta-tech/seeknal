#!/usr/bin/env python3
"""
Generate unified sample datasets for Seeknal documentation.

This script creates a consistent e-commerce dataset that works across
all three persona paths (Data Engineer, Analytics Engineer, ML Engineer).

Usage:
    python scripts/generate_sample_data.py [--output-dir examples/data]
"""

import argparse
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import pandas as pd  # type: ignore[import-untyped]
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas", file=sys.stderr)
    sys.exit(1)


# Configuration
NUM_PRODUCTS = 100
NUM_CUSTOMERS = 1000
NUM_SALES = 10000
NUM_ORDERS = 5000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)


# Data constants
REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America"]
COUNTRIES = {
    "North America": ["USA", "Canada", "Mexico"],
    "Europe": ["UK", "Germany", "France", "Spain", "Netherlands", "Belgium"],
    "Asia Pacific": ["Australia", "Japan", "Singapore"],
    "Latin America": ["Brazil", "Argentina", "Chile"]
}

CUSTOMER_SEGMENTS = ["enterprise", "mid-market", "smb", "startup"]
CUSTOMER_TIERS = ["platinum", "gold", "silver", "bronze"]

PRODUCT_CATEGORIES = [
    "Electronics", "Furniture", "Office Supplies", "Accessories",
    "Software", "Services", "Books", "Home & Garden"
]

PRODUCT_SUBCATEGORIES = {
    "Electronics": ["Computers", "Phones", "Tablets", "Audio", "Accessories"],
    "Furniture": ["Desks", "Chairs", "Storage", "Lighting"],
    "Office Supplies": ["Paper", "Pens", "Folders", "Calendars"],
    "Accessories": ["Bags", "Cases", "Cables", "Stands"],
    "Software": ["Productivity", "Security", "Design", "Development"],
    "Services": ["Consulting", "Support", "Training", "Installation"],
    "Books": ["Technical", "Business", "Design", "Programming"],
    "Home & Garden": ["Decor", "Tools", "Kitchen", "Outdoor"]
}

SHIPPING_METHODS = ["standard", "express", "overnight"]
PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "check"]
ORDER_STATUSES = ["pending", "completed", "cancelled", "refunded"]
SALES_STATUSES = ["completed", "pending", "cancelled", "refunded"]

SUPPLIERS = [
    "Acme Corp", "Global Supplies Inc", "Tech distributors Ltd",
    "Office Essentials", "Furniture Plus", "Electronics World",
    "Paper Products Co", "Software Solutions Inc"
]


def generate_products(n: int = NUM_PRODUCTS) -> pd.DataFrame:
    """Generate product catalog."""
    print(f"Generating {n} products...")

    products = []
    for i in range(1, n + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        subcategory = random.choice(PRODUCT_SUBCATEGORIES[category])

        # Price follows Pareto distribution (realistic pricing)
        base_price = random.paretovariate(2) * 10
        price = round(base_price, 2)
        cost = round(price * random.uniform(0.4, 0.7), 2)

        products.append({
            "product_id": f"PROD{i:03d}",
            "name": f"{category} - {subcategory} {i}",
            "category": category,
            "subcategory": subcategory,
            "price": price,
            "cost": cost,
            "supplier": random.choice(SUPPLIERS)
        })

    df = pd.DataFrame(products)
    print(f"  ✓ Generated {len(df)} products")
    return df


def generate_customers(n: int = NUM_CUSTOMERS) -> pd.DataFrame:
    """Generate customer master data."""
    print(f"Generating {n} customers...")

    customers = []
    names = [
        "John Smith", "Jane Doe", "Bob Johnson", "Alice Williams",
        "Charlie Brown", "Diana Prince", "Edward Stone", "Fiona Green"
    ]

    for i in range(1, n + 1):
        segment = random.choice(CUSTOMER_SEGMENTS)
        # Tier correlates with segment
        if segment == "enterprise":
            tier = random.choice(["platinum", "gold"])
        elif segment == "mid-market":
            tier = random.choice(["gold", "silver"])
        else:
            tier = random.choice(["silver", "bronze"])

        # Signup date distribution (more recent signups)
        days_ago = int(random.expovariate(1/365))  # Exponential distribution
        signup_date = START_DATE - timedelta(days=min(days_ago, 730))

        region = random.choices(REGIONS, weights=[40, 35, 15, 10])[0]
        country = random.choice(COUNTRIES[region])

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "name": f"{random.choice(names)} {i}",
            "email": f"customer{i:04d}@example.com",
            "segment": segment,
            "signup_date": signup_date.strftime("%Y-%m-%d"),
            "country": country,
            "tier": tier,
            "region": region
        })

    df = pd.DataFrame(customers)
    print(f"  ✓ Generated {len(df)} customers")
    return df


def generate_sales(
    n: int = NUM_SALES,
    products: pd.DataFrame = None,
    customers: pd.DataFrame = None
) -> pd.DataFrame:
    """Generate sales transaction data."""
    print(f"Generating {n} sales transactions...")

    if products is None:
        products = generate_products()
    if customers is None:
        customers = generate_customers()

    sales = []
    transaction_id = 1001

    # Generate transactions throughout the year
    current_date = START_DATE
    days_per_transaction = (END_DATE - START_DATE).days / n

    for i in range(n):
        # Select random customer (weighted toward active customers)
        customer_idx = random.choices(
            range(len(customers)),
            weights=[1.0 / (j % 100 + 1) for j in range(len(customers))],
            k=1
        )[0]
        customer = customers.iloc[customer_idx]

        # Select random product
        product_idx = random.randint(0, len(products) - 1)
        product = products.iloc[product_idx]

        # Transaction date (evenly distributed but with some randomness)
        days_offset = int(i * days_per_transaction + random.uniform(-2, 2))
        transaction_date = START_DATE + timedelta(days=max(0, days_offset))

        # Quantity and pricing
        quantity = random.randint(1, 10)
        unit_price = product["price"] * random.uniform(0.9, 1.1)  # Slight price variation

        # Status weighted toward completed
        status = random.choices(
            SALES_STATUSES,
            weights=[70, 15, 10, 5]
        )[0]

        sales.append({
            "transaction_id": transaction_id,
            "date": transaction_date.strftime("%Y-%m-%d"),
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": round(unit_price, 2),
            "region": customer["region"],
            "status": status
        })

        transaction_id += 1

    df = pd.DataFrame(sales)
    df = df.sort_values("date").reset_index(drop=True)
    print(f"  ✓ Generated {len(df)} sales transactions")
    return df


def generate_orders(sales: pd.DataFrame, n: int = NUM_ORDERS) -> pd.DataFrame:
    """Generate order-level aggregations from sales."""
    print(f"Generating {n} orders from sales data...")

    # Group sales by order (simulate orders with multiple items)
    # For simplicity, we'll create orders from a subset of sales
    sales_subset = sales.sample(n=min(n, len(sales)))

    orders = []
    order_id = 5001

    for _, sale in sales_subset.iterrows():
        # Calculate order total (add random shipping cost)
        subtotal = sale["quantity"] * sale["unit_price"]
        shipping_cost = random.choice([0, 9.99, 14.99, 24.99])
        total_amount = subtotal + shipping_cost

        # Order date (same day or next day as sale)
        sale_date = datetime.strptime(sale["date"], "%Y-%m-%d")
        order_date = sale_date + timedelta(days=random.choice([0, 1]))

        # Payment and shipping
        shipping_method = random.choices(
            SHIPPING_METHODS,
            weights=[50, 35, 15]
        )[0]

        payment_method = random.choices(
            PAYMENT_METHODS,
            weights=[40, 30, 20, 10]
        )[0]

        # Order status (correlated with sale status)
        if sale["status"] == "completed":
            order_status = random.choices(
                ORDER_STATUSES,
                weights=[5, 85, 5, 5]
            )[0]
        else:
            order_status = sale["status"]

        orders.append({
            "order_id": order_id,
            "customer_id": sale["customer_id"],
            "order_date": order_date.strftime("%Y-%m-%d"),
            "total_amount": round(total_amount, 2),
            "shipping_method": shipping_method,
            "payment_method": payment_method,
            "status": order_status
        })

        order_id += 1

    df = pd.DataFrame(orders)
    df = df.sort_values("order_date").reset_index(drop=True)
    print(f"  ✓ Generated {len(df)} orders")
    return df


def validate_data(
    products: pd.DataFrame,
    customers: pd.DataFrame,
    sales: pd.DataFrame,
    orders: pd.DataFrame
) -> bool:
    """Validate data quality and relationships."""
    print("\nValidating data...")

    errors = []

    # Check primary keys are unique
    if products["product_id"].duplicated().any():
        errors.append("Duplicate product_id found")
    if customers["customer_id"].duplicated().any():
        errors.append("Duplicate customer_id found")
    if sales["transaction_id"].duplicated().any():
        errors.append("Duplicate transaction_id found")
    if orders["order_id"].duplicated().any():
        errors.append("Duplicate order_id found")

    # Check foreign keys
    sales_customers = set(sales["customer_id"])
    customer_ids = set(customers["customer_id"])
    invalid_sales_customers = sales_customers - customer_ids
    if invalid_sales_customers:
        errors.append(f"Sales references non-existent customers: {invalid_sales_customers}")

    sales_products = set(sales["product_id"])
    product_ids = set(products["product_id"])
    invalid_sales_products = sales_products - product_ids
    if invalid_sales_products:
        errors.append(f"Sales references non-existent products: {invalid_sales_products}")

    orders_customers = set(orders["customer_id"])
    invalid_orders_customers = orders_customers - customer_ids
    if invalid_orders_customers:
        errors.append(f"Orders references non-existent customers: {invalid_orders_customers}")

    # Check for null values in key fields
    for df_name, df, key_columns in [
        ("products", products, ["product_id"]),
        ("customers", customers, ["customer_id"]),
        ("sales", sales, ["transaction_id", "customer_id", "product_id"]),
        ("orders", orders, ["order_id", "customer_id"])
    ]:
        for col in key_columns:
            if df[col].isnull().any():
                errors.append(f"{df_name}.{col} contains null values")

    if errors:
        print("  ✗ Validation errors:")
        for error in errors:
            print(f"    - {error}")
        return False
    else:
        print("  ✓ All validations passed")
        return True


def generate_schema_yml(output_dir: Path):
    """Generate schema.yml file describing the data."""
    schema_content = """# Sample Data Schema
# Seeknal Documentation Examples

version: 1

datasets:
  - name: sales
    description: E-commerce sales transactions
    rows: 10000
    columns:
      - name: transaction_id
        type: int
        description: Unique transaction identifier
      - name: date
        type: date
        description: Transaction date
      - name: customer_id
        type: str
        description: Foreign key to customers.customer_id
      - name: product_id
        type: str
        description: Foreign key to products.product_id
      - name: quantity
        type: int
        description: Quantity purchased
      - name: unit_price
        type: float
        description: Price per unit
      - name: region
        type: str
        description: Sales region
      - name: status
        type: str
        description: Transaction status

  - name: orders
    description: Order-level aggregations
    rows: 5000
    columns:
      - name: order_id
        type: int
        description: Unique order identifier
      - name: customer_id
        type: str
        description: Foreign key to customers.customer_id
      - name: order_date
        type: date
        description: Order date
      - name: total_amount
        type: float
        description: Order total including shipping
      - name: shipping_method
        type: str
        description: Shipping method
      - name: payment_method
        type: str
        description: Payment method
      - name: status
        type: str
        description: Order status

  - name: customers
    description: Customer master data
    rows: 1000
    columns:
      - name: customer_id
        type: str
        description: Unique customer identifier
      - name: name
        type: str
        description: Customer name
      - name: email
        type: str
        description: Customer email
      - name: segment
        type: str
        description: Customer segment
      - name: signup_date
        type: date
        description: Customer signup date
      - name: country
        type: str
        description: Customer country
      - name: tier
        type: str
        description: Customer tier
      - name: region
        type: str
        description: Customer region

  - name: products
    description: Product catalog
    rows: 100
    columns:
      - name: product_id
        type: str
        description: Unique product identifier
      - name: name
        type: str
        description: Product name
      - name: category
        type: str
        description: Product category
      - name: subcategory
        type: str
        description: Product subcategory
      - name: price
        type: float
        description: Selling price
      - name: cost
        type: float
        description: Cost price
      - name: supplier
        type: str
        description: Supplier name

relationships:
  - from: sales.customer_id
    to: customers.customer_id
    type: many_to_one
  - from: sales.product_id
    to: products.product_id
    type: many_to_one
  - from: orders.customer_id
    to: customers.customer_id
    type: many_to_one
"""

    schema_path = output_dir / "schema.yml"
    schema_path.write_text(schema_content)
    print(f"  ✓ Generated schema.yml")


def generate_readme(output_dir: Path):
    """Generate README.md for the data directory."""
    readme_content = """# Sample Data for Seeknal Documentation

This directory contains unified sample datasets used across all Seeknal documentation tutorials and examples.

## Datasets

| File | Rows | Description |
|------|------|-------------|
| [`sales.csv`](sales.csv) | 10,000 | E-commerce sales transactions |
| [`orders.csv`](orders.csv) | 5,000 | Order-level aggregations |
| [`customers.csv`](customers.csv) | 1,000 | Customer master data |
| [`products.csv`](products.csv) | 100 | Product catalog |
| [`schema.yml`](schema.yml) | - | Schema definitions |

## Usage

### Quick Start

```bash
# Use in your Seeknal project
seeknal init --name my_project
cp examples/data/sales.csv my_project/data/
```

### YAML Pipelines

```yaml
name: raw_sales
kind: source
source: csv
table: examples/data/sales.csv
description: Raw sales transactions
```

### Python Pipelines

```python
import pandas as pd

df = pd.read_csv("examples/data/sales.csv")
```

### Feature Store

```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB

fg = FeatureGroupDuckDB(
    name="customer_features",
    entity=customer_entity,
    project="retail_analytics"
)

fg.set_dataframe(pd.read_csv("examples/data/sales.csv"))
```

## Data Relationships

```
customers (1,000)
    ├──► sales (10,000)
    │       └──► products (100)
    └──► orders (5,000)
```

## Data Quality

- All foreign keys validated
- No null values in key fields
- Realistic distributions (Pareto pricing, exponential signup dates)
- Date range: 2024-01-01 to 2024-12-31

## Regeneration

To regenerate the datasets:

```bash
python scripts/generate_sample_data.py
```

## License

This sample data is generated for documentation purposes and may be used freely.
"""

    readme_path = output_dir / "README.md"
    readme_path.write_text(readme_content)
    print(f"  ✓ Generated README.md")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate unified sample datasets for Seeknal documentation"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("examples/data"),
        help="Output directory for generated files"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Seeknal Sample Data Generator")
    print("=" * 70)
    print()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Generate datasets
    products = generate_products(NUM_PRODUCTS)
    customers = generate_customers(NUM_CUSTOMERS)
    sales = generate_sales(NUM_SALES, products, customers)
    orders = generate_orders(sales, NUM_ORDERS)

    # Validate
    if not validate_data(products, customers, sales, orders):
        print("\n✗ Data validation failed. Please check the errors above.")
        exit(1)

    # Save to files
    print("\nSaving datasets...")
    products_path = args.output_dir / "products.csv"
    customers_path = args.output_dir / "customers.csv"
    sales_path = args.output_dir / "sales.csv"
    orders_path = args.output_dir / "orders.csv"

    products.to_csv(products_path, index=False)
    customers_path = args.output_dir / "customers.csv"
    customers.to_csv(customers_path, index=False)
    sales.to_csv(sales_path, index=False)
    orders.to_csv(orders_path, index=False)

    print(f"  ✓ Saved products.csv ({products_path.stat().st_size / 1024:.1f} KB)")
    print(f"  ✓ Saved customers.csv ({customers_path.stat().st_size / 1024:.1f} KB)")
    print(f"  ✓ Saved sales.csv ({sales_path.stat().st_size / 1024:.1f} KB)")
    print(f"  ✓ Saved orders.csv ({orders_path.stat().st_size / 1024:.1f} KB)")

    # Generate documentation
    print("\nGenerating documentation...")
    generate_schema_yml(args.output_dir)
    generate_readme(args.output_dir)

    # Summary
    print()
    print("=" * 70)
    print("✓ Sample data generation complete!")
    print()
    print("Generated files:")
    print(f"  - {args.output_dir}/sales.csv ({len(sales):,} rows)")
    print(f"  - {args.output_dir}/orders.csv ({len(orders):,} rows)")
    print(f"  - {args.output_dir}/customers.csv ({len(customers):,} rows)")
    print(f"  - {args.output_dir}/products.csv ({len(products):,} rows)")
    print(f"  - {args.output_dir}/schema.yml")
    print(f"  - {args.output_dir}/README.md")
    print()
    print("Total size: {:.1f} MB".format(
        sum(f.stat().st_size for f in args.output_dir.glob("*.csv")) / (1024 * 1024)
    ))
    print("=" * 70)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Create sample datasets and ingest them into PostgreSQL.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# PostgreSQL connection
DB_URL = "postgresql://circuit:circuit123@localhost:5432/circuit"

def create_sample_users(n: int = 100) -> pd.DataFrame:
    """Create sample user data."""
    np.random.seed(42)

    users = pd.DataFrame({
        "user_id": range(1, n + 1),
        "username": [f"user_{i}" for i in range(1, n + 1)],
        "email": [f"user_{i}@example.com" for i in range(1, n + 1)],
        "age": np.random.randint(18, 65, n),
        "country": np.random.choice(["US", "UK", "DE", "FR", "JP", "ID", "SG"], n),
        "signup_date": [
            datetime(2023, 1, 1) + timedelta(days=np.random.randint(0, 365))
            for _ in range(n)
        ],
        "is_premium": np.random.choice([True, False], n, p=[0.3, 0.7]),
    })
    return users


def create_sample_transactions(n: int = 500, user_count: int = 100) -> pd.DataFrame:
    """Create sample transaction data."""
    np.random.seed(43)

    transactions = pd.DataFrame({
        "transaction_id": range(1, n + 1),
        "user_id": np.random.randint(1, user_count + 1, n),
        "amount": np.round(np.random.exponential(50, n), 2),
        "currency": np.random.choice(["USD", "EUR", "GBP", "JPY", "IDR"], n),
        "category": np.random.choice(
            ["Electronics", "Clothing", "Food", "Travel", "Entertainment", "Health"],
            n
        ),
        "transaction_date": [
            datetime(2024, 1, 1) + timedelta(days=np.random.randint(0, 365))
            for _ in range(n)
        ],
        "status": np.random.choice(["completed", "pending", "failed"], n, p=[0.85, 0.10, 0.05]),
    })
    return transactions


def create_sample_products(n: int = 50) -> pd.DataFrame:
    """Create sample product data."""
    np.random.seed(44)

    categories = ["Electronics", "Clothing", "Food", "Travel", "Entertainment", "Health"]
    products = pd.DataFrame({
        "product_id": range(1, n + 1),
        "product_name": [f"Product {i}" for i in range(1, n + 1)],
        "category": np.random.choice(categories, n),
        "price": np.round(np.random.uniform(10, 500, n), 2),
        "stock_quantity": np.random.randint(0, 1000, n),
        "rating": np.round(np.random.uniform(1, 5, n), 1),
        "created_at": [
            datetime(2022, 1, 1) + timedelta(days=np.random.randint(0, 730))
            for _ in range(n)
        ],
    })
    return products


def main():
    print("Creating sample datasets...")

    # Create datasets
    users = create_sample_users(100)
    transactions = create_sample_transactions(500, 100)
    products = create_sample_products(50)

    print(f"  - users: {len(users)} rows")
    print(f"  - transactions: {len(transactions)} rows")
    print(f"  - products: {len(products)} rows")

    # Connect to PostgreSQL
    print(f"\nConnecting to PostgreSQL...")
    engine = create_engine(DB_URL)

    # Ingest data
    print("Ingesting data into PostgreSQL...")

    users.to_sql("sample_users", engine, if_exists="replace", index=False)
    print("  - sample_users table created")

    transactions.to_sql("sample_transactions", engine, if_exists="replace", index=False)
    print("  - sample_transactions table created")

    products.to_sql("sample_products", engine, if_exists="replace", index=False)
    print("  - sample_products table created")

    # Verify
    print("\nVerifying data ingestion...")
    with engine.connect() as conn:
        for table in ["sample_users", "sample_transactions", "sample_products"]:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"  - {table}: {count} rows")

    print("\nDone! Sample data has been ingested into PostgreSQL.")
    print("\nConnection string for testing:")
    print(f"  postgresql://circuit:circuit123@localhost:5432/circuit")
    print("\nTables created:")
    print("  - sample_users")
    print("  - sample_transactions")
    print("  - sample_products")


if __name__ == "__main__":
    main()

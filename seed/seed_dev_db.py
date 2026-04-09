#!/usr/bin/env python3
"""Seed seeknal dummy Postgres with products and orders tables.

Usage:
    pip install psycopg[binary]
    python seed_dev_db.py

    # Or with custom connection:
    DB_HOST=seeknal-postgres DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=seeknal_dev python seed_dev_db.py
"""

import os
import sys

import psycopg


def get_dsn():
    return (
        f"postgresql://{os.environ.get('DB_USER', 'postgres')}:"
        f"{os.environ.get('DB_PASSWORD', 'postgres')}@"
        f"{os.environ.get('DB_HOST', '127.0.0.1')}:"
        f"{os.environ.get('DB_PORT', '5432')}/"
        f"{os.environ.get('DB_NAME', 'seeknal_dev')}"
    )


SEED_SQL = """
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(10, 2),
    stock INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    total_price NUMERIC(12, 2),
    status VARCHAR(50) DEFAULT 'pending',
    order_date DATE DEFAULT CURRENT_DATE,
    customer_email VARCHAR(255)
);

-- Clean slate (idempotent)
TRUNCATE products, orders CASCADE;

INSERT INTO products (name, category, price, stock) VALUES
    ('Laptop Pro 15', 'Electronics', 1499.99, 50),
    ('Wireless Mouse', 'Electronics', 29.99, 200),
    ('Office Chair', 'Furniture', 349.00, 30),
    ('Standing Desk', 'Furniture', 599.00, 15),
    ('USB-C Hub', 'Electronics', 49.99, 100),
    ('Monitor 27 inch', 'Electronics', 399.99, 25),
    ('Keyboard Mechanical', 'Electronics', 129.99, 75),
    ('Desk Lamp', 'Furniture', 59.99, 60),
    ('Webcam HD', 'Electronics', 79.99, 45),
    ('Headset Pro', 'Electronics', 199.99, 35);

INSERT INTO orders (product_id, quantity, total_price, status, customer_email) VALUES
    (1, 2, 2999.98, 'completed', 'alice@example.com'),
    (2, 5, 149.95, 'completed', 'bob@example.com'),
    (3, 1, 349.00, 'pending', 'alice@example.com'),
    (5, 3, 149.97, 'completed', 'carol@example.com'),
    (6, 1, 399.99, 'shipped', 'bob@example.com'),
    (4, 2, 1198.00, 'pending', 'dave@example.com'),
    (7, 1, 129.99, 'completed', 'carol@example.com'),
    (10, 2, 399.98, 'shipped', 'alice@example.com'),
    (8, 3, 179.97, 'completed', 'eve@example.com'),
    (9, 1, 79.99, 'pending', 'dave@example.com');
"""


def main():
    dsn = get_dsn()
    print(f"Connecting to: {dsn.split('@')[1] if '@' in dsn else dsn}")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SEED_SQL)
        conn.commit()

        # Verify
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products")
            products = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders")
            orders = cur.fetchone()[0]
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
            tables = [r[0] for r in cur.fetchall()]

    print(f"Tables: {tables}")
    print(f"Seeded: {products} products, {orders} orders")
    print("Done!")


if __name__ == "__main__":
    main()

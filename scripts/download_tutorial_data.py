#!/usr/bin/env python3
"""
Helper script to download sample online retail dataset for Python pipelines tutorial.

Usage:
    python scripts/download_tutorial_data.py
"""

import os
import sys
from pathlib import Path
import urllib.request
import random
from datetime import datetime, timedelta

try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas")
    sys.exit(1)


def generate_synthetic_dataset(output_path: Path) -> None:
    """Generate a synthetic retail dataset for testing."""
    print("Generating synthetic retail dataset...")

    # Configuration - creating realistic e-commerce data
    num_customers = 500
    num_products = 500
    num_transactions = 10000

    customers = [f"C{i:05d}" for i in range(1, num_customers + 1)]
    products = [f"P{i:05d}" for i in range(1, num_products + 1)]
    countries = ["United Kingdom", "Germany", "France", "Spain", "Netherlands", "Belgium", "USA"]

    # Product descriptions
    descriptions = [
        "WHITE HANGING HEART T-LIGHT HOLDER",
        "REGENCY CAKESTAND 3 TIER",
        "JUMBO BAG RED RETROSPOT",
        "PARTY BUNTING",
        "LUNCH BAG RED RETROSPOT",
        "ASSORTED COLOUR BIRD ORNAMENT",
        "SET OF 3 CAKE TINS PANTRY DESIGN",
        "NATURAL SLATE HEART CHALKBOARD",
        "BOX OF 6 ASSORTED COLOUR TEASPOONS",
        "CHARLOTTE BAG DOLLY GIRL DESIGN",
        "VINTAGE BILLBOARD DRINK ME",
        "PACK OF 72 RETROSPOT CAKE CASES",
        "PACK OF 60 LUNCH BAG RED RETROSPOT",
        "GLASS STAR FROSTED T-LIGHT HOLDER",
        "HAND WARMER UNION JACK",
        "HAND WARMER RED RETROSPOT",
        "ASSORTED COLOURS SILK FAN",
        "WHITE BEADED HEART STRING OF 10",
        "WOODEN FRAME ANTIQUE WHITE",
        "GLASS STOKE TEA TUMBLERS",
        "FOURTY SIX PIECE CUTLERY SET",
    ]

    # Generate transactions
    data = []
    base_date = datetime(2011, 12, 1)

    for i in range(1, num_transactions + 1):
        invoice_no = f"{random.randint(480000, 550000)}"
        stock_code = random.choice(products)
        quantity = random.randint(1, 20)
        unit_price = round(random.uniform(1.0, 50.0), 2)
        customer_id = random.choice(customers)
        country = random.choice(countries)
        description = random.choice(descriptions)

        # Random date within 3 months
        days_offset = random.randint(0, 90)
        invoice_date = base_date - timedelta(days=days_offset)
        invoice_date_str = invoice_date.strftime("%Y-%m-%d %H:%M:%S")

        data.append({
            "InvoiceNo": invoice_no,
            "StockCode": stock_code,
            "Description": description,
            "Quantity": quantity,
            "InvoiceDate": invoice_date_str,
            "UnitPrice": unit_price,
            "CustomerID": customer_id,
            "Country": country
        })

    df = pd.DataFrame(data)

    # Sort by date
    df = df.sort_values("InvoiceDate").reset_index(drop=True)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save
    df.to_csv(output_path, index=False)

    print(f"\n✓ Generated synthetic dataset: {output_path}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)}")

    # Print sample data
    print("\nSample data (first 5 rows):")
    print(df.head().to_string(index=False))

    # Print statistics
    print(f"\nDataset statistics:")
    print(f"  Unique customers: {df['CustomerID'].nunique():,}")
    print(f"  Unique products: {df['StockCode'].nunique():,}")
    print(f"  Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")
    print(f"  Countries: {df['Country'].nunique()}")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")


def main():
    """Main entry point."""
    print("=" * 60)
    print("Seeknal Python Pipelines Tutorial - Data Download")
    print("=" * 60)
    print()

    # Use current directory if not in a seeknal project
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    output_path = data_dir / "online_retail_sample.csv"

    generate_synthetic_dataset(output_path)

    print()
    print("=" * 60)
    print("✓ Dataset ready! Next steps:")
    print()
    print("  1. Initialize project:")
    print("     seeknal init --name retail_analytics")
    print()
    print("  2. Follow the tutorial:")
    print("     docs/tutorials/python-pipelines-tutorial.md")
    print()
    print("  3. Or copy pipeline files directly:")
    print("     cp docs/tutorials/examples/*.py seeknal/pipelines/")
    print("=" * 60)


if __name__ == "__main__":
    main()

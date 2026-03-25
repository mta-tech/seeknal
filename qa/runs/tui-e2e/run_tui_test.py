#!/usr/bin/env python3
"""TUI E2E Test — validates seeknal ask CLI builds a real data pipeline.

This test runs the ACTUAL `seeknal ask` CLI via subprocess (not mocked),
validates real file outputs, and checks data quality in parquet files.

It tests what users experience in the terminal, not what unit tests prove.

Usage:
    uv run python qa/runs/tui-e2e/run_tui_test.py

Requirements:
    - GOOGLE_API_KEY in .env or environment
    - seeknal installed (uv run seeknal)
    - Star schema CSV files in qa/runs/tui-e2e/data/
"""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()
SEEKNAL_ROOT = PROJECT_DIR.parent.parent.parent

# Test configuration
TIMEOUT = 900  # 15 minutes max for full pipeline build
MIN_EXPECTED_OUTPUTS = 8  # At least 8 parquet files expected

# ============================================================
# Test Infrastructure
# ============================================================

results = []


def record(name: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    results.append({"name": name, "status": status, "detail": detail})
    marker = f"  [{status}]"
    print(f"{marker} {name}" + (f" — {detail}" if detail else ""))


def setup_project():
    """Create clean project with star schema data."""
    # Clean previous run
    for d in ["seeknal/sources", "seeknal/transforms", "seeknal/models",
              "seeknal/feature_groups", "target", "__pycache__"]:
        p = PROJECT_DIR / d
        if p.exists():
            shutil.rmtree(p)

    # Remove drafts
    for f in PROJECT_DIR.glob("draft_*"):
        f.unlink()

    # Ensure directories exist
    (PROJECT_DIR / "seeknal" / "sources").mkdir(parents=True, exist_ok=True)
    (PROJECT_DIR / "seeknal" / "transforms").mkdir(parents=True, exist_ok=True)
    (PROJECT_DIR / "data").mkdir(parents=True, exist_ok=True)

    # Write project file
    (PROJECT_DIR / "seeknal_project.yml").write_text("name: tui-e2e-test\n")

    # Copy .env from parent if exists
    env_source = SEEKNAL_ROOT / ".env"
    env_dest = PROJECT_DIR / ".env"
    if env_source.exists() and not env_dest.exists():
        shutil.copy(env_source, env_dest)

    print(f"Project: {PROJECT_DIR}")


def generate_test_data():
    """Generate star schema CSV data if not present."""
    import duckdb

    data_dir = PROJECT_DIR / "data"
    if (data_dir / "customers.csv").exists():
        print("Test data already exists, skipping generation.")
        return

    print("Generating star schema test data...")
    con = duckdb.connect(":memory:")

    # Customers
    con.execute("""
        COPY (
            SELECT
                'C' || LPAD(CAST(i AS VARCHAR), 3, '0') AS customer_id,
                'Customer_' || i AS name,
                CASE WHEN i % 3 = 0 THEN 'Premium'
                     WHEN i % 3 = 1 THEN 'Standard'
                     ELSE 'Basic' END AS segment,
                CASE WHEN i % 4 = 0 THEN 'Jakarta'
                     WHEN i % 4 = 1 THEN 'Surabaya'
                     WHEN i % 4 = 2 THEN 'Bandung'
                     ELSE 'Medan' END AS city,
                CASE WHEN i % 3 = 0 THEN 'organic'
                     WHEN i % 3 = 1 THEN 'paid_search'
                     ELSE 'social' END AS acquisition_channel,
                DATE '2023-01-01' + INTERVAL (i * 2) DAY AS join_date
            FROM generate_series(1, 100) t(i)
        ) TO '""" + str(data_dir / "customers.csv") + """' (HEADER, DELIMITER ',')
    """)

    # Products
    con.execute("""
        COPY (
            SELECT
                'P' || LPAD(CAST(i AS VARCHAR), 2, '0') AS product_id,
                CASE WHEN i <= 5 THEN 'Electronics'
                     WHEN i <= 10 THEN 'Fashion'
                     ELSE 'Food' END AS category,
                'Product_' || i AS product_name,
                (10000 + i * 5000) AS unit_price,
                (5000 + i * 2000) AS unit_cost
            FROM generate_series(1, 15) t(i)
        ) TO '""" + str(data_dir / "products.csv") + """' (HEADER, DELIMITER ',')
    """)

    # Orders (fact table)
    con.execute("""
        COPY (
            SELECT
                'ORD' || LPAD(CAST(i AS VARCHAR), 4, '0') AS order_id,
                DATE '2024-01-01' + INTERVAL (i % 365) DAY AS order_date,
                'C' || LPAD(CAST((i % 100) + 1 AS VARCHAR), 3, '0') AS customer_id,
                'P' || LPAD(CAST((i % 15) + 1 AS VARCHAR), 2, '0') AS product_id,
                (i % 4) + 1 AS quantity,
                CASE WHEN i % 5 = 0 THEN 'cancelled' ELSE 'completed' END AS status,
                CASE WHEN i % 3 = 0 THEN 10 ELSE 0 END AS discount_pct
            FROM generate_series(1, 500) t(i)
        ) TO '""" + str(data_dir / "orders.csv") + """' (HEADER, DELIMITER ',')
    """)

    # Events (behavioral)
    con.execute("""
        COPY (
            SELECT
                'EVT' || LPAD(CAST(i AS VARCHAR), 5, '0') AS event_id,
                DATE '2024-01-01' + INTERVAL (i % 365) DAY AS event_date,
                'C' || LPAD(CAST((i % 100) + 1 AS VARCHAR), 3, '0') AS customer_id,
                CASE WHEN i % 4 = 0 THEN 'purchase'
                     WHEN i % 4 = 1 THEN 'page_view'
                     WHEN i % 4 = 2 THEN 'add_to_cart'
                     ELSE 'search' END AS event_type,
                (i % 600) + 30 AS session_duration_sec
            FROM generate_series(1, 1000) t(i)
        ) TO '""" + str(data_dir / "events.csv") + """' (HEADER, DELIMITER ',')
    """)

    con.close()

    for f in data_dir.glob("*.csv"):
        import csv
        with open(f) as fh:
            rows = sum(1 for _ in csv.reader(fh)) - 1
        print(f"  {f.name}: {rows} rows")


# ============================================================
# Phase 1: Run the actual CLI
# ============================================================

def test_cli_execution():
    """Run `seeknal ask` via subprocess — the REAL TUI test."""
    name = "cli_execution"

    prompt = (
        "Build an E2E data pipeline from raw CSV files in data/. "
        "Bronze: all CSV files as sources. "
        "Silver: enriched_orders (orders JOIN customers JOIN products with revenue calculation), "
        "customer_360 (per-customer aggregations from orders + events). "
        "Gold: revenue_by_segment, product_performance. "
        "ML: customer_features transform (RFM from customer_360), "
        "then Python KMeans model for customer segmentation. "
        "Profile first, build all nodes, run with full=True, "
        "then inspect_output on enriched_orders and revenue_by_segment."
    )

    # Load env
    env = os.environ.copy()
    env_file = PROJECT_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()

    cmd = [
        "uv", "run", "seeknal", "ask",
        "--project", str(PROJECT_DIR),
        prompt,
    ]

    print(f"\n{'='*60}")
    print(f"Running: seeknal ask (timeout={TIMEOUT}s)")
    print(f"Prompt: {prompt[:100]}...")
    print(f"{'='*60}\n")

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
            cwd=str(SEEKNAL_ROOT),
            env=env,
        )
        elapsed = time.time() - start

        # Save output for debugging
        (PROJECT_DIR / "cli_stdout.txt").write_text(result.stdout)
        (PROJECT_DIR / "cli_stderr.txt").write_text(result.stderr)

        if result.returncode == 0:
            record(name, True, f"completed in {elapsed:.0f}s")
        else:
            record(name, False, f"exit code {result.returncode}, stderr: {result.stderr[:200]}")

        return result.stdout + result.stderr

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        record(name, False, f"timed out after {elapsed:.0f}s")
        return ""
    except Exception as e:
        record(name, False, f"error: {e}")
        return ""


# ============================================================
# Phase 2: Validate file outputs
# ============================================================

def test_sources_created():
    """Check that source YAML files were created in seeknal/sources/."""
    sources_dir = PROJECT_DIR / "seeknal" / "sources"
    source_files = list(sources_dir.glob("*.yml")) + list(sources_dir.glob("*.py"))

    if len(source_files) >= 3:
        names = [f.stem for f in source_files]
        record("sources_created", True, f"{len(source_files)} sources: {', '.join(names)}")
    else:
        record("sources_created", False, f"expected ≥3 sources, got {len(source_files)}")


def test_transforms_created():
    """Check that transform files were created."""
    transforms_dir = PROJECT_DIR / "seeknal" / "transforms"
    transform_files = list(transforms_dir.glob("*.yml")) + list(transforms_dir.glob("*.py"))

    if len(transform_files) >= 4:
        names = [f.stem for f in transform_files]
        record("transforms_created", True, f"{len(transform_files)} transforms: {', '.join(names[:6])}")
    else:
        record("transforms_created", False, f"expected ≥4 transforms, got {len(transform_files)}")


def test_no_drafts_remaining():
    """Check that no draft files remain in project root."""
    drafts = [f for f in PROJECT_DIR.glob("draft_*") if not f.name.endswith(".pyc")]
    drafts = [f for f in drafts if "__pycache__" not in str(f)]

    if len(drafts) == 0:
        record("no_drafts", True)
    else:
        names = [f.name for f in drafts]
        record("no_drafts", False, f"{len(drafts)} drafts remaining: {', '.join(names[:5])}")


def test_pipeline_outputs():
    """Check that pipeline produced parquet files."""
    intermediate = PROJECT_DIR / "target" / "intermediate"
    if not intermediate.exists():
        record("pipeline_outputs", False, "target/intermediate/ not found — pipeline didn't run")
        return

    parquets = sorted(intermediate.glob("*.parquet"))
    if len(parquets) >= MIN_EXPECTED_OUTPUTS:
        names = [f.stem for f in parquets]
        record("pipeline_outputs", True, f"{len(parquets)} outputs: {', '.join(names[:8])}")
    else:
        record("pipeline_outputs", False,
               f"expected ≥{MIN_EXPECTED_OUTPUTS} outputs, got {len(parquets)}")


# ============================================================
# Phase 3: Validate data quality
# ============================================================

def test_enriched_orders_quality():
    """Validate enriched_orders has joined columns and correct row count."""
    parquet = PROJECT_DIR / "target" / "intermediate" / "transform_enriched_orders.parquet"
    if not parquet.exists():
        # Try alternate naming
        candidates = list((PROJECT_DIR / "target" / "intermediate").glob("*enriched*"))
        if candidates:
            parquet = candidates[0]
        else:
            record("enriched_orders_quality", False, "parquet not found")
            return

    import duckdb
    con = duckdb.connect(":memory:")
    try:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet}')").fetchone()[0]
        cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet}')").fetchall()]

        checks = []
        if count >= 400:  # 500 orders, ~80% completed
            checks.append(f"rows={count}")
        else:
            record("enriched_orders_quality", False, f"expected ≥400 rows, got {count}")
            return

        # Check for joined columns
        expected_cols = {"order_id", "customer_id", "product_id"}
        found = expected_cols & set(cols)
        if len(found) >= 2:
            checks.append(f"cols={len(cols)}")
        else:
            record("enriched_orders_quality", False, f"missing join columns, got: {cols[:10]}")
            return

        record("enriched_orders_quality", True, ", ".join(checks))
    except Exception as e:
        record("enriched_orders_quality", False, str(e))
    finally:
        con.close()


def test_customer_360_quality():
    """Validate customer_360 has per-customer aggregations."""
    parquet = PROJECT_DIR / "target" / "intermediate" / "transform_customer_360.parquet"
    if not parquet.exists():
        candidates = list((PROJECT_DIR / "target" / "intermediate").glob("*customer_360*"))
        if candidates:
            parquet = candidates[0]
        else:
            record("customer_360_quality", False, "parquet not found")
            return

    import duckdb
    con = duckdb.connect(":memory:")
    try:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet}')").fetchone()[0]
        cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet}')").fetchall()]

        if count >= 50:  # Should have ~100 unique customers
            record("customer_360_quality", True, f"rows={count}, cols={len(cols)}")
        else:
            record("customer_360_quality", False, f"expected ≥50 rows, got {count}")
    except Exception as e:
        record("customer_360_quality", False, str(e))
    finally:
        con.close()


def test_gold_metrics():
    """Validate gold layer outputs exist and have reasonable data."""
    import duckdb
    con = duckdb.connect(":memory:")
    intermediate = PROJECT_DIR / "target" / "intermediate"

    gold_checks = {
        "revenue_by_segment": {"min_rows": 2, "pattern": "*revenue*segment*"},
        "product_performance": {"min_rows": 5, "pattern": "*product*performance*"},
    }

    for name, spec in gold_checks.items():
        candidates = list(intermediate.glob(spec["pattern"] + ".parquet"))
        if not candidates:
            # Try exact name
            exact = intermediate / f"transform_{name}.parquet"
            if exact.exists():
                candidates = [exact]

        if not candidates:
            record(f"gold_{name}", False, "parquet not found")
            continue

        try:
            parquet = candidates[0]
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet}')").fetchone()[0]
            if count >= spec["min_rows"]:
                record(f"gold_{name}", True, f"rows={count}")
            else:
                record(f"gold_{name}", False, f"expected ≥{spec['min_rows']} rows, got {count}")
        except Exception as e:
            record(f"gold_{name}", False, str(e))

    con.close()


def test_ml_output():
    """Validate ML model output exists and has cluster/segment columns."""
    intermediate = PROJECT_DIR / "target" / "intermediate"

    # Search for ML-specific outputs first (model/segmentation/kmeans),
    # then fall back to broader patterns. Exclude gold-layer files
    # like revenue_by_segment which are NOT ML outputs.
    gold_names = {"transform_revenue_by_segment", "transform_product_performance"}
    candidates = []
    for pattern in ["*segmentation*", "*kmeans*", "*model*", "*cluster*"]:
        for f in intermediate.glob(pattern + ".parquet"):
            if f.stem not in gold_names and f not in candidates:
                candidates.append(f)

    # Broader fallback: any parquet with cluster/segment columns
    if not candidates:
        import duckdb
        con = duckdb.connect(":memory:")
        for f in intermediate.glob("*.parquet"):
            if f.stem in gold_names:
                continue
            try:
                cols = [c[0] for c in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()]
                if any("cluster" in c.lower() or "segment_label" in c.lower() for c in cols):
                    candidates.append(f)
            except Exception:
                pass
        con.close()

    if not candidates:
        record("ml_output", False, "no ML output parquet found")
        return

    import duckdb
    con = duckdb.connect(":memory:")
    try:
        parquet = candidates[0]
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet}')").fetchone()[0]
        cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet}')").fetchall()]

        # Check for cluster or segment column
        ml_cols = [c for c in cols if "cluster" in c.lower() or "segment" in c.lower()]
        if count >= 50 and ml_cols:
            record("ml_output", True, f"rows={count}, ml_cols={ml_cols}, file={parquet.name}")
        elif count >= 50:
            record("ml_output", False, f"rows={count} but no cluster/segment column. cols={cols}")
        else:
            record("ml_output", False, f"expected ≥50 rows, got {count}. file={parquet.name}")
    except Exception as e:
        record("ml_output", False, str(e))
    finally:
        con.close()


def test_cli_output_has_data():
    """Check that CLI stdout contains actual data tables (not just 'conceptual')."""
    stdout_file = PROJECT_DIR / "cli_stdout.txt"
    if not stdout_file.exists():
        record("cli_shows_data", False, "no stdout captured")
        return

    output = stdout_file.read_text()
    # Check for inspect_output table markers
    has_table = "|" in output and "---" in output
    has_conceptual = "conceptual" in output.lower() or "the output will contain" in output.lower()

    if has_table and not has_conceptual:
        record("cli_shows_data", True, "real data tables in output")
    elif has_table:
        record("cli_shows_data", True, "has data tables (also has conceptual text)")
    else:
        record("cli_shows_data", False, "no data tables found in CLI output")


# ============================================================
# Main
# ============================================================

def main():
    print(f"{'='*60}")
    print(f"  TUI E2E Test — seeknal ask CLI Pipeline Builder")
    print(f"{'='*60}\n")

    start = time.time()

    # Setup
    setup_project()
    generate_test_data()

    # Phase 1: Run CLI
    cli_output = test_cli_execution()

    # Phase 2: Validate files
    test_sources_created()
    test_transforms_created()
    test_no_drafts_remaining()
    test_pipeline_outputs()

    # Phase 3: Validate data quality
    test_enriched_orders_quality()
    test_customer_360_quality()
    test_gold_metrics()
    test_ml_output()
    test_cli_output_has_data()

    # Summary
    elapsed = time.time() - start
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    total = len(results)

    print(f"\n{'='*60}")
    print(f"  Results: {passed}/{total} passed, {failed} failed ({elapsed:.0f}s)")
    print(f"{'='*60}\n")

    if failed > 0:
        print("Failed tests:")
        for r in results:
            if r["status"] == "FAIL":
                print(f"  - {r['name']}: {r['detail']}")

    overall = "PASS" if failed == 0 else "FAIL"
    print(f"\nOverall: {overall}")

    # Write results JSON for CI
    (PROJECT_DIR / "results.json").write_text(
        json.dumps({"overall": overall, "passed": passed, "failed": failed,
                     "total": total, "elapsed_s": round(elapsed),
                     "results": results}, indent=2)
    )

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

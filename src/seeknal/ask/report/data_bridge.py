"""DuckDB data bridge for Evidence reports.

Creates a .duckdb file with all seeknal parquets materialized as tables.
Evidence's DuckDB connector (Node.js) reads this file directly, giving
reports access to the same table names the agent already knows.

Tables are materialized (not views) because Evidence uses a Node.js DuckDB
binding that cannot resolve ``read_parquet()`` views created by Python DuckDB.
"""

from pathlib import Path


def create_duckdb_from_parquets(
    project_path: Path, db_path: Path
) -> tuple[int, list[str]]:
    """Create a DuckDB file with tables from project parquets.

    Scans target/intermediate/, target/cache/, and target/feature_store/
    for parquet files and materializes each as a named table.

    Args:
        project_path: Path to the seeknal project root.
        db_path: Where to write the .duckdb file.

    Returns:
        Tuple of (number of tables registered, list of table names).
    """
    import duckdb

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing file to start fresh
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    conn.execute("SET memory_limit='512MB'")
    conn.execute("SET threads=2")

    registered = 0
    seen_names: set[str] = set()

    root = project_path.resolve()

    # Register intermediate parquets (transform_*, source_*, etc.)
    intermediate = root / "target" / "intermediate"
    if intermediate.exists():
        for pq in sorted(intermediate.rglob("*.parquet")):
            table_name = pq.stem
            if table_name in seen_names:
                continue
            safe_path = str(pq.resolve()).replace("'", "''")
            try:
                conn.execute(
                    f'CREATE TABLE "{table_name}" AS '
                    f"SELECT * FROM read_parquet('{safe_path}')"
                )
                seen_names.add(table_name)
                registered += 1
            except Exception:
                pass

    # Register legacy cache parquets
    cache = root / "target" / "cache"
    if cache.exists():
        for pq in sorted(cache.rglob("*.parquet")):
            if pq.stem in seen_names:
                continue
            safe_path = str(pq.resolve()).replace("'", "''")
            try:
                conn.execute(
                    f'CREATE TABLE "{pq.stem}" AS '
                    f"SELECT * FROM read_parquet('{safe_path}')"
                )
                seen_names.add(pq.stem)
                registered += 1
            except Exception:
                pass

    # Register consolidated entity parquets
    feature_store = root / "target" / "feature_store"
    if feature_store.exists():
        for entity_dir in sorted(feature_store.iterdir()):
            if not entity_dir.is_dir():
                continue
            features_pq = entity_dir / "features.parquet"
            if features_pq.exists():
                table_name = f"entity_{entity_dir.name}"
                if table_name in seen_names:
                    continue
                safe_path = str(features_pq.resolve()).replace("'", "''")
                try:
                    conn.execute(
                        f'CREATE TABLE "{table_name}" AS '
                        f"SELECT * FROM read_parquet('{safe_path}')"
                    )
                    seen_names.add(table_name)
                    registered += 1
                except Exception:
                    pass

    conn.close()
    return registered, sorted(seen_names)

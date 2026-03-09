"""DuckDB view bridge for Evidence reports.

Creates a .duckdb file with all seeknal parquets registered as named views.
Evidence's DuckDB connector reads this file directly, giving reports access
to the same table names the agent already knows.

This mirrors the pattern in sandbox.py's load_project_data() but runs
in-process (not in a subprocess) and writes to a persistent .duckdb file.
"""

from pathlib import Path


def create_duckdb_from_parquets(project_path: Path, db_path: Path) -> int:
    """Create a DuckDB file with views pointing to project parquets.

    Scans target/intermediate/, target/cache/, and target/feature_store/
    for parquet files and registers each as a named view.

    Args:
        project_path: Path to the seeknal project root.
        db_path: Where to write the .duckdb file.

    Returns:
        Number of views registered.
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
            view_name = pq.stem
            if view_name in seen_names:
                continue
            safe_path = str(pq.resolve()).replace("'", "''")
            try:
                conn.execute(
                    f'CREATE VIEW "{view_name}" AS '
                    f"SELECT * FROM read_parquet('{safe_path}')"
                )
                seen_names.add(view_name)
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
                    f'CREATE VIEW "{pq.stem}" AS '
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
                view_name = f"entity_{entity_dir.name}"
                if view_name in seen_names:
                    continue
                safe_path = str(features_pq.resolve()).replace("'", "''")
                try:
                    conn.execute(
                        f'CREATE VIEW "{view_name}" AS '
                        f"SELECT * FROM read_parquet('{safe_path}')"
                    )
                    seen_names.add(view_name)
                    registered += 1
                except Exception:
                    pass

    conn.close()
    return registered

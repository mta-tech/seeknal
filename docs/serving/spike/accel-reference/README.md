# accel loader reference (read-only input)

Verbatim copy of the existing DuckDB acceleration ("accel") cache loader from the CBN
phase-1b reporting kit (`phase1b-reporting/accel/`, not a git repository, copied 2026-11-19).
Files carry a `.txt` suffix so they are neither imported nor collected by pytest.

It is reference input for porting the loader into `seeknal.serving.accel`. Do not import or
edit these files. Its own tests (and their fixtures, which use customer report names) are not
copied; write new tests with synthetic names.

What it does today:
- `engines.py`: a Postgres source read through DuckDB `ATTACH ... (TYPE postgres, READ_ONLY)`
  using libpq env vars (no secrets in SQL), a DuckDB file target, and a DuckDB version pin check
  (the cache file format must match the Serving image's DuckDB).
- `tables.py` / `placement.py`: table specs from a column registry; PII columns are written as
  typed NULL; placement JSON for Serving.
- `loader.py`: writes `mart-<date>-<uuid>.duckdb`, verifies row counts against the source,
  closes the file, swaps the `mart.duckdb` symlink atomically (temp symlink + `os.replace`),
  writes `freshness.json` after the swap, prunes old files (`keep`), cleans up on failure,
  scrubs secrets from logs and errors, and holds a lock.
- `settings.py`: memory/threads/keep/extension_dir/expected DuckDB version.

"""Report generation module for Seeknal Ask.

Produces interactive HTML reports using Evidence.dev as the rendering engine.
The agent generates Evidence-compatible markdown (SQL + chart components),
scaffolds an Evidence project wired to seeknal's parquet files, and runs
``evidence build`` to produce a static HTML dashboard.
"""

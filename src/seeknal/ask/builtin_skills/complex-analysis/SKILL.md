---
name: complex-analysis
description: "Run multi-step SQL plus Python/statistics/ML analysis while keeping tools thin and evidence grounded"
tags: [complex-analysis, python, machine-learning, statistics]
version: "1.0.0"
---

# Complex Analysis

Use this workflow when the user asks for deeper analysis than a single SQL
aggregation: correlations, clustering, forecasting, simple machine-learning
models, anomaly detection, visualization, or custom scoring.

## Workflow

1. **Scope with SQL first**
   - Use `list_tables`/`describe_table` when schema is unknown.
   - Use `execute_sql` to confirm row counts, columns, and a small preview.
   - Extract the narrowest table/query needed for Python.

2. **Run Python only when it adds value**
   - Load `execute-python-analysis` before `execute_python`.
   - In Python, use the pre-loaded `conn` object. Do not `import duckdb` or create a new connection.
   - Query data with `conn.sql("SELECT ...").df()`.
   - Available library names are pre-loaded when installed: pandas (`pd`),
     numpy (`np`), matplotlib (`plt`), scikit-learn (`sklearn`), and scipy
     (`scipy`). Check optional libraries before using them; if plotting is
     unavailable, return text/table evidence instead of retrying chart code.

3. **Model responsibly**
   - Prefer simple interpretable baselines over complex models.
   - Report features, target, sample size, and validation approach.
   - If there is too little data, say so and use descriptive analysis instead.

4. **Return evidence**
   - Include the SQL/Python results that support the conclusion.
   - Label exploratory findings as exploratory.
   - Mention generated plot paths only if plots were created.

## Safety

Connected source access remains read-only. Do not mutate external databases,
write pipeline files, or publish artifacts unless the user explicitly asks and
the relevant approval-gated skill allows it.

---
adr_id: ADR-005
date: 2026-02-20
status: accepted
title: DuckDB Postgres Extension as Sole Bridge for PostgreSQL I/O
---

# ADR-005: DuckDB Postgres Extension as Sole Bridge for PostgreSQL I/O

## Context

Seeknal needed PostgreSQL source reading (with pushdown queries) and materialization (full/incremental/upsert writes). The existing compute engine is DuckDB. A bridge layer was required to connect DuckDB pipelines to PostgreSQL for both ingestion and writing. Options included introducing psycopg2 or SQLAlchemy as a dedicated PostgreSQL client, or leveraging DuckDB's built-in postgres extension.

## Decision

Use DuckDB's postgres extension exclusively for all PostgreSQL I/O. Reads are handled via `ATTACH` + `SELECT` with `postgres_query()` for pushdown queries. Writes are handled via `CREATE TABLE AS` and `INSERT INTO` through the attached database, using the binary COPY protocol internally.

**Rationale:**
- Zero new dependencies — DuckDB is already the compute engine throughout Seeknal
- Binary COPY protocol provides faster bulk writes compared to row-by-row inserts via psycopg2
- Consistent with the existing Iceberg integration pattern of using DuckDB extensions for external system access
- Pushdown queries via `postgres_query()` reduce data transfer by filtering at the source

**Consequences:**
- [ ] No dependency management overhead for a PostgreSQL client library
- [ ] Single engine handles all I/O, reducing cognitive overhead and integration surface
- [ ] Binary COPY protocol yields better write throughput for bulk materialization
- [ ] Limited to capabilities exposed by DuckDB's postgres extension (type mapping constraints apply)
- [ ] Extension version is tied to the DuckDB release cycle

## Alternatives Considered

- psycopg2: Adds a new runtime dependency, requires explicit connection pooling, and results in row-by-row inserts unless batch APIs are used carefully
- SQLAlchemy: Heavy ORM abstraction — overkill for direct bulk writes; adds dependency and indirection without meaningful benefit given DuckDB already manages the query plan

## Related

- Spec: specs/2026-02-20-lineage-inspect-postgresql-materialization-merged.md
- Tasks: B-1, B-4, B-6

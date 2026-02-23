# Seeknal Documentation Assessment Report

**Date**: 2026-02-09
**Benchmark**: dbt and SQLMesh documentation standards
**Method**: Three developer persona agents independently read all documentation and scored on 7 dimensions

---

## Executive Summary

Seeknal's documentation scores **2.95/5.0 average** across all personas and dimensions. The strongest area is **practical examples (4.0/5)** — tutorials provide copy-pasteable code with expected output. The weakest areas are **reference quality (2.3/5)** and **discoverability (2.7/5)** — no CLI reference page, no rendered API docs, and no navigable documentation homepage.

**Compared to benchmarks:**
- dbt: ~4.0 average (strong quickstarts, weak troubleshooting)
- SQLMesh: ~3.8 average (strong concepts, weak CLI examples)
- **Seeknal: ~3.0 average (strong tutorials, weak reference/navigation)**

The gap is primarily structural, not content. Seeknal has good documentation — it's just hard to find and poorly organized.

---

## Score Comparison Table

| Dimension | Data Engineer (Priya) | Analytics Engineer (Marcus) | ML Engineer (Sofia) | Average |
|-----------|:---:|:---:|:---:|:---:|
| **Discoverability** | 3 | 2 | 3 | **2.7** |
| **Time-to-first-value** | 2 | 3 | 3 | **2.7** |
| **Conceptual clarity** | 3 | 3 | 3 | **3.0** |
| **Practical examples** | 4 | 4 | 4 | **4.0** |
| **Error guidance** | 3 | 3 | 3 | **3.0** |
| **Workflow completeness** | 3 | 3 | 3 | **3.0** |
| **Reference quality** | 3 | 2 | 2 | **2.3** |
| **Average** | **3.0** | **2.9** | **3.0** | **2.95** |

---

## Cross-Persona Themes (Issues Mentioned by 2+ Personas)

### 1. No navigable documentation homepage (All 3 personas)

`docs/index.md` uses MkDocs card syntax that only renders on a built site. Raw markdown is unreadable. No persona could find a clear starting point or navigate between documents.

> *Priya*: "The docs homepage does not surface the right content — links to API Reference and Iceberg, not the YAML tutorial."
> *Marcus*: "I had to search multiple directories and guess file names."
> *Sofia*: "No central documentation landing page that serves as a map."

### 2. No CLI command reference page (All 3 personas)

All 41+ commands are available via `seeknal --help` but there is no single reference page documenting every command with flags and examples.

> *Priya*: "You need to run `--help` on each subcommand individually."
> *Marcus*: "No dedicated CLI reference page documenting every flag."
> *Sofia*: "No CLI flag reference beyond `--help`."

### 3. No rendered/standalone API reference (2 personas: Marcus, Sofia)

The `docs/api/` files contain mkdocstrings directives (`:::seeknal.featurestore...`) that only work when the MkDocs site is built. No fallback parameter tables exist.

> *Marcus*: "The actual reference pages would need to be auto-generated from docstrings."
> *Sofia*: "Without building the documentation site, I cannot see parameter types, default values, or method signatures."

### 4. No glossary or concept hierarchy (2 personas: Priya, Sofia)

Terms like Flow, Task, Feature Group, Entity, Materialization, Node, DAG are used throughout but never centralized.

> *Priya*: "Terms are used throughout the docs but never defined in a single reference location."
> *Sofia*: "No single 'Concepts' page that explains the full mental model."

### 5. Two paradigms (Python API vs YAML workflow) not distinguished (2 personas: Priya, Marcus)

The getting-started guide teaches Python `Flow`/`DuckDBTask`, while the YAML tutorial teaches `seeknal run`. No guidance on when to use which.

> *Priya*: "There is no guidance on which to choose."
> *Marcus*: "Conceptual overload — the documentation simultaneously covers feature stores, ML pipelines, Spark, DuckDB, Iceberg, and YAML workflows."

### 6. Feature validation/testing is undocumented (2 personas: Marcus, Sofia)

`seeknal validate-features` and `seeknal audit` exist in CLI but have zero documentation.

> *Marcus*: "Data testing is table-stakes functionality. dbt's data tests are a core part of its value proposition."
> *Sofia*: "The validation framework exists but has zero documentation."

### 7. Installation friction — no PyPI package (2 personas: Priya, Marcus)

Installation requires downloading a `.whl` from GitHub releases instead of `pip install seeknal`.

> *Priya*: "The single biggest barrier to adoption."
> *Marcus*: "Adds friction... compare to `pip install dbt-duckdb && dbt init`."

---

## Persona-Specific Gaps

### Data Engineer Only
- **No database source connector documentation** — Only CSV/Parquet sources shown; no PostgreSQL/MySQL examples
- **No scheduling/orchestration integration** — No Airflow, Prefect, or cron integration docs
- **`seeknal repl` undocumented** — Command exists in CLI but has no tutorial

### Analytics Engineer Only
- **Semantic layer completely undocumented** — `seeknal query` and `seeknal deploy-metrics` exist in CLI with zero docs
- **`seeknal diff` has no reference page** — A flagship differentiator with no standalone documentation
- **Virtual environments underexplained** — Commands shown but no concept page explaining what environments are

### ML Engineer Only
- **No point-in-time join explanation** — Mentioned in README but never explained with diagrams
- **No online feature serving documentation** — 4 lines of code in README, no dedicated page
- **No training/serving parity guide** — Critical ML workflow gap vs Feast/Tecton

---

## What Works Well (Unanimous Across Personas)

1. **YAML pipeline tutorial** (`docs/tutorials/yaml-pipeline-tutorial.md`) — All 3 personas rated practical examples 4/5. Copy-pasteable code, realistic data, expected output at every step. This is the gold standard for Seeknal docs.

2. **Phase 2 tutorials** — Change categorization (BREAKING/NON_BREAKING/METADATA), parallel execution with topological layers, and environment management are genuinely differentiated features that are well-tutorialized.

3. **CLI design** — Clean, organized help text with useful flags. `seeknal init` scaffolds a complete project. `--show-plan`, `--dry-run`, `--parallel`, `--continue-on-error` cover production needs.

4. **DuckDB documentation** — The DuckDB getting-started guide and README feature store section are comprehensive with benchmarks, migration guide, and complete code examples.

---

## Benchmark Comparison

### vs dbt (target: 4.0)

| Area | dbt | Seeknal | Gap |
|------|:---:|:---:|-----|
| Quickstarts by platform | 9+ | 2 | Need DuckDB, Spark, Postgres, Iceberg quickstarts |
| Best practices hub | 30+ articles | 0 | No best practices section exists |
| CLI reference page | Complete | None | Need single-page CLI reference |
| Data testing docs | Comprehensive | None | `seeknal audit` undocumented |
| Documentation site | Rendered, searchable | Raw markdown | No built site available |
| Installation | `pip install dbt-core` | Download .whl | Need PyPI publication |

### vs SQLMesh (target: 3.8)

| Area | SQLMesh | Seeknal | Gap |
|------|:---:|:---:|-----|
| Guides vs Reference separation | Clear | Mixed | Tutorials serve both roles |
| Glossary | 31 terms | 0 | No glossary exists |
| Concept pages | Plans, Environments, Models | None standalone | Concepts embedded in tutorials |
| Virtual env documentation | Extensive | 1 tutorial | Need concept page |
| Change categorization | Documented | Tutorialized | Need reference page |
| Project structure guide | Dedicated page | Implicit | Need standalone guide |

### Seeknal's Unique Advantages (Not in dbt or SQLMesh)

These should be more prominently documented:
- Built-in feature store with offline/online serving
- Mixed YAML + Python pipelines
- Point-in-time joins (prevent data leakage)
- DuckDB + Spark dual engine with 2-line migration
- Feature group version management
- Parallel execution with topological layers

---

## Prioritized Improvement Roadmap

### Immediate (Week 1-2) — Fix Navigation & Reference

| # | Improvement | Impact | Effort | Personas |
|---|-----------|--------|--------|----------|
| 1 | **Create navigable docs/index.md** with plain-markdown TOC, persona paths, and category links | Discoverability 2.7→4.0 | Low | All 3 |
| 2 | **Create CLI reference page** listing all 41+ commands with flags, types, and examples | Reference 2.3→3.5 | Medium | All 3 |
| 3 | **Create glossary** defining 20-30 key terms with cross-links | Clarity 3.0→3.5 | Low | All 3 |
| 4 | **Add "Python API vs YAML Workflow" decision guide** | Clarity 3.0→4.0 | Low | DE, AE |

### Short-term (Week 3-4) — Fill Critical Content Gaps

| # | Improvement | Impact | Effort | Personas |
|---|-----------|--------|--------|----------|
| 5 | **Document semantic layer** (`seeknal query`, `seeknal deploy-metrics`) | Workflow 3.0→4.0 | Medium | AE |
| 6 | **Document data testing/validation** (`seeknal audit`, `seeknal validate-features`) | Workflow 3.0→4.0 | Medium | AE, ML |
| 7 | **Create `seeknal diff` reference page** with change categorization rules | Reference 2.3→3.5 | Low | AE |
| 8 | **Write point-in-time joins concept page** with visual timeline diagrams | Clarity 3.0→4.5 | Medium | ML |
| 9 | **Create virtual environments concept page** explaining architecture and cost model | Clarity 3.0→4.0 | Medium | DE, AE |

### Medium-term (Month 2) — Polish & Differentiate

| # | Improvement | Impact | Effort | Personas |
|---|-----------|--------|--------|----------|
| 10 | **Build standalone API reference** with parameter tables for top 10 classes | Reference 2.3→4.0 | High | ML |
| 11 | **Create training-to-serving end-to-end guide** | Workflow 3.0→4.5 | Medium | ML |
| 12 | **Add database source connector docs** (PostgreSQL, MySQL examples) | Workflow 3.0→4.0 | Medium | DE |
| 13 | **Create best practices hub** (project structure, YAML conventions, materialization strategy) | Clarity 3.0→4.0 | High | All 3 |
| 14 | **Write comparison page** (Seeknal vs dbt vs SQLMesh vs Feast) | Discoverability 2.7→4.0 | Medium | All 3 |
| 15 | **Publish to PyPI** (`pip install seeknal`) | Time-to-value 2.7→4.5 | Medium | All 3 |

### Projected Score After Improvements

| Dimension | Current | After Immediate | After Short-term | After Medium-term |
|-----------|:---:|:---:|:---:|:---:|
| Discoverability | 2.7 | **4.0** | 4.0 | 4.5 |
| Time-to-first-value | 2.7 | 2.7 | 3.0 | **4.0** |
| Conceptual clarity | 3.0 | **3.5** | **4.0** | 4.5 |
| Practical examples | 4.0 | 4.0 | 4.0 | 4.0 |
| Error guidance | 3.0 | 3.0 | 3.0 | 3.5 |
| Workflow completeness | 3.0 | 3.0 | **4.0** | 4.5 |
| Reference quality | 2.3 | **3.5** | 3.5 | **4.0** |
| **Average** | **2.95** | **3.4** | **3.6** | **4.1** |

The medium-term target of **4.1** would place Seeknal above both dbt (~4.0) and SQLMesh (~3.8) documentation quality.

---

## Methodology Notes

- Each persona agent independently read 8-10 documentation files plus CLI help output
- Scores are subjective assessments from the persona's workflow perspective
- "Unanimous" findings (mentioned by all 3) carry highest confidence
- Benchmark scores for dbt/SQLMesh are estimated based on research agent analysis

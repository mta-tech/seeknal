# Brainstorm: Seeknal Web Documentation with MkDocs

**Date**: 2026-02-09
**Status**: Ready for planning
**Author**: Claude Code session

---

## What We're Building

Convert Seeknal's existing markdown documentation into a fully navigable, deployed web site using MkDocs Material, hosted on GitHub Pages at `mta-tech.github.io/seeknal`.

The documentation content already scores 4.1/5.0 (post-improvement evaluation). The main gaps are structural: the `mkdocs.yml` nav only covers 12 of ~45 public-facing files, `index.md` uses build-only card syntax that's unreadable in raw markdown, and the site has never been built or deployed.

**Goal**: Get a complete, well-organized documentation site live with zero content loss and clear navigation flow.

---

## Why This Approach

**Approach chosen: Incremental — Deploy First, Polish Later**

We considered three options:

| Approach | Time | Risk | Value |
|----------|------|------|-------|
| Nav expansion + deploy | Low | Low | High — site goes live fast |
| Full redesign with custom theme | High | Medium | Medium — delays launch |
| **Incremental (chosen)** | **Low then Medium** | **Low** | **High — fast launch + iterative polish** |

The content is already strong. The bottleneck is navigation and deployment, not content quality. Getting the site live quickly provides immediate value and lets us iterate based on real usage.

**Rejected alternatives:**
- Full redesign: Over-engineering for current stage. No users are asking for custom themes.
- Manual API docs: Auto-generated mkdocstrings already configured; leveraging existing setup is simpler.

---

## Key Decisions

### 1. Deployment: GitHub Pages via GitHub Actions

- Auto-deploy on push to `main` branch
- Standard `mkdocs gh-deploy` workflow
- URL: `mta-tech.github.io/seeknal` (default GitHub Pages, no custom domain for now)

### 2. Navigation: Content-Type Based

Top-level nav organized by content type, not persona:

```
Home
Getting Started
  - Quick Start (DuckDB)
  - Comprehensive Guide
Concepts
  - Glossary
  - Point-in-Time Joins
  - Second-Order Aggregations
  - Virtual Environments
  - Change Categorization
  - Python vs YAML
Guides
  - Python Pipelines
  - Testing & Audits
  - Semantic Layer
  - Training to Serving
  - Tool Comparison
Tutorials
  - YAML Pipeline Tutorial
  - Python Pipelines Tutorial
  - Mixed YAML + Python
  - E-Commerce Walkthrough
  - Role-Specific Tutorials
    - Data Engineer: Environments
    - ML Engineer: Parallel Execution
    - Analytics Engineer: Metrics
Reference
  - CLI Commands
  - YAML Schema
  - Configuration
  - Python API (auto-generated)
```

### 3. Scope: Public Docs Only

**Included** (~45 files):
- `concepts/` — 6 concept pages
- `guides/` — 5 guide pages
- `reference/` — 3 reference pages + API auto-gen
- `tutorials/` — 8+ tutorial pages
- `examples/` — 6 example pages
- Root-level guides (DuckDB, Spark, Iceberg)
- `api/` — 4 auto-generated API pages

**Excluded** (internal-only):
- `brainstorms/` — Design exploration notes
- `plans/` — Implementation plans
- `assessments/` — Documentation quality reports
- `solutions/` — Internal debugging notes
- `deployments/` — Deployment logs
- `CLAUDE.md` files — AI assistant instructions

### 4. API Reference: Auto-Generated (mkdocstrings)

Leverage existing mkdocstrings configuration. The `api/` directory already has directives for core, featurestore, and tasks modules. Requires well-documented source code docstrings (already present).

### 5. Index Page: Dual-Compatible

Rewrite `docs/index.md` to work both as rendered MkDocs site AND readable raw markdown on GitHub. Replace Material-only card syntax with standard markdown that degrades gracefully.

---

## Documentation Flow

The navigation should guide users through a clear learning path:

```
New User Journey:
  Home → Getting Started → Quick Start → YAML Tutorial → Concepts

Data Engineer Path:
  Home → Getting Started → YAML Tutorial → Virtual Environments → CLI Reference

ML Engineer Path:
  Home → Getting Started → Python Pipelines → Training to Serving → PIT Joins

Analytics Engineer Path:
  Home → Getting Started → YAML Tutorial → Semantic Layer → Testing & Audits
```

Each page should have:
- Clear "Prerequisites" at the top (what to read first)
- "Next Steps" at the bottom (where to go next)
- Cross-references to related concepts

---

## Phase 1: Get It Live (Core Scope)

1. **Expand `mkdocs.yml` nav** to cover all ~45 public docs
2. **Fix `docs/index.md`** — replace card syntax with standard markdown
3. **Add GitHub Actions workflow** for auto-deploy to GitHub Pages
4. **Verify mkdocstrings** — test that API docs render correctly
5. **Test locally** with `mkdocs serve` — fix any broken links or rendering issues
6. **Deploy** — push to main, verify site is live

## Phase 2: Polish (Future)

- Add Mermaid diagrams to concept pages (PIT joins, second-order aggs, DAG execution)
- Custom landing page with hero section and feature highlights
- Add "Learning Paths" section per persona
- Search optimization (tags, metadata)
- Custom 404 page
- Version selector (if releases diverge)
- Social cards / OpenGraph metadata
- Add a 5-minute quickstart page

---

## Open Questions

1. **Tutorial example files** — Should `tutorials/examples-mixed/` YAML/Python files be included as downloadable assets or just referenced in tutorials?
2. **DuckDB/Spark root docs** — Should `duckdb-getting-started.md`, `duckdb-flow-guide.md`, and `spark-transformers-reference.md` be moved into `guides/` or kept at root?
3. **Iceberg materialization** — Is `iceberg-materialization.md` ready for public docs or still internal/draft?

---

## Constraints

- Must not break existing raw markdown readability on GitHub
- Must preserve all cross-references between documents
- GitHub Actions workflow should only deploy from `main` branch
- No custom domain setup needed (default GitHub Pages URL)
- MkDocs dependencies stay in `requirements-docs.txt` (separate from main project)

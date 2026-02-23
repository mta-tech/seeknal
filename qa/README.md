# QA Automation: Medallion E2E Pipeline Testing

Spec-driven QA automation using Claude Code agents. Inspired by [Bowser](https://github.com/disler/bowser).

## Quick Start

```bash
# Invoke the QA skill from Claude Code
/qa
```

This discovers all YAML specs in `qa/specs/`, spawns parallel worker agents, scaffolds seeknal projects, executes pipelines against live infrastructure, and validates results.

## Directory Structure

```
qa/
├── specs/                  # Test scenario definitions (YAML)
│   ├── csv-medallion.yml
│   ├── iceberg-medallion.yml
│   └── postgresql-medallion.yml
├── runs/                   # Generated test projects (gitignored)
│   ├── csv-medallion/
│   ├── iceberg-medallion/
│   └── postgresql-medallion/
└── README.md
```

## Adding a New Test

Drop a new YAML spec file in `qa/specs/`. No code changes needed.

Each spec defines:
- **Seed data**: Inline CSV or SQL data
- **Pipeline**: Bronze sources, silver transforms, gold aggregations
- **Validation**: Expected node/edge counts, row counts, feature coverage
- **Infrastructure**: What external services are required

## Prerequisites

| Source Type | Infrastructure Required |
|-------------|------------------------|
| CSV | None |
| Iceberg | atlas-dev-server (Lakekeeper at `http://172.19.0.9:8181`) |
| PostgreSQL | Local PostgreSQL (`localhost:5432`) |

## Spec Format

See existing specs in `qa/specs/` for examples. Key sections:

```yaml
name: my-test
description: What this test validates
source_type: csv|iceberg|postgres

infrastructure:
  requires: []  # or [lakekeeper], [postgresql, lakekeeper]

seed_data:
  file.csv: |
    col1,col2
    val1,val2

pipeline:
  bronze: [...]
  silver: [...]
  gold: [...]

validation:
  dag:
    expected_nodes: N
    expected_edges: [...]
  execution:
    success: true
  outputs:
    - node: transform.name
      min_rows: N

features_tested:
  - feature_name
```

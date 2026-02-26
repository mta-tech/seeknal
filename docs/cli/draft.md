---
summary: Generate template files for new pipeline nodes
read_when: You want to create a new source, transform, or feature group
related:
  - dry-run
  - apply
---

# seeknal draft

Generate a template file for a new pipeline node using Jinja2 templates.
The generated draft file can be edited and then applied using `seeknal apply`.

## Synopsis

```bash
seeknal draft [OPTIONS] NODE_TYPE NAME
```

## Description

The `draft` command creates a starter template file for a new pipeline node.
Templates are available for all node types and can be output in either YAML
(default) or Python format.

Template discovery order:
1. Project templates: `seeknal/templates/*.j2`
2. Package default templates

## Options

| Option | Description |
|--------|-------------|
| `NODE_TYPE` | Node type: `source`, `transform`, `feature-group`, `second-order-aggregation`, `model`, `aggregation`, `rule`, `exposure`, `semantic-model` |
| `NAME` | Node name (used for filename) |
| `--description`, `-d` | Node description |
| `--force`, `-f` | Overwrite existing draft file |
| `--python` | Generate Python file instead of YAML |
| `--deps` | Comma-separated Python dependencies for PEP 723 header (Python only) |

## Examples

### Create a YAML source draft

```bash
seeknal draft source postgres_users
```

### Create a YAML transform draft

```bash
seeknal draft transform clean_data
```

### Create a Python transform draft

```bash
seeknal draft transform clean_data --python
```

### Create a Python source with dependencies

```bash
seeknal draft source raw_users --python --deps pandas,requests
```

### Create a feature group with description

```bash
seeknal draft feature-group user_behavior --description "User behavior features"
```

### Create a second-order aggregation draft

```bash
seeknal draft second-order-aggregation region_metrics
```

### Create a Python feature group with dependencies

```bash
seeknal draft feature-group customer_features --python --deps pandas,duckdb
```

### Overwrite existing draft

```bash
seeknal draft transform clean_data --force
```

## Output Files

Generated files are created in the project root with a `draft_` prefix:

```
draft_source_postgres_users.yml
draft_transform_clean_data.yml
draft_feature_group_user_behavior.yml
```

## See Also

- [seeknal dry-run](dry-run.md) - Validate and preview draft files
- [seeknal apply](apply.md) - Apply draft to production

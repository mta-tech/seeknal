---
summary: Apply YAML or Python pipeline file to production
read_when: You have a draft file ready to be added to your project
related:
  - draft
  - dry-run
  - plan
---

# seeknal apply

Apply a draft or pipeline file to the production seeknal/ directory. This validates
the file and copies it to the appropriate location for execution.

## Synopsis

```bash
seeknal apply [OPTIONS] FILE_PATH
```

## Description

The `apply` command takes a draft file (created with `seeknal draft`) or any valid
YAML/Python pipeline file and applies it to your project. It performs validation
before applying to ensure the file is syntactically correct and follows Seeknal
conventions.

Files are copied to the appropriate subdirectory under `seeknal/` based on their
node type (sources/, transforms/, feature_groups/, models/, etc.).

## Options

| Option | Description |
|--------|-------------|
| `FILE_PATH` | Path to the YAML or Python file to apply |
| `--force`, `-f` | Overwrite existing file without confirmation |
| `--dry-run` | Validate and show what would be applied without making changes |

## Examples

### Apply a draft file

```bash
seeknal apply draft_source_customers.yml
```

### Apply with force (overwrite existing)

```bash
seeknal apply draft_transform_clean_data.yml --force
```

### Dry run to preview changes

```bash
seeknal apply draft_feature_group_user_behavior.yml --dry-run
```

## See Also

- [seeknal draft](draft.md) - Generate template files
- [seeknal dry-run](dry-run.md) - Validate and preview without applying
- [seeknal plan](plan.md) - Analyze changes and show execution plan

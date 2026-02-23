---
date: 2026-02-10
topic: draft-first-tutorials
---

# Draft-First Tutorial Standardization

## What We're Building

Update all Seeknal tutorials to teach the `draft → dry-run → apply` workflow as the primary development pattern. Remove all direct file creation examples that require users to manually create files, know paths, or write boilerplate from scratch.

## Why This Approach

### Problem
- Beginners struggle with file paths, directory structure, and naming conventions
- Manual file creation leads to syntax errors and missing required fields
- Tutorials are inconsistent - some show direct creation, others use draft

### Solution
- `seeknal draft` provides validated scaffolding for all node types (sources, transforms, feature-groups, models, aggregations, rules, exposures)
- The draft workflow teaches safe development practices (preview before applying)
- Standardizes on one consistent pattern across all documentation

### Why Approach B (Restructure Workflow) vs Simple Replacement
- Teaching the complete `draft → dry-run → apply` lifecycle builds better habits
- Users learn to preview changes before applying them
- More comprehensive understanding of the Seeknal development process

## Key Decisions

| Decision | Rationale |
|----------|-----------|
| **Update ALL tutorials** | Consistency - beginners won't encounter conflicting patterns |
| **Remove direct file creation completely** | Avoid confusion from multiple approaches |
| **Teach full draft→dry-run→apply lifecycle** | Users learn safe development from the start |
| **Keep Python --python flag prominent** | Python is still the preferred format for transforms |
| **Show file editing AFTER draft creation** | Users still need to understand how to customize the generated templates |

## New Tutorial Flow Pattern

### Before (Direct File Creation)
```bash
# 1. Create directory
mkdir -p seeknal/transforms

# 2. Create file with specific name and location
# seeknal/transforms/clean_data.yml

# 3. Write YAML from scratch (user must know schema)
type: transform
name: clean_data
source: raw_data
sql: |
  SELECT * FROM raw_data WHERE valid = true
```

### After (Draft-First Workflow)
```bash
# 1. Create draft with scaffolding
seeknal draft transform clean_data

# 2. Edit the generated template
# draft_transform_clean_data.yml is ready to customize

# 3. Preview what will happen
seeknal dry-run draft_transform_clean_data.yml

# 4. Apply to create the actual transform
seeknal apply draft_transform_clean_data.yml
```

## Tutorials to Update

1. **Getting Started Comprehensive** (`docs/getting-started-comprehensive.md`)
   - Main onboarding path - critical to get right

2. **Python Pipelines Tutorial** (`docs/tutorials/python-pipelines-tutorial.md`)
   - Should emphasize `seeknal draft --python`

3. **Mixed YAML-Python Pipelines** (`docs/tutorials/mixed-yaml-python-pipelines.md`)
   - Show draft workflow for both formats

4. **Feature Store Examples** (`docs/examples/featurestore.md`)
   - Update feature group creation examples

5. **README Quickstart** (`README.md`)
   - Consider if draft fits in quickstart or keeps it minimal

## Open Questions

1. **Should we add a "Why Draft?" section** explaining the benefits of the draft workflow vs manual creation?
2. **What about users coming from dbt** - should we acknowledge the similarity to dbt's workflow?
3. **Should the README quickstart also use draft**, or keep it minimal for fastest "hello world"?
4. **How to handle the Python pipeline decorator approach** - does draft work with @source/@transform decorators?

## Next Steps

→ `/workflows:plan` for implementation details

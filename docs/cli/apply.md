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

When `ATLAS_API_URL` is set, `seeknal apply` also dual-writes metadata to Atlas.
Atlas becomes the control plane for policy checks, asset registration, lineage,
and run reporting, while the local `seeknal/` artifact remains in place as cache
and compatibility state.

## Options

| Option | Description |
|--------|-------------|
| `FILE_PATH` | Path to the YAML or Python file to apply |
| `--force`, `-f` | Overwrite existing file without confirmation |
| `--no-parse` | Skip manifest regeneration after the file is applied |

## Atlas contract sync

Set these environment variables to enable Phase 1 Atlas integration:

```bash
export ATLAS_API_URL="http://atlas-dev-server:8000"
export ATLAS_API_TOKEN="<optional bearer token>"
export SEEKNAL_PROJECT_NAME="my_project"
export ATLAS_ENVIRONMENT="dev"
```

With Atlas sync enabled, `seeknal apply` performs this sequence:

1. Ask Atlas for a policy decision before mutating local files.
2. Move or update the local artifact in `seeknal/`.
3. Register the asset in Atlas.
4. Publish upstream lineage to Atlas.
5. Report the apply run outcome back to Atlas.

If Atlas denies the policy check, the local file is not moved. If Atlas fails
after the local write, the local artifact remains in place and `seeknal apply`
exits with an error so the sync issue is visible.

## Runtime data-access governance

The same Atlas configuration also activates **runtime** governance for data reads.
When `ATLAS_API_URL` is set, Seeknal delegates read decisions to the Atlas backend
(OpenFGA-backed) instead of trusting local configuration:

- Source **sample reads** (`seeknal source sync`, the `preview` context template) are
  access-checked per table, and any columns Atlas classifies as sensitive are masked
  before sample rows are written into the Ask context.
- The gate is **fail-closed**: a denied decision *or* an unreachable Atlas yields no
  rows (the preview degrades to "Preview unavailable"). Set `ATLAS_FAIL_OPEN=true`
  only where availability must outweigh enforcement.
- When `ATLAS_API_URL` is unset, governance is inactive and reads pass through
  unchanged.

Relevant environment variables:

```bash
export ATLAS_API_URL="http://atlas-dev-server:8000"   # activates governance
export ATLAS_API_TOKEN="<optional bearer token>"
export ATLAS_FAIL_OPEN="false"                        # default: fail-closed
export ATLAS_ACTOR="alice@example.com"                # defaults to the OS user
```

Inspect or test decisions from the CLI:

```bash
seeknal atlas governance status                       # is enforcement active?
seeknal atlas governance check prod.gold.customer --action read
```

## Examples

### Apply a draft file

```bash
seeknal apply draft_source_customers.yml
```

### Apply with force (overwrite existing)

```bash
seeknal apply draft_transform_clean_data.yml --force
```

### Apply without manifest regeneration

```bash
seeknal apply draft_feature_group_user_behavior.yml --no-parse
```

### Apply with Atlas contract sync enabled

```bash
ATLAS_API_URL=http://atlas-dev-server:8000 \
SEEKNAL_PROJECT_NAME=retail \
ATLAS_ENVIRONMENT=dev \
seeknal apply draft_transform_orders_enriched.yml
```

## See Also

- [seeknal draft](draft.md) - Generate template files
- [seeknal dry-run](dry-run.md) - Validate and preview without applying
- [seeknal plan](plan.md) - Analyze changes and show execution plan

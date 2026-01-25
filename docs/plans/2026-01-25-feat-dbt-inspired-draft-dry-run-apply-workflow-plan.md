---
title: dbt-inspired draft/dry-run/apply workflow
type: feat
date: 2026-01-25
---

# feat: dbt-inspired draft/dry-run/apply workflow

## Overview

Add a dbt-inspired three-phase workflow to Seeknal 2.0 for creating, validating, and applying data pipeline nodes via YAML files. This workflow provides a faster, more iterative development experience compared to the current Python-based approach.

**Three New Commands:**
- `seeknal draft <type> <name>` - Generate YAML template from Jinja2
- `seeknal dry-run <file.yml>` - Validate and preview execution
- `seeknal apply <file.yml>` - Move file to production and update manifest

## Problem Statement / Motivation

**Current Pain Points:**
1. **Slow iteration**: Users must write Python code for each node, rebuild, and test
2. **High learning curve**: New users must learn Python API + internal patterns
3. **No preview**: Cannot validate changes before applying to production
4. **Manual manifest updates**: No integration with DAG after file creation

**Inspired by dbt:**
- dbt's "draft → run → docs" workflow is proven effective
- YAML-first approach reduces boilerplate
- Preview before applying prevents breaking changes

**Success Criteria:**
- New users can create a feature_group in <5 minutes
- Dry-run catches 80%+ of errors before apply
- No manual manifest updates required

## Proposed Solution

### Phase 1: Draft Command (Static Templates)

Generate YAML templates using Jinja2:

```bash
$ seeknal draft feature-group user_behavior
✓ Created: draft_feature_group_user_behavior.yml
```

**Template discovery order:**
1. `project/seeknal/templates/feature_group.yml.j2` (project override)
2. Package template: `seeknal/templates/feature_group.yml.j2` (default)

**Supported node types:**
- `source` - External data sources
- `transform` - Reusable SQL transformations
- `feature-group` - Feature groups
- `model` - ML models
- `aggregation` - Reusable aggregations
- `rule` - Business rules

**File naming convention:**
- Draft: `draft_<type>_<name>.yml`
- Applied: `seeknal/<type>s/<name>.yml`

**Example output (draft_feature_group_user_behavior.yml):**
```yaml
kind: feature_group
name: user_behavior
description: "User engagement and activity features"
owner: "team@example.com"
tags: [user, engagement]

entity:
  name: user
  join_keys: [user_id]

materialization:
  event_time_col: event_timestamp
  offline:
    enabled: true
    format: parquet
  online:
    enabled: false

inputs:
  - ref: source.user_events

features:
  activity_score:
    description: "User activity score (0-100)"
    dtype: float
  activity_tier:
    description: "Activity tier (low/medium/high)"
    dtype: string
```

### Phase 2: Dry-Run Command (Validation + Preview)

Validate YAML and preview execution:

```bash
$ seeknal dry-run draft_feature_group_user_behavior.yml

Validating YAML...
✓ YAML syntax valid
✓ Schema validation passed
✓ Dependency check: source.user_events exists

Executing preview (limit 10 rows)...
┌──────────┬────────────────┬───────────────┐
│ user_id  │ activity_score │ activity_tier │
├──────────┼────────────────┼───────────────┤
│ 12345    │ 87.5           │ medium        │
│ 67890    │ 92.1           │ high          │
└──────────┴────────────────┴───────────────┘

✓ Preview completed in 0.3s
→ Run 'seeknal apply draft_feature_group_user_behavior.yml' to apply
```

**Validation steps:**
1. YAML syntax validation (with line numbers on error)
2. Schema validation (required fields)
3. Dependency validation (refs to upstream nodes)
4. Preview execution (limit 10 rows by default)

**Flags:**
- `--limit <n>` - Set row limit (default: 10)
- `--timeout <s>` - Set timeout in seconds (default: 30)
- `--schema-only` - Skip execution, validate schema only

### Phase 3: Apply Command (File Move + Manifest Update)

Apply the validated draft to production:

```bash
$ seeknal apply draft_feature_group_user_behavior.yml

Validating...
✓ All checks passed

Moving file...
  FROM: ./draft_feature_group_user_behavior.yml
  TO:   ./seeknal/feature_groups/user_behavior.yml

Updating manifest...
✓ Manifest regenerated

Diff:
  + feature_group.user_behavior
    - event_timestamp (event_time)
    - user_id (join_key)
    - activity_score (float)
    - activity_tier (string)
    - depends_on: source.user_events

✓ Applied successfully
```

**Workflow:**
1. Validate file exists and YAML is valid
2. Check if target exists (prompt or require `--force`)
3. Move file to `seeknal/<type>s/<name>.yml`
4. Run `seeknal parse` to regenerate `target/manifest.json`
5. Show diff of changes

**Flags:**
- `--force` - Overwrite existing files without prompt
- `--no-parse` - Skip manifest regeneration

**Conflict handling:**
```bash
$ seeknal apply draft_feature_group_user_behavior.yml

⚠ Node already exists: seeknal/feature_groups/user_behavior.yml

Changes:
  + description: "User behavior with new metrics"
  - description: "User engagement and activity features"
  + feature: churn_probability

Overwrite? Use --force to confirm
```

## Technical Considerations

### Architecture

**New files to create:**
```
src/seeknal/cli/
├── draft.py              # Draft command implementation
├── dry_run.py            # Dry-run command implementation
└── apply.py              # Apply command implementation

src/seeknal/workflow/
├── __init__.py
├── templates/            # Jinja2 templates
│   ├── source.yml.j2
│   ├── transform.yml.j2
│   ├── feature_group.yml.j2
│   ├── model.yml.j2
│   ├── aggregation.yml.j2
│   └── rule.yml.j2
├── validators.py         # YAML schema validation
├── executor.py           # Dry-run execution engine
└── manifest_diff.py      # Diff display logic
```

**Integration points:**
- `src/seeknal/cli/main.py:42` - Add new commands to Typer app
- `src/seeknal/dag/parser.py:28` - Reuse YAML parsing logic
- `src/seeknal/dag/manifest.py:15` - Extend node types if needed
- `src/seeknal/utils/path_security.py:18` - Validate file paths

### Implementation Phases

#### Phase 1: Core Draft Command (Week 1)
- Create Jinja2 templates for 7 node types
- Implement template discovery (project vs. package)
- Implement `seeknal draft <type> <name>` command
- Add `--description` flag
- Add `--force` flag for overwriting existing drafts

**Success criteria:**
- All 7 templates generate valid YAML
- Project templates override package templates
- File naming convention enforced

#### Phase 2: Dry-Run Validation (Week 2)
- Implement YAML syntax validation with error context
- Implement schema validation for each node type
- Implement dependency validation (refs to upstream nodes)
- Add `--limit` and `--timeout` flags
- Add `--schema-only` flag

**Success criteria:**
- YAML parse errors show line numbers + context
- Schema validation catches missing required fields
- Dependency warnings for missing refs

#### Phase 3: Dry-Run Execution (Week 3)
- Implement execution engine for each node type
- Use DuckDB for unified query execution
- Add table formatting (use `tabulate` package)
- Implement timeout handling
- Add execution time display

**Success criteria:**
- Dry-run executes with sample data (limit 10 rows)
- Timeout after 30 seconds (configurable)
- Table output formatted correctly

#### Phase 4: Apply Workflow (Week 4)
- Implement file move with directory auto-creation
- Implement conflict detection and `--force` flag
- Integrate with `seeknal parse` for manifest update
- Implement diff display
- Add rollback on failure

**Success criteria:**
- Files moved to `seeknal/<type>s/<name>.yml`
- Manifest regenerated automatically
- Diff shows added/modified nodes

#### Phase 5: Security & Edge Cases (Week 5)
- Add YAML injection prevention (`yaml.safe_load()`)
- Add path traversal validation (use existing `path_security.py`)
- Add credential detection warnings
- Handle all edge cases from SpecFlow analysis
- Add CI/CD mode (auto-detect TTY)

**Success criteria:**
- All security checks pass
- CI/CD friendly (no prompts)
- Comprehensive test coverage

### Security Considerations

**1. YAML Injection Prevention**
```python
# Always use yaml.safe_load()
import yaml

with open(file_path) as f:
    data = yaml.safe_load(f)  # Never use yaml.load()
```

**2. Path Traversal Prevention**
```python
from seeknal.utils.path_security import warn_if_insecure_path

# Validate paths before file operations
warn_if_insecure_path(target_path, context="apply command")
if ".." in Path(target).parts:
    raise ValueError("Path traversal not allowed")
```

**3. SQL Injection Prevention**
```python
from seeknal.validation import validate_column_name

# Validate all column names from YAML
for col in yaml_data.get('columns', []):
    validate_column_name(col)
```

**4. Credential Handling**
```python
# Detect credentials in YAML
credential_fields = ['password', 'api_key', 'token', 'secret']
if any(f in yaml_data for f in credential_fields):
    _echo_warning("Credentials found. Use environment variables instead.")
```

### Performance Implications

**Draft Command:**
- Target: <1 second for template generation
- I/O bound: File write only
- No network or database calls

**Dry-Run Command:**
- Target: <30 seconds (configurable timeout)
- Network bound: Source connections
- Query bound: Transform execution
- Use DuckDB for efficient SQL execution

**Apply Command:**
- Target: <5 seconds for file move + manifest update
- I/O bound: File operations
- Scales linearly with node count in manifest

### Dependencies & Risks

**Dependencies:**
- `Jinja2` - Template rendering (already in dependencies?)
- `tabulate` - Table formatting for dry-run output
- Existing DAG parsing infrastructure
- Existing manifest generation

**Risks:**

| Risk | Impact | Mitigation |
|------|--------|------------|
| Template schema incomplete | High | Define schemas in Phase 1 before templates |
| Dry-run execution complexity | High | Start with validation-only, add execution later |
| Breaking changes to existing code | Medium | Run full test suite on each PR |
| Poor user experience | Medium | User testing with real scenarios |
| Security vulnerabilities | High | Security review before merge |

## Acceptance Criteria

### Functional Requirements

#### Draft Command
- [x] `seeknal draft <type> <name>` creates `draft_<type>_<name>.yml`
- [x] Generated YAML is valid and parses with `yaml.safe_load()`
- [x] Templates include required fields for each node type
- [x] Project templates override package templates
- [x] Error if invalid node type provided
- [x] Error if draft file already exists (unless `--force`)
- [x] `--description` flag populates description field

#### Dry-Run Command
- [x] `seeknal dry-run <file.yml>` validates YAML syntax
- [x] Shows line number and context for parse errors
- [x] Validates required fields for node type
- [x] Validates refs to upstream nodes (with warnings)
- [x] Executes node with sample data (limit 10 rows)
- [x] Displays output in table format
- [x] Shows column names and types
- [x] `--limit` flag controls row count
- [x] Times out after 30 seconds (configurable)
- [x] Suggests `seeknal apply` on success

#### Apply Command
- [x] `seeknal apply <file.yml>` validates file exists
- [x] Moves file to `seeknal/<type>s/<name>.yml`
- [x] Creates target directory if not exists
- [x] Runs `seeknal parse` to regenerate manifest
- [x] Shows diff of changes
- [x] Prompts for confirmation if target exists
- [x] `--force` flag skips confirmation
- [x] Validates complete DAG after update
- [x] Rolls back if manifest update fails

### Non-Functional Requirements

#### Security
- [x] All YAML parsing uses `yaml.safe_load()`
- [x] File paths validated against path traversal
- [x] Credentials in YAML trigger warning
- [x] SQL injection prevention in transform nodes
- [x] Unsafe paths (/tmp) trigger warnings

#### Performance
- [x] Draft command completes in <1 second
- [x] Dry-run completes in <30 seconds (configurable)
- [x] Apply completes in <5 seconds
- [x] Manifest regeneration scales to 1000+ nodes

#### Reliability
- [x] Atomic apply (all-or-nothing)
- [x] Rollback on failure
- [x] No data loss on crash
- [x] Handles concurrent operations

#### Usability
- [x] Clear error messages with actionable suggestions
- [x] Color-coded output (success/error/warning)
- [x] Progress indicators for long operations
- [x] Auto-completion for CLI arguments
- [x] Consistent output format across commands

#### Compatibility
- [x] Works with existing common.yml structure
- [x] Backward compatible with existing FeatureGroup API
- [x] Supports Python 3.11+
- [x] Works on Linux, macOS, Windows
- [x] CI/CD friendly (non-interactive mode)

## Success Metrics

- [ ] Time to create first feature_group <5 minutes
- [ ] Dry-run catches 80%+ of errors before apply
- [ ] Zero manual manifest updates required
- [ ] Test coverage >80% for new code
- [ ] Security review passed
- [ ] 5+ user scenarios tested successfully

## References & Research

### Internal References

**Architecture & Patterns:**
- `src/seeknal/cli/main.py:42` - CLI command patterns with Typer
- `src/seeknal/dag/parser.py:28` - YAML parsing with `yaml.safe_load()`
- `src/seeknal/dag/manifest.py:15` - Node types and manifest structure
- `src/seeknal/utils/path_security.py:18` - Path validation (block /tmp)
- `src/seeknal/cli/repl.py:1` - SQL REPL with encryption patterns

**Design Documents:**
- `docs/plans/2026-01-08-seeknal-2.0-dbt-inspired-design.md` - Original plan with full YAML schemas (Appendix A)
- `docs/brainstorms/2026-01-25-draft-apply-workflow.md` - Brainstorm with implementation decisions
- `docs/brainstorms/2026-01-25-gap-analysis-plan-vs-brainstorm.md` - Gap analysis (this plan aligns with simplified approach)

**Learnings:**
- Use `yaml.safe_load()` for security (prevents YAML injection)
- Collect validation errors instead of failing fast
- Use consistent error symbols (✗, ✓, ⚠, ℹ)
- Validate paths with `warn_if_insecure_path()`
- DAG validation with cycle detection

### External References

**dbt Documentation:**
- https://docs.getdbt.com/docs/build/projects - Project structure
- https://docs.getdbt.com/reference/node-config - Node configuration
- https://docs.getdbt.com/cli/generate - Generate command patterns

**Best Practices:**
- https://jinja.palletsprojects.com/en/3.1.x/templates/ - Jinja2 template syntax
- https://pyyaml.org/wiki/PyYAMLDocumentation - YAML safe loading
- https://typer.tiangolo.com/ - Typer CLI framework

### Related Work

**Recent Features:**
- SQL REPL (commit e0436fa) - Pattern for interactive commands
- Feature group versioning - Pattern for diff display
- Source management - Pattern for credential encryption

**Testable Scenarios:**
1. First-time user creates draft and applies
2. Invalid YAML shows helpful error messages
3. Missing dependency shows warning
4. Apply conflict requires --force or confirmation
5. CI/CD mode works without prompts
6. Dry-run timeout prevents long-running queries

### Open Questions (from SpecFlow Analysis)

**Critical (to resolve in implementation):**
1. Exact YAML schemas for each node type - *Use Appendix A from original plan*
2. Jinja2 variables available to templates - *Start with {{ name }}, {{ description }}*
3. Template discovery order - *Check project first, then package*
4. Draft collision behavior - *Error unless --force flag*
5. Dry-run execution strategy per node type - *Use DuckDB for unified execution*
6. Dry-run timeout default - *30 seconds with --timeout flag*
7. Apply atomicity - *Best-effort: move file, then update manifest*
8. Dependency validation - *Check manifest, warn if missing*

**Important (reasonable assumptions documented):**
9. Auto-create directories - *Yes, create seeknal/{type}s/ on apply*
10. Diff display format - *Show node type, name, and column changes*
11. CI/CD mode - *Detect TTY, require --yes in non-interactive*
12. Common.yml integration - *YAML nodes override common.yml*
13. Manifest location - *target/manifest.json (same as seeknal parse)*
14. Limit flag behavior - *Limit rows (SQL LIMIT clause)*
15. Credential handling - *Allow but warn, suggest env vars*

---

*Plan created 2026-01-25 based on gap analysis and SpecFlow analysis*

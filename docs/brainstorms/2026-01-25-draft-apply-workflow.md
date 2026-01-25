# Brainstorm: YAML Workflow (draft → dry-run → apply)

**Date:** 2026-01-25
**Status:** Brainstorming Complete
**Related:** docs/plans/2026-01-08-seeknal-2.0-dbt-inspired-design.md

---

## What We're Building

A dbt-inspired YAML development workflow that enables users to:
1. **Scaffold** new node definitions via `seeknal draft <type> <name>`
2. **Preview** changes before applying via `seeknal dry-run <file.yml>`
3. **Apply** validated definitions to the project via `seeknal apply <file.yml>`

**Scope:** Focus ONLY on `draft` and `apply` commands with full dry-run preview.

---

## Why This Approach

### User-Driven Decisions
- **Static templates** - Simple, predictable, easy to maintain
- **Flat directory structure** - `seeknal/{type}s/` for easy navigation
- **Full execution preview** - See actual data output before committing

### Existing Foundation to Leverage
- ✅ DAG system already built (`src/seeknal/dag/`)
- ✅ Manifest generation works (`seeknal parse`)
- ✅ Diff detection implemented
- ✅ CLI patterns established (Typer, output helpers)
- ✅ YAML parsing for `common.yml`

### Gap to Fill
- ❌ No scaffolding mechanism
- ❌ No draft/apply workflow
- ❌ No way to add nodes incrementally

---

## Key Decisions

### Decision 1: Static Template System

**Use Jinja2 templates stored in `seeknal/templates/`**

```python
# Template locations
seeknal/templates/
├── source.yml.j2
├── transform.yml.j2
├── feature-group.yml.j2
├── model.yml.j2
├── aggregation.yml.j2
└── rule.yml.j2
```

**Why:**
- Simple to implement and maintain
- Predictable output for users
- Easy to customize per project
- Can add project-specific templates later

**Trade-off:** Less flexible than interactive wizard, but YAGNI - users can edit YAML directly.

---

### Decision 2: Draft Creates Local Files

**Workflow:**
```bash
$ seeknal draft feature-group user_behavior
✓ Created: draft_feature_group_user_behavior.yml

$ vim draft_feature_group_user_behavior.yml

$ seeknal dry-run draft_feature_group_user_behavior.yml
# Preview execution...

$ seeknal apply draft_feature_group_user_behavior.yml
✓ Moved to: seeknal/feature_groups/user_behavior.yml
✓ Manifest updated
```

**Why:**
- Draft files are visible in project root (can't miss them)
- Git-friendly (easy to see what's in progress)
- Apply moves to proper location (`seeknal/{type}s/`)

---

### Decision 3: Flat Directory Structure

**Target layout:**
```
project/
├── common.yml                    # Reusable components
├── seeknal/
│   ├── sources/                  # Source YAMLs
│   ├── transforms/               # Transform YAMLs
│   ├── feature_groups/           # Feature group YAMLs
│   ├── models/                   # Model YAMLs (future)
│   ├── aggregations/             # Aggregation YAMLs (from common)
│   └── rules/                    # Rule YAMLs (from common)
└── target/
    └── manifest.json             # Generated DAG
```

**Why:**
- Simple and predictable
- Easy to navigate
- Matches dbt's mental model
- Can add domain folders later if needed

---

### Decision 4: Dry-Run Does Full Preview

**What dry-run shows:**
1. YAML validation (syntax + schema)
2. Sample execution output (first 10 rows)
3. Column names and types
4. Estimated row count (with LIMIT clause)
5. Confirmation prompt

**Why:**
- Users want to see actual data before committing
- Catches errors early (schema mismatches, missing columns)
- Builds confidence in the change

**Implementation:** Reuse existing execution logic with row limit.

---

### Decision 5: Apply Updates Manifest Automatically

**What apply does:**
1. Validate file exists and is valid YAML
2. Move file from `draft_*.yml` to `seeknal/{type}/{name}.yml`
3. Run `seeknal parse` to regenerate manifest
4. Show diff of changes
5. Success message

**Why:**
- One command does everything
- Automatic manifest update reduces errors
- Diff provides feedback on what changed

---

## Implementation Approach

### Phase 1: Scaffolding (`draft`)

**Files to create:**
```
src/seeknal/scaffold/
├── __init__.py
├── templates.py      # Template loader
└── generator.py      # File generator
```

**CLI command:**
```python
@app.command()
def draft(
    node_type: NodeType = typer.Argument(...),
    name: str = typer.Argument(...),
    description: Optional[str] = typer.Option(None, "--description", "-d"),
):
    """Scaffold a new YAML definition."""
    template = load_template(node_type)
    content = render_template(template, name=name, description=description)
    write_file(f"draft_{node_type.value}_{name}.yml", content)
```

---

### Phase 2: Dry-Run Preview

**Files to create:**
```
src/seeknal/dag/
├── preview.py        # Preview execution logic
```

**CLI command:**
```python
@app.command("dry-run")
def dry_run(
    yaml_file: Path = typer.Argument(..., exists=True),
    limit: int = typer.Option(10, "--limit", "-l"),
):
    """Preview execution without side effects."""
    # 1. Parse and validate YAML
    node = parse_yaml_node(yaml_file)

    # 2. Execute with row limit
    result = execute_preview(node, limit=limit)

    # 3. Show output
    typer.echo(result.to_table())

    # 4. Prompt for apply
    typer.echo(f"\nReady to apply? Run: seeknal apply {yaml_file}")
```

---

### Phase 3: Apply Workflow

**CLI command:**
```python
@app.command()
def apply(
    yaml_file: Path = typer.Argument(..., exists=True),
    force: bool = typer.Option(False, "--force", "-f"),
):
    """Apply a draft YAML to the project."""
    # 1. Validate
    node = parse_yaml_node(yaml_file)

    # 2. Determine target path
    target_path = get_target_path(node)

    # 3. Move file
    shutil.move(yaml_file, target_path)

    # 4. Regenerate manifest
    parser = ProjectParser(...)
    new_manifest = parser.parse()

    # 5. Show diff
    diff = ManifestDiff.compare(old_manifest, new_manifest)
    typer.echo(diff.summary())

    _echo_success(f"Applied: {target_path}")
```

---

## Open Questions

1. **Template customization:** Should users be able to provide custom templates in their project?
   - *Proposal:* Check `project/seeknal/templates/` first, fall back to package templates.

2. **Naming conflicts:** What if `seeknal/feature_groups/user_behavior.yml` already exists?
   - *Proposal:* Show error and require `--force` flag to overwrite.

3. **Draft cleanup:** Should draft files be deleted after apply, or kept?
   - *Decision:* Delete (moved to target location).

4. **Python vs YAML:** Should we support generating Python code too?
   - *Proposal:* Out of scope for now. Future enhancement.

5. **dry-run dependencies:** What if the YAML references `ref("upstream")` that doesn't exist yet?
   - *Proposal:* Show warning but still preview the node's own logic.

---

## Success Criteria

- [ ] `seeknal draft feature-group foo` creates valid YAML template
- [ ] `seeknal dry-run draft_feature_group_foo.yml` shows sample output
- [ ] `seeknal apply draft_feature_group_foo.yml` moves file and updates manifest
- [ ] Files are created in correct `seeknal/{type}s/` directories
- [ ] Error messages are clear and actionable
- [ ] Git diff shows clean changes

---

## Next Steps

1. Run `/workflows:plan` to create implementation plan
2. Create Jinja2 templates for each node type
3. Implement scaffold module
4. Add CLI commands to main.py
5. Test end-to-end workflow

---

*Brainstorm captured from conversation on 2026-01-25*

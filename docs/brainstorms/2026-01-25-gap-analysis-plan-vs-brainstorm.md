# Gap Analysis: Original Plan vs. Brainstorm Implementation

**Date:** 2026-01-25
**Comparing:** `docs/plans/2026-01-08-seeknal-2.0-dbt-inspired-design.md` vs. `docs/brainstorms/2026-01-25-draft-apply-workflow.md`

---

## Summary

The brainstorm **aligns well** with the original plan but focuses on a **simpler, phased approach**. Key gaps are intentional scope reductions following YAGNI principles.

---

## Feature Comparison Matrix

| Feature | Original Plan | Brainstorm | Gap Status |
|---------|--------------|------------|------------|
| **draft command** | Interactive wizard | Static templates | ⚠️ Simplified |
| `draft --interactive` | ✅ Yes | ❌ No | Out of scope (YAGNI) |
| `draft --from <node>` | ✅ Yes | ❌ No | Future enhancement |
| **dry-run command** | ✅ Yes | ✅ Yes | ✅ Aligned |
| Sample output | ✅ 10 rows | ✅ 10 rows | ✅ Aligned |
| **apply command** | ✅ Yes | ✅ Yes | ✅ Aligned |
| Manifest update | ✅ Auto | ✅ Auto | ✅ Aligned |
| Diff display | ✅ Yes | ✅ Yes | ✅ Aligned |

---

## Detailed Gap Analysis

### Gap 1: Draft Command Modes

| Aspect | Original Plan | Brainstorm | Resolution |
|--------|--------------|------------|------------|
| **Scaffolding mode** | Interactive wizard prompts | Static Jinja2 templates | **Simplified for YAGNI** |
| **Command syntax** | `seeknal draft --interactive` | `seeknal draft <type> <name>` | **Different approach** |
| **Template source** | Unknown | `seeknal/templates/*.j2` | **Clarified in brainstorm** |
| **From existing node** | `seeknal draft --from feature_group.user_call_features new` | Not included | **Out of scope** |

**Reason for Gap:** The brainstorm prioritizes a simpler static template approach. Interactive wizards and reverse-engineering from existing nodes can be added later if needed.

**Recommendation:** Keep static templates for v1. Add `--interactive` as enhancement if users request it.

---

### Gap 2: Directory Structure

| Aspect | Original Plan | Brainstorm | Gap Status |
|--------|--------------|------------|------------|
| **Target layout** | Not specified | `seeknal/{type}s/` (flat) | **Clarified** |
| **Domain folders** | Not mentioned | Optional future | **Aligned** |

**Original Plan Excerpt:**
```yaml
# No explicit directory structure defined in plan
```

**Brainstorm Decision:**
```
seeknal/
├── sources/
├── transforms/
├── feature_groups/
├── models/
├── aggregations/
└── rules/
```

**Gap Status:** ✅ **Clarified** - Brainstorm adds necessary detail not in original plan.

---

### Gap 3: Template Schema Details

| Aspect | Original Plan | Brainstorm | Gap Status |
|--------|--------------|------------|------------|
| **Template structure** | Full YAML schema in Appendix | Referenced | ⚠️ To be implemented |
| **Required fields** | Defined in schema | TBD | ⚠️ To be implemented |
| **Optional fields** | Defined in schema | TBD | ⚠️ To be implemented |

**Original Plan Defines (Appendix A):**
```yaml
kind: source
name: traffic_day
description: "Hourly traffic data from network"
owner: platform-team@company.com
tags: [traffic, network]
source: hive
table: feateng_integtest.db_traffic_hourly
params: { }
columns: { }
freshness: { }
```

**Brainstorm Status:** Templates not yet created. This is implementation detail, not design gap.

**Recommendation:** Use original plan's schema as template reference during implementation.

---

### Gap 4: Dry-Run Execution Strategy

| Aspect | Original Plan | Brainstorm | Gap Status |
|--------|--------------|------------|------------|
| **Preview output** | Table format | Table format | ✅ Aligned |
| **Row limit** | 10 rows (implied) | 10 rows (configurable) | ✅ Enhanced |
| **Validation** | Implied | Explicit steps | ✅ Enhanced |
| **Dependencies** | Not specified | Warning if missing | ⚠️ New concern |

**Original Plan Excerpt:**
```bash
$ seeknal dry-run draft_feature_group_user_engagement.yml

Executing preview (limit 10 rows)...
┌──────────────┬────────────────┬───────────────┐
│ msisdn       │ activity_score │ activity_tier │
├──────────────┼────────────────┼───────────────┤
│ 628123456789 │ 87.5           │ medium        │
└──────────────┴────────────────┴───────────────┘
```

**Brainstorm Adds:**
1. Explicit YAML validation step
2. Column type inference
3. Dependency warning logic
4. Configurable row limit (`--limit`)

**Gap Status:** ✅ **Enhanced** - Brainstorm adds important implementation details.

---

### Gap 5: Apply Workflow Details

| Aspect | Original Plan | Brainstorm | Gap Status |
|--------|--------------|------------|------------|
| **File move** | `FROM: ./draft_*.yml` → `TO: ./seeknal/{type}/{name}.yml` | Same | ✅ Aligned |
| **Manifest update** | "✓ Manifest updated" | Explicit parse + diff | ✅ Enhanced |
| **Conflict handling** | Not specified | `--force` flag | ⚠️ New concern |
| **Draft cleanup** | "Moving file" | Delete after move | ✅ Clarified |

**Original Plan Excerpt:**
```bash
$ seeknal apply draft_feature_group_user_engagement.yml

Moving file...
  FROM: ./draft_feature_group_user_engagement.yml
  TO:   ./seeknal/features/user_engagement.yml

✓ Added to DAG
✓ Manifest updated
```

**Brainstorm Adds:**
1. `--force` flag for overwrite
2. Explicit diff display
3. Error handling for conflicts

**Gap Status:** ✅ **Enhanced** - Brainstorm adds error handling not in original plan.

---

### Gap 6: Command-Line Interface

| Aspect | Original Plan | Brainstorm | Gap Status |
|--------|--------------|------------|------------|
| `draft` syntax | `seeknal draft <type> <name>` | Same | ✅ Aligned |
| `dry-run` syntax | `seeknal dry-run <file.yml>` | Same | ✅ Aligned |
| `apply` syntax | `seeknal apply <file.yml>` | Same | ✅ Aligned |
| `draft --interactive` | ✅ Yes | ❌ No | ⚠️ Simplified |
| `draft --from` | ✅ Yes | ❌ No | ⚠️ Out of scope |
| `dry-run --limit` | Not specified | ✅ Yes | ✅ Enhancement |

**Gap Status:** ⚠️ **Simplified** - Brainstorm removes some options for YAGNI.

---

## Gap Summary by Category

### ✅ Aligned Features (No Gap)

1. **Core workflow:** draft → edit → dry-run → apply
2. **File naming:** `draft_<type>_<name>.yml` convention
3. **Target directory:** Flat structure under `seeknal/`
4. **Manifest update:** Automatic re-parse on apply
5. **Diff display:** Show what changed
6. **Output format:** Table display with sample data

### ⚠️ Simplified Features (Intentional Scope Reduction)

1. **Interactive mode:** Removed from brainstorm (YAGNI)
2. **`--from` flag:** Not included (can add later)
3. **Template variety:** Static only (vs. interactive generation)

**Rationale:** Start simple, add complexity based on user feedback.

### ✅ Enhanced Features (Brainstorm Adds Detail)

1. **Validation steps:** Explicit pre-execution validation
2. **Error handling:** `--force` flag, conflict detection
3. **Configurability:** `--limit` flag for row count
4. **Template system:** Jinja2-based with project override
5. **Dependency warnings:** Handle `ref()` to missing nodes

### ⚠️ To Be Implemented (Implementation Gaps, Not Design Gaps)

1. **Template content:** Actual Jinja2 templates
2. **Schema validation:** YAML schema enforcement
3. **Execution logic:** dry-run preview engine
4. **Error messages:** Specific validation errors

---

## Recommendations

### 1. Keep the Simplified Approach ✅

The brainstorm's focus on **static templates** is the right call for v1:
- Faster to implement
- Easier to maintain
- Predictable for users
- Can add interactive mode later if needed

### 2. Add Missing Implementation Details

The implementation plan needs to specify:
- [ ] Exact template content for each node type
- [ ] Schema validation rules
- [ ] Error message text
- [ ] Dependency resolution strategy

### 3. Consider Future Enhancements

Track these for v2:
- [ ] `seeknal draft --interactive` (wizard mode)
- [ ] `seeknal draft --from <existing_node>` (copy/edit)
- [ ] Domain-based folders (`seeknal/billing/feature_groups/`)
- [ ] Custom project templates

### 4. Alignment Checklist

Before implementation, verify:
- [ ] Command syntax matches original plan
- [ ] File structure matches dbt mental model
- [ ] YAML schema matches Appendix A of original plan
- [ ] Diff display style matches original example

---

## Conclusion

**Gap Status: ✅ HEALTHY**

The brainstorm is **well-aligned** with the original plan while making sensible simplifications for YAGNI. The main "gaps" are:
1. **Intentional scope reductions** (interactive mode, `--from` flag)
2. **Implementation details** to be filled in during planning phase
3. **Enhancements** (error handling, configurability) that improve on the original

**Recommendation:** Proceed with the brainstorm as-is. The simplified approach will ship faster and can be enhanced based on user feedback.

---

*Analysis created 2026-01-25*

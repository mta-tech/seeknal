# Multi-Persona Validation Plan

**Task**: #30
**Evaluator**: qa-docs-lead
**Date**: 2026-02-09
**Status**: In Progress

---

## Overview

Multi-persona users are professionals who span multiple disciplines:
- **Data Engineer + ML Engineer**: Builds pipelines and trains ML models
- **Analytics Engineer + Data Engineer**: Creates semantic models and manages data infrastructure
- **ML Engineer + Analytics Engineer**: Builds features and defines business metrics

This validation ensures Seeknal's documentation serves these multi-disciplinary users effectively.

---

## Validation Approach

### Phase 1: Cross-Path Navigation Testing

**Test**: Can users navigate between persona paths to find relevant content?

| Test Case | Source Path | Target Path | Expected Behavior |
|-----------|-------------|-------------|-------------------|
| 1.1 | Quick Start → DE Path | Choose Your Path card | Links to DE Chapter 1 |
| 1.2 | Quick Start → AE Path | Choose Your Path card | Links to AE Chapter 1 |
| 1.3 | Quick Start → MLE Path | Choose Your Path card | Links to MLE Chapter 1 |
| 1.4 | DE Chapter → Concepts | "See also" section | Links to relevant concepts |
| 1.5 | AE Chapter → Reference | CLI commands | Links to CLI reference |
| 1.6 | MLE Chapter → Building Blocks | Feature store internals | Links to building blocks |
| 1.7 | Any path → Homepage | Breadcrumbs/nav | Return to homepage |
| 1.8 | Any path → Other paths | "See also" links | Discover other relevant paths |

### Phase 2: Multi-Disciplinary Use Cases

**Test**: Can users complete workflows that span multiple personas?

#### Use Case 1: Data Engineer who needs ML features

**User Profile**: Data Engineer building data pipelines, now needs to add ML predictions

**Workflow**:
1. Complete DE Chapter 1 (ELT Pipeline)
2. Need: Add feature store for ML team
3. Action: Find MLE Chapter 1 (Feature Store)
4. Verify: Can navigate from DE to MLE content
5. Verify: Concepts transfer between paths

**Success Criteria**:
- Clear link from DE to MLE content
- Consistent terminology across paths
- No prerequisite blocking (don't force complete DE path first)

#### Use Case 2: Analytics Engineer who needs data infrastructure

**User Profile**: Analytics Engineer creating metrics, now needs to manage data sources

**Workflow**:
1. Complete AE Chapter 1 (Semantic Models)
2. Need: Add new data source
3. Action: Find DE Chapter 1 content on sources
4. Verify: Can navigate from AE to DE content
5. Verify: API/source concepts explained consistently

**Success Criteria**:
- Can find DE content from AE path
- Source definitions consistent across paths
- No contradictory information

#### Use Case 3: ML Engineer who needs business metrics

**User Profile**: ML Engineer building features, now needs to define metrics for model monitoring

**Workflow**:
1. Complete MLE Chapter 1 (Feature Store)
2. Need: Create business metrics for monitoring
3. Action: Find AE Chapter 2 (Business Metrics)
4. Verify: Can navigate from MLE to AE content
5. Verify: Metric concepts explained accessibly

**Success Criteria**:
- Clear path from MLE to AE content
- Metric definitions don't require AE background
- Practical examples for ML use case

### Phase 3: Concept Consistency Testing

**Test**: Are core concepts explained consistently across all persona paths?

| Concept | DE Path | AE Path | MLE Path | Consistent? |
|---------|---------|---------|----------|-------------|
| Pipeline Builder Workflow | Chapter 1 | - | - | ⏳ To verify |
| Source/Transform/Output | Chapter 1 | - | - | ⏳ To verify |
| Virtual Environments | Chapter 3 | - | - | ⏳ To verify |
| YAML vs Python | Chapter 1 | - | - | ⏳ To verify |
| Point-in-Time Joins | - | - | Chapter 1 | ⏳ To verify |
| Feature Groups | - | - | Chapter 1 | ⏳ To verify |
| Semantic Models | - | Chapter 1 | - | ⏳ To verify |
| Metrics | - | Chapter 2 | - | ⏳ To verify |

**Validation**: Each concept should be explained once in detail (concepts/), then referenced consistently in persona paths.

### Phase 4: "See Also" Link Testing

**Test**: Do cross-references work bidirectionally?

| From | To | Link Present? | Reciprocal? |
|------|----|---------------|-------------|
| DE Chapter 1 | AE Chapter 1 | ⏳ | ⏳ |
| DE Chapter 1 | MLE Chapter 1 | ⏳ | ⏳ |
| AE Chapter 1 | DE Chapter 1 | ⏳ | ⏳ |
| AE Chapter 1 | MLE Chapter 1 | ⏳ | ⏳ |
| MLE Chapter 1 | DE Chapter 1 | ⏳ | ⏳ |
| MLE Chapter 1 | AE Chapter 1 | ⏳ | ⏳ |
| Quick Start | All paths | ⏳ | ⏳ |
| Concepts | Relevant paths | ⏳ | ⏳ |

**Success Criteria**: If Path A references Path B, Path B should reference Path A for related topics.

### Phase 5: Terminology Consistency

**Test**: Are terms used consistently across all documentation?

| Term | Definition Location | Used Consistently? |
|------|---------------------|-------------------|
| Flow | Concepts | ⏳ |
| Pipeline | Concepts | ⏳ |
| Task | Concepts | ⏳ |
| Node | Concepts | ⏳ |
| DAG | Concepts | ⏳ |
| Feature Group | Concepts/MLE | ⏳ |
| Semantic Model | Concepts/AE | ⏳ |
| Entity | Concepts | ⏳ |
| Materialization | Concepts | ⏳ |

**Validation**: Each term should have one definition (in glossary or concept doc), used consistently everywhere.

---

## Current Documentation Status

### Quick Start (Complete)
- ✅ Generic example (sales data pipeline)
- ✅ Links to all three persona paths
- ✅ "Not sure which path?" guidance

### DE Path Overview (Complete)
- ✅ Task #9 completed
- ✅ Chapter 1: ELT Pipeline (Task #10)
- ✅ Chapter 2: Incremental Models (Task #11)
- 🔄 Chapter 3: Production Environments (Task #12 - in progress)

### AE Path (Not Started)
- ⏳ AE Path Overview (Task #14)
- ⏳ AE Chapter 1: Semantic Models (Task #15)
- ⏳ AE Chapter 2: Business Metrics (Task #16)
- ⏳ AE Chapter 3: Self-Serve Analytics (Task #17)

### MLE Path (Not Started)
- ⏳ MLE Path Overview (Task #19)
- ⏳ MLE Chapter 1: Feature Store (Task #20)
- ⏳ MLE Chapter 2: Second-Order Aggregation (Task #21)
- ⏳ MLE Chapter 3: Training-to-Serving Parity (Task #22)

---

## Interim Validation (Current State)

Since only DE path is complete, I'll validate:

### What Can Be Validated Now

1. **Quick Start → DE Path Navigation**
   - Does Quick Start link to DE path?
   - Can user find DE Chapter 1 from Quick Start?

2. **DE Path Internal Navigation**
   - Do DE chapters link to each other?
   - Does DE path link to concepts?

3. **DE Path → Reference Documentation**
   - Can DE users find CLI reference?
   - Can DE users find YAML schema reference?

4. **Terminology Consistency (DE)**
   - Are terms used consistently in DE path?
   - Do DE chapters reference concept docs?

### What Cannot Be Validated Yet

1. **Cross-path navigation** (AE/MLE paths don't exist)
2. **Multi-disciplinary use cases** (need AE/MLE content)
3. **Reciprocal linking** (only DE path exists)

---

## Validation Report Template

```markdown
# Multi-Persona Validation Report

**Date**: [YYYY-MM-DD]
**Evaluator**: qa-docs-lead
**Paths Available**: [List available paths]

## Cross-Path Navigation
[Results of navigation tests]

## Multi-Disciplinary Use Cases
[Results of use case tests]

## Concept Consistency
[Results of concept consistency tests]

## "See Also" Links
[Results of link reciprocity tests]

## Terminology Consistency
[Results of terminology tests]

## Issues Found
[List issues with severity]

## Recommendations
[Suggestions for improvement]

## Decision
[ ] PASS - Multi-persona support validated
[ ] NEEDS REVISION - Issues identified
[ ] CANNOT COMPLETE - Content missing
```

---

## Next Steps

1. **Immediate**: Validate Quick Start → DE Path navigation
2. **When AE path complete**: Validate DE ↔ AE cross-navigation
3. **When MLE path complete**: Validate full multi-persona support
4. **Final validation**: Test all multi-disciplinary use cases

---

**End of Plan**

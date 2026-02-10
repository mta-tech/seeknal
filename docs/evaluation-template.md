# Documentation Evaluation Framework

**Version**: 1.0
**Date**: 2026-02-09
**Owner**: qa-docs-lead
**Status**: Active

---

## Overview

This framework defines the production readiness criteria for all Seeknal documentation. Every document must pass evaluation before being marked as production-ready.

**Core Question**: "Can I use this for real work?"

---

## Evaluation Criteria

### 1. Production Readiness (PASS/FAIL)

**The "Can I use this for real work?" test**

A document passes if a user from the target persona can:
1. Complete the task without external help
2. Understand what they're doing (not just copy-pasting)
3. Recover from common errors
4. Find the document from a relevant starting point

**Checklist**:
- [ ] All commands are copy-pasteable and work as written
- [ ] Prerequisites are clearly stated before the document begins
- [ ] Expected output is shown for every code example
- [ ] File paths and project structure are unambiguous
- [ ] Environment-specific context is clear (dev vs production)
- [ ] Time estimates are accurate (within ±50% of stated time)

**Red Flags** (automatic FAIL):
- Commands that produce errors
- Missing steps or "left as exercise"
- Assumes knowledge not documented elsewhere
- Dead-end links (internal or external)
- Version mismatches (docs don't match software)

---

### 2. Clarity Assessment (1-5 scale)

**Score**: 1 = Confusing, 3 = Adequate, 5 = Crystal Clear

**Dimensions**:

| Dimension | Score (1-5) | Notes |
|-----------|:-----------:|-------|
| Terminology consistency | | Are terms used consistently? Is jargon explained? |
| Logical flow | | Do concepts build on each other? Any jumps? |
| Sentence clarity | | Are sentences concise? Any ambiguity? |
| Visual hierarchy | | Are headings, lists, and code blocks used well? |
| Context setting | | Does each section explain WHY before HOW? |

**Calculation**: Average of all dimensions = Clarity Score

**Passing threshold**: 3.5/5.0 average

---

### 3. Completeness Check (PASS/FAIL)

**No missing steps, no orphaned concepts**

**Checklist**:
- [ ] Every concept mentioned is explained or linked
- [ ] Every code example is explained line-by-line for complex parts
- [ ] Edge cases are mentioned (what happens if X?)
- [ ] Troubleshooting section covers common errors
- [ ] Next steps are clear at document end
- [ ] Prerequisites link to setup docs if needed

**Common Gaps to Check**:
- Installation instructions not linked
- Configuration options mentioned but not explained
- Error messages shown without how to fix them
- Commands shown without expected output
- "See the docs for more" without specific link

---

### 4. Code Example Validation (PASS/FAIL)

**All code must be tested and verified**

**Checklist**:
- [ ] Code runs without errors (or errors are expected and documented)
- [ ] Imports/packages are listed
- [ ] File paths are relative to project root or clearly specified
- [ ] Sample data is provided or link to sample data
- [ ] Expected output matches actual output
- [ ] Code is formatted (Black for Python, consistent YAML)

**For YAML Examples**:
- [ ] Valid YAML syntax
- [ ] Schema version specified if required
- [ ] Required fields present
- [ ] Comments explain non-obvious configurations

**For CLI Examples**:
- [ ] Full command shown (no "..." unless explained)
- [ ] Flags are explained
- [ ] Environment variables mentioned if needed

---

### 5. Cross-Reference Verification (PASS/FAIL)

**Every link goes somewhere useful**

**Checklist**:
- [ ] All internal links resolve to existing documents
- [ ] All external links are not broken (check with tool)
- [ ] Concept terms are linked on first use
- [ ] "See also" sections are reciprocal (A links to B, B links to A)
- [ ] Links use relative paths, not absolute URLs
- [ ] API references link to API docs

**Link Quality Standards**:
- Descriptive link text (not "click here")
- No orphaned pages (every page is linked from somewhere)
- No circular references that create infinite loops
- References include section anchors when relevant

---

### 6. Persona Alignment (PASS/FAIL)

**Content matches the target persona's needs**

**Persona-Specific Checks**:

#### Data Engineer (ELT Focus)
- [ ] Data sources/connectors are relevant
- [ ] Scheduling/orchestration covered
- [ ] Incremental processing explained
- [ ] Production deployment considerations

#### Analytics Engineer (Semantic Focus)
- [ ] Business metrics are meaningful
- [ ] BI tool integration mentioned
- [ ] Semantic modeling concepts clear
- [ ] Self-serve analytics workflows

#### ML Engineer (Feature Store Focus)
- [ ] Point-in-time joins explained
- [ ] Training/serving parity addressed
- [ ] Feature versioning covered
- [ ] Model integration patterns

#### Multi-Persona (Quick Start, Concepts)
- [ ] Examples are universally understandable
- [ ] No domain-specific jargon without explanation
- [ ] All three personas can find their next step

---

### 7. Accessibility & Inclusivity (PASS/FAIL)

**Documentation is welcoming and usable**

**Checklist**:
- [ ] Language assumes no prior knowledge of Seeknal
- [ ] Examples use inclusive names and scenarios
- [ ] No humor that could be exclusionary
- [ ] Technical alternatives are mentioned (Windows/Mac/Linux)
- [ ] Images have alt text or captions
- [ ] Color alone is not used to convey information

---

## Evaluation Process

### Phase 1: Writer Self-Review (REQUIRED)

Before submitting for evaluation:
1. Run through this checklist yourself
2. Fix any FAIL items
3. Note any borderline scores in comments
4. Submit to qa-docs-lead with "Ready for Evaluation"

### Phase 2: QA Lead Review

1. Review the document against all criteria
2. Test code examples if applicable
3. Verify all links
4. Provide detailed feedback
5. Return to writer for fixes or approve

### Phase 3: Evaluator Review (for persona-specific content)

1. Eval-new-user reviews from new user perspective
2. Domain expert reviews if applicable
3. Cross-reference check by qa-docs-lead
4. Final approval

### Phase 4: Production Readiness Assessment

1. All evaluation tasks for a section are complete
2. Multi-persona validation passed (Task #30)
3. No critical issues remaining
4. Mark section as production-ready

---

## Issue Tracking

### Issue Severity Levels

| Severity | Description | Example | Action |
|----------|-------------|---------|--------|
| **Critical** | Blocks use for real work | Command produces error | Must fix before merge |
| **High** | Major usability problem | Missing important step | Should fix before merge |
| **Medium** | Noticeable problem | Unclear explanation | Fix before production |
| **Low** | Minor improvement | Formatting inconsistency | Fix when convenient |
| **Info** | Observation | Nice-to-have suggestion | Optional |

### Issue Format

When logging issues, use this template:

```markdown
### [Severity]: Issue Title

**Location**: Section/Paragraph
**Persona Affected**: Which persona(s)
**Description**: What's wrong
**Impact**: Why it matters
**Suggested Fix**: How to fix it (if applicable)
```

---

## Document-Specific Templates

### Quick Start Evaluation

**Special Considerations**:
- Time limit accuracy (must complete in stated time)
- Installation instructions must be foolproof
- Error recovery for common setup issues
- Clear path forward after completion

**Key Metrics**:
- Time to first successful run
- Number of external resources needed
- Confusion points reported

### Tutorial/Chapter Evaluation

**Special Considerations**:
- Prerequisites are cumulative (assume previous chapters)
- Code examples build on each other
- Chapter-level objectives are met
- Progression feels natural

**Key Metrics**:
- Completion rate (do users finish?)
- Understanding retention (quizzes/exercises)
- Time vs. estimated time

### Reference Documentation Evaluation

**Special Considerations**:
- Searchability (keywords, synonyms)
- Examples for every major feature
- Parameter signatures are accurate
- Version information is clear

**Key Metrics**:
- Time to find information
- Accuracy of information
- Completeness of coverage

### Concept Documentation Evaluation

**Special Considerations**:
- Defines terms before using them
- Uses diagrams for complex relationships
- Provides mental model, not just facts
- Links to practical applications

**Key Metrics**:
- Concept understanding (user can explain it back)
- Ability to apply concept in new context

---

## Evaluation Report Template

When completing an evaluation, provide:

```markdown
# Evaluation Report: [Document Name]

**Evaluator**: [Name]
**Date**: [YYYY-MM-DD]
**Document Version**: [Version/Commit]

## Summary

[Overall assessment: PASS/NEEDS REVISION/FAIL]

## Scores

- Production Readiness: [PASS/FAIL]
- Clarity: [X.X]/5.0
- Completeness: [PASS/FAIL]
- Code Examples: [PASS/FAIL]
- Cross-References: [PASS/FAIL]
- Persona Alignment: [PASS/FAIL]
- Accessibility: [PASS/FAIL]

## Detailed Findings

### Critical Issues
[List any critical issues found]

### High Priority Issues
[List any high priority issues]

### Medium/Low Issues
[List medium and low priority issues]

### Positive Highlights
[What works well - celebrate good documentation!]

## Recommendations

[Specific recommendations for improvement]

## Decision

[ ] APPROVED - Ready for production
[ ] NEEDS REVISION - Address [specific] issues
[ ] REJECT - Major rewrite needed

---

## Signature

**Evaluator**: [Name]
**Date**: [YYYY-MM-DD]
```

---

## Quality Gates

### Documentation cannot be marked production-ready until:

1. All Critical and High issues are resolved
2. Clarity score >= 3.5/5.0
3. All code examples verified working
4. All links resolve correctly
5. At least one evaluator from target persona approves
6. qa-docs-lead gives final approval

### Documentation can be marked "Beta" when:

1. All Critical issues are resolved
2. Code examples work
3. Major content gaps are identified but not yet filled
4. Noted as "Work in Progress" at top of document

---

## Tools and Automation

### Link Checking

Use the following command to check all internal links:

```bash
# Check for broken internal links
find docs -name "*.md" -exec grep -l "\[.*\](.*\.md)" {} \;
```

### Code Example Testing

Each code example should have a corresponding test file:

```
docs/examples/
├── quick-start-test.py
├── elt-pipeline-test.py
├── semantic-models-test.py
└── feature-store-test.py
```

### Automated Spell Check

```bash
# Install aspell
pip install pyspelling

# Run spell check
pyspelling
```

---

## Continuous Improvement

### QA Metrics to Track

1. **Issue Discovery Rate**: Issues found per page reviewed
2. **Revision Cycles**: Average iterations before approval
3. **Approval Time**: Days from submission to approval
4. **Persona Success Rate**: Completion rate per persona path

### Framework Updates

This framework should be reviewed quarterly and updated when:
- New evaluation patterns emerge
- Tools or automation become available
- Persona feedback indicates gaps
- Project documentation strategy changes

---

## Related Documents

- [Documentation Assessment Report](/docs/assessments/2026-02-09-documentation-assessment-report.md)
- [Documentation Redesign: Persona-Driven Pipeline Builder](/docs/brainstorms/2026-02-09-documentation-redesign-persona-driven.md)
- [Post-Improvement Evaluation](/docs/assessments/2026-02-09-post-improvement-evaluation.md)

---

## Appendix: Quick Reference

### PASS/FAIL Criteria Summary

| Criterion | PASS | FAIL |
|-----------|------|------|
| Production Readiness | User can complete task | Blocked by errors or missing info |
| Clarity | 3.5+ average | <3.5 average |
| Completeness | All steps present, edges covered | Missing steps or concepts |
| Code Examples | Tested and verified | Untested or produces errors |
| Cross-References | All links resolve | Broken links or orphans |
| Persona Alignment | Matches target persona needs | Wrong level or focus |
| Accessibility | Inclusive and clear | Exclusionary language |

### Issue Response SLAs

| Severity | Target Response |
|----------|-----------------|
| Critical | Same day |
| High | 2 business days |
| Medium | 1 week |
| Low | Next sprint |

---

**End of Framework**

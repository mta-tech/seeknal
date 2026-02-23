# Documentation Evaluation Checklist

**Quick Reference Guide for Evaluators**

Use this checklist when reviewing documentation. Print or keep open while evaluating.

---

## Document Metadata

**Document**: _________________________________
**Evaluator**: _________________________________
**Date**: _________________________________
**Target Persona**: [ ] DE [ ] AE [ ] MLE [ ] Multi
**Document Type**: [ ] Quick Start [ ] Tutorial [ ] Concept [ ] Reference

---

## 1. PRODUCTION READINESS (PASS/FAIL)

**Core Question: Can I use this for real work?**

### Commands & Code
- [ ] All commands are copy-pasteable
- [ ] Commands work as written (no errors)
- [ ] Expected output shown for every example
- [ ] File paths are unambiguous
- [ ] Prerequisites stated upfront

### Environment & Context
- [ ] Dev vs production context is clear
- [ ] Environment variables mentioned if needed
- [ ] Time estimates are accurate (±50%)

### Red Flags (Automatic FAIL if present)
- [ ] Commands produce errors
- [ ] Missing steps
- [ ] Assumes undocumented knowledge
- [ ] Dead-end links
- [ ] Version mismatches

**Decision**: [ ] PASS [ ] FAIL

**Notes**: _________________________________________________
_________________________________________________

---

## 2. CLARITY ASSESSMENT (Score 1-5)

Rate each dimension, then calculate average.

| Dimension | Score (1-5) |
|-----------|:-----------:|
| **Terminology consistency** | 1  2  3  4  5 |
| Terms used consistently? Jargon explained? | |
| | |
| **Logical flow** | 1  2  3  4  5 |
| Concepts build progressively? No jumps? | |
| | |
| **Sentence clarity** | 1  2  3  4  5 |
| Concise? Unambiguous? | |
| | |
| **Visual hierarchy** | 1  2  3  4  5 |
| Headings/lists/code blocks used well? | |
| | |
| **Context setting** | 1  2  3  4  5 |
| Explains WHY before HOW? | |
| | |

**Average Score**: _____ / 5.0

**Passing Threshold**: 3.5/5.0

**Decision**: [ ] PASS (≥3.5) [ ] FAIL (<3.5)

**Notes**: _________________________________________________

---

## 3. COMPLETENESS CHECK (PASS/FAIL)

### Content Coverage
- [ ] Every concept explained or linked
- [ ] Code examples explained (line-by-line for complex parts)
- [ ] Edge cases mentioned
- [ ] Troubleshooting section present
- [ ] Next steps clear at end
- [ ] Prerequisites linked if needed

### Common Gaps to Watch For
- [ ] Installation not linked
- [ ] Config options mentioned but not explained
- [ ] Errors shown without fixes
- [ ] Commands without output
- [ ] "See docs" without specific link

**Decision**: [ ] PASS [ ] FAIL

**Missing Items**: _________________________________________________

---

## 4. CODE EXAMPLE VALIDATION (PASS/FAIL)

### General Requirements
- [ ] Code runs without errors
- [ ] Imports/packages listed
- [ ] File paths specified
- [ ] Sample data provided/linked
- [ ] Expected output matches actual
- [ ] Code is formatted

### YAML-Specific
- [ ] Valid YAML syntax
- [ ] Schema version specified if needed
- [ ] Required fields present
- [ ] Comments explain non-obvious config

### CLI-Specific
- [ ] Full command shown (no "..." unless explained)
- [ ] Flags explained
- [ ] Environment vars mentioned if needed

**Decision**: [ ] PASS [ ] FAIL

**Issues Found**: _________________________________________________

---

## 5. CROSS-REFERENCE VERIFICATION (PASS/FAIL)

### Link Quality
- [ ] All internal links resolve
- [ ] All external links work
- [ ] Concept terms linked on first use
- [ ] "See also" sections reciprocal
- [ ] Relative paths used
- [ ] API refs link to API docs

### Link Quality Standards
- [ ] Descriptive link text (not "click here")
- [ ] No orphaned pages
- [ ] No circular refs
- [ ] Section anchors when relevant

**Decision**: [ ] PASS [ ] FAIL

**Broken Links**: _________________________________________________

---

## 6. PERSONA ALIGNMENT (PASS/FAIL)

### Data Engineer (if applicable)
- [ ] Data sources/connectors relevant
- [ ] Scheduling/orchestration covered
- [ ] Incremental processing explained
- [ ] Production deployment considerations

### Analytics Engineer (if applicable)
- [ ] Business metrics meaningful
- [ ] BI tool integration mentioned
- [ ] Semantic modeling clear
- [ ] Self-serve workflows

### ML Engineer (if applicable)
- [ ] Point-in-time joins explained
- [ ] Training/serving parity addressed
- [ ] Feature versioning covered
- [ ] Model integration patterns

### Multi-Persona (if applicable)
- [ ] Examples universally understandable
- [ ] No domain jargon without explanation
- [ ] All personas can find next step

**Decision**: [ ] PASS [ ] FAIL

**Misalignment Notes**: _________________________________________________

---

## 7. ACCESSIBILITY & INCLUSIVITY (PASS/FAIL)

- [ ] Assumes no prior Seeknal knowledge
- [ ] Inclusive names/scenarios in examples
- [ ] No exclusionary humor
- [ ] Cross-platform considerations (Win/Mac/Linux)
- [ ] Images have alt text/captions
- [ ] Color not sole information carrier

**Decision**: [ ] PASS [ ] FAIL

**Notes**: _________________________________________________

---

## ISSUE SUMMARY

### Critical Issues (Blocks use)
1. _________________________________________________
2. _________________________________________________

### High Priority Issues (Major usability)
1. _________________________________________________
2. _________________________________________________

### Medium/Low Issues (Improvements)
1. _________________________________________________
2. _________________________________________________

---

## FINAL DECISION

[ ] **APPROVED** - Ready for production
[ ] **NEEDS REVISION** - Address [specific] issues
[ ] **REJECT** - Major rewrite needed

**If APPROVED**: What makes this document excellent?
_________________________________________________
_________________________________________________

**If NEEDS REVISION/REJECT**: What is the main issue?
_________________________________________________
_________________________________________________

---

## EVALUATOR SIGN-OFF

**Name**: _________________________________
**Date**: _________________________________

**Time Spent**: _____ minutes

**Additional Comments**:
_________________________________________________
_________________________________________________
_________________________________________________

---

## QUICK SCORING REFERENCE

### Clarity Scale Guide

**5 - Excellent**: Crystal clear, teaches effectively
**4 - Good**: Minor improvements possible
**3 - Adequate**: Gets the job done, some confusion
**2 - Poor**: Significant confusion or gaps
**1 - Confusing**: Does not communicate effectively

### Issue Severity Guide

**Critical**: Blocks use for real work (errors, missing steps)
**High**: Major usability problem (unclear, incomplete)
**Medium**: Noticeable but usable (formatting, minor gaps)
**Low**: Minor polish (typos, style)
**Info**: Suggestions for improvement

---

## FOR EVALUATION COORDINATOR

**Review Completed**: [ ] Yes [ ] No

**Follow-up Actions**:
- [ ] Send feedback to writer
- [ ] Track issues resolution
- [ ] Schedule re-evaluation
- [ ] Update QA metrics

**Date Follow-up Scheduled**: _________________________________

---

**End of Checklist**

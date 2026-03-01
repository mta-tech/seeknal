---
name: workflow:review
description: Review changes against best practices and identify improvements. Use before committing changes.
allowed-tools: Task, TaskOutput, Bash, Glob, Grep, Read, Edit, Write, NotebookEdit, WebFetch, TodoWrite, WebSearch, AskUserQuestion, Skill
model: opus
context: fork
agent: general-purpose
user-invocable: true
---

# Review Mode Skill

## Purpose

Review all changes against best practices before committing.

## Variables

SCOPE: The scope of review (architecture, code-quality, security, performance)

## Instructions

When invoked, review the changes:

1. **Architecture Review:**
   - Check design patterns
   - Verify separation of concerns
   - Assess maintainability
   - Identify coupling issues

2. **Code Quality Review:**
   - Check naming conventions
   - Verify error handling
   - Assess code organization
   - Identify duplication

3. **Security Review:**
   - Check for vulnerabilities
   - Verify input validation
   - Assess authorization
   - Identify data exposure

4. **Performance Review:**
   - Check for N+1 queries
   - Assess algorithm complexity
   - Identify bottlenecks
   - Review caching strategy

## Workflow

```
1. Analyze all changes
2. Check against best practices
3. Identify improvements
4. Create review checklist
5. Provide feedback
```

## Review Checklist

For each review type, check:
- [ ] Follows project conventions
- [ ] No security vulnerabilities
- [ ] Adequate error handling
- [ ] Properly tested
- [ ] Well documented
- [ ] Performance conscious

## Report

After review, provide:
- Summary of findings
- List of issues found
- Severity (critical/high/medium/low)
- Recommended fixes
- Approval decision

## Example

```bash
/review:architecture
/review:security
/review:code-quality
```

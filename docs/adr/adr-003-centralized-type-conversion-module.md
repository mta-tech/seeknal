---
adr_id: ADR-003
date: 2026-02-11
status: accepted
title: Centralized type conversion in shared module
---

# ADR-003: Centralized type conversion in shared module

## Context

Type conversion logic was duplicated between `resolver.py` and `helpers.py` with inconsistent behavior. Specifically, boolean conversion was implemented differently in each file, leading to potential bugs and user confusion.

The duplication created several issues:
- Inconsistent conversion behavior across the codebase
- Maintenance burden when updating conversion logic
- Testing complexity - same logic needed to be tested in multiple places
- Potential for subtle bugs when behaviors diverged

## Decision

Create a centralized `type_conversion.py` module that serves as the single source of truth for all type conversion operations.

**Rationale:**
- Single source of truth for type conversion ensures consistent behavior across the entire codebase
- Easier to add new type conversion rules - update one module instead of multiple files
- Simplified testing - test one module instead of multiple duplicated implementations
- Reduced code duplication follows DRY (Don't Repeat Yourself) principle
- Makes the conversion logic more discoverable and maintainable

**Consequences:**
- [ ] Consistent boolean conversion across all parameter handling
- [ ] Easier to add new type conversion rules
- [ ] Simplified testing (test one module instead of multiple)
- [ ] Reduced code duplication
- [ ] Single place to update when conversion behavior needs to change

## Alternatives Considered

- **Keep duplicated code**: Maintenance burden, inconsistent behavior, potential for bugs
- **Use third-party conversion library**: Adds external dependency, may not match specific requirements

## Related

- Spec: specs/fix-integration-security-issues.md
- Files: src/seeknal/workflow/parameters/type_conversion.py
- Related ADRs: ADR-002 (Warning-based parameter collision handling)

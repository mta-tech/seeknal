---
adr_id: ADR-004
date: 2026-02-11
status: accepted
title: Parameter name validation with regex pattern
---

# ADR-004: Parameter name validation with regex pattern

## Context

Parameter names were not validated, allowing any string to be used. This created several problems:

1. **Security concern**: Invalid parameter names could potentially access system environment variables
2. **User confusion**: Names with special characters, hyphens, or leading digits were technically allowed but inconsistent with Python conventions
3. **API clarity**: No clear guidance on what constitutes a valid parameter name

Users could use names like:
- `my-param` (hyphens)
- `123param` (leading digits)
- `param!@#` (special characters)

## Decision

Validate parameter names using a regex pattern that enforces Python identifier syntax: names must start with a letter or underscore, followed by alphanumeric characters or underscores only.

**Pattern:** `^[a-zA-Z_][a-zA-Z0-9_]*$`

**Rationale:**
- Restricting parameter names to Python identifier conventions prevents confusion and potential security issues
- Consistent with Python naming conventions, making the API more intuitive
- Regex validation provides clear, enforceable rules with helpful error messages
- Prevents accidental access to system environment variables through malformed names
- Clear error messages guide users to fix invalid names

**Consequences:**
- [ ] Invalid parameter names raise `ValueError` with clear error messages
- [ ] Parameter names must follow Python identifier conventions
- [ ] Hyphens, special characters, and leading digits are rejected
- [ ] Breaking change: previously accepted invalid names will now fail
- [ ] Better API clarity and security

## Alternatives Considered

- **Allow any parameter name**: Security risk, confusing behavior, potential for bugs
- **Whitelist approach**: List allowed names - not scalable for user-defined parameters
- **Blacklist approach**: List disallowed names - cat-and-mouse game, always playing catch-up

## Related

- Spec: specs/fix-integration-security-issues.md
- Files: src/seeknal/workflow/parameters/helpers.py
- Related ADRs: ADR-002 (Warning-based parameter collision handling)

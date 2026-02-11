---
adr_id: ADR-001
date: 2026-02-11
status: accepted
title: Defense in depth: path sanitization AND validation
---

# ADR-001: Defense in depth: path sanitization AND validation

## Context

Critical path traversal vulnerability was discovered in FileStateBackend where `run_id` and `node_id` parameters were used directly in path construction without any validation or sanitization. This allowed attackers to use sequences like `../` to escape the base directory and potentially read or write files outside the intended state storage location.

The vulnerability affected methods like:
- `_get_run_state_path()`
- `_get_node_state_path()`
- `delete_run()`
- `list_nodes()`
- And other file operations

## Decision

Implement defense in depth by using BOTH path sanitization AND validation:

1. **Sanitization**: Remove dangerous sequences (`../`, `..\\`, `/`, `\\`) from user input before use
2. **Validation**: Use `is_insecure_path()` to reject insecure base paths at initialization

**Rationale:**
- Using both sanitization and validation provides multiple layers of protection against path traversal attacks
- Sanitization ensures user input is cleaned before use, removing dangerous sequences
- Validation at initialization prevents the use of known insecure directories (`/tmp`, `/var/tmp`, `/dev/shm`)
- Defense in depth means if one layer fails, the other still provides protection
- This approach follows security best practices for handling untrusted input

**Consequences:**
- [ ] Path traversal sequences (`../`, `..\\`, `/`, `\\`) are removed from user input
- [ ] Insecure base paths (`/tmp`, `/var/tmp`, `/dev/shm`) are rejected at initialization
- [ ] All file operations remain contained within the base directory
- [ ] Minor breaking change: `FileStateBackend.__init__()` raises `ValueError` for insecure paths
- [ ] More robust security against path traversal attacks

## Alternatives Considered

- **Sanitization only**: Less robust - edge cases could slip through sanitization logic
- **Validation only**: Insufficient - user input still needs cleaning before use
- **Path.resolve() with strict checking**: Alternative approach but less explicit about what's being blocked

## Related

- Spec: specs/fix-integration-security-issues.md
- Files: src/seeknal/state/file_backend.py, tests/state/test_file_backend.py
- Related ADRs: ADR-002 (Warning-based parameter collision handling)

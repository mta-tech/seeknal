---
title: Fix Integration Security and Functionality Issues
type: fix
date: 2026-02-11
status: ready
priority: critical
source: docs/plans/2026-02-11-fix-integration-security-issues-plan.md
---

# Plan: Fix Integration Security and Functionality Issues

## Overview

Address critical and important security/functionality issues identified during integration of `feat/iceberg-datasource-1` and `feat/iceberg-source-1` branches into `feat/add-sql-mesth-feat`.

**Risk Level:** High (includes critical path traversal vulnerability)

**Key Deliverables:**
- Path traversal vulnerability fix (Critical)
- Parameter name collision detection (Critical)
- Parameter name validation (Important)
- Type shadowing fix (Important)
- Consolidated type conversion (Important)
- Environment variable documentation (Important)
- Date range validation (Important)
- path_security.py integration (Suggestion)

**Architecture Note:**
The codebase has established security patterns:
- `src/seeknal/utils/path_security.py` - Path validation module
- `src/seeknal/validation.py` - SQL injection prevention
- `SEEKNAL_PARAM_` prefix convention for environment variables

## Task Description

Fix 8 security and functionality issues identified during integration review:

1. **Critical:** Path traversal vulnerability in `FileStateBackend` - `_get_run_state_path()` and `_get_node_state_path()` construct paths directly from user input without validation
2. **Important:** Parameter name collision - reserved context values (`run_id`, `run_date`) can be silently overridden by user parameters
3. **Important:** No validation of user parameter names - `get_param()` can access system environment variables without validation
4. **Important:** `type` parameter shadows Python built-in in `helpers.py`
5. **Important:** Inconsistent type conversion logic between resolver and helper
6. **Important:** Environment variable type loss - all values converted to strings
7. **Important:** Date parsing lacks robust validation - `fromisoformat()` accepts invalid dates
8. **Suggestion:** Missing `path_security.py` integration in new file backend

## Objective

### Primary Objectives
- Close critical path traversal vulnerability in `FileStateBackend`
- Prevent parameter name collisions with reserved system names
- Add parameter name validation with clear error messages
- Fix code quality issues (type shadowing, inconsistent conversion)

### Secondary Objectives
- Improve date validation robustness
- Consolidate type conversion logic
- Document environment variable limitations

### Success Metrics
- **Vulnerabilities closed:** 1 critical, 3 important
- **Test coverage:** Security tests added for all new validation
- **No regressions:** All existing tests pass

## Problem Statement

The integration review identified multiple security and code quality issues:

1. **Critical Path Traversal:** `FileStateBackend._get_run_state_path()` and `_get_node_state_path()` construct paths directly from user input (`run_id`, `node_id`) without validation, allowing potential `../` attacks to escape the base directory.

2. **Parameter Collision:** User parameters can silently override reserved context values like `run_id`, `run_date`, `project_id`, causing unexpected behavior.

3. **Missing Validation:** Parameter names are not validated, allowing access to arbitrary environment variables.

4. **Code Quality Issues:** Using `type` as parameter name shadows Python built-in, type conversion is inconsistent across modules.

## Proposed Solution

### Phase 1: Fix Path Traversal Vulnerability (Critical)

**File:** `src/seeknal/state/file_backend.py`

Integrate with existing `path_security.py` module and sanitize user input:

```python
from seeknal.utils.path_security import warn_if_insecure_path, is_insecure_path

class FileStateBackend(StateBackend):
    def __init__(self, base_path: Path):
        base_path = Path(base_path).expanduser().resolve()

        if is_insecure_path(str(base_path)):
            raise ValueError(
                f"Insecure state path detected: {base_path}. "
                f"Use a secure directory like ~/.local/share/seeknal"
            )

        self.base_path = base_path

    def _sanitize_run_id(self, run_id: str) -> str:
        """Remove path traversal sequences from run_id."""
        return run_id.replace("..", "").replace("/", "").replace("\\", "")

    def _get_run_state_path(self, run_id: str) -> Path:
        """Get file path for a run's state with security validation."""
        safe_run_id = self._sanitize_run_id(run_id)
        run_dir = self.base_path / safe_run_id
        return run_dir / "run_state.json"
```

### Phase 2: Fix Parameter Name Collision (Critical)

**File:** `src/seeknal/workflow/parameters/resolver.py`

Add reserved parameter names and emit warnings on collision:

```python
RESERVED_PARAM_NAMES = {"run_id", "run_date", "project_id", "workspace_path"}

def _resolve_user_parameters(self, cli_params: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve user parameters with collision detection."""
    resolved = {}

    for name, value in cli_params.items():
        if name in RESERVED_PARAM_NAMES:
            import warnings
            warnings.warn(
                f"Parameter '{name}' collides with reserved system name. "
                f"System value will take precedence. Use a different name."
            )
        resolved[name] = value

    return resolved
```

### Phase 3: Add Parameter Name Validation (Important)

**File:** `src/seeknal/workflow/parameters/helpers.py`

Validate parameter names are alphanumeric only:

```python
import re

PARAM_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

def get_param(
    name: str,
    default: Optional[Any] = None,
    param_type: Optional[Type[T]] = None,
) -> Any:
    """Get a resolved parameter value with type conversion."""

    # Validate parameter name
    if not PARAM_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid parameter name '{name}'. "
            f"Parameter names must be alphanumeric with underscores only."
        )

    # Rest of implementation...
```

### Phase 4: Fix `type` Parameter Shadowing (Important)

**File:** `src/seeknal/workflow/parameters/helpers.py`

Rename `type` parameter to `param_type`:

```python
def get_param(
    name: str,
    default: Optional[Any] = None,
    param_type: Optional[Type[T]] = None,  # Changed from 'type'
) -> Any:
    """Get a resolved parameter value with type conversion."""
    if param_type is not None:
        if param_type == bool:
            # ...
```

### Phase 5: Consolidate Type Conversion Logic (Important)

**Files:** `src/seeknal/workflow/parameters/type_conversion.py` (new)

Create shared type conversion utility:

```python
def convert_to_bool(value: Any) -> bool:
    """Convert value to boolean with consistent rules."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.lower().strip()
        if normalized in ('true', '1', 'yes', 'on'):
            return True
        if normalized in ('false', '0', 'no', 'off'):
            return False
    return bool(value)

def convert_to_type(value: Any, target_type: Type[T]) -> Any:
    """Convert value to target type with consistent rules."""
    if target_type == bool:
        return convert_to_bool(value)
    if target_type == int:
        return int(value)
    if target_type == float:
        return float(value)
    return str(value)
```

### Phase 6: Document Environment Variable Type Loss (Important)

**File:** `src/seeknal/workflow/executors/python_executor.py`

Add documentation explaining string conversion:

```python
"""
Environment Variable Type Handling:

All parameter values are passed as environment variables to Python scripts.
Due to environment variable limitations, all values are converted to strings.

For type preservation, consider:
1. Using JSON-encoded values: SEEKNAL_PARAM_CONFIG='{"key":"value"}'
2. Type-prefixed variables: SEEKNAL_PARAM_COUNT__type=int=100
"""
```

### Phase 7: Add Date Range Validation (Important)

**File:** `src/seeknal/cli/main.py`

Add robust date validation:

```python
from datetime import datetime, timedelta

def parse_date_safely(date_str: str, param_name: str) -> datetime:
    """Parse date with robust validation."""
    try:
        if "T" in date_str:
            dt = datetime.fromisoformat(date_str)
        else:
            dt = datetime.fromisoformat(f"{date_str}T00:00:00")

        # Validate reasonable range
        if dt.year < 2000 or dt.year > 2100:
            raise ValueError(f"Year out of valid range: {dt.year}")

        # Don't allow future dates beyond 1 year
        if dt > datetime.now() + timedelta(days=365):
            raise ValueError(f"Date too far in future: {dt}")

        return dt
    except ValueError as e:
        _echo_error(f"Invalid {param_name}: {e}")
        raise typer.Exit(1)
```

### Phase 8: Verify path_security.py Integration (Suggestion)

**File:** `src/seeknal/state/file_backend.py`

Ensure `warn_if_insecure_path()` is called for all path operations.

## Relevant Files

### Existing Files to Modify
- `src/seeknal/state/file_backend.py` - Path traversal fix
- `src/seeknal/workflow/parameters/resolver.py` - Parameter collision detection
- `src/seeknal/workflow/parameters/helpers.py` - Parameter validation, type rename
- `src/seeknal/workflow/executors/python_executor.py` - Documentation
- `src/seeknal/cli/main.py` - Date validation

### New Files to Create
- `src/seeknal/workflow/parameters/type_conversion.py` - Shared type conversion
- `tests/state/test_file_backend_security.py` - Security tests
- `tests/workflow/test_parameter_validation.py` - Parameter validation tests
- `tests/workflow/test_type_conversion.py` - Type conversion tests

### Existing Security Modules
- `src/seeknal/utils/path_security.py` - Path validation (to integrate)
- `src/seeknal/validation.py` - SQL injection prevention (reference)

## Implementation Phases

### Phase 1: Fix Path Traversal Vulnerability (Critical)

**Priority:** Critical - Security vulnerability

**Tasks:**

1.1. **Integrate path_security.py in FileStateBackend**
- Add `is_insecure_path()` check in `__init__`
- Create `_sanitize_run_id()` method
- Create `_sanitize_node_id()` method
- Update `_get_run_state_path()` to use sanitized run_id
- Update `_get_node_state_path()` to use sanitized node_id
- Add security error for insecure base paths

**Acceptance Criteria:**
- [ ] Path traversal sequences (`../`, `\`, `/`) are removed from user input
- [ ] Integration with `path_security.py` module
- [ ] Security error raised for insecure paths
- [ ] Unit tests added for path sanitization

**Files:**
- `src/seeknal/state/file_backend.py`
- `tests/state/test_file_backend_security.py`

**Success Criteria:**
- [ ] Path traversal attempts blocked
- [ ] Existing tests pass
- [ ] New security tests pass

---

### Phase 2: Fix Parameter Name Collision (Critical)

**Priority:** Critical - Prevents silent bugs

**Tasks:**

2.1. **Add Reserved Parameter Names Detection**
- Define `RESERVED_PARAM_NAMES` constant
- Update `_resolve_user_parameters()` to check for collisions
- Emit warning when collision detected
- Ensure system values take precedence

**Acceptance Criteria:**
- [ ] Warning emitted when collision detected
- [ ] System values take precedence over user params
- [ ] Reserved names documented in user-facing docs

**Files:**
- `src/seeknal/workflow/parameters/resolver.py`
- `docs/getting-started-comprehensive.md` (add documentation)

**Success Criteria:**
- [ ] Warnings emitted for collisions
- [ ] System values always win
- [ ] Documentation updated

---

### Phase 3: Add Parameter Name Validation (Important)

**Priority:** Important - Prevents confusion and bugs

**Tasks:**

3.1. **Add Parameter Name Pattern Validation**
- Define `PARAM_NAME_PATTERN` regex
- Add validation in `get_param()`
- Raise `ValueError` for invalid names
- Add tests for edge cases

**Acceptance Criteria:**
- [ ] Parameter names validated against pattern
- [ ] Error raised for invalid names
- [ ] Unit tests for invalid parameter names
- [ ] Error raised for reserved names like `PATH`, `HOME`

**Files:**
- `src/seeknal/workflow/parameters/helpers.py`
- `tests/workflow/test_parameter_validation.py`

**Success Criteria:**
- [ ] Invalid parameter names rejected
- [ ] Clear error messages
- [ ] All tests pass

---

### Phase 4: Fix `type` Parameter Shadowing (Important)

**Priority:** Important - Code quality

**Tasks:**

4.1. **Rename `type` to `param_type` in helpers.py**
- Rename parameter in `get_param()` signature
- Update all internal references
- Find and update all call sites
- Update type hints

**Acceptance Criteria:**
- [ ] Parameter renamed to `param_type`
- [ ] All references updated in call sites
- [ ] Tests pass

**Files:**
- `src/seeknal/workflow/parameters/helpers.py`
- Search for all call sites in codebase

**Success Criteria:**
- [ ] No Python built-in shadowing
- [ ] All tests pass
- [ ] No broken imports

---

### Phase 5: Consolidate Type Conversion Logic (Important)

**Priority:** Important - Consistency

**Tasks:**

5.1. **Create Shared Type Conversion Module**
- Create `type_conversion.py`
- Implement `convert_to_bool()` with consistent rules
- Implement `convert_to_type()` generic converter
- Update resolver to use new module
- Update helper to use new module

**Acceptance Criteria:**
- [ ] Shared utility module created
- [ ] Both resolver and helper use `convert_to_type()`
- [ ] Tests for consistent boolean conversion
- [ ] Boolean accepts: `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

**Files:**
- `src/seeknal/workflow/parameters/type_conversion.py` (new)
- `src/seeknal/workflow/parameters/resolver.py`
- `src/seeknal/workflow/parameters/helpers.py`
- `tests/workflow/test_type_conversion.py`

**Success Criteria:**
- [ ] Consistent type conversion across codebase
- [ ] All boolean representations work
- [ ] Tests pass

---

### Phase 6: Document Environment Variable Type Loss (Important)

**Priority:** Important - User expectations

**Tasks:**

6.1. **Add Documentation for Environment Variable Limitations**
- Add docstring explaining string conversion
- Document type preservation alternatives
- Update user-facing docs

**Acceptance Criteria:**
- [ ] Documentation added explaining string conversion
- [ ] Type preservation alternative documented
- [ ] User-facing docs updated

**Files:**
- `src/seeknal/workflow/executors/python_executor.py`
- `docs/getting-started-comprehensive.md`

**Success Criteria:**
- [ ] Clear documentation
- [ ] Alternatives documented

---

### Phase 7: Add Date Range Validation (Important)

**Priority:** Important - Prevents confusion from invalid dates

**Tasks:**

7.1. **Implement Robust Date Parsing**
- Create `parse_date_safely()` function
- Add year range validation (2000-2100)
- Add future date check (max 1 year ahead)
- Update CLI commands to use new function
- Add tests for edge cases

**Acceptance Criteria:**
- [ ] Year range validation (2000-2100)
- [ ] Future date check (max 1 year ahead)
- [ ] Clear error messages for invalid dates
- [ ] Tests for edge cases

**Files:**
- `src/seeknal/cli/main.py`
- `tests/cli/test_date_validation.py`

**Success Criteria:**
- [ ] Invalid dates rejected
- [ ] Clear error messages
- [ ] All tests pass

---

### Phase 8: Verify path_security.py Integration (Suggestion)

**Priority:** Suggestion - Ensure completeness

**Tasks:**

8.1. **Verify Complete path_security Integration**
- Review all path operations in FileStateBackend
- Ensure `warn_if_insecure_path()` called appropriately
- Add any missing security checks

**Acceptance Criteria:**
- [ ] All path operations validated
- [ ] Security warnings issued where appropriate
- [ ] Code review complete

**Files:**
- `src/seeknal/state/file_backend.py`

**Success Criteria:**
- [ ] Complete security coverage
- [ ] No unvalidated path operations

---

## Alternative Approaches Considered

### Path Sanitization vs Path Validation
- **Considered:** Using only `is_insecure_path()` validation
- **Chosen:** Both sanitization AND validation - defense in depth
- **Reasoning:** Sanitization removes obvious attacks, validation catches edge cases

### Parameter Collision: Error vs Warning
- **Considered:** Raising error for parameter collisions
- **Chosen:** Emitting warning and continuing
- **Reasoning:** Backward compatibility - existing workflows might have colliding names

### Type Conversion: Centralized vs Distributed
- **Considered:** Keeping type conversion in each module
- **Chosen:** Centralized in `type_conversion.py`
- **Reasoning:** Consistency, easier testing, single source of truth

## Team Orchestration

This is a security fix with 8 sequential phases. Phases 1-2 are critical and should be done first. Phases 3-7 are important but can be done in any order. Phase 8 is verification.

### Task Management Strategy

1. **Sequential Execution:** Each phase builds on security patterns - execute in priority order
2. **Testing First:** Write tests before implementing each fix
3. **Verification:** Run full test suite after each phase
4. **Critical Priority:** Phases 1-2 must be completed first

### Dependencies

```
Phase 1 (Critical) - No dependencies
    ↓
Phase 2 (Critical) - No dependencies
    ↓
Phase 3 (Important) - Depends on Phase 2
    ↓
Phase 4 (Important) - Depends on Phase 3
    ↓
Phase 5 (Important) - Depends on Phase 3, 4
    ↓
Phase 6 (Important) - No dependencies
    ↓
Phase 7 (Important) - No dependencies
    ↓
Phase 8 (Suggestion) - Depends on Phase 1
```

### Team Members

#### Security Specialist (builder-security)
- **Name:** builder-security
- **Role:** Security-focused Python developer
- **Agent Type:** general-purpose
- **Responsibilities:**
  - Path traversal vulnerability fix (Phase 1)
  - Parameter validation (Phase 3)
  - Security test coverage

#### Backend Engineer (builder-backend)
- **Name:** builder-backend
- **Role:** Backend Python developer
- **Agent Type:** general-purpose
- **Responsibilities:**
  - Parameter collision detection (Phase 2)
  - Type conversion consolidation (Phase 5)
  - Type shadowing fix (Phase 4)

#### Test Engineer (builder-test)
- **Name:** builder-test
- **Role:** Test-focused developer
- **Agent Type:** general-purpose
- **Responsibilities:**
  - Security tests for path traversal
  - Parameter validation tests
  - Type conversion tests
  - Date validation tests

#### Documentation Specialist (builder-docs)
- **Name:** builder-docs
- **Role:** Documentation maintainer
- **Agent Type:** general-purpose
- **Responsibilities:**
  - Environment variable documentation (Phase 6)
  - User-facing docs updates
  - Reserved parameter documentation

## Step by Step Tasks

### 1. Fix Path Traversal Vulnerability (Critical)
- **Task ID:** security-001
- **Depends On:** none
- **Assigned To:** builder-security
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Read `src/seeknal/state/file_backend.py`
2. Read `src/seeknal/utils/path_security.py` to understand security patterns
3. Add `is_insecure_path()` check in `__init__`
4. Create `_sanitize_run_id()` method removing `..`, `/`, `\`
5. Create `_sanitize_node_id()` method
6. Update `_get_run_state_path()` to use sanitized run_id
7. Update `_get_node_state_path()` to use sanitized node_id

**Tests to Create:**
- Test path traversal with `../` is blocked
- Test absolute paths are blocked
- Test valid run_ids work correctly
- Test invalid base_path raises error

### 2. Add Security Tests for Path Traversal
- **Task ID:** security-002
- **Depends On:** security-001
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Create `tests/state/test_file_backend_security.py`
2. Test `../` in run_id is sanitized
3. Test absolute paths in run_id are blocked
4. Test null bytes are handled
5. Test very long run_ids are handled
6. Test special characters are handled

### 3. Fix Parameter Name Collision (Critical)
- **Task ID:** security-003
- **Depends On:** none (can run parallel to security-001)
- **Assigned To:** builder-backend
- **Agent Type:** general-purpose
- **Parallel:** true (with security-001)

**Implementation:**
1. Read `src/seeknal/workflow/parameters/resolver.py`
2. Define `RESERVED_PARAM_NAMES = {"run_id", "run_date", "project_id", "workspace_path"}`
3. Update `_resolve_user_parameters()` to check for collisions
4. Emit `warnings.warn()` when collision detected
5. Ensure system values take precedence (resolve user params first, then overlay system params)

**Tests to Create:**
- Test collision warning is emitted
- Test system value takes precedence
- Test no warning when no collision

### 4. Add Parameter Name Validation (Important)
- **Task ID:** security-004
- **Depends On:** security-003
- **Assigned To:** builder-security
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Read `src/seeknal/workflow/parameters/helpers.py`
2. Define `PARAM_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")`
3. Add validation in `get_param()` before processing
4. Raise `ValueError` with clear message for invalid names

**Tests to Create:**
- Test valid names pass
- Test names starting with digit fail
- Test names with special characters fail
- Test empty string fails
- Test reserved names like `PATH` fail

### 5. Fix `type` Parameter Shadowing (Important)
- **Task ID:** refactor-001
- **Depends On:** security-004
- **Assigned To:** builder-backend
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Use Grep to find all occurrences of `get_param` calls
2. Rename `type` to `param_type` in `get_param()` signature
3. Update all internal references to `param_type`
4. Update all call sites to use `param_type=` keyword argument

**Tests to Create:**
- Test function works with renamed parameter
- Test backward compatibility (if needed)

### 6. Create Shared Type Conversion Module (Important)
- **Task ID:** refactor-002
- **Depends On:** refactor-001
- **Assigned To:** builder-backend
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Create `src/seeknal/workflow/parameters/type_conversion.py`
2. Implement `convert_to_bool()` accepting: `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`
3. Implement `convert_to_type()` generic converter
4. Update `resolver.py` to use new module
5. Update `helpers.py` to use new module
6. Remove duplicate conversion code

**Tests to Create:**
- Test `convert_to_bool()` with all variants
- Test `convert_to_type()` for int, float, str, bool
- Test case insensitivity
- Test whitespace handling

### 7. Document Environment Variable Type Loss (Important)
- **Task ID:** docs-001
- **Depends On:** none
- **Assigned To:** builder-docs
- **Agent Type:** general-purpose
- **Parallel:** true (with security-001)

**Implementation:**
1. Read `src/seeknal/workflow/executors/python_executor.py`
2. Add module docstring explaining string conversion
3. Document type preservation alternatives (JSON, type-prefixed)
4. Update `docs/getting-started-comprehensive.md` with parameterization section

### 8. Add Date Range Validation (Important)
- **Task ID:** validation-001
- **Depends On:** none
- **Assigned To:** builder-backend
- **Agent Type:** general-purpose
- **Parallel:** true (with security-001)

**Implementation:**
1. Read `src/seeknal/cli/main.py` to find date parsing
2. Create `parse_date_safely()` function
3. Add year range validation (2000-2100)
4. Add future date check (max 1 year ahead)
5. Update CLI commands to use new function
6. Add clear error messages

**Tests to Create:**
- Test valid dates pass
- Test invalid dates like `2024-13-45` fail
- Test years < 2000 fail
- Test years > 2100 fail
- Test future dates > 1 year fail
- Test ISO format with time works

### 9. Add Date Validation Tests
- **Task ID:** test-001
- **Depends On:** validation-001
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Create `tests/cli/test_date_validation.py`
2. Test all edge cases from validation-001
3. Test error messages are clear

### 10. Verify path_security.py Integration
- **Task ID:** verify-001
- **Depends On:** security-001
- **Assigned To:** builder-security
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Review all path operations in `FileStateBackend`
2. Ensure all paths use `warn_if_insecure_path()`
3. Add any missing security checks
4. Document security assumptions

### 11. Update Documentation for Reserved Parameters
- **Task ID:** docs-002
- **Depends On:** security-003
- **Assigned To:** builder-docs
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Update getting-started guide with reserved parameter names
2. Document what happens on collision
3. Add examples of proper parameter naming

### 12. Run Full Test Suite
- **Task ID:** test-002
- **Depends On:** security-002, security-004, refactor-002, test-001
- **Assigned To:** builder-test
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Run `pytest` full suite
2. Verify all existing tests pass
3. Verify all new tests pass
4. Report any failures

## Acceptance Criteria

### Functional Requirements

#### Security Fixes
- [ ] Path traversal vulnerability closed - `../` sequences removed from user input
- [ ] Parameter name collision prevented - warnings emitted, system values take precedence
- [ ] Parameter name validation added - alphanumeric with underscores only

#### Code Quality
- [ ] `type` parameter renamed to `param_type` - no Python built-in shadowing
- [ ] Type conversion logic consolidated - single `type_conversion.py` module
- [ ] Consistent boolean conversion - accepts `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

#### Validation
- [ ] Date range validation added - years 2000-2100, max 1 year future
- [ ] Environment variable limitations documented
- [ ] path_security.py integrated in FileStateBackend

### Non-Functional Requirements
- [ ] No breaking changes to existing APIs
- [ ] Clear error messages for all validation failures
- [ ] Backward compatibility maintained (warnings, not errors for collisions)

### Quality Gates
- [ ] All new security tests pass
- [ ] All existing tests pass (no regressions)
- [ ] Code follows existing patterns
- [ ] Documentation updated

## Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Critical vulnerabilities | 1 | 0 | Security review |
| Important issues | 7 | 0 | Issue tracking |
| Test coverage (security) | 0% | 90%+ | pytest coverage |
| Tests passing | N/A | 100% | pytest |

## Dependencies & Prerequisites

### External Dependencies
| Dependency | Version | Purpose | Risk |
|------------|---------|---------|------|
| None | - | Uses only stdlib and existing modules | Low |

### Internal Dependencies
| Dependency | Status | Notes |
|------------|--------|-------|
| `src/seeknal/utils/path_security.py` | Existing | Must be integrated |
| `src/seeknal/validation.py` | Existing | Reference for validation patterns |
| Phase 1 (Path traversal) | Must complete first | Critical security fix |
| Phase 2 (Collision) | Must complete early | Enables later phases |

## Risk Analysis & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Parameter validation breaks existing workflows | Medium | Medium | Add clear error messages and migration guide |
| Type conversion changes affect behavior | Low | Low | Document new behavior clearly |
| Path sanitization too restrictive | Low | Medium | Review edge cases in testing |
| Tests fail due to existing code assumptions | Medium | Low | Review and fix test assumptions |

## Resource Requirements

### Development Time Estimate
| Phase | Complexity | Estimate |
|-------|------------|----------|
| Phase 1: Path traversal | Medium | 2 hours |
| Phase 2: Parameter collision | Low | 1 hour |
| Phase 3: Parameter validation | Low | 1 hour |
| Phase 4: Type shadowing | Low | 1 hour |
| Phase 5: Type conversion | Medium | 2 hours |
| Phase 6: Env var docs | Low | 0.5 hours |
| Phase 7: Date validation | Low | 1 hour |
| Phase 8: Verification | Low | 0.5 hours |
| **Total** | | **9 hours** |

### Testing Time Estimate
| Task | Estimate |
|------|----------|
| Security tests | 2 hours |
| Parameter validation tests | 1 hour |
| Type conversion tests | 1 hour |
| Date validation tests | 1 hour |
| Full test suite run | 0.5 hours |
| **Total** | **5.5 hours** |

### Total Estimate: 14.5 hours

## Documentation Plan

| Document | Location | When |
|----------|----------|------|
| Reserved parameter names | `docs/getting-started-comprehensive.md` | After Phase 2 |
| Type conversion behavior | `docs/getting-started-comprehensive.md` | After Phase 5 |
| Environment variable limitations | `docs/getting-started-comprehensive.md` | After Phase 6 |
| Date format validation | `docs/getting-started-comprehensive.md` | After Phase 7 |

## Validation Commands

```bash
# Run all tests
pytest

# Run specific test files
pytest tests/state/test_file_backend_security.py
pytest tests/workflow/test_parameter_validation.py
pytest tests/workflow/test_type_conversion.py
pytest tests/cli/test_date_validation.py

# Run with coverage
pytest --cov=src/seeknal/state
pytest --cov=src/seeknal/workflow/parameters

# Security check - ensure no path traversal works
python -c "from seeknal.state.file_backend import FileStateBackend; ..."

# Check parameter validation
python -c "from seeknal.workflow.parameters.helpers import get_param; get_param('../etc/passwd')"
```

## Notes

### Implementation Notes
- Run phases in priority order (security first)
- Each phase can be tested independently
- Total estimated effort: 14.5 hours

### References
- Code review: `docs/reviews/integration-review-2026-02-11.md`
- Security patterns: `src/seeknal/utils/path_security.py`
- SQL validation: `src/seeknal/validation.py`
- Parameter handling: `src/seeknal/workflow/parameters/`

### Migration Notes
- No breaking changes to existing APIs
- New validation may raise errors for previously accepted invalid inputs
- Parameter collisions now emit warnings (previously silent)

---

## Checklist Summary

### Phase 1: Path Traversal (Critical) 🔴
- [x] 1. Fix Path Traversal Vulnerability (security-001)
- [x] 2. Add Security Tests for Path Traversal (security-002)
- [x] 10. Verify path_security.py Integration (verify-001)

### Phase 2: Parameter Collision (Critical) 🔴
- [x] 3. Fix Parameter Name Collision (security-003)
- [x] 11. Update Documentation for Reserved Parameters (docs-002)

### Phase 3: Parameter Validation (Important) 🟡
- [x] 4. Add Parameter Name Validation (security-004)

### Phase 4: Type Shadowing (Important) 🟡
- [x] 5. Fix `type` Parameter Shadowing (refactor-001)

### Phase 5: Type Conversion (Important) 🟡
- [x] 6. Create Shared Type Conversion Module (refactor-002)

### Phase 6: Env Var Documentation (Important) 🟡
- [x] 7. Document Environment Variable Type Loss (docs-001)

### Phase 7: Date Validation (Important) 🟡
- [x] 8. Add Date Range Validation (validation-001)
- [x] 9. Add Date Validation Tests (test-001)

### Final Validation
- [x] 12. Run Full Test Suite (test-002)

---

## Compounded

**Date:** 2026-02-11
**Status:** Complete
**Knowledge:** Documented and compounded

### Documentation Created

**Architecture Decision Records (4):**
- `docs/adr/adr-001-defense-in-depth-path-security.md` - Path sanitization AND validation
- `docs/adr/adr-002-warning-based-parameter-collision-handling.md` - Backward-compatible handling
- `docs/adr/adr-003-centralized-type-conversion-module.md` - Single source of truth
- `docs/adr/adr-004-parameter-name-validation-with-regex.md` - Python identifier validation

**Solutions (3):**
- `docs/solutions/security-vulnerabilities/path-traversal-file-state-backend.md` - CRITICAL security fix
- `docs/solutions/code-quality/python-built-in-shadowing-parameter-type.md` - Code quality improvement
- `docs/solutions/data-validation/date-parsing-validation-cli-commands.md` - Date validation enhancement

**Deployment:**
- `docs/deployment.md` - Updated with changelog for 2026-02-11

**Agent Guidelines:**
- `CLAUDE.md` - Updated with 8 security and code quality patterns

**Analysis:**
- `docs/analysis/task-analysis.json` - Structured task analysis
- `docs/analysis/security-fix-analysis.md` - Analysis summary
- `docs/analysis/compound-report.md` - Final compound report

### Key Learnings

1. **Defense in Depth:** Path sanitization AND validation provides multiple layers of protection
2. **Backward Compatibility:** Warning-based parameter collision handling maintains compatibility
3. **Code Quality:** Centralized type conversion ensures consistent behavior across codebase
4. **Validation:** Date parsing with domain-specific bounds (year range: 2000-2100, max 1 year future)
5. **Naming Conventions:** Parameter names must follow Python identifier conventions

### Breaking Changes

1. **FileStateBackend.__init__()** now raises ValueError for insecure paths
2. **Parameter names** with special characters now raise ValueError

**Migration Guide:**
- Set `SEEKNAL_BASE_CONFIG_PATH` to secure directory (not /tmp)
- Rename parameters with special characters to alphanumeric only

### Summary Metrics

| Metric | Count | Status |
|--------|-------|--------|
| Architecture Decisions | 4 | ✓ Complete |
| Solutions Documented | 3 | ✓ Complete |
| Patterns Added | 8 | ✓ Complete |
| Deployment Changes | 5 | ✓ Complete |
| Security Fixes | 1 (critical) | ✓ Resolved |

---

**Build Complete:** All tasks implemented, tested, documented, and compounded.

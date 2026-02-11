---
category: code-quality
component: parameter-helpers
tags: [code-quality, python-builtin-shadowing, naming-conventions, refactoring]
date_resolved: 2026-02-11
related_build: fix-integration-security-issues
related_tasks: [refactor-001]
---

# Python Built-in Shadowing in Parameter Name

## Problem Symptom

The `get_param()` function in `helpers.py` used `type` as a parameter name, which shadows Python's built-in `type()` function. This is a code quality anti-pattern that can cause confusion and subtle bugs.

**Issue:** Parameter named 'type' shadows Python built-in type()

**Impact:** Code confusion, potential for subtle bugs, violates PEP 8 naming conventions, makes code less readable.

## Investigation Steps

1. **Code review** - Identified shadowing issue during security-focused code review
2. **Grep search** - Used `grep -r "def.*type.*:"` to find all occurrences of 'type' as a parameter
3. **Call site analysis** - Checked all call sites to understand parameter usage
4. **Impact assessment** - Confirmed the shadowing was internal to the function and didn't affect external behavior

## Root Cause

Using 'type' as a parameter name shadows the Python built-in `type()` function. This violates PEP 8 naming conventions and creates confusion:

```python
# WRONG - 'type' shadows built-in
def get_param(name: str, type: Optional[Type[T]] = None) -> Any:
    if type is not None:  # Ambiguous - is this built-in or parameter?
        return convert_to_type(value, type)
```

When a parameter name shadows a built-in, you cannot access the built-in within that function's scope. While this specific case didn't cause bugs (the built-in wasn't needed), it's still an anti-pattern.

## Working Solution

Renamed the `type` parameter to `param_type` throughout the codebase:

### 1. Updated Function Signature

```python
# BEFORE (wrong)
def get_param(
    name: str,
    type: Optional[Type[T]] = None,
    default: Optional[T] = None,
    strict: bool = True
) -> Optional[T]:

# AFTER (correct)
def get_param(
    name: str,
    param_type: Optional[Type[T]] = None,
    default: Optional[T] = None,
    strict: bool = True
) -> Optional[T]:
```

### 2. Updated Internal References

```python
# BEFORE
if type is not None:
    return convert_to_type(value, type)
if default is not None:
    return convert_to_type(default, type)  # Confusing!

# AFTER
if param_type is not None:
    return convert_to_type(value, param_type)
if default is not None:
    return convert_to_type(default, param_type)  # Clear!
```

### 3. Updated Docstring

```python
"""
Get a parameter value from environment with optional type conversion.

Args:
    name: Parameter name (will be uppercased for env lookup)
    param_type: Optional target type for conversion (bool, int, float, str)
    default: Optional default value if parameter not found
    strict: If True, raise error when parameter not found (default: True)

Returns:
    The parameter value, optionally converted to the specified type

Raises:
    ValueError: If parameter not found and strict=True (or no default provided)
"""
```

**Result:** No Python built-in shadowing, clearer intent, better code quality.

## Prevention Strategies

1. **Code review checklist** - Check for shadowing of Python built-ins during review
2. **Enable linters** - Use pylint with `builtin-shadowing` warning or flake8 with `shadowing` plugin
3. **Know the built-ins** - Be familiar with common Python built-ins to avoid: `list`, `dict`, `set`, `tuple`, `str`, `int`, `float`, `bool`, `type`, `id`, `input`, `open`, `eval`, `exec`, `object`, `None`, `True`, `False`
4. **Use descriptive names** - Instead of `type`, use `param_type`, `data_type`, `value_type`, etc.
5. **IDE warnings** - Enable Python IDE warnings for built-in shadowing (PyCharm, VSCode both support this)

## Test Cases Added

While this was a straightforward refactor with no behavior change, existing tests verify the refactoring didn't break functionality:

```python
def test_get_param_with_type_conversion_bool():
    """Test boolean type conversion works after rename."""
    os.environ['TEST_BOOL'] = 'true'
    result = get_param('test_bool', param_type=bool)
    assert result is True

def test_get_param_with_type_conversion_int():
    """Test integer type conversion works after rename."""
    os.environ['TEST_INT'] = '42'
    result = get_param('test_int', param_type=int)
    assert result == 42

def test_get_param_default_value_with_type():
    """Test default value with type conversion works after rename."""
    result = get_param('nonexistent', param_type=int, default=100)
    assert result == 100
```

## Cross-References

- Related: `src/seeknal/workflow/parameters/helpers.py` - Fixed implementation
- Related: `src/seeknal/workflow/parameters/type_conversion.py` - Type conversion module
- PEP 8 Style Guide: https://peps.python.org/pep-0008/#descriptive-naming-styles
- pylint builtin-shadowing: https://pylint.pycqa.org/en/latest/user_guide/messages/error/shadow-built-in.html

## Related Patterns

This fix aligns with the "Avoid Python built-in shadowing" pattern documented in the task analysis:

```python
# CORRECT
def get_param(name: str, param_type: Optional[Type[T]] = None) -> Any:
    if param_type is not None:
        return convert_to_type(value, param_type)

# WRONG
def get_param(name: str, type: Optional[Type[T]] = None) -> Any:
    if type is not None:
        return convert_to_type(value, type)  # 'type' shadows built-in
```

## Status

**Fully resolved** - Parameter renamed to `param_type`, all references updated, existing tests pass.

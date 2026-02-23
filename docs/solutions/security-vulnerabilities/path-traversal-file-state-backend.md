---
category: security-vulnerabilities
component: file-state-backend
tags: [security, path-traversal, file-operations, sanitization, validation]
date_resolved: 2026-02-11
related_build: fix-integration-security-issues
related_tasks: [security-001]
---

# Path Traversal Vulnerability in FileStateBackend

## Problem Symptom

FileStateBackend constructed file paths directly from user input without validation or sanitization. Attackers could use path traversal sequences (`../`, `..\\`) to escape the base directory and access arbitrary files on the system.

**Vulnerability:** Critical - Path traversal allowing arbitrary file read/write outside intended directory

**Impact:** Potential data breach, unauthorized file access, data corruption, complete system compromise depending on file permissions.

## Investigation Steps

1. **Code review** - Identified direct string concatenation of `run_id` and `node_id` parameters into file paths in `_get_run_state_path()` and `_get_node_state_path()` methods
2. **Vulnerability testing** - Tested with `../` sequences - confirmed paths could escape base directory and access parent directories
3. **Pattern analysis** - Reviewed existing `path_security.py` module for integration patterns and security best practices
4. **Attack surface mapping** - Identified all methods using unsanitized IDs: `delete_run()`, `list_nodes()`, and other file operations

## Root Cause

`run_id` and `node_id` parameters from user input were used directly in path construction without sanitization. The vulnerable code pattern:

```python
# VULNERABLE - Direct path construction from user input
def _get_run_state_path(self, run_id: str) -> Path:
    return self.base_path / run_id  # No validation!

def _get_node_state_path(self, run_id: str, node_id: str) -> Path:
    run_path = self._get_run_state_path(run_id)
    return run_path / node_id  # No validation!
```

This allows attackers to supply `run_id = "../../../etc/passwd"` to escape the base directory.

## Working Solution

Implemented defense-in-depth approach with both sanitization and validation:

### 1. Sanitization Methods

Added methods to remove dangerous sequences from user input:

```python
def _sanitize_run_id(self, run_id: str) -> str:
    """Remove path traversal sequences from run_id."""
    sanitized = re.sub(r'\.\.[/\\]', '', run_id)  # Remove ../ or ..\
    sanitized = re.sub(r'[/\\]', '', sanitized)   # Remove remaining slashes
    sanitized = re.sub(r'\.\.', '', sanitized)     # Remove any remaining ..
    return sanitized or "default"  # Ensure non-empty

def _sanitize_node_id(self, node_id: str) -> str:
    """Remove path traversal sequences from node_id."""
    sanitized = re.sub(r'\.\.[/\\]', '', node_id)
    sanitized = re.sub(r'[/\\]', '', sanitized)
    sanitized = re.sub(r'\.\.', '', sanitized)
    return sanitized or "default"
```

### 2. Base Path Validation

Added security check at initialization to reject insecure base paths:

```python
from seeknal.utils.path_security import is_insecure_path

def __init__(self, base_path: Path):
    base_path = Path(base_path).expanduser().resolve()

    if is_insecure_path(str(base_path)):
        raise ValueError(
            f"Insecure base path detected: '{base_path}'. "
            "Use a secure location such as ~/.seeknal/state"
        )

    self.base_path = base_path
    self.base_path.mkdir(parents=True, exist_ok=True)
```

### 3. Updated Path Construction

Modified all path construction methods to use sanitized IDs:

```python
def _get_run_state_path(self, run_id: str) -> Path:
    sanitized_id = self._sanitize_run_id(run_id)
    return self.base_path / sanitized_id

def _get_node_state_path(self, run_id: str, node_id: str) -> Path:
    sanitized_run_id = self._sanitize_run_id(run_id)
    sanitized_node_id = self._sanitize_node_id(node_id)
    return self.base_path / sanitized_run_id / f"{sanitized_node_id}.json"
```

**Result:** Path traversal attempts blocked, all file operations contained within base directory, insecure paths rejected at initialization.

## Prevention Strategies

1. **Never trust user input in file paths** - Always sanitize and validate before using in file operations
2. **Use path_security.is_insecure_path()** - Validate all paths, especially base directories
3. **Defense in depth** - Apply both sanitization AND validation (not just one)
4. **Security code review checklist** - Review all file operations for path traversal vulnerabilities
5. **Test with malicious inputs** - Include tests with `../`, `..\\`, absolute paths, and null bytes
6. **Principle of least privilege** - Use the most restrictive file permissions possible

## Test Cases Added

```python
def test_sanitize_run_id_parent_traversal():
    """Test that ../ sequences are removed from run_id."""
    backend = FileStateBackend(base_path=temp_dir)
    result = backend._sanitize_run_id("../../../etc/passwd")
    assert result == "etcpasswd"  # Traversal sequences removed

def test_sanitize_node_id_parent_traversal():
    """Test that ..\\ sequences are removed from node_id."""
    backend = FileStateBackend(base_path=temp_dir)
    result = backend._sanitize_node_id("..\\..\\windows\\system32")
    assert result == "windowssystem32"  # Traversal sequences removed

def test_insecure_base_path_rejects_tmp():
    """Test that /tmp is rejected as insecure base path."""
    with pytest.raises(ValueError, match="Insecure base path"):
        FileStateBackend(base_path=Path("/tmp"))

def test_path_traversal_cannot_read_files_outside_base():
    """Test that path traversal cannot escape base directory."""
    backend = FileStateBackend(base_path=temp_dir)
    # Attempt to read file outside base directory
    result = backend.load_run_state("../../../etc/passwd")
    assert result is None  # Should not escape base dir
```

## Cross-References

- Related: `src/seeknal/utils/path_security.py` - Path security utilities
- Related: `src/seeknal/state/file_backend.py` - Fixed implementation
- OWASP Path Traversal: https://owasp.org/www-community/attacks/Path_Traversal
- CWE-22: Improper Limitation of a Pathname to a Restricted Directory ('Path Traversal')

## Architecture Impact

Minor breaking change: `FileStateBackend.__init__()` now raises `ValueError` for insecure base paths. Users must set `SEEKNAL_BASE_CONFIG_PATH` to a secure directory like `~/.seeknal/state` instead of `/tmp`.

## Status

**Fully resolved** - All path traversal vectors blocked with defense-in-depth approach.

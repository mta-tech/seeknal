# Gotchas & Pitfalls

Things to watch out for in this codebase.

## [2026-01-05 09:46]
mkdocs build --strict fails due to griffe warnings about missing type annotations in source code. These are informational warnings indicating the source code lacks Python type hints (e.g., `def foo() -> str`) while docstrings document types. The build itself succeeds without --strict. To pass --strict mode, all source code functions would need return type annotations added - a major refactoring effort outside documentation scope.

_Context: Documentation build for API reference using mkdocstrings with griffe parser. The source code uses Google-style docstrings with inline type documentation, but lacks Python 3 type annotations in function signatures._

## [2026-01-05 10:26]
Test suite requires specific environment setup - CLI tests need database connections, DuckDB tests need fixture files, and PySpark tests require distutils (removed in Python 3.12+). 258/274 tests pass in documentation branch without additional setup.

_Context: Running subtask-9-3 verification to ensure no regressions from docstring changes. Test failures are pre-existing environment issues, not caused by documentation work._

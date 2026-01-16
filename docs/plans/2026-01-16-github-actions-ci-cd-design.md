# GitHub Actions CI/CD Design for Seeknal

**Date:** 2026-01-16
**Status:** Approved
**Author:** Claude Code

## Overview

Design a complete CI/CD pipeline for seeknal using GitHub Actions, inspired by the [langchain-ai/deepagents](https://github.com/langchain-ai/deepagents) implementation, adapted for seeknal's single-package structure.

## Goals

1. **Automated Testing** - Run pytest on all PRs and pushes
2. **Code Quality** - Enforce ruff linting and mypy type checking
3. **Version Validation** - Ensure version bumps on release PRs
4. **Automated Releases** - Publish to PyPI on merges to main

## Architecture

### Workflow Structure

```
.github/
├── workflows/
│   ├── ci.yml          # Main orchestrator
│   ├── _lint.yml       # Reusable lint job
│   ├── _test.yml       # Reusable test job
│   └── release.yml     # Auto-publish on main
└── scripts/
    └── check_version_bump.py  # Version validation
```

### Key Differences from DeepAgents

| Aspect | DeepAgents | Seeknal |
|--------|-----------|---------|
| Structure | Monorepo (3+ packages) | Single package |
| Python | 3.11, 3.12, 3.13 | 3.11 only |
| Build | uv build | Hatchling |
| Linting | ruff | ruff + mypy |
| Release | Manual trigger | Auto on main merge |
| Version Check | None | Custom script |

## Workflows

### 1. CI Workflow (`ci.yml`)

**Triggers:** Pull requests, pushes to main, merge groups

**Jobs:**

| Job | Purpose |
|-----|---------|
| `lint` | Runs _lint.yml reusable workflow |
| `test` | Runs _test.yml reusable workflow |
| `version-check` | Validates version bump for releases |
| `ci-success` | Gatekeeper job ensuring all pass |

**Concurrency:** Cancels redundant runs via `concurrency.group`

### 2. Lint Workflow (`_lint.yml`)

**Reusable workflow** called by `ci.yml`

**Jobs:**

| Job | Tools |
|-----|-------|
| `ruff` | ruff check, black --check |
| `mypy` | mypy src/seeknal/ |

**Timeout:** 10-15 minutes per job

**Cache:** pip cache for dependencies

### 3. Test Workflow (`_test.yml`)

**Reusable workflow** called by `ci.yml`

**Job:** `pytest`

**Commands:**
```bash
pytest --cov=seeknal --cov-report=xml --cov-report=term-missing
```

**Coverage:** Uploads to Codecov (optional)

**Timeout:** 30 minutes

### 4. Release Workflow (`release.yml`)

**Triggers:** Push to main, manual dispatch

**Jobs:**

| Job | Purpose |
|-----|---------|
| `build` | Build package, extract version |
| `publish` | Publish to PyPI (trusted publishing) |
| `create-release` | Create GitHub release with notes |

**Permissions:** `contents:write`, `id-token:write`

## Version Bump Check

### Script: `scripts/check_version_bump.py`

Validates that `pyproject.toml` version is greater than the latest git tag.

**Logic:**
1. Read version from `pyproject.toml`
2. Fetch latest `v*` tag from git
3. Fail if current version <= latest tag
4. Pass if no tags exist (first release)

## Dependencies

### Required in `pyproject.toml`

```toml
[project.optional-dependencies]
dev = [
    "black>=24.10.0",
    "pytest>=8.3.4",
    "ruff>=0.8.0",
    "mypy>=1.0.0",
    "build>=0.10.0",
]
```

## PyPI Trusted Publishing

The release workflow uses [Trusted Publishing](https://blog.pypi.org/posts/2023-04-20-introducing-trusted-publishers/) instead of API tokens.

**Required PyPI Configuration:**
1. Go to package on PyPI
2. Settings → Publishing
3. Add GitHub Actions publisher:
   - Workflow: `release.yml`
   - Repository: `mta-tech/seeknal`

## Implementation Order

1. Update `pyproject.toml` with lint dependencies
2. Create `.github/workflows/` directory structure
3. Write `_lint.yml` (test independently first)
4. Write `_test.yml` (test independently first)
5. Write `ci.yml` (orchestrator)
6. Create `scripts/check_version_bump.py`
7. Write `release.yml` (test on feature branch first)
8. Configure PyPI trusted publishing

## Testing Strategy

1. **Test workflows locally** using [act](https://github.com/nektos/act)
2. **Test on feature branch** before enabling on main
3. **Verify version check** with test tags
4. **Test release flow** on TestPyPI first

## Success Criteria

- [ ] All tests pass on PRs
- [ ] Ruff and mypy enforce code quality
- [ ] Version bump check catches missing bumps
- [ ] Release workflow publishes to PyPI
- [ ] GitHub releases created automatically

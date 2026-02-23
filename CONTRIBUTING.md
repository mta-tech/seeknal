# Contributing to Seeknal

Thank you for your interest in contributing to Seeknal! This guide will help you get started.

## Quick Start (First-Time Contributors)

1. Fork the repository and clone your fork
2. Set up your development environment:

```bash
git clone https://github.com/<your-username>/seeknal.git
cd seeknal
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e ".[all]"
```

3. Create a branch for your changes:

```bash
git checkout -b feat/your-feature-name
```

4. Make your changes, add tests, and verify:

```bash
pytest src/tests/
```

5. Submit a pull request against `main`.

## AI-Assisted Development Policy

We embrace AI-assisted development. If you used AI tools (Claude, Copilot, ChatGPT, etc.) to develop your contribution, you **must** include the following in your PR description:

### Required for AI-assisted PRs

- **The prompt or spec file** that guided the AI. This can be:
  - The initial prompt you gave the AI
  - A spec/plan document (paste inline or link to the file)
  - A summary of the iterative prompts used

- **Which AI tool** was used (e.g., "Claude Code", "GitHub Copilot", "ChatGPT")

- **What was AI-generated vs. human-written** — A brief note on what the AI produced and what you modified or wrote yourself

### Why we require this

- **Reproducibility**: Others can understand and verify the approach
- **Knowledge sharing**: Good prompts are reusable by the team
- **Review quality**: Reviewers can assess whether the AI was given correct context
- **Accountability**: The contributor is responsible for all code, regardless of how it was generated

### PR description template for AI-assisted work

```markdown
## Summary
[What this PR does]

## AI Assistance
- **Tool**: Claude Code / GitHub Copilot / ChatGPT / Other
- **Prompt/Spec**: [Paste the prompt or link to the spec file]
- **Human modifications**: [What you changed from the AI output]

## Test plan
- [ ] Tests added/updated
- [ ] All tests pass locally
```

### For non-AI contributions

No special requirements — use the standard PR template. Just describe what changed and why.

## Development Setup

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Full Setup

```bash
# Clone and install
git clone https://github.com/mta-tech/seeknal.git
cd seeknal
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e ".[all]"

# Copy configuration
cp .env.example .env
mkdir -p ~/.seeknal
cp config.toml.example ~/.seeknal/config.toml

# Verify
seeknal --help
pytest src/tests/
```

## Code Style

- **Formatter**: Black (line length 100)
- **Type hints**: Required for public APIs
- **Docstrings**: Google style
- **Naming**: No shadowing of Python built-ins (`type`, `list`, `id`, `input`)

Run formatting before committing:

```bash
black src/seeknal --line-length 100
```

## Testing

We use pytest with three tiers:

| Tier | Location | When to run | What it tests |
|------|----------|-------------|---------------|
| **Unit** | `src/tests/` | Every PR | Core logic, no external deps |
| **Integration** | `src/tests/` (marked) | When touching DB/connections | Database, connections |
| **E2E** | `src/tests/e2e/` | Major features | Full pipeline execution |

```bash
# Run all unit tests
pytest src/tests/

# Run specific test file
pytest src/tests/workflow/test_env_aware.py

# Run E2E tests (requires infrastructure)
pytest src/tests/e2e/ -m e2e
```

New features **must** include tests. Bug fixes **should** include a regression test.

## Pull Request Guidelines

### Before submitting

- [ ] Your branch is up to date with `main`
- [ ] All tests pass locally
- [ ] Code follows the project's style conventions
- [ ] New features include tests
- [ ] Documentation is updated if behavior changes
- [ ] AI-assisted contributions include the prompt/spec (see above)

### PR process

1. Submit your PR against `main`
2. Fill in the PR description (include AI disclosure if applicable)
3. Expect a review within **2-3 business days**
4. Address review feedback
5. PRs are squash-merged

### Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: Add multi-target materialization
fix: Resolve race condition in parallel execution
docs: Update REPL auto-registration guide
refactor: Extract profile loader into separate module
chore: Update dependencies
```

## Reporting Issues

- Use [GitHub Issues](https://github.com/mta-tech/seeknal/issues) for bugs and feature requests
- Search existing issues before creating a new one
- For bugs: include steps to reproduce, expected vs actual behavior, and your environment (Python version, OS)

## License

By contributing to Seeknal, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).

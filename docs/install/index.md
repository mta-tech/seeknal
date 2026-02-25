# Install Seeknal

Get Seeknal up and running on your machine in under 5 minutes.

---

## Prerequisites

| Requirement | Minimum Version | Check Command |
|-------------|-----------------|---------------|
| Python | 3.11 or higher | `python --version` |
| pip | Latest | `pip --version` |
| uv (recommended) | Latest | `uv --version` |

### Check Your Python Version

```bash
python --version
# Should show Python 3.11.x or higher
```

**If you see Python 3.10 or lower**: Upgrade to Python 3.11+ first. See [Python Installation](https://www.python.org/downloads/).

---

## Installation

### From PyPI (Recommended)

```bash
pip install seeknal
```

This installs Seeknal and all required dependencies.

### From GitHub Releases

If you need a specific version or pre-release build:

```bash
# Visit: https://github.com/mta-tech/seeknal/releases
# Download the .whl file, then:
pip install seeknal-<version>-py3-none-any.whl
```

### Using uv

[uv](https://docs.astral.sh/uv/) is a fast Python package manager:

```bash
uv pip install seeknal
```

---

## Verify Installation

Test that Seeknal is installed correctly:

```bash
# Check Seeknal version
seeknal --version

# Test Python import
python -c "from seeknal.tasks.duckdb import DuckDBTask; print('✓ Seeknal installed successfully!')"
```

**Expected output:**
```
Seeknal 2.1.0
✓ Seeknal installed successfully!
```

---

## Optional Configuration

Seeknal works out of the box — no configuration required for basic usage.

For advanced usage (custom config location, team environments), you can override defaults:

```bash
# Optional: Override default config directory (default: ~/.seeknal/)
export SEEKNAL_BASE_CONFIG_PATH="$HOME/.seeknal"

# Optional: Override user config file (default: ~/.seeknal/config.toml)
export SEEKNAL_USER_CONFIG_PATH="$HOME/.seeknal/config.toml"
```

Seeknal auto-creates `~/.seeknal/` and its SQLite database on first use.

---

## Installation Failed?

If you encounter any issues during installation, see the [Troubleshooting Guide](./troubleshooting.md) for solutions to common problems:

- Python version issues
- Permission errors
- Build tool errors (Windows)
- OpenSSL issues (macOS)
- Missing dependencies (Linux)

---

## What's Next?

Now that Seeknal is installed, you're ready to start building:

1. **[Quick Start](../quick-start/)** — Run your first pipeline in 10 minutes
2. **[Choose Your Path](../getting-started/)** — Select a learning path for your role
3. **[CLI Reference](../reference/cli.md)** — Explore all available commands

---

**Need help?** [Troubleshooting](./troubleshooting.md) | [GitHub Issues](https://github.com/mta-tech/seeknal/issues) | [Community Discussions](https://github.com/mta-tech/seeknal/discussions)

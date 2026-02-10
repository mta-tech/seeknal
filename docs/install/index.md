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

## Installation Methods

### Method 1: Using uv (Recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package manager. This is the recommended approach.

#### Step 1: Install uv

**macOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows (PowerShell):**
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Step 2: Create Virtual Environment

```bash
# Create environment with Python 3.11
uv venv --python 3.11

# Activate the environment
source .venv/bin/activate  # Linux/macOS
.\\.venv\\Scripts\\activate  # Windows
```

#### Step 3: Install Seeknal

```bash
# Download the latest wheel from GitHub releases
# Visit: https://github.com/mta-tech/seeknal/releases

# Install Seeknal
uv pip install seeknal-<version>-py3-none-any.whl
```

---

### Method 2: Using pip

If you prefer standard pip:

#### Step 1: Create Virtual Environment

```bash
# Create environment
python -m venv .venv

# Activate the environment
source .venv/bin/activate  # Linux/macOS
.\\.venv\\Scripts\\activate  # Windows
```

#### Step 2: Upgrade pip

```bash
pip install --upgrade pip
```

#### Step 3: Install Seeknal

```bash
# Download the latest wheel from GitHub releases
# Visit: https://github.com/mta-tech/seeknal/releases

# Install Seeknal
pip install seeknal-<version>-py3-none-any.whl
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
Seeknal 2.0.0
✓ Seeknal installed successfully!
```

---

## Optional Configuration

For advanced usage, configure environment variables:

```bash
# Create config directory
mkdir -p ~/.seeknal

# Set environment variables (add to ~/.bashrc or ~/.zshrc)
export SEEKNAL_BASE_CONFIG_PATH="$HOME/.seeknal"
export SEEKNAL_USER_CONFIG_PATH="$HOME/.seeknal/config.toml"
```

**Note**: For basic usage, configuration is not required. Seeknal works out of the box with sensible defaults.

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

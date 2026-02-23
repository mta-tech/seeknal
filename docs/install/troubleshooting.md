# Installation Troubleshooting

**Installation failed?** Don't worry — most installation issues have simple fixes. This guide covers the top 5 installation failures and their solutions.

---

## Quick Diagnosis

Before diving into specific errors, try these quick checks:

```bash
# Check your Python version (must be 3.11+)
python --version

# Check which Python you're using
which python

# Check if you're in a virtual environment
echo $VIRTUAL_ENV
```

If Python version is <3.11, upgrade first: [Python Installation Guide](#python-version-issues)

---

## Error 1: Python Version Issues

### Symptom

```
ERROR: Package 'seeknal' requires a different Python: 3.10.x not in '>=3.11'
```

### Cause

Seeknal requires Python 3.11 or higher. You're running an older version.

### Solution

#### Option 1: Install Python 3.11+ (Recommended)

**macOS (using Homebrew):**
```bash
brew install python@3.11
python3.11 --version
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install python3.11
python3.11 --version
```

**Windows:**
- Download from [python.org](https://www.python.org/downloads/)
- During installation, check "Add Python to PATH"

#### Option 2: Use pyenv (macOS/Linux)

```bash
# Install pyenv
brew install pyenv  # macOS
# or
curl https://pyenv.run | bash  # Linux

# Install Python 3.11
pyenv install 3.11

# Set as local version for this project
pyenv local 3.11

# Verify
python --version
```

#### Option 3: Use conda

```bash
# Create environment with Python 3.11
conda create -n seeknal python=3.11
conda activate seeknal

# Verify
python --version
```

---

## Error 2: Permission Errors

### Symptom

```
ERROR: Could not install packages due to an EnvironmentError: [Errno 13] Permission denied
```

### Cause

Trying to install to system Python without proper permissions.

### Solution

#### Option 1: Use Virtual Environment (Recommended)

```bash
# Create a virtual environment
python -m venv .venv

# Activate it
source .venv/bin/activate  # Linux/macOS
.\\.venv\\Scripts\\activate  # Windows

# Now install Seeknal
pip install seeknal-<version>-py3-none-any.whl
```

#### Option 2: Use User Install

```bash
# Install to user directory (no sudo needed)
pip install --user seeknal-<version>-py3-none-any.whl

# Add user bin to PATH (add to ~/.bashrc or ~/.zshrc)
export PATH="$HOME/.local/bin:$PATH"
```

#### Option 3: Fix System Permissions (Not Recommended)

```bash
# Only if you must install system-wide
sudo chown -R $USER /usr/local/lib/python3.11/site-packages
```

---

## Error 3: Build Tool Errors (Windows)

### Symptom

```
error: Microsoft Visual C++ 14.0 or greater is required.
```

### Cause

Some Python packages require C compilation on Windows.

### Solution

#### Option 1: Install Pre-built Wheel (Recommended)

Seeknal distributes pre-built wheels. Make sure you're downloading the `.whl` file:

```bash
# Download from https://github.com/mta-tech/seeknal/releases
# Install the wheel file directly
pip install seeknal-<version>-py3-none-any.whl
```

#### Option 2: Install Visual Studio Build Tools

If you need to build from source:

1. Download [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
2. Install "Desktop development with C++"
3. Re-run installation

---

## Error 4: OpenSSL Issues (macOS)

### Symptom

```
Error: OpenSSL 1.1.1 not found
```

or

```
ssl.SSLCertVerificationError
```

### Cause

OpenSSL library issues on macOS, especially with Homebrew Python.

### Solution

#### Option 1: Fix OpenSSL Paths

```bash
# Set OpenSSL paths
export LDFLAGS="-L$(brew --prefix openssl)/lib"
export CPPFLAGS="-I$(brew --prefix openssl)/include"
export PKG_CONFIG_PATH="$(brew --prefix openssl)/lib/pkgconfig"

# Reinstall Python
brew reinstall python@3.11
```

#### Option 2: Use System Python

```bash
# Use macOS system Python instead of Homebrew
/usr/bin/python3 --version

# Create virtual environment with system Python
/usr/bin/python3 -m venv .venv
source .venv/bin/activate
```

#### Option 3: Use pyenv

```bash
# pyenv handles OpenSSL correctly
brew install pyenv
pyenv install 3.11
pyenv local 3.11
```

---

## Error 5: Missing Build Dependencies (Linux)

### Symptom

```
error: command 'gcc' failed: No such file or directory
```

or

```
fatal error: Python.h: No such file or directory
```

### Cause

Missing Python development headers and build tools.

### Solution

#### Ubuntu/Debian

```bash
sudo apt update
sudo apt install -y build-essential python3-dev python3.11-venv
```

#### CentOS/RHEL/Fedora

```bash
sudo dnf install -y gcc python3-devel python3.11-venv
```

#### Arch Linux

```bash
sudo pacman -S base-devel python
```

---

## Verification

After fixing the issue, verify your installation:

```bash
# Check Seeknal is installed
pip show seeknal

# Test import
python -c "from seeknal.tasks.duckdb import DuckDBTask; print('✓ Seeknal installed successfully!')"

# Check version
seeknal --version
```

**Expected output:**
```
✓ Seeknal installed successfully!
Seeknal version 2.0.0
```

---

## Still Having Issues?

### 1. Check the Logs

Enable verbose output to see what's happening:

```bash
pip install -v seeknal-<version>-py3-none-any.whl
```

### 2. Try a Clean Install

```bash
# Remove old installation
pip uninstall seeknal

# Create fresh virtual environment
python -m venv .venv
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install Seeknal
pip install seeknal-<version>-py3-none-any.whl
```

### 3. Search GitHub Issues

[Search existing issues](https://github.com/mta-tech/seeknal/issues) — someone may have solved your problem already.

### 4. Open a New Issue

If nothing works, [open an issue on GitHub](https://github.com/mta-tech/seeknal/issues/new) with:

- **Your OS**: Windows/macOS/Linux (which version?)
- **Python version**: `python --version`
- **pip version**: `pip --version`
- **Error message**: Full error output
- **Installation method**: pip or uv

### 5. Community Help

- [GitHub Discussions](https://github.com/mta-tech/seeknal/discussions) — Ask the community
- Check if your question has been answered before posting

---

## Prevention Tips

**Best practices to avoid installation issues:**

1. **Always use virtual environments** — Keeps projects isolated
2. **Keep Python updated** — Use latest 3.11+ or 3.12+
3. **Use uv for faster installs** — More reliable than pip
4. **Read error messages carefully** — They usually tell you what's wrong
5. **Keep build tools updated** — Especially on Windows and Linux

---

## Quick Reference: Common Commands

```bash
# Check Python version
python --version

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.\\.venv\\Scripts\\activate  # Windows

# Install with uv (recommended)
uv pip install seeknal-<version>-py3-none-any.whl

# Install with pip
pip install seeknal-<version>-py3-none-any.whl

# Verify installation
pip show seeknal
python -c "from seeknal.tasks.duckdb import DuckDBTask; print('OK')"
seeknal --version
```

---

**Still stuck?** [Installation Guide](./index.md) | [Open an Issue](https://github.com/mta-tech/seeknal/issues/new) | [Community Discussions](https://github.com/mta-tech/seeknal/discussions)

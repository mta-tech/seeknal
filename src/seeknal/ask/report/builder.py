"""Evidence build runner for seeknal reports.

Handles npm install (with shared node_modules cache) and
Evidence build execution via subprocess (npx → npm run fallback).
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


def _print_progress(console: Any, line: str) -> None:
    """Print a subprocess progress line to the Rich console (dim, escaped)."""
    try:
        from rich.markup import escape

        console.print(f"  [dim]{escape(line)}[/dim]")
    except Exception:
        pass  # Don't let display errors break the build


def build_report(report_path: Path, timeout: int = 120, console: Any = None) -> str:
    """Build an Evidence report into static HTML.

    Checks for Node.js availability, installs dependencies if needed
    (using a shared cache), then runs ``npm run build``.

    Args:
        report_path: Path to the Evidence project directory.
        timeout: Maximum build time in seconds.
        console: Optional Rich Console for streaming progress output.

    Returns:
        Path to the built index.html on success, or an error message.
    """
    # Check Node.js availability
    node_error = _check_node()
    if node_error:
        return node_error

    # Ensure dependencies are installed (shared cache)
    # First install can take 2-3 minutes; subsequent installs are near-instant via cache
    install_error = _ensure_node_modules(report_path, timeout=180, console=console)
    if install_error:
        return install_error

    # Run evidence build: try npx with scoped name first, fall back to npm run
    env = _get_build_env()
    result = _run_evidence_build(report_path, env, timeout, console=console)
    if result is None:
        return f"Evidence build timed out after {timeout} seconds."

    if result.returncode != 0:
        stderr = result.stderr.strip()
        if len(stderr) > 2000:
            stderr = stderr[-2000:]
        return (
            f"Evidence build failed:\n{stderr}\n\n"
            "Fix the page content and try again."
        )

    # Find build output
    build_dir = report_path / "build"
    index_html = build_dir / "index.html"
    if index_html.exists():
        return str(index_html)

    # Fallback: check .evidence/build
    alt_index = report_path / ".evidence" / "build" / "index.html"
    if alt_index.exists():
        return str(alt_index)

    return f"Build completed but index.html not found in {build_dir}"


def _check_node() -> str:
    """Check that Node.js 18+ and npm/npx are available.

    Returns:
        Empty string if OK, error message otherwise.
    """
    npm = shutil.which("npm")
    if not npm:
        return (
            "Node.js is required for report generation but was not found.\n"
            "Install Node.js 18+ from https://nodejs.org/ and ensure "
            "'npm' is on your PATH."
        )

    node = shutil.which("node")
    if not node:
        return (
            "Node.js is required for report generation but was not found.\n"
            "Install Node.js 18+ from https://nodejs.org/"
        )

    # Check version
    try:
        result = subprocess.run(
            [node, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        version_str = result.stdout.strip().lstrip("v")
        major = int(version_str.split(".")[0])
        if major < 18:
            return (
                f"Node.js 18+ is required but found v{version_str}.\n"
                "Please upgrade Node.js: https://nodejs.org/"
            )
    except (subprocess.TimeoutExpired, OSError, ValueError):
        pass  # Proceed optimistically

    return ""


def _ensure_node_modules(report_path: Path, timeout: int = 60, console: Any = None) -> str:
    """Install npm dependencies using a shared cache.

    Uses a shared node_modules at target/reports/.evidence-cache/node_modules/
    symlinked into each report directory to avoid duplicating 200-500MB
    per report.

    Returns:
        Empty string if OK, error message otherwise.
    """
    report_path = report_path.resolve()
    node_modules = report_path / "node_modules"

    # Remove broken symlinks
    if node_modules.is_symlink() and not node_modules.exists():
        node_modules.unlink()

    if node_modules.exists():
        return ""

    # Shared cache location
    cache_dir = report_path.parent / ".evidence-cache"
    cache_modules = cache_dir / "node_modules"

    if cache_modules.exists() and cache_modules.is_dir():
        # Symlink from cache using relative path (../.evidence-cache/node_modules)
        try:
            rel_target = os.path.relpath(cache_modules, report_path)
            node_modules.symlink_to(rel_target)
            return ""
        except OSError:
            pass  # Fall through to npm install

    # First time: install into cache, then symlink
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Copy package.json to cache dir for install
    pkg_json = report_path / "package.json"
    cache_pkg = cache_dir / "package.json"
    if pkg_json.exists():
        shutil.copy2(str(pkg_json), str(cache_pkg))

    env = _get_build_env()
    try:
        proc = subprocess.Popen(
            ["npm", "install", "--prefer-offline"],
            cwd=str(cache_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        for line in proc.stdout:
            stripped = line.rstrip()
            if console and stripped:
                _print_progress(console, stripped)
        proc.wait(timeout=timeout)
        stderr_output = proc.stderr.read()
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        return "npm install timed out. Check your network connection."
    except OSError as e:
        return f"Error running npm install: {e}"

    if proc.returncode != 0:
        stderr = stderr_output.strip()
        if len(stderr) > 1000:
            stderr = stderr[-1000:]
        return f"npm install failed:\n{stderr}"

    # Symlink into report directory using relative path
    if cache_modules.exists():
        try:
            rel_target = os.path.relpath(cache_modules, report_path)
            node_modules.symlink_to(rel_target)
        except OSError:
            # Fallback: install directly in report dir
            try:
                subprocess.run(
                    ["npm", "install", "--prefer-offline"],
                    cwd=str(report_path),
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    env=env,
                )
            except (subprocess.TimeoutExpired, OSError):
                return "Failed to install dependencies."

    return ""


def _run_evidence_build(
    report_path: Path, env: dict[str, str], timeout: int, console: Any = None
) -> subprocess.CompletedProcess | None:
    """Run Evidence build, trying npx first then falling back to npm run.

    Returns:
        CompletedProcess on success or failure, None on timeout.
    """

    def _stream_popen(cmd: list[str]) -> subprocess.CompletedProcess | None:
        """Run a command with Popen, streaming stdout to console.

        Returns CompletedProcess on completion, None on timeout.
        """
        proc = subprocess.Popen(
            cmd,
            cwd=str(report_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        stdout_lines: list[str] = []
        for line in proc.stdout:
            stripped = line.rstrip()
            stdout_lines.append(stripped)
            if console and stripped:
                _print_progress(console, stripped)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            return None
        stderr_output = proc.stderr.read()
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=proc.returncode,
            stdout="\n".join(stdout_lines),
            stderr=stderr_output,
        )

    # Try npx with scoped package name (more direct)
    try:
        result = _stream_popen(["npx", "@evidence-dev/evidence", "build"])
        if result is None:
            return None  # Timeout
        if result.returncode == 0:
            return result
        # If npx failed with "could not determine executable", try npm run
        if "could not determine executable" in result.stderr:
            pass  # Fall through to npm run
        else:
            return result  # Real build error, return as-is
    except OSError:
        pass  # npx not available, fall through

    # Fallback: npm run build (uses scripts.build from package.json)
    try:
        return _stream_popen(["npm", "run", "build"])
    except OSError as e:
        # Return a fake failed result with the error
        return subprocess.CompletedProcess(
            args=["npm", "run", "build"],
            returncode=1,
            stdout="",
            stderr=f"Error launching build: {e}",
        )


def _get_build_env() -> dict[str, str]:
    """Build environment for Evidence subprocess.

    Strips sensitive variables (API keys, tokens, secrets).
    """
    env = os.environ.copy()
    for key in list(env.keys()):
        key_lower = key.lower()
        if any(
            s in key_lower
            for s in ("api_key", "secret", "token", "password", "credential")
        ):
            del env[key]
    return env

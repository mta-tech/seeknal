"""Evidence build runner for seeknal reports.

Handles npm install (with shared node_modules cache) and
Evidence build execution via subprocess (npx → npm run fallback).
"""

import os
import shutil
import subprocess
from pathlib import Path


def _copy_npmrc_to_cache(report_path: Path, cache_dir: Path) -> None:
    """Mirror the scaffolded .npmrc into the shared cache dir.

    The scaffolder writes ``.npmrc`` (with ``legacy-peer-deps=true``) into
    the report directory, but ``npm install`` runs inside the shared
    ``.evidence-cache`` dir — so without this copy, npm ignores the flag
    and aborts on Evidence v40's svelte-preprocess/stylus peer conflict.
    """
    src = report_path / ".npmrc"
    if not src.exists():
        # Fallback: synthesize a minimal .npmrc so install still succeeds
        # even when the scaffolder didn't run (e.g. external callers).
        (cache_dir / ".npmrc").write_text("legacy-peer-deps=true\n", encoding="utf-8")
        return
    shutil.copy2(str(src), str(cache_dir / ".npmrc"))


def build_report(report_path: Path, timeout: int = 120) -> str:
    """Build an Evidence report into static HTML.

    Checks for Node.js availability, installs dependencies if needed
    (using a shared cache), then runs ``npm run build``.

    Args:
        report_path: Path to the Evidence project directory.
        timeout: Maximum build time in seconds.

    Returns:
        Path to the built index.html on success, or an error message.
    """
    # Check Node.js availability
    node_error = _check_node()
    if node_error:
        return node_error

    # Ensure dependencies are installed (shared cache)
    # First install can take 2-3 minutes; subsequent installs are near-instant via cache
    install_error = _ensure_node_modules(report_path, timeout=180)
    if install_error:
        return install_error

    # Run evidence build: try npx with scoped name first, fall back to npm run
    env = _get_build_env()
    result = _run_evidence_build(report_path, env, timeout)
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


def _ensure_node_modules(report_path: Path, timeout: int = 60) -> str:
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

    # Copy package.json + .npmrc to cache dir for install. The .npmrc is
    # what enables legacy-peer-deps for Evidence v40's svelte-preprocess /
    # stylus peerOptional conflict — without it, fresh installs abort.
    pkg_json = report_path / "package.json"
    cache_pkg = cache_dir / "package.json"
    if pkg_json.exists():
        shutil.copy2(str(pkg_json), str(cache_pkg))
    _copy_npmrc_to_cache(report_path, cache_dir)

    env = _get_build_env()
    try:
        result = subprocess.run(
            ["npm", "install", "--prefer-offline", "--legacy-peer-deps"],
            cwd=str(cache_dir),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return "npm install timed out. Check your network connection."
    except OSError as e:
        return f"Error running npm install: {e}"

    if result.returncode != 0:
        stderr = result.stderr.strip()
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
    report_path: Path, env: dict[str, str], timeout: int
) -> subprocess.CompletedProcess | None:
    """Run Evidence build, trying npx first then falling back to npm run.

    Returns:
        CompletedProcess on success or failure, None on timeout.
    """
    # Try npx with scoped package name (more direct)
    try:
        result = subprocess.run(
            ["npx", "@evidence-dev/evidence", "build"],
            cwd=str(report_path),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.returncode == 0:
            return result
        # If npx failed with "could not determine executable", try npm run
        if "could not determine executable" in result.stderr:
            pass  # Fall through to npm run
        else:
            return result  # Real build error, return as-is
    except subprocess.TimeoutExpired:
        return None
    except OSError:
        pass  # npx not available, fall through

    # Fallback: npm run build (uses scripts.build from package.json)
    try:
        return subprocess.run(
            ["npm", "run", "build"],
            cwd=str(report_path),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return None
    except OSError as e:
        # Return a fake failed result with the error
        result = subprocess.CompletedProcess(
            args=["npm", "run", "build"],
            returncode=1,
            stdout="",
            stderr=f"Error launching build: {e}",
        )
        return result


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


# ---------------------------------------------------------------------------
# Async variant with auto-background support
# ---------------------------------------------------------------------------

async def async_build_report(report_path: Path, timeout: int = 120) -> str:
    """Async version of :func:`build_report` with auto-background.

    Long-running steps (npm install, evidence build) are run via
    :func:`run_with_auto_background` so they auto-background if they
    exceed the configured threshold.
    """
    node_error = _check_node()
    if node_error:
        return node_error

    # npm install (can take 2-3 minutes first time)
    install_error = await _async_ensure_node_modules(report_path, timeout=180)
    if install_error:
        return install_error

    # Evidence build
    env = _get_build_env()
    build_result = await _async_run_evidence_build(report_path, env, timeout)
    if build_result is None:
        return f"Evidence build timed out after {timeout} seconds."

    if isinstance(build_result, str):
        # Backgrounded placeholder or error
        return build_result

    stdout, stderr, returncode = build_result
    if returncode != 0:
        stderr_trimmed = stderr.strip()
        if len(stderr_trimmed) > 2000:
            stderr_trimmed = stderr_trimmed[-2000:]
        return (
            f"Evidence build failed:\n{stderr_trimmed}\n\n"
            "Fix the page content and try again."
        )

    # Find build output
    build_dir = report_path / "build"
    index_html = build_dir / "index.html"
    if index_html.exists():
        return str(index_html)

    alt_index = report_path / ".evidence" / "build" / "index.html"
    if alt_index.exists():
        return str(alt_index)

    return f"Build completed but index.html not found in {build_dir}"


async def _async_ensure_node_modules(report_path: Path, timeout: int = 60) -> str:
    """Async npm install with auto-background."""
    report_path = report_path.resolve()
    node_modules = report_path / "node_modules"

    if node_modules.is_symlink() and not node_modules.exists():
        node_modules.unlink()

    if node_modules.exists():
        return ""

    # Shared cache
    cache_dir = report_path.parent / ".evidence-cache"
    cache_modules = cache_dir / "node_modules"

    if cache_modules.exists() and cache_modules.is_dir():
        try:
            rel_target = os.path.relpath(cache_modules, report_path)
            node_modules.symlink_to(rel_target)
            return ""
        except OSError:
            pass

    cache_dir.mkdir(parents=True, exist_ok=True)
    pkg_json = report_path / "package.json"
    cache_pkg = cache_dir / "package.json"
    if pkg_json.exists():
        shutil.copy2(str(pkg_json), str(cache_pkg))
    _copy_npmrc_to_cache(report_path, cache_dir)

    env = _get_build_env()
    npm = shutil.which("npm")
    if not npm:
        return "npm not found. Install Node.js 18+."

    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.background import run_with_auto_background

    try:
        registry = get_tool_context().background_registry
    except RuntimeError:
        registry = None

    npm_install_cmd = [npm, "install", "--prefer-offline", "--legacy-peer-deps"]

    if registry is not None:
        result = await run_with_auto_background(
            npm_install_cmd,
            tool_name="generate_report",
            description="npm install (Evidence dependencies)",
            registry=registry,
            cwd=str(cache_dir),
            env=env,
            timeout=timeout,
            background_threshold=get_tool_context().background_threshold,
        )
        if isinstance(result, str):
            return result  # Backgrounded placeholder
        _, stderr, returncode = result
    else:
        try:
            proc = subprocess.run(
                npm_install_cmd,
                cwd=str(cache_dir), capture_output=True, text=True,
                timeout=timeout, env=env,
            )
            stderr, returncode = proc.stderr, proc.returncode
        except subprocess.TimeoutExpired:
            return "npm install timed out."
        except OSError as e:
            return f"Error running npm install: {e}"

    if returncode != 0:
        stderr_trimmed = stderr.strip()
        if len(stderr_trimmed) > 1000:
            stderr_trimmed = stderr_trimmed[-1000:]
        return f"npm install failed:\n{stderr_trimmed}"

    # Symlink into report dir
    if cache_modules.exists():
        try:
            rel_target = os.path.relpath(cache_modules, report_path)
            node_modules.symlink_to(rel_target)
        except OSError:
            pass

    return ""


async def _async_run_evidence_build(
    report_path: Path, env: dict[str, str], timeout: int,
) -> tuple[str, str, int] | str | None:
    """Async Evidence build with auto-background."""
    npx = shutil.which("npx")
    npm = shutil.which("npm")

    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.background import run_with_auto_background

    try:
        registry = get_tool_context().background_registry
    except RuntimeError:
        registry = None

    # Try npx first
    if npx and registry:
        result = await run_with_auto_background(
            [npx, "@evidence-dev/evidence", "build"],
            tool_name="generate_report",
            description="Evidence build (HTML generation)",
            registry=registry,
            cwd=str(report_path),
            env=env,
            timeout=timeout,
            background_threshold=get_tool_context().background_threshold,
        )
        if isinstance(result, str):
            return result  # Backgrounded
        stdout, stderr, returncode = result
        if returncode == 0:
            return (stdout, stderr, returncode)
        if "could not determine executable" not in stderr:
            return (stdout, stderr, returncode)
        # Fall through to npm run

    # Fallback: npm run build
    if npm and registry:
        result = await run_with_auto_background(
            [npm, "run", "build"],
            tool_name="generate_report",
            description="Evidence build (npm run build)",
            registry=registry,
            cwd=str(report_path),
            env=env,
            timeout=timeout,
            background_threshold=get_tool_context().background_threshold,
        )
        if isinstance(result, str):
            return result
        return result

    # No registry — sync fallback
    return _run_evidence_build(report_path, env, timeout) and None

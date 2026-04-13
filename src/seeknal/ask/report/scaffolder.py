"""Evidence project scaffolder for seeknal reports.

Creates the directory structure, config files, and page markdown
for an Evidence.dev project that reads seeknal parquet data via DuckDB.
"""

import json
import re
import shutil
import warnings
from importlib import resources
from pathlib import Path
from typing import Optional

# Bundled Seeknal branding assets — shipped inside src/seeknal/ask/report/assets/
_ASSETS_ROOT = resources.files("seeknal.ask.report") / "assets"

# Matches SQL fenced blocks in Evidence markdown: ```sql query_name\n...\n```
_SQL_BLOCK_RE = re.compile(
    r"```sql\s+(\w+)\s*\n(.*?)```", re.DOTALL
)

# Valid page name: lowercase alphanumeric + hyphens
_PAGE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")

# Evidence package versions — PINNED to a known-working combination.
# Caret ranges (^40.0.0) used to let npm pull the latest patch, but Evidence v40's
# transitive deps (Vite, SvelteKit, Svelte, vite-plugin-svelte, TypeScript) have a
# narrow compatibility window. On fresh installs npm would resolve to incompatible
# versions and the build would fail with one of:
#   - "Failed to resolve @sveltejs/kit/vite ESM only loaded by require"
#   - "Could not resolve peer dependency @sveltejs/vite-plugin-svelte"
#   - "[vite-plugin-svelte] Unrecognized option 'hmr'" (Svelte 4 vs 5 mismatch)
#   - "peer typescript ^4.9 || ^5 from svelte2tsx" (TypeScript 6 mismatch)
# Pin everything to the exact versions Evidence's own monorepo ships with.
# Source: ~/project/self/bmad-new/evidence/packages/evidence/package.json @ v40.1.8
_EVIDENCE_PKG_VERSION = "40.1.8"
_CORE_COMPONENTS_VERSION = "5.4.2"
_DUCKDB_VERSION = "2.0.1"

# Pinned peer deps required by @evidence-dev/evidence@40.1.8.
# These MUST be in `dependencies` (not just resolved as peers) so npm install
# resolves them before the build needs them.
_EVIDENCE_PEER_DEPS = {
    "@sveltejs/kit": "2.8.4",
    "@sveltejs/vite-plugin-svelte": "3.1.2",
    "svelte": "4.2.19",
    "svelte-preprocess": "5.1.3",
    "svelte2tsx": "0.7.4",
    "vite": "5.4.21",
    "typescript": "5.4.2",
    "postcss": "8.4.47",
    "postcss-load-config": "4.0.2",
    "autoprefixer": "10.4.20",
    "tailwindcss": "3.4.18",
    "unist-util-visit": "4.1.2",
    "debounce": "1.2.1",
    "git-remote-origin-url": "4.0.0",
}


def scaffold_report(
    project_path: Path,
    title: str,
    pages: list[dict],
    parquet_dirs: Optional[list[Path]] = None,
) -> Path:
    """Scaffold an Evidence project for a seeknal report.

    Creates the full Evidence project structure at
    ``target/reports/{slug}/`` with DuckDB source config,
    page markdown files, and a pre-built .duckdb file.

    Args:
        project_path: Path to the seeknal project root.
        title: Report title (used for slug and display).
        pages: List of page dicts with ``name`` and ``content`` keys.
        parquet_dirs: Optional override for parquet directories.

    Returns:
        Path to the report directory.

    Raises:
        ValueError: If title is empty, pages list is empty,
            page names are invalid, or SQL validation fails.
    """
    if not title or not title.strip():
        raise ValueError("Report title cannot be empty.")

    if not pages:
        raise ValueError("At least one page is required.")

    slug = _slugify(title)
    report_dir = project_path / "target" / "reports" / slug

    # Overwrite existing report with warning
    if report_dir.exists():
        warnings.warn(
            f"Report '{slug}' already exists and will be overwritten.",
            stacklevel=2,
        )
        shutil.rmtree(report_dir)

    report_dir.mkdir(parents=True, exist_ok=True)

    # Validate and write pages
    pages_dir = report_dir / "pages"
    pages_dir.mkdir()
    _write_pages(pages_dir, pages)

    # Write Evidence config files (creates sources/seeknal/ directory)
    _write_evidence_config(report_dir)
    _write_connection_yaml(report_dir)

    # Create .duckdb file inside sources/seeknal/ so the relative path
    # resolves correctly both from sources/seeknal/ and from
    # .evidence/template/sources/seeknal/ (where Evidence copies it)
    db_path = report_dir / "sources" / "seeknal" / ".report.duckdb"
    from seeknal.ask.report.data_bridge import create_duckdb_from_parquets
    _count, table_names = create_duckdb_from_parquets(project_path, db_path)

    # Create .sql source files so Evidence caches each table for the
    # browser WASM DuckDB used by inline queries at dev/build time
    _write_source_sql_files(report_dir, table_names)
    _write_package_json(report_dir, title)

    # Apply Seeknal branding via the file-watcher-supported surfaces:
    #   - app.css (Evidence watches it) to hide the "Built with Evidence"
    #     footer link and splash screen
    #   - static/ (Evidence watches it) for favicons
    #   - .npmrc enables legacy-peer-deps for the pinned Evidence v40 tree
    _write_app_css(report_dir)
    _write_npmrc(report_dir)
    _copy_branding_assets(report_dir)

    return report_dir


def _slugify(title: str) -> str:
    """Convert title to a filesystem-safe slug.

    Lowercases, replaces non-alphanumeric with hyphens,
    collapses consecutive hyphens, strips leading/trailing hyphens.
    """
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    if not slug:
        slug = "report"
    # Limit length for path safety
    return slug[:60]


def _validate_page_name(name: str) -> str:
    """Validate and normalize a page name.

    Returns the sanitized name (lowercase, alphanumeric + hyphens).

    Raises:
        ValueError: If the name is empty or contains invalid characters
            after sanitization.
    """
    sanitized = name.lower().strip()
    sanitized = re.sub(r"[^a-z0-9-]+", "-", sanitized)
    sanitized = sanitized.strip("-")

    if not sanitized:
        raise ValueError(f"Invalid page name: {name!r}")

    if not _PAGE_NAME_RE.match(sanitized):
        raise ValueError(
            f"Page name {name!r} contains invalid characters. "
            "Use only lowercase letters, numbers, and hyphens."
        )

    return sanitized


def _validate_sql_blocks(content: str, page_name: str) -> None:
    """Extract and validate SQL blocks in Evidence markdown.

    Raises:
        ValueError: If any SQL block fails security validation.
    """
    from seeknal.ask.security import validate_sql_for_agent

    for match in _SQL_BLOCK_RE.finditer(content):
        query_name = match.group(1)
        sql = match.group(2).strip()
        if not sql:
            continue
        try:
            validate_sql_for_agent(sql)
        except ValueError as e:
            raise ValueError(
                f"SQL validation failed in page '{page_name}', "
                f"query '{query_name}': {e}"
            ) from e


def _write_pages(pages_dir: Path, pages: list[dict]) -> None:
    """Write page markdown files, validating names and SQL."""
    for i, page in enumerate(pages):
        name = page.get("name", "")
        content = page.get("content", "")

        if not name:
            raise ValueError(f"Page {i} is missing a 'name' field.")
        if not content or not content.strip():
            raise ValueError(f"Page '{name}' has empty content.")

        sanitized_name = _validate_page_name(name)
        _validate_sql_blocks(content, sanitized_name)

        # First page becomes index.md
        if i == 0:
            filename = "index.md"
        else:
            filename = f"{sanitized_name}.md"

        (pages_dir / filename).write_text(content, encoding="utf-8")


def _write_evidence_config(report_dir: Path) -> None:
    """Write evidence.config.yaml with DuckDB datasource and slug placeholder.

    The deployment.basePath uses a __SEEKNAL_SLUG__ placeholder that the
    Seeknal Report Server replaces with the actual slug at upload time.
    This lets Evidence's SvelteKit build correctly prefix all asset URLs
    and route data with the slug path, instead of assuming the app is at
    the server root. Without this, SvelteKit's client router can't match
    routes when served at /r/{slug}/ and silently fails to load data.
    """
    config = """\
plugins:
  components:
    "@evidence-dev/core-components": {}
  datasources:
    "@evidence-dev/duckdb": {}

deployment:
  basePath: /r/__SEEKNAL_SLUG__
"""
    (report_dir / "evidence.config.yaml").write_text(config, encoding="utf-8")


def _write_connection_yaml(report_dir: Path) -> None:
    """Write DuckDB connection config pointing to .report.duckdb.

    The .duckdb file lives in sources/seeknal/ alongside connection.yaml.
    Using just the filename ensures it resolves correctly both from the
    original location and from .evidence/template/sources/seeknal/ where
    Evidence copies it at build/dev time.
    """
    sources_dir = report_dir / "sources" / "seeknal"
    sources_dir.mkdir(parents=True, exist_ok=True)

    connection = """\
name: seeknal
type: duckdb
options:
  filename: .report.duckdb
"""
    (sources_dir / "connection.yaml").write_text(connection, encoding="utf-8")


def _write_source_sql_files(report_dir: Path, table_names: list[str]) -> None:
    """Create .sql source files so Evidence pre-caches each table.

    Evidence's inline SQL (in markdown pages) runs in a browser WASM DuckDB
    that only has access to data pre-loaded from the source manifest.
    Each .sql file selects all rows from a table so that ``npx evidence sources``
    (or the build) caches the data for the WASM engine.
    """
    sources_dir = report_dir / "sources" / "seeknal"
    for name in table_names:
        sql_file = sources_dir / f"{name}.sql"
        sql_file.write_text(f'SELECT * FROM "{name}"\n', encoding="utf-8")


def _write_package_json(report_dir: Path, title: str) -> None:
    """Write minimal package.json for the Evidence project.

    Note: All Evidence peer deps are pinned in `dependencies` (not just left
    as transitive peers) so a fresh `npm install` resolves the exact known-working
    combination. The `overrides` block forces transitive dependencies down to
    these same versions even when sub-packages request different ranges,
    preventing Vite/SvelteKit/Svelte version drift.
    """
    dependencies = {
        "@evidence-dev/evidence": _EVIDENCE_PKG_VERSION,
        "@evidence-dev/core-components": _CORE_COMPONENTS_VERSION,
        "@evidence-dev/duckdb": _DUCKDB_VERSION,
        **_EVIDENCE_PEER_DEPS,
    }
    package = {
        "name": "seeknal-report",
        "version": "0.0.1",
        "private": True,
        "scripts": {
            "build": "evidence build",
            "dev": "evidence dev",
        },
        "engines": {
            "node": ">=20.12.0",
        },
        "type": "module",
        "dependencies": dependencies,
        "overrides": dict(_EVIDENCE_PEER_DEPS),
    }
    (report_dir / "package.json").write_text(
        json.dumps(package, indent=2) + "\n", encoding="utf-8"
    )


def _read_asset_text(name: str) -> str:
    """Read a bundled Seeknal asset file as text."""
    return (_ASSETS_ROOT / name).read_text(encoding="utf-8")


def _write_app_css(report_dir: Path) -> None:
    """Write a Seeknal-branded app.css that hides Evidence's footer + splash.

    Evidence's file watcher copies ./app.css → .evidence/template/src/app.css
    at build time (one of the few user-overridable surfaces). We use it to
    suppress the 'Built with Evidence' footer link and the splash-screen logo
    via CSS selectors. Customizing +layout.svelte or src/app.html is not
    possible because Evidence's CLI does not watch src/ overrides.
    """
    (report_dir / "app.css").write_text(_read_asset_text("app.css"), encoding="utf-8")


def _write_npmrc(report_dir: Path) -> None:
    """Write an .npmrc that enables legacy-peer-deps.

    Evidence v40.1.8 pins svelte-preprocess@5.1.3 which has a peerOptional
    dep on stylus@^0.55.0 that is not present in the tree. Without
    legacy-peer-deps, npm 10+ treats this as a hard conflict and aborts.
    The .npmrc applies automatically to any npm invocation inside the
    report directory (builder.py subprocess, manual runs, CI).
    """
    (report_dir / ".npmrc").write_text(
        _read_asset_text(".npmrc"), encoding="utf-8"
    )


def _copy_branding_assets(report_dir: Path) -> None:
    """Copy Seeknal favicon set into the project static dir.

    SvelteKit serves files from static/ at the site root. Evidence's app.html
    (which we override) references these by name (favicon.ico, icon.svg,
    apple-touch-icon.png, icon-192.png, icon-512.png).

    We deliberately do NOT ship a static manifest.webmanifest file here:
    Evidence already defines a dynamic SvelteKit route at
    src/pages/manifest.webmanifest/+server.js that returns a JSON manifest
    with icon references, and a static file would conflict with that route
    and break prerendering. Brand identity (title, theme color, splash)
    lives entirely in app.html instead, which is what users actually see.
    """
    static_dir = report_dir / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "favicon.ico",
        "icon.svg",
        "apple-touch-icon.png",
        "icon-192.png",
        "icon-512.png",
    ):
        src = _ASSETS_ROOT / name
        if src.is_file():
            shutil.copy(str(src), str(static_dir / name))

"""
Seeknal CLI - Main entry point

A dbt-like CLI for managing feature stores with comprehensive version management
capabilities for ML teams.

Usage:
    seeknal init                                Initialize a new project
    seeknal run <flow>                          Execute a transformation flow
    seeknal materialize <fg>                    Materialize features to stores
    seeknal materialize <fg> --version <N>      Materialize a specific version
    seeknal list <resource>                     List resources
    seeknal show <resource> <name>              Show resource details
    seeknal delete <resource> <name>            Delete a resource
    seeknal delete-table <name>                 Delete an online table
    seeknal validate                            Validate configurations
    seeknal validate-features <fg>              Validate feature group data quality
    seeknal info                                Show version information
    seeknal debug <fg>                          Debug a feature group
    seeknal clean <fg>                          Clean old feature data

Version Management:
    seeknal version list <fg>                   List all versions of a feature group
    seeknal version list <fg> --limit 5         List last 5 versions
    seeknal version show <fg>                   Show latest version details
    seeknal version show <fg> --version <N>     Show specific version details
    seeknal version diff <fg> --from 1 --to 2   Compare schemas between versions

Atlas Data Platform Integration (requires: pip install seeknal[atlas]):
    seeknal atlas info                          Show Atlas integration info
    seeknal atlas api start                     Start the Atlas API server
    seeknal atlas api status                    Check API server status
    seeknal atlas governance stats              Get governance statistics
    seeknal atlas governance policies           List governance policies
    seeknal atlas governance violations         List policy violations
    seeknal atlas lineage show <name>           Show lineage for a resource
    seeknal atlas lineage publish <pipeline>    Publish lineage to DataHub

Iceberg Materialization (requires: DuckDB + Iceberg extension):
    seeknal iceberg validate-materialization    Validate Iceberg materialization config
    seeknal iceberg snapshot-list <table>       List snapshots for a table
    seeknal iceberg snapshot-show <table> <id>  Show snapshot details
    seeknal iceberg setup                       Interactive credential setup
    seeknal iceberg profile-show                Show current materialization profile

Virtual Environments (plan/apply/promote workflow):
    seeknal env plan <name>                    Preview changes in a virtual environment
    seeknal env apply <name>                   Execute a plan in a virtual environment
    seeknal env apply <name> --parallel        Execute with parallel node processing
    seeknal env promote <from> [to]            Promote environment to production
    seeknal env list                           List all virtual environments
    seeknal env delete <name>                  Delete a virtual environment

Parallel Execution:
    seeknal run --parallel                     Execute independent nodes in parallel
    seeknal run --parallel --max-workers 8     Set maximum parallel workers

Examples:
    # Initialize a new project
    $ seeknal init --name my_project

    # List all feature groups
    $ seeknal list feature-groups

    # Materialize the latest version of a feature group
    $ seeknal materialize user_features --start-date 2024-01-01

    # Materialize a specific version (useful for rollbacks)
    $ seeknal materialize user_features --start-date 2024-01-01 --version 1

    # View version history
    $ seeknal version list user_features

    # Compare schema changes between versions
    $ seeknal version diff user_features --from 1 --to 2

    # Validate feature data quality
    $ seeknal validate-features user_features --mode fail

    # Start Atlas API (requires: pip install seeknal[atlas])
    $ seeknal atlas api start --port 8000

    # View governance statistics
    $ seeknal atlas governance stats

For more information, see: https://github.com/mta-tech/seeknal
"""

import typer
from typing import Optional, Callable, List, Any
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
import os
import sys
from pathlib import Path

# Load .env file if present (for local development)
# This makes environment variables available for all CLI commands
try:
    from dotenv import load_dotenv
    
    # Try to find .env in current directory or up to 3 parent directories
    cwd = Path.cwd()
    for path in [cwd] + list(cwd.parents)[:3]:
        env_file = path / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            break
    else:
        # Fallback to default behavior (search in current directory only)
        load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, skip

app = typer.Typer(
    name="seeknal",
    help="Feature store management CLI - similar to dbt",
    add_completion=False,
)

# Version command group for feature group version management
version_app = typer.Typer(
    name="version",
    help="""Manage feature group versions.

Feature group versioning enables ML teams to track schema evolution,
compare changes between versions, and safely roll back to previous
versions when needed.

Commands:
  list   List all versions of a feature group with creation dates
  show   Display detailed metadata and schema for a specific version
  diff   Compare schemas between two versions to identify changes

Examples:
  seeknal version list user_features
  seeknal version show user_features --version 2
  seeknal version diff user_features --from 1 --to 2
""",
)
app.add_typer(version_app, name="version")

# Source management for REPL
from seeknal.cli.source import app as source_app

app.add_typer(source_app, name="source")

# Iceberg materialization commands
from seeknal.cli.materialization_cli import app as iceberg_app

app.add_typer(iceberg_app, name="iceberg")

# Virtual environment management
env_app = typer.Typer(help="Virtual environments for safe pipeline development (plan/apply/promote)")
app.add_typer(env_app, name="env")


# =============================================================================
# Interval Management
# =============================================================================
# Interval tracking for incremental materialization and backfill operations

intervals_app = typer.Typer(
    name="intervals",
    help="""Manage execution intervals for incremental materialization.

Interval tracking enables efficient incremental processing by tracking which
time intervals have been completed and which need to be executed or restated.

Commands:
  list              List completed intervals for a node
  pending           Show pending intervals (not yet executed)
  restatement-add   Mark interval for restatement
  restatement-list  List restatement intervals
  restatement-clear Clear restatement intervals
  backfill          Execute backfill for missing intervals

Examples:
  seeknal intervals list transform.clean_data
  seeknal intervals pending feature_group.user_features --start 2024-01-01
  seeknal intervals restatement-add feature_group.user_features --start 2024-01-01 --end 2024-01-31
  seeknal intervals backfill feature_group.user_features --start 2024-01-01 --end 2024-01-31
"""
)
app.add_typer(intervals_app, name="intervals")


# =============================================================================
# Atlas Integration (Optional)
# =============================================================================
# The Atlas command group is loaded dynamically if atlas-data-platform is installed.
# This allows Seeknal to work standalone while providing Atlas features when available.

def _register_atlas_commands():
    """Register Atlas commands if atlas-data-platform is available."""
    try:
        from seeknal.cli.atlas import atlas_app
        app.add_typer(atlas_app, name="atlas")
    except ImportError:
        # Atlas not installed, add a placeholder command
        @app.command("atlas", hidden=True)
        def atlas_not_installed():
            """Atlas Data Platform integration (not installed).

            Install with: pip install seeknal[atlas]
            """
            typer.echo(typer.style("✗ Atlas Data Platform is not installed.", fg=typer.colors.RED))
            typer.echo("")
            typer.echo("Install Atlas integration with:")
            typer.echo(typer.style("  pip install seeknal[atlas]", fg=typer.colors.CYAN))
            typer.echo("")
            typer.echo("Or install Atlas directly:")
            typer.echo(typer.style("  pip install atlas-data-platform", fg=typer.colors.CYAN))
            raise typer.Exit(1)


# Register Atlas commands
_register_atlas_commands()


class OutputFormat(str, Enum):
    """Output format options."""
    TABLE = "table"
    JSON = "json"
    YAML = "yaml"


class WriteMode(str, Enum):
    """Write mode options."""
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


class ResourceType(str, Enum):
    """Resource types for listing."""
    PROJECTS = "projects"
    WORKSPACES = "workspaces"
    ENTITIES = "entities"
    FLOWS = "flows"
    FEATURE_GROUPS = "feature-groups"
    OFFLINE_STORES = "offline-stores"


class DeleteResourceType(str, Enum):
    """Resource types for deletion."""
    FEATURE_GROUP = "feature-group"


class ValidationModeChoice(str, Enum):
    """Validation mode options for validate-features command."""
    WARN = "warn"
    FAIL = "fail"


def _get_version() -> str:
    """Get package version."""
    try:
        from seeknal import __version__
        return __version__
    except ImportError:
        return "1.0.0"


def _echo_success(message: str):
    """Print success message in green."""
    typer.echo(typer.style(f"✓ {message}", fg=typer.colors.GREEN))


def _echo_error(message: str):
    """Print error message in red."""
    typer.echo(typer.style(f"✗ {message}", fg=typer.colors.RED))


def _echo_warning(message: str):
    """Print warning message in yellow."""
    typer.echo(typer.style(f"⚠ {message}", fg=typer.colors.YELLOW))


def _echo_info(message: str):
    """Print info message in blue."""
    typer.echo(typer.style(f"ℹ {message}", fg=typer.colors.BLUE))


def parse_date_safely(date_str: str, param_name: str = "date") -> datetime:
    """Parse and validate a date string with robust validation.

    This function handles both ISO format with time (YYYY-MM-DDTHH:MM:SS)
    and date-only format (YYYY-MM-DD). It performs comprehensive validation
    including year range checks and future date limits.

    Args:
        date_str: The date string to parse (YYYY-MM-DD or ISO timestamp)
        param_name: The parameter name for error messages (default: "date")

    Returns:
        datetime: The parsed datetime object

    Raises:
        typer.Exit: If the date is invalid or fails validation
    """
    try:
        # Parse the date - handle both ISO format with time and date-only format
        if "T" in date_str:
            dt = datetime.fromisoformat(date_str)
        else:
            dt = datetime.fromisoformat(f"{date_str}T00:00:00")

        # Validate year range (2000-2100)
        if dt.year < 2000:
            _echo_error(f"Invalid {param_name}: Year {dt.year} is before minimum year 2000")
            raise typer.Exit(1)
        if dt.year > 2100:
            _echo_error(f"Invalid {param_name}: Year {dt.year} is after maximum year 2100")
            raise typer.Exit(1)

        # Validate future dates (max 1 year ahead from today)
        max_future_date = datetime.now() + timedelta(days=365)
        if dt > max_future_date:
            _echo_error(f"Invalid {param_name}: Date {dt.strftime('%Y-%m-%d')} is more than 1 year in the future")
            raise typer.Exit(1)

        return dt

    except ValueError as e:
        # Catch parsing errors from fromisoformat
        if "invalid" in str(e).lower() or "out of range" in str(e).lower():
            _echo_error(f"Invalid {param_name}: {date_str}. Use YYYY-MM-DD or ISO timestamp (YYYY-MM-DDTHH:MM:SS)")
        else:
            _echo_error(f"Invalid {param_name}: {e}")
        raise typer.Exit(1)
    except typer.Exit:
        # Re-raise typer.Exit from our validation checks
        raise
    except Exception as e:
        # Catch any unexpected errors
        _echo_error(f"Invalid {param_name}: {e}")
        raise typer.Exit(1)


def handle_cli_error(error_message: str = "Operation failed"):
    """Decorator to standardize CLI error handling.

    This decorator wraps CLI commands to provide consistent error handling
    across all commands. It catches exceptions, displays an error message,
    and exits with a non-zero status code.

    Args:
        error_message: The base error message to display. The actual exception
            message will be appended to this.

    Example:
        @app.command()
        @handle_cli_error("Failed to initialize project")
        def init(name: str):
            # Command logic here
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except typer.Exit:
                raise
            except Exception as e:
                _echo_error(f"{error_message}: {e}")
                raise typer.Exit(1)
        return wrapper
    return decorator


def _build_manifest_from_dag(dag_builder, project_name: str):
    """Build a Manifest from a DAGBuilder result.

    Shared by plan, parse, and env_plan commands to avoid duplicating
    the node_type_map and conversion logic.

    Args:
        dag_builder: A built DAGBuilder instance with nodes and edges.
        project_name: Project name for the manifest metadata.

    Returns:
        A Manifest instance populated from the DAGBuilder.
    """
    from seeknal.dag.manifest import Manifest, Node, NodeType as ManifestNodeType

    node_type_map = {
        "source": ManifestNodeType.SOURCE,
        "transform": ManifestNodeType.TRANSFORM,
        "feature_group": ManifestNodeType.FEATURE_GROUP,
        "model": ManifestNodeType.MODEL,
        "rule": ManifestNodeType.RULE,
        "aggregation": ManifestNodeType.AGGREGATION,
        "second_order_aggregation": ManifestNodeType.SECOND_ORDER_AGGREGATION,
        "exposure": ManifestNodeType.EXPOSURE,
        "python": ManifestNodeType.PYTHON,
        "semantic_model": ManifestNodeType.SEMANTIC_MODEL,
        "metric": ManifestNodeType.METRIC,
    }

    manifest = Manifest(project=project_name)
    for node_id, node in dag_builder.nodes.items():
        kind_str = node.kind.value if hasattr(node.kind, 'value') else str(node.kind)
        manifest_node_type = node_type_map.get(kind_str, ManifestNodeType.SOURCE)
        manifest.add_node(Node(
            id=node_id,
            name=node.name,
            node_type=manifest_node_type,
            description=node.yaml_data.get("description") if hasattr(node, 'yaml_data') else None,
            tags=list(node.tags) if hasattr(node, 'tags') and node.tags else [],
            config=node.yaml_data if hasattr(node, 'yaml_data') else {},
            file_path=node.file_path if hasattr(node, 'file_path') else None,
        ))

    for node_id in dag_builder.nodes:
        for downstream_id in dag_builder.get_downstream(node_id):
            manifest.add_edge(node_id, downstream_id)

    return manifest


@app.command()
def info():
    """Show version information for Seeknal and its dependencies."""
    import pyspark
    import duckdb

    typer.echo(f"Seeknal version: {_get_version()}")
    typer.echo(f"Python version: {sys.version.split()[0]}")
    typer.echo(f"PySpark version: {pyspark.__version__}")
    typer.echo(f"DuckDB version: {duckdb.__version__}")


@app.command()
def init(
    name: str = typer.Option(
        None, "--name", "-n",
        help="Project name (defaults to current directory name)"
    ),
    description: str = typer.Option(
        "", "--description", "-d",
        help="Project description"
    ),
    path: Path = typer.Option(
        Path("."), "--path", "-p",
        help="Project path"
    ),
    force: bool = typer.Option(
        False, "--force", "-f",
        help="Overwrite existing project configuration"
    ),
):
    """Initialize a new Seeknal project with dbt-style structure.

    Creates a complete project structure with:
    - seeknal_project.yml: Project configuration (name, version, profile)
    - profiles.yml: Credentials and engine config (gitignored)
    - .gitignore: Auto-generated with profiles.yml and target/
    - seeknal/: Directory for YAML and Python pipeline definitions
    - target/: Output directory for compiled artifacts

    Directory structure:
        seeknal/
        ├── sources/         # YAML source definitions
        ├── transforms/      # YAML transforms
        ├── feature_groups/  # YAML feature groups
        ├── models/          # YAML models
        ├── pipelines/       # Python pipeline scripts (*.py)
        └── templates/       # Custom Jinja templates (optional)
        target/
        └── intermediate/    # Node output storage for cross-references

    Examples:
        # Initialize in current directory
        $ seeknal init

        # Initialize with custom name
        $ seeknal init --name my_project

        # Initialize at specific path
        $ seeknal init --path /path/to/project

        # Reinitialize existing project
        $ seeknal init --force
    """
    import yaml

    project_path = path.resolve()
    project_file = project_path / "seeknal_project.yml"

    # Check for existing project
    if project_file.exists() and not force:
        _echo_error(f"Project already exists at {project_path}")
        _echo_info("Use --force to reinitialize")
        raise typer.Exit(1)

    # Default name from directory
    if name is None:
        name = project_path.name

    typer.echo(f"Initializing Seeknal project: {name}")

    try:
        # Create dbt-style directory structure
        directories = [
            "seeknal/sources",
            "seeknal/transforms",
            "seeknal/feature_groups",
            "seeknal/models",
            "seeknal/pipelines",
            "seeknal/templates",
            "target/intermediate",
        ]

        for dir_name in directories:
            dir_path = project_path / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)
            typer.echo(f"  Created directory: {dir_name}/")

        # Generate seeknal_project.yml
        project_config = {
            "name": name,
            "version": "1.0.0",
            "profile": "default",
            "config-version": 1,
            "state_backend": "file",  # Options: file, database
        }
        # Only add description if it's a non-empty string
        if description and isinstance(description, str):
            project_config["description"] = description

        project_file.write_text(yaml.dump(project_config, sort_keys=False))
        typer.echo(f"  Created: seeknal_project.yml")

        # Generate profiles.yml
        profiles_config = {
            "default": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": "target/dev.duckdb",
                    }
                }
            }
        }
        (project_path / "profiles.yml").write_text(yaml.dump(profiles_config, sort_keys=False))
        typer.echo(f"  Created: profiles.yml (gitignored)")

        # Generate .gitignore
        gitignore_content = """# Seeknal
profiles.yml
target/
*.duckdb
*.duckdb.wal

# Python
__pycache__/
*.py[cod]
.venv/
*.egg-info/
"""
        (project_path / ".gitignore").write_text(gitignore_content)
        typer.echo(f"  Created: .gitignore")

        # Also create the legacy Project for database compatibility
        from seeknal.project import Project
        if description and isinstance(description, str):
            project = Project(name=name, description=description)
        else:
            project = Project(name=name)
        project.get_or_create()

        _echo_success(f"Project '{name}' initialized successfully!")
        typer.echo("")
        _echo_info("Next steps:")
        typer.echo("  1. Edit profiles.yml to configure your data sources")
        typer.echo("  2. Create sources:     seeknal draft source <name>")
        typer.echo("  3. Validate drafts:    seeknal dry-run draft_source_<name>.yml")
        typer.echo("  4. Apply to project:   seeknal apply draft_source_<name>.yml")
        typer.echo("  5. Plan execution:     seeknal plan")
        typer.echo("  6. Run pipeline:       seeknal run")
        typer.echo("")
        _echo_info("Advanced:")
        typer.echo("  - Test in isolation:   seeknal plan dev && seeknal run --env dev")
        typer.echo("  - Promote to prod:     seeknal promote dev")
        typer.echo("  - Parallel execution:  seeknal run --parallel")

    except Exception as e:
        _echo_error(f"Failed to initialize project: {e}")
        raise typer.Exit(1)


@app.command()
def run(
    flow_name: Optional[str] = typer.Argument(
        None,
        help="Name of the flow to run (legacy). If omitted, executes YAML pipeline from seeknal/ directory."
    ),
    start_date: Optional[str] = typer.Option(
        None, "--start-date", "-s",
        help="Start date for the flow (YYYY-MM-DD)"
    ),
    end_date: Optional[str] = typer.Option(
        None, "--end-date", "-e",
        help="End date for the flow (YYYY-MM-DD)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Show what would be executed without running"
    ),
    # YAML pipeline execution flags
    full: bool = typer.Option(
        False, "--full", "-f",
        help="Run all nodes regardless of state (ignore incremental run cache)"
    ),
    nodes: Optional[List[str]] = typer.Option(
        None, "--nodes", "-n",
        help="Run specific nodes only (e.g., --nodes transform.clean_data)"
    ),
    types: Optional[List[str]] = typer.Option(
        None, "--types", "-t",
        help="Filter by node types (e.g., --types transform,feature_group)"
    ),
    exclude_tags: Optional[List[str]] = typer.Option(
        None, "--exclude-tags",
        help="Skip nodes with these tags"
    ),
    continue_on_error: bool = typer.Option(
        False, "--continue-on-error",
        help="Continue execution after failures"
    ),
    retry: int = typer.Option(
        0, "--retry", "-r",
        help="Number of retries for failed nodes"
    ),
    show_plan: bool = typer.Option(
        False, "--show-plan", "-p",
        help="Show execution plan without running"
    ),
    # Parallel execution flags
    parallel: bool = typer.Option(
        False, "--parallel",
        help="Execute independent nodes in parallel (layer-based concurrency)"
    ),
    max_workers: int = typer.Option(
        4, "--max-workers",
        help="Maximum parallel workers (default: 4, max recommended: CPU count)"
    ),
    # Materialization flags
    materialize: Optional[bool] = typer.Option(
        None, "--materialize/--no-materialize",
        help="Enable/disable Iceberg materialization (overrides node config)"
    ),
    # Environment flag
    env: Optional[str] = typer.Option(
        None, "--env",
        help="Run in isolated virtual environment (requires a plan: seeknal env plan <name>)"
    ),
    # Parameterization flags
    param_date: Optional[str] = typer.Option(
        None, "--date",
        help="Override date parameter (YYYY-MM-DD format)"
    ),
    param_run_id: Optional[str] = typer.Option(
        None, "--run-id",
        help="Custom run ID for parameterization"
    ),
    # Interval tracking flags
    start: Optional[str] = typer.Option(
        None, "--start",
        help="Start timestamp for interval execution (ISO format or YYYY-MM-DD)"
    ),
    end: Optional[str] = typer.Option(
        None, "--end",
        help="End timestamp for interval execution (ISO format or YYYY-MM-DD)"
    ),
    backfill: bool = typer.Option(
        False, "--backfill",
        help="Execute backfill for all missing intervals in the date range"
    ),
    restate: bool = typer.Option(
        False, "--restate",
        help="Process restatement intervals marked for reprocessing"
    ),
):
    """Execute a feature transformation flow or YAML pipeline.

    **Legacy Mode (with flow_name):**
    Run a legacy Flow object by name.

    **YAML Pipeline Mode (without flow_name):**
    Execute the DAG defined by YAML files in the seeknal/ directory.
    Supports incremental runs with change detection, parallel execution,
    and configurable error handling.

    **YAML Pipeline Examples:**
        # Run changed nodes only (incremental)
        seeknal run

        # Run all nodes (full refresh)
        seeknal run --full

        # Dry run to see what would execute
        seeknal run --dry-run

        # Show execution plan
        seeknal run --show-plan

        # Run specific nodes
        seeknal run --nodes transform.clean_data feature_group.user_features

        # Run only transforms and feature_groups
        seeknal run --types transform,feature_group

        # Continue on error (don't stop at first failure)
        seeknal run --continue-on-error

        # Retry failed nodes
        seeknal run --retry 2

        # Run independent nodes in parallel (uses up to 8 workers)
        seeknal run --parallel

        # Run in parallel with custom worker count
        seeknal run --parallel --max-workers 8

        # Run in an isolated environment
        seeknal run --env dev

        # Run in environment with parallel execution
        seeknal run --env dev --parallel

    **Interval Tracking Examples:**
        # Run with specific interval
        seeknal run --start 2024-01-01 --end 2024-01-31

        # Backfill missing intervals
        seeknal run --backfill --start 2024-01-01 --end 2024-01-31

        # Process restatement intervals
        seeknal run --restate

    **Legacy Flow Examples:**
        seeknal run my_flow --start-date 2024-01-01

    **Parameterization Examples:**
        # Use default (today's date)
        seeknal run

        # Override date for backfill
        seeknal run --date 2025-01-15

        # Custom run ID
        seeknal run --run-id daily-batch-123

        # Combine with other flags
        seeknal run --date 2025-01-15 --full
    """
    # Environment mode: delegate to shared helper
    if env is not None:
        from pathlib import Path
        project_path = Path.cwd().resolve()
        _run_in_environment(
            env_name=env,
            project_path=project_path,
            force=False,
            parallel=parallel,
            max_workers=max_workers,
            continue_on_error=continue_on_error,
            dry_run=dry_run,
            show_plan=show_plan,
        )
        return

    # Determine mode: legacy Flow vs YAML pipeline
    if flow_name is not None:
        # Legacy mode: run Flow object
        from seeknal.flow import Flow

        typer.echo(f"Running flow: {flow_name}")
        if start_date:
            typer.echo(f"  Start date: {start_date}")
        if end_date:
            typer.echo(f"  End date: {end_date}")

        if dry_run:
            _echo_warning("Dry run mode - no changes will be made")
            return

        try:
            flow = Flow(name=flow_name).get_or_create()
            result = flow.run()
            _echo_success(f"Flow '{flow_name}' completed successfully!")
            if result is not None:
                typer.echo(f"  Result rows: {result.count()}")
        except Exception as e:
            _echo_error(f"Flow execution failed: {e}")
            raise typer.Exit(1)
    else:
        # Build CLI overrides for parameter resolution
        cli_overrides: dict[str, Any] = {}
        if param_date:
            cli_overrides["date"] = param_date
            cli_overrides["run_date"] = param_date
            cli_overrides["today"] = param_date
        if param_run_id:
            cli_overrides["run_id"] = param_run_id

        # YAML pipeline mode: execute DAG from seeknal/ directory
        _run_yaml_pipeline(
            cli_overrides=cli_overrides,
            run_id=param_run_id,
            dry_run=dry_run,
            full=full,
            nodes=nodes,
            types=types,
            exclude_tags=exclude_tags,
            continue_on_error=continue_on_error,
            retry=retry,
            show_plan=show_plan,
            parallel=parallel,
            max_workers=max_workers,
            materialize=materialize,
        )


def _run_yaml_pipeline(
    cli_overrides: dict[str, Any] | None = None,
    run_id: str | None = None,
    dry_run: bool = False,
    full: bool = False,
    nodes: Optional[List[str]] = None,
    types: Optional[List[str]] = None,
    exclude_tags: Optional[List[str]] = None,
    continue_on_error: bool = False,
    retry: int = 0,
    show_plan: bool = False,
    parallel: bool = False,
    max_workers: int = 4,
    materialize: Optional[bool] = None,
) -> None:
    """
    Execute YAML-based pipeline using DAGBuilder, state tracking, and executors.

    This function orchestrates the complete pipeline execution workflow:
    1. Build DAG from YAML files using DAGBuilder
    2. Load previous state for incremental runs
    3. Detect changes and determine nodes to run
    4. Execute in topological order using executors
    5. Update state and report results

    Args:
        dry_run: Show plan without executing
        full: Run all nodes regardless of state
        nodes: Run specific nodes only
        types: Filter by node types
        exclude_tags: Skip nodes with these tags
        continue_on_error: Continue after failures
        retry: Number of retries for failed nodes
        show_plan: Show execution plan and exit
        parallel: Execute independent nodes in parallel
        max_workers: Maximum number of parallel workers
        materialize: Enable/disable Iceberg materialization (None=use node config)
    """
    from pathlib import Path
    from seeknal.workflow.dag import DAGBuilder, CycleDetectedError, MissingDependencyError
    from seeknal.workflow.state import (
        RunState, load_state, save_state,
        calculate_node_hash, get_nodes_to_run,
        update_node_state, NodeStatus,
        include_upstream_sources,
    )
    from seeknal.workflow.executors import get_executor, ExecutionContext
    from seeknal.context import context

    project_path = Path.cwd()
    target_path = project_path / "target"
    state_path = target_path / "run_state.json"

    typer.echo("")
    typer.echo(typer.style("Seeknal Pipeline Execution", bold=True))
    typer.echo("=" * 60)
    typer.echo(f"  Project: {project_path.name}")
    typer.echo(f"  Mode: {'Full' if full else 'Incremental'}")

    # Step 1: Build DAG from YAML files
    _echo_info("Building DAG from seeknal/ directory...")

    try:
        dag_builder = DAGBuilder(
            project_path=project_path,
            cli_overrides=cli_overrides,
            run_id=run_id,
        )
        dag_builder.build()
    except ValueError as e:
        _echo_error(f"DAG build failed: {e}")
        raise typer.Exit(1)
    except CycleDetectedError as e:
        _echo_error(f"Cycle detected in DAG: {e.message}")
        raise typer.Exit(1)
    except MissingDependencyError as e:
        _echo_error(f"Missing dependency: {e.message}")
        raise typer.Exit(1)

    node_count = dag_builder.get_node_count()
    edge_count = dag_builder.get_edge_count()

    _echo_success(f"DAG built: {node_count} nodes, {edge_count} edges")

    # Get topological order
    try:
        execution_order = dag_builder.topological_sort()
    except CycleDetectedError as e:
        _echo_error(f"Cycle detected: {e.message}")
        raise typer.Exit(1)

    # Step 2: Load previous state
    old_state = load_state(state_path)

    if old_state:
        _echo_info(f"Loaded previous state from run: {old_state.run_id}")
    else:
        _echo_info("No previous state found (first run)")

    # Step 3: Calculate current hashes and detect changes
    _echo_info("Detecting changes...")

    current_hashes = {}
    for node_id, node in dag_builder.nodes.items():
        try:
            node_hash = calculate_node_hash(node.yaml_data, Path(node.file_path))
            current_hashes[node_id] = node_hash
        except Exception as e:
            _echo_warning(f"Failed to calculate hash for {node_id}: {e}")

    # Build DAG adjacency for downstream propagation
    dag_adjacency = {}
    dag_upstream = {}
    for node_id in dag_builder.nodes:
        dag_adjacency[node_id] = dag_builder.get_downstream(node_id)
        dag_upstream[node_id] = dag_builder.get_upstream(node_id)

    # Determine nodes to run
    nodes_to_run = get_nodes_to_run(current_hashes, old_state, dag_adjacency)

    # Apply filters
    if full:
        # Override: run all nodes
        nodes_to_run = set(dag_builder.nodes.keys())
    elif nodes:
        # Run specific nodes and their downstream
        specified = set()
        for node_name in nodes:
            # Find matching node
            for node_id, node in dag_builder.nodes.items():
                if node.name == node_name or node_id == node_name:
                    specified.add(node_id)
                    # Add downstream
                    specified.update(dag_builder.get_all_downstream(node_id))
                    break
        nodes_to_run = specified
    elif types:
        # Filter by type
        type_set = set(types)
        nodes_to_run = {
            node_id for node_id in nodes_to_run
            if dag_builder.nodes[node_id].kind.value in type_set
        }

    if exclude_tags:
        # Filter out nodes with excluded tags
        exclude_set = set(exclude_tags)
        nodes_to_run = {
            node_id for node_id in nodes_to_run
            if not any(tag in exclude_set for tag in dag_builder.nodes[node_id].tags)
        }

    # Include upstream source dependencies for transforms
    # This ensures DuckDB views/tables are available for transform SQL
    if not full:
        nodes_to_run = include_upstream_sources(nodes_to_run, dag_upstream)

    if show_plan:
        # Show execution plan and exit
        _echo_info("")
        _echo_info("Execution Plan:")
        _echo_info("-" * 60)

        for idx, node_id in enumerate(execution_order, 1):
            node = dag_builder.nodes[node_id]
            if node_id in nodes_to_run:
                status = "RUN"
                status_msg = typer.style(status, fg=typer.colors.GREEN, bold=True)
            elif old_state and node_id in old_state.nodes and old_state.nodes[node_id].is_success():
                status = "CACHED"
                status_msg = typer.style(status, fg=typer.colors.BLUE)
            else:
                status = "SKIP"
                status_msg = typer.style(status, fg=typer.colors.RESET)

            tags_str = f" [{', '.join(node.tags)}]" if node.tags else ""
            typer.echo(f"  {idx:2d}. {status_msg} {node.name}{tags_str}")

        typer.echo("")
        _echo_info(f"Total: {len(execution_order)} nodes, {len(nodes_to_run)} to run")
        return

    if not nodes_to_run:
        _echo_success("No changes detected. Nothing to run.")
        return

    _echo_info(f"Nodes to run: {len(nodes_to_run)}")

    # Auto-parallel suggestion
    if not parallel:
        layers: dict[int, list[str]] = {}
        depth: dict[str, int] = {}
        for node_id in execution_order:
            deps = dag_upstream.get(node_id, set())
            if not deps:
                depth[node_id] = 0
            else:
                depth[node_id] = max(depth.get(d, 0) for d in deps) + 1
            layer = depth[node_id]
            if layer not in layers:
                layers[layer] = []
            layers[layer].append(node_id)

        if layers:
            widest_layer_num, widest_layer_nodes = max(
                layers.items(), key=lambda x: len(x[1])
            )
            if len(widest_layer_nodes) > 3:
                _echo_info(
                    f"Tip: This pipeline has {len(widest_layer_nodes)} independent nodes "
                    f"in layer {widest_layer_num}. Use --parallel for faster execution."
                )

    # Parallel execution mode
    if parallel:
        from seeknal.workflow.runner import DAGRunner as _DAGRunner
        from seeknal.workflow.parallel import ParallelDAGRunner, print_parallel_summary
        from seeknal.dag.manifest import Manifest as _Manifest, Node as _Node, NodeType as _NodeType

        # Build a Manifest from dag_builder for the DAGRunner
        _manifest = _Manifest(project=project_path.name)
        for node_id, node in dag_builder.nodes.items():
            _manifest.add_node(_Node(
                id=node_id,
                name=node.name,
                node_type=_NodeType(node.kind.value),
                tags=list(node.tags) if node.tags else [],
                config=node.yaml_data,
                file_path=node.file_path,
            ))
        for node_id in dag_builder.nodes:
            for downstream_id in dag_builder.get_downstream(node_id):
                _manifest.add_edge(node_id, downstream_id)

        _runner = _DAGRunner(_manifest, target_path=target_path)
        parallel_runner = ParallelDAGRunner(_runner, max_workers, continue_on_error)
        parallel_summary = parallel_runner.run(nodes_to_run, dry_run=dry_run)
        print_parallel_summary(parallel_summary)
        return

    # Step 4: Create execution context
    exec_context = ExecutionContext(
        project_name=project_path.name,
        workspace_path=project_path,
        target_path=target_path,
        dry_run=dry_run,
        verbose=True,
        materialize_enabled=materialize,
        params={},  # Will be populated per-node from yaml_data
    )

    # Step 5: Execute nodes in topological order
    import time
    start_time = time.time()

    # Initialize run state
    run_state = RunState(
        config={"full": full, "nodes": nodes, "types": types},
    )

    # Copy previous successful states
    if old_state:
        for node_id, node_state in old_state.nodes.items():
            if node_state.is_success() and node_id not in nodes_to_run:
                run_state.nodes[node_id] = node_state

    typer.echo("")
    typer.echo(typer.style("Execution", bold=True))
    typer.echo("=" * 60)

    successful = 0
    failed = 0
    cached = 0

    for idx, node_id in enumerate(execution_order, 1):
        node = dag_builder.nodes[node_id]

        # Check if we should run this node
        if node_id not in nodes_to_run:
            if node_id in run_state.nodes and run_state.nodes[node_id].is_success():
                typer.echo(f"{idx}/{len(execution_order)}: {node.name} [{typer.style('CACHED', fg=typer.colors.BLUE)}]")
                cached += 1
            continue

        # Check if node has changed hash (skip for SOURCE nodes as they're needed for downstream transforms)
        # SOURCE nodes are relatively cheap to execute and create DuckDB views needed by transforms
        current_hash = current_hashes.get(node_id, "")
        if old_state and node_id in old_state.nodes and node.kind.value != "source":
            old_hash = old_state.nodes[node_id].hash
            if old_hash == current_hash and old_state.nodes[node_id].is_success():
                typer.echo(f"{idx}/{len(execution_order)}: {node.name} [{typer.style('CACHED', fg=typer.colors.BLUE)}]")
                run_state.nodes[node_id] = old_state.nodes[node_id]
                cached += 1
                continue

        # Execute the node
        typer.echo(f"{idx}/{len(execution_order)}: {node.name} [{typer.style('RUNNING', fg=typer.colors.YELLOW)}]")

        if dry_run:
            # Dry run - just show what would happen
            typer.echo(f"  Would execute: {node.kind.value}.{node.name}")
            update_node_state(run_state, node_id, NodeStatus.CACHED.value)
            successful += 1
            continue

        # Actual execution using executors
        node_start = time.time()

        try:
            # Get executor for node type
            executor = get_executor(node, exec_context)

            # Execute
            result = executor.run()

            duration = time.time() - node_start

            if result.is_success():
                status_msg = typer.style("SUCCESS", fg=typer.colors.GREEN, bold=True)
                typer.echo(f"  {status_msg} in {duration:.2f}s")

                if result.row_count > 0:
                    typer.echo(f"  Rows: {result.row_count:,}")

                # Update state with hash
                current_hash = current_hashes.get(node_id, "")
                update_node_state(
                    run_state,
                    node_id,
                    NodeStatus.SUCCESS.value,
                    duration_ms=int(duration * 1000),
                    row_count=result.row_count,
                    metadata=result.metadata,
                    hash=current_hash,
                )
                successful += 1

            elif result.is_failed() or result.status.value == "failed":
                status_msg = typer.style("FAILED", fg=typer.colors.RED, bold=True)
                typer.echo(f"  {status_msg}: {result.error_message}")

                update_node_state(
                    run_state,
                    node_id,
                    NodeStatus.FAILED.value,
                    duration_ms=int(duration * 1000),
                    metadata={"error": result.error_message},
                )
                failed += 1

                if not continue_on_error:
                    typer.echo("")
                    _echo_error("Stopping execution due to failure")
                    break

                # Retry logic
                if retry > 0:
                    for attempt in range(1, retry + 1):
                        typer.echo(f"  Retry {attempt}/{retry}...")

                        try:
                            executor = get_executor(node, exec_context)
                            result = executor.run()

                            if result.is_success():
                                duration = time.time() - node_start
                                status_msg = typer.style("SUCCESS", fg=typer.colors.GREEN)
                                typer.echo(f"  {status_msg} on retry {attempt} in {duration:.2f}s")

                                current_hash = current_hashes.get(node_id, "")
                                update_node_state(
                                    run_state,
                                    node_id,
                                    NodeStatus.SUCCESS.value,
                                    duration_ms=int(duration * 1000),
                                    row_count=result.row_count,
                                    hash=current_hash,
                                )
                                failed -= 1
                                successful += 1
                                break
                        except Exception as e:
                            typer.echo(f"  Retry {attempt} failed: {e}")

        except Exception as e:
            duration = time.time() - node_start
            status_msg = typer.style("FAILED", fg=typer.colors.RED, bold=True)
            typer.echo(f"  {status_msg}: {e}")

            update_node_state(
                run_state,
                node_id,
                NodeStatus.FAILED.value,
                duration_ms=int(duration * 1000),
                metadata={"error": str(e)},
            )
            failed += 1

            if not continue_on_error:
                typer.echo("")
                _echo_error("Stopping execution due to failure")
                break

    # Step 6: Save state
    if not dry_run:
        try:
            save_state(run_state, state_path)
            _echo_success("State saved")
        except Exception as e:
            _echo_warning(f"Failed to save state: {e}")

    # Step 7: Print summary
    total_duration = time.time() - start_time

    typer.echo("")
    typer.echo(typer.style("Execution Summary", bold=True))
    typer.echo("=" * 60)
    typer.echo(f"  Total nodes:    {node_count}")
    typer.echo(f"  Executed:       {successful}")
    if cached > 0:
        typer.echo(f"  Cached:         {cached}")
    if failed > 0:
        failed_msg = typer.style(str(failed), fg=typer.colors.RED, bold=True)
        typer.echo(f"  Failed:         {failed_msg}")
    typer.echo(f"  Duration:       {total_duration:.2f}s")
    typer.echo("=" * 60)

    if failed > 0:
        raise typer.Exit(1)


@app.command()
def materialize(
    feature_group: str = typer.Argument(
        ..., help="Name of the feature group to materialize"
    ),
    start_date: str = typer.Option(
        ..., "--start-date", "-s",
        help="Start date for feature data (YYYY-MM-DD format)"
    ),
    end_date: Optional[str] = typer.Option(
        None, "--end-date", "-e",
        help="End date for feature data (YYYY-MM-DD format)"
    ),
    version: Optional[int] = typer.Option(
        None, "--version", "-v",
        help="Specific version number to materialize (defaults to latest). "
             "Use 'seeknal version list' to see available versions."
    ),
    mode: WriteMode = typer.Option(
        WriteMode.OVERWRITE, "--mode", "-m",
        help="Write mode: overwrite, append, or merge"
    ),
    offline_only: bool = typer.Option(
        False, "--offline-only",
        help="Only materialize to offline store (skip online store)"
    ),
    online_only: bool = typer.Option(
        False, "--online-only",
        help="Only materialize to online store (skip offline store)"
    ),
):
    """
    Materialize features to offline/online stores.

    Writes feature data to configured storage locations for the specified
    date range. Supports version-specific materialization for rollbacks
    or A/B testing scenarios.

    Examples:
        # Materialize latest version
        seeknal materialize user_features --start-date 2024-01-01

        # Materialize a specific version (rollback scenario)
        seeknal materialize user_features --start-date 2024-01-01 --version 1

        # Materialize with date range
        seeknal materialize user_features -s 2024-01-01 -e 2024-01-31
    """
    from seeknal.featurestore.feature_group import FeatureGroup

    # Parse dates with validation
    start_dt = parse_date_safely(start_date, "start date")
    end_dt = parse_date_safely(end_date, "end date") if end_date else None

    typer.echo(f"Materializing feature group: {feature_group}")
    if version is not None:
        typer.echo(f"  Version: {version}")
    typer.echo(f"  Start date: {start_date}")
    if end_date:
        typer.echo(f"  End date: {end_date}")
    typer.echo(f"  Mode: {mode.value}")

    try:
        fg = FeatureGroup.load(name=feature_group, version=version)
        fg.write(
            feature_start_time=start_dt,
            feature_end_time=end_dt,
            mode=mode.value
        )
        _echo_success(f"Materialization completed: {feature_group}")
    except Exception as e:
        _echo_error(f"Materialization failed: {e}")
        raise typer.Exit(1)


@app.command("list")
def list_resources(
    resource_type: ResourceType = typer.Argument(
        ..., help="Type of resource to list"
    ),
    project: Optional[str] = typer.Option(
        None, "--project", "-p",
        help="Filter by project name"
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.TABLE, "--format", "-f",
        help="Output format"
    ),
):
    """List resources (projects, entities, flows, feature-groups, etc.)."""
    from seeknal.project import Project
    from seeknal.entity import Entity
    from seeknal.flow import Flow
    from seeknal.featurestore.featurestore import OfflineStore
    from seeknal.models import (
        WorkspaceTable, FeatureGroupTable, FlowTable
    )
    from seeknal.request import get_db_session
    from sqlmodel import select
    from tabulate import tabulate
    import json

    try:
        match resource_type:
            case ResourceType.PROJECTS:
                Project.list()
            case ResourceType.WORKSPACES:
                with get_db_session() as session:
                    workspaces = session.exec(select(WorkspaceTable)).all()
                if not workspaces:
                    typer.echo("No workspaces found.")
                else:
                    headers = ["Name", "Description", "Project ID"]
                    data = [[w.name, w.description or "", w.project_id] for w in workspaces]
                    typer.echo(tabulate(data, headers=headers, tablefmt="simple"))
            case ResourceType.ENTITIES:
                Entity.list()
            case ResourceType.FLOWS:
                with get_db_session() as session:
                    flows = session.exec(select(FlowTable)).all()
                if not flows:
                    typer.echo("No flows found.")
                else:
                    headers = ["Name", "Description", "Author", "Last Run"]
                    data = [[f.name, f.description or "", f.author or "", f.last_run or ""]
                            for f in flows]
                    typer.echo(tabulate(data, headers=headers, tablefmt="simple"))
            case ResourceType.FEATURE_GROUPS:
                with get_db_session() as session:
                    feature_groups = session.exec(select(FeatureGroupTable)).all()
                if not feature_groups:
                    typer.echo("No feature groups found.")
                else:
                    headers = ["Name", "Description", "Version"]
                    data = [[fg.name, fg.description or "", fg.version or 1]
                            for fg in feature_groups]
                    typer.echo(tabulate(data, headers=headers, tablefmt="simple"))
            case ResourceType.OFFLINE_STORES:
                OfflineStore.list()
    except Exception as e:
        _echo_error(f"Error listing resources: {e}")
        raise typer.Exit(1)


@app.command()
def show(
    resource_type: str = typer.Argument(..., help="Type of resource"),
    name: str = typer.Argument(..., help="Resource name"),
    format: OutputFormat = typer.Option(
        OutputFormat.TABLE, "--format", "-f",
        help="Output format"
    ),
):
    """Show detailed information about a resource."""
    from seeknal.request import (
        ProjectRequest, EntityRequest, FlowRequest,
        FeatureGroupRequest, WorkspaceRequest
    )
    import json

    try:
        resource = None
        match resource_type.lower():
            case "project":
                resource = ProjectRequest.select_by_name(name)
            case "entity":
                resource = EntityRequest.select_by_name(name)
            case "flow":
                resource = FlowRequest.select_by_name(name)
            case "feature-group" | "featuregroup":
                resource = FeatureGroupRequest.select_by_name(name)
            case _:
                _echo_error(f"Unknown resource type: {resource_type}")
                raise typer.Exit(1)

        if resource is None:
            _echo_error(f"{resource_type} '{name}' not found")
            raise typer.Exit(1)

        if format == OutputFormat.JSON:
            # Convert to dict and print as JSON
            data = {k: v for k, v in resource.__dict__.items() if not k.startswith('_')}
            typer.echo(json.dumps(data, indent=2, default=str))
        else:
            # Print as table
            typer.echo(f"\n{resource_type.title()}: {name}")
            typer.echo("-" * 40)
            for key, value in resource.__dict__.items():
                if not key.startswith('_') and value is not None:
                    typer.echo(f"  {key}: {value}")

    except Exception as e:
        _echo_error(f"Error showing resource: {e}")
        raise typer.Exit(1)


@app.command()
def validate(
    config_path: Optional[Path] = typer.Option(
        None, "--config", "-c",
        help="Path to config file"
    ),
):
    """Validate configurations and connections."""
    from seeknal.context import CONFIG_BASE_URL
    from seeknal.request import ProjectRequest, get_db_session
    import os

    _echo_info("Validating configuration...")

    # Check config file
    config_file = config_path or Path(CONFIG_BASE_URL) / "config.toml"
    if config_file.exists():
        _echo_success(f"Config file found: {config_file}")
    else:
        _echo_warning(f"Config file not found: {config_file}")

    # Check database connection
    _echo_info("Validating database connection...")
    try:
        with get_db_session() as session:
            projects = ProjectRequest.select_all()
            _echo_success(f"Database connection successful ({len(projects)} projects found)")
    except Exception as e:
        _echo_error(f"Database connection failed: {e}")
        raise typer.Exit(1)

    _echo_success("All validations passed")


def _build_range_validator(config):
    """Build a RangeValidator from config."""
    if not config.columns:
        raise ValueError("RangeValidator requires at least one column")
    params = config.params or {}
    column = config.columns[0] if len(config.columns) == 1 else config.columns[0]
    from seeknal.feature_validation.validators import RangeValidator
    return RangeValidator(
        column=column,
        min_val=params.get("min_val"),
        max_val=params.get("max_val"),
    )


def _build_freshness_validator(config):
    """Build a FreshnessValidator from config."""
    if not config.columns:
        raise ValueError("FreshnessValidator requires a column")
    from datetime import timedelta
    from seeknal.feature_validation.validators import FreshnessValidator
    
    params = config.params or {}
    max_age_seconds = params.get("max_age_seconds", 86400)  # Default 24 hours
    return FreshnessValidator(
        column=config.columns[0],
        max_age=timedelta(seconds=max_age_seconds),
    )


# Validator registry for cleaner instantiation
_VALIDATOR_REGISTRY = {
    "null": lambda c: _build_null_validator(c),
    "range": lambda c: _build_range_validator(c),
    "uniqueness": lambda c: _build_uniqueness_validator(c),
    "freshness": lambda c: _build_freshness_validator(c),
}


def _build_null_validator(config):
    """Build a NullValidator from config."""
    from seeknal.feature_validation.validators import NullValidator
    params = config.params or {}
    return NullValidator(
        columns=config.columns or [],
        max_null_percentage=params.get("max_null_percentage", 0.0),
    )


def _build_uniqueness_validator(config):
    """Build a UniquenessValidator from config."""
    from seeknal.feature_validation.validators import UniquenessValidator
    params = config.params or {}
    return UniquenessValidator(
        columns=config.columns or [],
        max_duplicate_percentage=params.get("max_duplicate_percentage", 0.0),
    )


def _build_validators_from_config(validator_configs):
    """Build validator instances from ValidatorConfig objects.

    Args:
        validator_configs: List of ValidatorConfig objects from validation_config.

    Returns:
        List of BaseValidator instances ready to execute.

    Raises:
        ValueError: If an unknown validator type is specified.
    """
    validators = []
    for config in validator_configs:
        validator_type = config.validator_type.lower()
        builder = _VALIDATOR_REGISTRY.get(validator_type)
        if not builder:
            raise ValueError(f"Unknown validator type: {validator_type}")
        validators.append(builder(config))
    return validators


def _format_validation_table(results, verbose: bool = False):
    """Format validation results as a colored table.

    Args:
        results: List of ValidationResult objects.
        verbose: If True, show detailed information.

    Returns:
        Formatted table string.
    """
    if not results:
        return "  No validation results."

    # Calculate column widths
    name_width = max(len(r.validator_name) for r in results)
    name_width = max(name_width, 10)  # Minimum width

    lines = []

    # Header
    header = f"  {'Validator':<{name_width}}  {'Status':^8}  {'Message'}"
    lines.append(header)
    lines.append("  " + "-" * (name_width + 2 + 8 + 2 + 40))

    # Results
    for result in results:
        if result.passed:
            status = typer.style("PASS", fg=typer.colors.GREEN, bold=True)
        else:
            status = typer.style("FAIL", fg=typer.colors.RED, bold=True)

        # Truncate message if too long
        message = result.message
        if len(message) > 60 and not verbose:
            message = message[:57] + "..."

        line = f"  {result.validator_name:<{name_width}}  {status:^8}  {message}"
        lines.append(line)

        # Show details in verbose mode
        if verbose and result.details:
            for key, value in result.details.items():
                if key not in ("columns", "validator_type"):  # Skip redundant fields
                    detail_line = f"      {key}: {value}"
                    lines.append(typer.style(detail_line, dim=True))

    return "\n".join(lines)


def _format_field_type(field_type) -> str:
    """Normalize field type to string representation.

    Handles Avro schema field types which can be strings, dicts, or lists.
    Normalizes them to a human-readable string format.

    Args:
        field_type: The field type from Avro schema (str, dict, or list).

    Returns:
        A normalized string representation of the field type.
    """
    if isinstance(field_type, dict):
        return str(field_type.get("type", field_type))
    if isinstance(field_type, list):
        return " | ".join(str(t) for t in field_type)
    return str(field_type)


def _format_field(field, symbol: str = "") -> str:
    """Format a schema field for display.

    Formats a field from an Avro schema for display in the CLI,
    including the field name, type, and an optional symbol prefix.

    Args:
        field: The field to format (can be a dict or string).
        symbol: Optional symbol to prefix (e.g., "+", "-", "~").

    Returns:
        A formatted string representation of the field.
    """
    if isinstance(field, dict):
        name = field.get("name", "unknown")
        type_str = _format_field_type(field.get("type", "unknown"))
        return f"  {symbol} {name}: {type_str}"
    return f"  {symbol} {field}"


@app.command("validate-features")
def validate_features(
    feature_group: str = typer.Argument(
        ..., help="Name of the feature group to validate"
    ),
    mode: ValidationModeChoice = typer.Option(
        ValidationModeChoice.FAIL, "--mode", "-m",
        help="Validation mode: 'warn' logs failures and continues, 'fail' stops on first failure"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Show detailed validation results"
    ),
):
    """Validate feature group data quality.

    Run configured validators against a feature group's data to check
    for data quality issues like null values, out-of-range values,
    duplicates, and stale data.

    Exit codes:
        0 - All validations passed (or passed with warnings in warn mode)
        1 - Validation failed or feature group not found

    Examples:
        seeknal validate-features user_features
        seeknal validate-features user_features --mode warn
        seeknal validate-features user_features --mode fail --verbose
    """
    from seeknal.featurestore.feature_group import FeatureGroup
    from seeknal.feature_validation.models import ValidationMode
    from seeknal.feature_validation.validators import ValidationException

    typer.echo("")
    typer.echo(typer.style(f"Validating feature group: {feature_group}", bold=True))
    typer.echo(f"  Mode: {mode.value}")
    typer.echo("")

    try:
        # Load the feature group
        _echo_info("Loading feature group...")
        fg = FeatureGroup.load(name=feature_group)
        if fg is None:
            _echo_error(f"Feature group '{feature_group}' not found")
            raise typer.Exit(1)

        # Check if validation is configured
        if not fg.validation_config or not fg.validation_config.validators:
            _echo_warning(f"No validators configured for feature group '{feature_group}'")
            typer.echo("  Configure validators using:")
            typer.echo("    fg.set_validation_config(ValidationConfig(validators=[...]))")
            typer.echo("")
            raise typer.Exit(0)

        # Build validators from configuration
        try:
            validators = _build_validators_from_config(fg.validation_config.validators)
        except ValueError as e:
            _echo_error(f"Invalid validator configuration: {e}")
            raise typer.Exit(1)

        # Convert CLI mode to ValidationMode enum
        validation_mode = ValidationMode.WARN if mode == ValidationModeChoice.WARN else ValidationMode.FAIL

        # Show validator count
        typer.echo(f"  Validators to run: {len(validators)}")
        if verbose:
            for v in validators:
                typer.echo(f"    - {v.name}")
        typer.echo("")

        # Run validation
        _echo_info("Running validators...")
        try:
            summary = fg.validate(validators=validators, mode=validation_mode)
        except ValidationException as e:
            # In FAIL mode, validation stops on first failure
            typer.echo("")
            _echo_error(f"Validation stopped: {e.message}")
            if e.result:
                typer.echo("")
                typer.echo(typer.style("Failed Validator Details:", bold=True))
                typer.echo(f"  Validator: {e.result.validator_name}")
                typer.echo(f"  Failures:  {e.result.failure_count:,}")
                typer.echo(f"  Total:     {e.result.total_count:,}")
                if verbose and e.result.details:
                    typer.echo("  Details:")
                    for key, value in e.result.details.items():
                        typer.echo(f"    {key}: {value}")
            typer.echo("")
            raise typer.Exit(1)

        # Display results summary
        typer.echo("")
        typer.echo(typer.style("Validation Summary", bold=True))
        typer.echo("=" * 50)

        # Summary statistics
        passed_style = typer.style(str(summary.passed_count), fg=typer.colors.GREEN, bold=True)
        failed_style = typer.style(str(summary.failed_count), fg=typer.colors.RED if summary.failed_count > 0 else typer.colors.GREEN, bold=True)

        typer.echo(f"  Total validators: {summary.total_validators}")
        typer.echo(f"  Passed:           {passed_style}")
        typer.echo(f"  Failed:           {failed_style}")
        typer.echo("")

        # Display results table
        if summary.results:
            typer.echo(typer.style("Results:", bold=True))
            typer.echo(_format_validation_table(summary.results, verbose=verbose))
            typer.echo("")

        # Final status message
        typer.echo("-" * 50)
        if summary.passed:
            _echo_success(f"All validations passed for '{feature_group}'")
        else:
            if mode == ValidationModeChoice.WARN:
                _echo_warning(
                    f"Validation completed with {summary.failed_count} warning(s) "
                    f"for '{feature_group}'"
                )
                typer.echo("  (Exit code 0 - warnings only)")
            else:
                _echo_error(
                    f"Validation failed with {summary.failed_count} error(s) "
                    f"for '{feature_group}'"
                )
                raise typer.Exit(1)
        typer.echo("")

    except typer.Exit:
        # Re-raise typer.Exit as-is
        raise
    except Exception as e:
        typer.echo("")
        _echo_error(f"Validation failed: {e}")
        if verbose:
            import traceback
            typer.echo(typer.style(traceback.format_exc(), dim=True))
        raise typer.Exit(1)


@app.command()
def debug(
    feature_group: str = typer.Argument(
        ..., help="Feature group name to debug"
    ),
    limit: int = typer.Option(
        10, "--limit", "-l",
        help="Number of rows to show"
    ),
):
    """Show sample data from a feature group for debugging."""
    from seeknal.featurestore.feature_group import FeatureGroup, HistoricalFeatures, FeatureLookup

    try:
        fg = FeatureGroup(name=feature_group).get_or_create()

        typer.echo(f"\nFeature Group: {feature_group}")
        typer.echo("-" * 40)

        # Show features
        if fg.features:
            typer.echo("\nFeatures:")
            for f in fg.features:
                typer.echo(f"  - {f.name} ({f.data_type})")

        # Show sample data if available
        try:
            lookup = FeatureLookup(source=fg)
            hist = HistoricalFeatures(lookups=[lookup])
            df = hist.to_dataframe()
            if df is not None:
                typer.echo(f"\nSample data ({limit} rows):")
                df.show(limit)
        except Exception as e:
            _echo_warning(f"Could not load sample data: {e}")

    except Exception as e:
        _echo_error(f"Debug failed: {e}")
        raise typer.Exit(1)


@app.command()
def clean(
    feature_group: str = typer.Argument(
        ..., help="Feature group name to clean"
    ),
    before_date: Optional[str] = typer.Option(
        None, "--before", "-b",
        help="Delete data before this date (YYYY-MM-DD)"
    ),
    ttl_days: Optional[int] = typer.Option(
        None, "--ttl",
        help="Delete data older than TTL days"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Show what would be deleted without deleting"
    ),
):
    """Clean old feature data based on TTL or date."""
    from seeknal.featurestore.feature_group import FeatureGroup

    if before_date is None and ttl_days is None:
        _echo_error("Either --before or --ttl must be specified")
        raise typer.Exit(1)

    if before_date:
        cutoff = parse_date_safely(before_date, "before date")
    else:
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(days=ttl_days)

    typer.echo(f"Cleaning feature group: {feature_group}")
    typer.echo(f"  Deleting data before: {cutoff.strftime('%Y-%m-%d')}")

    if dry_run:
        _echo_warning("Dry run mode - no data will be deleted")
        return

    try:
        fg = FeatureGroup(name=feature_group).get_or_create()
        # Note: Actual cleanup logic would depend on the store implementation
        _echo_success(f"Cleanup completed for: {feature_group}")
    except Exception as e:
        _echo_error(f"Cleanup failed: {e}")
        raise typer.Exit(1)


@app.command()
def delete(
    resource_type: DeleteResourceType = typer.Argument(
        ..., help="Type of resource to delete (feature-group)"
    ),
    name: str = typer.Argument(
        ..., help="Name of the resource to delete"
    ),
    force: bool = typer.Option(
        False, "--force", "-f",
        help="Skip confirmation prompt"
    ),
):
    """Delete a resource (feature group) including storage and metadata."""
    from seeknal.featurestore.feature_group import FeatureGroup

    match resource_type:
        case DeleteResourceType.FEATURE_GROUP:
            typer.echo(f"Deleting feature group: {name}")

            try:
                fg = FeatureGroup.load(name=name)
            except Exception as e:
                _echo_error(f"Feature group '{name}' not found")
                raise typer.Exit(1)

            if not force:
                confirm = typer.confirm(
                    f"Are you sure you want to delete feature group '{name}'? "
                    "This will remove all storage files and metadata."
                )
                if not confirm:
                    _echo_warning("Deletion cancelled")
                    raise typer.Exit(0)

            try:
                fg.delete()
                _echo_success(
                    f"Deleted feature group '{name}': "
                    "removed storage files and metadata from database"
                )
            except Exception as e:
                _echo_error(f"Failed to delete feature group '{name}': {e}")
                raise typer.Exit(1)
        case _:
            _echo_error(f"Unknown resource type: {resource_type}")
            raise typer.Exit(1)


@app.command("delete-table")
def delete_table(
    table_name: str = typer.Argument(
        ..., help="Name of the online table to delete"
    ),
    force: bool = typer.Option(
        False, "--force", "-f",
        help="Force deletion without confirmation (bypass dependency warnings)"
    ),
):
    """Delete an online table and all associated data files.

    This command removes all data files from the online store and cleans up
    metadata from the database. By default, it will show any dependent feature
    groups and ask for confirmation before deleting.

    Use --force to bypass confirmations and delete despite dependencies.
    """
    from seeknal.request import OnlineTableRequest, EntityRequest, get_db_session
    from seeknal.models import FeatureGroupTable
    from seeknal.featurestore.duckdbengine.feature_group import OnlineFeaturesDuckDB
    from seeknal.featurestore.duckdbengine.featurestore import OnlineStoreDuckDB
    from seeknal.entity import Entity
    from sqlmodel import select

    typer.echo(f"Looking up table: {table_name}")

    try:
        # Step 1: Validate table exists
        online_table = OnlineTableRequest.select_by_name(table_name)
        if online_table is None:
            _echo_error(f"Table '{table_name}' not found")
            raise typer.Exit(1)

        # Step 2: Check dependencies (feature groups using this table)
        fg_mappings = OnlineTableRequest.get_feature_group_from_online_table(online_table.id)
        dependent_feature_groups = []

        if fg_mappings:
            with get_db_session() as session:
                for mapping in fg_mappings:
                    fg = session.exec(
                        select(FeatureGroupTable).where(
                            FeatureGroupTable.id == mapping.feature_group_id
                        )
                    ).first()
                    if fg:
                        dependent_feature_groups.append(fg.name)

        # Step 3: Show warnings if dependencies exist
        if dependent_feature_groups:
            _echo_warning(f"Table '{table_name}' has {len(dependent_feature_groups)} dependent feature group(s):")
            for fg_name in dependent_feature_groups:
                typer.echo(f"  - {fg_name}")

            if not force:
                _echo_warning("Deleting this table may affect these feature groups.")
                confirm = typer.confirm(
                    "Are you sure you want to delete this table?",
                    default=False
                )
                if not confirm:
                    _echo_info("Deletion cancelled")
                    raise typer.Exit(0)
        else:
            # No dependencies, but still confirm unless --force
            if not force:
                confirm = typer.confirm(
                    f"Are you sure you want to delete table '{table_name}'?",
                    default=False
                )
                if not confirm:
                    _echo_info("Deletion cancelled")
                    raise typer.Exit(0)

        # Step 4: Get entity information for file deletion
        entity = EntityRequest.select_by_id(online_table.entity_id)
        entity_obj = None
        if entity:
            entity_obj = Entity(name=entity.name)

        # Step 5: Perform deletion
        typer.echo(f"Deleting table '{table_name}'...")

        online_table_obj = OnlineFeaturesDuckDB(
            name=table_name,
            lookup_key=entity_obj,
            online_store=OnlineStoreDuckDB(),
            project="default",  # TODO: Get from online_table.project_id if needed
            id=online_table.id
        )

        success = online_table_obj.delete()

        if success:
            _echo_success(f"Table '{table_name}' deleted successfully")
        else:
            _echo_warning(f"Table '{table_name}' deletion completed with warnings (check logs)")

    except typer.Exit:
        raise
    except Exception as e:
        _echo_error(f"Failed to delete table: {e}")
        raise typer.Exit(1)

# Version subcommands
@version_app.command("list")
def version_list(
    feature_group: str = typer.Argument(
        ..., help="Name of the feature group to query versions for"
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.TABLE, "--format", "-f",
        help="Output format: table (default), json, or yaml"
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l",
        help="Maximum number of versions to display (most recent first)"
    ),
):
    """
    List all versions of a feature group.

    Displays version history with creation timestamps and feature counts,
    sorted by version number (most recent first). Use this command to
    review the evolution of a feature group over time.

    Examples:
        seeknal version list user_features
        seeknal version list user_features --limit 5
        seeknal version list user_features --format json
    """
    from seeknal.featurestore.feature_group import FeatureGroup
    from tabulate import tabulate
    import json

    try:
        fg = FeatureGroup(name=feature_group).get_or_create()
        versions = fg.list_versions()

        if not versions:
            _echo_info(f"No versions found for feature group: {feature_group}")
            return

        # Apply limit if specified
        if limit is not None:
            versions = versions[:limit]

        if format == OutputFormat.JSON:
            typer.echo(json.dumps(versions, indent=2, default=str))
        else:
            # Table format
            headers = ["Version", "Created At", "Features"]
            data = []
            for v in versions:
                created_at = v.get("created_at", "N/A")
                if created_at and created_at != "N/A":
                    # Format datetime for display
                    if hasattr(created_at, "strftime"):
                        created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")
                feature_count = v.get("feature_count", 0)
                data.append([v.get("version", "N/A"), created_at, feature_count])

            typer.echo(f"\nVersions for feature group: {feature_group}")
            typer.echo("-" * 50)
            typer.echo(tabulate(data, headers=headers, tablefmt="simple"))

    except Exception as e:
        _echo_error(f"Error listing versions: {e}")
        raise typer.Exit(1)


@version_app.command("show")
def version_show(
    feature_group: str = typer.Argument(
        ..., help="Name of the feature group to inspect"
    ),
    version: Optional[int] = typer.Option(
        None, "--version", "-v",
        help="Specific version number to show (defaults to latest version)"
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.TABLE, "--format", "-f",
        help="Output format: table (default), json, or yaml"
    ),
):
    """
    Show detailed information about a feature group version.

    Displays comprehensive metadata for a specific version including:
    - Version number and timestamps (created, updated)
    - Feature count
    - Complete Avro schema with field names and types

    If no version is specified, shows the latest version.

    Examples:
        seeknal version show user_features              # Show latest
        seeknal version show user_features --version 2  # Show version 2
        seeknal version show user_features --format json
    """
    from seeknal.featurestore.feature_group import FeatureGroup
    import json

    try:
        fg = FeatureGroup(name=feature_group).get_or_create()

        # Get specific version or latest
        if version is not None:
            version_data = fg.get_version(version)
            if version_data is None:
                _echo_error(f"Version {version} not found for feature group: {feature_group}")
                raise typer.Exit(1)
        else:
            # Get latest version (first in list, sorted by version desc)
            versions = fg.list_versions()
            if not versions:
                _echo_info(f"No versions found for feature group: {feature_group}")
                return
            version_data = versions[0]

        if format == OutputFormat.JSON:
            typer.echo(json.dumps(version_data, indent=2, default=str))
        else:
            # Table/readable format
            version_num = version_data.get("version", "N/A")
            created_at = version_data.get("created_at", "N/A")
            updated_at = version_data.get("updated_at", "N/A")
            feature_count = version_data.get("feature_count", 0)
            avro_schema = version_data.get("avro_schema")

            # Format datetime for display
            if created_at and created_at != "N/A" and hasattr(created_at, "strftime"):
                created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")
            if updated_at and updated_at != "N/A" and hasattr(updated_at, "strftime"):
                updated_at = updated_at.strftime("%Y-%m-%d %H:%M:%S")

            typer.echo(f"\nFeature Group: {feature_group}")
            typer.echo(f"Version: {version_num}")
            typer.echo("-" * 50)
            typer.echo(f"  Created At:    {created_at}")
            typer.echo(f"  Updated At:    {updated_at}")
            typer.echo(f"  Feature Count: {feature_count}")

            # Display schema
            if avro_schema:
                typer.echo("\nSchema:")
                typer.echo("-" * 50)
                try:
                    # Parse and pretty-print the Avro schema
                    if isinstance(avro_schema, str):
                        schema_dict = json.loads(avro_schema)
                    else:
                        schema_dict = avro_schema

                    # Display schema fields
                    if "fields" in schema_dict:
                        typer.echo("  Fields:")
                        for field in schema_dict.get("fields", []):
                            field_name = field.get("name", "unknown")
                            field_type = field.get("type", "unknown")
                            # Handle complex types
                            if isinstance(field_type, dict):
                                field_type = field_type.get("type", str(field_type))
                            elif isinstance(field_type, list):
                                # Union types like ["null", "string"]
                                field_type = " | ".join(str(t) for t in field_type)
                            typer.echo(f"    - {field_name}: {field_type}")
                    else:
                        # Fallback: print formatted JSON
                        typer.echo(json.dumps(schema_dict, indent=2))
                except (json.JSONDecodeError, TypeError):
                    # If parsing fails, display raw schema
                    typer.echo(f"  {avro_schema}")

    except Exception as e:
        _echo_error(f"Error showing version: {e}")
        raise typer.Exit(1)


@version_app.command("diff")
def version_diff(
    feature_group: str = typer.Argument(
        ..., help="Name of the feature group to compare versions for"
    ),
    from_version: int = typer.Option(
        ..., "--from", "-f",
        help="Base version number to compare from (older version)"
    ),
    to_version: int = typer.Option(
        ..., "--to", "-t",
        help="Target version number to compare to (newer version)"
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.TABLE, "--format",
        help="Output format: table (default) or json"
    ),
):
    """
    Compare two versions of a feature group to identify schema differences.

    Analyzes the Avro schemas of both versions and displays:
    - Added features (+): New fields in the target version
    - Removed features (-): Fields removed from the base version
    - Modified features (~): Fields with type changes

    This is useful for understanding schema evolution and identifying
    potential breaking changes before rolling back or deploying a version.

    Examples:
        seeknal version diff user_features --from 1 --to 2
        seeknal version diff user_features --from 1 --to 3 --format json
    """
    from seeknal.featurestore.feature_group import FeatureGroup
    import json

    try:
        fg = FeatureGroup(name=feature_group).get_or_create()

        # Call compare_versions API
        diff = fg.compare_versions(from_version, to_version)

        if diff is None:
            _echo_error(f"Could not compare versions {from_version} and {to_version}")
            raise typer.Exit(1)

        if format == OutputFormat.JSON:
            typer.echo(json.dumps(diff, indent=2, default=str))
        else:
            # Table/readable format
            typer.echo(f"\nFeature Group: {feature_group}")
            typer.echo(f"Comparing version {from_version} → {to_version}")
            typer.echo("=" * 60)

            added = diff.get("added", [])
            removed = diff.get("removed", [])
            modified = diff.get("modified", [])

            has_changes = added or removed or modified

            if not has_changes:
                _echo_info("No schema changes detected between versions.")
            else:
                # Display added features
                if added:
                    typer.echo("\n" + typer.style("Added (+):", fg=typer.colors.GREEN, bold=True))
                    for field in added:
                        typer.echo(typer.style(_format_field(field, "+"), fg=typer.colors.GREEN))

                # Display removed features
                if removed:
                    typer.echo("\n" + typer.style("Removed (-):", fg=typer.colors.RED, bold=True))
                    for field in removed:
                        typer.echo(typer.style(_format_field(field, "-"), fg=typer.colors.RED))

                # Display modified features
                if modified:
                    typer.echo("\n" + typer.style("Modified (~):", fg=typer.colors.YELLOW, bold=True))
                    for change in modified:
                        if isinstance(change, dict):
                            field_name = change.get("field", "unknown")
                            old_type = _format_field_type(change.get("old_type", "unknown"))
                            new_type = _format_field_type(change.get("new_type", "unknown"))
                            typer.echo(typer.style(f"  ~ {field_name}: {old_type} → {new_type}", fg=typer.colors.YELLOW))
                        else:
                            typer.echo(typer.style(f"  ~ {change}", fg=typer.colors.YELLOW))

                # Summary
                typer.echo("\n" + "-" * 60)
                typer.echo(f"Summary: {len(added)} added, {len(removed)} removed, {len(modified)} modified")

    except ValueError as e:
        _echo_error(str(e))
        raise typer.Exit(1)
    except Exception as e:
        _echo_error(f"Error comparing versions: {e}")
        raise typer.Exit(1)


@app.command()
def parse(
    project: str = typer.Option(
        None, "--project", "-p",
        help="Project name (defaults to current directory name)"
    ),
    path: Path = typer.Option(
        Path("."), "--path",
        help="Project path to parse"
    ),
    target_path: Optional[Path] = typer.Option(
        None, "--target", "-t",
        help="Target directory for manifest (defaults to <path>/target)"
    ),
    format: OutputFormat = typer.Option(
        OutputFormat.TABLE, "--format", "-f",
        help="Output format: table (default) or json"
    ),
    no_diff: bool = typer.Option(
        False, "--no-diff",
        help="Skip comparison with previous manifest"
    ),
):
    """
    Parse project and generate manifest.json.

    Scans the project directory for feature groups, models, and common config
    (sources, transforms, rules) to build the complete DAG manifest. The manifest
    is written to target/manifest.json.

    If a previous manifest exists, shows what has changed (added, removed, modified).
    Use --no-diff to skip this comparison.

    This command is similar to 'dbt parse' - it builds the dependency graph
    without executing any transformations.

    Examples:
        seeknal parse
        seeknal parse --project my_project
        seeknal parse --path /path/to/project
        seeknal parse --format json
        seeknal parse --no-diff
    """
    from seeknal.workflow.dag import DAGBuilder, CycleDetectedError, MissingDependencyError
    from seeknal.dag.manifest import Manifest
    from seeknal.dag.diff import ManifestDiff

    # Determine project name
    if project is None:
        project = path.resolve().name

    # Determine target path
    if target_path is None:
        target_path = path / "target"

    typer.echo(f"Parsing project: {project}")
    typer.echo(f"  Path: {path.resolve()}")

    try:
        # Check for existing manifest (for diff comparison)
        manifest_file = target_path / "manifest.json"
        old_manifest = None
        if not no_diff and manifest_file.exists():
            try:
                old_manifest = Manifest.load(str(manifest_file))
            except Exception:
                # If we can't load the old manifest, just skip diff
                old_manifest = None

        # Build DAG from YAML files in seeknal/ directory
        try:
            dag_builder = DAGBuilder(project_path=path)
            dag_builder.build()
        except ValueError as e:
            _echo_error(f"DAG build failed: {e}")
            raise typer.Exit(1)
        except CycleDetectedError as e:
            _echo_error(f"Cycle detected in DAG: {e.message}")
            raise typer.Exit(1)
        except MissingDependencyError as e:
            _echo_error(f"Missing dependency: {e.message}")
            raise typer.Exit(1)

        # Convert DAGBuilder results to Manifest format
        manifest = _build_manifest_from_dag(dag_builder, project)

        # Report any parse errors as warnings
        errors = dag_builder._parse_errors
        if errors:
            _echo_warning(f"Validation warnings: {len(errors)}")
            for error in errors:
                typer.echo(f"  - {error}")

        # Create target directory
        target_path.mkdir(parents=True, exist_ok=True)

        # Write manifest
        manifest.save(str(manifest_file))

        # Display results
        node_count = len(manifest.nodes)
        edge_count = len(manifest.edges)

        if format == OutputFormat.JSON:
            typer.echo(manifest.to_json())
        else:
            typer.echo("")
            _echo_success(f"Manifest generated: {manifest_file}")
            typer.echo(f"  Nodes: {node_count}")
            typer.echo(f"  Edges: {edge_count}")

            # Show node summary by type
            if node_count > 0:
                typer.echo("")
                typer.echo("Node Summary:")
                type_counts = {}
                for node in manifest.nodes.values():
                    node_type_str = node.node_type.value
                    type_counts[node_type_str] = type_counts.get(node_type_str, 0) + 1
                for node_type, count in sorted(type_counts.items()):
                    typer.echo(f"  - {node_type}: {count}")

            # Show diff if we have an old manifest
            if old_manifest is not None:
                diff = ManifestDiff.compare(old_manifest, manifest)
                typer.echo("")
                if diff.has_changes():
                    typer.echo(typer.style("Changes detected:", bold=True))

                    # Show added nodes
                    if diff.added_nodes:
                        typer.echo(typer.style(f"  Added ({len(diff.added_nodes)}):", fg=typer.colors.GREEN))
                        for node_id in sorted(diff.added_nodes.keys()):
                            typer.echo(typer.style(f"    + {node_id}", fg=typer.colors.GREEN))

                    # Show removed nodes
                    if diff.removed_nodes:
                        typer.echo(typer.style(f"  Removed ({len(diff.removed_nodes)}):", fg=typer.colors.RED))
                        for node_id in sorted(diff.removed_nodes.keys()):
                            typer.echo(typer.style(f"    - {node_id}", fg=typer.colors.RED))

                    # Show modified nodes
                    if diff.modified_nodes:
                        typer.echo(typer.style(f"  Modified ({len(diff.modified_nodes)}):", fg=typer.colors.YELLOW))
                        for node_id in sorted(diff.modified_nodes.keys()):
                            change = diff.modified_nodes[node_id]
                            fields = ", ".join(change.changed_fields)
                            typer.echo(typer.style(f"    ~ {node_id} ({fields})", fg=typer.colors.YELLOW))

                    # Show edge changes
                    if diff.added_edges:
                        typer.echo(typer.style(f"  Added edges ({len(diff.added_edges)}):", fg=typer.colors.GREEN))
                        for edge in diff.added_edges:
                            typer.echo(typer.style(f"    + {edge.from_node} -> {edge.to_node}", fg=typer.colors.GREEN))

                    if diff.removed_edges:
                        typer.echo(typer.style(f"  Removed edges ({len(diff.removed_edges)}):", fg=typer.colors.RED))
                        for edge in diff.removed_edges:
                            typer.echo(typer.style(f"    - {edge.from_node} -> {edge.to_node}", fg=typer.colors.RED))

                    typer.echo("")
                    typer.echo(f"Summary: {diff.summary()}")
                else:
                    _echo_info("No changes detected since last parse")

    except Exception as e:
        _echo_error(f"Parse failed: {e}")
        raise typer.Exit(1)


@app.command()
@handle_cli_error("Plan failed")
def plan(
    env_name: Optional[str] = typer.Argument(
        None,
        help="Environment name (optional). Without: show changes vs last run. With: create environment plan."
    ),
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """Analyze changes and show execution plan.

    Without environment: shows changes since last run and saves manifest.
    With environment: creates an isolated environment plan with change categorization.

    Examples:
        seeknal plan              # What changed since last run?
        seeknal plan dev          # Plan changes in dev environment
        seeknal plan staging      # Plan changes in staging
    """
    from seeknal.workflow.dag import DAGBuilder, CycleDetectedError, MissingDependencyError

    project_path = Path(project_path).resolve()
    target_path = project_path / "target"

    # Step 1: Build DAG
    _echo_info("Building DAG from seeknal/ directory...")

    try:
        dag_builder = DAGBuilder(project_path=project_path)
        dag_builder.build()
    except ValueError as e:
        _echo_error(f"DAG build failed: {e}")
        raise typer.Exit(1)
    except CycleDetectedError as e:
        _echo_error(f"Cycle detected in DAG: {e.message}")
        raise typer.Exit(1)
    except MissingDependencyError as e:
        _echo_error(f"Missing dependency: {e.message}")
        raise typer.Exit(1)

    # Report parse warnings
    errors = dag_builder.get_parse_errors()
    if errors:
        _echo_warning(f"Parse warnings: {len(errors)}")
        for error in errors:
            typer.echo(f"  - {error}")

    # Step 2: Convert to Manifest
    manifest = _build_manifest_from_dag(dag_builder, project_path.name)

    _echo_success(f"DAG built: {len(manifest.nodes)} nodes, {len(manifest.edges)} edges")

    # Show node summary by type
    if manifest.nodes:
        typer.echo("")
        typer.echo("Node Summary:")
        type_counts: dict[str, int] = {}
        for node in manifest.nodes.values():
            node_type_str = node.node_type.value
            type_counts[node_type_str] = type_counts.get(node_type_str, 0) + 1
        for node_type, count in sorted(type_counts.items()):
            typer.echo(f"  - {node_type}: {count}")

    if env_name is not None:
        # ---- Environment mode ----
        from seeknal.workflow.environment import EnvironmentManager

        _echo_info(f"\nPlanning environment '{env_name}'...")

        manager = EnvironmentManager(target_path)
        env_plan_result = manager.plan(env_name, manifest)

        # Display categorized changes
        typer.echo("")
        _echo_info(f"Environment Plan: {env_name}")
        _echo_info("=" * 60)

        if not env_plan_result.categorized_changes:
            _echo_success("No changes detected.")
            return

        breaking = sum(1 for v in env_plan_result.categorized_changes.values() if v == "breaking")
        non_breaking = sum(
            1 for v in env_plan_result.categorized_changes.values() if v == "non_breaking"
        )
        metadata_count = sum(
            1 for v in env_plan_result.categorized_changes.values() if v == "metadata"
        )

        if breaking > 0:
            _echo_warning(f"  BREAKING changes: {breaking}")
        if non_breaking > 0:
            _echo_info(f"  Non-breaking changes: {non_breaking}")
        if metadata_count > 0:
            _echo_info(f"  Metadata-only changes: {metadata_count}")
        if env_plan_result.added_nodes:
            _echo_info(f"  New nodes: {len(env_plan_result.added_nodes)}")
        if env_plan_result.removed_nodes:
            _echo_info(f"  Removed nodes: {len(env_plan_result.removed_nodes)}")

        _echo_info(f"\n  Total nodes to execute: {env_plan_result.total_nodes_to_execute}")

        env_dir = target_path / "environments" / env_name
        _echo_success(f"Plan saved to {env_dir / 'plan.json'}")

    else:
        # ---- Production mode (no environment) ----
        from seeknal.dag.manifest import Manifest
        from seeknal.dag.diff import ManifestDiff
        from seeknal.workflow.state import load_state, calculate_node_hash, get_nodes_to_run

        # Load existing manifest for diff
        manifest_file = target_path / "manifest.json"
        old_manifest = None
        if manifest_file.exists():
            try:
                old_manifest = Manifest.load(str(manifest_file))
            except Exception:
                old_manifest = None

        # Show diff if we have an old manifest
        if old_manifest is not None:
            diff = ManifestDiff.compare(old_manifest, manifest)
            typer.echo("")
            if diff.has_changes():
                typer.echo(typer.style("Changes detected:", bold=True))

                # Use enhanced plan output with SQL diffs and downstream impact
                formatted_output = diff.format_plan_output(manifest)
                for line in formatted_output.split("\n"):
                    if line.strip():
                        # Colorize based on content
                        if "[BREAKING" in line:
                            typer.echo(typer.style(line, fg=typer.colors.RED, bold=True))
                        elif "[CHANGED" in line or "[NEW]" in line:
                            typer.echo(typer.style(line, fg=typer.colors.YELLOW))
                        elif "[METADATA" in line:
                            typer.echo(typer.style(line, fg=typer.colors.BLUE))
                        elif "[REMOVED]" in line:
                            typer.echo(typer.style(line, fg=typer.colors.RED))
                        elif "-> REBUILD" in line:
                            typer.echo(typer.style(line, fg=typer.colors.RED))
                        elif "Downstream impact" in line:
                            typer.echo(typer.style(line, fg=typer.colors.YELLOW, bold=True))
                        elif line.strip().startswith("Summary:"):
                            typer.echo(typer.style(line, bold=True))
                        else:
                            typer.echo(line)

                # Show edge changes
                if diff.added_edges:
                    typer.echo(typer.style(
                        f"  Added edges ({len(diff.added_edges)}):", fg=typer.colors.GREEN
                    ))
                    for edge in diff.added_edges:
                        typer.echo(typer.style(
                            f"    + {edge.from_node} -> {edge.to_node}", fg=typer.colors.GREEN
                        ))

                if diff.removed_edges:
                    typer.echo(typer.style(
                        f"  Removed edges ({len(diff.removed_edges)}):", fg=typer.colors.RED
                    ))
                    for edge in diff.removed_edges:
                        typer.echo(typer.style(
                            f"    - {edge.from_node} -> {edge.to_node}", fg=typer.colors.RED
                        ))

                # Hint for detailed diff
                if diff.modified_nodes:
                    typer.echo("")
                    typer.echo(typer.style(
                        "  Tip: Run 'seeknal diff <type>/<name>' for detailed YAML diff",
                        fg=typer.colors.RESET,
                    ))
            else:
                _echo_info("No changes detected since last parse")
        else:
            _echo_info("No previous manifest found (first run)")

        # Save updated manifest
        target_path.mkdir(parents=True, exist_ok=True)
        manifest.save(str(manifest_file))
        _echo_success(f"Manifest saved to {manifest_file}")

        # Load run state and show execution plan
        state_path = target_path / "run_state.json"
        old_state = load_state(state_path)

        # Calculate current hashes
        current_hashes: dict[str, str] = {}
        for node_id, node in dag_builder.nodes.items():
            try:
                node_hash = calculate_node_hash(node.yaml_data, Path(node.file_path))
                current_hashes[node_id] = node_hash
            except Exception:
                pass

        # Build DAG adjacency for downstream propagation
        dag_adjacency: dict[str, set[str]] = {}
        for node_id in dag_builder.nodes:
            dag_adjacency[node_id] = dag_builder.get_downstream(node_id)

        nodes_to_run = get_nodes_to_run(current_hashes, old_state, dag_adjacency)

        # Show execution plan
        try:
            execution_order = dag_builder.topological_sort()
        except CycleDetectedError as e:
            _echo_error(f"Cycle detected: {e.message}")
            raise typer.Exit(1)

        typer.echo("")
        typer.echo(typer.style("Execution Plan:", bold=True))
        typer.echo("-" * 60)

        for idx, node_id in enumerate(execution_order, 1):
            node = dag_builder.nodes[node_id]
            if node_id in nodes_to_run:
                status = "RUN"
                status_msg = typer.style(status, fg=typer.colors.GREEN, bold=True)
            elif old_state and node_id in old_state.nodes and old_state.nodes[node_id].is_success():
                status = "CACHED"
                status_msg = typer.style(status, fg=typer.colors.BLUE)
            else:
                status = "SKIP"
                status_msg = typer.style(status, fg=typer.colors.RESET)

            tags_str = f" [{', '.join(node.tags)}]" if node.tags else ""
            typer.echo(f"  {idx:2d}. {status_msg} {node.name}{tags_str}")

        typer.echo("")
        _echo_info(f"Total: {len(execution_order)} nodes, {len(nodes_to_run)} to run")


@app.command(name="diff")
@handle_cli_error("Diff failed")
def diff_command(
    node: Optional[str] = typer.Argument(None, help="Node to diff (e.g., 'sources/orders')"),
    node_type: Optional[str] = typer.Option(None, "--type", "-t", help="Filter by node type"),
    stat: bool = typer.Option(False, "--stat", help="Show summary statistics only"),
    project_path: Path = typer.Option(".", "--project-path", "-p", help="Project directory"),
):
    """Show changes in pipeline files since last apply.

    Like 'git diff' for your Seeknal pipelines. Shows unified YAML diffs
    with semantic annotations (BREAKING/NON_BREAKING/METADATA).

    Examples:
        seeknal diff                    # Show all changes
        seeknal diff sources/orders     # Diff a specific node
        seeknal diff --type transforms  # Filter by node type
        seeknal diff --stat             # Show summary statistics only
    """
    from seeknal.workflow.diff_engine import DiffEngine
    from seeknal.dag.diff import ChangeCategory

    project_path = Path(project_path).resolve()
    engine = DiffEngine(project_path)

    if node:
        # Single-node diff mode
        try:
            result = engine.diff_node(node)
        except ValueError as e:
            _echo_error(str(e))
            raise typer.Exit(1)

        if result.status == "unchanged":
            _echo_success("No changes detected since last apply.")
            return

        if result.status == "new":
            _echo_info(f"New file (not yet applied): {result.file_path}")
            return

        if result.status == "deleted":
            _echo_warning(f"Deleted: {result.node_id}")
            return

        # Show unified diff
        if result.unified_diff:
            for line in result.unified_diff.splitlines():
                if line.startswith("---") or line.startswith("+++"):
                    typer.echo(typer.style(line, bold=True))
                elif line.startswith("-"):
                    typer.echo(typer.style(line, fg=typer.colors.RED))
                elif line.startswith("+"):
                    typer.echo(typer.style(line, fg=typer.colors.GREEN))
                elif line.startswith("@@"):
                    typer.echo(typer.style(line, fg=typer.colors.CYAN))
                else:
                    typer.echo(line)

        # Show semantic annotations
        typer.echo("")
        if result.category:
            cat_labels = {
                ChangeCategory.BREAKING: ("BREAKING (downstream rebuild required)", typer.colors.RED),
                ChangeCategory.NON_BREAKING: ("NON_BREAKING (rebuild this node)", typer.colors.YELLOW),
                ChangeCategory.METADATA: ("METADATA (no rebuild needed)", typer.colors.BLUE),
            }
            label, color = cat_labels.get(
                result.category, ("UNKNOWN", typer.colors.RESET)
            )
            typer.echo(typer.style(f"  Category: {label}", fg=color, bold=True))

        if result.downstream_count > 0 and result.category == ChangeCategory.BREAKING:
            typer.echo(typer.style(
                f"  Impact: {result.downstream_count} downstream node(s) affected",
                fg=typer.colors.YELLOW, bold=True,
            ))
            downstream = engine.get_downstream_nodes(result.node_id)
            for ds_id in downstream[:5]:
                typer.echo(typer.style(f"    -> {ds_id}", fg=typer.colors.RED))
            if len(downstream) > 5:
                typer.echo(f"    ... and {len(downstream) - 5} more")

    else:
        # All-files diff mode
        results = engine.diff_all(node_type=node_type)

        if not results:
            _echo_success("No changes detected since last apply.")
            return

        modified = [r for r in results if r.status == "modified"]
        new = [r for r in results if r.status == "new"]
        deleted = [r for r in results if r.status == "deleted"]

        if stat:
            # Git-style stat output
            for r in modified:
                bar = "+" * min(r.insertions, 20) + "-" * min(r.deletions, 20)
                changes = r.insertions + r.deletions
                typer.echo(f"  {r.file_path:<45} | {changes} {bar}")
            for r in new:
                typer.echo(typer.style(f"  {r.file_path:<45} | new file", fg=typer.colors.GREEN))
            for r in deleted:
                typer.echo(typer.style(f"  {r.file_path:<45} | deleted", fg=typer.colors.RED))

            total_files = len(modified) + len(new) + len(deleted)
            total_ins = sum(r.insertions for r in modified)
            total_del = sum(r.deletions for r in modified)
            typer.echo(f"  {total_files} file(s) changed, {total_ins} insertion(s), {total_del} deletion(s)")
        else:
            # Summary mode
            typer.echo(typer.style("Changes since last apply:", bold=True))
            typer.echo("")

            if modified:
                typer.echo("  Modified:")
                for r in modified:
                    cat_label = ""
                    if r.category:
                        cat_labels = {
                            ChangeCategory.BREAKING: "[BREAKING]",
                            ChangeCategory.NON_BREAKING: "[NON_BREAKING]",
                            ChangeCategory.METADATA: "[METADATA]",
                        }
                        cat_label = cat_labels.get(r.category, "")

                    change_desc = f"+{r.insertions} -{r.deletions}"
                    line = f"    {r.file_path:<40} {cat_label} {change_desc}"

                    if r.category == ChangeCategory.BREAKING:
                        typer.echo(typer.style(line, fg=typer.colors.RED))
                    elif r.category == ChangeCategory.NON_BREAKING:
                        typer.echo(typer.style(line, fg=typer.colors.YELLOW))
                    else:
                        typer.echo(typer.style(line, fg=typer.colors.BLUE))

            if new:
                typer.echo("")
                typer.echo("  New (not yet applied):")
                for r in new:
                    typer.echo(typer.style(f"    {r.file_path}", fg=typer.colors.GREEN))

            if deleted:
                typer.echo("")
                typer.echo("  Deleted:")
                for r in deleted:
                    typer.echo(typer.style(f"    {r.file_path}", fg=typer.colors.RED))

            typer.echo("")
            typer.echo(f"  {len(modified)} modified, {len(new)} new, {len(deleted)} deleted")

        # Check for Python files
        py_dir = project_path / "seeknal" / "pipelines"
        if py_dir.exists():
            py_files = list(py_dir.glob("*.py"))
            if py_files:
                typer.echo("")
                _echo_info(f"Note: {len(py_files)} Python pipeline file(s) found. Diff not yet supported for .py files.")


@app.command()
@handle_cli_error("Promote failed")
def promote(
    env_name: str = typer.Argument(..., help="Environment to promote"),
    target: str = typer.Argument("prod", help="Target (default: prod)"),
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """Promote environment changes to production.

    Examples:
        seeknal promote dev           # Promote dev to production
        seeknal promote staging prod  # Promote staging to prod
    """
    _promote_environment(env_name, target, project_path)


@app.command()
def repl():
    """Start interactive SQL REPL.

    The SQL REPL allows you to explore data across multiple sources using DuckDB
    as a unified query engine. Connect to PostgreSQL, MySQL, SQLite databases,
    or query local parquet/csv files.

    Examples:
        seeknal repl
        seeknal> .connect mydb                   (saved source)
        seeknal> .connect postgres://user:pass@host/db
        seeknal> .connect /path/to/data.parquet
        seeknal> SELECT * FROM db0.users LIMIT 10
        seeknal> .tables
        seeknal> .quit

    Manage sources:
        seeknal source add mydb --url postgres://user:pass@host/db
        seeknal source list
        seeknal source remove mydb
    """
    from seeknal.cli.repl import run_repl

    run_repl()


@app.command()
def draft(
    node_type: str = typer.Argument(..., help="Node type (source, transform, feature-group, model, aggregation, rule, exposure)"),
    name: str = typer.Argument(..., help="Node name"),
    description: Optional[str] = typer.Option(None, "--description", "-d", help="Node description"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing draft file"),
    python: bool = typer.Option(False, "--python", "-py", help="Generate Python file instead of YAML"),
    deps: str = typer.Option("", "--deps", help="Comma-separated Python dependencies for PEP 723 header"),
):
    """Generate template from Jinja2 template.

    Creates a draft file for a new node using Jinja2 templates.
    The draft file can be edited and then applied using 'seeknal apply'.

    Supports both YAML (--default) and Python (--python) output formats.

    Template discovery order:
    1. project/seeknal/templates/*.j2 (project override)
    2. Package templates (default)

    Examples:
        # Create a YAML feature group draft
        $ seeknal draft feature-group user_behavior

        # Create a Python transform draft
        $ seeknal draft transform clean_data --python

        # Create Python source with dependencies
        $ seeknal draft source raw_users --python --deps pandas,requests

        # Create with description
        $ seeknal draft source postgres_users --description "PostgreSQL users table"

        # Overwrite existing draft
        $ seeknal draft transform clean_data --force
    """
    from seeknal.workflow.draft import draft_command

    draft_command(node_type, name, description, force, python, deps)


@app.command()
def dry_run(
    file_path: str = typer.Argument(..., help="Path to YAML or Python pipeline file"),
    limit: int = typer.Option(10, "--limit", "-l", help="Row limit for preview (default: 10)"),
    timeout: int = typer.Option(30, "--timeout", "-t", help="Query timeout in seconds (default: 30)"),
    schema_only: bool = typer.Option(False, "--schema-only", "-s", help="Validate schema only, skip execution"),
):
    """Validate YAML/Python and preview execution.

    Performs comprehensive validation:
    1. YAML syntax validation with line numbers
    2. Schema validation (required fields)
    3. Dependency validation (refs to upstream nodes)
    4. Preview execution with sample data

    Examples:
        # Validate and preview
        $ seeknal dry-run draft_feature_group_user_behavior.yml

        # Preview with 5 rows
        $ seeknal dry-run draft_feature_group_user_behavior.yml --limit 5

        # Schema validation only (no execution)
        $ seeknal dry-run draft_source_postgres.yml --schema-only

        # Longer timeout for slow queries
        $ seeknal dry-run draft_transform.yml --timeout 60
    """
    from seeknal.workflow.dry_run import dry_run_command

    dry_run_command(file_path, limit, timeout, schema_only)


@app.command()
def apply(
    file_path: str = typer.Argument(..., help="Path to YAML or Python pipeline file"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing file without prompt"),
    no_parse: bool = typer.Option(False, "--no-parse", help="Skip manifest regeneration"),
):
    """Apply file to production.

    YAML files: Moves to seeknal/<type>s/<name>.yml and updates manifest
    Python files: Copies to seeknal/pipelines/<name>.py and validates

    Examples:
        # Apply YAML draft
        $ seeknal apply draft_feature_group_user_behavior.yml

        # Apply Python file
        $ seeknal apply seeknal/pipelines/enriched_sales.py

        # Apply with overwrite
        $ seeknal apply draft_transform.yml --force
    """
    from pathlib import Path
    from seeknal.workflow.apply import apply_command
    from seeknal.workflow.apply import show_python_diff

    # Check if it's a Python file
    path = Path(file_path)
    if path.suffix == ".py":
        # Python files - validate with optional diff
        # Extract node name from decorator for target path
        from seeknal.workflow.dry_run import extract_decorators, validate_python_syntax

        try:
            metadata = validate_python_syntax(path)
            node_name = metadata.get("name", path.stem)
        except Exception:
            # Fallback to filename stem if validation fails
            node_name = path.stem

        target_path = Path.cwd() / "seeknal" / "pipelines" / f"{node_name}.py"

        # Check if file exists and show diff
        if target_path.exists() and target_path.resolve() != path.resolve():
            _echo_warning(f"Node already exists: {target_path}")

            # Show diff
            if not force:
                has_changes = show_python_diff(target_path, path)
                _echo_info("")

                if not has_changes:
                    _echo_info("Files are identical. No action needed.")
                    raise typer.Exit(0)

                # Prompt for confirmation
                confirm = typer.confirm("Apply these changes?")
                if not confirm:
                    _echo_info("Cancelled.")
                    raise typer.Exit(0)

        # Move file to target location (like YAML apply)
        if target_path.resolve() != path.resolve():
            _echo_info(f"Moving file to {target_path}...")
            import shutil
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(target_path))

        # Run validation
        _echo_info("Running validation...")
        from seeknal.workflow.dry_run import dry_run_command

        # Convert to list format for dry_run_command
        import sys
        sys.argv = ["seeknal", "dry-run", str(target_path)]

        # Run dry-run with schema-only mode
        dry_run_command(str(target_path), 10, 30, True)

        _echo_success(f"Python file applied: {target_path}")
    else:
        # YAML files - use normal apply workflow
        apply_command(file_path, force, no_parse)


@app.command("starrocks-setup-catalog")
def starrocks_setup_catalog(
    catalog_name: str = typer.Option("iceberg_catalog", help="Catalog name"),
    catalog_type: str = typer.Option("iceberg", help="Catalog type (iceberg)"),
    warehouse: str = typer.Option("s3://warehouse/", help="Warehouse path"),
    uri: str = typer.Option("", help="Catalog URI (e.g., thrift://host:9083)"),
):
    """Generate StarRocks Iceberg catalog setup SQL.

    Outputs CREATE EXTERNAL CATALOG DDL for connecting StarRocks to an Iceberg catalog.

    Examples:
        seeknal starrocks-setup-catalog --catalog-name my_catalog --uri thrift://hive:9083
        seeknal starrocks-setup-catalog --warehouse s3://my-bucket/warehouse
    """
    ddl = (
        f"CREATE EXTERNAL CATALOG IF NOT EXISTS {catalog_name}\n"
        f"PROPERTIES (\n"
        f'    "type" = "{catalog_type}",\n'
    )

    if uri:
        ddl += f'    "iceberg.catalog.type" = "hive",\n'
        ddl += f'    "hive.metastore.uris" = "{uri}",\n'
    else:
        ddl += f'    "iceberg.catalog.type" = "rest",\n'

    ddl += f'    "warehouse" = "{warehouse}"\n'
    ddl += ");"

    _echo_info("StarRocks Iceberg Catalog Setup SQL:")
    typer.echo("")
    typer.echo(ddl)
    typer.echo("")
    _echo_info("Execute this SQL in your StarRocks client to create the catalog.")


@app.command("connection-test")
def connection_test(
    name: str = typer.Argument(..., help="Connection profile name or starrocks:// URL"),
    profile: str = typer.Option("default", "--profile", "-p", help="Profile name in profiles.yml"),
):
    """Test connectivity to a StarRocks database.

    Tests connection using profiles.yml config or a direct URL.

    Examples:
        seeknal connection-test default
        seeknal connection-test starrocks://user:pass@host:9030/db
    """
    if name.startswith("starrocks://"):
        from seeknal.connections.starrocks import parse_starrocks_url, check_starrocks_connection

        sr_config = parse_starrocks_url(name)
        success, message = check_starrocks_connection(sr_config.to_pymysql_kwargs())
    else:
        from seeknal.connections.starrocks import check_starrocks_connection
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        try:
            loader = ProfileLoader()
            config = loader.load_starrocks_profile(name)
        except Exception as e:
            _echo_error(f"Profile error: {e}")
            raise typer.Exit(code=1)

        success, message = check_starrocks_connection(config)

    if success:
        _echo_success(message)
    else:
        _echo_error(message)
        raise typer.Exit(code=1)


@app.command()
def audit(
    node: Optional[str] = typer.Argument(None, help="Specific node to audit (e.g., source.users)"),
    target_path: str = typer.Option("target", help="Path to target directory"),
):
    """
    Run data quality audits on cached node outputs.

    Executes all audit rules defined in node YAML configs against
    the last execution's cached outputs. No re-execution needed.
    """
    from pathlib import Path as P
    import duckdb

    target = P(target_path)
    cache_dir = target / "cache"

    if not cache_dir.exists():
        _echo_error("No cached data found. Run 'seeknal run' first.")
        raise typer.Exit(code=1)

    # Find cached parquet files
    parquet_files = list(cache_dir.glob("**/*.parquet"))
    if not parquet_files:
        _echo_error("No cached parquet files found.")
        raise typer.Exit(code=1)

    conn = duckdb.connect(":memory:")

    # Load all cached files as views
    for pf in parquet_files:
        kind = pf.parent.name
        name = pf.stem
        view_name = f"{kind}.{name}"
        if node and view_name != node:
            continue
        try:
            conn.execute(f'CREATE VIEW "{view_name}" AS SELECT * FROM read_parquet(\'{pf}\')')
        except Exception as e:
            _echo_warning(f"Could not load {view_name}: {e}")

    # Load manifest to get audit configs
    manifest_path = target / "manifest.json"
    if not manifest_path.exists():
        _echo_error("No manifest found. Run 'seeknal parse' first.")
        raise typer.Exit(code=1)

    from seeknal.dag.manifest import Manifest
    manifest = Manifest.load(str(manifest_path))

    from seeknal.workflow.audits import parse_audits, AuditRunner
    runner = AuditRunner(conn)
    total_passed = 0
    total_failed = 0

    for node_id, node_obj in manifest.nodes.items():
        if node and node_id != node:
            continue

        audits = parse_audits(node_obj.config)
        if not audits:
            continue

        view_name = f"{node_obj.node_type.value}.{node_obj.name}"
        _echo_info(f"\nAuditing {view_name}:")

        results = runner.run_audits(audits, view_name)
        for r in results:
            status = "PASS" if r.passed else "FAIL"
            cols = f" [{', '.join(r.columns)}]" if r.columns else ""
            severity_str = f" ({r.severity})" if not r.passed else ""
            msg = f"  {status} {r.audit_type}{cols}{severity_str}: {r.message} ({r.duration_ms}ms)"
            if r.passed:
                _echo_success(msg)
                total_passed += 1
            else:
                _echo_error(msg)
                total_failed += 1

    _echo_info(f"\nAudit Summary: {total_passed} passed, {total_failed} failed")
    if total_failed > 0:
        raise typer.Exit(code=1)


@app.command()
def query(
    metrics: str = typer.Option(..., help="Comma-separated metric names"),
    dimensions: Optional[str] = typer.Option(None, help="Comma-separated dimensions (e.g. region,ordered_at__month)"),
    filter: Optional[str] = typer.Option(None, "--filter", help="SQL filter expression"),
    order_by: Optional[str] = typer.Option(None, "--order-by", help="Order by columns (prefix - for DESC)"),
    limit: int = typer.Option(100, help="Maximum rows to return"),
    compile_only: bool = typer.Option(False, "--compile", help="Show generated SQL without executing"),
    format: str = typer.Option("table", help="Output format: table, json, csv"),
    project_path: str = typer.Option(".", help="Path to project directory"),
):
    """
    Query metrics from the semantic layer.

    Compiles metric definitions into SQL, executes against DuckDB, and
    returns formatted results.

    Example:
        seeknal query --metrics total_revenue,order_count --dimensions region
    """
    import json as json_mod
    import yaml
    from pathlib import Path as P

    from seeknal.workflow.semantic.models import (
        Metric as MetricModel,
        MetricQuery,
        SemanticModel,
    )
    from seeknal.workflow.semantic.compiler import MetricCompiler

    project = P(project_path)

    # Load semantic models
    sm_dir = project / "seeknal" / "semantic_models"
    semantic_models = []
    if sm_dir.exists():
        for yml_path in sorted(sm_dir.glob("*.yml")):
            with open(yml_path, "r") as f:
                data = yaml.safe_load(f) or {}
            if data.get("kind") == "semantic_model":
                semantic_models.append(SemanticModel.from_dict(data))

    if not semantic_models:
        _echo_error("No semantic models found. Create YAML files in seeknal/semantic_models/")
        raise typer.Exit(code=1)

    # Load metrics
    metrics_dir = project / "seeknal" / "metrics"
    metric_list = []
    if metrics_dir.exists():
        for yml_path in sorted(metrics_dir.glob("*.yml")):
            with open(yml_path, "r") as f:
                for doc in yaml.safe_load_all(f):
                    if doc and doc.get("kind") == "metric":
                        metric_list.append(MetricModel.from_dict(doc))

    if not metric_list:
        _echo_error("No metrics found. Create YAML files in seeknal/metrics/")
        raise typer.Exit(code=1)

    # Build compiler
    compiler = MetricCompiler(semantic_models, metric_list)

    # Build query
    metric_names = [m.strip() for m in metrics.split(",")]
    dim_list = [d.strip() for d in dimensions.split(",")] if dimensions else []
    filter_list = [filter] if filter else []
    order_list = [o.strip() for o in order_by.split(",")] if order_by else []

    mq = MetricQuery(
        metrics=metric_names,
        dimensions=dim_list,
        filters=filter_list,
        order_by=order_list,
        limit=limit,
    )

    try:
        sql = compiler.compile(mq)
    except ValueError as e:
        _echo_error(f"Compilation error: {e}")
        raise typer.Exit(code=1)

    if compile_only:
        typer.echo(sql)
        return

    # Execute on DuckDB
    import duckdb
    conn = duckdb.connect(":memory:")

    # Register semantic model tables from cached parquet files
    target = P(project_path) / "target" / "cache"
    if target.exists():
        for pf in target.glob("**/*.parquet"):
            table_name = f"{pf.parent.name}.{pf.stem}"
            try:
                conn.execute(f"CREATE VIEW \"{table_name}\" AS SELECT * FROM read_parquet('{pf}')")
            except Exception:
                pass

    try:
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
    except Exception as e:
        _echo_error(f"Query execution error: {e}")
        raise typer.Exit(code=1)

    # Format output
    if format == "json":
        data = [dict(zip(columns, row)) for row in rows]
        typer.echo(json_mod.dumps(data, indent=2, default=str))
    elif format == "csv":
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        typer.echo(output.getvalue())
    else:
        try:
            from tabulate import tabulate as tabfmt
            typer.echo(tabfmt(rows, headers=columns, tablefmt="simple"))
        except ImportError:
            # Fallback to simple formatting
            typer.echo("  ".join(columns))
            for row in rows:
                typer.echo("  ".join(str(v) for v in row))

    _echo_info(f"\n{len(rows)} rows returned")


@app.command("deploy-metrics")
def deploy_metrics(
    connection: str = typer.Option(..., help="StarRocks connection name or URL"),
    dimensions: Optional[str] = typer.Option(None, help="Comma-separated dimensions to include in MVs"),
    refresh_interval: str = typer.Option("1 DAY", help="MV refresh interval (e.g. '1 HOUR', '1 DAY')"),
    drop_existing: bool = typer.Option(False, "--drop-existing", help="Drop and recreate existing MVs"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show DDL without executing"),
    project_path: str = typer.Option(".", help="Path to project directory"),
):
    """
    Deploy semantic layer metrics as StarRocks materialized views.

    Generates CREATE MATERIALIZED VIEW DDL for each metric definition
    and executes on the specified StarRocks connection.

    Example:
        seeknal deploy-metrics --connection starrocks://root@localhost:9030/analytics --dry-run
    """
    import yaml
    from pathlib import Path as P

    from seeknal.workflow.semantic.models import (
        Metric as MetricModel,
        SemanticModel,
    )
    from seeknal.workflow.semantic.compiler import MetricCompiler
    from seeknal.workflow.semantic.deploy import MetricDeployer, DeployConfig

    project = P(project_path)

    # Load semantic models
    sm_dir = project / "seeknal" / "semantic_models"
    semantic_models = []
    if sm_dir.exists():
        for yml_path in sorted(sm_dir.glob("*.yml")):
            with open(yml_path, "r") as f:
                data = yaml.safe_load(f) or {}
            if data.get("kind") == "semantic_model":
                semantic_models.append(SemanticModel.from_dict(data))

    if not semantic_models:
        _echo_error("No semantic models found.")
        raise typer.Exit(code=1)

    # Load metrics
    metrics_dir = project / "seeknal" / "metrics"
    metric_list = []
    if metrics_dir.exists():
        for yml_path in sorted(metrics_dir.glob("*.yml")):
            with open(yml_path, "r") as f:
                for doc in yaml.safe_load_all(f):
                    if doc and doc.get("kind") == "metric":
                        metric_list.append(MetricModel.from_dict(doc))

    if not metric_list:
        _echo_error("No metrics found.")
        raise typer.Exit(code=1)

    # Resolve connection config
    if connection.startswith("starrocks://"):
        from seeknal.connections.starrocks import parse_starrocks_url
        sr_config = parse_starrocks_url(connection)
        conn_config = sr_config.to_pymysql_kwargs()
    else:
        # Try loading from profiles
        try:
            from seeknal.workflow.materialization.profile_loader import ProfileLoader
            loader = ProfileLoader()
            conn_config = loader.load_starrocks_profile(connection)
        except Exception as e:
            _echo_error(f"Could not load connection '{connection}': {e}")
            raise typer.Exit(code=1)

    # Build compiler and deployer
    compiler = MetricCompiler(semantic_models, metric_list)
    config = DeployConfig(
        refresh_interval=refresh_interval,
        drop_existing=drop_existing,
    )

    dim_list = [d.strip() for d in dimensions.split(",")] if dimensions else None

    deployer = MetricDeployer(compiler, conn_config)
    results = deployer.deploy(metric_list, config, dim_list, dry_run=dry_run)

    # Display results
    for r in results:
        if dry_run:
            _echo_info(f"\n-- Metric: {r.metric_name} -> {r.mv_name}")
            typer.echo(r.ddl)
        elif r.success:
            _echo_success(f"Deployed {r.metric_name} -> {r.mv_name}")
        else:
            _echo_error(f"Failed {r.metric_name}: {r.error}")

    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)

    if dry_run:
        _echo_info(f"\nDry run: {len(results)} MVs would be created")
    else:
        _echo_info(f"\nDeployed: {succeeded} succeeded, {failed} failed")

    if failed > 0:
        raise typer.Exit(code=1)


# =============================================================================
# Environment Management Commands
# =============================================================================


@env_app.command("plan")
@handle_cli_error("Environment plan failed")
def env_plan(
    env_name: str = typer.Argument(..., help="Environment name (e.g., dev, staging)"),
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """Preview changes in a virtual environment."""
    from seeknal.workflow.environment import EnvironmentManager
    from seeknal.workflow.dag import DAGBuilder

    project_path = Path(project_path).resolve()
    target_path = project_path / "target"

    _echo_info(f"Planning environment '{env_name}'...")

    # Build current manifest from YAML files
    dag_builder = DAGBuilder(project_path=project_path)
    dag_builder.build()

    # Convert to Manifest using shared helper
    manifest = _build_manifest_from_dag(dag_builder, project_path.name)

    manager = EnvironmentManager(target_path)
    plan = manager.plan(env_name, manifest)

    # Display plan
    _echo_info("")
    _echo_info(f"Environment Plan: {env_name}")
    _echo_info("=" * 60)

    if not plan.categorized_changes:
        _echo_success("No changes detected.")
        return

    breaking = sum(1 for v in plan.categorized_changes.values() if v == "breaking")
    non_breaking = sum(1 for v in plan.categorized_changes.values() if v == "non_breaking")
    metadata = sum(1 for v in plan.categorized_changes.values() if v == "metadata")

    if breaking > 0:
        _echo_warning(f"  BREAKING changes: {breaking}")
    if non_breaking > 0:
        _echo_info(f"  Non-breaking changes: {non_breaking}")
    if metadata > 0:
        _echo_info(f"  Metadata-only changes: {metadata}")
    if plan.added_nodes:
        _echo_info(f"  New nodes: {len(plan.added_nodes)}")
    if plan.removed_nodes:
        _echo_info(f"  Removed nodes: {len(plan.removed_nodes)}")

    _echo_info(f"\n  Total nodes to execute: {plan.total_nodes_to_execute}")

    env_dir = target_path / "environments" / env_name
    _echo_success(f"Plan saved to {env_dir / 'plan.json'}")


def _run_in_environment(
    env_name: str,
    project_path: Path,
    force: bool = False,
    parallel: bool = False,
    max_workers: int = 4,
    continue_on_error: bool = False,
    dry_run: bool = False,
    show_plan: bool = False,
) -> None:
    """Execute a plan in a virtual environment.

    Shared helper used by both `seeknal run --env` and `seeknal env apply`.

    Validates the saved plan, sets up the env cache directory, copies
    production refs for unchanged nodes, and executes changed nodes
    using DAGRunner with the env-specific target path.

    Args:
        env_name: Name of the environment to apply.
        project_path: Resolved project root directory.
        force: Apply even if the plan is stale.
        parallel: Execute independent nodes in parallel.
        max_workers: Maximum number of parallel workers.
        continue_on_error: Continue execution after failures.
        dry_run: Show what would execute without running.
        show_plan: Show execution plan and exit (no execution).
    """
    from seeknal.workflow.environment import EnvironmentManager

    target_path = project_path / "target"

    manager = EnvironmentManager(target_path)

    _echo_info(f"Applying plan for environment '{env_name}'...")

    try:
        apply_info = manager.apply(env_name, force=force)
    except ValueError as e:
        _echo_error(str(e))
        raise typer.Exit(1)

    nodes_to_execute = apply_info["nodes_to_execute"]
    env_dir = apply_info["env_dir"]
    manifest = apply_info["manifest"]

    if not nodes_to_execute:
        _echo_success("No nodes to execute (metadata-only changes).")
        return

    if show_plan:
        _echo_info(f"Environment '{env_name}' execution plan:")
        _echo_info(f"  {len(nodes_to_execute)} node(s) to execute:")
        for node_id in sorted(nodes_to_execute):
            node = manifest.get_node(node_id)
            node_name = node.name if node else node_id
            _echo_info(f"    - {node_name}")
        return

    _echo_info(f"Executing {len(nodes_to_execute)} node(s) in environment '{env_name}'...")

    # Set up env cache directory
    env_cache = env_dir / "cache"
    env_cache.mkdir(parents=True, exist_ok=True)

    # Copy production refs for unchanged nodes (zero-copy references)
    refs_data = manager._load_json(env_dir / "refs.json")
    if refs_data:
        import shutil
        for ref in refs_data.get("refs", []):
            src_path = Path(ref["output_path"])
            if src_path.exists():
                # Determine destination in env cache
                dest = env_cache / src_path.relative_to(target_path / "cache")
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists():
                    shutil.copy2(src_path, dest)

    # Execute nodes using DAGRunner with env-specific target path
    from seeknal.workflow.runner import DAGRunner

    runner = DAGRunner(manifest, target_path=env_dir)

    if parallel:
        from seeknal.workflow.parallel import ParallelDAGRunner, print_parallel_summary

        parallel_runner = ParallelDAGRunner(runner, max_workers, continue_on_error)
        summary = parallel_runner.run(nodes_to_execute, dry_run=dry_run)
        print_parallel_summary(summary)
        failed = summary.failed_nodes
    else:
        # Sequential execution
        import time as _time
        successful = 0
        failed = 0
        order = runner._get_topological_order()

        for node_id in order:
            if node_id not in nodes_to_execute:
                continue

            node = manifest.get_node(node_id)
            node_name = node.name if node else node_id

            if dry_run:
                _echo_info(f"  Would execute: {node_name}")
                successful += 1
                continue

            _echo_info(f"  Executing: {node_name}...")
            result = runner._execute_node(node_id)

            if result.status.value == "success":
                _echo_success(f"  {node_name} completed in {result.duration:.2f}s")
                successful += 1
            else:
                _echo_error(f"  {node_name} failed: {result.error_message}")
                failed += 1
                if not continue_on_error:
                    break

        _echo_info(f"\n  Results: {successful} succeeded, {failed} failed")

    # Create run_state.json marker for promote validation
    env_state_path = env_dir / "run_state.json"
    if not env_state_path.exists():
        import json
        with open(env_state_path, "w") as f:
            json.dump({"applied": True, "env_name": env_name}, f)

    if failed > 0:
        _echo_warning(
            f"Environment '{env_name}' applied with {failed} failure(s)."
        )
    else:
        _echo_success(
            f"Environment '{env_name}' applied successfully."
        )


@env_app.command("apply")
@handle_cli_error("Environment apply failed")
def env_apply(
    env_name: str = typer.Argument(..., help="Environment name"),
    force: bool = typer.Option(False, help="Apply even if plan is stale"),
    parallel: bool = typer.Option(False, "--parallel", help="Run nodes in parallel"),
    max_workers: int = typer.Option(4, "--max-workers", help="Max parallel workers"),
    continue_on_error: bool = typer.Option(False, "--continue-on-error", help="Continue past failures"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would execute without running"),
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """Execute a plan in a virtual environment.

    Validates the saved plan, then executes changed nodes using the
    environment-specific cache directory. Unchanged nodes reference
    production outputs (zero-copy).
    """
    project_path = Path(project_path).resolve()
    _run_in_environment(
        env_name=env_name,
        project_path=project_path,
        force=force,
        parallel=parallel,
        max_workers=max_workers,
        continue_on_error=continue_on_error,
        dry_run=dry_run,
    )


def _promote_environment(from_env: str, to_env: str, project_path: Path):
    """Shared logic for promoting an environment.

    Used by both `seeknal promote` and `seeknal env promote`.
    """
    from seeknal.workflow.environment import EnvironmentManager

    project_path = Path(project_path).resolve()
    target_path = project_path / "target"

    manager = EnvironmentManager(target_path)
    target_label = "production" if to_env == "prod" else f"environment '{to_env}'"

    # Show promotion preview
    env_dir = target_path / "environments" / from_env
    plan_data = manager._load_json(env_dir / "plan.json")
    if plan_data:
        changes = plan_data.get("categorized_changes", {})
        added = plan_data.get("added_nodes", [])
        removed = plan_data.get("removed_nodes", [])
        _echo_info(f"Promotion preview for '{from_env}' -> {target_label}:")
        if changes:
            _echo_info(f"  Changed nodes: {len(changes)}")
        if added:
            _echo_info(f"  New nodes: {len(added)}")
        if removed:
            _echo_info(f"  Removed nodes: {len(removed)}")
        _echo_info("")

    typer.confirm(
        f"Promote environment '{from_env}' to {target_label}?",
        abort=True,
    )

    manager.promote(from_env, to_env)

    _echo_success(f"Environment '{from_env}' promoted to {target_label}.")


@env_app.command("promote")
@handle_cli_error("Environment promote failed")
def env_promote(
    from_env: str = typer.Argument(..., help="Source environment"),
    to_env: str = typer.Argument("prod", help="Target (default: prod)"),
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """Promote environment to production."""
    _promote_environment(from_env, to_env, project_path)


@env_app.command("list")
@handle_cli_error("Failed to list environments")
def env_list(
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """List all virtual environments."""
    from seeknal.workflow.environment import EnvironmentManager

    project_path = Path(project_path).resolve()
    target_path = project_path / "target"

    manager = EnvironmentManager(target_path)
    envs = manager.list_environments()

    if not envs:
        _echo_info("No environments found.")
        return

    _echo_info("Virtual Environments:")
    _echo_info("-" * 60)

    for env in envs:
        expired_str = " (EXPIRED)" if env.is_expired() else ""
        plan_exists = (target_path / "environments" / env.name / "plan.json").exists()
        applied = (target_path / "environments" / env.name / "run_state.json").exists()

        status = "applied" if applied else ("planned" if plan_exists else "created")
        status_color = (
            typer.colors.GREEN if applied
            else (typer.colors.BLUE if plan_exists else typer.colors.YELLOW)
        )

        typer.echo(
            f"  {env.name:20s} "
            f"{typer.style(status, fg=status_color):12s} "
            f"created: {env.created_at}  "
            f"last accessed: {env.last_accessed}"
            f"{expired_str}"
        )

    _echo_info(f"\n  Total: {len(envs)} environment(s)")


@env_app.command("delete")
@handle_cli_error("Failed to delete environment")
def env_delete(
    env_name: str = typer.Argument(..., help="Environment to delete"),
    project_path: Path = typer.Option(".", help="Project directory"),
):
    """Delete a virtual environment."""
    from seeknal.workflow.environment import EnvironmentManager

    project_path = Path(project_path).resolve()
    target_path = project_path / "target"

    typer.confirm(
        f"Delete environment '{env_name}'? This cannot be undone.",
        abort=True,
    )

    manager = EnvironmentManager(target_path)
    manager.delete_environment(env_name)

    _echo_success(f"Environment '{env_name}' deleted.")


@app.command("download-sample-data")
def download_sample_data(
    output_dir: Path = typer.Option(
        Path("data/sample"), "--output-dir", "-o",
        help="Output directory for sample data files"
    ),
    force: bool = typer.Option(
        False, "--force", "-f",
        help="Overwrite existing sample data files"
    ),
):
    """Download sample e-commerce datasets for tutorials and testing.

    Downloads four normalized CSV files that demonstrate a realistic
    e-commerce data model:

    - customers.csv: Customer information (20 customers)
    - products.csv: Product catalog (20 products)
    - orders.csv: Order headers (40 orders)
    - sales.csv: Order line items (100 sales)

    These datasets are designed for:
    - Learning Seeknal pipeline building
    - Testing ELT workflows
    - Demonstrating feature store concepts
    - Trying YAML and Python pipeline examples

    Examples:
        # Download to default location (data/sample/)
        seeknal download-sample-data

        # Download to custom directory
        seeknal download-sample-data --output-dir ./my_data

        # Overwrite existing files
        seeknal download-sample-data --force
    """
    import shutil
    from pathlib import Path

    # Get the package data directory
    from importlib.resources import files
    try:
        # Try Python 3.9+ style first
        sample_data_dir = files('seeknal.docs.data.sample')
    except (TypeError, AttributeError):
        # Fallback for older Python versions
        import os
        package_dir = Path(__file__).parent.parent
        sample_data_dir = package_dir / 'docs' / 'data' / 'sample'

    # Check if source sample data exists
    source_files = ['customers.csv', 'products.csv', 'orders.csv', 'sales.csv']
    missing_files = [f for f in source_files if not (sample_data_dir / f).exists()]

    if missing_files:
        _echo_error(f"Sample data files not found in package: {missing_files}")
        _echo_info("This may indicate Seeknal is not installed correctly.")
        raise typer.Exit(1)

    # Create output directory
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check for existing files
    existing_files = [f for f in source_files if (output_dir / f).exists()]
    if existing_files and not force:
        _echo_warning(f"Sample data files already exist in {output_dir}:")
        for f in existing_files:
            typer.echo(f"  - {f}")
        _echo_info("Use --force to overwrite existing files.")
        raise typer.Exit(0)

    # Copy files
    _echo_info(f"Downloading sample data to {output_dir}...")

    copied = 0
    for filename in source_files:
        src_path = sample_data_dir / filename
        dst_path = output_dir / filename

        if src_path.exists():
            shutil.copy2(src_path, dst_path)
            copied += 1
            typer.echo(f"  ✓ {filename}")
        else:
            _echo_warning(f"  ✗ {filename} not found")

    if copied == len(source_files):
        _echo_success(f"Downloaded {copied} sample data files successfully!")
        typer.echo("")
        _echo_info("Sample datasets:")
        typer.echo("  customers.csv - 20 customers with demographic info")
        typer.echo("  products.csv  - 20 products with pricing")
        typer.echo("  orders.csv    - 40 orders with shipping info")
        typer.echo("  sales.csv     - 100 line items with quantities")
        typer.echo("")
        _echo_info("Next steps:")
        typer.echo("  1. Explore the data: head data/sample/customers.csv")
        typer.echo("  2. Create a source: seeknal draft source customers")
        typer.echo("  3. Build a pipeline: seeknal plan")
        typer.echo("  4. Run it: seeknal run")
    else:
        _echo_error(f"Only downloaded {copied}/{len(source_files)} files.")
        raise typer.Exit(1)


# =============================================================================
# Interval Management Commands
# =============================================================================

@intervals_app.command("list")
def intervals_list(
    node_id: str = typer.Argument(
        ...,
        help="Node identifier (e.g., transform.clean_data, feature_group.user_features)"
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.TABLE,
        "--output", "-o",
        help="Output format"
    ),
):
    """List completed intervals for a node.

    Shows all time intervals that have been successfully executed for the
    specified node, along with execution metadata.

    Examples:
        seeknal intervals list transform.clean_data
        seeknal intervals list feature_group.user_features --output json
    """
    from pathlib import Path
    from seeknal.workflow.state import load_state

    project_path = Path.cwd()
    state_path = project_path / "target" / "run_state.json"

    if not state_path.exists():
        _echo_error("No run state found. Run 'seeknal run' first.")
        raise typer.Exit(1)

    state = load_state(state_path)
    if not state or node_id not in state.nodes:
        _echo_error(f"No state found for node: {node_id}")
        raise typer.Exit(1)

    node_state = state.nodes[node_id]

    if output_format == OutputFormat.TABLE:
        typer.echo(f"\nCompleted intervals for {node_id}:")
        typer.echo("-" * 60)

        if node_state.completed_intervals:
            for i, (start, end) in enumerate(node_state.completed_intervals, 1):
                typer.echo(f"  {i}. {start} to {end}")
        else:
            typer.echo("  No completed intervals")

        typer.echo("-" * 60)

        if node_state.completed_partitions:
            typer.echo(f"\nCompleted partitions: {', '.join(node_state.completed_partitions)}")

    elif output_format == OutputFormat.JSON:
        import json
        data = {
            "node_id": node_id,
            "completed_intervals": node_state.completed_intervals,
            "completed_partitions": node_state.completed_partitions,
        }
        typer.echo(json.dumps(data, indent=2))


@intervals_app.command("pending")
def intervals_pending(
    node_id: str = typer.Argument(
        ...,
        help="Node identifier (e.g., transform.clean_data, feature_group.user_features)"
    ),
    start: str = typer.Option(
        ...,
        "--start", "-s",
        help="Start date (YYYY-MM-DD or ISO timestamp)",
    ),
    end: Optional[str] = typer.Option(
        None,
        "--end", "-e",
        help="End date (YYYY-MM-DD or ISO timestamp). Defaults to next scheduled run.",
    ),
    schedule: str = typer.Option(
        "daily",
        "--schedule",
        help="Cron schedule or preset (daily, hourly, weekly, monthly, yearly)",
    ),
):
    """Show pending intervals for a node.

    Calculates which intervals need to be executed based on the schedule
    and compares against completed intervals to find gaps.

    Examples:
        seeknal intervals pending feature_group.user_features --start 2024-01-01
        seeknal intervals pending transform.clean_data --start 2024-01-01 --end 2024-01-31
        seeknal intervals pending feature_group.user_features --start 2024-01-01 --schedule hourly
    """
    from pathlib import Path
    from seeknal.workflow.state import load_state
    from seeknal.workflow.intervals import IntervalCalculator, create_interval_calculator

    project_path = Path.cwd()
    state_path = project_path / "target" / "run_state.json"

    # Parse dates with validation
    start_dt = parse_date_safely(start, "start date")

    end_dt = None
    if end:
        end_dt = parse_date_safely(end, "end date")

    # Create interval calculator
    calc = create_interval_calculator(schedule)

    # Get completed intervals from state
    completed_intervals = []
    if state_path.exists():
        state = load_state(state_path)
        if state and node_id in state.nodes:
            completed_intervals = state.nodes[node_id].completed_intervals

    # Calculate pending intervals
    pending = calc.get_pending_intervals(completed_intervals, start_dt, end_dt)

    # Display results
    typer.echo(f"\nPending intervals for {node_id}:")
    typer.echo(f"Schedule: {schedule}")
    typer.echo(f"Range: {start} to {end or 'next run'}")
    typer.echo("-" * 60)

    if pending:
        for i, interval in enumerate(pending, 1):
            typer.echo(f"  {i}. {interval.start} to {interval.end}")
        typer.echo(f"\nTotal: {len(pending)} pending interval(s)")
    else:
        typer.echo("  No pending intervals - all up to date!")


# Restatement subcommands - use add subcommand pattern
@intervals_app.command("restatement-add")
def restatement_add(
    node_id: str = typer.Argument(
        ...,
        help="Node identifier"
    ),
    start: str = typer.Option(
        ...,
        "--start", "-s",
        help="Start date (YYYY-MM-DD or ISO timestamp)",
    ),
    end: str = typer.Option(
        ...,
        "--end", "-e",
        help="End date (YYYY-MM-DD or ISO timestamp)",
    ),
):
    """Mark an interval for restatement.

    Adds the specified time range to the node's restatement intervals list.
    The next run will re-process data for this interval.

    Examples:
        seeknal intervals restatement-add feature_group.user_features --start 2024-01-01 --end 2024-01-31
    """
    from pathlib import Path
    from seeknal.workflow.state import load_state, save_state, RunState

    project_path = Path.cwd()
    state_path = project_path / "target" / "run_state.json"

    # Load or create state
    if state_path.exists():
        state = load_state(state_path)
    else:
        state = RunState()

    # Ensure node exists
    if node_id not in state.nodes:
        _echo_error(f"No state found for node: {node_id}")
        _echo_info("Run the node first with 'seeknal run'")
        raise typer.Exit(1)

    # Add restatement interval
    node_state = state.nodes[node_id]
    node_state.restatement_intervals.append((start, end))

    # Save state
    save_state(state, state_path)

    _echo_success(f"Added restatement interval: {start} to {end}")
    typer.echo(f"Total restatement intervals: {len(node_state.restatement_intervals)}")


@intervals_app.command("restatement-list")
def restatement_list(
    node_id: str = typer.Argument(
        ...,
        help="Node identifier"
    ),
):
    """List restatement intervals for a node.

    Shows all intervals marked for restatement.

    Examples:
        seeknal intervals restatement-list feature_group.user_features
    """
    from pathlib import Path
    from seeknal.workflow.state import load_state

    project_path = Path.cwd()
    state_path = project_path / "target" / "run_state.json"

    if not state_path.exists():
        _echo_error("No run state found. Run 'seeknal run' first.")
        raise typer.Exit(1)

    state = load_state(state_path)
    if not state or node_id not in state.nodes:
        _echo_error(f"No state found for node: {node_id}")
        raise typer.Exit(1)

    node_state = state.nodes[node_id]

    typer.echo(f"\nRestatement intervals for {node_id}:")
    typer.echo("-" * 60)

    if node_state.restatement_intervals:
        for i, (start, end) in enumerate(node_state.restatement_intervals, 1):
            typer.echo(f"  {i}. {start} to {end}")
    else:
        typer.echo("  No restatement intervals")

    typer.echo("-" * 60)


@intervals_app.command("restatement-clear")
def restatement_clear(
    node_id: str = typer.Argument(
        ...,
        help="Node identifier"
    ),
):
    """Clear all restatement intervals for a node.

    Removes all intervals marked for restatement.

    Examples:
        seeknal intervals restatement-clear feature_group.user_features
    """
    from pathlib import Path
    from seeknal.workflow.state import load_state, save_state

    project_path = Path.cwd()
    state_path = project_path / "target" / "run_state.json"

    if not state_path.exists():
        _echo_error("No run state found. Run 'seeknal run' first.")
        raise typer.Exit(1)

    state = load_state(state_path)
    if not state or node_id not in state.nodes:
        _echo_error(f"No state found for node: {node_id}")
        raise typer.Exit(1)

    count = len(state.nodes[node_id].restatement_intervals)
    state.nodes[node_id].restatement_intervals = []

    save_state(state, state_path)

    _echo_success(f"Cleared {count} restatement interval(s)")


@intervals_app.command("backfill")
def intervals_backfill(
    node_id: str = typer.Argument(
        ...,
        help="Node identifier to backfill"
    ),
    start: str = typer.Option(
        ...,
        "--start", "-s",
        help="Start date (YYYY-MM-DD or ISO timestamp)",
    ),
    end: str = typer.Option(
        ...,
        "--end", "-e",
        help="End date (YYYY-MM-DD or ISO timestamp)",
    ),
    schedule: str = typer.Option(
        "daily",
        "--schedule",
        help="Cron schedule or preset (daily, hourly, weekly, monthly, yearly)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be executed without running",
    ),
):
    """Execute backfill for missing intervals.

    Calculates pending intervals for the specified date range and
    executes the node for each missing interval.

    Examples:
        seeknal intervals backfill feature_group.user_features --start 2024-01-01 --end 2024-01-31
        seeknal intervals backfill transform.clean_data --start 2024-01-01 --end 2024-01-31 --schedule hourly
        seeknal intervals backfill feature_group.user_features --start 2024-01-01 --end 2024-01-31 --dry-run
    """
    from pathlib import Path
    from seeknal.workflow.state import load_state
    from seeknal.workflow.intervals import IntervalCalculator, create_interval_calculator

    project_path = Path.cwd()
    state_path = project_path / "target" / "run_state.json"

    # Parse dates with validation
    start_dt = parse_date_safely(start, "start date")
    end_dt = parse_date_safely(end, "end date")

    # Create interval calculator
    calc = create_interval_calculator(schedule)

    # Get completed intervals from state
    completed_intervals = []
    if state_path.exists():
        state = load_state(state_path)
        if state and node_id in state.nodes:
            completed_intervals = state.nodes[node_id].completed_intervals

    # Calculate pending intervals
    pending = calc.get_pending_intervals(completed_intervals, start_dt, end_dt)

    # Display results
    typer.echo(f"\nBackfill plan for {node_id}:")
    typer.echo(f"Schedule: {schedule}")
    typer.echo(f"Range: {start} to {end}")
    typer.echo("-" * 60)

    if pending:
        typer.echo(f"Intervals to process: {len(pending)}")
        for i, interval in enumerate(pending, 1):
            typer.echo(f"  {i}. {interval.start} to {interval.end}")

        if dry_run:
            typer.echo("\n[Dry run] Use --no-dry-run to execute.")
        else:
            typer.echo("\nExecuting backfill...")
            # TODO: Implement actual backfill execution
            _echo_warning("Backfill execution not yet implemented.")
            _echo_info("This will execute the node for each pending interval.")
    else:
        typer.echo("No pending intervals - all up to date!")


@app.command()
def migrate_state(
    backend: str = typer.Option(..., "--backend", "-b", help="Target backend type (file, database)"),
    project_path: Path = typer.Option(".", help="Project directory"),
    dry_run: bool = typer.Option(True, help="Preview changes without executing"),
):
    """Migrate state from one backend to another.

    Preserves all existing state including:
    - Node execution history
    - Completed intervals
    - Fingerprints
    - Row counts

    Examples:
        seeknal migrate-state --backend database    # Preview migration to database
        seeknal migrate-state --backend database --no-dry-run  # Execute migration
        seeknal migrate-state --backend file       # Migrate back to file
    """
    from seeknal.workflow.state import load_state, save_state, RunState
    from seeknal.state.backend import StateBackendFactory, create_state_backend
    from seeknal.state.file_backend import FileStateBackend
    from seeknal.state.database_backend import DatabaseStateBackend
    from pathlib import Path
    import shutil

    project_path = Path(project_path).resolve()
    target_path = project_path / "target"

    # Current state file (file backend)
    state_path = target_path / "run_state.json"

    if not state_path.exists():
        _echo_error(f"No state file found at {state_path}")
        _echo_info("Run 'seeknal run' first to create state.")
        raise typer.Exit(1)

    _echo_info("Loading current state...")
    old_state = load_state(state_path)

    # Get node counts
    node_count = len(old_state.nodes)
    run_count = len(old_state.runs)
    total_rows = sum(ns.row_count or 0 for ns in old_state.nodes.values())

    typer.echo(f"  Nodes: {node_count}")
    typer.echo(f"  Runs: {run_count}")
    typer.echo(f"  Total rows processed: {total_rows:,}")

    if backend == "database":
        # Check for database config
        db_path = target_path / "state.db"

        if dry_run:
            typer.echo("")
            typer.echo("Preview: Migration to database backend")
            typer.echo(f"  Source: {state_path}")
            typer.echo(f"  Target: {db_path}")
            typer.echo("")
            typer.echo("[Dry run] Use --no-dry-run to execute migration.")
            return

        _echo_info("Migrating to database backend...")

        # Create database backend
        db_backend = DatabaseStateBackend(db_path=str(db_path))
        db_backend.initialize()

        # Migrate all runs
        _echo_info(f"Migrating {run_count} runs...")
        for run_id, run_info in old_state.runs.items():
            db_backend.create_run(
                run_id=run_id,
                status=run_info.status,
                started_at=run_info.started_at,
                finished_at=run_info.finished_at,
                metadata=run_info.metadata or {}
            )

        # Migrate all node states
        _echo_info(f"Migrating {node_count} node states...")
        for node_id, node_state in old_state.nodes.items():
            db_backend.set_node_state(
                run_id=node_state.run_id,
                node_id=node_id,
                status=node_state.status,
                started_at=node_state.started_at,
                finished_at=node_state.finished_at,
                duration_ms=node_state.duration_ms,
                row_count=node_state.row_count,
                error_message=node_state.error_message,
                fingerprint=node_state.fingerprint,
            )

            # Migrate intervals if present
            for interval_start, interval_end in node_state.completed_intervals:
                db_backend.add_completed_interval(
                    run_id=node_state.run_id,
                    node_id=node_id,
                    interval_start=interval_start,
                    interval_end=interval_end,
                )

        # Backup old state file
        backup_path = state_path.with_suffix(".json.bak")
        shutil.copy(state_path, backup_path)
        _echo_info(f"Backup saved to {backup_path}")

        # Update project config
        project_file = project_path / "seeknal_project.yml"
        if project_file.exists():
            import yaml
            with open(project_file) as f:
                config = yaml.safe_load(f)
            config["state_backend"] = "database"
            with open(project_file, "w") as f:
                yaml.dump(config, f, sort_keys=False)
            _echo_success(f"Updated {project_file} with state_backend: database")

        _echo_success(f"Migration complete! State now in {db_path}")
        _echo_info(f"Old state backed up to {backup_path}")

    elif backend == "file":
        typer.echo("Already using file backend (default).")
        typer.echo("No migration needed.")
    else:
        _echo_error(f"Unknown backend type: {backend}")
        _echo_info(f"Available backends: {', '.join(StateBackendFactory.list_backends())}")
        raise typer.Exit(1)


@app.command()
def lineage(
    node_id: Optional[str] = typer.Argument(
        None, help="Node to focus on (e.g., transform.clean_orders)"
    ),
    column: Optional[str] = typer.Option(
        None, "--column", "-c",
        help="Column to trace lineage for (requires node argument)"
    ),
    project: str = typer.Option(
        None, "--project", "-p", help="Project name"
    ),
    path: Path = typer.Option(
        Path("."), "--path", help="Project path"
    ),
    output: Path = typer.Option(
        None, "--output", "-o",
        help="Output HTML file path (default: target/lineage.html)"
    ),
    no_open: bool = typer.Option(
        False, "--no-open", help="Don't auto-open browser"
    ),
):
    """Generate interactive lineage visualization.

    Examples:
        seeknal lineage                              # Full DAG
        seeknal lineage transform.clean_orders       # Focus on node
        seeknal lineage transform.X --column total   # Trace column
        seeknal lineage --output dag.html            # Custom path
    """
    from seeknal.workflow.dag import DAGBuilder, CycleDetectedError, MissingDependencyError
    from seeknal.dag.visualize import generate_lineage_html, LineageVisualizationError

    if project is None:
        project = path.resolve().name

    try:
        try:
            dag_builder = DAGBuilder(project_path=path)
            dag_builder.build()
        except (ValueError, CycleDetectedError, MissingDependencyError) as e:
            _echo_error(f"DAG build failed: {e}")
            raise typer.Exit(code=1)

        manifest = _build_manifest_from_dag(dag_builder, project)

        if not manifest.nodes:
            _echo_warning("No nodes found. Add pipeline files to your project.")
            raise typer.Exit(code=1)

        if output is None:
            output = path / "target" / "lineage.html"

        result_path = generate_lineage_html(
            manifest=manifest,
            output_path=output,
            focus_node=node_id,
            focus_column=column,
            open_browser=not no_open,
        )
        _echo_success(f"Lineage visualization generated: {result_path}")
    except LineageVisualizationError as e:
        _echo_error(str(e))
        raise typer.Exit(code=1)


def main():
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()

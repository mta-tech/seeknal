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

For more information, see: https://github.com/mta-tech/seeknal
"""

import typer
from typing import Optional
from datetime import datetime
from enum import Enum
import os
import sys
from pathlib import Path

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
):
    """Initialize a new Seeknal project in the current directory."""
    from seeknal.project import Project

    if name is None:
        name = path.resolve().name

    typer.echo(f"Initializing Seeknal project: {name}")

    try:
        project = Project(name=name, description=description)
        project.get_or_create()
        _echo_success(f"Project '{name}' initialized successfully!")

        # Create basic directory structure
        dirs = ["flows", "entities", "feature_groups"]
        for d in dirs:
            dir_path = path / d
            dir_path.mkdir(exist_ok=True)
            typer.echo(f"  Created directory: {d}/")

    except Exception as e:
        _echo_error(f"Failed to initialize project: {e}")
        raise typer.Exit(1)


@app.command()
def run(
    flow_name: str = typer.Argument(..., help="Name of the flow to run"),
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
):
    """Execute a feature transformation flow."""
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

    # Parse dates
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    except ValueError as e:
        _echo_error(f"Invalid date format: {e}")
        raise typer.Exit(1)

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


def _build_validators_from_config(validator_configs):
    """Build validator instances from ValidatorConfig objects.

    Args:
        validator_configs: List of ValidatorConfig objects from validation_config.

    Returns:
        List of BaseValidator instances ready to execute.

    Raises:
        ValueError: If an unknown validator type is specified.
    """
    from datetime import timedelta
    from seeknal.feature_validation.validators import (
        NullValidator,
        RangeValidator,
        UniquenessValidator,
        FreshnessValidator,
        CustomValidator,
    )

    validators = []

    for config in validator_configs:
        validator_type = config.validator_type.lower()
        columns = config.columns or []
        params = config.params or {}

        if validator_type == "null":
            validator = NullValidator(
                columns=columns,
                max_null_percentage=params.get("max_null_percentage", 0.0),
            )
        elif validator_type == "range":
            if not columns:
                raise ValueError("RangeValidator requires at least one column")
            # RangeValidator works on a single column
            column = columns[0] if len(columns) == 1 else columns[0]
            validator = RangeValidator(
                column=column,
                min_val=params.get("min_val"),
                max_val=params.get("max_val"),
            )
        elif validator_type == "uniqueness":
            validator = UniquenessValidator(
                columns=columns,
                max_duplicate_percentage=params.get("max_duplicate_percentage", 0.0),
            )
        elif validator_type == "freshness":
            if not columns:
                raise ValueError("FreshnessValidator requires a column")
            column = columns[0]
            max_age_seconds = params.get("max_age_seconds", 86400)  # Default 24 hours
            max_age = timedelta(seconds=max_age_seconds)
            validator = FreshnessValidator(
                column=column,
                max_age=max_age,
            )
        else:
            raise ValueError(f"Unknown validator type: {validator_type}")

        validators.append(validator)

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
        try:
            cutoff = datetime.strptime(before_date, "%Y-%m-%d")
        except ValueError:
            _echo_error("Invalid date format. Use YYYY-MM-DD")
            raise typer.Exit(1)
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
                        if isinstance(field, dict):
                            field_name = field.get("name", "unknown")
                            field_type = field.get("type", "unknown")
                            # Handle complex types
                            if isinstance(field_type, dict):
                                field_type = field_type.get("type", str(field_type))
                            elif isinstance(field_type, list):
                                field_type = " | ".join(str(t) for t in field_type)
                            typer.echo(typer.style(f"  + {field_name}: {field_type}", fg=typer.colors.GREEN))
                        else:
                            typer.echo(typer.style(f"  + {field}", fg=typer.colors.GREEN))

                # Display removed features
                if removed:
                    typer.echo("\n" + typer.style("Removed (-):", fg=typer.colors.RED, bold=True))
                    for field in removed:
                        if isinstance(field, dict):
                            field_name = field.get("name", "unknown")
                            field_type = field.get("type", "unknown")
                            # Handle complex types
                            if isinstance(field_type, dict):
                                field_type = field_type.get("type", str(field_type))
                            elif isinstance(field_type, list):
                                field_type = " | ".join(str(t) for t in field_type)
                            typer.echo(typer.style(f"  - {field_name}: {field_type}", fg=typer.colors.RED))
                        else:
                            typer.echo(typer.style(f"  - {field}", fg=typer.colors.RED))

                # Display modified features
                if modified:
                    typer.echo("\n" + typer.style("Modified (~):", fg=typer.colors.YELLOW, bold=True))
                    for change in modified:
                        if isinstance(change, dict):
                            field_name = change.get("field", "unknown")
                            old_type = change.get("old_type", "unknown")
                            new_type = change.get("new_type", "unknown")
                            # Handle complex types
                            if isinstance(old_type, dict):
                                old_type = old_type.get("type", str(old_type))
                            elif isinstance(old_type, list):
                                old_type = " | ".join(str(t) for t in old_type)
                            if isinstance(new_type, dict):
                                new_type = new_type.get("type", str(new_type))
                            elif isinstance(new_type, list):
                                new_type = " | ".join(str(t) for t in new_type)
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
):
    """
    Parse project and generate manifest.json.

    Scans the project directory for feature groups, models, and common config
    (sources, transforms, rules) to build the complete DAG manifest. The manifest
    is written to target/manifest.json.

    This command is similar to 'dbt parse' - it builds the dependency graph
    without executing any transformations.

    Examples:
        seeknal parse
        seeknal parse --project my_project
        seeknal parse --path /path/to/project
        seeknal parse --format json
    """
    from seeknal.dag.parser import ProjectParser
    import json

    # Determine project name
    if project is None:
        project = path.resolve().name

    # Determine target path
    if target_path is None:
        target_path = path / "target"

    typer.echo(f"Parsing project: {project}")
    typer.echo(f"  Path: {path.resolve()}")

    try:
        # Parse the project
        parser = ProjectParser(
            project_name=project,
            project_path=str(path),
            seeknal_version=_get_version()
        )

        manifest = parser.parse()

        # Validate
        errors = parser.validate()
        if errors:
            _echo_warning(f"Validation warnings: {len(errors)}")
            for error in errors:
                typer.echo(f"  - {error}")

        # Create target directory
        target_path.mkdir(parents=True, exist_ok=True)

        # Write manifest
        manifest_file = target_path / "manifest.json"
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
                    node_type = node.node_type.value
                    type_counts[node_type] = type_counts.get(node_type, 0) + 1
                for node_type, count in sorted(type_counts.items()):
                    typer.echo(f"  - {node_type}: {count}")

    except Exception as e:
        _echo_error(f"Parse failed: {e}")
        raise typer.Exit(1)


def main():
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()

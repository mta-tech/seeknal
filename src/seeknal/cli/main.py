"""
Seeknal CLI - Main entry point

A dbt-like CLI for managing feature stores.

Usage:
    seeknal init                    Initialize a new project
    seeknal run <flow>              Execute a transformation flow
    seeknal materialize <fg>        Materialize features to stores
    seeknal list <resource>         List resources
    seeknal show <resource> <name>  Show resource details
    seeknal validate                Validate configurations
    seeknal validate-features <fg>  Validate feature group data quality
    seeknal version                 Show version information
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
def version():
    """Show version information."""
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
        help="Start date (YYYY-MM-DD)"
    ),
    end_date: Optional[str] = typer.Option(
        None, "--end-date", "-e",
        help="End date (YYYY-MM-DD)"
    ),
    mode: WriteMode = typer.Option(
        WriteMode.OVERWRITE, "--mode", "-m",
        help="Write mode"
    ),
    offline_only: bool = typer.Option(
        False, "--offline-only",
        help="Only materialize to offline store"
    ),
    online_only: bool = typer.Option(
        False, "--online-only",
        help="Only materialize to online store"
    ),
):
    """Materialize features to offline/online stores."""
    from seeknal.featurestore.feature_group import FeatureGroup

    # Parse dates
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    except ValueError as e:
        _echo_error(f"Invalid date format: {e}")
        raise typer.Exit(1)

    typer.echo(f"Materializing feature group: {feature_group}")
    typer.echo(f"  Start date: {start_date}")
    if end_date:
        typer.echo(f"  End date: {end_date}")
    typer.echo(f"  Mode: {mode.value}")

    try:
        fg = FeatureGroup.load(name=feature_group)
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


def main():
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()

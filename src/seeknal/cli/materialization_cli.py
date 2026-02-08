"""
CLI commands for Iceberg materialization.

This module provides Typer-based CLI commands for managing Iceberg
materialization configuration and snapshots.

Commands:
- validate-materialization: Validate materialization configuration
- snapshot-list: List snapshots for an Iceberg table
- snapshot-show: Show details of a specific snapshot
- materialize-setup: Interactive setup for materialization credentials
"""

import typer
from pathlib import Path
from typing import Optional

from seeknal.workflow.materialization.profile_loader import ProfileLoader, CredentialManager
from seeknal.workflow.materialization.operations import SnapshotManager, DuckDBIcebergExtension


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

app = typer.Typer(help="Iceberg materialization commands")


@app.command()
def validate_materialization(
    profile_path: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Path to profiles.yml (default: ~/.seeknal/profiles.yml)",
    ),
):
    """
    Validate materialization configuration.

    Checks that:
    - Profile file exists and is valid YAML
    - Catalog credentials are accessible
    - Catalog connection succeeds
    - Warehouse path is valid
    - Schema evolution settings are valid

    Example:
        seeknal iceberg validate-materialization
        seeknal iceberg validate-materialization --profile custom-profiles.yml
    """
    try:
        _echo_info("Validating materialization configuration...")

        # Load profile
        if profile_path:
            profile = Path(profile_path)
            loader = ProfileLoader(profile_path=profile)
        else:
            loader = ProfileLoader()

        # Validate configuration
        loader.validate_materialization()

        _echo_success("✓ Materialization configuration is valid")

    except Exception as e:
        _echo_error(f"Validation failed: {e}")
        raise typer.Exit(1)


@app.command()
def snapshot_list(
    table_name: str = typer.Argument(
        ...,
        help="Fully qualified table name (e.g., warehouse.prod.orders)",
    ),
    limit: int = typer.Option(
        10,
        "--limit",
        "-l",
        help="Maximum number of snapshots to list (default: 10)",
    ),
):
    """
    List snapshots for an Iceberg table.

    Shows the most recent snapshots for the specified table,
    including snapshot ID, timestamp, and schema version.

    Example:
        seeknal iceberg snapshot-list warehouse.prod.orders
        seeknal iceberg snapshot-list warehouse.prod.orders --limit 20
    """
    try:
        import duckdb

        _echo_info(f"Listing snapshots for {table_name}...")

        # Get DuckDB connection
        con = duckdb.connect(":memory:")

        # Load Iceberg extension
        DuckDBIcebergExtension.load_extension(con)

        # Create snapshot manager
        snapshot_mgr = SnapshotManager(con)

        # List snapshots
        snapshots = snapshot_mgr.list_snapshots(table_name, limit=limit)

        if not snapshots:
            _echo_warning(f"No snapshots found for {table_name}")
            return

        # Display snapshots
        _echo_success(f"\nFound {len(snapshots)} snapshot(s):\n")

        for snapshot in snapshots:
            snapshot_id_short = snapshot.snapshot_id[:8]
            timestamp_str = snapshot.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            _echo_info(
                f"  {snapshot_id_short} | "
                f"{timestamp_str} | "
                f"Schema v{snapshot.schema_version}"
            )

    except Exception as e:
        _echo_error(f"Failed to list snapshots: {e}")
        raise typer.Exit(1)


@app.command()
def snapshot_show(
    table_name: str = typer.Argument(
        ...,
        help="Fully qualified table name (e.g., warehouse.prod.orders)",
    ),
    snapshot_id: str = typer.Argument(
        ...,
        help="Snapshot ID (or first 8 characters)",
    ),
):
    """
    Show details of a specific snapshot.

    Displays detailed information about a snapshot including
    creation timestamp, schema version, row count, and expiration.

    Example:
        seeknal iceberg snapshot-show warehouse.prod.orders 12345678
    """
    try:
        import duckdb

        # Get DuckDB connection
        con = duckdb.connect(":memory:")

        # Load Iceberg extension
        DuckDBIcebergExtension.load_extension(con)

        # Create snapshot manager
        snapshot_mgr = SnapshotManager(con)

        # Get snapshot (handle partial snapshot IDs)
        all_snapshots = snapshot_mgr.list_snapshots(table_name, limit=100)

        # Find matching snapshot
        snapshot = None
        for s in all_snapshots:
            if s.snapshot_id.startswith(snapshot_id):
                snapshot = s
                break

        if not snapshot:
            _echo_error(f"Snapshot {snapshot_id} not found for {table_name}")
            raise typer.Exit(1)

        # Display snapshot details
        _echo_success(f"\nSnapshot Details\n")
        _echo_info(f"  ID:              {snapshot.snapshot_id}")
        _echo_info(f"  Created:         {snapshot.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        _echo_info(f"  Schema Version:  {snapshot.schema_version}")

        if snapshot.expires_at:
            expires_str = snapshot.expires_at.strftime("%Y-%m-%d %H:%M:%S")
            _echo_info(f"  Expires At:      {expires_str}")
        else:
            _echo_info(f"  Expires At:      Never (retention policy)")

    except Exception as e:
        _echo_error(f"Failed to get snapshot details: {e}")
        raise typer.Exit(1)


@app.command()
def setup(
    use_keyring: bool = typer.Option(
        False,
        "--keyring",
        "-k",
        help="Use system keyring for credentials (more secure)",
    ),
):
    """
    Interactive setup for materialization credentials.

    Prompts for Lakekeeper catalog credentials and stores them
    either in environment variables (displayed as export commands)
    or in the system keyring.

    Example:
        # Environment variables (default)
        seeknal iceberg setup

        # System keyring (more secure)
        seeknal iceberg setup --keyring
    """
    try:
        credential_mgr = CredentialManager(use_keyring=use_keyring)

        _echo_info("Setting up Iceberg materialization credentials...")
        _echo_info("")

        # Run interactive setup
        credential_mgr.setup_credentials()

        _echo_success("\n✓ Credentials configured successfully")
        _echo_info("\nYou can now enable materialization in your profiles.yml:\n")
        _echo_info("    materialization:")
        _echo_info("      enabled: true")
        _echo_info("      catalog:")
        _echo_info("        uri: ${LAKEKEEPER_URI}")
        _echo_info("        warehouse: s3://my-bucket/warehouse")

    except Exception as e:
        _echo_error(f"Setup failed: {e}")
        raise typer.Exit(1)


@app.command()
def profile_show(
    profile_path: Optional[str] = typer.Option(
        None,
        "--profile",
        "-p",
        help="Path to profiles.yml (default: ~/.seeknal/profiles.yml)",
    ),
):
    """
    Show current materialization profile configuration.

    Displays the active materialization configuration from profiles.yml,
    including catalog settings, schema evolution options, and defaults.

    Example:
        seeknal iceberg profile-show
        seeknal iceberg profile-show --profile custom-profiles.yml
    """
    try:
        # Load profile
        if profile_path:
            profile = Path(profile_path)
            loader = ProfileLoader(profile_path=profile)
        else:
            loader = ProfileLoader()

        config = loader.load_profile()

        if not config.enabled:
            _echo_warning("Materialization is not enabled in the profile")
            _echo_info("\nTo enable, set 'enabled: true' in profiles.yml:\n")
            _echo_info("    materialization:")
            _echo_info("      enabled: true")
            return

        # Display configuration
        _echo_success("Materialization Profile\n")

        _echo_info("  Enabled:         Yes")
        _echo_info(f"  Catalog Type:    {config.catalog.type.value}")
        _echo_info(f"  Catalog URI:      {config.catalog.uri or '(not set)'}")
        _echo_info(f"  Warehouse:        {config.catalog.warehouse or '(not set)'}")
        _echo_info(f"  Default Mode:     {config.default_mode.value}")
        _echo_info(f"  TLS Verification:  {config.catalog.verify_tls}")

        _echo_info("\n  Schema Evolution:")
        _echo_info(f"    Mode:           {config.schema_evolution.mode.value}")
        _echo_info(f"    Allow Add:       {config.schema_evolution.allow_column_add}")
        _echo_info(f"    Allow Type:      {config.schema_evolution.allow_column_type_change}")

        if config.partition_by:
            _echo_info(f"  Partitions:       {', '.join(config.partition_by)}")

    except Exception as e:
        _echo_error(f"Failed to load profile: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()

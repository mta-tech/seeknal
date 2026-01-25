"""
Apply command for Seeknal workflow.

Moves file to production and updates manifest.
"""

import typer
from pathlib import Path
from typing import Optional
import sys
import yaml
import shutil
import subprocess

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from seeknal.cli.main import _echo_success, _echo_error, _echo_warning, _echo_info
from seeknal.utils.path_security import warn_if_insecure_path


def validate_draft_file(file_path: str) -> Path:
    """Validate draft file exists.

    Args:
        file_path: Path to draft YAML file

    Returns:
        Path object

    Raises:
        typer.Exit: If file doesn't exist
    """
    path = Path(file_path)

    if not path.exists():
        _echo_error(f"File not found: {file_path}")
        raise typer.Exit(1)

    return path


def get_node_type_and_name(yaml_data: dict) -> tuple[str, str]:
    """Extract node type and name from YAML data.

    Args:
        yaml_data: Parsed YAML data

    Returns:
        Tuple of (node_type, name)

    Raises:
        ValueError: If kind or name is missing
    """
    kind = yaml_data.get("kind")
    name = yaml_data.get("name")

    if not kind:
        raise ValueError("Missing required field: kind")
    if not name:
        raise ValueError("Missing required field: name")

    return (kind, name)


def get_target_path(node_type: str, name: str) -> Path:
    """Get target file path for applied node.

    Args:
        node_type: Node kind (e.g., "feature_group")
        name: Node name

    Returns:
        Target path (seeknal/<type>s/<name>.yml)
    """
    # Normalize node type to directory name
    type_dir = f"{node_type}s"

    # Create target path
    target_dir = Path.cwd() / "seeknal" / type_dir
    target_file = target_dir / f"{name}.yml"

    return target_file


def check_conflict(target_path: Path, force: bool, new_data: dict) -> tuple[bool, Optional[dict]]:
    """Check if target file exists and handle conflict.

    Args:
        target_path: Target file path
        force: Skip confirmation
        new_data: New YAML data for comparison

    Returns:
        Tuple of (should_proceed, existing_yaml_data)

    Raises:
        typer.Exit: If user cancels
    """
    if not target_path.exists():
        return True, None

    # Load existing YAML for comparison
    try:
        with open(target_path, "r") as f:
            existing_data = yaml.safe_load(f)
    except Exception:
        existing_data = None

    if not force:
        _echo_warning(f"Node already exists: {target_path}")

        # Show diff if we have both versions
        if existing_data:
            show_yaml_diff(existing_data, new_data, target_path)

            _echo_info("")
            _echo_info("Use --force to apply these changes")

        raise typer.Exit(1)

    return True, existing_data


def show_yaml_diff(old_data: dict, new_data: Optional[dict], target_path: Path) -> None:
    """Show differences between old and new YAML.

    Args:
        old_data: Existing YAML data
        new_data: New YAML data (None if showing prompt)
        target_path: Target file path
    """
    _echo_info("")
    _echo_info("Changes:")

    changes_found = False

    # Compare description
    old_desc = old_data.get("description", "")
    new_desc = new_data.get("description", "") if new_data else ""

    if old_desc != new_desc:
        if old_desc:
            _echo_info(f"  - description: \"{old_desc}\"")
        if new_desc:
            _echo_info(f"  + description: \"{new_desc}\"")
        changes_found = True

    # Compare owner
    old_owner = old_data.get("owner", "")
    new_owner = new_data.get("owner", "") if new_data else ""

    if old_owner != new_owner:
        if old_owner:
            _echo_info(f"  - owner: {old_owner}")
        if new_owner:
            _echo_info(f"  + owner: {new_owner}")
        changes_found = True

    # Compare columns (for sources)
    old_cols = old_data.get("columns", {})
    new_cols = new_data.get("columns", {}) if new_data else {}

    if old_cols != new_cols:
        # Removed columns
        for col in set(old_cols.keys()) - set(new_cols.keys()):
            _echo_info(f"  - column: {col}")
            changes_found = True

        # Added columns
        for col in set(new_cols.keys()) - set(old_cols.keys()):
            desc = new_cols.get(col, "")
            _echo_info(f"  + column: {col} ({desc})")
            changes_found = True

        # Modified columns
        for col in set(old_cols.keys()) & set(new_cols.keys()):
            if old_cols[col] != new_cols[col]:
                _echo_info(f"  ~ column: {col}")
                _echo_info(f"      - \"{old_cols[col]}\"")
                _echo_info(f"      + \"{new_cols[col]}\"")
                changes_found = True

    # Compare features (for feature_groups)
    old_feats = old_data.get("features", {})
    new_feats = new_data.get("features", {}) if new_data else {}

    if old_feats != new_feats:
        # Removed features
        for feat in set(old_feats.keys()) - set(new_feats.keys()):
            _echo_info(f"  - feature: {feat}")
            changes_found = True

        # Added features
        for feat in set(new_feats.keys()) - set(old_feats.keys()):
            dtype = new_feats[feat].get("dtype", "unknown") if isinstance(new_feats[feat], dict) else "unknown"
            _echo_info(f"  + feature: {feat} ({dtype})")
            changes_found = True

        # Modified features
        for feat in set(old_feats.keys()) & set(new_feats.keys()):
            if old_feats[feat] != new_feats[feat]:
                _echo_info(f"  ~ feature: {feat}")
                changes_found = True

    # Compare transform SQL (for transforms/feature_groups)
    old_transform = old_data.get("transform", "")
    new_transform = new_data.get("transform", "") if new_data else ""

    if old_transform != new_transform:
        if old_transform:
            _echo_info(f"  - transform: [SQL changed]")
        if new_transform:
            _echo_info(f"  + transform: [SQL changed]")
        changes_found = True

    # Compare table/source
    old_table = old_data.get("table", "")
    new_table = new_data.get("table", "") if new_data else ""

    if old_table != new_table:
        if old_table:
            _echo_info(f"  - table: {old_table}")
        if new_table:
            _echo_info(f"  + table: {new_table}")
        changes_found = True

    # Compare inputs/dependencies
    old_inputs = old_data.get("inputs", [])
    new_inputs = new_data.get("inputs", []) if new_data else []

    if old_inputs != new_inputs:
        old_refs = [inp.get("ref", "") for inp in old_inputs]
        new_refs = [inp.get("ref", "") for inp in new_inputs]

        if set(old_refs) != set(new_refs):
            if old_refs:
                _echo_info(f"  - depends_on: {', '.join(old_refs)}")
            if new_refs:
                _echo_info(f"  + depends_on: {', '.join(new_refs)}")
            changes_found = True

    if not changes_found:
        _echo_info("  (no changes detected)")


def move_draft_file(source_path: Path, target_path: Path) -> None:
    """Move draft file to target location.

    Args:
        source_path: Source draft file path
        target_path: Target file path

    Raises:
        OSError: If move fails
    """
    # Create target directory if needed
    target_path.parent.mkdir(parents=True, exist_ok=True)

    # Move file
    shutil.move(str(source_path), str(target_path))


def update_manifest(target_path: Path) -> bool:
    """Update manifest by running seeknal parse.

    Args:
        target_path: Path to applied file (for context)

    Returns:
        True if successful, False otherwise

    Raises:
        subprocess.CalledProcessError: If parse fails
    """
    try:
        # Run seeknal parse
        result = subprocess.run(
            ["seeknal", "parse"],
            cwd=Path.cwd(),
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            _echo_error(f"Manifest update failed: {result.stderr}")
            return False

        return True
    except subprocess.TimeoutExpired:
        _echo_error("Manifest update timed out")
        return False
    except FileNotFoundError:
        # seeknal command not found (might be in dev mode)
        _echo_warning("Manifest update skipped (seeknal parse not available)")
        return True


def apply_command(
    file_path: str = typer.Argument(..., help="Path to draft YAML file"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing file without prompt"),
    no_parse: bool = typer.Option(False, "--no-parse", help="Skip manifest regeneration"),
):
    """Apply draft file to production and update manifest.

    Workflow:
    1. Validate file exists and YAML is valid
    2. Check if target exists (prompt or require --force)
    3. Move file to seeknal/<type>s/<name>.yml
    4. Run seeknal parse to regenerate manifest
    5. Show diff of changes

    Examples:
        # Apply draft file
        $ seeknal apply draft_feature_group_user_behavior.yml

        # Apply with overwrite
        $ seeknal apply draft_feature_group_user_behavior.yml --force

        # Apply without updating manifest
        $ seeknal apply draft_source_postgres.yml --no-parse
    """
    # Validate file exists
    draft_path = validate_draft_file(file_path)

    _echo_info("Validating...")

    # Parse YAML
    try:
        with open(draft_path, "r") as f:
            yaml_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        _echo_error(f"Invalid YAML: {e}")
        raise typer.Exit(1)

    # Extract node type and name
    try:
        node_type, name = get_node_type_and_name(yaml_data)
    except ValueError as e:
        _echo_error(str(e))
        raise typer.Exit(1)

    # Get target path
    target_path = get_target_path(node_type, name)

    # Validate target path for security
    is_insecure, secure_alt = warn_if_insecure_path(str(target_path), "apply command")
    if is_insecure:
        _echo_warning(f"Using secure alternative: {secure_alt}")
        target_path = Path(secure_alt)

    # Check for conflicts and get existing data
    try:
        should_proceed, existing_data = check_conflict(target_path, force, yaml_data)
    except typer.Exit:
        raise

    if not should_proceed:
        return

    _echo_success("All checks passed")

    # Move file
    _echo_info("Moving file...")
    _echo_info(f"  FROM: {draft_path}")
    _echo_info(f"  TO:   {target_path}")

    try:
        move_draft_file(draft_path, target_path)
    except OSError as e:
        _echo_error(f"Failed to move file: {e}")
        raise typer.Exit(1)

    # Update manifest
    if not no_parse:
        _echo_info("Updating manifest...")

        if update_manifest(target_path):
            _echo_success("Manifest regenerated")
        else:
            _echo_warning("Manifest update failed, but file was moved")

    # Show diff if this was an update
    if existing_data:
        _echo_info("")
        show_yaml_diff(existing_data, yaml_data, target_path)
    else:
        # Show summary for new node
        _echo_info("")
        _echo_info("Added:")
        _echo_info(f"  + {node_type}.{name}")

        # Show columns if present
        if "columns" in yaml_data:
            for col, desc in yaml_data["columns"].items():
                _echo_info(f"    - {col} ({desc})")

        # Show features if present
        if "features" in yaml_data:
            for feat, config in yaml_data["features"].items():
                dtype = config.get("dtype", "unknown")
                _echo_info(f"    - {feat} ({dtype})")

        # Show depends_on if present
        if "inputs" in yaml_data:
            deps = [inp.get("ref", "") for inp in yaml_data["inputs"]]
            if deps:
                _echo_info(f"    - depends_on: {', '.join(deps)}")

    _echo_success("Applied successfully")


if __name__ == "__main__":
    typer.run(apply_command)

"""
Apply command for Seeknal workflow.

Moves file to production and updates manifest.
"""

import difflib
import ast

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

# Common config kind → target file mapping
COMMON_CONFIG_MAPPING = {
    "common-source": {
        "file": "sources.yml",
        "list_key": "sources",
        "fields": ["ref", "params"],
    },
    "common-rule": {
        "file": "rules.yml",
        "list_key": "rules",
        "fields": ["value"],
    },
    "common-transformation": {
        "file": "transformations.yml",
        "list_key": "transformations",
        "fields": ["sql"],
    },
}


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


def check_conflict(
    source_path: Path, target_path: Path, force: bool, new_data: dict
) -> tuple[bool, Optional[dict], bool]:
    """Check if target file exists and handle conflict.

    Args:
        source_path: Source file path (the file being applied)
        target_path: Target file path
        force: Skip confirmation
        new_data: New YAML data for comparison

    Returns:
        Tuple of (should_proceed, existing_yaml_data, is_same_file)

    Raises:
        typer.Exit: If user cancels
    """
    # Check if source and target are the same file
    is_same_file = source_path.resolve() == target_path.resolve()

    if not target_path.exists():
        return True, None, is_same_file

    # Load existing YAML for comparison
    try:
        with open(target_path, "r") as f:
            existing_data = yaml.safe_load(f)
    except Exception:
        existing_data = None

    # If source == target, the file is already in place
    # Just need to run parse to update manifest
    if is_same_file:
        return True, existing_data, is_same_file

    _echo_warning(f"Node already exists: {target_path}")

    # Show diff if we have both versions
    if existing_data:
        has_changes = show_yaml_diff(existing_data, new_data, target_path)

        _echo_info("")

        if not has_changes:
            _echo_info("Files are identical. No action needed.")
            raise typer.Exit(0)

        # If --force flag provided, skip confirmation
        if force:
            return True, existing_data, is_same_file

        # Prompt for confirmation
        confirm = typer.confirm("Apply these changes?")
        if not confirm:
            _echo_info("Cancelled.")
            raise typer.Exit(0)

    return True, existing_data, is_same_file


def show_yaml_diff(old_data: dict, new_data: Optional[dict], target_path: Path) -> bool:
    """Show differences between old and new YAML.

    Args:
        old_data: Existing YAML data
        new_data: New YAML data (None if showing prompt)
        target_path: Target file path

    Returns:
        True if changes were found, False otherwise
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

    # Compare schema (for sources)
    old_schema = old_data.get("schema", [])
    new_schema = new_data.get("schema", []) if new_data else []

    if old_schema != new_schema:
        old_cols = {s.get("name"): s.get("data_type") for s in old_schema if isinstance(s, dict)}
        new_cols = {s.get("name"): s.get("data_type") for s in new_schema if isinstance(s, dict)}

        # Removed schema columns
        for col in set(old_cols.keys()) - set(new_cols.keys()):
            _echo_info(f"  - schema: {col} ({old_cols[col]})")
            changes_found = True

        # Added schema columns
        for col in set(new_cols.keys()) - set(old_cols.keys()):
            _echo_info(f"  + schema: {col} ({new_cols[col]})")
            changes_found = True

        # Modified schema columns
        for col in set(old_cols.keys()) & set(new_cols.keys()):
            if old_cols[col] != new_cols[col]:
                _echo_info(f"  ~ schema: {col}: {old_cols[col]} → {new_cols[col]}")
                changes_found = True

    # Compare tags
    old_tags = set(old_data.get("tags", []))
    new_tags = set(new_data.get("tags", [])) if new_data else set()

    if old_tags != new_tags:
        removed_tags = old_tags - new_tags
        added_tags = new_tags - old_tags

        for tag in removed_tags:
            _echo_info(f"  - tag: {tag}")
            changes_found = True

        for tag in added_tags:
            _echo_info(f"  + tag: {tag}")
            changes_found = True

    # Compare params
    old_params = old_data.get("params", {})
    new_params = new_data.get("params", {}) if new_data else {}

    if old_params != new_params:
        all_keys = set(old_params.keys()) | set(new_params.keys())
        for key in all_keys:
            old_val = old_params.get(key)
            new_val = new_params.get(key)
            if old_val != new_val:
                if old_val is not None and new_val is None:
                    _echo_info(f"  - param: {key}={old_val}")
                elif old_val is None and new_val is not None:
                    _echo_info(f"  + param: {key}={new_val}")
                else:
                    _echo_info(f"  ~ param: {key}: {old_val} → {new_val}")
                changes_found = True

    # Compare materialization
    old_mat = old_data.get("materialization", {})
    new_mat = new_data.get("materialization", {}) if new_data else {}

    if old_mat != new_mat:
        if not old_mat and new_mat:
            _echo_info("  + materialization: [added]")
            changes_found = True
        elif old_mat and not new_mat:
            _echo_info("  - materialization: [removed]")
            changes_found = True
        else:
            _echo_info("  ~ materialization: [changed]")
            changes_found = True

    # Compare entity
    old_entity = old_data.get("entity", {})
    new_entity = new_data.get("entity", {}) if new_data else {}

    if old_entity != new_entity:
        _echo_info("  ~ entity: [changed]")
        changes_found = True

    if not changes_found:
        _echo_info("  (no changes detected)")

    return changes_found


def show_python_diff(old_path: Path, new_path: Path) -> bool:
    """Show differences between old and new Python files.

    Args:
        old_path: Path to existing Python file
        new_path: Path to new Python file

    Returns:
        True if changes were found, False otherwise
    """
    _echo_info("")
    _echo_info("Changes:")

    # Read both files
    old_content = old_path.read_text()
    new_content = new_path.read_text()

    # Check if files are identical
    if old_content == new_content:
        _echo_info("  (no changes detected)")
        return False

    changes_found = False

    # Try AST-level comparison for meaningful diffs
    try:
        old_tree = ast.parse(old_content, filename=str(old_path))
        new_tree = ast.parse(new_content, filename=str(new_path))

        # Compare decorators
        old_decorators = extract_decorators_from_tree(old_tree)
        new_decorators = extract_decorators_from_tree(new_tree)

        # Compare decorator metadata
        for key in ["name", "kind", "description"]:
            old_val = old_decorators.get(key, "")
            new_val = new_decorators.get(key, "")

            if old_val != new_val:
                if old_val:
                    _echo_info(f"  - {key}: \"{old_val}\"")
                if new_val:
                    _echo_info(f"  + {key}: \"{new_val}\"")
                changes_found = True

        # Compare PEP 723 metadata
        from seeknal.workflow.dry_run import extract_pep723_metadata
        old_pep = extract_pep723_metadata(old_content)
        new_pep = extract_pep723_metadata(new_content)

        # Compare dependencies
        old_deps = set(old_pep.get("dependencies", []))
        new_deps = set(new_pep.get("dependencies", []))

        if old_deps != new_deps:
            removed = old_deps - new_deps
            added = new_deps - old_deps
            for dep in removed:
                _echo_info(f"  - dependency: {dep}")
                changes_found = True
            for dep in added:
                _echo_info(f"  + dependency: {dep}")
                changes_found = True

        # Compare Python version
        old_py = old_pep.get("requires_python", "")
        new_py = new_pep.get("requires_python", "")
        if old_py != new_py:
            if old_py:
                _echo_info(f"  - requires_python: {old_py}")
            if new_py:
                _echo_info(f"  + requires_python: {new_py}")
            changes_found = True

    except SyntaxError:
        # Fall back to line diff if AST parsing fails
        pass

    # Show line diff for function body changes
    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)

    # Find actual code changes (exclude whitespace-only)
    line_diff = list(difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile=str(old_path),
        tofile=str(new_path),
        lineterm=""
    ))

    # Filter to only show meaningful changes
    meaningful_lines = []
    for line in line_diff:
        # Skip diff headers
        if line.startswith("---") or line.startswith("+++") or line.startswith("@@"):
            continue
        # Skip empty line changes
        if line.strip() in ["", "+", "-"]:
            continue
        meaningful_lines.append(line)

    if meaningful_lines:
        # Show summary of line changes
        added_count = sum(1 for l in meaningful_lines if l.startswith("+") and not l.startswith("+++"))
        removed_count = sum(1 for l in meaningful_lines if l.startswith("-") and not l.startswith("---"))

        if added_count > 0 or removed_count > 0:
            _echo_info(f"  ~ code: {removed_count} removals, {added_count} additions")
            changes_found = True

            # Show first few meaningful changes
            shown = 0
            max_show = 10
            for line in meaningful_lines:
                if shown >= max_show:
                    remaining = len(meaningful_lines) - max_show
                    if remaining > 0:
                        _echo_info(f"    ... and {remaining} more changes")
                    break
                _echo_info(f"    {line}")
                shown += 1

    if not changes_found:
        _echo_info("  (no semantic changes detected)")

    return changes_found


def extract_decorators_from_tree(tree: ast.AST) -> dict:
    """Extract pipeline decorator information from AST tree.

    Args:
        tree: AST tree

    Returns:
        Dictionary with decorator info (name, kind, description)
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Check decorators
            for decorator in node.decorator_list:
                decorator_name = None
                decorator_args = {}

                if isinstance(decorator, ast.Call):
                    if isinstance(decorator.func, ast.Name):
                        decorator_name = decorator.func.id
                        for keyword in decorator.keywords:
                            if isinstance(keyword.value, ast.Constant):
                                decorator_args[keyword.arg] = keyword.value.value
                elif isinstance(decorator, ast.Name):
                    decorator_name = decorator.id

                if decorator_name in ["source", "transform", "feature_group", "second_order_aggregation"]:
                    docstring = ast.get_docstring(node)
                    return {
                        "kind": decorator_name,
                        "name": decorator_args.get("name", node.name),
                        "description": decorator_args.get("description", docstring or ""),
                        "function_name": node.name
                    }

    return {}


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


def apply_common_config(
    draft_path: Path, yaml_data: dict, kind: str, name: str, force: bool, no_parse: bool
) -> None:
    """Merge a common config entry into seeknal/common/<file>.yml.

    Instead of moving the draft file, this reads the target common config file,
    appends the new entry, and writes it back.

    Args:
        draft_path: Path to the draft file
        yaml_data: Parsed YAML data from draft
        kind: The common config kind (e.g., "common-source")
        name: Entry name/id
        force: Overwrite existing entry without prompt
        no_parse: Skip manifest regeneration
    """
    mapping = COMMON_CONFIG_MAPPING[kind]
    target_dir = Path.cwd() / "seeknal" / "common"
    target_file = target_dir / mapping["file"]
    list_key = mapping["list_key"]
    fields = mapping["fields"]

    # Build the entry dict
    entry = {"id": name}
    for field in fields:
        if field in yaml_data:
            entry[field] = yaml_data[field]

    # Read existing file or create empty structure
    existing_data = {}
    if target_file.exists():
        try:
            with open(target_file, "r") as f:
                existing_data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            _echo_error(f"Invalid YAML in {target_file}: {e}")
            raise typer.Exit(1)

    entries = existing_data.get(list_key, [])

    # Check for ID collision
    existing_ids = [e.get("id") for e in entries]
    if name in existing_ids:
        if not force:
            _echo_warning(f"Entry '{name}' already exists in {target_file}")
            confirm = typer.confirm("Overwrite existing entry?")
            if not confirm:
                _echo_info("Cancelled.")
                raise typer.Exit(0)
        # Remove existing entry for replacement
        entries = [e for e in entries if e.get("id") != name]

    # Append entry
    entries.append(entry)
    existing_data[list_key] = entries

    # Write merged YAML
    target_dir.mkdir(parents=True, exist_ok=True)
    with open(target_file, "w") as f:
        yaml.dump(existing_data, f, default_flow_style=False, sort_keys=False)

    # Delete draft file
    draft_path.unlink()

    _echo_success(f"Merged '{name}' into {target_file.relative_to(Path.cwd())}")

    # Update manifest
    if not no_parse:
        _echo_info("Updating manifest...")
        if update_manifest(target_file):
            _echo_success("Manifest regenerated")
        else:
            _echo_warning("Manifest update failed, but entry was merged")


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

    # Route common config kinds to merge logic
    if node_type in COMMON_CONFIG_MAPPING:
        apply_common_config(draft_path, yaml_data, node_type, name, force, no_parse)
        return

    # Get target path
    target_path = get_target_path(node_type, name)

    # Validate target path for security
    is_insecure, secure_alt = warn_if_insecure_path(str(target_path), "apply command")
    if is_insecure:
        _echo_warning(f"Using secure alternative: {secure_alt}")
        target_path = Path(secure_alt)

    # Check for conflicts and get existing data
    try:
        should_proceed, existing_data, is_same_file = check_conflict(
            draft_path, target_path, force, yaml_data
        )
    except typer.Exit:
        raise

    if not should_proceed:
        return

    _echo_success("All checks passed")

    # Move file (skip if source == target)
    if is_same_file:
        _echo_info("File already in correct location")
    else:
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

    # Save applied state snapshot for diff baseline
    try:
        from seeknal.workflow.diff_engine import write_applied_state_entry

        project_path = Path.cwd()
        node_id = f"{node_type}.{name}"
        yaml_content = target_path.read_text(encoding="utf-8")
        rel_path = str(target_path.relative_to(project_path))
        write_applied_state_entry(project_path, node_id, rel_path, yaml_content)
    except Exception:
        pass  # Non-critical: don't fail apply if snapshot fails

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
            columns = yaml_data["columns"]
            if isinstance(columns, dict):
                for col, desc in columns.items():
                    _echo_info(f"    - {col} ({desc})")
            elif isinstance(columns, list):
                for col in columns:
                    _echo_info(f"    - {col}")

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

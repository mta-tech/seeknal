"""
Draft command for Seeknal workflow.

Generates YAML templates from Jinja2 templates for node creation.
"""

import typer
from pathlib import Path
from typing import Optional
import sys
import os

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from jinja2 import Environment, FileSystemLoader, select_autoescape
from seeknal.cli.main import _echo_success, _echo_error, _echo_warning, _echo_info

# Node type mapping
NODE_TYPES = {
    "source": "source",
    "transform": "transform",
    "feature-group": "feature_group",
    "feature_group": "feature_group",
    "model": "model",
    "aggregation": "aggregation",
    "second-order-aggregation": "second_order_aggregation",
    "profile": "profile",
    "rule": "rule",
    "exposure": "exposure",
    "semantic-model": "semantic_model",
    "semantic_model": "semantic_model",
    "metric": "metric",
    "common-source": "common_source",
    "common-rule": "common_rule",
    "common-transformation": "common_transformation",
    "common-transform": "common_transformation",
}

# Template file mapping
TEMPLATE_FILES = {
    "source": "source.yml.j2",
    "transform": "transform.yml.j2",
    "feature_group": "feature_group.yml.j2",
    "model": "model.yml.j2",
    "aggregation": "aggregation.yml.j2",
    "second_order_aggregation": "second_order_aggregation.yml.j2",
    "profile": "profile.yml.j2",
    "rule": "rule.yml.j2",
    "exposure": "exposure.yml.j2",
    "semantic_model": "semantic_model.yml.j2",
    "metric": "metric.yml.j2",
    "common_source": "common_source.yml.j2",
    "common_rule": "common_rule.yml.j2",
    "common_transformation": "common_transformation.yml.j2",
}

# Python template file mapping
PYTHON_TEMPLATE_FILES = {
    "source": "source.py.j2",
    "transform": "transform.py.j2",
    "feature_group": "feature_group.py.j2",
    "model": "model.py.j2",
    "aggregation": "aggregation.py.j2",
    "second_order_aggregation": "second_order_aggregation.py.j2",
    "rule": "rule.py.j2",
    "exposure": "exposure.py.j2",
}


def get_template_env() -> Environment:
    """Get Jinja2 environment with template discovery order.

    Checks:
    1. project/seeknal/templates/*.j2 (project override)
    2. Package templates: src/seeknal/workflow/templates/*.j2 (default)

    Returns:
        Jinja2 Environment configured with template paths
    """
    template_paths = []

    # Check for project templates
    project_root = Path.cwd()
    project_templates = project_root / "seeknal" / "templates"
    if project_templates.exists():
        template_paths.append(str(project_templates))
        _echo_info(f"Using project templates from: {project_templates}")

    # Add package templates (default)
    package_templates = Path(__file__).parent / "templates"
    template_paths.append(str(package_templates))

    return Environment(
        loader=FileSystemLoader(template_paths),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def validate_node_type(node_type: str) -> str:
    """Validate and normalize node type.

    Args:
        node_type: User-provided node type

    Returns:
        Normalized node type key

    Raises:
        typer.Exit: If node type is invalid
    """
    if node_type not in NODE_TYPES:
        valid_types = ", ".join(sorted(set(NODE_TYPES.keys())))
        _echo_error(f"Invalid node type: '{node_type}'")
        _echo_info(f"Valid types: {valid_types}")
        raise typer.Exit(1)
    return NODE_TYPES[node_type]


def validate_name(name: str) -> str:
    """Validate node name.

    Args:
        name: User-provided node name

    Returns:
        Validated name

    Raises:
        typer.Exit: If name is invalid
    """
    if not name:
        _echo_error("Name cannot be empty")
        raise typer.Exit(1)

    # Check for invalid characters
    invalid_chars = [" ", "/", "\\", ".", ".."]
    for char in invalid_chars:
        if char in name:
            _echo_error(f"Name cannot contain '{char}'")
            raise typer.Exit(1)

    # Check length
    if len(name) > 128:
        _echo_error("Name cannot exceed 128 characters")
        raise typer.Exit(1)

    return name


def generate_filename(node_type: str, name: str, python: bool = False) -> str:
    """Generate draft filename.

    Args:
        node_type: Normalized node type
        name: Validated node name
        python: Whether to generate Python file (True) or YAML file (False)

    Returns:
        Draft filename
    """
    ext = "py" if python else "yml"
    return f"draft_{node_type}_{name}.{ext}"


def render_template(template_env: Environment, node_type: str, name: str, description: Optional[str], python: bool = False, deps: Optional[list[str]] = None) -> str:
    """Render Jinja2 template.

    Args:
        template_env: Jinja2 Environment
        node_type: Normalized node type
        name: Node name
        description: Optional description
        python: Whether to generate Python file (True) or YAML file (False)
        deps: Optional list of PEP 723 dependencies for Python files

    Returns:
        Rendered content
    """
    if python:
        template_file = PYTHON_TEMPLATE_FILES.get(node_type)
    else:
        template_file = TEMPLATE_FILES.get(node_type)

    if not template_file:
        raise ValueError(f"No template found for node type: {node_type}")

    template = template_env.get_template(template_file)

    # Template context
    context = {
        "name": name,
        "description": description or f"{node_type.replace('_', ' ')} node",
        "deps": deps or [],
    }

    return template.render(**context)


def write_draft_file(filename: str, content: str, force: bool = False) -> Path:
    """Write draft file to disk.

    Args:
        filename: Draft filename
        content: YAML content
        force: Overwrite existing file

    Returns:
        Path to written file

    Raises:
        typer.Exit: If file exists and force=False
    """
    draft_path = Path.cwd() / filename

    if draft_path.exists() and not force:
        _echo_error(f"Draft file already exists: {filename}")
        _echo_info("Use --force to overwrite")
        raise typer.Exit(1)

    # Write file
    draft_path.write_text(content)
    return draft_path


def draft_command(
    node_type: str = typer.Argument(..., help="Node type (source, transform, feature-group, model, aggregation, second-order-aggregation, rule, exposure)"),
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
    # Validate inputs
    normalized_type = validate_node_type(node_type)
    validated_name = validate_name(name)

    # Parse dependencies if provided
    dep_list = ["seeknal"]  # Always include seeknal as default
    if deps:
        user_deps = [d.strip() for d in deps.split(",") if d.strip()]
        # Add seeknal as default dependency if not present
        if "seeknal" not in user_deps:
            dep_list.extend(user_deps)
        else:
            dep_list = user_deps

    # Generate filename
    filename = generate_filename(normalized_type, validated_name, python)

    # Get template environment
    try:
        template_env = get_template_env()
    except Exception as e:
        _echo_error(f"Failed to load templates: {e}")
        raise typer.Exit(1)

    # Render template
    try:
        content = render_template(template_env, normalized_type, validated_name, description, python, dep_list)
    except Exception as e:
        _echo_error(f"Failed to render template: {e}")
        raise typer.Exit(1)

    # Write draft file
    try:
        draft_path = write_draft_file(filename, content, force)
    except typer.Exit:
        raise
    except Exception as e:
        _echo_error(f"Failed to write draft file: {e}")
        raise typer.Exit(1)

    _echo_success(f"Created: {filename}")

    if python:
        _echo_info(f"Edit the file, then run: seeknal apply {filename}")
    else:
        _echo_info(f"Edit the file, then run: seeknal dry-run {filename}")


if __name__ == "__main__":
    typer.run(draft_command)

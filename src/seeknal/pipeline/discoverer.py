"""
Python pipeline discovery and parsing.

This module provides functionality to discover Python pipeline files,
extract decorated nodes (@source, @transform, @feature_group), and
analyze dependencies via AST parsing.
"""

import ast
import importlib.util
import sys
from pathlib import Path
from typing import Generator


class PythonPipelineDiscoverer:
    """Discover and parse Python pipeline files.

    Scans the seeknal/pipelines/ directory for Python files, imports them
    to extract decorator metadata, and analyzes ctx.ref() dependencies via AST.

    Example:
        discoverer = PythonPipelineDiscoverer(project_path=Path("/my/project"))
        for py_file in discoverer.discover_files():
            nodes = discoverer.parse_file(py_file)
            deps = discoverer.extract_dependencies_ast(py_file)
    """

    def __init__(self, project_path: Path) -> None:
        """Initialize the discoverer.

        Args:
            project_path: Path to the project root directory
        """
        self.project_path = Path(project_path).resolve()
        self.pipelines_dir = self.project_path / "seeknal" / "pipelines"

    def discover_files(self) -> Generator[Path, None, None]:
        """Find all Python files in pipelines directory.

        Yields:
            Paths to Python files (excluding private files starting with _)
        """
        if not self.pipelines_dir.exists():
            return

        for py_file in self.pipelines_dir.glob("*.py"):
            if not py_file.name.startswith("_"):
                yield py_file

    def parse_file(self, file_path: Path) -> list[dict]:
        """Parse a Python file and extract decorated nodes.

        Uses importlib to execute the file in an isolated module context,
        then reads the decorator metadata from the global registry.

        Args:
            file_path: Path to the Python file

        Returns:
            List of node metadata dictionaries

        Raises:
            SyntaxError: If the file has syntax errors
            ImportError: If the file cannot be imported
        """
        from seeknal.pipeline.decorators import get_registered_nodes, clear_registry  # ty: ignore[unresolved-import]

        # Clear registry before importing to avoid cross-file pollution
        clear_registry()

        # Import the module
        module_name = file_path.stem
        spec = importlib.util.spec_from_file_location(module_name, file_path)

        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module from {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
        except SyntaxError as e:
            raise SyntaxError(f"Syntax error in {file_path}:{e.lineno}: {e.msg}")
        except Exception as e:
            raise ImportError(f"Error importing {file_path}: {e}")
        finally:
            # Clean up module from sys.modules
            sys.modules.pop(module_name, None)

        # Collect registered nodes
        nodes = []
        for node_id, node_meta in get_registered_nodes().items():
            # Pick up @materialize decorator list from the original function
            func = node_meta.get("func")
            if func is not None:
                mat_list = getattr(func, "_seeknal_materializations", None)
                if mat_list:
                    node_meta["materializations"] = list(mat_list)
            nodes.append(node_meta)

        # Clear registry again for cleanliness
        clear_registry()

        return nodes

    def extract_dependencies_ast(self, file_path: Path) -> dict[str, list[str]]:
        """Extract ctx.ref() dependencies using AST analysis.

        Parses the Python file's AST to find all ctx.ref() calls,
        which represent dependencies on upstream nodes.

        Args:
            file_path: Path to the Python file

        Returns:
            Dictionary mapping function names to their dependency node IDs

        Example:
            >>> deps = discoverer.extract_dependencies_ast(file_path)
            >>> deps
            {'clean_users': ['source.raw_users'], 'user_features': ['transform.clean_users']}
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read())
        except SyntaxError as e:
            # Return empty dict on syntax errors - will be reported elsewhere
            return {}

        deps: dict[str, list[str]] = {}
        current_func = None

        class RefVisitor(ast.NodeVisitor):
            """AST visitor to extract ctx.ref() calls."""

            def __init__(self):
                self.deps = deps
                self.current_func = None

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                """Visit function definition and track current function."""
                # Skip decorator functions themselves
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name) and decorator.id in ("source", "transform", "feature_group", "second_order_aggregation"):
                        return

                self.current_func = node.name
                self.deps[self.current_func] = []
                self.generic_visit(node)
                self.current_func = None

            def visit_Call(self, node: ast.Call) -> None:
                """Visit function call to find ctx.ref() patterns."""
                # Look for ctx.ref("...") calls
                if (
                    isinstance(node.func, ast.Attribute)
                    and node.func.attr == "ref"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "ctx"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                ):
                    ref_target = node.args[0].value
                    if self.current_func and isinstance(ref_target, str):
                        self.deps[self.current_func].append(ref_target)
                self.generic_visit(node)

        visitor = RefVisitor()
        visitor.visit(tree)
        return deps

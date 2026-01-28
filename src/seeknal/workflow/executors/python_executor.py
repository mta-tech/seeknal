"""
Python executor for running Python pipeline nodes via uv.

This module provides the PythonExecutor class that executes Python pipeline
files using Astral's uv tool with PEP 723 inline script metadata support.
"""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutionContext,
    ExecutorResult,
    ExecutionStatus,
    ExecutorValidationError,
    register_executor,
)
from seeknal.dag.manifest import Node, NodeType


def check_uv_installed() -> bool:
    """Check if uv is available in PATH.

    Returns:
        True if uv command is available, False otherwise
    """
    return shutil.which("uv") is not None


@register_executor(NodeType.PYTHON)
class PythonExecutor(BaseExecutor):
    """
    Execute Python pipeline nodes via uv run.

    This executor handles Python pipeline files (*.py) that use the
    @source, @transform, and @feature_group decorators. It executes
    them using Astral's uv tool, which supports PEP 723 inline
    script metadata for dependency management.

    Key Features:
    - Executes Python files via `uv run`
    - Preserves PEP 723 inline dependencies
    - Provides helpful error message if uv is missing
    - 30-minute subprocess timeout
    - Output truncation for large logs

    Example:
        @transform(name="clean_data")
        def clean_data(ctx):
            df = ctx.ref("source.raw_data")
            return ctx.duckdb.sql("SELECT * FROM df WHERE active").df()
    """

    @property
    def node_type(self) -> NodeType:
        """Return PYTHON node type."""
        return NodeType.PYTHON

    def validate(self) -> None:
        """Validate Python execution requirements.

        Raises:
            ExecutorValidationError: If uv is not installed or file doesn't exist
        """
        if not check_uv_installed():
            raise ExecutorValidationError(
                self.node.id,
                "uv is required for Python pipelines but was not found.\n"
                "Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
            )

        file_path = Path(self.node.file_path)
        if not file_path.exists():
            raise ExecutorValidationError(
                self.node.id,
                f"Pipeline file not found: {file_path}"
            )

    def execute(self) -> ExecutorResult:
        """Execute Python pipeline via uv run.

        Returns:
            ExecutorResult with execution outcome

        Raises:
            ExecutorValidationError: If validation fails
        """
        import time

        file_path = Path(self.node.file_path)
        start_time = time.time()

        # Build execution script that runs the specific function
        runner_script = self._generate_runner_script()
        runner_path = self.context.target_path / f"_runner_{self.node.name}.py"
        runner_path.parent.mkdir(parents=True, exist_ok=True)
        runner_path.write_text(runner_script)

        # Execute via uv run
        cmd = ["uv", "run", str(runner_path)]

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.context.workspace_path),
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minute timeout
                env=self._get_subprocess_env(),
            )

            duration = time.time() - start_time

            if result.returncode != 0:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.FAILED,
                    duration_seconds=duration,
                    metadata={
                        "stdout": result.stdout[-10000:] if len(result.stdout) > 10000 else result.stdout,
                        "stderr": result.stderr[-10000:] if len(result.stderr) > 10000 else result.stderr,
                        "returncode": result.returncode,
                    },
                )

            # Read row count from output
            row_count = 0
            output_path = self.context.target_path / "intermediate" / f"{self.node.id.replace('.', '_')}.parquet"
            if output_path.exists():
                try:
                    import pandas as pd
                    row_count = len(pd.read_parquet(output_path))
                except Exception:
                    pass  # Row count not critical

            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=row_count,
                output_path=output_path if output_path.exists() else None,
                metadata={"stdout": result.stdout[-1000:] if len(result.stdout) > 1000 else result.stdout},
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.FAILED,
                duration_seconds=duration,
                metadata={"error": "Execution timed out after 30 minutes"},
            )

    def _generate_runner_script(self) -> str:
        """Generate wrapper script to execute specific function.

        Returns:
            Python script content as string
        """
        import seeknal
        seeknal_path = Path(seeknal.__file__).parent.parent

        file_path = Path(self.node.file_path)
        func_name = self.node.name

        # Read original file to preserve PEP 723 header, but remove seeknal from deps
        # since we're adding sys.path and seeknal is a local package
        original_content = file_path.read_text()
        pep723_header = self._extract_pep723_header(original_content)
        # Remove seeknal from PEP 723 deps as it's a local package
        pep723_header = pep723_header.replace('#     "seeknal",\n', '')
        pep723_header = pep723_header.replace('#     "seeknal"', '')

        return f'''{pep723_header}
import sys
sys.path.insert(0, "{seeknal_path}")
sys.path.insert(0, "{self.context.workspace_path}")

from pathlib import Path
from seeknal.pipeline.context import PipelineContext

# Import the pipeline module
import importlib.util
spec = importlib.util.spec_from_file_location("pipeline", "{file_path}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# Create execution context
ctx = PipelineContext(
    project_path=Path("{self.context.workspace_path}"),
    target_dir=Path("{self.context.target_path}"),
    config={self.node.config.get("profile_config", {})},
)

try:
    # Execute the function
    func = getattr(module, "{func_name}")
    result = func(ctx)
    print(f"Executed {func_name} successfully")
finally:
    ctx.close()
'''

    def _extract_pep723_header(self, content: str) -> str:
        """Extract PEP 723 inline script metadata.

        Args:
            content: Python file content

        Returns:
            PEP 723 header string or empty string if not found
        """
        lines = content.split("\n")
        header_lines = []
        in_header = False

        for line in lines:
            if line.strip() == "# /// script":
                in_header = True
            if in_header:
                header_lines.append(line)
            if line.strip() == "# ///" and in_header:
                break

        return "\n".join(header_lines) if header_lines else ""

    def _get_subprocess_env(self) -> dict[str, str]:
        """Get environment variables for subprocess.

        Returns:
            Environment variables dictionary
        """
        env = os.environ.copy()
        # Add seeknal-specific env vars
        env["SEEKNAL_PROJECT_PATH"] = str(self.context.workspace_path)
        return env

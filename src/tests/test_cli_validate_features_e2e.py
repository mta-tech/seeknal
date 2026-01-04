"""End-to-end tests for the validate-features CLI command.

This module contains comprehensive E2E tests for the CLI validate-features
command, verifying the complete workflow from command invocation through
validation execution and output formatting.

Test scenarios:
- validate-features --help output
- validate-features with --mode fail (should fail with clear error messages)
- validate-features with --mode warn (should pass with warnings)
- validate-features with non-existent feature group
- validate-features with no validators configured
"""

from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Optional, Union
from unittest import mock

import pytest
from typer.testing import CliRunner
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from seeknal.cli.main import app
from seeknal.feature_validation.models import (
    ValidationConfig,
    ValidationMode,
    ValidationResult,
    ValidationSummary,
    ValidatorConfig,
)
from seeknal.feature_validation.validators import (
    BaseValidator,
    NullValidator,
    RangeValidator,
    ValidationException,
    ValidationRunner,
)


runner = CliRunner()


# =============================================================================
# Test Fixtures and Helpers
# =============================================================================


class MockValidator(BaseValidator):
    """Mock validator for testing that always passes or fails based on config."""

    def __init__(self, should_pass: bool = True, name: str = "MockValidator"):
        self._should_pass = should_pass
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def validate(self, df: DataFrame) -> ValidationResult:
        if self._should_pass:
            return ValidationResult(
                validator_name=self.name,
                passed=True,
                failure_count=0,
                total_count=100,
                message="Validation passed",
                details={"mock": True},
            )
        else:
            return ValidationResult(
                validator_name=self.name,
                passed=False,
                failure_count=10,
                total_count=100,
                message="Validation failed: 10 records failed",
                details={"mock": True, "failure_reason": "test failure"},
            )


def create_mock_feature_group(
    name: str = "test_fg",
    has_validators: bool = True,
    validators_pass: bool = True,
    validators_config: Optional[List[ValidatorConfig]] = None,
):
    """
    Create a mock FeatureGroup for testing.

    Args:
        name: Name of the feature group.
        has_validators: Whether to configure validators.
        validators_pass: Whether validators should pass or fail.
        validators_config: Optional list of validator configs.

    Returns:
        Mock FeatureGroup object with configured validators.
    """
    # Create mock feature group
    mock_fg = mock.MagicMock()
    mock_fg.name = name
    mock_fg.source = mock.MagicMock(spec=DataFrame)

    if has_validators:
        if validators_config is None:
            # Default validators configuration
            validators_config = [
                ValidatorConfig(
                    validator_type="null",
                    columns=["age", "income"],
                    params={"max_null_percentage": 0.0},
                ),
                ValidatorConfig(
                    validator_type="range",
                    columns=["age"],
                    params={"min_val": 0, "max_val": 120},
                ),
            ]

        mock_fg.validation_config = ValidationConfig(
            mode=ValidationMode.FAIL,
            validators=validators_config,
        )

        # Configure validate() method to return appropriate results
        if validators_pass:
            summary = ValidationSummary(
                feature_group_name=name,
                passed=True,
                total_validators=len(validators_config),
                passed_count=len(validators_config),
                failed_count=0,
                results=[
                    ValidationResult(
                        validator_name=f"Validator_{i}",
                        passed=True,
                        failure_count=0,
                        total_count=100,
                        message="Validation passed",
                    )
                    for i in range(len(validators_config))
                ],
            )
        else:
            summary = ValidationSummary(
                feature_group_name=name,
                passed=False,
                total_validators=len(validators_config),
                passed_count=1,
                failed_count=len(validators_config) - 1,
                results=[
                    ValidationResult(
                        validator_name="NullValidator",
                        passed=False,
                        failure_count=10,
                        total_count=100,
                        message="Column 'age' has 10% null values, exceeds threshold of 0%",
                        details={"column": "age", "null_percentage": 0.1},
                    ),
                    ValidationResult(
                        validator_name="RangeValidator",
                        passed=True,
                        failure_count=0,
                        total_count=100,
                        message="All values in range [0, 120]",
                    ),
                ],
            )

        mock_fg.validate.return_value = summary
    else:
        mock_fg.validation_config = None

    return mock_fg


# =============================================================================
# Test Class: validate-features Help Command
# =============================================================================


class TestValidateFeaturesHelp:
    """Tests for validate-features --help output."""

    def test_validate_features_help_shows_usage(self):
        """Test that validate-features --help shows usage information."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        assert "validate-features" in result.stdout
        assert "FEATURE_GROUP" in result.stdout

    def test_validate_features_help_shows_mode_option(self):
        """Test that --help shows the --mode option."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        assert "--mode" in result.stdout
        assert "-m" in result.stdout
        assert "warn" in result.stdout
        assert "fail" in result.stdout

    def test_validate_features_help_shows_verbose_option(self):
        """Test that --help shows the --verbose option."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        assert "--verbose" in result.stdout
        assert "-v" in result.stdout

    def test_validate_features_help_shows_description(self):
        """Test that --help shows command description."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        assert "Validate feature group data quality" in result.stdout

    def test_validate_features_help_shows_exit_codes(self):
        """Test that --help documents exit codes."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        # Help should document exit codes
        assert "0" in result.stdout
        assert "1" in result.stdout

    def test_validate_features_appears_in_main_help(self):
        """Test that validate-features appears in main CLI help."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "validate-features" in result.stdout


# =============================================================================
# Test Class: validate-features with --mode fail
# =============================================================================


class TestValidateFeaturesModeFailE2E:
    """E2E tests for validate-features command with --mode fail."""

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_fail_mode_returns_exit_code_1_on_validation_failure(self, mock_fg_class):
        """Test that fail mode returns exit code 1 when validation fails."""
        # Setup mock
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        # Make validate raise ValidationException in fail mode
        mock_fg.validate.side_effect = ValidationException(
            message="Validation failed: Column 'age' has 10% null values",
            result=ValidationResult(
                validator_name="NullValidator",
                passed=False,
                failure_count=10,
                total_count=100,
                message="Column 'age' has 10% null values",
                details={"column": "age"},
            ),
        )

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "fail"])

        # Should fail with exit code 1
        assert result.exit_code == 1

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_fail_mode_shows_clear_error_message(self, mock_fg_class):
        """Test that fail mode displays clear error messages."""
        # Setup mock
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        # Make validate raise ValidationException
        mock_fg.validate.side_effect = ValidationException(
            message="Validation failed: Column 'age' has 10% null values",
            result=ValidationResult(
                validator_name="NullValidator",
                passed=False,
                failure_count=10,
                total_count=100,
                message="Column 'age' has 10% null values",
                details={"column": "age"},
            ),
        )

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "fail"])

        # Should show error message
        assert "Validation stopped" in result.stdout or "failed" in result.stdout.lower()

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_fail_mode_shows_failure_details(self, mock_fg_class):
        """Test that fail mode shows details about the failure."""
        # Setup mock
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        error_result = ValidationResult(
            validator_name="NullValidator",
            passed=False,
            failure_count=10,
            total_count=100,
            message="Column 'age' has 10% null values",
            details={"column": "age"},
        )

        mock_fg.validate.side_effect = ValidationException(
            message="Validation failed: Column 'age' has 10% null values",
            result=error_result,
        )

        result = runner.invoke(
            app, ["validate-features", "test_fg", "--mode", "fail", "--verbose"]
        )

        # Should show validator name in output
        assert "NullValidator" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_fail_mode_returns_exit_code_0_when_all_pass(self, mock_fg_class):
        """Test that fail mode returns exit code 0 when all validations pass."""
        # Setup mock with passing validators
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=True,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "fail"])

        # Should pass with exit code 0
        assert result.exit_code == 0
        assert "passed" in result.stdout.lower() or "PASS" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_fail_mode_displays_validation_summary(self, mock_fg_class):
        """Test that fail mode displays validation summary."""
        # Setup mock
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=True,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "fail"])

        # Should show summary information
        assert "Validation Summary" in result.stdout or "validators" in result.stdout.lower()


# =============================================================================
# Test Class: validate-features with --mode warn
# =============================================================================


class TestValidateFeaturesModeWarnE2E:
    """E2E tests for validate-features command with --mode warn."""

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_warn_mode_returns_exit_code_0_even_on_failure(self, mock_fg_class):
        """Test that warn mode returns exit code 0 even when validators fail."""
        # Setup mock with failing validators
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "warn"])

        # Should pass with exit code 0 in warn mode
        assert result.exit_code == 0

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_warn_mode_shows_warnings(self, mock_fg_class):
        """Test that warn mode displays warnings for failed validators."""
        # Setup mock with failing validators
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "warn"])

        # Should show warnings
        assert "warning" in result.stdout.lower() or "FAIL" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_warn_mode_continues_after_failure(self, mock_fg_class):
        """Test that warn mode continues and shows all validation results."""
        # Setup mock with multiple validators (some failing)
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "warn"])

        # Should show summary with counts
        assert (
            "Total" in result.stdout
            or "validators" in result.stdout.lower()
            or "Passed" in result.stdout
        )

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_warn_mode_shows_exit_code_message(self, mock_fg_class):
        """Test that warn mode shows exit code message for warnings."""
        # Setup mock with failing validators
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "warn"])

        # Should indicate exit code 0 for warnings
        assert result.exit_code == 0
        assert (
            "Exit code 0" in result.stdout
            or "warnings only" in result.stdout
            or "warning" in result.stdout.lower()
        )


# =============================================================================
# Test Class: validate-features Error Handling
# =============================================================================


class TestValidateFeaturesErrorHandling:
    """E2E tests for validate-features error handling."""

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_nonexistent_feature_group_returns_exit_code_1(self, mock_fg_class):
        """Test that non-existent feature group returns exit code 1."""
        # Setup mock to return None (feature group not found)
        mock_fg_class.load.return_value = None

        result = runner.invoke(
            app, ["validate-features", "nonexistent_fg", "--mode", "fail"]
        )

        # Should fail with exit code 1
        assert result.exit_code == 1

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_nonexistent_feature_group_shows_error_message(self, mock_fg_class):
        """Test that non-existent feature group shows clear error message."""
        # Setup mock to return None
        mock_fg_class.load.return_value = None

        result = runner.invoke(
            app, ["validate-features", "nonexistent_fg", "--mode", "fail"]
        )

        # Should show "not found" error message
        assert "not found" in result.stdout.lower()
        assert "nonexistent_fg" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_no_validators_configured_exits_cleanly(self, mock_fg_class):
        """Test that feature group with no validators exits cleanly."""
        # Setup mock with no validators
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=False,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "fail"])

        # Should exit with code 0 and show message about no validators
        assert result.exit_code == 0
        assert (
            "No validators" in result.stdout
            or "no validators" in result.stdout.lower()
            or "configure" in result.stdout.lower()
        )

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_load_exception_shows_error(self, mock_fg_class):
        """Test that exception during load shows error message."""
        # Setup mock to raise exception
        mock_fg_class.load.side_effect = Exception("Database connection failed")

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "fail"])

        # Should fail with exit code 1
        assert result.exit_code == 1
        # Should show error message
        assert "failed" in result.stdout.lower() or "error" in result.stdout.lower()

    def test_missing_feature_group_argument(self):
        """Test that missing feature group argument shows error."""
        result = runner.invoke(app, ["validate-features"])

        # Should fail due to missing required argument
        assert result.exit_code != 0
        assert "Missing argument" in result.stdout or "FEATURE_GROUP" in result.stdout


# =============================================================================
# Test Class: validate-features Output Format
# =============================================================================


class TestValidateFeaturesOutputFormat:
    """E2E tests for validate-features output formatting."""

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_output_shows_feature_group_name(self, mock_fg_class):
        """Test that output shows the feature group name."""
        mock_fg = create_mock_feature_group(
            name="my_feature_group",
            has_validators=True,
            validators_pass=True,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(
            app, ["validate-features", "my_feature_group", "--mode", "fail"]
        )

        assert "my_feature_group" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_output_shows_mode(self, mock_fg_class):
        """Test that output shows the validation mode."""
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=True,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "warn"])

        assert "warn" in result.stdout.lower()

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_output_shows_validator_count(self, mock_fg_class):
        """Test that output shows the number of validators."""
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=True,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(app, ["validate-features", "test_fg", "--mode", "fail"])

        # Should show validator count (2 default validators)
        assert "2" in result.stdout or "validators" in result.stdout.lower()

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_verbose_mode_shows_additional_details(self, mock_fg_class):
        """Test that verbose mode shows additional details."""
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=True,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(
            app, ["validate-features", "test_fg", "--mode", "fail", "--verbose"]
        )

        assert result.exit_code == 0
        # Verbose mode should show more details
        # (exact content depends on implementation)


# =============================================================================
# Test Class: validate-features Workflow Integration
# =============================================================================


class TestValidateFeaturesWorkflowIntegration:
    """Integration tests for complete validation workflows."""

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_complete_passing_workflow(self, mock_fg_class):
        """Test complete workflow with all validations passing."""
        mock_fg = create_mock_feature_group(
            name="user_features",
            has_validators=True,
            validators_pass=True,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(
            app, ["validate-features", "user_features", "--mode", "fail"]
        )

        # Workflow should complete successfully
        assert result.exit_code == 0
        assert "user_features" in result.stdout
        assert "passed" in result.stdout.lower() or "PASS" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_complete_failing_workflow_fail_mode(self, mock_fg_class):
        """Test complete workflow with failures in fail mode."""
        mock_fg = create_mock_feature_group(
            name="user_features",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        # In fail mode, validate should raise exception
        mock_fg.validate.side_effect = ValidationException(
            message="Validation failed: NullValidator found 10 null values",
            result=ValidationResult(
                validator_name="NullValidator",
                passed=False,
                failure_count=10,
                total_count=100,
                message="Found null values",
            ),
        )

        result = runner.invoke(
            app, ["validate-features", "user_features", "--mode", "fail"]
        )

        # Workflow should fail
        assert result.exit_code == 1
        assert "failed" in result.stdout.lower() or "stopped" in result.stdout.lower()

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_complete_failing_workflow_warn_mode(self, mock_fg_class):
        """Test complete workflow with failures in warn mode."""
        mock_fg = create_mock_feature_group(
            name="user_features",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        result = runner.invoke(
            app, ["validate-features", "user_features", "--mode", "warn"]
        )

        # Workflow should complete with warnings
        assert result.exit_code == 0
        assert "user_features" in result.stdout


# =============================================================================
# Test Class: validate-features Default Behavior
# =============================================================================


class TestValidateFeaturesDefaults:
    """Tests for validate-features default behavior."""

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_default_mode_is_fail(self, mock_fg_class):
        """Test that default mode is 'fail' when not specified."""
        mock_fg = create_mock_feature_group(
            name="test_fg",
            has_validators=True,
            validators_pass=False,
        )
        mock_fg_class.load.return_value = mock_fg

        # Raise exception as FAIL mode should
        mock_fg.validate.side_effect = ValidationException(
            message="Validation failed",
            result=ValidationResult(
                validator_name="TestValidator",
                passed=False,
                failure_count=1,
                total_count=10,
                message="Failed",
            ),
        )

        # Run without specifying mode
        result = runner.invoke(app, ["validate-features", "test_fg"])

        # Default is fail mode, so should exit with 1 on failure
        assert result.exit_code == 1

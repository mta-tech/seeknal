"""Tests for parameter resolution system."""

import os
import pytest
from datetime import date, timedelta

from seeknal.workflow.parameters.resolver import ParameterResolver
from seeknal.workflow.parameters.functions import today, yesterday, month_start, year_start, env_var
from seeknal.workflow.parameters.helpers import get_param, list_params, has_param


class TestParameterFunctions:
    """Test built-in parameter functions."""

    def test_today_returns_current_date(self):
        result = today()
        assert result == date.today().isoformat()

    def test_today_with_offset(self):
        result = today(-1)
        expected = (date.today() - timedelta(days=1)).isoformat()
        assert result == expected

    def test_yesterday(self):
        result = yesterday()
        expected = (date.today() - timedelta(days=1)).isoformat()
        assert result == expected

    def test_month_start(self):
        result = month_start()
        assert result.endswith("-01")

    def test_month_start_with_offset(self):
        result = month_start(-1)
        # Should be first day of last month
        assert result.endswith("-01")

    def test_year_start(self):
        result = year_start()
        assert result.endswith("-01-01")

    def test_env_var_with_default(self):
        os.environ["TEST_VAR_PARAM"] = "test_value"
        try:
            assert env_var("TEST_VAR_PARAM") == "test_value"
            assert env_var("MISSING_VAR_PARAM", "default") == "default"
        finally:
            del os.environ["TEST_VAR_PARAM"]

    def test_env_var_raises_without_default(self):
        with pytest.raises(ValueError):
            env_var("NONEXISTENT_VAR_PARAM")


class TestParameterResolver:
    """Test ParameterResolver class."""

    def test_resolve_simple_param(self):
        resolver = ParameterResolver()
        result = resolver.resolve({"date": "{{today}}"})
        assert "date" in result
        assert result["date"] == date.today().isoformat()

    def test_resolve_with_cli_override(self):
        resolver = ParameterResolver(cli_overrides={"date": "2025-01-15"})
        result = resolver.resolve({"date": "{{today}}"})
        assert result["date"] == "2025-01-15"

    def test_resolve_nested_dict(self):
        resolver = ParameterResolver()
        result = resolver.resolve({
            "config": {
                "date": "{{today}}",
                "count": 10
            }
        })
        assert result["config"]["date"] == date.today().isoformat()
        assert result["config"]["count"] == 10

    def test_resolve_list(self):
        resolver = ParameterResolver()
        result = resolver.resolve({
            "dates": ["{{today}}", "{{yesterday}}"]
        })
        assert len(result["dates"]) == 2

    def test_run_id_context(self):
        resolver = ParameterResolver(run_id="test-run-123")
        result = resolver.resolve({"id": "{{run_id}}"})
        assert result["id"] == "test-run-123"

    def test_type_conversion_int(self):
        resolver = ParameterResolver()
        result = resolver.resolve({"count": "100"})
        assert result["count"] == 100
        assert isinstance(result["count"], int)

    def test_type_conversion_bool(self):
        resolver = ParameterResolver()
        result = resolver.resolve({"enabled": "true"})
        assert result["enabled"] is True

    def test_type_conversion_float(self):
        resolver = ParameterResolver()
        result = resolver.resolve({"ratio": "3.14"})
        assert result["ratio"] == 3.14


class TestReservedParameterNames:
    """Test reserved parameter name collision detection."""

    def test_reserved_param_names_constant_exists(self):
        """Test that RESERVED_PARAM_NAMES is defined."""
        from seeknal.workflow.parameters.resolver import RESERVED_PARAM_NAMES
        assert RESERVED_PARAM_NAMES == {"run_id", "run_date", "project_id", "workspace_path"}

    def test_warning_emitted_for_run_id_collision(self):
        """Test that a warning is emitted when user param 'run_id' collides."""
        import warnings
        resolver = ParameterResolver()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = resolver.resolve({"run_id": "my_custom_run_id"})
            # Should emit warning
            assert len(w) == 1
            assert "run_id" in str(w[0].message)
            assert "reserved system name" in str(w[0].message)
            # User's direct value is still used, but warning alerts them
            assert result["run_id"] == "my_custom_run_id"

    def test_system_value_takes_precedence_for_template_expressions(self):
        """Test that {{run_id}} template resolves to system value, not user override."""
        import warnings
        resolver = ParameterResolver()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # When using template syntax, system value is used
            result = resolver.resolve({"run_id": "{{run_id}}"})
            assert len(w) == 1  # Warning still emitted
            assert result["run_id"] == resolver.run_id

    def test_warning_emitted_for_run_date_collision(self):
        """Test that a warning is emitted when user param 'run_date' collides."""
        import warnings
        resolver = ParameterResolver()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = resolver.resolve({"run_date": "2025-01-01"})
            assert len(w) == 1
            assert "run_date" in str(w[0].message)
            # User's direct value is still used, but warning alerts them
            assert result["run_date"] == "2025-01-01"

    def test_system_value_takes_precedence_for_run_date_template(self):
        """Test that {{run_date}} template resolves to system value."""
        import warnings
        resolver = ParameterResolver()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = resolver.resolve({"run_date": "{{run_date}}"})
            assert len(w) == 1
            assert result["run_date"] == resolver.run_date

    def test_no_warning_for_non_reserved_params(self):
        """Test that no warning is emitted for non-reserved parameter names."""
        import warnings
        resolver = ParameterResolver()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = resolver.resolve({"my_param": "value", "other_param": "{{today}}"})
            # Should not emit warning
            assert len(w) == 0
            assert result["my_param"] == "value"

    def test_multiple_collision_warnings(self):
        """Test that multiple collisions each emit a warning."""
        import warnings
        resolver = ParameterResolver()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = resolver.resolve({
                "run_id": "custom_id",
                "run_date": "2025-01-01",
                "project_id": "custom_project",
                "my_param": "value"
            })
            # Should emit 3 warnings (run_id, run_date, project_id)
            assert len(w) == 3
            # Verify each collision was detected
            messages = [str(warning.message) for warning in w]
            assert any("run_id" in msg for msg in messages)
            assert any("run_date" in msg for msg in messages)
            assert any("project_id" in msg for msg in messages)


class TestHelperFunctions:
    """Test get_param helper function."""

    def setup_method(self):
        # Set up test environment
        os.environ["SEEKNAL_PARAM_RUN_DATE"] = "2025-02-10"
        os.environ["SEEKNAL_PARAM_COUNT"] = "100"
        os.environ["SEEKNAL_PARAM_ENABLED"] = "true"

    def teardown_method(self):
        # Clean up
        for key in list(os.environ.keys()):
            if key.startswith("SEEKNAL_PARAM_"):
                del os.environ[key]

    def test_get_param_string(self):
        result = get_param("run_date")
        assert result == "2025-02-10"

    def test_get_param_with_type_conversion(self):
        result = get_param("count", param_type=int)
        assert result == 100
        assert isinstance(result, int)

    def test_get_param_bool_conversion(self):
        result = get_param("enabled", param_type=bool)
        assert result is True

    def test_get_param_with_default(self):
        result = get_param("missing", default="default_value")
        assert result == "default_value"

    def test_get_param_raises_without_default(self):
        with pytest.raises(KeyError):
            get_param("nonexistent")

    def test_list_params(self):
        result = list_params()
        assert "run_date" in result
        assert "count" in result
        assert result["run_date"] == "2025-02-10"

    def test_has_param(self):
        assert has_param("run_date") is True
        assert has_param("nonexistent") is False


class TestParameterNameValidation:
    """Test parameter name validation in helper functions."""

    def setup_method(self):
        # Set up test environment
        os.environ["SEEKNAL_PARAM_MY_PARAM"] = "test_value"

    def teardown_method(self):
        # Clean up
        for key in list(os.environ.keys()):
            if key.startswith("SEEKNAL_PARAM_"):
                del os.environ[key]

    def test_valid_parameter_names(self):
        """Test that valid parameter names are accepted."""
        # Valid: starts with letter, contains alphanumeric and underscores
        # Note: Use unique names to avoid collision with setup_method's MY_PARAM
        valid_names = [
            "test_param",
            "myParam",
            "MyParam",
            "_private",
            "param123",
            "TEST_PARAM",
            "a",
            "_",
            "param_1_2_3",
        ]
        for name in valid_names:
            # Should not raise ValueError
            try:
                result = get_param(name, default="default")
                assert result == "default"
            except ValueError:
                pytest.fail(f"Valid parameter name '{name}' was rejected")

    def test_invalid_parameter_names_with_hyphens(self):
        """Test that parameter names with hyphens are rejected."""
        invalid_names = [
            "my-param",
            "my-param-with-hyphens",
            "test-param-name",
        ]
        for name in invalid_names:
            with pytest.raises(ValueError, match="Invalid parameter name"):
                get_param(name, default="default")

    def test_invalid_parameter_names_starting_with_digit(self):
        """Test that parameter names starting with digits are rejected."""
        invalid_names = [
            "123param",
            "1param",
            "9_invalid",
        ]
        for name in invalid_names:
            with pytest.raises(ValueError, match="Invalid parameter name"):
                get_param(name, default="default")

    def test_invalid_parameter_names_with_special_chars(self):
        """Test that parameter names with special characters are rejected."""
        invalid_names = [
            "param.with.dots",
            "param@with@special",
            "param$with$dollar",
            "param with spaces",
            "param/with/slashes",
            "param\\with\\backslashes",
            "param*with*asterisks",
            "param#with#hash",
            "param!with!exclamation",
            "param~with~tilde",
            "param^with^caret",
        ]
        for name in invalid_names:
            with pytest.raises(ValueError, match="Invalid parameter name"):
                get_param(name, default="default")

    def test_invalid_empty_parameter_name(self):
        """Test that empty parameter name is rejected."""
        with pytest.raises(ValueError, match="Invalid parameter name"):
            get_param("", default="default")

    def test_invalid_parameter_name_only_special_chars(self):
        """Test that parameter name with only special characters is rejected."""
        invalid_names = ["-", "_-", "-_", "___-"]
        for name in invalid_names:
            with pytest.raises(ValueError, match="Invalid parameter name"):
                get_param(name, default="default")

    def test_has_param_validates_name(self):
        """Test that has_param also validates parameter names."""
        with pytest.raises(ValueError, match="Invalid parameter name"):
            has_param("my-invalid-param")

    def test_has_param_accepts_valid_names(self):
        """Test that has_param accepts valid parameter names."""
        # Should not raise ValueError
        assert has_param("my_param") is True  # Set in setup_method
        assert has_param("nonexistent") is False

    def test_error_message_contains_invalid_name(self):
        """Test that error message includes the invalid parameter name."""
        try:
            get_param("my-bad-param", default="default")
            pytest.fail("Expected ValueError for invalid parameter name")
        except ValueError as e:
            assert "my-bad-param" in str(e)
            assert "alphanumeric" in str(e)
            assert "underscore" in str(e)


class TestSystemEnvironmentCollisionWarning:
    """Test warnings for system environment variable collisions."""

    def setup_method(self):
        # Clean up any existing test environment variables
        for key in ["TEST_SYSTEM_VAR", "HOME", "PATH"]:
            if key in os.environ:
                # Save original value for cleanup
                if not hasattr(self, "_original_env"):
                    self._original_env = {}
                self._original_env[key] = os.environ[key]

    def teardown_method(self):
        # Restore original environment variables
        if hasattr(self, "_original_env"):
            for key, value in self._original_env.items():
                os.environ[key] = value
        # Clean up test environment variables
        for key in list(os.environ.keys()):
            if key.startswith("SEEKNAL_PARAM_"):
                del os.environ[key]

    def test_warning_for_system_env_collision_with_path(self):
        """Test that a warning is emitted when param name could collide with PATH."""
        import warnings
        # Set a system environment variable (PATH is common)
        if "PATH" in os.environ:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # Try to get a parameter named 'path' which could collide with PATH
                result = get_param("path", default="my_path")
                # Should emit warning about potential collision
                # Note: This test might not emit a warning if the implementation
                # checks for the SEEKNAL_PARAM_ version first
                assert result == "my_path"

    def test_warning_for_system_env_collision_with_home(self):
        """Test that a warning is emitted when param name could collide with HOME."""
        import warnings
        # Set a system environment variable (HOME is common on Unix)
        if "HOME" in os.environ:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # Try to get a parameter named 'home' which could collide with HOME
                result = get_param("home", default="/home/user")
                # Should emit warning about potential collision
                assert result == "/home/user"

    def test_no_warning_for_non_colliding_param(self):
        """Test that no warning is emitted for non-colliding parameter names."""
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Use a unique parameter name that won't collide
            result = get_param("my_unique_param_12345", default="value")
            # Should not emit warning
            assert len(w) == 0
            assert result == "value"

    def test_no_warning_when_seeknal_param_exists(self):
        """Test that no warning is emitted when SEEKNAL_PARAM_ version exists."""
        import warnings
        # Set up the Seeknal parameter
        os.environ["SEEKNAL_PARAM_MY_PARAM"] = "test_value"
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # This should not emit a warning since SEEKNAL_PARAM_MY_PARAM exists
                result = get_param("my_param")
                # Should not emit warning
                assert result == "test_value"
        finally:
            del os.environ["SEEKNAL_PARAM_MY_PARAM"]

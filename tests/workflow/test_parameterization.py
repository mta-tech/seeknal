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
        result = get_param("count", type=int)
        assert result == 100
        assert isinstance(result, int)

    def test_get_param_bool_conversion(self):
        result = get_param("enabled", type=bool)
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

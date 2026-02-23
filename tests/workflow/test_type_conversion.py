"""Tests for shared type conversion utilities."""

import pytest

from seeknal.workflow.parameters.type_conversion import convert_to_bool, convert_to_type


class TestConvertToBool:
    """Test convert_to_bool function with various inputs."""

    def test_true_lowercase(self):
        """Test 'true' converts to True."""
        assert convert_to_bool('true') is True

    def test_true_uppercase(self):
        """Test 'TRUE' converts to True."""
        assert convert_to_bool('TRUE') is True

    def test_true_mixed_case(self):
        """Test 'True' converts to True."""
        assert convert_to_bool('True') is True

    def test_true_with_whitespace(self):
        """Test '  true  ' converts to True."""
        assert convert_to_bool('  true  ') is True

    def test_one_string(self):
        """Test '1' converts to True."""
        assert convert_to_bool('1') is True

    def test_yes_lowercase(self):
        """Test 'yes' converts to True."""
        assert convert_to_bool('yes') is True

    def test_yes_uppercase(self):
        """Test 'YES' converts to True."""
        assert convert_to_bool('YES') is True

    def test_on_lowercase(self):
        """Test 'on' converts to True."""
        assert convert_to_bool('on') is True

    def test_on_uppercase(self):
        """Test 'ON' converts to True."""
        assert convert_to_bool('ON') is True

    def test_false_lowercase(self):
        """Test 'false' converts to False."""
        assert convert_to_bool('false') is False

    def test_false_uppercase(self):
        """Test 'FALSE' converts to False."""
        assert convert_to_bool('FALSE') is False

    def test_false_mixed_case(self):
        """Test 'False' converts to False."""
        assert convert_to_bool('False') is False

    def test_false_with_whitespace(self):
        """Test '  false  ' converts to False."""
        assert convert_to_bool('  false  ') is False

    def test_zero_string(self):
        """Test '0' converts to False."""
        assert convert_to_bool('0') is False

    def test_no_lowercase(self):
        """Test 'no' converts to False."""
        assert convert_to_bool('no') is False

    def test_no_uppercase(self):
        """Test 'NO' converts to False."""
        assert convert_to_bool('NO') is False

    def test_off_lowercase(self):
        """Test 'off' converts to False."""
        assert convert_to_bool('off') is False

    def test_off_uppercase(self):
        """Test 'OFF' converts to False."""
        assert convert_to_bool('OFF') is False

    def test_bool_true_passthrough(self):
        """Test True passes through as True."""
        assert convert_to_bool(True) is True

    def test_bool_false_passthrough(self):
        """Test False passes through as False."""
        assert convert_to_bool(False) is False

    def test_integer_one(self):
        """Test integer 1 converts to True."""
        assert convert_to_bool(1) is True

    def test_integer_zero(self):
        """Test integer 0 converts to False."""
        assert convert_to_bool(0) is False

    def test_integer_negative(self):
        """Test negative integer converts to True (Python truthiness)."""
        assert convert_to_bool(-1) is True

    def test_integer_positive(self):
        """Test positive integer > 1 converts to True (Python truthiness)."""
        assert convert_to_bool(42) is True

    def test_empty_string(self):
        """Test empty string converts to False."""
        assert convert_to_bool('') is False

    def test_whitespace_string(self):
        """Test whitespace-only string converts to True (Python truthiness for non-empty strings)."""
        assert convert_to_bool('   ') is True

    def test_random_string(self):
        """Test random string converts to True (Python truthiness for non-empty strings)."""
        assert convert_to_bool('random') is True

    def test_non_empty_string(self):
        """Test non-empty non-boolean string converts to True (Python truthiness)."""
        assert convert_to_bool('hello') is True


class TestConvertToType:
    """Test convert_to_type function with various target types."""

    def test_convert_string_to_bool(self):
        """Test converting 'true' string to bool."""
        assert convert_to_type('true', bool) is True

    def test_convert_string_false_to_bool(self):
        """Test converting 'false' string to bool."""
        assert convert_to_type('false', bool) is False

    def test_convert_string_one_to_bool(self):
        """Test converting '1' string to bool."""
        assert convert_to_type('1', bool) is True

    def test_convert_string_zero_to_bool(self):
        """Test converting '0' string to bool."""
        assert convert_to_type('0', bool) is False

    def test_convert_string_yes_to_bool(self):
        """Test converting 'yes' string to bool."""
        assert convert_to_type('yes', bool) is True

    def test_convert_string_no_to_bool(self):
        """Test converting 'no' string to bool."""
        assert convert_to_type('no', bool) is False

    def test_convert_string_on_to_bool(self):
        """Test converting 'on' string to bool."""
        assert convert_to_type('on', bool) is True

    def test_convert_string_off_to_bool(self):
        """Test converting 'off' string to bool."""
        assert convert_to_type('off', bool) is False

    def test_convert_string_to_int(self):
        """Test converting numeric string to int."""
        assert convert_to_type('100', int) == 100

    def test_convert_string_negative_to_int(self):
        """Test converting negative numeric string to int."""
        assert convert_to_type('-50', int) == -50

    def test_convert_string_to_float(self):
        """Test converting numeric string to float."""
        assert convert_to_type('3.14', float) == 3.14

    def test_convert_string_negative_to_float(self):
        """Test converting negative numeric string to float."""
        assert convert_to_type('-2.5', float) == -2.5

    def test_convert_int_to_string(self):
        """Test converting int to string."""
        assert convert_to_type(123, str) == '123'

    def test_convert_float_to_string(self):
        """Test converting float to string."""
        assert convert_to_type(3.14, str) == '3.14'

    def test_convert_bool_to_string(self):
        """Test converting bool to string."""
        assert convert_to_type(True, str) == 'True'

    def test_convert_preserves_bool_type(self):
        """Test that bool input to bool type is preserved."""
        assert convert_to_type(True, bool) is True
        assert convert_to_type(False, bool) is False

    def test_convert_int_to_int(self):
        """Test that int input to int type is preserved."""
        assert convert_to_type(42, int) == 42

    def test_convert_float_to_float(self):
        """Test that float input to float type is preserved."""
        assert convert_to_type(3.14, float) == 3.14

    def test_convert_string_to_string(self):
        """Test that string input to string type is preserved."""
        assert convert_to_type('hello', str) == 'hello'

    def test_convert_to_custom_type(self):
        """Test conversion to custom type via direct conversion."""
        class CustomType:
            def __init__(self, value):
                self.value = value

        result = convert_to_type('test', CustomType)
        assert isinstance(result, CustomType)
        assert result.value == 'test'


class TestConsistencyBetweenModules:
    """Test that type conversion is consistent between resolver and helpers."""

    def test_boolean_conversion_consistency(self):
        """Test that both modules handle boolean conversion consistently."""
        from seeknal.workflow.parameters.resolver import ParameterResolver
        from seeknal.workflow.parameters.helpers import get_param
        import os

        # Test values that should all convert to True
        true_values = ['true', 'TRUE', '1', 'yes', 'YES', 'on', 'ON']
        for val in true_values:
            # Test resolver
            resolver = ParameterResolver()
            resolver_result = resolver._type_convert(val)
            assert resolver_result is True, f"Resolver failed for '{val}'"

            # Test helpers via environment
            os.environ['SEEKNAL_PARAM_TEST'] = val
            try:
                helper_result = get_param('test', param_type=bool)
                assert helper_result is True, f"Helper failed for '{val}'"
            finally:
                del os.environ['SEEKNAL_PARAM_TEST']

        # Test values that should all convert to False
        false_values = ['false', 'FALSE', '0', 'no', 'NO', 'off', 'OFF']
        for val in false_values:
            # Test resolver
            resolver = ParameterResolver()
            resolver_result = resolver._type_convert(val)
            assert resolver_result is False, f"Resolver failed for '{val}'"

            # Test helpers via environment
            os.environ['SEEKNAL_PARAM_TEST'] = val
            try:
                helper_result = get_param('test', param_type=bool)
                assert helper_result is False, f"Helper failed for '{val}'"
            finally:
                del os.environ['SEEKNAL_PARAM_TEST']

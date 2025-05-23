import pytest
from unittest import mock
import json
from datetime import datetime as dt, time as dt_time, timezone as dt_timezone
import pendulum
import pandas as pd
import typer # For pretty_returns

from seeknal.utils import (
    Later,
    to_snake,
    camel_to_snake, # alias for to_snake
    check_is_dict_same,
    _check_is_dict_same, # alias
    is_notebook,
    pretty_returns,
    get_df_schema,
)

# Fixtures
@pytest.fixture
def mock_pendulum_now(mocker):
    """Fixture to mock pendulum.now() for consistent testing."""
    # Default mock: 2023-10-26 10:00:00 in UTC for simplicity in some tests
    # Tests can override this by re-mocking within the test if specific dates/times are needed.
    mock_now = pendulum.datetime(2023, 10, 26, 10, 0, 0, tz='UTC')
    return mocker.patch('pendulum.now', return_value=mock_now)

@pytest.fixture
def mock_datetime_now(mocker):
    """Fixture to mock datetime.now() for consistent 'today' in Later default init."""
    # This will affect Later's default time if not specified
    mock_now_dt = dt(2023, 10, 26, 10, 30, 0) # Local time assumed by Later's default
    # We need to mock datetime.now in the module where it's called by Later (datetime.datetime.now)
    # Assuming Later directly calls datetime.datetime.now()
    return mocker.patch('datetime.datetime.now', return_value=mock_now_dt)


# --- Tests for Later Class ---
class TestLater:

    # --- Initialization Tests ---
    def test_later_init_defaults(self, mock_datetime_now):
        """Test Later default initialization."""
        # mock_datetime_now will provide a fixed "current" time for default `time`
        # pendulum.now() (if used for default date) should also be mocked if not using mock_datetime_now for date part
        
        # Get the mocked "current" time to compare
        mocked_current_time = mock_datetime_now.return_value.time()
        
        later_instance = Later()
        assert later_instance.when == "today"
        assert later_instance.time == mocked_current_time
        assert later_instance.timezone == pendulum.local_timezone() # Default timezone

    def test_later_init_specific_values(self):
        """Test Later initialization with specific when, time, and timezone."""
        specific_time = dt_time(14, 30, 0)
        specific_tz_str = "America/New_York"
        specific_pendulum_tz = pendulum.timezone(specific_tz_str)

        later_instance = Later(when="1d", time=specific_time, timezone=specific_tz_str)
        assert later_instance.when == "1d"
        assert later_instance.time == specific_time
        assert later_instance.timezone == specific_pendulum_tz

        later_instance_tz_obj = Later(when="2h", time=specific_time, timezone=specific_pendulum_tz)
        assert later_instance_tz_obj.timezone == specific_pendulum_tz

    def test_later_init_time_as_string(self):
        """Test Later initialization with time as a string."""
        later_instance = Later(time="09:15:30")
        assert later_instance.time == dt_time(9, 15, 30)

        with pytest.raises(ValueError):
            Later(time="invalid-time-string")

    # --- get_date_hour() Tests ---
    @pytest.mark.parametrize("later_when, later_time_str, later_tz_str, mock_now_str, expected_datetime_str", [
        # Test with when="today"
        ("today", "15:00:00", "UTC", "2023-10-26 10:00:00 UTC", "2023-10-26 15:00:00 UTC"),
        ("today", "08:30:00", "America/New_York", "2023-10-26 10:00:00 America/New_York", "2023-10-26 08:30:00 America/New_York"),
        # Test with when="<N>d"
        ("1d", "10:00:00", "UTC", "2023-10-26 10:00:00 UTC", "2023-10-27 10:00:00 UTC"),
        ("3d", "22:00:00", "Europe/London", "2023-10-26 10:00:00 Europe/London", "2023-10-29 22:00:00 Europe/London"),
        # Test with when="<N>h"
        ("1h", "10:00:00", "UTC", "2023-10-26 10:00:00 UTC", "2023-10-26 11:00:00 UTC"), # Base time is 10:00, 1h later is 11:00
        ("3h", "14:15:00", "Asia/Tokyo", "2023-10-26 08:00:00 Asia/Tokyo", "2023-10-26 17:15:00 Asia/Tokyo"), # Base time for calc is 14:15, 3h later is 17:15
        # Test with when="<N>m"
        ("10m", "10:00:00", "UTC", "2023-10-26 10:00:00 UTC", "2023-10-26 10:10:00 UTC"),
        ("30m", "16:45:00", "Australia/Sydney", "2023-10-26 10:00:00 Australia/Sydney", "2023-10-26 17:15:00 Australia/Sydney"),
    ])
    def test_later_get_date_hour_string_when(self, mocker, later_when, later_time_str, later_tz_str, mock_now_str, expected_datetime_str):
        """Test Later.get_date_hour() with various string 'when' inputs."""
        mock_now_pendulum = pendulum.parse(mock_now_str)
        mocker.patch('pendulum.now', return_value=mock_now_pendulum)
        # Also mock datetime.now as Later() default time might use it if not specified in constructor
        mocker.patch('datetime.datetime.now', return_value=mock_now_pendulum.in_timezone(pendulum.local_timezone()))


        later_time_obj = dt_time.fromisoformat(later_time_str)
        later_instance = Later(when=later_when, time=later_time_obj, timezone=later_tz_str)
        
        expected_pendulum_dt = pendulum.parse(expected_datetime_str)
        result_pendulum_dt = later_instance.get_date_hour()

        assert result_pendulum_dt == expected_pendulum_dt
        assert result_pendulum_dt.timezone_name == expected_pendulum_dt.timezone_name


    def test_later_get_date_hour_when_is_datetime(self):
        """Test Later.get_date_hour() when 'when' is a datetime object."""
        # Native datetime object (timezone naive)
        naive_dt = dt(2023, 11, 15, 12, 0, 0)
        later_instance_naive = Later(when=naive_dt, time="00:00:00") # time in Later is ignored if when is datetime
        
        # Expected: pendulum object in local timezone
        expected_pendulum_naive = pendulum.instance(naive_dt, tz=pendulum.local_timezone())
        assert later_instance_naive.get_date_hour() == expected_pendulum_naive

        # Native datetime object (timezone aware)
        aware_dt = dt(2023, 11, 15, 12, 0, 0, tzinfo=dt_timezone.utc)
        later_instance_aware = Later(when=aware_dt, time="00:00:00")
        
        # Expected: pendulum object with that specific timezone
        expected_pendulum_aware = pendulum.instance(aware_dt)
        assert later_instance_aware.get_date_hour() == expected_pendulum_aware
        assert later_instance_aware.get_date_hour().timezone_name == "UTC"

        # Pendulum datetime object
        pendulum_dt_input = pendulum.datetime(2024, 1, 1, 10, 0, 0, tz="America/Los_Angeles")
        later_instance_pendulum = Later(when=pendulum_dt_input, time="00:00:00")
        assert later_instance_pendulum.get_date_hour() == pendulum_dt_input


    def test_later_get_date_hour_invalid_when_string(self):
        """Test Later.get_date_hour() with invalid 'when' string formats."""
        later_instance_invalid1 = Later(when="1x", time="10:00:00")
        with pytest.raises(AttributeError, match="'when' must be a datetime or in format"):
            later_instance_invalid1.get_date_hour()

        later_instance_invalid2 = Later(when="invalid", time="10:00:00")
        with pytest.raises(AttributeError, match="'when' must be a datetime or in format"):
            later_instance_invalid2.get_date_hour()

        later_instance_invalid3 = Later(when="d1", time="10:00:00") # Number must come first
        with pytest.raises(AttributeError, match="'when' must be a datetime or in format"):
            later_instance_invalid3.get_date_hour()

    # --- to_utc() Tests ---
    @pytest.mark.parametrize("later_when, later_time_str, later_tz_str, mock_now_str, expected_utc_str_or_dt", [
        # Test with when="today", time in specific TZ, convert to UTC
        ("today", "15:00:00", "America/New_York", "2023-10-26 10:00:00 America/New_York", "2023-10-26 19:00:00+00:00"), # 15:00 EDT (UTC-4) is 19:00 UTC
        # Test with when="1d", time in UTC, should remain same time but marked UTC
        ("1d", "10:00:00", "UTC", "2023-10-26 08:00:00 UTC", "2023-10-27 10:00:00+00:00"),
        # Test with when="2h", time in a non-UTC TZ
        ("2h", "09:00:00", "Europe/Paris", "2023-10-26 10:00:00 Europe/Paris", "2023-10-26 09:00:00+00:00"), # 09:00 Paris (CEST, UTC+2) + 2h = 11:00 CEST -> 09:00 UTC
         # Note: The above Paris example might be tricky. If 'now' is 10:00 Paris, and Later time is 09:00.
         # get_date_hour for "2h" would be: (10:00 Paris).date() + 09:00 time = 26th Oct 09:00 Paris. Then add 2h = 26th Oct 11:00 Paris.
         # 26th Oct 11:00 Paris (UTC+2) is 26th Oct 09:00 UTC. This looks correct.
    ])
    def test_later_to_utc_returns_pendulum_object(self, mocker, later_when, later_time_str, later_tz_str, mock_now_str, expected_utc_str_or_dt):
        """Test Later.to_utc() returns a Pendulum datetime object in UTC."""
        mock_now_pendulum = pendulum.parse(mock_now_str)
        mocker.patch('pendulum.now', return_value=mock_now_pendulum)
        mocker.patch('datetime.datetime.now', return_value=mock_now_pendulum.in_timezone(pendulum.local_timezone()))

        later_time_obj = dt_time.fromisoformat(later_time_str)
        later_instance = Later(when=later_when, time=later_time_obj, timezone=later_tz_str)

        expected_utc_pendulum = pendulum.parse(expected_utc_str_or_dt)
        result_pendulum_utc = later_instance.to_utc(as_string=False)

        assert isinstance(result_pendulum_utc, pendulum.DateTime)
        assert result_pendulum_utc == expected_utc_pendulum
        assert result_pendulum_utc.timezone_name == "UTC"


    def test_later_to_utc_returns_string(self, mocker, mock_pendulum_now): # Use the fixture for a fixed now
        """Test Later.to_utc(as_string=True) returns an ISO 8601 string."""
        # mock_pendulum_now is 2023-10-26 10:00:00 UTC
        # Let's use a local time for Later instance to see conversion
        later_instance = Later(when="today", time="12:00:00", timezone="America/Los_Angeles") # 12:00 PDT (UTC-7)
        # mock_pendulum_now gives date 2023-10-26.
        # So, get_date_hour will be 2023-10-26 12:00:00 in America/Los_Angeles.
        # This is 2023-10-26 19:00:00 UTC.
        
        expected_utc_iso_string = "2023-10-26T19:00:00+00:00" # Pendulum's default to_iso8601_string includes offset
        
        result_string_utc = later_instance.to_utc(as_string=True)
        assert isinstance(result_string_utc, str)
        assert result_string_utc == expected_utc_iso_string

    def test_later_to_utc_with_datetime_when(self):
        """Test Later.to_utc() when 'when' was initialized with a datetime object."""
        # Aware datetime object
        aware_dt = dt(2023, 11, 20, 15, 0, 0, tzinfo=pendulum.timezone("America/New_York")) # 15:00 EST (UTC-5)
        later_instance = Later(when=aware_dt) # time and tz of Later are ignored
        
        expected_utc_dt = pendulum.instance(aware_dt).in_timezone("UTC") # Should be 2023-11-20 20:00:00 UTC
        
        result_utc = later_instance.to_utc(as_string=False)
        assert result_utc == expected_utc_dt
        assert result_utc.timezone_name == "UTC"

        result_utc_str = later_instance.to_utc(as_string=True)
        assert result_utc_str == expected_utc_dt.to_iso8601_string()


    def test_later_to_utc_reraises_exception_from_get_date_hour(self):
        """Test Later.to_utc() re-raises AttributeError if get_date_hour fails."""
        later_instance_invalid = Later(when="invalid_format")
        with pytest.raises(AttributeError, match="'when' must be a datetime or in format"):
            later_instance_invalid.to_utc()

        with pytest.raises(AttributeError, match="'when' must be a datetime or in format"):
            later_instance_invalid.to_utc(as_string=True)


# --- Tests for to_snake (and camel_to_snake) ---
@pytest.mark.parametrize("input_str, expected_output", [
    ("camelCase", "camel_case"),
    ("PascalCase", "pascal_case"),
    ("snake_case", "snake_case"),
    ("already_snake_case", "already_snake_case"),
    ("Word", "word"), # Single capitalized word
    ("word", "word"),   # Single lowercase word
    ("version1Test", "version1_test"),
    ("version2TestAnother", "version2_test_another"),
    ("ALLCAPSWord", "allcaps_word"), # Handle consecutive caps followed by lowercase
    ("WordALLCAPS", "word_allcaps"), # Handle word followed by all caps
    ("HTTPResponseCode", "http_response_code"), # Multiple consecutive caps
    ("HTTPResponseCodeXYZ", "http_response_code_xyz"),
    ("aB", "a_b"), # Short strings
    ("B", "b"),   # Single capital letter
    ("", ""),     # Empty string
    (" ", " "),   # String with only a space (should remain unchanged based on current logic)
    (" leadingSpace", " leading_space"), # Based on regex, leading space might be kept
    ("trailingSpace ", "trailing_space "), # Trailing space
    ("word_With_Underscore", "word_with_underscore"), # Mixed
    ("PartiallyCamelCase_WithUnderscore", "partially_camel_case_with_underscore")
])
def test_to_snake_various_inputs(input_str, expected_output):
    """Test to_snake with a variety of input strings."""
    assert to_snake(input_str) == expected_output
    assert camel_to_snake(input_str) == expected_output # Test alias as well

def test_to_snake_with_none_input():
    """Test to_snake with None input, should raise TypeError."""
    with pytest.raises(TypeError): # Or specific error if defined by function
        to_snake(None)


# --- Tests for check_is_dict_same (and _check_is_dict_same) ---
class TestCheckIsDictSame:
    def test_identical_dictionaries(self):
        """Test with two identical dictionaries."""
        dict1 = {"name": "test", "version": 1, "details": {"status": "active"}}
        dict2 = {"name": "test", "version": 1, "details": {"status": "active"}}
        assert check_is_dict_same(dict1, dict2) is True
        assert _check_is_dict_same(dict1, dict2) is True # Test alias

    def test_different_dictionaries_value_diff(self):
        """Test with dictionaries having different values."""
        dict1 = {"name": "test", "version": 1}
        dict2 = {"name": "test", "version": 2}
        assert check_is_dict_same(dict1, dict2) is False

    def test_different_dictionaries_key_diff(self):
        """Test with dictionaries having different keys."""
        dict1 = {"name": "test", "version": 1}
        dict2 = {"name": "test", "id": "abc"}
        assert check_is_dict_same(dict1, dict2) is False

    def test_different_length_dictionaries(self):
        """Test with dictionaries of different lengths."""
        dict1 = {"name": "test", "version": 1}
        dict2 = {"name": "test"}
        assert check_is_dict_same(dict1, dict2) is False

    def test_same_content_different_key_order(self):
        """Test with dictionaries having same content but different key order."""
        # json.dumps with sort_keys=True handles this
        dict1 = {"version": 1, "name": "test", "details": {"status": "active", "code": 100}}
        dict2 = {"name": "test", "details": {"code": 100, "status": "active"}, "version": 1}
        assert check_is_dict_same(dict1, dict2) is True

    def test_nested_dictionaries_identical(self):
        """Test with identical nested dictionaries."""
        dict1 = {"config": {"url": "http://localhost", "port": 8080}, "user": "admin"}
        dict2 = {"user": "admin", "config": {"port": 8080, "url": "http://localhost"}}
        assert check_is_dict_same(dict1, dict2) is True

    def test_nested_dictionaries_different(self):
        """Test with different nested dictionaries."""
        dict1 = {"config": {"url": "http://localhost", "port": 8080}, "user": "admin"}
        dict2 = {"config": {"url": "http://remotehost", "port": 8000}, "user": "admin"}
        assert check_is_dict_same(dict1, dict2) is False

    def test_empty_dictionaries(self):
        """Test with two empty dictionaries."""
        assert check_is_dict_same({}, {}) is True

    def test_one_empty_one_not(self):
        """Test with one empty and one non-empty dictionary."""
        dict1 = {}
        dict2 = {"key": "value"}
        assert check_is_dict_same(dict1, dict2) is False
        assert check_is_dict_same(dict2, dict1) is False

    def test_dictionaries_with_list_values_identical(self):
        """Test dictionaries with list values that are identical."""
        dict1 = {"items": [1, 2, {"a": 3}], "status": "ok"}
        dict2 = {"status": "ok", "items": [1, 2, {"a": 3}]}
        assert check_is_dict_same(dict1, dict2) is True
    
    def test_dictionaries_with_list_values_different_order_in_list(self):
        """Test dictionaries with list values where list order differs."""
        # json.dumps does not sort lists, so this should be False
        dict1 = {"items": [1, {"a": 3}, 2], "status": "ok"}
        dict2 = {"status": "ok", "items": [1, 2, {"a": 3}]}
        assert check_is_dict_same(dict1, dict2) is False

    def test_dictionaries_with_list_values_different_content_in_list(self):
        """Test dictionaries with list values where list content differs."""
        dict1 = {"items": [1, 2, {"a": 4}], "status": "ok"}
        dict2 = {"status": "ok", "items": [1, 2, {"a": 3}]}
        assert check_is_dict_same(dict1, dict2) is False

    def test_type_mismatch_in_values(self):
        """Test dictionaries where values have different types but might stringify similarly."""
        dict1 = {"key": 1}
        dict2 = {"key": "1"}
        assert check_is_dict_same(dict1, dict2) is False # "1" vs 1

    def test_non_string_keys(self):
        """Test dictionaries with non-string keys (e.g., integers)."""
        # json.dumps converts non-string keys to strings.
        dict1 = {1: "one", "two": 2}
        dict2 = {"1": "one", "two": 2} 
        assert check_is_dict_same(dict1, dict2) is True 

        dict3 = {1: "one", 2: "two"}
        dict4 = {"1": "one", "2": "two"}
        assert check_is_dict_same(dict3, dict4) is True

        dict5 = {1: "one", "2": "two"} # Mixed original key types
        dict6 = {"1": "one", 2: "two"} # Mixed original key types
        assert check_is_dict_same(dict5, dict6) is True


# --- Tests for is_notebook ---
class TestIsNotebook:

    @mock.patch('builtins.__import__') # Mock the import system
    def test_is_notebook_zmq_shell(self, mock_import):
        """Test is_notebook() returns True when in a ZMQInteractiveShell (Jupyter)."""
        # Mock get_ipython and its return value
        mock_ipython = mock.MagicMock()
        
        # Configure the shell object that get_ipython returns
        mock_shell = mock.MagicMock()
        mock_shell.__class__.__name__ = 'ZMQInteractiveShell'
        mock_ipython.return_value.shell = mock_shell

        # This is tricky: is_notebook uses a try-except NameError for get_ipython.
        # A more direct way is to ensure get_ipython is available in the global scope 
        # or mock where it's being called from if it's module-prefixed e.g. some_module.get_ipython
        # Assuming `get_ipython` is called directly (e.g. from IPython.get_ipython)
        # If `is_notebook` does `from IPython import get_ipython`, then `mock.patch('seeknal.utils.get_ipython')`
        # or patch `IPython.get_ipython` globally.
        # For this test, let's assume `get_ipython` is globally available when running in IPython.
        
        with mock.patch('seeknal.utils.get_ipython', mock_ipython, create=True): # create=True if not normally there
            assert is_notebook() is True

    @mock.patch('builtins.__import__')
    def test_is_notebook_terminal_shell(self, mock_import):
        """Test is_notebook() returns False when in a TerminalInteractiveShell (IPython terminal)."""
        mock_ipython = mock.MagicMock()
        mock_shell = mock.MagicMock()
        mock_shell.__class__.__name__ = 'TerminalInteractiveShell'
        mock_ipython.return_value.shell = mock_shell
        
        with mock.patch('seeknal.utils.get_ipython', mock_ipython, create=True):
            assert is_notebook() is False

    @mock.patch('builtins.__import__')
    def test_is_notebook_no_ipython_defined(self, mock_import):
        """Test is_notebook() returns False when get_ipython is not defined (NameError)."""
        # Simulate NameError by making get_ipython raise it when called
        # The function `is_notebook` itself has the try-except NameError.
        # So we need to ensure that `get_ipython` is not found in the scope of `is_notebook`.
        # This is the default behavior if `get_ipython` is not imported or defined.
        # An alternative is to mock `get_ipython` to raise NameError.
        with mock.patch('seeknal.utils.get_ipython', side_effect=NameError("name 'get_ipython' is not defined"), create=True):
             assert is_notebook() is False
    
    @mock.patch('builtins.__import__')
    def test_is_notebook_ipython_present_but_shell_is_none(self, mock_import):
        """Test is_notebook() returns False if get_ipython().shell is None."""
        mock_ipython = mock.MagicMock()
        mock_ipython.return_value.shell = None # Shell is None

        with mock.patch('seeknal.utils.get_ipython', mock_ipython, create=True):
            assert is_notebook() is False
            
    @mock.patch('builtins.__import__')
    def test_is_notebook_ipython_shell_is_not_known_type(self, mock_import):
        """Test is_notebook() returns False if shell is an unknown type."""
        mock_ipython = mock.MagicMock()
        mock_shell = mock.MagicMock()
        mock_shell.__class__.__name__ = 'SomeOtherShell' # Unknown shell type
        mock_ipython.return_value.shell = mock_shell

        with mock.patch('seeknal.utils.get_ipython', mock_ipython, create=True):
            assert is_notebook() is False


# --- Tests for get_df_schema ---
class TestGetDfSchema:

    def test_get_df_schema_valid_input(self):
        """Test get_df_schema with valid 'content' and 'schema'."""
        res_json = {
            "content": [
                '{"id": 1, "name": "Alice", "active": true}',
                '{"id": 2, "name": "Bob", "active": false}'
            ],
            "schema": ["int", "str", "bool"]
        }
        # Assuming column names are derived as "col_0", "col_1", ... if not in schema
        # Or, if schema is expected to be like [{"name": "id", "type": "int"}, ...]
        # The current `get_df_schema` seems to imply schema is just a list of types.
        # Let's assume it expects dicts with "name" and "type" in "schema" based on usage.
        # Re-adjusting test based on the more likely structure of schema from the function's docstring.
        res_json_corrected_schema = {
             "content": [
                '{"id": 1, "name": "Alice", "active": true}',
                '{"id": 2, "name": "Bob", "active": false}'
            ],
            "schema": [ # List of dicts, each dict is a column definition
                {"name": "id", "type": "int"},
                {"name": "name", "type": "str"},
                {"name": "active", "type": "bool"}
            ]
        }

        parsed_content, schema_str = get_df_schema(res_json_corrected_schema)

        expected_parsed_content = [
            {"id": 1, "name": "Alice", "active": True}, # Note: Python bools are True/False
            {"id": 2, "name": "Bob", "active": False}
        ]
        expected_schema_str = "id (int), name (str), active (bool)"

        assert parsed_content == expected_parsed_content
        assert schema_str == expected_schema_str

    def test_get_df_schema_empty_content(self):
        """Test get_df_schema with empty 'content'."""
        res_json = {
            "content": [],
            "schema": [{"name": "id", "type": "int"}]
        }
        parsed_content, schema_str = get_df_schema(res_json)
        assert parsed_content == []
        assert schema_str == "id (int)"

    def test_get_df_schema_missing_content_key(self):
        """Test get_df_schema when 'content' key is missing."""
        res_json = {"schema": [{"name": "id", "type": "int"}]}
        parsed_content, schema_str = get_df_schema(res_json)
        assert parsed_content is None # Or [] depending on implementation, current is None
        assert schema_str == "id (int)"

    def test_get_df_schema_missing_schema_key(self):
        """Test get_df_schema when 'schema' key is missing."""
        res_json = {"content": ['{"id": 1}']}
        parsed_content, schema_str = get_df_schema(res_json)
        expected_parsed_content = [{"id": 1}]
        assert parsed_content == expected_parsed_content
        assert schema_str is None # Or "" depending on implementation, current is None

    def test_get_df_schema_malformed_json_in_content(self):
        """Test get_df_schema with malformed JSON string in 'content'."""
        res_json = {
            "content": ["{'id': 1, 'name': 'Alice'}", "{'id': 2, 'name': 'Bob'}"], # Single quotes = malformed for json.loads
             "schema": [{"name": "id", "type": "int"}, {"name": "name", "type": "str"}]
        }
        # json.loads will raise JSONDecodeError
        with pytest.raises(json.JSONDecodeError):
            get_df_schema(res_json)

    def test_get_df_schema_schema_not_list_of_dicts(self):
        """Test get_df_schema when 'schema' items are not dictionaries (as expected)."""
        res_json = {
            "content": ['{"id": 1}'],
            "schema": ["id_int", "name_str"] # List of strings, not dicts
        }
        # This should cause a TypeError when trying to access item['name'] or item['type']
        with pytest.raises(TypeError, match="'str' object is not subscriptable"): # Or similar error
            get_df_schema(res_json)


# --- Tests for pretty_returns ---
@mock.patch('typer.echo')
@mock.patch('typer.style')
class TestPrettyReturns:

    def test_pretty_returns_basic_json_no_df(self, mock_typer_style, mock_typer_echo):
        """Test pretty_returns with basic JSON, not returning DataFrame."""
        res_json = {
            "message": "Success",
            "data": {"id": 123, "status": "completed"},
            "schema": [{"name": "id", "type": "int"}, {"name": "status", "type": "str"}] # Schema for display
        }
        
        # Mock typer.style to return the text as is for simplicity, or check specific styling calls
        mock_typer_style.side_effect = lambda text, fg=None, bg=None, bold=False, dim=False, underline=False, blink=False, reverse=False, reset=True: str(text)

        returned_value = pretty_returns(res_json)

        assert returned_value == res_json # Should return original JSON if not df

        # Verify typer.echo calls
        # First echo is for the main message (if any, or just the JSON)
        # Second echo is for the schema string
        
        # Exact calls depend on the logic within pretty_returns.
        # Assuming it prints the "message" if present, then the schema.
        # If no "message", it might print the whole JSON.
        # The provided snippet seems to print the schema string, and then optionally a DataFrame.
        # It doesn't explicitly print the whole JSON or a "message" key using typer.echo.
        # The primary output tested here is the schema string.
        
        # Let's assume get_df_schema IS called internally to get the schema_str for printing
        # even if return_as_df is False.
        
        # Expected schema string based on res_json
        expected_schema_str = "id (int), status (str)"
        
        # Check that typer.echo was called with the schema string
        # This requires knowing how schema_str is constructed and printed.
        # The function calls `get_df_schema` so schema_str should be well-formed.
        # It then prints: "Schema: <schema_str>"
        
        # We need to find a call to typer.echo that contains the schema string.
        # This is tricky as styling is applied.
        # Let's check if any call to echo contains "Schema:" and parts of the schema.
        
        found_schema_echo = False
        for call_args in mock_typer_echo.call_args_list:
            args, kwargs = call_args
            if args and isinstance(args[0], str) and "Schema:" in args[0] and expected_schema_str in args[0]:
                found_schema_echo = True
                break
        assert found_schema_echo, f"Schema string '{expected_schema_str}' not found in typer.echo calls."


    @mock.patch('seeknal.utils.get_df_schema') # Mock get_df_schema itself
    def test_pretty_returns_as_df_true(self, mock_get_df_schema_func, mock_typer_style, mock_typer_echo):
        """Test pretty_returns with return_as_df=True."""
        res_json = {"content": ['{"id":1}'], "schema": [{"name":"id", "type":"int"}]}
        
        # Configure mock for get_df_schema
        mock_parsed_content = [{"id": 1}]
        mock_schema_str = "id (int)"
        mock_get_df_schema_func.return_value = (mock_parsed_content, mock_schema_str)

        # Mock typer.style
        mock_typer_style.side_effect = lambda text, **kwargs: str(text)

        result = pretty_returns(res_json, return_as_df=True)

        mock_get_df_schema_func.assert_called_once_with(res_json)
        
        # Verify schema printing
        found_schema_echo = False
        for call_args in mock_typer_echo.call_args_list:
            args, _ = call_args
            if args and isinstance(args[0], str) and "Schema:" in args[0] and mock_schema_str in args[0]:
                found_schema_echo = True
                break
        assert found_schema_echo

        assert "df" in result
        assert isinstance(result["df"], pd.DataFrame)
        assert result["df"]["id"].tolist() == [1]
        assert "schema" in result
        assert result["schema"] == mock_schema_str


    def test_pretty_returns_no_content_for_df(self, mock_typer_style, mock_typer_echo):
        """Test pretty_returns with return_as_df=True but no 'content' for DataFrame."""
        res_json = {"schema": [{"name": "id", "type": "int"}]} # No content
        
        # typer.style mock
        mock_typer_style.side_effect = lambda text, **kwargs: str(text)

        returned_value = pretty_returns(res_json, return_as_df=True)

        # Should print schema
        expected_schema_str = "id (int)"
        found_schema_echo = False
        for call_args in mock_typer_echo.call_args_list:
            args, _ = call_args
            if args and isinstance(args[0], str) and "Schema:" in args[0] and expected_schema_str in args[0]:
                found_schema_echo = True
                break
        assert found_schema_echo

        # Should return the original JSON as 'df' cannot be created
        assert returned_value == res_json
        # Or, it might return a dict with an empty DataFrame or specific message.
        # Based on `df = pd.DataFrame(parsed_content)`, if parsed_content is None or empty,
        # it might create an empty DF or raise error.
        # Current `get_df_schema` returns `None` for content if key missing.
        # `pd.DataFrame(None)` raises ValueError.
        # So, this case should ideally be handled gracefully in pretty_returns.
        # The provided snippet for pretty_returns does:
        # if parsed_content and schema_str and return_as_df:
        # So, if parsed_content is None (from get_df_schema if "content" is missing),
        # it will skip DataFrame creation and return res_json. This is what we assert.


    def test_pretty_returns_res_json_is_none(self, mock_typer_style, mock_typer_echo):
        """Test pretty_returns when res_json is None."""
        returned_value = pretty_returns(None)
        mock_typer_echo.assert_not_called() # Should not print anything
        assert returned_value is None

    def test_pretty_returns_res_json_is_not_dict(self, mock_typer_style, mock_typer_echo):
        """Test pretty_returns when res_json is not a dictionary."""
        res_not_dict = "This is a string, not JSON"
        returned_value = pretty_returns(res_not_dict)
        
        # It should print the string representation of res_not_dict
        mock_typer_echo.assert_called_once_with(str(res_not_dict))
        assert returned_value == res_not_dict


# --- Tests for to_snake ---
# (Will be added later)

# --- Tests for check_is_dict_same ---
# (Will be added later)

# --- Tests for is_notebook ---
# (Will be added later)

# --- Tests for pretty_returns and get_df_schema ---
# (Will be added later)

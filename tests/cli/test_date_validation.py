"""Tests for parse_date_safely function in seeknal.cli.main.

Tests cover:
- Year range validation (2000-2100)
- Future date validation (max 1 year ahead)
- Invalid date format handling
- Valid dates are accepted
- Error messages are user-friendly
"""

from datetime import datetime, timedelta
from unittest.mock import patch
import pytest
import typer

from seeknal.cli.main import parse_date_safely


class TestParseDateSafelyYearValidation:
    """Tests for year range validation (2000-2100)."""

    def test_year_before_2000_is_rejected(self):
        """Year before 2000 should be rejected with user-friendly error."""
        with pytest.raises(typer.Exit) as exc_info:
            parse_date_safely("1999-12-31")

        assert exc_info.value.exit_code == 1

    def test_year_1999_is_rejected(self):
        """Year 1999 should be rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            parse_date_safely("1999-01-01")

        assert exc_info.value.exit_code == 1

    def test_year_1900_is_rejected(self):
        """Year 1900 should be rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            parse_date_safely("1900-06-15")

        assert exc_info.value.exit_code == 1

    def test_year_2000_is_accepted(self):
        """Year 2000 should be accepted (minimum valid year)."""
        result = parse_date_safely("2000-01-01")
        assert result.year == 2000
        assert result.month == 1
        assert result.day == 1

    def test_year_2001_is_accepted(self):
        """Year 2001 should be accepted."""
        result = parse_date_safely("2001-06-15")
        assert result.year == 2001

    def test_current_year_is_accepted(self):
        """Current year should be accepted."""
        current_year = datetime.now().year
        result = parse_date_safely(f"{current_year}-01-01")
        assert result.year == current_year

    def test_year_2100_is_accepted(self):
        """Year 2100 should be accepted (maximum valid year)."""
        result = parse_date_safely("2100-12-31")
        assert result.year == 2100
        assert result.month == 12
        assert result.day == 31

    def test_year_2101_is_rejected(self):
        """Year 2101 should be rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            parse_date_safely("2101-01-01")

        assert exc_info.value.exit_code == 1

    def test_year_2150_is_rejected(self):
        """Year 2150 should be rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            parse_date_safely("2150-06-15")

        assert exc_info.value.exit_code == 1

    def test_year_edge_case_boundary(self):
        """Test exact boundary years: 1999, 2000, 2100, 2101."""
        # 1999 should fail
        with pytest.raises(typer.Exit):
            parse_date_safely("1999-12-31")

        # 2000 should pass
        result = parse_date_safely("2000-01-01")
        assert result.year == 2000

        # 2100 should pass
        result = parse_date_safely("2100-12-31")
        assert result.year == 2100

        # 2101 should fail
        with pytest.raises(typer.Exit):
            parse_date_safely("2101-01-01")


class TestParseDateSafelyFutureDateValidation:
    """Tests for future date validation (max 1 year ahead)."""

    def test_today_is_accepted(self):
        """Today's date should be accepted."""
        today = datetime.now().date()
        result = parse_date_safely(today.isoformat())
        assert result.date() == today

    def test_tomorrow_is_accepted(self):
        """Tomorrow's date should be accepted."""
        tomorrow = datetime.now() + timedelta(days=1)
        result = parse_date_safely(tomorrow.strftime("%Y-%m-%d"))
        assert result.date() == tomorrow.date()

    def test_one_month_ahead_is_accepted(self):
        """Date 1 month ahead should be accepted."""
        future_date = datetime.now() + timedelta(days=30)
        result = parse_date_safely(future_date.strftime("%Y-%m-%d"))
        assert result.date() == future_date.date()

    def test_six_months_ahead_is_accepted(self):
        """Date 6 months ahead should be accepted."""
        future_date = datetime.now() + timedelta(days=180)
        result = parse_date_safely(future_date.strftime("%Y-%m-%d"))
        assert result.date() == future_date.date()

    def test_one_year_ahead_is_accepted(self):
        """Date exactly 1 year ahead should be accepted."""
        future_date = datetime.now() + timedelta(days=365)
        result = parse_date_safely(future_date.strftime("%Y-%m-%d"))
        assert result.date() == future_date.date()

    def test_one_year_and_one_day_ahead_is_rejected(self):
        """Date more than 1 year ahead should be rejected."""
        future_date = datetime.now() + timedelta(days=366)
        with pytest.raises(typer.Exit) as exc_info:
            parse_date_safely(future_date.strftime("%Y-%m-%d"))

        assert exc_info.value.exit_code == 1

    def test_two_years_ahead_is_rejected(self):
        """Date 2 years ahead should be rejected."""
        future_date = datetime.now() + timedelta(days=730)
        with pytest.raises(typer.Exit) as exc_info:
            parse_date_safely(future_date.strftime("%Y-%m-%d"))

        assert exc_info.value.exit_code == 1

    def test_future_date_boundary_exact_365_days(self):
        """Test exact boundary: 365 days ahead should pass."""
        future_date = datetime.now() + timedelta(days=365)
        result = parse_date_safely(future_date.strftime("%Y-%m-%d"))
        assert result.date() == future_date.date()

    def test_future_date_boundary_366_days(self):
        """Test exact boundary: 366 days ahead should fail."""
        future_date = datetime.now() + timedelta(days=366)
        with pytest.raises(typer.Exit):
            parse_date_safely(future_date.strftime("%Y-%m-%d"))


class TestParseDateSafelyInvalidFormats:
    """Tests for invalid date format handling."""

    def test_empty_string_is_rejected(self):
        """Empty string should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("")

    def test_garbage_string_is_rejected(self):
        """Random garbage string should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("not-a-date")

    def test_invalid_month_is_rejected(self):
        """Invalid month number should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024-13-01")

    def test_invalid_day_is_rejected(self):
        """Invalid day number should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024-01-32")

    def test_invalid_day_for_february_is_rejected(self):
        """February 30 should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024-02-30")

    def test_wrong_separator_is_rejected(self):
        """Wrong date separator should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024/01/01")

    def test_missing_day_is_rejected(self):
        """Missing day component should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024-01")

    def test_only_year_is_rejected(self):
        """Only year component should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024")

    def test_reversed_format_is_rejected(self):
        """DD-MM-YYYY format should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("01-01-2024")

    def test_text_month_is_rejected(self):
        """Text month format should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024-Jan-01")

    def test_extra_whitespace_is_rejected(self):
        """Extra whitespace should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely(" 2024-01-01 ")

    def test_partial_timestamp_is_rejected(self):
        """Incomplete ISO timestamp should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2024-01-01T12:00")


class TestParseDateSafelyValidFormats:
    """Tests for valid date formats."""

    def test_standard_date_format(self):
        """Standard YYYY-MM-DD format should be accepted."""
        result = parse_date_safely("2024-06-15")
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 15

    def test_iso_format_with_time(self):
        """ISO format with time component should be accepted."""
        result = parse_date_safely("2024-06-15T14:30:00")
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 0

    def test_iso_format_with_midnight(self):
        """ISO format with midnight time should be accepted."""
        result = parse_date_safely("2024-06-15T00:00:00")
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0

    def test_leap_year_date_is_accepted(self):
        """Leap year date (Feb 29) should be accepted in leap years."""
        result = parse_date_safely("2024-02-29")
        assert result.year == 2024
        assert result.month == 2
        assert result.day == 29

    def test_non_leap_year_feb_29_is_rejected(self):
        """Feb 29 in non-leap year should be rejected."""
        with pytest.raises(typer.Exit):
            parse_date_safely("2023-02-29")

    def test_end_of_month_dates(self):
        """Various end-of-month dates should be accepted."""
        # January 31
        result = parse_date_safely("2024-01-31")
        assert result.day == 31

        # March 31
        result = parse_date_safely("2024-03-31")
        assert result.day == 31

        # April 30
        result = parse_date_safely("2024-04-30")
        assert result.day == 30

    def test_mid_year_dates(self):
        """Various dates throughout the year should be accepted."""
        dates = [
            "2024-01-15",
            "2024-02-15",
            "2024-03-15",
            "2024-04-15",
            "2024-05-15",
            "2024-06-15",
            "2024-07-15",
            "2024-08-15",
            "2024-09-15",
            "2024-10-15",
            "2024-11-15",
            "2024-12-15",
        ]

        for date_str in dates:
            result = parse_date_safely(date_str)
            assert result.day == 15


class TestParseDateSafelyParameterName:
    """Tests for custom parameter name in error messages."""

    def test_custom_param_name_year_error(self):
        """Custom parameter name should appear in year validation errors."""
        with pytest.raises(typer.Exit):
            # Mock the echo function to capture the message
            with patch("seeknal.cli.main._echo_error") as mock_echo:
                try:
                    parse_date_safely("1999-01-01", param_name="start_date")
                except typer.Exit:
                    pass

                # Check that error message includes custom param name
                assert mock_echo.called
                call_args = mock_echo.call_args[0][0]
                assert "start_date" in call_args

    def test_custom_param_name_future_error(self):
        """Custom parameter name should appear in future date errors."""
        with pytest.raises(typer.Exit):
            with patch("seeknal.cli.main._echo_error") as mock_echo:
                try:
                    future_date = datetime.now() + timedelta(days=400)
                    parse_date_safely(future_date.strftime("%Y-%m-%d"), param_name="end_date")
                except typer.Exit:
                    pass

                assert mock_echo.called
                call_args = mock_echo.call_args[0][0]
                assert "end_date" in call_args

    def test_custom_param_name_format_error(self):
        """Custom parameter name should appear in format errors."""
        with pytest.raises(typer.Exit):
            with patch("seeknal.cli.main._echo_error") as mock_echo:
                try:
                    parse_date_safely("invalid-date", param_name="execution_date")
                except typer.Exit:
                    pass

                assert mock_echo.called
                call_args = mock_echo.call_args[0][0]
                assert "execution_date" in call_args

    def test_default_param_name(self):
        """Default parameter name should be 'date'."""
        with pytest.raises(typer.Exit):
            with patch("seeknal.cli.main._echo_error") as mock_echo:
                try:
                    parse_date_safely("1999-01-01")
                except typer.Exit:
                    pass

                assert mock_echo.called
                call_args = mock_echo.call_args[0][0]
                assert "date" in call_args


class TestParseDateSafelyEdgeCases:
    """Tests for edge cases."""

    def test_early_year_2000_dates(self):
        """Test early dates in year 2000."""
        result = parse_date_safely("2000-01-01")
        assert result.year == 2000

        result = parse_date_safely("2000-12-31")
        assert result.year == 2000

    def test_late_year_2100_dates(self):
        """Test late dates in year 2100."""
        result = parse_date_safely("2100-01-01")
        assert result.year == 2100

        result = parse_date_safely("2100-12-31")
        assert result.year == 2100

    def test_first_day_of_month(self):
        """First day of various months should be accepted."""
        for month in range(1, 13):
            date_str = f"2024-{month:02d}-01"
            result = parse_date_safely(date_str)
            assert result.day == 1

    def test_with_timezone_component(self):
        """ISO format with timezone should be handled."""
        # Basic ISO format without timezone is accepted
        result = parse_date_safely("2024-06-15T14:30:00")
        assert result.hour == 14

    def test_padded_vs_unpadded_numbers(self):
        """Both padded and unpadded numbers should work."""
        # Padded (correct) format works
        result = parse_date_safely("2024-06-05")
        assert result.month == 6
        assert result.day == 5

        # fromisoformat requires padding, so single digits fail
        with pytest.raises(typer.Exit):
            parse_date_safely("2024-6-5")

    def test_epoch_boundaries(self):
        """Test dates near year 2000 boundary."""
        # Last day of 1999 (should fail)
        with pytest.raises(typer.Exit):
            parse_date_safely("1999-12-31")

        # First day of 2000 (should pass)
        result = parse_date_safely("2000-01-01")
        assert result.year == 2000

    def test_millennium_boundaries(self):
        """Test dates at millennium boundaries."""
        # Year 2000 boundary
        result = parse_date_safely("2000-01-01")
        assert result.year == 2000

        # Year 2100 boundary
        result = parse_date_safely("2100-12-31")
        assert result.year == 2100


class TestParseDateSafelyReturnValues:
    """Tests for return values."""

    def test_returns_datetime_object(self):
        """Function should return a datetime object."""
        result = parse_date_safely("2024-06-15")
        assert isinstance(result, datetime)

    def test_time_component_is_midnight_for_date_only(self):
        """Date-only format should have time set to midnight."""
        result = parse_date_safely("2024-06-15")
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0

    def test_time_component_is_preserved_for_iso_format(self):
        """ISO format should preserve time component."""
        result = parse_date_safely("2024-06-15T14:30:45")
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 45

    def test_date_only_vs_iso_format_same_date(self):
        """Date-only and ISO format for same date should have same date part."""
        date_only = parse_date_safely("2024-06-15")
        iso_format = parse_date_safely("2024-06-15T00:00:00")

        assert date_only.date() == iso_format.date()


class TestParseDateSafelyRealWorldScenarios:
    """Tests for real-world usage scenarios."""

    def test_data_pipeline_start_date(self):
        """Common data pipeline start date (early 2024) should work."""
        result = parse_date_safely("2024-01-01")
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1

    def test_feature_window_dates(self):
        """Common feature engineering window dates should work."""
        start = parse_date_safely("2024-01-01")
        end = parse_date_safely("2024-12-31")

        assert end > start

    def test_backfill_date_range(self):
        """Typical backfill date range should work."""
        start_date = parse_date_safely("2023-06-01")
        end_date = parse_date_safely("2024-01-31")

        assert end_date > start_date
        assert end_date.year == 2024

    def test_recent_data_dates(self):
        """Recent dates for current data should work."""
        today = datetime.now()
        last_month = today - timedelta(days=30)

        result = parse_date_safely(last_month.strftime("%Y-%m-%d"))
        assert result.year <= today.year

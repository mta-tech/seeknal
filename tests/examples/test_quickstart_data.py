"""
Tests for quickstart sample data CSV validation.

This module validates the sample_data.csv file used in the quickstart examples
to ensure it has the correct schema, data types, and values for the tutorial.
"""

import os
from datetime import datetime

import pandas as pd
import pytest


# Path to sample data relative to project root
SAMPLE_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "examples", "quickstart", "sample_data.csv"
)

# Expected columns and their properties
EXPECTED_COLUMNS = [
    "user_id",
    "event_time",
    "product_category",
    "action_type",
    "device_type",
    "purchase_amount",
    "session_duration",
    "items_viewed",
    "cart_value",
]

# Critical columns that should not have null values
CRITICAL_COLUMNS = ["user_id", "event_time", "action_type", "device_type"]

# Valid categorical values
VALID_ACTION_TYPES = ["view", "add_to_cart", "remove_from_cart", "purchase", "wishlist"]
VALID_DEVICE_TYPES = ["desktop", "mobile", "tablet"]
VALID_PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Beauty",
    "Toys",
    "Books",
    "Food & Beverage",
]


@pytest.fixture(scope="module")
def sample_data():
    """Load sample data CSV for testing."""
    if not os.path.exists(SAMPLE_DATA_PATH):
        pytest.skip(f"Sample data not found at {SAMPLE_DATA_PATH}")
    return pd.read_csv(SAMPLE_DATA_PATH)


class TestCSVExists:
    """Tests for CSV file existence and basic readability."""

    def test_sample_data_file_exists(self):
        """Verify the sample data CSV file exists."""
        assert os.path.exists(SAMPLE_DATA_PATH), (
            f"Sample data file not found at {SAMPLE_DATA_PATH}"
        )

    def test_sample_data_is_readable(self):
        """Verify the sample data CSV can be read without errors."""
        df = pd.read_csv(SAMPLE_DATA_PATH)
        assert df is not None


class TestCSVSchema:
    """Tests for CSV schema validation."""

    def test_has_all_required_columns(self, sample_data):
        """Verify all expected columns are present."""
        missing_columns = set(EXPECTED_COLUMNS) - set(sample_data.columns)
        assert not missing_columns, (
            f"Missing columns: {missing_columns}"
        )

    def test_column_count(self, sample_data):
        """Verify the number of columns matches expected."""
        assert len(sample_data.columns) == len(EXPECTED_COLUMNS), (
            f"Expected {len(EXPECTED_COLUMNS)} columns, got {len(sample_data.columns)}"
        )

    def test_column_order(self, sample_data):
        """Verify columns are in expected order."""
        assert list(sample_data.columns) == EXPECTED_COLUMNS, (
            f"Column order mismatch. Expected: {EXPECTED_COLUMNS}, Got: {list(sample_data.columns)}"
        )


class TestDataTypes:
    """Tests for data type validation."""

    def test_user_id_is_string(self, sample_data):
        """Verify user_id column contains string values."""
        assert sample_data["user_id"].dtype == object, (
            f"user_id should be string type, got {sample_data['user_id'].dtype}"
        )

    def test_event_time_is_parseable(self, sample_data):
        """Verify event_time can be parsed as datetime."""
        try:
            pd.to_datetime(sample_data["event_time"])
        except Exception as e:
            pytest.fail(f"event_time cannot be parsed as datetime: {e}")

    def test_purchase_amount_is_numeric(self, sample_data):
        """Verify purchase_amount contains numeric values."""
        assert pd.api.types.is_numeric_dtype(sample_data["purchase_amount"]), (
            f"purchase_amount should be numeric, got {sample_data['purchase_amount'].dtype}"
        )

    def test_session_duration_is_numeric(self, sample_data):
        """Verify session_duration contains numeric values."""
        assert pd.api.types.is_numeric_dtype(sample_data["session_duration"]), (
            f"session_duration should be numeric, got {sample_data['session_duration'].dtype}"
        )

    def test_items_viewed_is_numeric(self, sample_data):
        """Verify items_viewed contains numeric values."""
        assert pd.api.types.is_numeric_dtype(sample_data["items_viewed"]), (
            f"items_viewed should be numeric, got {sample_data['items_viewed'].dtype}"
        )

    def test_cart_value_is_numeric(self, sample_data):
        """Verify cart_value contains numeric values."""
        assert pd.api.types.is_numeric_dtype(sample_data["cart_value"]), (
            f"cart_value should be numeric, got {sample_data['cart_value'].dtype}"
        )


class TestRowCount:
    """Tests for row count validation."""

    def test_row_count_minimum(self, sample_data):
        """Verify dataset has at least 500 rows."""
        assert len(sample_data) >= 500, (
            f"Expected at least 500 rows, got {len(sample_data)}"
        )

    def test_row_count_maximum(self, sample_data):
        """Verify dataset has at most 1000 rows."""
        assert len(sample_data) <= 1000, (
            f"Expected at most 1000 rows, got {len(sample_data)}"
        )

    def test_row_count_is_reasonable(self, sample_data):
        """Verify dataset has a reasonable number of rows for quickstart."""
        row_count = len(sample_data)
        assert 500 <= row_count <= 1000, (
            f"Row count {row_count} not in expected range [500, 1000]"
        )


class TestNullValues:
    """Tests for null value validation in critical columns."""

    def test_no_nulls_in_user_id(self, sample_data):
        """Verify no null values in user_id column."""
        null_count = sample_data["user_id"].isnull().sum()
        assert null_count == 0, (
            f"Found {null_count} null values in user_id column"
        )

    def test_no_nulls_in_event_time(self, sample_data):
        """Verify no null values in event_time column."""
        null_count = sample_data["event_time"].isnull().sum()
        assert null_count == 0, (
            f"Found {null_count} null values in event_time column"
        )

    def test_no_nulls_in_action_type(self, sample_data):
        """Verify no null values in action_type column."""
        null_count = sample_data["action_type"].isnull().sum()
        assert null_count == 0, (
            f"Found {null_count} null values in action_type column"
        )

    def test_no_nulls_in_device_type(self, sample_data):
        """Verify no null values in device_type column."""
        null_count = sample_data["device_type"].isnull().sum()
        assert null_count == 0, (
            f"Found {null_count} null values in device_type column"
        )

    def test_critical_columns_have_no_nulls(self, sample_data):
        """Verify all critical columns have no null values."""
        for col in CRITICAL_COLUMNS:
            null_count = sample_data[col].isnull().sum()
            assert null_count == 0, (
                f"Found {null_count} null values in critical column '{col}'"
            )


class TestCategoricalValues:
    """Tests for categorical value validation."""

    def test_valid_action_types(self, sample_data):
        """Verify all action_type values are valid."""
        invalid_actions = set(sample_data["action_type"].unique()) - set(VALID_ACTION_TYPES)
        assert not invalid_actions, (
            f"Invalid action types found: {invalid_actions}. Valid: {VALID_ACTION_TYPES}"
        )

    def test_valid_device_types(self, sample_data):
        """Verify all device_type values are valid."""
        invalid_devices = set(sample_data["device_type"].unique()) - set(VALID_DEVICE_TYPES)
        assert not invalid_devices, (
            f"Invalid device types found: {invalid_devices}. Valid: {VALID_DEVICE_TYPES}"
        )

    def test_valid_product_categories(self, sample_data):
        """Verify all product_category values are valid."""
        invalid_categories = set(sample_data["product_category"].unique()) - set(VALID_PRODUCT_CATEGORIES)
        assert not invalid_categories, (
            f"Invalid product categories found: {invalid_categories}. Valid: {VALID_PRODUCT_CATEGORIES}"
        )


class TestNumericRanges:
    """Tests for numeric value range validation."""

    def test_purchase_amount_non_negative(self, sample_data):
        """Verify purchase_amount values are non-negative."""
        negative_count = (sample_data["purchase_amount"] < 0).sum()
        assert negative_count == 0, (
            f"Found {negative_count} negative purchase_amount values"
        )

    def test_session_duration_positive(self, sample_data):
        """Verify session_duration values are positive."""
        non_positive_count = (sample_data["session_duration"] <= 0).sum()
        assert non_positive_count == 0, (
            f"Found {non_positive_count} non-positive session_duration values"
        )

    def test_items_viewed_non_negative(self, sample_data):
        """Verify items_viewed values are non-negative."""
        negative_count = (sample_data["items_viewed"] < 0).sum()
        assert negative_count == 0, (
            f"Found {negative_count} negative items_viewed values"
        )

    def test_cart_value_non_negative(self, sample_data):
        """Verify cart_value values are non-negative."""
        negative_count = (sample_data["cart_value"] < 0).sum()
        assert negative_count == 0, (
            f"Found {negative_count} negative cart_value values"
        )


class TestDataQuality:
    """Tests for overall data quality."""

    def test_unique_user_count(self, sample_data):
        """Verify reasonable number of unique users."""
        unique_users = sample_data["user_id"].nunique()
        assert unique_users >= 10, (
            f"Expected at least 10 unique users, got {unique_users}"
        )

    def test_multiple_events_per_user(self, sample_data):
        """Verify some users have multiple events (for feature aggregation)."""
        events_per_user = sample_data.groupby("user_id").size()
        users_with_multiple = (events_per_user > 1).sum()
        assert users_with_multiple > 0, (
            "No users have multiple events - can't demonstrate aggregation"
        )

    def test_purchase_events_exist(self, sample_data):
        """Verify there are purchase events in the dataset."""
        purchase_count = (sample_data["action_type"] == "purchase").sum()
        assert purchase_count > 0, (
            "No purchase events found in dataset"
        )

    def test_non_purchase_events_have_zero_amount(self, sample_data):
        """Verify non-purchase events have zero purchase_amount."""
        non_purchase = sample_data[sample_data["action_type"] != "purchase"]
        non_zero_amount = (non_purchase["purchase_amount"] != 0).sum()
        assert non_zero_amount == 0, (
            f"Found {non_zero_amount} non-purchase events with non-zero purchase_amount"
        )

    def test_date_range_is_recent(self, sample_data):
        """Verify dates are within a reasonable recent range."""
        dates = pd.to_datetime(sample_data["event_time"])
        min_date = dates.min()
        max_date = dates.max()

        # Dates should be from 2024 or later (recent for tutorial)
        assert min_date.year >= 2024, (
            f"Minimum date {min_date} is too old for quickstart data"
        )

    def test_all_device_types_represented(self, sample_data):
        """Verify all device types are represented in the data."""
        present_devices = set(sample_data["device_type"].unique())
        missing_devices = set(VALID_DEVICE_TYPES) - present_devices
        assert not missing_devices, (
            f"Device types not represented: {missing_devices}"
        )

    def test_multiple_action_types_present(self, sample_data):
        """Verify multiple action types are present."""
        action_count = sample_data["action_type"].nunique()
        assert action_count >= 3, (
            f"Expected at least 3 different action types, got {action_count}"
        )

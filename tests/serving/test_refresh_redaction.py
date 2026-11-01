"""Tests for PlacementRefresher failure log redaction and last_error formatting."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from seeknal.serving.discovery.config import DiscoveryError
from seeknal.serving.discovery.refresh import PlacementRefresher


def test_refresh_failure_redacts_message_and_traceback_from_log_and_last_error(caplog):
    caplog.set_level(logging.DEBUG)

    secret_in_message = "SECRET-XYZ-9999-LEAK"

    def discover_with_secret():
        raise DiscoveryError(f"Database auth failed with password: {secret_in_message}")

    pipeline = MagicMock()
    refresher = PlacementRefresher(
        discover_fn=discover_with_secret,
        pipeline=pipeline,
        interval_seconds=60.0,
    )

    ok = refresher.refresh_once()
    assert ok is False

    # 1. last_error is set to class name only
    assert refresher.last_error == "DiscoveryError"
    assert secret_in_message not in refresher.last_error

    # 2. caplog contains class name only, without error message or traceback
    assert "Catalog refresh failed: DiscoveryError" in caplog.text
    assert secret_in_message not in caplog.text
    assert "Traceback" not in caplog.text


def test_refresh_failure_arbitrary_exception_class_name(caplog):
    caplog.set_level(logging.DEBUG)

    class CustomNetworkFailure(Exception):
        pass

    secret_key = "SECRET-XYZ-KEY-ABCD"

    def failing_discover():
        raise CustomNetworkFailure(f"Connection timed out, auth key: {secret_key}")

    pipeline = MagicMock()
    refresher = PlacementRefresher(
        discover_fn=failing_discover,
        pipeline=pipeline,
        interval_seconds=60.0,
    )

    ok = refresher.refresh_once()
    assert ok is False
    assert refresher.last_error == "CustomNetworkFailure"
    assert secret_key not in refresher.last_error
    assert "Catalog refresh failed: CustomNetworkFailure" in caplog.text
    assert secret_key not in caplog.text

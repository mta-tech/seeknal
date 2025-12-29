"""Tests for path security utilities."""

import logging
import os
import stat
import tempfile
from unittest import mock

import pytest

from seeknal.utils.path_security import (
    INSECURE_PATH_PREFIXES,
    get_secure_default_path,
    get_secure_path_recommendation,
    is_insecure_path,
    warn_if_insecure_path,
    _is_world_writable,
)


class TestIsInsecurePath:
    """Tests for is_insecure_path function."""

    @pytest.mark.parametrize(
        "path",
        [
            "/tmp",
            "/tmp/",
            "/tmp/feature_store",
            "/tmp/my_data/nested/path",
            "/var/tmp",
            "/var/tmp/data",
            "/dev/shm",
            "/dev/shm/cache",
        ],
    )
    def test_insecure_paths_detected(self, path):
        """Known insecure paths should be detected."""
        assert is_insecure_path(path) is True

    @pytest.mark.parametrize(
        "path",
        [
            "/home/user/.seeknal",
            "/home/user/.seeknal/data",
            "~/.seeknal",
            "/opt/myapp/data",
            "/var/lib/myapp",
        ],
    )
    def test_secure_paths_not_flagged(self, path):
        """Secure paths should not be flagged as insecure."""
        # Mock _is_world_writable to return False for these paths
        with mock.patch(
            "seeknal.utils.path_security._is_world_writable", return_value=False
        ):
            assert is_insecure_path(path) is False

    def test_empty_path(self):
        """Empty path should not be flagged."""
        assert is_insecure_path("") is False
        assert is_insecure_path(None) is False

    def test_tmpdir_prefix_not_matched(self):
        """Paths that start with 'tmp' but not '/tmp' should not be flagged."""
        with mock.patch(
            "seeknal.utils.path_security._is_world_writable", return_value=False
        ):
            assert is_insecure_path("/home/user/tmpdir") is False
            assert is_insecure_path("/tmpdata") is False

    def test_path_normalization(self):
        """Paths with redundant separators should be normalized."""
        assert is_insecure_path("/tmp//feature_store") is True
        assert is_insecure_path("/tmp/./data") is True
        assert is_insecure_path("/tmp/../tmp/data") is True


class TestIsWorldWritable:
    """Tests for _is_world_writable helper function."""

    def test_world_writable_directory(self, tmp_path):
        """World-writable directories should be detected."""
        world_writable_dir = tmp_path / "world_writable"
        world_writable_dir.mkdir()
        # Set world-writable permission
        os.chmod(world_writable_dir, 0o777)

        assert _is_world_writable(str(world_writable_dir)) is True

    def test_non_world_writable_directory(self, tmp_path):
        """Non-world-writable directories should not be flagged."""
        secure_dir = tmp_path / "secure"
        secure_dir.mkdir()
        # Set restrictive permissions
        os.chmod(secure_dir, 0o700)

        assert _is_world_writable(str(secure_dir)) is False

    def test_non_existent_path(self):
        """Non-existent paths should check parent directories."""
        # This should not raise an error
        result = _is_world_writable("/nonexistent/path/that/does/not/exist")
        # Result depends on whether any parent is world-writable
        assert isinstance(result, bool)


class TestGetSecureDefaultPath:
    """Tests for get_secure_default_path function."""

    def test_default_path_is_seeknal_dir(self):
        """Default path should be ~/.seeknal when no env vars are set."""
        with mock.patch.dict(
            os.environ,
            {
                "SEEKNAL_BASE_CONFIG_PATH": "",
                "XDG_DATA_HOME": "",
            },
            clear=False,
        ):
            # Remove the env vars completely
            os.environ.pop("SEEKNAL_BASE_CONFIG_PATH", None)
            os.environ.pop("XDG_DATA_HOME", None)

            path = get_secure_default_path()
            expected = os.path.expanduser("~/.seeknal")
            assert path == expected

    def test_seeknal_base_config_path_override(self):
        """SEEKNAL_BASE_CONFIG_PATH should override default."""
        with mock.patch.dict(
            os.environ,
            {"SEEKNAL_BASE_CONFIG_PATH": "/custom/path"},
        ):
            path = get_secure_default_path()
            assert path == "/custom/path"

    def test_xdg_data_home_compliance(self):
        """XDG_DATA_HOME should be used if set."""
        with mock.patch.dict(
            os.environ,
            {"XDG_DATA_HOME": "/home/user/.local/share"},
        ):
            # Remove SEEKNAL_BASE_CONFIG_PATH to test XDG fallback
            os.environ.pop("SEEKNAL_BASE_CONFIG_PATH", None)

            path = get_secure_default_path()
            assert path == "/home/user/.local/share/seeknal"

    def test_subdir_appended(self):
        """Subdirectory should be appended to base path."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SEEKNAL_BASE_CONFIG_PATH", None)
            os.environ.pop("XDG_DATA_HOME", None)

            path = get_secure_default_path("data")
            expected = os.path.join(os.path.expanduser("~/.seeknal"), "data")
            assert path == expected

    def test_nested_subdir(self):
        """Nested subdirectories should work correctly."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SEEKNAL_BASE_CONFIG_PATH", None)
            os.environ.pop("XDG_DATA_HOME", None)

            path = get_secure_default_path("data/features")
            expected = os.path.join(os.path.expanduser("~/.seeknal"), "data/features")
            assert path == expected


class TestGetSecurePathRecommendation:
    """Tests for get_secure_path_recommendation function."""

    def test_tmp_path_recommendation(self):
        """Recommendation for /tmp paths should preserve subdirectory."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SEEKNAL_BASE_CONFIG_PATH", None)
            os.environ.pop("XDG_DATA_HOME", None)

            result = get_secure_path_recommendation("/tmp/feature_store")
            expected = os.path.join(os.path.expanduser("~/.seeknal"), "feature_store")
            assert result == expected

    def test_nested_tmp_path_recommendation(self):
        """Recommendation for nested /tmp paths should preserve full subdirectory."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SEEKNAL_BASE_CONFIG_PATH", None)
            os.environ.pop("XDG_DATA_HOME", None)

            result = get_secure_path_recommendation("/tmp/my/nested/path")
            expected = os.path.join(
                os.path.expanduser("~/.seeknal"), "my/nested/path"
            )
            assert result == expected

    def test_var_tmp_path_recommendation(self):
        """Recommendation for /var/tmp paths should work correctly."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SEEKNAL_BASE_CONFIG_PATH", None)
            os.environ.pop("XDG_DATA_HOME", None)

            result = get_secure_path_recommendation("/var/tmp/data")
            expected = os.path.join(os.path.expanduser("~/.seeknal"), "data")
            assert result == expected


class TestWarnIfInsecurePath:
    """Tests for warn_if_insecure_path function."""

    def test_insecure_path_logs_warning(self, caplog):
        """Insecure paths should trigger a warning log."""
        with caplog.at_level(logging.WARNING):
            is_insecure, alt = warn_if_insecure_path("/tmp/data")

        assert is_insecure is True
        assert alt is not None
        assert "Security Warning" in caplog.text
        assert "/tmp/data" in caplog.text

    def test_secure_path_no_warning(self, caplog):
        """Secure paths should not trigger a warning."""
        with mock.patch(
            "seeknal.utils.path_security.is_insecure_path", return_value=False
        ):
            with caplog.at_level(logging.WARNING):
                is_insecure, alt = warn_if_insecure_path("/home/user/.seeknal/data")

        assert is_insecure is False
        assert alt is None
        assert "Security Warning" not in caplog.text

    def test_context_included_in_warning(self, caplog):
        """Context string should be included in the warning message."""
        with caplog.at_level(logging.WARNING):
            warn_if_insecure_path("/tmp/data", context="offline store")

        assert "for offline store" in caplog.text

    def test_custom_logger(self):
        """Custom logger should be used when provided."""
        mock_logger = mock.Mock(spec=logging.Logger)

        is_insecure, alt = warn_if_insecure_path("/tmp/data", logger=mock_logger)

        assert is_insecure is True
        mock_logger.warning.assert_called_once()
        warning_message = mock_logger.warning.call_args[0][0]
        assert "Security Warning" in warning_message

    def test_empty_path_no_warning(self, caplog):
        """Empty path should not trigger a warning."""
        with caplog.at_level(logging.WARNING):
            is_insecure, alt = warn_if_insecure_path("")

        assert is_insecure is False
        assert alt is None
        assert "Security Warning" not in caplog.text

    def test_returns_secure_alternative(self):
        """Function should return a secure alternative path."""
        is_insecure, alt = warn_if_insecure_path("/tmp/feature_store")

        assert is_insecure is True
        assert alt is not None
        assert "/tmp" not in alt
        assert "feature_store" in alt


class TestInsecurePathPrefixes:
    """Tests for INSECURE_PATH_PREFIXES constant."""

    def test_contains_expected_prefixes(self):
        """INSECURE_PATH_PREFIXES should contain common insecure locations."""
        assert "/tmp" in INSECURE_PATH_PREFIXES
        assert "/var/tmp" in INSECURE_PATH_PREFIXES
        assert "/dev/shm" in INSECURE_PATH_PREFIXES

    def test_is_tuple(self):
        """INSECURE_PATH_PREFIXES should be immutable (tuple)."""
        assert isinstance(INSECURE_PATH_PREFIXES, tuple)

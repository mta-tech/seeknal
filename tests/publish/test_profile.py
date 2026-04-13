from __future__ import annotations

import pytest

from seeknal.publish.profile import PublishProfile


class TestPublishProfile:
    def test_publish_profile_fields(self):
        profile = PublishProfile(server="https://reports.example.com", api_key="secret-key")
        assert profile.server == "https://reports.example.com"
        assert profile.api_key == "secret-key"

    def test_publish_profile_defaults(self):
        profile = PublishProfile(server="https://reports.example.com")
        assert profile.server == "https://reports.example.com"
        assert profile.api_key is None

    def test_publish_profile_server_required(self):
        with pytest.raises(TypeError):
            PublishProfile()  # type: ignore[call-arg]

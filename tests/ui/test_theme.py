"""Tests for seeknal.ui.theme module."""

from seeknal.ui.theme import (
    DARK_THEME,
    LIGHT_THEME,
    ANSI_THEME,
    THEMES,
    STYLE_NAMES,
    get_theme,
)


class TestThemeDefinitions:
    """Verify all themes define the required style names."""

    def test_dark_theme_has_all_styles(self):
        for name in STYLE_NAMES:
            assert name in DARK_THEME.styles, f"DARK_THEME missing style: {name}"

    def test_light_theme_has_all_styles(self):
        for name in STYLE_NAMES:
            assert name in LIGHT_THEME.styles, f"LIGHT_THEME missing style: {name}"

    def test_ansi_theme_has_all_styles(self):
        for name in STYLE_NAMES:
            assert name in ANSI_THEME.styles, f"ANSI_THEME missing style: {name}"

    def test_all_themes_have_identical_style_keys(self):
        dark_keys = set(DARK_THEME.styles.keys())
        light_keys = set(LIGHT_THEME.styles.keys())
        ansi_keys = set(ANSI_THEME.styles.keys())
        # Compare only our custom styles (themes inherit Rich defaults too)
        custom = set(STYLE_NAMES)
        assert custom.issubset(dark_keys)
        assert custom.issubset(light_keys)
        assert custom.issubset(ansi_keys)


class TestThemeRegistry:
    def test_themes_dict_has_expected_keys(self):
        assert "dark" in THEMES
        assert "light" in THEMES
        assert "dark-ansi" in THEMES
        assert "light-ansi" in THEMES

    def test_get_theme_returns_correct_theme(self):
        assert get_theme("dark") is DARK_THEME
        assert get_theme("light") is LIGHT_THEME
        assert get_theme("dark-ansi") is ANSI_THEME

    def test_get_theme_fallback(self):
        assert get_theme("nonexistent") is DARK_THEME
        assert get_theme("") is DARK_THEME

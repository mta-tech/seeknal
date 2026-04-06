"""Tests for MenuState data model."""

import pytest

from seeknal.ui.menu_state import MenuState, OptionItem


class TestOptionItem:
    def test_defaults(self):
        item = OptionItem(label="Test")
        assert item.label == "Test"
        assert item.description == ""
        assert item.preview is None
        assert item.recommended is False


class TestMenuStateFromOptions:
    def test_basic_options(self):
        options = [
            {"label": "A", "description": "Option A"},
            {"label": "B", "description": "Option B"},
        ]
        state = MenuState.from_options("Pick one?", options)

        # 2 options + 1 "Other"
        assert len(state.options) == 3
        assert state.options[0].label == "A"
        assert state.options[1].label == "B"
        assert state.options[2].label == "Other"
        assert state.question == "Pick one?"
        assert state.cursor == 0
        assert state.multi_select is False

    def test_recommended_sets_initial_cursor(self):
        options = [
            {"label": "A", "description": "First"},
            {"label": "B", "description": "Second", "recommended": "true"},
            {"label": "C", "description": "Third"},
        ]
        state = MenuState.from_options("Pick?", options)
        assert state.cursor == 1

    def test_recommended_case_insensitive(self):
        options = [
            {"label": "A"},
            {"label": "B", "recommended": "True"},
        ]
        state = MenuState.from_options("Pick?", options)
        assert state.cursor == 1

    def test_multi_select_flag(self):
        options = [{"label": "A"}, {"label": "B"}]
        state = MenuState.from_options("Pick?", options, multi_select=True)
        assert state.multi_select is True
        assert state.selected == set()

    def test_empty_options(self):
        state = MenuState.from_options("Pick?", [])
        # Only "Other" option
        assert len(state.options) == 1
        assert state.options[0].label == "Other"

    def test_preview_content_preserved(self):
        options = [
            {"label": "Grid", "description": "Grid layout", "preview": "+--+\n|  |\n+--+"},
            {"label": "List", "description": "List layout"},
        ]
        state = MenuState.from_options("Layout?", options)
        assert state.options[0].preview == "+--+\n|  |\n+--+"
        assert state.options[1].preview is None

    def test_missing_keys_use_defaults(self):
        options = [{"label": "A"}]
        state = MenuState.from_options("Pick?", options)
        assert state.options[0].description == ""
        assert state.options[0].preview is None
        assert state.options[0].recommended is False

    def test_other_always_appended_last(self):
        options = [{"label": "X"}, {"label": "Y"}, {"label": "Z"}]
        state = MenuState.from_options("Pick?", options)
        assert state.options[-1].label == "Other"
        assert state.options[-1].description == "Type your own"


class TestMenuStateGetAnswer:
    def test_single_select_returns_focused_label(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}, {"label": "B"}])
        state.cursor = 1
        assert state.get_answer() == "B"

    def test_single_select_other_with_text(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}])
        # Cursor on "Other" (index 1), with text typed
        state.cursor = 1
        state.text_input = "Custom answer"
        assert state.get_answer() == "Custom answer"

    def test_single_select_other_without_text(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}])
        state.cursor = 1
        state.text_input = ""
        assert state.get_answer() == "Other"

    def test_multi_select_comma_separated(self):
        state = MenuState.from_options(
            "Pick?", [{"label": "A"}, {"label": "B"}, {"label": "C"}], multi_select=True
        )
        state.selected = {0, 2}
        assert state.get_answer() == "A, C"

    def test_multi_select_nothing_selected_returns_focused(self):
        state = MenuState.from_options(
            "Pick?", [{"label": "A"}, {"label": "B"}], multi_select=True
        )
        state.cursor = 1
        assert state.get_answer() == "B"

    def test_multi_select_with_other_text(self):
        options = [{"label": "A"}, {"label": "B"}]
        state = MenuState.from_options("Pick?", options, multi_select=True)
        other_idx = 2  # A, B, Other
        state.selected = {0, other_idx}
        state.text_input = "Custom"
        assert state.get_answer() == "A, Custom"

    def test_single_select_other_whitespace_only(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}])
        state.cursor = 1
        state.text_input = "   "
        # Whitespace-only text_input should return "Other" label
        assert state.get_answer() == "Other"


class TestMenuStateHasPreview:
    def test_no_preview(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}, {"label": "B"}])
        assert state.has_preview is False

    def test_with_preview(self):
        state = MenuState.from_options(
            "Pick?", [{"label": "A", "preview": "some content"}, {"label": "B"}]
        )
        assert state.has_preview is True

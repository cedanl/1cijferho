"""Tests for parse_metadata helper functions."""

import pytest

from eencijferho.core.parse_metadata import (
    _is_separator,
    _next_nonempty_index,
    _parse_description_section,
    _is_long_key_continuation,
    _process_key_value_line,
    _process_continuation_line,
    _process_fallback_values,
)


class TestParseMetadataHelpers:
    """Test parse_metadata module helper functions."""

    def test_is_separator_with_dashes(self):
        """Detect separator line with dashes."""
        lines = ["---", "----", "-----"]
        assert _is_separator(lines, 0) is True
        assert _is_separator(lines, 1) is True
        assert _is_separator(lines, 2) is True

    def test_is_separator_with_spaces(self):
        """Detect separator with trailing spaces."""
        lines = ["---  ", "---- ", "----- "]
        assert _is_separator(lines, 0) is True
        assert _is_separator(lines, 1) is True

    def test_is_not_separator_with_text(self):
        """Reject lines with text as separator."""
        lines = ["---text", "- - -", "Separator"]
        assert _is_separator(lines, 0) is False
        assert _is_separator(lines, 1) is False
        assert _is_separator(lines, 2) is False

    def test_is_separator_out_of_bounds(self):
        """Handle out of bounds index."""
        lines = ["text"]
        assert _is_separator(lines, 999) is False

    def test_next_nonempty_index_basic(self):
        """Find next non-empty line."""
        lines = ["text", "", "", "next"]
        assert _next_nonempty_index(lines, 0) == 0
        assert _next_nonempty_index(lines, 1) == 3
        assert _next_nonempty_index(lines, 2) == 3

    def test_next_nonempty_index_all_empty(self):
        """Handle all empty lines."""
        lines = ["", "", ""]
        assert _next_nonempty_index(lines, 0) == 3

    def test_next_nonempty_index_end_of_list(self):
        """Handle end of list."""
        lines = ["text"]
        assert _next_nonempty_index(lines, 1) == 1

    def test_is_long_key_continuation_by_position(self):
        """Detect continuation by = position >= 40."""
        raw_long = "This is a field name with many characters = value"  # = at position 42
        assert _is_long_key_continuation(raw_long, "short") is True

    def test_is_long_key_continuation_by_key_length(self):
        """Detect continuation by key length > 20."""
        raw = "short = value"
        long_key = "a" * 21
        assert _is_long_key_continuation(raw, long_key) is True

    def test_is_long_key_continuation_numeric_key(self):
        """Allow numeric keys even if long."""
        raw = "key = value"
        numeric_key = "123456789"
        assert _is_long_key_continuation(raw, numeric_key) is False

    def test_is_not_continuation(self):
        """Regular key-value pair."""
        raw = "key = value"
        assert _is_long_key_continuation(raw, "key") is False

    def test_parse_description_section_basic(self):
        """Parse basic description section."""
        lines = ["Line 1", "Line 2", "Mogelijke waarden:"]
        i, desc = _parse_description_section(lines, 0)
        assert i == 3
        assert desc == ["Line 1", "Line 2"]

    def test_parse_description_section_with_next_var(self):
        """Stop at next variable header."""
        lines = ["Desc 1", "Desc 2", "", "NextVar", "---"]
        i, desc = _parse_description_section(lines, 0)
        assert "Desc 1" in desc
        assert "Desc 2" in desc

    def test_parse_description_section_empty(self):
        """Handle empty description."""
        lines = ["", "Mogelijke waarden:"]
        i, desc = _parse_description_section(lines, 0)
        assert i == 2

    def test_process_key_value_line_simple_kv(self):
        """Process simple key=value line."""
        values = {}
        last_key = _process_key_value_line("key = value", "key = value", "TestVar", None, values)
        assert last_key == "key"
        assert values["key"] == "value"

    def test_process_key_value_line_special_case_indicatie_geboren(self):
        """Handle special case for 'Indicatie geboren' variable."""
        values = {}
        last_key = _process_key_value_line("99 = something", "99 = something", "Indicatie geboren", None, values)
        assert last_key is None
        assert values["99"] == "Onbekend"

    def test_process_key_value_line_continuation_append(self):
        """Append to existing key in continuation."""
        values = {"key1": "value1"}
        last_key = _process_key_value_line(
            "key1 = continuation",
            "key1 = continuation",
            "TestVar",
            "key1",
            values
        )
        assert last_key == "key1"
        assert "continuation" in values["key1"]

    def test_process_key_value_line_no_match(self):
        """Return last_key unchanged when no key=value pattern."""
        values = {"existing": "value"}
        last_key = _process_key_value_line("no equals here", "no equals here", "TestVar", "existing", values)
        assert last_key == "existing"
        assert values == {"existing": "value"}

    def test_process_continuation_line_with_last_key(self):
        """Append continuation line to last key."""
        values = {"key1": "original"}
        _process_continuation_line("some continuation", "key1", values)
        assert "continuation" in values["key1"]
        assert "original" in values["key1"]

    def test_process_continuation_line_no_last_key(self):
        """Skip continuation line when no last_key."""
        values = {}
        _process_continuation_line("orphan line", None, values)
        assert values == {}

    def test_process_fallback_values_reference(self):
        """Detect reference line and set as reference."""
        values = {}
        values_lines = ["Zie bestand: some_file.csv"]
        _process_fallback_values(values, values_lines)
        assert values == {"reference": "Zie bestand: some_file.csv"}

    def test_process_fallback_values_reference_zie(self):
        """Detect 'Zie' as reference."""
        values = {}
        values_lines = ["Zie Appendix B"]
        _process_fallback_values(values, values_lines)
        assert values == {"reference": "Zie Appendix B"}

    def test_process_fallback_values_list(self):
        """Store multiple lines as list."""
        values = {}
        values_lines = ["Option A", "Option B", "Option C"]
        _process_fallback_values(values, values_lines)
        assert values == {"list": values_lines}

    def test_process_fallback_values_with_existing_values(self):
        """Skip fallback when values already exist."""
        values = {"key": "value"}
        values_lines = ["should", "not", "be", "added"]
        _process_fallback_values(values, values_lines)
        assert values == {"key": "value"}

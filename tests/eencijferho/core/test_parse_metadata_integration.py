"""Integration tests for parse_metadata_file function."""

import pytest
import tempfile
import os

from eencijferho.core.parse_metadata import parse_metadata_file


class TestParseMetadataIntegration:
    """Integration tests for parse_metadata_file with realistic metadata."""

    def test_parse_simple_variable(self):
        """Parse a simple variable with description and values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Geslacht
---
Geslacht van de persoon.

Mogelijke waarden:
1 = Man
2 = Vrouw
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            assert result[0]['name'] == 'Geslacht'
            assert 'persoon' in result[0]['description']
            assert result[0]['values']['1'] == 'Man'
            assert result[0]['values']['2'] == 'Vrouw'

    def test_parse_multiple_variables(self):
        """Parse multiple variables from one file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Geslacht
---
Gender information.

Mogelijke waarden:
1 = Man
2 = Vrouw

Leeftijd
---
Age of person.

Mogelijke waarden:
18-65 = Working age
65+ = Retired
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 2
            assert result[0]['name'] == 'Geslacht'
            assert result[1]['name'] == 'Leeftijd'
            assert '1' in result[0]['values']
            assert '18-65' in result[1]['values']

    def test_parse_with_reference(self):
        """Parse variable with 'Zie' reference instead of values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""SpecialCode
---
Code reference in separate file.

Mogelijke waarden:
Zie bestand: codes.csv
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            assert 'reference' in result[0]['values']
            assert 'codes.csv' in result[0]['values']['reference']

    def test_parse_with_value_list(self):
        """Parse variable with list of values instead of key=value pairs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Province
---
Dutch province name.

Mogelijke waarden:
Noord-Holland
Zuid-Holland
Friesland
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            assert 'list' in result[0]['values']
            assert 'Noord-Holland' in result[0]['values']['list']

    def test_parse_with_notes(self):
        """Parse variable with note lines starting with *."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Status
---
Student status.

Mogelijke waarden:
1 = Active
2 = Inactive
* Note: This field was updated in 2023
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            assert 'Note' in result[0]['description']
            assert 'updated in 2023' in result[0]['description']

    def test_parse_with_long_key_continuation(self):
        """Parse values with long keys that span multiple lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Explanation
---
Some explanation field.

Mogelijke waarden:
Very Long Explanation Key Name = This value continues
                                   on the next line with more text
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            # Long key should trigger continuation handling
            assert any('continues' in v or 'next line' in v
                      for v in result[0]['values'].values())

    def test_parse_empty_file(self):
        """Parse empty metadata file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("")
            result = parse_metadata_file(metadata_file)
            assert result == []

    def test_parse_no_duplicate_variables(self):
        """Ensure duplicate variable names are not added twice."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Geslacht
---
First occurrence.

Mogelijke waarden:
1 = M

Geslacht
---
Second occurrence (should be ignored).

Mogelijke waarden:
2 = F
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            assert result[0]['name'] == 'Geslacht'

    def test_parse_with_empty_lines_in_description(self):
        """Handle empty lines within description section."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Field
---
Line 1 of description.

Line 2 after blank line.

Mogelijke waarden:
A = Value A
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            assert 'Line 1' in result[0]['description']
            assert 'Line 2' in result[0]['description']

    def test_parse_special_case_indicatie_geboren(self):
        """Test special handling for 'Indicatie geboren' variable with code 99."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Indicatie geboren
---
Birth date indicator.

Mogelijke waarden:
1 = Known
99 = Unknown code
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            assert result[0]['name'] == 'Indicatie geboren'
            assert result[0]['values']['99'] == 'Onbekend'

    def test_parse_variable_without_mogelijke_waarden(self):
        """Skip variable when there's no 'Mogelijke waarden:' section."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""VarWithoutValues
---
This variable has no values section.

Some other text here.

Geslacht
---
Gender.

Mogelijke waarden:
1 = Male
2 = Female
""")
            result = parse_metadata_file(metadata_file)
            # VarWithoutValues should be skipped since no "Mogelijke waarden:"
            assert len(result) == 1
            assert result[0]['name'] == 'Geslacht'

    def test_parse_long_key_continuation_in_values(self):
        """Test continuation of long key-value pairs across lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Description
---
Field description.

Mogelijke waarden:
Very Long Key Name That Exceeds 40 Characters = First part of value
continuation of the value on next line
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            # The long key should trigger continuation handling
            values_dict = result[0]['values']
            # One of the values should have continuation
            assert any('continuation' in str(v).lower() for v in values_dict.values())

    def test_parse_invalid_header_not_followed_by_separator(self):
        """Ignore line that looks like header but isn't followed by separator."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""This Looks Like A Header
But it's not because there's no separator below it.

ActualVar
---
Real variable.

Mogelijke waarden:
1 = Value
""")
            result = parse_metadata_file(metadata_file)
            # Only ActualVar should be found
            assert len(result) == 1
            assert result[0]['name'] == 'ActualVar'

    def test_parse_with_multiple_notes(self):
        """Parse variable with multiple note lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Status
---
Status field.

Mogelijke waarden:
1 = Active
* First note
* Second note
* Third note
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            # All notes should be in description
            desc = result[0]['description']
            assert 'First note' in desc
            assert 'Second note' in desc
            assert 'Third note' in desc

    def test_parse_mixed_key_value_and_continuation(self):
        """Parse mixture of key=value pairs and continuation lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                f.write("""Code
---
Code field.

Mogelijke waarden:
1 = First value
with continuation
2 = Second value
with multiple
continuation lines
3 = Third value
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            values = result[0]['values']
            assert '1' in values
            assert '2' in values
            assert '3' in values
            # Check that continuation worked
            assert 'continuation' in values['1']
            assert 'multiple' in values['2']

    def test_parse_long_key_continuation_exact_equals_position(self):
        """Test long key continuation when equals sign is at exact position 40+."""
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_file = os.path.join(tmpdir, "test.txt")
            with open(metadata_file, 'w', encoding='latin-1') as f:
                # When a key=value line has equals at position 40+, it's treated as a continuation
                # of the previous key (not as a new key). This tests that behavior.
                f.write("""VeryLongFieldDescription
---
Field with very long keys.

Mogelijke waarden:
VeryShortKey = First value
Extremely Long Key Name That Is More Than Forty Characters = continuation appended to VeryShortKey
Another Short Key = separate value
""")
            result = parse_metadata_file(metadata_file)
            assert len(result) == 1
            values = result[0]['values']
            # VeryShortKey should have the continuation appended to it
            assert 'VeryShortKey' in values
            # The value should contain both original and appended text
            veryshortkey_value = values['VeryShortKey']
            assert 'First value' in veryshortkey_value
            assert 'continuation' in veryshortkey_value.lower()
            # Another Short Key should be separate
            assert 'Another Short Key' in values
            assert values['Another Short Key'] == 'separate value'

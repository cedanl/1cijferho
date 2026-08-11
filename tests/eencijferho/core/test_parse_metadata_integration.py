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

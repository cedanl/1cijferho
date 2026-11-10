"""Integration tests for parse_metadata_file function."""

import pytest
import tempfile
import os

from eencijferho.core.parse_metadata import parse_metadata_file


class TestParseMetadataIntegration:
    """Integration tests for parse_metadata_file with realistic metadata."""

    def test_parse_simple_variable(self):
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


class TestParseMetadataContinuationMarker:
    """DUO's '>' marker: the same code, in a different case.

    Text fragments below are copied verbatim from
    Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt, which has 16 of them.
    """

    # BB lines 979-980
    SIMPLE = """Diplomajaar
---
De jaaraanduiding van de periode waarin het diploma is behaald.

Mogelijke waarden:
0000 = geen examen geregistreerd
> 0000 voor overige inschrijvingen

"""

    # BB lines 1181-1184: the note wraps onto a second line
    WRAPPED = """Vooropleiding hoogste vooropleiding
---
Vooropleiding zoals geregistreerd.

Mogelijke waarden:
0000 = vooropleiding onbekend of geregistreerd diplomajaar is ongeldig
> 0000 voor overige inschrijvingen. NB wanneer het diplomajaar 2003 is, dan
betreft het een diploma behaald in het schooljaar 2003/2004.

"""

    # BB lines 2136-2138: a plain wrapped value, no '>' involved
    PLAIN_WRAP = """Aantal actieve inschrijvingen HO
---
Aantal actieve inschrijvingen.

Mogelijke waarden:
00 = inschrijving heeft soort inschrijving actuele instelling-type HO binnen soort HO van 3 of 5
     (of 8 of C)

"""

    def _parse(self, tmp_path, text):
        metadata_file = tmp_path / "test.txt"
        metadata_file.write_text(text, encoding="latin-1")
        return parse_metadata_file(str(metadata_file))

    def test_marker_does_not_end_up_in_the_value(self, tmp_path):
        """The marker is documentation about a code, not part of what the code means."""
        result = self._parse(tmp_path, self.SIMPLE)
        assert result[0]["values"]["0000"] == "geen examen geregistreerd"

    def test_no_value_description_contains_the_marker(self, tmp_path):
        result = self._parse(tmp_path, self.SIMPLE)
        assert not any(">" in str(v) for v in result[0]["values"].values())

    def test_marker_marks_the_list_as_non_exhaustive(self, tmp_path):
        """'… voor overige inschrijvingen' means the list does not cover every case.

        value_validation skips these lists; it needs to know this without
        grepping the value text for ' > '.
        """
        result = self._parse(tmp_path, self.SIMPLE)
        assert result[0]["non_exhaustive_values"] is True

    def test_wrapped_marker_block_is_swallowed_whole(self, tmp_path):
        """The NB note wraps. Half of it leaking in is worse than none of it."""
        result = self._parse(tmp_path, self.WRAPPED)
        assert result[0]["values"]["0000"] == "vooropleiding onbekend of geregistreerd diplomajaar is ongeldig"

    def test_list_without_marker_is_not_flagged(self, tmp_path):
        result = self._parse(tmp_path, "Geslacht\n---\nGeslacht.\n\nMogelijke waarden:\n1 = Man\n2 = Vrouw\n\n")
        assert not result[0].get("non_exhaustive_values")

    def test_plain_wrapped_value_is_still_appended(self, tmp_path):
        """Guard: only '>' starts a note. Ordinary wrapping keeps working."""
        result = self._parse(tmp_path, self.PLAIN_WRAP)
        assert result[0]["values"]["00"].endswith("(of 8 of C)")
        assert not result[0].get("non_exhaustive_values")

    def test_note_is_kept_in_the_description(self, tmp_path):
        """The note is documentation about the field, so it moves to the description.

        It must not be dropped: a data tool that silently discards what the
        source says is exactly the failure mode this parser had elsewhere.
        """
        result = self._parse(tmp_path, self.WRAPPED)
        assert "> 0000 voor overige inschrijvingen" in result[0]["description"]
        assert "betreft het een diploma behaald in het schooljaar 2003/2004" in result[0]["description"]

    def test_code_after_the_block_is_still_parsed(self, tmp_path):
        """The block must not swallow the rest of the list."""
        text = (
            "Diplomajaar\n---\nBeschrijving.\n\nMogelijke waarden:\n"
            "0000 = geen examen geregistreerd\n"
            "> 0000 voor overige inschrijvingen\n\n"
            "2015 = diploma behaald in 2015\n\n"
        )
        result = self._parse(tmp_path, text)
        assert result[0]["values"]["2015"] == "diploma behaald in 2015"
        assert result[0]["values"]["0000"] == "geen examen geregistreerd"


class TestParseMetadataNoteAfterValues:
    """Free text after a blank line below the list is a note, not part of the last value.

    The value lists and notes below come from
    Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt (BB). Headers and
    descriptions are shortened; accented letters are written in ASCII.
    """

    # BB lines 2253-2275, verbatim
    INDICATIE_GEBOREN = """Indicatie geboren
-----------------
Indicatie die aangeeft of de persoon zelf geboren is in Nederland of niet
en of een of beide ouders geboren zijn in Nederland.

Mogelijke waarden:
11 = geboren in Nederland en beide ouders geboren in Nederland
12 = geboren in Nederland en een ouder geboren in Nederland, een in het buitenland
13 = geboren in Nederland en beide ouders geboren in het buitenland
21 = geboren in het buitenland en beide ouders geboren in Nederland
22 = geboren in het buitenland en een ouder geboren in Nederland, een in het buitenland
23 = geboren in het buitenland en beide ouders geboren in het buitenland
99 = onbekend

Het CBS dikt de waarden als volgt in:
* Nederlandse herkomst = 11
* Migranten = 21, 22 en 23
* Kinderen van migranten = 12 en 13

OCW dikt de waarden als volgt in:
* Nederlandse herkomst = 11, 21
* Migranten = 22 en 23
* Kinderen van migranten = 12 en 13

"""

    # BB lines 1015-1019: the note wraps onto an indented line
    WRAPPED_NOTE = """Indicatie eerste einddiploma instelling
---------------------------------------
Indicatie eerste einddiploma.

Mogelijke waarden:
[leeg] = geen einddiplomarecord
01 = Eerste einddiploma in het hoger onderwijs
02 = Niet eerste einddiploma in het hoger onderwijs

Einddiploma HBO: associate degree, bachelor, initieel, master, vervolg en
                 postinitiele master.
Einddiploma WO:  bachelor, initieel, master, beroeps, vervolg en postinitiele master.

NB: Eerste/Niet eerste wordt bepaald over het hele bestand.

"""

    # BB lines 512-515 (values shortened): the note's second line holds '= 5', which is not a code
    NOTE_WITH_EQUALS = """Croho subonderdeel
------------------
Het croho-subonderdeel.

Mogelijke waarden:
XX = opleiding telt niet mee

NB Records van postinitiele masters zijn wel opgenomen in 1 cijfer HO, maar worden
beschouwd als opleidingen die niet meetellen (soort inschrijving = 5).

"""

    # BB lines 254-263 (one bullet left out): an indented paragraph after a blank line is still the value
    INDENTED_PARAGRAPH = """Reden uitschrijving
-------------------
Code, die aangeeft om welke reden de inschrijving is beeindigd.

Mogelijke waarden:
1 = De student waarvan:
    inschrijvingen t/m studiejaar 1992-1993:
    - de inschrijving aan een HBO-instelling tussentijds wordt beeindigd nadat
      het afsluitend examen (hoofdfase) is behaald.

    inschrijvingen met ingang van studiejaar 1993-1994:
    - de inschrijving aan een WO- of HBO-instelling wordt beeindigd nadat het
      afsluitend examen is behaald.

2 = De student, waarvan de inschrijving tussentijds wordt beeindigd

"""

    def _parse(self, tmp_path, text):
        metadata_file = tmp_path / "test.txt"
        metadata_file.write_text(text, encoding="latin-1")
        return parse_metadata_file(str(metadata_file))

    def test_indicatie_geboren_keeps_duo_spelling(self, tmp_path):
        """DUO writes '99 = onbekend'; the parser must not overwrite it."""
        result = self._parse(tmp_path, self.INDICATIE_GEBOREN)
        assert result[0]["values"]["99"] == "onbekend"

    def test_indicatie_geboren_note_goes_to_the_description(self, tmp_path):
        result = self._parse(tmp_path, self.INDICATIE_GEBOREN)
        desc = result[0]["description"]
        assert "Het CBS dikt de waarden als volgt in:" in desc
        assert "OCW dikt de waarden als volgt in:" in desc
        assert set(result[0]["values"]) == {"11", "12", "13", "21", "22", "23", "99"}

    def test_wrapped_note_leaves_the_last_value_alone(self, tmp_path):
        result = self._parse(tmp_path, self.WRAPPED_NOTE)
        values = result[0]["values"]
        assert values["02"] == "Niet eerste einddiploma in het hoger onderwijs"
        desc = result[0]["description"]
        assert "Einddiploma HBO: associate degree, bachelor, initieel, master, vervolg en postinitiele master." in desc
        assert "NB: Eerste/Niet eerste wordt bepaald over het hele bestand." in desc

    def test_equals_sign_inside_a_note_is_not_a_code(self, tmp_path):
        result = self._parse(tmp_path, self.NOTE_WITH_EQUALS)
        assert result[0]["values"] == {"XX": "opleiding telt niet mee"}
        assert "(soort inschrijving = 5)." in result[0]["description"]

    def test_indented_paragraph_after_blank_line_stays_in_the_value(self, tmp_path):
        result = self._parse(tmp_path, self.INDENTED_PARAGRAPH)
        values = result[0]["values"]
        assert "inschrijvingen met ingang van studiejaar 1993-1994:" in values["1"]
        assert values["1"].endswith("afsluitend examen is behaald.")
        assert values["2"] == "De student, waarvan de inschrijving tussentijds wordt beeindigd"
        assert "1993-1994" not in result[0]["description"]

    def test_key_after_a_note_is_still_parsed(self, tmp_path):
        """A note does not swallow a key=value line that follows it."""
        text = (
            "Code bekostiging\n---\nBeschrijving.\n\nMogelijke waarden:\n"
            "[leeg] = onbekend, wordt behandeld als N\n\n"
            "Vanaf 2009 betekent dit veld iets anders.\n"
            "J = Ja\n\n"
        )
        result = self._parse(tmp_path, text)
        assert result[0]["values"] == {"[leeg]": "onbekend, wordt behandeld als N", "J": "Ja"}
        assert "Vanaf 2009 betekent dit veld iets anders." in result[0]["description"]

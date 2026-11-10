"""The NFWA preset must deliver the diploma year in the form NFWA computes with.

NFWA compares the diploma year of the highest prior education with the
enrolment year. DUO writes 0000 when that year is unknown or invalid. Left as a
code it reads as the year 0, and NFWA then counts the student as having taken a
gap year.
"""

import json

import polars as pl

from eencijferho.core.decoder import decode_fields
from eencijferho.presets import PRESET_CONFIGS

_UNKNOWN_YEAR_LABEL = "vooropleiding onbekend of geregistreerd diplomajaar is ongeldig"


def _decode_with_nfwa_preset(tmp_path) -> pl.DataFrame:
    """Decode two EV rows with the settings of the NFWA preset."""
    # Variable names as extract writes them, column names as in the EV layout:
    # DUO names the last field differently in the two places.
    variable_metadata = [
        {"name": "Geslacht", "values": {"M": "man", "V": "vrouw", "O": "onbekend"}},
        {"name": "Diplomajaar", "values": {"0000": "geen examen geregistreerd"}},
        {
            "name": "Diplomajaar van de hoogste vooropl. vóór het HO",
            "values": {"0000": _UNKNOWN_YEAR_LABEL},
        },
        {
            "name": "Diplomajaar van de hoogste vooropl. binnen het HO",
            "values": {"0000": _UNKNOWN_YEAR_LABEL},
        },
        {
            "name": "Diplomajaar van de hoogste vooropleiding",
            "values": {"0000": _UNKNOWN_YEAR_LABEL},
        },
    ]
    variable_metadata_path = tmp_path / "variable_metadata.json"
    variable_metadata_path.write_text(json.dumps(variable_metadata), encoding="utf-8")
    dec_metadata_path = tmp_path / "Bestandsbeschrijving_Dec-bestanden.json"
    dec_metadata_path.write_text(json.dumps({"tables": []}), encoding="utf-8")
    df = pl.DataFrame(
        {
            "Geslacht": ["M", "V"],
            "Diplomajaar": ["0000", "2023"],
            "Diplomajaar van de hoogste vooropl. vóór het HO": ["0000", "2019"],
            "Diplomajaar van de hoogste vooropl. binnen het HO": ["0000", "2021"],
            "Diplomajaar hoogste vooropleiding": ["0000", "2021"],
        }
    )
    settings = PRESET_CONFIGS["nfwa"]["settings"]

    return decode_fields(
        df,
        str(dec_metadata_path),
        dec_tables={},
        variable_metadata_path=str(variable_metadata_path),
        decode_columns=settings["decode_columns"],
        enrich_variables=settings["enrich_variables"],
    )


def test_nfwa_preset_labels_unknown_diploma_year_of_highest_prior_education(tmp_path):
    result = _decode_with_nfwa_preset(tmp_path)

    assert result["Diplomajaar hoogste vooropleiding"].to_list() == [
        "vooropleiding onbekend of geregistreerd diplomajaar is ongeldig",
        "2021",
    ]


def test_nfwa_preset_keeps_the_other_diploma_years_as_years(tmp_path):
    # The Evaluatietool Selectie shares these settings and subtracts "Diplomajaar"
    # from the enrolment year, so that column must stay a year.
    result = _decode_with_nfwa_preset(tmp_path)

    assert result["Diplomajaar"].to_list() == ["0000", "2023"]
    assert result["Diplomajaar van de hoogste vooropl. vóór het HO"].to_list() == [
        "0000",
        "2019",
    ]
    assert result["Diplomajaar van de hoogste vooropl. binnen het HO"].to_list() == [
        "0000",
        "2021",
    ]

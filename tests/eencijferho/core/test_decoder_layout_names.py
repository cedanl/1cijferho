"""Fields that DUO names differently in its descriptions than in the data file layout.

The Dec description and the variable description call a field
"Hoogste vooropleiding vóór het HO oorspr. code" or "Diplomajaar van de hoogste
vooropleiding". The layout of the EV file calls the same fields
"Hoogste vooropleiding vóór het HO oorspronkelijke code" and "Diplomajaar
hoogste vooropleiding". The decoder matches names exactly (#202), so it has to
know these pairs: without them the fields keep their codes, and a consumer such
as NFWA reads the diploma year 0000 (unknown) as the year 0.
"""

import json
import re
from pathlib import Path

import polars as pl
import pytest

from eencijferho.core.decoder import (
    _LAYOUT_NAMES,
    decode_fields,
    decode_fields_dec_only,
)
from eencijferho.io.text_encoding import decode_description_bytes

_DEMO_DIR = Path(__file__).resolve().parents[3] / "data" / "01-input" / "DEMO"

# (name in a DUO description, name of the same field in the EV layout)
_DEC_PAIRS = [
    (
        "Hoogste Vooropleiding vóór het HO oorspr. code",
        "Hoogste vooropleiding vóór het HO oorspronkelijke code",
    ),
    (
        "Hoogste vooropleiding binnen het HO oorspr. code",
        "Hoogste vooropleiding binnen het HO oorspronkelijke code",
    ),
]
_VARIABLE_PAIRS = [
    ("Diplomajaar van de hoogste vooropleiding", "Diplomajaar hoogste vooropleiding"),
    (
        "Indicatie eerstejaars opleiding actueel equivalent",
        "Indicatie eerstejaars opl. actueel equivalent",
    ),
    (
        "Indicatie eerstejaars actuele opleiding-instelling",
        "Indicatie eerstejaars actuele opl.-instelling",
    ),
    (
        "Indicatie eerstejaars act. inst - type ho binnen soort ho",
        "Indicatie eerstejrs act. inst - type ho binnen soort ho",
    ),
]


def _write_json(path: Path, data) -> str:
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def _dec_vooropl_metadata(tmp_path: Path) -> str:
    """The Dec_vooropl.asc table as extract writes it from the DUO description."""
    table = {
        "table_title": "Dec_vooropl.asc",
        "content": [
            "                                             Startpositie  Aantal posities",
            "Code vooropleiding oorspr. code                     1             5",
            "Onderwijssector                                     6             3",
            "Omschrijving vooropleiding oorspr. code             9           180",
        ],
        "decoding_variables": [name for name, _ in _DEC_PAIRS],
    }
    return _write_json(
        tmp_path / "Bestandsbeschrijving_Dec-bestanden.json", {"tables": [table]}
    )


def _dec_vooropl_table() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Code vooropleiding oorspr. code": ["V0170", "H0010"],
            "Onderwijssector": ["VO", "HBO"],
            "Omschrijving vooropleiding oorspr. code": ["VWO-ATHENEUM", "HBO bachelor"],
        }
    )


@pytest.mark.parametrize(
    "decode_columns", [None, [name for name, _ in _DEC_PAIRS]], ids=["all", "selected"]
)
def test_dec_labels_reach_the_field_that_the_layout_spells_differently(
    tmp_path, decode_columns
):
    df = pl.DataFrame(
        {
            "Hoogste vooropleiding vóór het HO oorspronkelijke code": [
                "V0170",
                "00000",
            ],
            "Hoogste vooropleiding binnen het HO oorspronkelijke code": [
                "00000",
                "H0010",
            ],
        }
    )

    result = decode_fields_dec_only(
        df,
        _dec_vooropl_metadata(tmp_path),
        {"Dec_vooropl.asc": _dec_vooropl_table()},
        decode_columns=decode_columns,
    )

    before_ho = "hoogste_vooropleiding_voor_het_ho_oorspronkelijke_code"
    within_ho = "hoogste_vooropleiding_binnen_het_ho_oorspronkelijke_code"
    assert result[f"{before_ho}__omschrijving_vooropleiding_oorspr_code"].to_list() == [
        "VWO-ATHENEUM",
        None,
    ]
    assert result[f"{before_ho}__onderwijssector"].to_list() == ["VO", None]
    assert result[f"{within_ho}__omschrijving_vooropleiding_oorspr_code"].to_list() == [
        None,
        "HBO bachelor",
    ]
    assert result[
        "Hoogste vooropleiding vóór het HO oorspronkelijke code"
    ].to_list() == ["V0170", "00000"]


@pytest.mark.parametrize(("description_name", "layout_name"), _VARIABLE_PAIRS)
def test_variable_labels_reach_the_field_that_the_layout_spells_differently(
    tmp_path, description_name, layout_name
):
    variable_metadata = _write_json(
        tmp_path / "variable_metadata.json",
        [
            {
                "name": description_name,
                "values": {"0000": "onbekend", "1": "eerstejaars"},
            }
        ],
    )
    dec_metadata = _write_json(tmp_path / "dec.json", {"tables": []})
    df = pl.DataFrame({layout_name: ["0000", "1", "2021"]})

    result = decode_fields(
        df, dec_metadata, dec_tables={}, variable_metadata_path=variable_metadata
    )

    assert result[layout_name].to_list() == ["onbekend", "eerstejaars", "2021"]


def test_selecting_a_variable_by_its_description_name_enriches_the_layout_column(
    tmp_path,
):
    variable_metadata = _write_json(
        tmp_path / "variable_metadata.json",
        [
            {
                "name": "Diplomajaar van de hoogste vooropleiding",
                "values": {"0000": "onbekend"},
            },
            {"name": "Diplomajaar", "values": {"0000": "geen examen"}},
        ],
    )
    dec_metadata = _write_json(tmp_path / "dec.json", {"tables": []})
    df = pl.DataFrame(
        {"Diplomajaar hoogste vooropleiding": ["0000"], "Diplomajaar": ["0000"]}
    )

    result = decode_fields(
        df,
        dec_metadata,
        dec_tables={},
        variable_metadata_path=variable_metadata,
        enrich_variables=["Diplomajaar van de hoogste vooropleiding"],
    )

    assert result["Diplomajaar hoogste vooropleiding"].to_list() == ["onbekend"]
    assert result["Diplomajaar"].to_list() == ["0000"]


def test_a_column_with_the_description_name_itself_is_used_first(tmp_path):
    # Should DUO align the layout with the description, the field must still decode.
    variable_metadata = _write_json(
        tmp_path / "variable_metadata.json",
        [
            {
                "name": "Diplomajaar van de hoogste vooropleiding",
                "values": {"0000": "onbekend"},
            }
        ],
    )
    dec_metadata = _write_json(tmp_path / "dec.json", {"tables": []})
    df = pl.DataFrame(
        {
            "Diplomajaar van de hoogste vooropleiding": ["0000"],
            "Diplomajaar hoogste vooropleiding": ["0000"],
        }
    )

    result = decode_fields(
        df, dec_metadata, dec_tables={}, variable_metadata_path=variable_metadata
    )

    assert result["Diplomajaar van de hoogste vooropleiding"].to_list() == ["onbekend"]
    assert result["Diplomajaar hoogste vooropleiding"].to_list() == ["0000"]


def test_a_file_without_the_layout_column_is_left_alone(tmp_path):
    # VAKHAVW has "Vooropleiding oorspronkelijke code": a different field with a
    # similar name. The EV mappings must not land on it (#202).
    variable_metadata = _write_json(
        tmp_path / "variable_metadata.json",
        [
            {
                "name": "Hoogste vooropleiding vóór het HO oorspr. code",
                "values": {"V0170": "VWO"},
            },
            {
                "name": "Hoogste vooropleiding binnen het HO oorspr. code",
                "values": {"V0170": "HBO"},
            },
        ],
    )
    df = pl.DataFrame({"Vooropleiding oorspronkelijke code": ["V0170", "V0370"]})

    result = decode_fields(
        df,
        _dec_vooropl_metadata(tmp_path),
        {"Dec_vooropl.asc": _dec_vooropl_table()},
        variable_metadata_path=variable_metadata,
    )

    assert result.columns == ["Vooropleiding oorspronkelijke code"]
    assert result["Vooropleiding oorspronkelijke code"].to_list() == ["V0170", "V0370"]


def _demo_description(filename: str) -> str:
    path = _DEMO_DIR / filename
    return decode_description_bytes(path.read_bytes(), str(path))


@pytest.mark.parametrize(
    "ev_description",
    [
        "Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt",
        "Bestandsbeschrijving_1cyferho_2024_v1.1_DEMO.txt",
    ],
)
@pytest.mark.parametrize(
    ("description_name", "layout_name"), sorted(_LAYOUT_NAMES.items())
)
def test_every_pair_in_the_table_is_taken_from_the_duo_descriptions(
    ev_description, description_name, layout_name
):
    ev_text = _demo_description(ev_description)
    dec_text = _demo_description("Bestandsbeschrijving_Dec-bestanden.txt")

    # The layout lists a field as: name, start position, width.
    layout_row = re.compile(rf"^{re.escape(layout_name)}\s+\d+\s+\d+\s*$", re.MULTILINE)
    # The variable description has the name as a heading, the Dec description
    # lists it as "* name" under the table it decodes.
    described = re.compile(rf"^(\* )?{re.escape(description_name)}\s*$", re.MULTILINE)

    assert layout_row.search(ev_text)
    assert described.search(ev_text) or described.search(dec_text)

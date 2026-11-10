"""Inferred layouts for Dec_brinnummer / Dec_brinvestigingsnummer (#204).

DUO publishes no description for these two files; the layout lives in
``dec_layouts``. These tests run it through the same steps as a DUO layout
(JSON -> Excel -> converter) on synthetic fixed-width rows.
"""

import json

import polars as pl
import pytest

from eencijferho.core.converter import converter
from eencijferho.core.dec_layouts import (
    SUPPLEMENTARY_LAYOUT_FILENAME,
    supplementary_tables,
)
from eencijferho.core.extractor import _build_table_rows, extract_excel_from_json

# Synthetic rows in the layout of the DUO files: key, naam (30), postcode (4),
# plaats (24), oprichting (8), opheffing (8), denominatie code (3) + naam.
BRIN_ROW = (
    "00AA" + "Voorbeeldschool".ljust(30) + "1234" + "PLAATS".ljust(24)
    + "19800101" + " " * 8 + "OPB" + "Openbaar"
)
BRINVEST_ROW = (
    "00AA" + "01" + "Voorbeeldvestiging".ljust(30) + "1234" + "PLAATS".ljust(24)
    + "19800101" + "20140801" + "RK " + "Rooms-Katholiek"
)


@pytest.mark.parametrize("title,keys", [
    ("Dec_brinnummer.asc", [("Brinnummer", 1, 4)]),
    ("Dec_brinvestigingsnummer.asc", [("Brinnummer", 1, 4), ("Vestigingsnummer", 5, 2)]),
])
def test_layout_rows_parse_with_contiguous_positions(title, keys):
    content = supplementary_tables()[title]["content"]
    header = content[0]
    rows, valid = _build_table_rows(
        content, header.find("Startpositie"), header.find("Aantal posities"),
    )
    fields = [(r[1], r[2], r[3]) for r in rows[1:]]
    assert valid == len(content) - 1
    assert fields[: len(keys)] == keys
    end = 1
    for _, start, width in fields:
        assert start == end
        end = start + width


def _convert(tmp_path, title, row):
    table = {"table_number": 1, **supplementary_tables()[title]}
    json_path = tmp_path / f"{SUPPLEMENTARY_LAYOUT_FILENAME}.json"
    json_path.write_text(json.dumps({"filename": SUPPLEMENTARY_LAYOUT_FILENAME, "tables": [table]}))
    results, created, _ = extract_excel_from_json(str(json_path), str(tmp_path))
    assert created == 1
    xlsx = tmp_path / results[0]["output_file"]
    assert title in xlsx.name  # the matcher finds the layout by file name

    asc = tmp_path / title
    asc.write_bytes((row + "\r\n").encode("latin-1"))
    out, _ = converter(str(asc), str(xlsx), str(tmp_path / "out"))
    return pl.read_csv(out, separator=";", infer_schema_length=0)


def test_dec_brinnummer_converts_with_leading_zeros(tmp_path):
    df = _convert(tmp_path, "Dec_brinnummer.asc", BRIN_ROW)
    assert df["Brinnummer"].to_list() == ["00AA"]
    assert df["Naam"].to_list() == ["Voorbeeldschool"]
    assert df["Postcodecijfers"].to_list() == ["1234"]
    assert df["Naam denominatie (= richting)"].to_list() == ["Openbaar"]


def test_dec_brinvestigingsnummer_converts_both_keys(tmp_path):
    df = _convert(tmp_path, "Dec_brinvestigingsnummer.asc", BRINVEST_ROW)
    assert df["Brinnummer"].to_list() == ["00AA"]
    assert df["Vestigingsnummer"].to_list() == ["01"]
    assert df["Naam"].to_list() == ["Voorbeeldvestiging"]
    assert df["Datum opheffing"].to_list() == ["20140801"]

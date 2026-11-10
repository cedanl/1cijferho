"""
Layouts for Dec_* lookup files that DUO ships without a file description.

The Vakkenbestanden description refers to ``Dec_brinnummer.asc`` and
``Dec_brinvestigingsnummer.asc`` ("te decoderen met ..."), but no DUO
bestandsbeschrijving describes their columns. Without a layout the converter
cannot read them, so the VAKHAVW fields that point to them stay undecoded.

The layouts below are inferred by 1CijferHO from the fixed-width files
themselves. They follow the published layouts of ``Dec_instellingscode.asc``
and ``Dec_instellingscodevest.asc`` (same RIO source, same column widths).
Only the key columns (Brinnummer, Vestigingsnummer) are relied on for the
join; the other columns are best-effort labels.

The tables use the same JSON shape as the tables that
``extractor.extract_tables_from_txt`` writes, so the existing steps (Excel
export, validation, matching, conversion) handle them unchanged.
"""

# Name of the JSON (and prefix of the Excel files) written for these layouts.
# It must not match ``Bestandsbeschrijving_Dec-bestanden*``: that glob selects
# the DUO Dec JSON elsewhere in the pipeline.
SUPPLEMENTARY_LAYOUT_FILENAME = "Lay-out_afgeleid_1CijferHO"

# Note written next to every inferred layout, in logs and the JSON.
INFERRED_LAYOUT_NOTE = (
    "Lay-out afgeleid door 1CijferHO uit het bestand zelf; "
    "DUO publiceert geen bestandsbeschrijving voor dit bestand."
)

_HEADER = " " * 45 + "Startpositie  Aantal posities"

# (name, start position, width, remark) per field. A "}" remark marks the
# fields that together form the key, as in the DUO descriptions.
_FIELDS: dict[str, list[tuple[str, int, int, str]]] = {
    "Dec_brinnummer.asc": [
        ("Brinnummer", 1, 4, ""),
        ("Naam", 5, 30, ""),
        ("Postcodecijfers", 35, 4, ""),
        ("Plaats", 39, 24, ""),
        ("Datum oprichting", 63, 8, "(EEJJMMDD)"),
        ("Datum opheffing", 71, 8, "(EEJJMMDD)"),
        ("Code denominatie (= richting)", 79, 3, ""),
        ("Naam denominatie (= richting)", 82, 30, ""),
    ],
    "Dec_brinvestigingsnummer.asc": [
        ("Brinnummer", 1, 4, "} sleutel"),
        ("Vestigingsnummer", 5, 2, "}"),
        ("Naam", 7, 30, ""),
        ("Postcodecijfers", 37, 4, ""),
        ("Plaats", 41, 24, ""),
        ("Datum oprichting", 65, 8, "(EEJJMMDD)"),
        ("Datum opheffing", 73, 8, "(EEJJMMDD)"),
        ("Code denominatie (= richting)", 81, 3, ""),
        ("Naam denominatie (= richting)", 84, 30, ""),
    ],
}


def _layout_row(name: str, start: int, width: int, remark: str) -> str:
    """Render one field the way DUO aligns it under the header columns."""
    row = f"{name:<48}{start:>5}{width:>14}"
    return f"{row}      {remark}" if remark else row


def supplementary_tables() -> dict[str, dict]:
    """Inferred Dec layouts keyed by file name (``Dec_X.asc``).

    Each value has the keys of an extracted bestandsbeschrijving table:
    ``table_title``, ``content``, ``decoding_variables`` (empty: the Vakken
    description names the fields that use these tables) and ``note``.
    """
    return {
        title: {
            "table_title": title,
            "content": [_HEADER] + [_layout_row(*field) for field in fields],
            "decoding_variables": [],
            "note": INFERRED_LAYOUT_NOTE,
        }
        for title, fields in _FIELDS.items()
    }

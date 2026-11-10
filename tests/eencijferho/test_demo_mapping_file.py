"""The DEMO mapping file must cover the DEMO data, so the student number link can be tried.

The example file (koppeling_bsn_studentnummer_VOORBEELD.csv) only shows the
format: none of its BSNs occur in the DEMO files, so a run with it adds an empty
studentnummer column.

When the DEMO EV file changes, regenerate the mapping file: one row for each
distinct BSN in order of first appearance, numbered from 1000001.
"""

from pathlib import Path

import polars as pl

from eencijferho.utils.translator import translate_bsn_to_local_id

_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
_MAPPING_FILE = _DATA_DIR / "koppeling_bsn_studentnummer_DEMO.csv"
_DEMO_EV_FILE = _DATA_DIR / "01-input" / "DEMO" / "EV299XX24_DEMO.asc"
# Burgerservicenummer in the 2024 EV layout: start position 306, 9 wide.
_BSN_FIELD = slice(305, 314)


def _write_demo_bsn_column(path: Path) -> None:
    """Write the BSN of every DEMO enrolment row as a one-column CSV."""
    with open(_DEMO_EV_FILE, encoding="latin-1") as source:
        bsns = [line[_BSN_FIELD].strip() for line in source if line.strip()]
    path.write_text("Burgerservicenummer\n" + "\n".join(bsns) + "\n", encoding="utf-8")


def test_every_demo_enrolment_gets_a_student_number(tmp_path):
    _write_demo_bsn_column(tmp_path / "EV299XX24_DEMO.csv")

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(_MAPPING_FILE))

    result = pl.read_csv(
        tmp_path / "EV299XX24_DEMO.csv", separator=";", infer_schema_length=0
    )
    assert result.height == 43663
    assert result["studentnummer"].null_count() == 0


def test_demo_student_numbers_are_unique():
    mapping = pl.read_csv(_MAPPING_FILE, separator=";", infer_schema_length=0)

    assert mapping["studentnummer"].n_unique() == mapping.height

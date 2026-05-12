# Tests for eencijferho.utils.translator

import pytest
import polars as pl

from eencijferho.utils.translator import translate_bsn_to_local_id


_BSN_COL = "Burgerservicenummer"


def _write_csv(path, rows: list[dict], sep: str = ";") -> None:
    cols = list(rows[0].keys())
    lines = [sep.join(cols)] + [sep.join(str(r[c]) for c in cols) for r in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def _read_csv(path) -> pl.DataFrame:
    return pl.read_csv(path, separator=";", infer_schema_length=0)


# ---------------------------------------------------------------------------
# Happy path: CSV (semicolon)
# ---------------------------------------------------------------------------

def test_adds_studentnummer_column_csv_semicolon(tmp_path):
    _write_csv(
        tmp_path / "EV2023.csv",
        [{_BSN_COL: "111111111", "Naam": "Jan"}, {_BSN_COL: "222222222", "Naam": "Piet"}],
    )
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "111111111", "studentnummer": "S001"},
                         {"burgerservicenummer": "222222222", "studentnummer": "S002"}])

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert "studentnummer" in df.columns
    assert df.filter(pl.col(_BSN_COL) == "111111111")["studentnummer"][0] == "S001"
    assert df.filter(pl.col(_BSN_COL) == "222222222")["studentnummer"][0] == "S002"


# ---------------------------------------------------------------------------
# Happy path: CSV (comma fallback)
# ---------------------------------------------------------------------------

def test_adds_studentnummer_csv_comma_separator(tmp_path):
    _write_csv(
        tmp_path / "EV2023.csv",
        [{_BSN_COL: "333333333", "Naam": "Anna"}],
    )
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "333333333", "studentnummer": "S003"}], sep=",")

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert df["studentnummer"][0] == "S003"


# ---------------------------------------------------------------------------
# Happy path: Parquet mapping file
# ---------------------------------------------------------------------------

def test_adds_studentnummer_parquet_mapping(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_BSN_COL: "444444444", "Naam": "Klaas"}])
    mapping_parquet = tmp_path / "mapping.parquet"
    pl.DataFrame({"burgerservicenummer": ["444444444"], "studentnummer": ["S004"]}).write_parquet(
        mapping_parquet
    )

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping_parquet))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert df["studentnummer"][0] == "S004"


# ---------------------------------------------------------------------------
# VAKHAVW files are processed
# ---------------------------------------------------------------------------

def test_processes_vakhavw_file(tmp_path):
    _write_csv(tmp_path / "VAKHAVW2023.csv", [{_BSN_COL: "555555555", "Vak": "NL"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "555555555", "studentnummer": "S005"}])

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "VAKHAVW2023.csv")
    assert df["studentnummer"][0] == "S005"


# ---------------------------------------------------------------------------
# Unmatched BSN → null in studentnummer (left join)
# ---------------------------------------------------------------------------

def test_unmatched_bsn_produces_null(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_BSN_COL: "999999999", "Naam": "Onbekend"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "000000000", "studentnummer": "S000"}])

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert "studentnummer" in df.columns
    assert df["studentnummer"][0] is None


# ---------------------------------------------------------------------------
# Files without BSN column are skipped
# ---------------------------------------------------------------------------

def test_skips_file_without_bsn_column(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{"Naam": "Jan", "Jaar": "2023"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "111111111", "studentnummer": "S001"}])

    log = translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert "studentnummer" not in df.columns
    assert "overgeslagen" in log


# ---------------------------------------------------------------------------
# Non-EV/VAKHAVW files are untouched
# ---------------------------------------------------------------------------

def test_skips_non_ev_vakhavw_files(tmp_path):
    _write_csv(tmp_path / "Dec_geslacht.csv", [{_BSN_COL: "111111111", "Code": "M"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "111111111", "studentnummer": "S001"}])

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "Dec_geslacht.csv")
    assert "studentnummer" not in df.columns


# ---------------------------------------------------------------------------
# Fan-out (duplicate BSN in mapping) → ValueError, no files written
# ---------------------------------------------------------------------------

def test_fanout_raises_and_does_not_write(tmp_path):
    original_content = f"{_BSN_COL};Naam\n111111111;Jan\n"
    ev = tmp_path / "EV2023.csv"
    ev.write_text(original_content, encoding="utf-8")

    mapping = tmp_path / "mapping.csv"
    _write_csv(
        mapping,
        [{"burgerservicenummer": "111111111", "studentnummer": "S001"},
         {"burgerservicenummer": "111111111", "studentnummer": "S999"}],
    )

    with pytest.raises(ValueError, match="geen bestanden gewijzigd"):
        translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    # Two-phase guarantee: original file must be unchanged
    assert ev.read_text(encoding="utf-8") == original_content


# ---------------------------------------------------------------------------
# Two-phase: if one file fails, no files are written
# ---------------------------------------------------------------------------

def test_two_phase_no_partial_writes(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_BSN_COL: "111111111", "Naam": "Jan"}])
    _write_csv(tmp_path / "VAKHAVW2023.csv", [{_BSN_COL: "222222222", "Vak": "NL"}])

    mapping = tmp_path / "mapping.csv"
    # 222222222 appears twice → fan-out for VAKHAVW; EV should also not be written
    _write_csv(
        mapping,
        [{"burgerservicenummer": "111111111", "studentnummer": "S001"},
         {"burgerservicenummer": "222222222", "studentnummer": "S002"},
         {"burgerservicenummer": "222222222", "studentnummer": "S999"}],
    )

    ev_before = (tmp_path / "EV2023.csv").read_text(encoding="utf-8")
    vakhavw_before = (tmp_path / "VAKHAVW2023.csv").read_text(encoding="utf-8")

    with pytest.raises(ValueError):
        translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    assert (tmp_path / "EV2023.csv").read_text(encoding="utf-8") == ev_before
    assert (tmp_path / "VAKHAVW2023.csv").read_text(encoding="utf-8") == vakhavw_before


# ---------------------------------------------------------------------------
# Missing column in mapping → ValueError with available columns listed
# ---------------------------------------------------------------------------

def test_missing_bsn_col_in_mapping_raises(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_BSN_COL: "111111111", "Naam": "Jan"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"onjuiste_kolom": "111111111", "studentnummer": "S001"}])

    with pytest.raises(ValueError, match="onjuiste_kolom|Beschikbare"):
        translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))


def test_missing_id_col_in_mapping_raises(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_BSN_COL: "111111111", "Naam": "Jan"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "111111111", "verkeerde_id": "S001"}])

    with pytest.raises(ValueError, match="verkeerde_id|Beschikbare"):
        translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))


# ---------------------------------------------------------------------------
# Unknown file extension → ValueError
# ---------------------------------------------------------------------------

def test_unknown_extension_raises(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_BSN_COL: "111111111", "Naam": "Jan"}])
    mapping = tmp_path / "mapping.xlsx"
    mapping.write_bytes(b"dummy")

    with pytest.raises(ValueError, match=".xlsx"):
        translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))


# ---------------------------------------------------------------------------
# Custom column names
# ---------------------------------------------------------------------------

def test_custom_column_names(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_BSN_COL: "777777777", "Naam": "Els"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"bsn": "777777777", "lokaal_id": "L777"}])

    translate_bsn_to_local_id(
        output_dir=str(tmp_path),
        mapping_file=str(mapping),
        mapping_bsn_col="bsn",
        mapping_id_col="lokaal_id",
    )

    df = _read_csv(tmp_path / "EV2023.csv")
    assert "lokaal_id" in df.columns
    assert df["lokaal_id"][0] == "L777"


# ---------------------------------------------------------------------------
# All variants (_decoded, _enriched) are also updated
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("suffix", ["", "_decoded", "_enriched"])
def test_all_ev_variants_updated(tmp_path, suffix):
    fname = f"EV2023{suffix}.csv"
    _write_csv(tmp_path / fname, [{_BSN_COL: "123123123", "Naam": "Test"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "123123123", "studentnummer": "S123"}])

    translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / fname)
    assert df["studentnummer"][0] == "S123"


# ---------------------------------------------------------------------------
# Empty output dir — no files, no error
# ---------------------------------------------------------------------------

def test_no_ev_files_returns_log(tmp_path):
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"burgerservicenummer": "111111111", "studentnummer": "S001"}])

    log = translate_bsn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    assert "0 bestand" in log

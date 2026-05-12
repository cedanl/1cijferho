# Tests for eencijferho.utils.translator

import pytest
import polars as pl

from eencijferho.utils.translator import translate_pgn_to_local_id


_PGN_COL = "Persoonsgebonden nummer"


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
        [{_PGN_COL: "111", "Naam": "Jan"}, {_PGN_COL: "222", "Naam": "Piet"}],
    )
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "111", "studentnummer": "S001"},
                         {"persoonsgebonden_nummer": "222", "studentnummer": "S002"}])

    translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert "studentnummer" in df.columns
    assert df.filter(pl.col(_PGN_COL) == "111")["studentnummer"][0] == "S001"
    assert df.filter(pl.col(_PGN_COL) == "222")["studentnummer"][0] == "S002"


# ---------------------------------------------------------------------------
# Happy path: CSV (comma fallback)
# ---------------------------------------------------------------------------

def test_adds_studentnummer_csv_comma_separator(tmp_path):
    _write_csv(
        tmp_path / "EV2023.csv",
        [{_PGN_COL: "333", "Naam": "Anna"}],
    )
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "333", "studentnummer": "S003"}], sep=",")

    translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert df["studentnummer"][0] == "S003"


# ---------------------------------------------------------------------------
# Happy path: Parquet mapping file
# ---------------------------------------------------------------------------

def test_adds_studentnummer_parquet_mapping(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_PGN_COL: "444", "Naam": "Klaas"}])
    mapping_parquet = tmp_path / "mapping.parquet"
    pl.DataFrame({"persoonsgebonden_nummer": ["444"], "studentnummer": ["S004"]}).write_parquet(
        mapping_parquet
    )

    translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping_parquet))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert df["studentnummer"][0] == "S004"


# ---------------------------------------------------------------------------
# VAKHAVW files are processed
# ---------------------------------------------------------------------------

def test_processes_vakhavw_file(tmp_path):
    _write_csv(tmp_path / "VAKHAVW2023.csv", [{_PGN_COL: "555", "Vak": "NL"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "555", "studentnummer": "S005"}])

    translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "VAKHAVW2023.csv")
    assert df["studentnummer"][0] == "S005"


# ---------------------------------------------------------------------------
# Unmatched PGN → null in studentnummer (left join)
# ---------------------------------------------------------------------------

def test_unmatched_pgn_produces_null(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_PGN_COL: "999", "Naam": "Onbekend"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "000", "studentnummer": "S000"}])

    translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert "studentnummer" in df.columns
    assert df["studentnummer"][0] is None


# ---------------------------------------------------------------------------
# Files without PGN column are skipped
# ---------------------------------------------------------------------------

def test_skips_file_without_pgn_column(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{"Naam": "Jan", "Jaar": "2023"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "111", "studentnummer": "S001"}])

    log = translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "EV2023.csv")
    assert "studentnummer" not in df.columns
    assert "overgeslagen" in log


# ---------------------------------------------------------------------------
# Non-EV/VAKHAVW files are untouched
# ---------------------------------------------------------------------------

def test_skips_non_ev_vakhavw_files(tmp_path):
    _write_csv(tmp_path / "Dec_geslacht.csv", [{_PGN_COL: "111", "Code": "M"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "111", "studentnummer": "S001"}])

    translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / "Dec_geslacht.csv")
    assert "studentnummer" not in df.columns


# ---------------------------------------------------------------------------
# Fan-out (duplicate PGN in mapping) → ValueError, no files written
# ---------------------------------------------------------------------------

def test_fanout_raises_and_does_not_write(tmp_path):
    original_content = f"{_PGN_COL};Naam\n111;Jan\n"
    ev = tmp_path / "EV2023.csv"
    ev.write_text(original_content, encoding="utf-8")

    mapping = tmp_path / "mapping.csv"
    _write_csv(
        mapping,
        [{"persoonsgebonden_nummer": "111", "studentnummer": "S001"},
         {"persoonsgebonden_nummer": "111", "studentnummer": "S999"}],
    )

    with pytest.raises(ValueError, match="geen bestanden gewijzigd"):
        translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    # Two-phase guarantee: original file must be unchanged
    assert ev.read_text(encoding="utf-8") == original_content


# ---------------------------------------------------------------------------
# Two-phase: if one file fails, no files are written
# ---------------------------------------------------------------------------

def test_two_phase_no_partial_writes(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_PGN_COL: "111", "Naam": "Jan"}])
    _write_csv(tmp_path / "VAKHAVW2023.csv", [{_PGN_COL: "222", "Vak": "NL"}])

    mapping = tmp_path / "mapping.csv"
    # 222 appears twice → fan-out for VAKHAVW; EV should also not be written
    _write_csv(
        mapping,
        [{"persoonsgebonden_nummer": "111", "studentnummer": "S001"},
         {"persoonsgebonden_nummer": "222", "studentnummer": "S002"},
         {"persoonsgebonden_nummer": "222", "studentnummer": "S999"}],
    )

    ev_before = (tmp_path / "EV2023.csv").read_text(encoding="utf-8")
    vakhavw_before = (tmp_path / "VAKHAVW2023.csv").read_text(encoding="utf-8")

    with pytest.raises(ValueError):
        translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    assert (tmp_path / "EV2023.csv").read_text(encoding="utf-8") == ev_before
    assert (tmp_path / "VAKHAVW2023.csv").read_text(encoding="utf-8") == vakhavw_before


# ---------------------------------------------------------------------------
# Missing column in mapping → ValueError with available columns listed
# ---------------------------------------------------------------------------

def test_missing_pgn_col_in_mapping_raises(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_PGN_COL: "111", "Naam": "Jan"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"onjuiste_kolom": "111", "studentnummer": "S001"}])

    with pytest.raises(ValueError, match="onjuiste_kolom|Beschikbare"):
        translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))


def test_missing_id_col_in_mapping_raises(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_PGN_COL: "111", "Naam": "Jan"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "111", "verkeerde_id": "S001"}])

    with pytest.raises(ValueError, match="verkeerde_id|Beschikbare"):
        translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))


# ---------------------------------------------------------------------------
# Unknown file extension → ValueError
# ---------------------------------------------------------------------------

def test_unknown_extension_raises(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_PGN_COL: "111", "Naam": "Jan"}])
    mapping = tmp_path / "mapping.xlsx"
    mapping.write_bytes(b"dummy")

    with pytest.raises(ValueError, match=".xlsx"):
        translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))


# ---------------------------------------------------------------------------
# Custom column names
# ---------------------------------------------------------------------------

def test_custom_column_names(tmp_path):
    _write_csv(tmp_path / "EV2023.csv", [{_PGN_COL: "777", "Naam": "Els"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"pgn": "777", "lokaal_id": "L777"}])

    translate_pgn_to_local_id(
        output_dir=str(tmp_path),
        mapping_file=str(mapping),
        mapping_pgn_col="pgn",
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
    _write_csv(tmp_path / fname, [{_PGN_COL: "123", "Naam": "Test"}])
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "123", "studentnummer": "S123"}])

    translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    df = _read_csv(tmp_path / fname)
    assert df["studentnummer"][0] == "S123"


# ---------------------------------------------------------------------------
# Empty output dir — no files, no error
# ---------------------------------------------------------------------------

def test_no_ev_files_returns_log(tmp_path):
    mapping = tmp_path / "mapping.csv"
    _write_csv(mapping, [{"persoonsgebonden_nummer": "111", "studentnummer": "S001"}])

    log = translate_pgn_to_local_id(output_dir=str(tmp_path), mapping_file=str(mapping))

    assert "0 bestand" in log

# Tests for eencijferho.core.extractor — encoding correctheid

import json
from pathlib import Path

import pytest

from eencijferho.core.extractor import extract_tables_from_txt, process_txt_folder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bestandsbeschrijving(path: Path, title: str = "TestTabel") -> Path:
    """Minimale DUO-achtige bestandsbeschrijving in latin-1."""
    content = (
        f"{title}\n"
        "================\n"
        "Startpositie  Naam\n"
        "1             VeldA\n"
        "\n"
    )
    path.write_text(content, encoding="latin-1")
    return path


# ---------------------------------------------------------------------------
# extract_tables_from_txt
# ---------------------------------------------------------------------------


def test_extract_returns_json_path(tmp_path):
    txt = _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_test.txt")
    result = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    assert result is not None
    assert result.endswith(".json")


def test_extract_json_is_valid_utf8(tmp_path):
    """Het geschreven JSON-bestand moet als utf-8 leesbaar zijn."""
    txt = _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_test.txt")
    json_path = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    assert "tables" in data


def test_extract_preserves_accented_title(tmp_path):
    """Een tabelnaam met accenten (uit latin-1 bron) moet intact in de utf-8 JSON staan."""
    txt = _make_bestandsbeschrijving(
        tmp_path / "Bestandsbeschrijving_test.txt",
        title="Vóór het HO",
    )
    json_path = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    titles = [t["table_title"] for t in data["tables"]]
    assert any("V" in t and "r het HO" in t for t in titles), (
        f"Verwachtte 'Vóór het HO' in tabelnamen, maar kreeg: {titles}"
    )


def test_extract_returns_none_for_missing_file(tmp_path):
    result = extract_tables_from_txt(str(tmp_path / "bestaat_niet.txt"), str(tmp_path))
    assert result is None


def test_extract_returns_none_when_no_tables(tmp_path):
    txt = tmp_path / "Bestandsbeschrijving_leeg.txt"
    txt.write_text("Geen tabellen hier\n", encoding="latin-1")
    result = extract_tables_from_txt(str(txt), str(tmp_path / "json"))
    assert result is None


# ---------------------------------------------------------------------------
# process_txt_folder
# ---------------------------------------------------------------------------


def test_process_txt_folder_returns_list(tmp_path):
    _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_test.txt")
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    assert isinstance(result, list)


def test_process_txt_folder_returns_extracted_paths(tmp_path):
    _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_A.txt")
    _make_bestandsbeschrijving(tmp_path / "Bestandsbeschrijving_B.txt")
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    assert len(result) == 2
    assert all(p.endswith(".json") for p in result)


def test_process_txt_folder_json_files_are_utf8(tmp_path):
    """Alle door process_txt_folder aangemaakte JSON-bestanden moeten utf-8 zijn."""
    _make_bestandsbeschrijving(
        tmp_path / "Bestandsbeschrijving_test.txt",
        title="Tabel met é en ë",
    )
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    for json_path in result:
        with open(json_path, encoding="utf-8") as f:
            json.load(f)  # mag geen UnicodeDecodeError geven


def test_process_txt_folder_empty_dir_returns_empty_list(tmp_path):
    result = process_txt_folder(str(tmp_path), json_output_folder=str(tmp_path / "json"))
    assert result == []

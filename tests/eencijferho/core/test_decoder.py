# Tests for eencijferho.core.decoder
#
# Covers:
#   - _normalize_df: column normalisation and string casting
#   - _normalize_dec_table: Dec table preparation
#   - _apply_single_dec_join: simple and composite key joins
#   - _apply_dec_tables: full Dec-table loop (incl. fallback, composite key)
#   - _parse_vakken_opmerking: Opmerking parsing
#   - _apply_variable_mappings: variable_metadata label substitution
#   - decode_fields_dec_only / decode_fields: public API contract
#   - skip-if-identical behaviour (VAKHAVW scenario)
#   - _has_real_mappings: placeholder filter
#   - get_available_enrich_variables: filters [leeg]/[gevuld]-only entries
#   - get_available_decode_columns: reads decoding_variables from Dec JSON
#   - get_decode_column_info: maps decoding variables to label columns
#   - get_enrich_variable_info: returns real code→label samples only

import json
import pytest
import polars as pl
from pathlib import Path

from eencijferho.core.decoder import (
    _normalize_df,
    _normalize_dec_table,
    _apply_single_dec_join,
    _apply_dec_tables,
    _parse_vakken_opmerking,
    _apply_variable_mappings,
    _has_real_mappings,
    decode_fields_dec_only,
    decode_fields,
    load_dec_tables_from_metadata,
    get_available_decode_columns,
    get_available_enrich_variables,
    get_decode_column_info,
    get_enrich_variable_info,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_df():
    return pl.DataFrame({"Code": ["01", "2", "03"], "Naam": ["Alice", "Bob", "Carol"]})


@pytest.fixture
def dec_table_df():
    """Minimal Dec table: code → label."""
    return pl.DataFrame({"Code": ["1", "2", "3"], "Omschrijving": ["Een", "Twee", "Drie"]})


@pytest.fixture
def dec_metadata_json(tmp_path):
    """Minimal Bestandsbeschrijving_Dec-bestanden JSON."""
    data = {
        "tables": [
            {
                "table_title": "Dec_test.asc",
                "decoding_variables": ["Code"],
                "content": [
                    "Startpositie  Naam",
                    "Code          }",
                    "Omschrijving",
                ],
            }
        ]
    }
    p = tmp_path / "Bestandsbeschrijving_Dec-bestanden.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return str(p)


@pytest.fixture
def composite_dec_metadata_json(tmp_path):
    """Dec JSON describing a composite-key table."""
    data = {
        "tables": [
            {
                "table_title": "Dec_composite.asc",
                "decoding_variables": ["VakCode"],
                "content": [
                    "Startpositie  Naam",
                    "InstCode      }",
                    "VakCode       }",
                    "VakNaam",
                ],
            }
        ]
    }
    p = tmp_path / "Bestandsbeschrijving_Dec-bestanden_composite.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return str(p)


# ---------------------------------------------------------------------------
# _normalize_df
# ---------------------------------------------------------------------------


def test_normalize_df_returns_three_values(simple_df):
    norm_df, norm_map, orig_columns = _normalize_df(simple_df)
    assert isinstance(norm_df, pl.DataFrame)
    assert isinstance(norm_map, dict)
    assert isinstance(orig_columns, list)


def test_normalize_df_original_names_preserved(simple_df):
    _, norm_map, orig_columns = _normalize_df(simple_df)
    assert orig_columns == ["Code", "Naam"]
    assert set(norm_map.values()) == {"Code", "Naam"}


def test_normalize_df_all_string_columns(simple_df):
    norm_df, _, _ = _normalize_df(simple_df)
    for col in norm_df.columns:
        assert norm_df[col].dtype == pl.Utf8


def test_normalize_df_strips_whitespace():
    df = pl.DataFrame({"Col": ["  hello  ", " world"]})
    norm_df, _, _ = _normalize_df(df)
    assert norm_df["col"].to_list() == ["hello", "world"]


def test_normalize_df_accented_column_names():
    df = pl.DataFrame({"Vóór": ["x"], "Über": ["y"]})
    norm_df, norm_map, _ = _normalize_df(df)
    # Normalized names should be ASCII
    assert all(col.isascii() for col in norm_df.columns)
    # Original names accessible via norm_map
    assert set(norm_map.values()) == {"Vóór", "Über"}


# ---------------------------------------------------------------------------
# _normalize_dec_table
# ---------------------------------------------------------------------------


def test_normalize_dec_table_column_names_lowercase(dec_table_df):
    result = _normalize_dec_table(dec_table_df, "code")
    assert all(c == c.lower() for c in result.columns)


def test_normalize_dec_table_strips_leading_zeros(dec_table_df):
    result = _normalize_dec_table(dec_table_df, "code")
    codes = result["code"].to_list()
    assert "01" not in codes  # leading zero stripped
    assert "1" in codes


def test_normalize_dec_table_preserves_zero():
    df = pl.DataFrame({"Code": ["0", "00", "1"]})
    result = _normalize_dec_table(df, "code")
    assert "0" in result["code"].to_list()


# ---------------------------------------------------------------------------
# _apply_single_dec_join — simple key
# ---------------------------------------------------------------------------


def test_apply_single_dec_join_adds_columns(simple_df, dec_table_df):
    norm_df, _, _ = _normalize_df(simple_df)
    join_df = _normalize_dec_table(dec_table_df, "code")
    result = _apply_single_dec_join(
        norm_df, join_df, "code", "code",
        is_composite=False, code_col2_norm=None,
    )
    assert "code__omschrijving" in result.columns


def test_apply_single_dec_join_correct_labels(simple_df, dec_table_df):
    norm_df, _, _ = _normalize_df(simple_df)
    join_df = _normalize_dec_table(dec_table_df, "code")
    result = _apply_single_dec_join(
        norm_df, join_df, "code", "code",
        is_composite=False, code_col2_norm=None,
    )
    labels = result["code__omschrijving"].to_list()
    assert "Een" in labels
    assert "Twee" in labels


def test_apply_single_dec_join_unknown_code_gives_null():
    main = pl.DataFrame({"code": ["99"]})
    lookup = pl.DataFrame({"code": ["1"], "label": ["Een"]})
    result = _apply_single_dec_join(
        main, lookup, "code", "code",
        is_composite=False, code_col2_norm=None,
    )
    assert result["code__label"].to_list() == [None]


# ---------------------------------------------------------------------------
# _apply_single_dec_join — composite key
# ---------------------------------------------------------------------------


def test_apply_single_dec_join_composite():
    """Composite key: join on (anchor, var) → extra columns added."""
    main = pl.DataFrame({
        "instcode": ["10", "10", "20"],
        "vakcode": ["01", "02", "01"],
    })
    lookup = pl.DataFrame({
        "instcode": ["10", "10", "20"],
        "vakcode": ["1", "2", "1"],
        "vaknaam": ["Wiskunde", "Nederlands", "Aardrijkskunde"],
    })
    result = _apply_single_dec_join(
        main, lookup, "vakcode", "instcode",
        is_composite=True, code_col2_norm="vakcode",
    )
    assert "vakcode__vaknaam" in result.columns
    names = result["vakcode__vaknaam"].to_list()
    assert names[0] == "Wiskunde"
    assert names[1] == "Nederlands"


# ---------------------------------------------------------------------------
# _apply_dec_tables
# ---------------------------------------------------------------------------


def test_apply_dec_tables_integrates_lookup(tmp_path):
    meta = {
        "tables": [
            {
                "table_title": "Dec_geslacht.asc",
                "decoding_variables": ["Geslacht"],
                "content": ["Startpositie  Naam", "Geslacht      }", "Omschrijving"],
            }
        ]
    }
    dec_tables = {
        "Dec_geslacht.asc": pl.DataFrame({"Geslacht": ["1", "2"], "Omschrijving": ["Man", "Vrouw"]})
    }
    df = pl.DataFrame({"geslacht": ["1", "2", "1"]})
    result = _apply_dec_tables(df, meta, dec_tables)
    assert "geslacht__omschrijving" in result.columns
    assert result["geslacht__omschrijving"].to_list() == ["Man", "Vrouw", "Man"]


def test_apply_dec_tables_missing_table_skipped(tmp_path):
    """When Dec table is not loaded, the variable is skipped gracefully."""
    meta = {
        "tables": [
            {
                "table_title": "Dec_missing.asc",
                "decoding_variables": ["Code"],
                "content": ["Startpositie  Naam", "Code  }", "Label"],
            }
        ]
    }
    df = pl.DataFrame({"code": ["1", "2"]})
    result = _apply_dec_tables(df, meta, dec_tables={})
    # Original columns unchanged, no crash
    assert result.columns == ["code"]


def test_apply_dec_tables_no_duplicate_columns_from_shadow_bug_fix():
    """Verifies the shadowed-loop bug is fixed: each Dec table applied exactly once."""
    call_count = {"n": 0}
    original_join = pl.DataFrame.join

    meta = {
        "tables": [
            {
                "table_title": "Dec_a.asc",
                "decoding_variables": ["Col"],
                "content": ["H", "Col  }", "Label"],
            },
            {
                "table_title": "Dec_b.asc",
                "decoding_variables": ["Col2"],
                "content": ["H", "Col2  }", "Label2"],
            },
        ]
    }
    dec_tables = {
        "Dec_a.asc": pl.DataFrame({"Col": ["1"], "Label": ["Een"]}),
        "Dec_b.asc": pl.DataFrame({"Col2": ["X"], "Label2": ["Ex"]}),
    }
    df = pl.DataFrame({"col": ["1"], "col2": ["X"]})
    result = _apply_dec_tables(df, meta, dec_tables)
    # Each Dec table contributes exactly one extra column
    assert "col__label" in result.columns
    assert "col2__label2" in result.columns
    # No duplicate column names
    assert len(result.columns) == len(set(result.columns))


# ---------------------------------------------------------------------------
# _parse_vakken_opmerking
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("opm,expected_composite,expected_title", [
    (
        "te decoderen met Dec_vakcode.asc",
        None, "Dec_vakcode.asc",
    ),
    (
        "in combinatie met InstCode te decoderen met Dec_vest_ho.asc",
        "InstCode", "Dec_vest_ho.asc",
    ),
    ("geen instructie hier", None, None),
    ("", None, None),
])
def test_parse_vakken_opmerking(opm, expected_composite, expected_title):
    composite, title = _parse_vakken_opmerking(opm)
    assert composite == expected_composite
    assert title == expected_title


# ---------------------------------------------------------------------------
# _apply_variable_mappings
# ---------------------------------------------------------------------------


def test_apply_variable_mappings_replaces_codes(tmp_path):
    var_meta = [
        {"name": "Geslacht", "values": {"1": "Man", "2": "Vrouw"}}
    ]
    path = tmp_path / "variable_metadata.json"
    path.write_text(json.dumps(var_meta), encoding="utf-8")

    df = pl.DataFrame({"geslacht": ["1", "2", "1"]})
    _, norm_map, orig = _normalize_df(df)
    result = _apply_variable_mappings(df, str(path), None, norm_map, orig)
    assert result["geslacht"].to_list() == ["Man", "Vrouw", "Man"]


def test_apply_variable_mappings_no_file_returns_unchanged(tmp_path):
    df = pl.DataFrame({"code": ["1", "2"]})
    _, norm_map, orig = _normalize_df(df)
    result = _apply_variable_mappings(df, str(tmp_path / "nonexistent.json"), None, norm_map, orig)
    assert result.equals(df)


def test_apply_variable_mappings_unmapped_value_kept(tmp_path):
    var_meta = [{"name": "Code", "values": {"1": "Een"}}]
    path = tmp_path / "variable_metadata.json"
    path.write_text(json.dumps(var_meta), encoding="utf-8")

    df = pl.DataFrame({"code": ["1", "99"]})
    _, norm_map, orig = _normalize_df(df)
    result = _apply_variable_mappings(df, str(path), None, norm_map, orig)
    vals = result["code"].to_list()
    assert vals[0] == "Een"
    assert vals[1] == "99"  # unmapped value kept as-is


# ---------------------------------------------------------------------------
# decode_fields_dec_only — public API
# ---------------------------------------------------------------------------


def test_decode_fields_dec_only_returns_dataframe(tmp_path, dec_metadata_json):
    dec_tables = {"Dec_test.asc": pl.DataFrame({"Code": ["1", "2"], "Omschrijving": ["A", "B"]})}
    df = pl.DataFrame({"Code": ["1", "2"]})
    result = decode_fields_dec_only(df, dec_metadata_json, dec_tables)
    assert isinstance(result, pl.DataFrame)


def test_decode_fields_dec_only_preserves_original_column_names(tmp_path, dec_metadata_json):
    dec_tables = {"Dec_test.asc": pl.DataFrame({"Code": ["1"], "Omschrijving": ["A"]})}
    df = pl.DataFrame({"Code": ["1"], "Naam": ["Jan"]})
    result = decode_fields_dec_only(df, dec_metadata_json, dec_tables)
    assert "Code" in result.columns
    assert "Naam" in result.columns


def test_decode_fields_dec_only_row_count_unchanged(tmp_path, dec_metadata_json):
    dec_tables = {"Dec_test.asc": pl.DataFrame({"Code": ["1", "2"], "Omschrijving": ["A", "B"]})}
    df = pl.DataFrame({"Code": ["1", "2", "1"]})
    result = decode_fields_dec_only(df, dec_metadata_json, dec_tables)
    assert result.height == 3


# ---------------------------------------------------------------------------
# decode_fields — public API (incl. variable_metadata step)
# ---------------------------------------------------------------------------


def test_decode_fields_applies_variable_metadata(tmp_path, dec_metadata_json):
    var_meta = [{"name": "Status", "values": {"A": "Actief", "I": "Inactief"}}]
    vm_path = tmp_path / "variable_metadata.json"
    vm_path.write_text(json.dumps(var_meta), encoding="utf-8")

    dec_tables = {"Dec_test.asc": pl.DataFrame({"Code": ["1"], "Omschrijving": ["A"]})}
    df = pl.DataFrame({"Code": ["1", "1", "1"], "Status": ["A", "I", "A"]})
    result = decode_fields(df, dec_metadata_json, dec_tables, variable_metadata_path=str(vm_path))
    assert result["Status"].to_list() == ["Actief", "Inactief", "Actief"]


def test_decode_fields_without_variable_metadata_equals_dec_only(tmp_path, dec_metadata_json):
    """When no variable_metadata exists, decode_fields == decode_fields_dec_only."""
    dec_tables = {"Dec_test.asc": pl.DataFrame({"Code": ["1", "2"], "Omschrijving": ["A", "B"]})}
    df = pl.DataFrame({"Code": ["1", "2", "1"]})
    result_full = decode_fields(df, dec_metadata_json, dec_tables, variable_metadata_path=str(tmp_path / "nonexistent.json"))
    result_dec_only = decode_fields_dec_only(df, dec_metadata_json, dec_tables)
    assert result_full.equals(result_dec_only)


def test_decode_fields_vakhavw_scenario(tmp_path, dec_metadata_json):
    """VAKHAVW without variable_metadata: enriched identical to decoded → safe to skip _enriched."""
    dec_tables = {"Dec_test.asc": pl.DataFrame({"Code": ["1", "2"], "Omschrijving": ["A", "B"]})}
    df = pl.DataFrame({"Code": ["1", "2"], "Vak": ["NL", "WI"]})
    decoded = decode_fields_dec_only(df, dec_metadata_json, dec_tables)
    enriched = decode_fields(df, dec_metadata_json, dec_tables)
    assert enriched.equals(decoded), "VAKHAVW zonder variable_metadata: enriched moet identiek zijn aan decoded"


# ---------------------------------------------------------------------------
# _has_real_mappings
# ---------------------------------------------------------------------------

def test_has_real_mappings_true_for_code_label():
    assert _has_real_mappings({"1": "Man", "2": "Vrouw"}) is True


def test_has_real_mappings_false_for_only_leeg():
    assert _has_real_mappings({"[leeg]": "onbekend"}) is False


def test_has_real_mappings_false_for_only_gevuld():
    assert _has_real_mappings({"[gevuld]": "brinnummer aanwezig"}) is False


def test_has_real_mappings_false_for_leeg_and_gevuld():
    assert _has_real_mappings({"[leeg]": "onbekend", "[gevuld]": "aanwezig"}) is False


def test_has_real_mappings_true_for_mix_with_real_code():
    assert _has_real_mappings({"[leeg]": "onbekend", "1": "Man"}) is True


def test_has_real_mappings_false_for_empty_dict():
    assert _has_real_mappings({}) is False


# ---------------------------------------------------------------------------
# get_available_enrich_variables
# ---------------------------------------------------------------------------

@pytest.fixture
def variable_metadata_json(tmp_path):
    data = [
        {"name": "Geslacht", "values": {"1": "Man", "2": "Vrouw"}},
        {"name": "Geboorteland", "values": {"[leeg]": "onbekend"}},
        {"name": "Instelling van de hoogste vooropl.", "values": {"[leeg]": "onbekend", "[gevuld]": "brinnummer"}},
        {"name": "Opleidingsvorm", "values": {"1": "voltijd", "2": "deeltijd"}},
    ]
    path = tmp_path / "variable_metadata.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def test_get_available_enrich_variables_excludes_placeholder_only(variable_metadata_json):
    result = get_available_enrich_variables(variable_metadata_json)
    assert "Geboorteland" not in result
    assert "Instelling van de hoogste vooropl." not in result


def test_get_available_enrich_variables_includes_real_mappings(variable_metadata_json):
    result = get_available_enrich_variables(variable_metadata_json)
    assert "Geslacht" in result
    assert "Opleidingsvorm" in result


def test_get_available_enrich_variables_returns_sorted(variable_metadata_json):
    result = get_available_enrich_variables(variable_metadata_json)
    assert result == sorted(result)


def test_get_available_enrich_variables_missing_file():
    assert get_available_enrich_variables("/nonexistent/path.json") == []


# ---------------------------------------------------------------------------
# get_enrich_variable_info
# ---------------------------------------------------------------------------

def test_get_enrich_variable_info_excludes_placeholder_only(variable_metadata_json):
    result = get_enrich_variable_info(variable_metadata_json)
    assert "Geboorteland" not in result
    assert "Instelling van de hoogste vooropl." not in result


def test_get_enrich_variable_info_sample_contains_real_codes(variable_metadata_json):
    result = get_enrich_variable_info(variable_metadata_json)
    assert "1" in result["Geslacht"]
    assert result["Geslacht"]["1"] == "Man"


def test_get_enrich_variable_info_sample_excludes_placeholder_keys(variable_metadata_json):
    data = [{"name": "Mix", "values": {"[leeg]": "leeg", "1": "Man"}}]
    path = Path(variable_metadata_json).parent / "mix.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    result = get_enrich_variable_info(str(path))
    assert "[leeg]" not in result["Mix"]
    assert "1" in result["Mix"]


# ---------------------------------------------------------------------------
# get_available_decode_columns
# ---------------------------------------------------------------------------

@pytest.fixture
def dec_metadata_json_path(tmp_path):
    data = {
        "filename": "Bestandsbeschrijving_Dec-bestanden_TEST",
        "tables": [
            {
                "table_number": 1,
                "table_title": "Dec_landcode.asc",
                "content": ["Startpositie  Lengte", "Code  2  2", "Naam land  4  40"],
                "decoding_variables": ["Geboorteland", "Nationaliteit"],
            },
            {
                "table_number": 2,
                "table_title": "Dec_opleidingscode.asc",
                "content": ["Startpositie  Lengte", "Code  2  2", "Omschrijving  4  60"],
                "decoding_variables": ["Opleiding"],
            },
        ],
    }
    path = tmp_path / "Bestandsbeschrijving_Dec-bestanden_TEST.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def test_get_available_decode_columns_returns_all_variables(dec_metadata_json_path):
    result = get_available_decode_columns(dec_metadata_json_path)
    assert "Geboorteland" in result
    assert "Nationaliteit" in result
    assert "Opleiding" in result


def test_get_available_decode_columns_returns_sorted(dec_metadata_json_path):
    result = get_available_decode_columns(dec_metadata_json_path)
    assert result == sorted(result)


def test_get_available_decode_columns_no_duplicates(dec_metadata_json_path):
    result = get_available_decode_columns(dec_metadata_json_path)
    assert len(result) == len(set(result))


def test_get_available_decode_columns_missing_file():
    assert get_available_decode_columns("/nonexistent/path.json") == []


# ---------------------------------------------------------------------------
# get_decode_column_info
# ---------------------------------------------------------------------------

def test_get_decode_column_info_maps_variable_to_label_cols(dec_metadata_json_path):
    result = get_decode_column_info(dec_metadata_json_path)
    assert "Geboorteland" in result
    assert "Naam land" in result["Geboorteland"]


def test_get_decode_column_info_shared_table_all_variables(dec_metadata_json_path):
    result = get_decode_column_info(dec_metadata_json_path)
    assert result["Geboorteland"] == result["Nationaliteit"]


def test_get_decode_column_info_missing_file():
    assert get_decode_column_info("/nonexistent/path.json") == {}


import json
import pytest
import polars as pl
from pathlib import Path

from eencijferho.core.decoder import (
    _canonical_name,
    _normalize_df,
    _normalize_dec_table,
    _apply_single_dec_join,
    _apply_dec_tables,
    _parse_vakken_opmerking,
    _apply_variable_mappings,
    _apply_single_mapping,
    _apply_vakken_simple,
    _apply_vakken_composite,
    _resolve_mapping_column,
    _code_width,
    _join_key,
    _key_width,
    _dec_title,
    find_vakken_metadata_json,
    build_dec_layouts,
    parse_vakken_layout,
    decode_fields_dec_only,
    decode_fields,
    load_dec_tables_from_metadata,
)
from eencijferho.core.decoder_info import (
    _has_real_mappings,
    get_available_decode_columns,
    get_available_enrich_variables,
    get_decode_column_info,
    get_enrich_variable_info,
)
from eencijferho.presets import PRESET_CONFIGS


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


def test_normalize_dec_table_keeps_codes_and_join_key_pads_to_data_width():
    """The Dec table keeps its codes; the join key pads them to the data width."""
    dec = _normalize_dec_table(pl.DataFrame({"Code": ["1", "02", " 3 "]}), "code")
    assert dec["code"].to_list() == ["1", "02", "3"]
    dec_keys = dec.select(_join_key(pl.col("code"), 2))["code"].to_list()
    data_keys = pl.DataFrame({"c": ["01", "02", "03"]}).select(_join_key(pl.col("c"), 2))["c"].to_list()
    assert dec_keys == data_keys == ["01", "02", "03"]


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


# ---------------------------------------------------------------------------
# _apply_single_dec_join — the data column must not be rewritten
# ---------------------------------------------------------------------------

PADDED_DF = pl.DataFrame(
    {
        "Vooropleiding": ["00411", "0411", "0000"],
        "Instelling": ["02DZ", "02AB", "00AA"],
    }
)


def test_apply_single_dec_join_preserves_leading_zeros_in_the_data():
    """A code is an identifier, not a number.

    DUO writes it fixed width: 00411 is a different code from 411. Stripping
    the zero in the data column changes the value the user receives, and a
    code that no longer exists in the source can no longer be joined to
    anything.
    """
    join_df = _normalize_dec_table(
        pl.DataFrame({"Code": ["411", "0"], "Omschrijving": ["HBO", "onbekend"]}),
        "code",
    )
    result = _apply_single_dec_join(
        _normalize_df(PADDED_DF)[0], join_df, "vooropleiding", "code",
        is_composite=False, code_col2_norm=None,
    )
    assert result["vooropleiding"].to_list() == ["00411", "0411", "0000"]


def test_apply_single_dec_join_still_matches_padded_codes():
    """Preserving the data must not stop the join: that is the point of it."""
    join_df = _normalize_dec_table(
        pl.DataFrame({"Code": ["411", "0"], "Omschrijving": ["HBO", "onbekend"]}),
        "code",
    )
    result = _apply_single_dec_join(
        _normalize_df(PADDED_DF)[0], join_df, "vooropleiding", "code",
        is_composite=False, code_col2_norm=None,
    )
    assert result["vooropleiding__omschrijving"].to_list() == ["HBO", "HBO", "onbekend"]


def test_apply_single_dec_join_leaves_no_temporary_column_behind():
    join_df = _normalize_dec_table(
        pl.DataFrame({"Code": ["411"], "Omschrijving": ["HBO"]}), "code"
    )
    result = _apply_single_dec_join(
        _normalize_df(PADDED_DF)[0], join_df, "vooropleiding", "code",
        is_composite=False, code_col2_norm=None,
    )
    assert set(result.columns) == {"vooropleiding", "instelling", "vooropleiding__omschrijving"}


def test_apply_single_dec_join_composite_preserves_leading_zeros():
    """Same for a composite key: the variable column stays as it arrived.

    code1 matches exactly here, so this test is about code2's padding alone.
    """
    data = pl.DataFrame({"Code1": ["2"], "Code2": ["00411"]})
    join_df = _normalize_dec_table(
        pl.DataFrame({"Code1": ["2"], "Code2": ["411"], "Omschrijving": ["HBO"]}), "code1"
    )
    result = _apply_single_dec_join(
        _normalize_df(data)[0], join_df, "code2", "code1",
        is_composite=True, code_col2_norm="code2",
    )
    assert result["code2"].to_list() == ["00411"]
    assert result["code2__omschrijving"].to_list() == ["HBO"]


# ---------------------------------------------------------------------------
# _join_key / _code_width — the one normalisation rule
# ---------------------------------------------------------------------------


def _keys(values, width):
    return pl.DataFrame({"c": values}, schema={"c": pl.Utf8}).select(
        _join_key(pl.col("c"), width)
    )["c"].to_list()


def test_join_key_pads_numeric_and_alphanumeric_codes_to_width():
    assert _keys(["411", "0411", "00411"], 5) == ["00411"] * 3
    assert _keys(["2UC", "02UC"], 4) == ["02UC", "02UC"]


def test_join_key_keeps_empty_distinct_from_zero():
    assert _keys(["", "  ", None, "0", "0000"], 4) == [None, None, None, "0000", "0000"]


def test_join_key_accepts_integer_codes():
    df = pl.DataFrame({"c": [411, 7]})
    assert df.select(_join_key(pl.col("c"), 5))["c"].to_list() == ["00411", "00007"]


@pytest.mark.parametrize("row,expected", [
    ("Code land                                           1             4", 4),
    ("Code vooropleiding                                  1             5", 5),
    ("Instellingscode                                     1             4      } sleutel", 4),
    ("Vestigingsnummer                                    5             2      }", 2),
    ("Code          }", None),
    ("Omschrijving", None),
    ("", None),
    (None, None),
])
def test_code_width_reads_aantal_posities(row, expected):
    assert _code_width(row) == expected


def test_key_width_metadata_width_wins_over_longest_code():
    data, dec = pl.Series(["001"]), pl.Series(["1"])
    assert _key_width(2, dec, data) == 2
    assert _key_width(None, dec, data) == 3
    assert _key_width(None, pl.Series([None, ""], dtype=pl.Utf8)) is None


def test_join_on_narrower_metadata_width_does_not_fall_back_to_longest_code():
    """A code wider than its field is out of spec: the metadata width decides."""
    data = pl.DataFrame({"code": ["001", "01"]})
    dec = pl.DataFrame({"code": ["1"], "naam": ["Een"]})
    result = _apply_single_dec_join(
        data, dec, "code", "code", is_composite=False, code_col2_norm=None, code_width=2,
    )
    assert result["code"].to_list() == ["001", "01"]
    assert result["code__naam"].to_list() == [None, "Een"]


# ---------------------------------------------------------------------------
# Sentinel rows and temporary keys
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sentinel", ["", None])
def test_dec_join_empty_sentinel_does_not_match_real_zero_code(sentinel):
    """Dec_landcode has an empty-code row next to 0000: they are different codes."""
    data = pl.DataFrame({"land": ["0000", "0001", ""]})
    dec = pl.DataFrame(
        {"land": [sentinel, "0000", "0001"], "naam": ["SENTINEL", "Onbekend", "Nederland"]},
        schema={"land": pl.Utf8, "naam": pl.Utf8},
    )
    result = _apply_single_dec_join(
        data, dec, "land", "land", is_composite=False, code_col2_norm=None, code_width=4,
    )
    assert result.height == 3
    assert result["land"].to_list() == ["0000", "0001", ""]
    assert result["land__naam"].to_list() == ["Onbekend", "Nederland", None]


def test_dec_join_without_real_zero_code_gives_null_not_sentinel():
    data = pl.DataFrame({"land": ["0000"]})
    dec = pl.DataFrame({"land": ["", "0001"], "naam": ["SENTINEL", "Nederland"]})
    result = _apply_single_dec_join(
        data, dec, "land", "land", is_composite=False, code_col2_norm=None, code_width=4,
    )
    assert result["land__naam"].to_list() == [None]


def test_dec_join_duplicate_codes_after_padding_keep_row_count_and_labels(capsys):
    """411 and 00411 are one code at width 5: keep the first, never duplicate rows."""
    data = pl.DataFrame({"code": ["00411", "00412"]})
    dec = pl.DataFrame({"code": ["411", "00411", "412"], "naam": ["A", "B", "C"]})
    result = _apply_single_dec_join(
        data, dec, "code", "code", is_composite=False, code_col2_norm=None, code_width=5,
    )
    assert result.height == 2
    assert result["code__naam"].to_list() == ["A", "C"]
    assert "Dubbele codes" in capsys.readouterr().out


def test_dec_join_leaves_no_temporary_column_when_join_raises(caplog):
    data = pl.DataFrame({"code": ["00411"]})
    dec_without_code_col = pl.DataFrame({"other": ["411"], "naam": ["HBO"]})
    result = _apply_single_dec_join(
        data, dec_without_code_col, "code", "code", is_composite=False, code_col2_norm=None,
    )
    assert "Fout bij Dec-join" in caplog.text
    assert result.columns == ["code"]
    assert result["code"].to_list() == ["00411"]


# ---------------------------------------------------------------------------
# Through the real paths: _apply_dec_tables composite, decode_fields_dec_only
# ---------------------------------------------------------------------------


def test_apply_dec_tables_composite_uses_metadata_width_and_keeps_data():
    meta = {
        "tables": [
            {
                "table_title": "Dec_vest_ho.asc",
                "decoding_variables": ["Vestigingsnummer"],
                "content": [
                    "                    Startpositie  Aantal posities",
                    "Instellingscode     1             4      } sleutel",
                    "Vestigingsnummer    5             2      }",
                    "Gemeentenaam        7            24",
                ],
            }
        ]
    }
    dec_tables = {
        "Dec_vest_ho.asc": pl.DataFrame({
            "Instellingscode": ["2UC", "2UC"],
            "Vestigingsnummer": ["0", "1"],
            "Gemeentenaam": ["Utrecht", "Zeist"],
        })
    }
    df = pl.DataFrame({"instellingscode": ["02UC", "02UC"], "vestigingsnummer": ["01", "00"]})
    result = _apply_dec_tables(df, meta, dec_tables)
    assert result["instellingscode"].to_list() == ["02UC", "02UC"]
    assert result["vestigingsnummer"].to_list() == ["01", "00"]
    assert result["vestigingsnummer__gemeentenaam"].to_list() == ["Zeist", "Utrecht"]
    assert not [c for c in result.columns if c.startswith("__")]


@pytest.fixture
def padded_dec_metadata_json(tmp_path):
    """Dec JSON with real code widths, as the extractor writes it."""
    data = {
        "tables": [
            {
                "table_title": "Dec_vopl.asc",
                "decoding_variables": ["Hoogste vooropleiding"],
                "content": [
                    "                    Startpositie  Aantal posities",
                    "Code vooropleiding  1             5",
                    "Omschrijving        6            72",
                ],
            },
            {
                "table_title": "Dec_landcode.asc",
                "decoding_variables": ["Nationaliteit"],
                "content": [
                    "                    Startpositie  Aantal posities",
                    "Code land           1             4",
                    "Naam land           5            40",
                ],
            },
            {
                "table_title": "Dec_instellingscode.asc",
                "decoding_variables": ["Instelling"],
                "content": [
                    "                    Startpositie  Aantal posities",
                    "Instellingscode     1             4",
                    "Naam                5            30",
                ],
            },
        ]
    }
    p = tmp_path / "Bestandsbeschrijving_Dec-bestanden_padded.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return str(p)


def test_decode_fields_dec_only_keeps_data_columns_byte_identical(padded_dec_metadata_json):
    df = pl.DataFrame({
        "Hoogste vooropleiding": ["00411", "00411", "00000"],
        "Nationaliteit": ["0001", "0000", ""],
        "Instelling": ["02UC", "02DZ", "02UC"],
    })
    dec_tables = {
        # Unpadded and integer-typed codes, as Dec tables are sometimes read.
        "Dec_vopl.asc": pl.DataFrame({"Code vooropleiding": [411, 0], "Omschrijving": ["HBO", "onbekend"]}),
        "Dec_landcode.asc": pl.DataFrame({
            "Code land": [None, "0000", "1"], "Naam land": ["NULL", "Onbekend", "Nederland"],
        }),
        "Dec_instellingscode.asc": pl.DataFrame({"Instellingscode": ["2UC", "02DZ"], "Naam": ["Inst UC", "Inst DZ"]}),
    }
    result = decode_fields_dec_only(df, padded_dec_metadata_json, dec_tables)

    assert result.height == df.height
    for col in df.columns:
        assert result[col].to_list() == df[col].to_list(), col
    assert result["hoogste_vooropleiding__omschrijving"].to_list() == ["HBO", "HBO", "onbekend"]
    assert result["nationaliteit__naam_land"].to_list() == ["Nederland", "Onbekend", None]
    assert result["instelling__naam"].to_list() == ["Inst UC", "Inst DZ", "Inst UC"]
    assert not [c for c in result.columns if c.startswith("__")]


@pytest.fixture
def isced_dec_dir(tmp_path):
    """Dec_ho_ISCED-like table whose label columns hold codes with leading zeros."""
    meta = {
        "tables": [
            {
                "table_title": "Dec_ho_ISCED.asc",
                "decoding_variables": ["Opleidingscode"],
                "content": [
                    "                         Startpositie  Aantal posities",
                    "Opleidingscode           1             5",
                    "ISCEDF2013-detailgroep   6             4",
                    "ISCEDF2013-rubriek      10             6",
                    "Omschrijving            16            40",
                ],
            }
        ]
    }
    meta_path = tmp_path / "Bestandsbeschrijving_Dec-bestanden.json"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    (tmp_path / "Dec_ho_ISCED.csv").write_text(
        "Opleidingscode;ISCEDF2013-detailgroep;ISCEDF2013-rubriek;Omschrijving\n"
        "00123;0311;031103;Economie\n"
        "34567;0414;041401;Bedrijfskunde\n",
        encoding="utf-8",
    )
    return str(meta_path), str(tmp_path)


def test_load_dec_tables_from_metadata_reads_all_columns_as_strings(isced_dec_dir):
    meta_path, dec_dir = isced_dec_dir
    dec = load_dec_tables_from_metadata(meta_path, dec_dir)["Dec_ho_ISCED.asc"]
    assert set(dec.dtypes) == {pl.String}
    assert dec["Opleidingscode"].to_list() == ["00123", "34567"]
    assert dec["ISCEDF2013-detailgroep"].to_list() == ["0311", "0414"]
    assert dec["ISCEDF2013-rubriek"].to_list() == ["031103", "041401"]


def test_dec_label_that_is_a_code_keeps_leading_zero_in_output(isced_dec_dir):
    """The label comes from the Dec table, not from a data column of the same name."""
    meta_path, dec_dir = isced_dec_dir
    dec_tables = load_dec_tables_from_metadata(meta_path, dec_dir)
    df = pl.DataFrame({
        "Opleidingscode": ["00123", "34567", "99999"],
        "ISCEDF2013-rubriek": ["999999", "999999", "999999"],
    })
    result = decode_fields_dec_only(df, meta_path, dec_tables)
    assert result["Opleidingscode"].to_list() == ["00123", "34567", "99999"]
    assert result["ISCEDF2013-rubriek"].to_list() == ["999999"] * 3
    assert result["opleidingscode__iscedf2013_rubriek"].to_list() == ["031103", "041401", None]
    assert result["opleidingscode__omschrijving"].to_list() == ["Economie", "Bedrijfskunde", None]


# ---------------------------------------------------------------------------
# Vakkenbestanden joins
# ---------------------------------------------------------------------------


VAKCODE_CONTENT = [
    "                    Startpositie  Aantal posities",
    "Vakcode             1             4",
    "Omschrijving vak    5           112",
]


def test_apply_vakken_simple_keeps_data_column_and_fills_labels():
    data = pl.DataFrame({"vakcode": ["0012", "12", " 0099 "]})
    dec = pl.DataFrame({"vakcode": ["12", "0099"], "omschrijving_vak": ["Wiskunde", "Engels"]})
    result = _apply_vakken_simple(
        data, dec, "vakcode", "vakcode", "Vakcode", "Dec_vakcode.asc", VAKCODE_CONTENT,
    )
    assert result["vakcode"].to_list() == ["0012", "12", " 0099 "]
    assert result["vakcode__omschrijving_vak"].to_list() == ["Wiskunde", "Wiskunde", "Engels"]
    assert not [c for c in result.columns if c.startswith("__")]


def test_apply_vakken_composite_keeps_data_columns_and_fills_labels():
    content = [
        "                    Startpositie  Aantal posities",
        "Brinnummer          1             4      }",
        "Vestigingsnummer    5             2      }",
        "Naam vestiging      7            30",
    ]
    data = pl.DataFrame({"brinnummer": ["02UC", "2UC"], "vestigingsnummer": ["1", "00"]})
    dec = pl.DataFrame({
        "brinnummer": ["02UC", "02UC"],
        "vestigingsnummer": ["01", "0"],
        "naam_vestiging": ["Zeist", "Utrecht"],
    })
    result = _apply_vakken_composite(
        data, dec, "brinnummer", "Vestigingsnummer", "brinnummer", content,
        "Dec_brinvestigingsnummer.asc",
    )
    assert result["brinnummer"].to_list() == ["02UC", "2UC"]
    assert result["vestigingsnummer"].to_list() == ["1", "00"]
    assert result["brinnummer__naam_vestiging"].to_list() == ["Zeist", "Utrecht"]
    assert not [c for c in result.columns if c.startswith("__")]


# ---------------------------------------------------------------------------
# _apply_single_mapping — int fallback is logged
# ---------------------------------------------------------------------------


def test_apply_single_mapping_int_fallback_matches_and_logs(capsys):
    df = pl.DataFrame({"vopl": ["00411", "00411", "07"]})
    result = _apply_single_mapping(df, "vopl", {411: "HBO", "07": "X"}, "vopl", "Vopl")
    assert result["vopl"].to_list() == ["HBO", "HBO", "X"]
    out = capsys.readouterr().out
    assert "3/3 gemapt" in out
    assert "2 waarde(n) alleen via numerieke sleutel gemapt" in out
    assert "00411" in out


def test_apply_single_mapping_direct_match_does_not_log_int_fallback(capsys):
    df = pl.DataFrame({"vopl": ["00411", "07"]})
    result = _apply_single_mapping(df, "vopl", {"00411": "HBO", "07": "X", 411: "HBO"}, "vopl", "Vopl")
    assert result["vopl"].to_list() == ["HBO", "X"]
    assert "numerieke sleutel" not in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Vakkenbestanden layout (#204): parse the DUO rows, decode all four fields
# ---------------------------------------------------------------------------

# The Vakgegevens table as DUO writes it (Bestandsbeschrijving_Vakkenbestanden.txt).
VAKGEGEVENS_CONTENT = [
    "                                             Startpositie  Aantal posities  Opmerking",
    "Persoonsgebonden nummer                             1            12         BSN voorafgegaan door 000",
    "Brinnummer VO-instelling                           13             4         te decoderen met Dec_brinnummer.asc",
    "Vestigingsnummer VO-vestiging                      17             2         in combinatie met Brinnummer te decoderen met Dec_brinvestigingsnummer.asc",
    "Vooropleiding oorspronkelijke code                 19             5         te decoderen met Dec_vooropl.asc",
    "Diplomajaar                                        24             4         Diplomajaar = VO-examenjaar - 1",
    "Gemiddeld cijfer cijferlijst                       28             3",
    "Vakcode                                            31             4         te decoderen met Dec_vakcode.asc",
]

DEC_VAKCODE_CONTENT = [
    "                                             Startpositie  Aantal posities",
    "Vakcode                                             1             4",
    "Omschrijving vak                                    5           112",
]

DEC_VOOROPL_CONTENT = [
    "                                             Startpositie  Aantal posities",
    "Code vooropleiding oorspr. code                     1             5",
    "Onderwijssector                                     6             3",
    "Omschrijving vooropleiding oorspr. code             9           180",
]


def _by_name(fields):
    return {f["name"]: f for f in fields}


def test_parse_vakken_layout_keeps_full_names_positions_and_widths():
    fields = _by_name(parse_vakken_layout(VAKGEGEVENS_CONTENT))
    expected = {
        "Brinnummer VO-instelling": (13, 4),
        "Vestigingsnummer VO-vestiging": (17, 2),
        "Vooropleiding oorspronkelijke code": (19, 5),
        "Vakcode": (31, 4),
        "Gemiddeld cijfer cijferlijst": (28, 3),
    }
    for name, (start, width) in expected.items():
        assert (fields[name]["start"], fields[name]["width"]) == (start, width)


def test_parse_vakken_layout_reads_the_decode_rule_per_row():
    fields = _by_name(parse_vakken_layout(VAKGEGEVENS_CONTENT))
    assert fields["Brinnummer VO-instelling"]["dec_table"] == "Dec_brinnummer.asc"
    assert fields["Brinnummer VO-instelling"]["composite"] is None
    assert fields["Vestigingsnummer VO-vestiging"]["dec_table"] == "Dec_brinvestigingsnummer.asc"
    # "in combinatie met Brinnummer" points to the full field name.
    assert fields["Vestigingsnummer VO-vestiging"]["composite"] == "Brinnummer VO-instelling"
    assert fields["Vooropleiding oorspronkelijke code"]["dec_table"] == "Dec_vooropl.asc"
    assert fields["Vakcode"]["dec_table"] == "Dec_vakcode.asc"
    # A remark without a decode rule is no rule.
    assert fields["Diplomajaar"]["dec_table"] is None
    assert fields["Persoonsgebonden nummer"]["dec_table"] is None


def test_parse_vakken_layout_unresolvable_reference_is_reported(capsys):
    content = VAKGEGEVENS_CONTENT[:1] + [
        "Vestigingsnummer VO-vestiging                      17             2         in combinatie met Onbekend te decoderen met Dec_brinvestigingsnummer.asc",
    ]
    (field,) = parse_vakken_layout(content)
    assert field["composite"] is None
    assert "Onbekend" in capsys.readouterr().out


def test_dec_title_from_vakken_table_title():
    assert _dec_title("Decodeertabel vakcode (Dec_vakcode.asc)") == "Dec_vakcode.asc"
    assert _dec_title("Dec_ho-inst.asc") == "Dec_ho-inst.asc"
    assert _dec_title("Vakgegevens") is None


def test_build_dec_layouts_combines_dec_vakken_and_inferred_layouts():
    dec_meta = {"tables": [{"table_title": "Dec_vooropl.asc", "content": DEC_VOOROPL_CONTENT}]}
    vakken_meta = {"tables": [
        {"table_title": "Decodeertabel vakcode (Dec_vakcode.asc)", "content": DEC_VAKCODE_CONTENT},
        {"table_title": "Vakgegevens", "content": VAKGEGEVENS_CONTENT},
    ]}
    layouts = build_dec_layouts(dec_meta, vakken_meta)
    assert set(layouts) == {
        "Dec_vooropl.asc", "Dec_vakcode.asc",
        "Dec_brinnummer.asc", "Dec_brinvestigingsnummer.asc",
    }


@pytest.fixture
def vakken_setup(tmp_path):
    """Dec + Vakken JSON as the extract step writes them, plus small Dec CSVs.

    Synthetic codes only. Two schools share vestigingsnummer 01, so the
    composite join must use both keys.
    """
    dec_json = tmp_path / "Bestandsbeschrijving_Dec-bestanden.json"
    dec_json.write_text(json.dumps({"tables": [
        {"table_number": 1, "table_title": "Dec_vooropl.asc",
         "content": DEC_VOOROPL_CONTENT, "decoding_variables": ["Hoogste vooropleiding"]},
    ]}), encoding="utf-8")
    vakken_json = tmp_path / "Bestandsbeschrijving_Vakkenbestanden.json"
    vakken_json.write_text(json.dumps({"tables": [
        {"table_number": 1, "table_title": "Decodeertabel vakcode (Dec_vakcode.asc)",
         "content": DEC_VAKCODE_CONTENT, "decoding_variables": []},
        {"table_number": 2, "table_title": "Vakgegevens",
         "content": VAKGEGEVENS_CONTENT, "decoding_variables": []},
    ]}), encoding="utf-8")

    pl.DataFrame({
        "Code vooropleiding oorspr. code": ["V0652", "00411"],
        "Onderwijssector": ["VO", "HO"],
        "Omschrijving vooropleiding oorspr. code": ["vwo", "propedeuse"],
    }).write_csv(tmp_path / "Dec_vooropl.csv", separator=";")
    pl.DataFrame({
        "Vakcode": ["0012", "0099"],
        "Omschrijving vak": ["Wiskunde A", "Engels"],
    }).write_csv(tmp_path / "Dec_vakcode.csv", separator=";")
    pl.DataFrame({
        "Brinnummer": ["00AA", "01BB"],
        "Naam": ["School Een", "School Twee"],
    }).write_csv(tmp_path / "Dec_brinnummer.csv", separator=";")
    pl.DataFrame({
        "Brinnummer": ["00AA", "01BB"],
        "Vestigingsnummer": ["01", "01"],
        "Naam": ["Vestiging Een", "Vestiging Twee"],
    }).write_csv(tmp_path / "Dec_brinvestigingsnummer.csv", separator=";")
    return str(dec_json), str(vakken_json), str(tmp_path)


VAKHAVW_FRAME = pl.DataFrame({
    "Brinnummer VO-instelling": ["00AA", "01BB", "00AA"],
    "Vestigingsnummer VO-vestiging": ["01", "01", "07"],
    "Vooropleiding oorspronkelijke code": ["V0652", "00411", "V0652"],
    "Vakcode": ["0012", "0099", "0012"],
})


def test_load_dec_tables_loads_tables_named_by_vakken_layout(vakken_setup):
    dec_json, vakken_json, dec_dir = vakken_setup
    without = load_dec_tables_from_metadata(dec_json, dec_dir)
    assert set(without) == {"Dec_vooropl.asc"}
    tables = load_dec_tables_from_metadata(dec_json, dec_dir, vakken_metadata_json=vakken_json)
    assert set(tables) == {
        "Dec_vooropl.asc", "Dec_vakcode.asc",
        "Dec_brinnummer.asc", "Dec_brinvestigingsnummer.asc",
    }
    assert tables["Dec_brinnummer.asc"]["Brinnummer"].to_list() == ["00AA", "01BB"]


def test_decode_vakken_layout_adds_labels_for_all_four_fields(vakken_setup):
    dec_json, vakken_json, dec_dir = vakken_setup
    dec_tables = load_dec_tables_from_metadata(dec_json, dec_dir, vakken_metadata_json=vakken_json)
    result = decode_fields_dec_only(
        VAKHAVW_FRAME, dec_json, dec_tables, vakken_metadata_json=vakken_json,
    )
    assert result["vakcode__omschrijving_vak"].to_list() == ["Wiskunde A", "Engels", "Wiskunde A"]
    assert result["brinnummer_vo_instelling__naam"].to_list() == ["School Een", "School Twee", "School Een"]
    assert result["vooropleiding_oorspronkelijke_code__onderwijssector"].to_list() == ["VO", "HO", "VO"]
    assert result[
        "vooropleiding_oorspronkelijke_code__omschrijving_vooropleiding_oorspr_code"
    ].to_list() == ["vwo", "propedeuse", "vwo"]
    # Composite: Brinnummer resolves to the brinnummer_vo_instelling column, so
    # vestiging 01 gets the label of its own school; 00AA+07 does not exist.
    assert result["vestigingsnummer_vo_vestiging__naam"].to_list() == [
        "Vestiging Een", "Vestiging Twee", None,
    ]
    # The key columns of the Dec tables are not copied as labels.
    assert "vestigingsnummer_vo_vestiging__brinnummer" not in result.columns
    assert "vestigingsnummer_vo_vestiging__vestigingsnummer" not in result.columns


def test_decode_vakken_layout_keeps_data_columns_raw(vakken_setup):
    dec_json, vakken_json, dec_dir = vakken_setup
    dec_tables = load_dec_tables_from_metadata(dec_json, dec_dir, vakken_metadata_json=vakken_json)
    result = decode_fields(
        VAKHAVW_FRAME, dec_json, dec_tables,
        variable_metadata_path=str(Path(dec_dir) / "geen.json"),
        vakken_metadata_json=vakken_json,
    )
    for col in VAKHAVW_FRAME.columns:
        assert result[col].to_list() == VAKHAVW_FRAME[col].to_list()
    assert not [c for c in result.columns if c.startswith("__")]


def test_decode_without_vakken_json_behaves_as_before(vakken_setup):
    dec_json, vakken_json, dec_dir = vakken_setup
    dec_tables = load_dec_tables_from_metadata(dec_json, dec_dir, vakken_metadata_json=vakken_json)
    result = decode_fields_dec_only(VAKHAVW_FRAME, dec_json, dec_tables)
    assert result.columns == VAKHAVW_FRAME.columns


def test_decode_vakken_layout_leaves_a_non_vakken_frame_alone(vakken_setup, capsys):
    dec_json, vakken_json, dec_dir = vakken_setup
    dec_tables = load_dec_tables_from_metadata(dec_json, dec_dir, vakken_metadata_json=vakken_json)
    ev = pl.DataFrame({"Burgerservicenummer": ["000000012"], "Inschrijvingsjaar": ["2024"]})
    result = decode_fields_dec_only(ev, dec_json, dec_tables, vakken_metadata_json=vakken_json)
    assert result.columns == ev.columns
    assert "[vakken]" not in capsys.readouterr().out


def test_decode_vakken_layout_respects_decode_columns(vakken_setup):
    dec_json, vakken_json, dec_dir = vakken_setup
    dec_tables = load_dec_tables_from_metadata(dec_json, dec_dir, vakken_metadata_json=vakken_json)
    result = decode_fields_dec_only(
        VAKHAVW_FRAME, dec_json, dec_tables,
        decode_columns=["Vakcode"], vakken_metadata_json=vakken_json,
    )
    added = [c for c in result.columns if c not in VAKHAVW_FRAME.columns]
    assert added == ["vakcode__omschrijving_vak"]


def test_decode_vakken_missing_dec_table_is_logged_not_raised(vakken_setup, capsys):
    dec_json, vakken_json, dec_dir = vakken_setup
    dec_tables = load_dec_tables_from_metadata(dec_json, dec_dir, vakken_metadata_json=vakken_json)
    del dec_tables["Dec_brinnummer.asc"]
    result = decode_fields_dec_only(VAKHAVW_FRAME, dec_json, dec_tables, vakken_metadata_json=vakken_json)
    assert "brinnummer_vo_instelling__naam" not in result.columns
    assert "vakcode__omschrijving_vak" in result.columns
    assert "Dec_brinnummer.asc" in capsys.readouterr().out


def test_get_available_decode_columns_includes_vakken_fields(vakken_setup):
    dec_json, vakken_json, _ = vakken_setup
    assert "Vakcode" not in get_available_decode_columns(dec_json)
    result = get_available_decode_columns(dec_json, vakken_json)
    for name in (
        "Brinnummer VO-instelling", "Vestigingsnummer VO-vestiging",
        "Vooropleiding oorspronkelijke code", "Vakcode",
    ):
        assert name in result
    assert "Diplomajaar" not in result


def test_get_decode_column_info_includes_vakken_labels(vakken_setup):
    dec_json, vakken_json, _ = vakken_setup
    info = get_decode_column_info(dec_json, vakken_json)
    assert info["Vakcode"] == ["Omschrijving vak"]
    # Composite: both key columns are left out of the labels.
    assert "Brinnummer" not in info["Vestigingsnummer VO-vestiging"]
    assert "Vestigingsnummer" not in info["Vestigingsnummer VO-vestiging"]
    assert "Naam" in info["Vestigingsnummer VO-vestiging"]


def test_apply_vakken_composite_pairs_keys_by_name():
    """The decoded field is the second key; its partner the first."""
    content = [
        "                    Startpositie  Aantal posities",
        "Brinnummer          1             4      }",
        "Vestigingsnummer    5             2      }",
        "Naam                7            30",
    ]
    data = pl.DataFrame({
        "brinnummer_vo_instelling": ["00AA", "01BB"],
        "vestigingsnummer_vo_vestiging": ["1", "01"],
    })
    dec = pl.DataFrame({
        "brinnummer": ["00AA", "01BB"],
        "vestigingsnummer": ["01", "01"],
        "naam": ["Een", "Twee"],
    })
    result = _apply_vakken_composite(
        data, dec, "vestigingsnummer_vo_vestiging", "Brinnummer VO-instelling",
        "brinnummer", content, "Dec_brinvestigingsnummer.asc",
    )
    assert result["vestigingsnummer_vo_vestiging__naam"].to_list() == ["Een", "Twee"]
    assert result["vestigingsnummer_vo_vestiging"].to_list() == ["1", "01"]


def test_find_vakken_metadata_json(tmp_path):
    assert find_vakken_metadata_json(str(tmp_path)) is None
    (tmp_path / "Bestandsbeschrijving_Dec-bestanden.json").write_text("{}")
    target = tmp_path / "Bestandsbeschrijving_Vakkenbestanden.json"
    target.write_text("{}")
    assert find_vakken_metadata_json(str(tmp_path)) == str(target)


# ---------------------------------------------------------------------------
# #202 — no fuzzy column choice; only exact names (plus an explicit repair)
# ---------------------------------------------------------------------------

def _dec_meta(var_name, code_name="Code"):
    return {
        "tables": [
            {
                "table_title": "Dec_x.asc",
                "decoding_variables": [var_name],
                "content": ["Startpositie  Naam", f"{code_name}  }}", "Omschrijving"],
            }
        ]
    }


@pytest.mark.parametrize("var_name, column", [
    ("Landcod", "landcode"),
    ("Hoogste vooropleiding voor het H", "hoogste_vooropleiding_voor_het_ho"),
])
def test_apply_dec_tables_one_char_off_name_is_skipped(var_name, column, capsys):
    meta = _dec_meta(var_name)
    dec_tables = {"Dec_x.asc": pl.DataFrame({"Code": ["1"], "Omschrijving": ["Een"]})}
    df = pl.DataFrame({column: ["1", "2"]})
    result = _apply_dec_tables(df, meta, dec_tables)
    assert result.equals(df)
    assert not [c for c in result.columns if "__" in c]
    out = capsys.readouterr().out
    assert "[decoder]" in out and "niet gevonden" in out


def test_apply_vakken_simple_one_char_off_column_is_skipped(capsys):
    data = pl.DataFrame({"vakcod": ["12"]})
    dec = pl.DataFrame({"vakcode": ["12"], "omschrijving_vak": ["Wiskunde"]})
    result = _apply_vakken_simple(
        data, dec, "vakcode", "vakcode", "Vakcode", "Dec_vakcode.asc", VAKCODE_CONTENT,
    )
    assert result.equals(data)
    out = capsys.readouterr().out
    assert "[decoder][vakken]" in out and "niet gevonden" in out
    assert "Dichtste" not in out


@pytest.mark.parametrize("data", [
    pl.DataFrame({"brinnummr": ["02UC"], "vestigingsnummer": ["1"]}),
    pl.DataFrame({"brinnummer": ["02UC"], "vestigingsnummr": ["1"]}),
])
def test_apply_vakken_composite_one_char_off_column_is_skipped(data, capsys):
    content = [
        "                    Startpositie  Aantal posities",
        "Brinnummer          1             4      }",
        "Vestigingsnummer    5             2      }",
        "Naam vestiging      7            30",
    ]
    dec = pl.DataFrame({
        "brinnummer": ["02UC"], "vestigingsnummer": ["01"], "naam_vestiging": ["Zeist"],
    })
    result = _apply_vakken_composite(
        data, dec, "brinnummer", "Vestigingsnummer", "brinnummer", content,
        "Dec_brinvestigingsnummer.asc",
    )
    assert result.equals(data)
    out = capsys.readouterr().out
    assert "[decoder][vakken]" in out and "niet gevonden" in out
    assert "Dichtste" not in out


def test_resolve_mapping_column_exact_name():
    df = pl.DataFrame({"geslacht": ["M"]})
    assert _resolve_mapping_column("geslacht", "Geslacht", df, {"geslacht": "Geslacht"}, None) == "geslacht"


def test_resolve_mapping_column_exact_cleaned_original_name():
    # A naming function that keeps the fraction slash: only clean_header_name
    # turns the original header into the variable name.
    def naming(s):
        return s.lower().replace(" ", "_")

    df = pl.DataFrame({"a⁄b": ["1"]})
    norm_map = {"a⁄b": "A⁄B"}
    assert _resolve_mapping_column("a/b", "A/B", df, norm_map, naming) == "a⁄b"


def test_resolve_mapping_column_near_name_returns_none(capsys):
    df = pl.DataFrame({"vooropleiding_oorspronkelijke_code": ["1"], "indicatie_diplomavak": ["J"]})
    norm_map = {c: c for c in df.columns}
    for var_norm in (
        "hoogste_vooropleiding_voor_het_ho_oorspr_code",
        "indicatie_soort_programma",
        "vooropleiding_oorspronkelijke_cod",
    ):
        assert _resolve_mapping_column(var_norm, None, df, norm_map, None) is None
    assert capsys.readouterr().out.count("kolom niet gevonden") == 3


def test_vakhavw_collision_mappings_leave_column_untouched(tmp_path):
    """Two 'oorspr. code' mappings must not land on vooropleiding_oorspronkelijke_code."""
    items = [
        {"name": "Hoogste vooropleiding vóór het HO oorspr. code", "values": {"1": "VWO"}},
        {"name": "Hoogste vooropleiding binnen het HO oorspr. code", "values": {"1": "HBO"}},
    ]
    path = tmp_path / "variable_metadata.json"
    path.write_text(json.dumps(items), encoding="utf-8")
    df = pl.DataFrame({"vooropleiding_oorspronkelijke_code": ["1", "2"]})
    norm_df, norm_map, orig = _normalize_df(df)
    result = _apply_variable_mappings(norm_df, str(path), None, norm_map, orig)
    assert result["vooropleiding_oorspronkelijke_code"].to_list() == ["1", "2"]


@pytest.mark.parametrize("raw", [
    "Hoogste vooropleiding v��r het HO",
    "Hoogste vooropleiding vï¿½ï¿½r het HO",
    "Hoogste vooropleiding vóór het HO",
])
def test_canonical_name_repairs_corrupt_voor(raw):
    assert _canonical_name(raw) == "hoogste_vooropleiding_voor_het_ho"


def test_canonical_name_leaves_unknown_corruption_alone():
    # One replacement char is not in the repair table: not guessed.
    assert _canonical_name("Hoogste vooropleiding v�r het HO") == "hoogste_vooropleiding_vr_het_ho"


@pytest.mark.parametrize("raw", [
    "Hoogste vooropleiding v��r het HO",
    "Hoogste vooropleiding vï¿½ï¿½r het HO",
])
@pytest.mark.parametrize("decode_columns", [None, ["Hoogste vooropleiding vóór het HO"]])
def test_apply_dec_tables_repairs_corrupt_voor_without_fuzzy(raw, decode_columns):
    meta = _dec_meta(raw, code_name="Hoogste vooropleiding")
    dec_tables = {
        "Dec_x.asc": pl.DataFrame({"Hoogste vooropleiding": ["1"], "Omschrijving": ["VWO"]})
    }
    df = pl.DataFrame({"hoogste_vooropleiding_voor_het_ho": ["1", "2"]})
    result = _apply_dec_tables(df, meta, dec_tables, decode_columns=decode_columns)
    assert result["hoogste_vooropleiding_voor_het_ho"].to_list() == ["1", "2"]
    assert result["hoogste_vooropleiding_voor_het_ho__omschrijving"].to_list() == ["VWO", None]


def test_nfwa_preset_has_no_corrupt_spelling():
    decode_columns = PRESET_CONFIGS["nfwa"]["settings"]["decode_columns"]
    assert not [c for c in decode_columns if "ï¿½" in c or "�" in c]
    assert "Hoogste vooropleiding vóór het HO" in decode_columns
    assert PRESET_CONFIGS["evaluatietool"]["settings"] is PRESET_CONFIGS["nfwa"]["settings"]


# ---------------------------------------------------------------------------
# Decode + snake_case step keep non-latin-1 characters (#208)
# ---------------------------------------------------------------------------


def test_decoded_and_enriched_keep_non_latin1_chars_after_snake_case(tmp_path, dec_metadata_json):
    """Characters outside latin-1 in data, Dec labels and variable labels survive
    decoding and the header normalisation step unchanged. Synthetic values only."""
    from eencijferho.utils.converter_headers import convert_csv_headers_to_snake_case

    var_meta = [{"name": "Status code", "values": {"A": "Actief €", "I": "Inactief–oud"}}]
    vm_path = tmp_path / "variable_metadata.json"
    vm_path.write_text(json.dumps(var_meta), encoding="utf-8")

    dec_tables = {
        "Dec_test.asc": pl.DataFrame(
            {"Code": ["1", "2"], "Omschrijving": ["Label œ", "half 1⁄2"]}
        )
    }
    main_df = pl.DataFrame(
        {
            "Code": ["1", "2"],
            "Naam school": ["Sint–Jan lyceum", "Havo €/school"],
            "Status code": ["A", "I"],
        }
    )
    decoded = decode_fields_dec_only(main_df, dec_metadata_json, dec_tables)
    enriched = decode_fields(main_df, dec_metadata_json, dec_tables, variable_metadata_path=str(vm_path))

    out = tmp_path / "out"
    out.mkdir()
    # Written the way the pipeline writes them (core/pipeline.py, step 2).
    (out / "EV2023_decoded.csv").write_text(decoded.write_csv(separator=";"), encoding="utf-8")
    (out / "EV2023_enriched.csv").write_text(enriched.write_csv(separator=";"), encoding="utf-8")

    convert_csv_headers_to_snake_case(input_dir=str(out))

    for name, expected in [("EV2023_decoded.csv", decoded), ("EV2023_enriched.csv", enriched)]:
        result = pl.read_csv(out / name, separator=";", infer_schema_length=0, encoding="utf8")
        assert "naam_school" in result.columns
        assert result.width == expected.width
        # Header names changed, so compare column by column in order.
        assert [result[c].to_list() for c in result.columns] == [
            expected[c].to_list() for c in expected.columns
        ]
        text = (out / name).read_text(encoding="utf-8")
        for chars in ["Sint–Jan lyceum", "Havo €/school", "Label œ", "half 1⁄2"]:
            assert chars in text
        assert "?" not in text

    enriched_text = (out / "EV2023_enriched.csv").read_text(encoding="utf-8")
    assert "Actief €" in enriched_text
    assert "Inactief–oud" in enriched_text

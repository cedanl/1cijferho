# Tests for eencijferho.utils.converter_validation

from eencijferho.utils.converter_validation import converter_validation


def test_converter_validation_passes_on_match(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log(
        [{"input_file": "EV2023.asc", "status": "success", "total_lines": 100}]
    )
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 100,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["successful_conversions"] == 1
    assert result["failed_conversions"] == 0


def test_converter_validation_fails_on_mismatch(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log(
        [{"input_file": "EV2023.asc", "status": "success", "total_lines": 99}]
    )
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 100,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["failed_conversions"] == 1
    assert result["successful_conversions"] == 0


def test_converter_validation_error_message_contains_counts(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log(
        [{"input_file": "EV2023.asc", "status": "success", "total_lines": 50}]
    )
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 100,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    error = result["file_details"][0]["error"]
    assert "50" in error or "100" in error


def test_converter_validation_skips_missing_from_conversion_log(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log([])
    match = make_match_log(
        [
            {
                "input_file": "EV2023.asc",
                "row_count": 5,
                "status": "matched",
                "matches": [],
            }
        ]
    )
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["total_files"] == 0


def test_converter_validation_writes_output_log(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log([])
    match = make_match_log([])
    out = tmp_path / "sub" / "validation.json"
    converter_validation(str(conv), str(match), str(out))
    assert out.exists()


def test_converter_validation_status_completed(
    tmp_path, make_conversion_log, make_match_log
):
    conv = make_conversion_log([])
    match = make_match_log([])
    result = converter_validation(str(conv), str(match), str(tmp_path / "out.json"))
    assert result["status"] == "completed"

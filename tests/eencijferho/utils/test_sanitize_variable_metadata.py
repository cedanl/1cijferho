"""Tests for sanitize_variable_metadata module."""

import json
import tempfile
import os
import pytest
from functools import wraps

from eencijferho.utils.sanitize_variable_metadata import sanitize_variable_metadata_json


def _run_in_tmpdir(func):
    """Decorator to run a test in a temporary directory."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                os.chdir(tmpdir)
                return func(*args, **kwargs)
            finally:
                os.chdir(original_cwd)
    return wrapper


class TestSanitizeVariableMetadata:
    """Test sanitize_variable_metadata_json function."""

    @_run_in_tmpdir
    def test_sanitize_no_changes_needed(self, capsys):
        """Skip sanitization when no commas or semicolons present."""
        json_path = "variables.json"
        data = [
            {"name": "Var1", "values": {"1": "Value one", "2": "Value two"}},
        ]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        sanitize_variable_metadata_json(json_path)

        captured = capsys.readouterr()
        assert "No changes needed" in captured.out

        # Data should be unchanged
        with open(json_path, "r", encoding="utf-8") as f:
            result = json.load(f)
        assert result[0]["values"]["1"] == "Value one"

    @_run_in_tmpdir
    def test_sanitize_removes_commas(self, capsys):
        """Remove commas from value strings."""
        json_path = "variables.json"
        data = [
            {"name": "Var1", "values": {"1": "Value, with comma", "2": "Normal"}},
        ]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        sanitize_variable_metadata_json(json_path)

        captured = capsys.readouterr()
        assert "Sanitized" in captured.out

        with open(json_path, "r", encoding="utf-8") as f:
            result = json.load(f)
        assert result[0]["values"]["1"] == "Value with comma"
        assert result[0]["values"]["2"] == "Normal"

    @_run_in_tmpdir
    def test_sanitize_removes_semicolons(self, capsys):
        """Remove semicolons from value strings."""
        json_path = "variables.json"
        data = [
            {"name": "Var1", "values": {"1": "Value; with semicolon"}},
        ]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        sanitize_variable_metadata_json(json_path)

        captured = capsys.readouterr()
        assert "Sanitized" in captured.out

        with open(json_path, "r", encoding="utf-8") as f:
            result = json.load(f)
        assert result[0]["values"]["1"] == "Value with semicolon"

    @_run_in_tmpdir
    def test_sanitize_removes_both_comma_and_semicolon(self, capsys):
        """Remove both commas and semicolons."""
        json_path = "variables.json"
        data = [
            {"name": "Var1", "values": {"1": "Value, with; both"}},
        ]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        sanitize_variable_metadata_json(json_path)

        with open(json_path, "r", encoding="utf-8") as f:
            result = json.load(f)
        assert result[0]["values"]["1"] == "Value with both"

    @_run_in_tmpdir
    def test_sanitize_handles_non_string_values(self, capsys):
        """Skip non-string values gracefully."""
        json_path = "variables.json"
        data = [
            {
                "name": "Var1",
                "values": {
                    "1": "String value",
                    "2": 123,  # Non-string
                    "3": None,  # Non-string
                },
            },
        ]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        sanitize_variable_metadata_json(json_path)

        captured = capsys.readouterr()
        assert "No changes needed" in captured.out

    @_run_in_tmpdir
    def test_sanitize_handles_values_not_dict(self, capsys):
        """Skip values that aren't dictionaries."""
        json_path = "variables.json"
        data = [
            {"name": "Var1", "values": ["list", "instead", "of", "dict"]},
        ]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        sanitize_variable_metadata_json(json_path)

        captured = capsys.readouterr()
        assert "No changes needed" in captured.out

    @_run_in_tmpdir
    def test_sanitize_multiple_variables(self, capsys):
        """Sanitize multiple variables in one file."""
        json_path = "variables.json"
        data = [
            {"name": "Var1", "values": {"1": "Value, one"}},
            {"name": "Var2", "values": {"2": "Value; two"}},
            {"name": "Var3", "values": {"3": "Value three"}},
        ]
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        sanitize_variable_metadata_json(json_path)

        with open(json_path, "r", encoding="utf-8") as f:
            result = json.load(f)
        assert result[0]["values"]["1"] == "Value one"
        assert result[1]["values"]["2"] == "Value two"
        assert result[2]["values"]["3"] == "Value three"

    def test_sanitize_path_traversal_rejected(self):
        """Reject path traversal attempts."""
        with pytest.raises(ValueError, match="Path traversal"):
            sanitize_variable_metadata_json("../../etc/passwd")

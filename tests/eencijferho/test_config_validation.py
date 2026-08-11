"""Tests for config module path validation."""

import os
import pytest

from eencijferho.config import validate_safe_path


class TestValidateSafePath:
    """Test path traversal protection."""

    def test_valid_path_in_current_dir(self):
        """Allow paths within current directory."""
        result = validate_safe_path("data/01-input", base_dir=".")
        assert result == "data/01-input"

    def test_valid_path_in_subdirectory(self):
        """Allow paths within subdirectories."""
        result = validate_safe_path("data/01-input/DEMO", base_dir="data")
        assert result == "data/01-input/DEMO"

    def test_path_traversal_attempt_rejected(self):
        """Reject path traversal attempts."""
        with pytest.raises(ValueError, match="Path traversal"):
            validate_safe_path("../../etc/passwd", base_dir="data")

    def test_absolute_path_outside_base_rejected(self):
        """Reject absolute paths outside base directory."""
        with pytest.raises(ValueError, match="Path traversal"):
            validate_safe_path("/etc/passwd", base_dir="data")

    def test_base_directory_itself_allowed(self):
        """Allow exact base directory path."""
        base = os.path.realpath(".")
        result = validate_safe_path(base, base_dir=".")
        assert result == base

    def test_relative_path_with_dots_rejected(self):
        """Reject relative paths with ../ traversal."""
        with pytest.raises(ValueError, match="Path traversal"):
            validate_safe_path("../../../sensitive", base_dir="data/01-input")

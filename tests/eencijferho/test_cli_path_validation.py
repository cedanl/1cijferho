"""Tests for CLI path validation."""

import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock
from eencijferho.cli import _validate_safe_path


class TestCLIPathValidation:
    """Test path validation in CLI commands."""

    def test_validate_safe_path_valid_directory(self):
        """Allow valid paths within current directory."""
        # Use relative path within current directory
        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                os.chdir(tmpdir)
                # Should not raise
                _validate_safe_path(".")
            finally:
                os.chdir(original_cwd)

    def test_validate_safe_path_subdirectory(self):
        """Allow valid subdirectories."""
        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                os.chdir(tmpdir)
                os.makedirs("subdir")
                # Should not raise
                _validate_safe_path("subdir")
            finally:
                os.chdir(original_cwd)

    def test_validate_safe_path_traversal_attack_rejected(self):
        """Reject path traversal attempts."""
        with pytest.raises(SystemExit):
            _validate_safe_path("../../etc/passwd")

    def test_validate_safe_path_absolute_path_outside_base(self):
        """Reject absolute paths outside base directory."""
        with pytest.raises(SystemExit):
            _validate_safe_path("/etc/passwd")

    def test_validate_safe_path_multiple_traversal_rejected(self):
        """Reject multiple traversal attempts."""
        with pytest.raises(SystemExit):
            _validate_safe_path("../../../sensitive/data")

    def test_validate_safe_path_current_dir(self):
        """Allow current directory reference."""
        # Should not raise
        _validate_safe_path(".")

    def test_validate_safe_path_relative_safe_path(self):
        """Allow safe relative paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a safe relative path within tmpdir
            safe_subdir = os.path.join(tmpdir, "safe")
            os.makedirs(safe_subdir)
            # Change to tmpdir to test relative path
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                # Should not raise
                _validate_safe_path("safe")
            finally:
                os.chdir(original_cwd)

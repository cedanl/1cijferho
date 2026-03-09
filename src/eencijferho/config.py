# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Standalone configuration for the eencijferho package.
No Streamlit dependency - safe to use in any Python environment.
"""

import os

# Default demo mode
DEFAULT_DEMO_MODE: bool = True

# Directory constants (based on default demo mode)
INPUT_DIR: str = "data/01-input/DEMO" if DEFAULT_DEMO_MODE else "data/01-input"
OUTPUT_DIR: str = "data/02-output/DEMO" if DEFAULT_DEMO_MODE else "data/02-output"
DECODER_INPUT_DIR: str = "data/01-input"
METADATA_DIR: str = "data/00-metadata"


def get_input_dir(demo_mode: bool = DEFAULT_DEMO_MODE) -> str:
    """Get input directory based on demo mode."""
    return "data/01-input/DEMO" if demo_mode else "data/01-input"


def get_output_dir(demo_mode: bool = DEFAULT_DEMO_MODE) -> str:
    """Get output directory based on demo mode."""
    return "data/02-output/DEMO" if demo_mode else "data/02-output"


def get_decoder_input_dir() -> str:
    """Get decoder input directory (always at root, regardless of demo mode)."""
    return "data/01-input"


def get_metadata_dir(output_dir: str | None = None) -> str:
    """Get metadata directory.

    When called with an explicit output_dir (e.g. from the CLI), metadata is
    stored as a subdirectory of that output directory:  <output_dir>/metadata

    When called without arguments (e.g. from the legacy Streamlit app via
    src/config.py), the classic ``data/00-metadata`` location is returned.
    """
    if output_dir is not None:
        return os.path.join(output_dir, "metadata")
    return METADATA_DIR

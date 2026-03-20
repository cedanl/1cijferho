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
from dataclasses import dataclass, field

@dataclass
class OutputConfig:
    """Controls which output variants the pipeline produces.

    Attributes:
        variants: Which decoded variants to create. Supported values:
            ``"decoded"`` (Dec-only substitution) and ``"enriched"``
            (Dec + variable_metadata label substitution).
        formats: Extra output formats. ``"parquet"`` compresses each CSV
            to a Parquet file.
        encrypt: When True, sensitive columns (e.g. BSN) are encrypted.
        column_casing: Header style applied to all output CSV/Parquet files.
            ``"snake_case"`` converts headers to snake_case; ``"none"``
            leaves headers unchanged.

    Example — CSV-only, no encryption, no header rename::

        OutputConfig(variants=["decoded"], formats=[], encrypt=False, column_casing="none")
    """

    variants: list[str] = field(default_factory=lambda: ["decoded", "enriched"])
    formats: list[str] = field(default_factory=lambda: ["parquet"])
    encrypt: bool = True
    column_casing: str = "snake_case"

    def __post_init__(self) -> None:
        valid_variants = frozenset({"decoded", "enriched"})
        valid_formats = frozenset({"parquet"})
        valid_casing = frozenset({"snake_case", "none"})

        invalid_variants = set(self.variants) - valid_variants
        if invalid_variants:
            raise ValueError(f"Ongeldige variants: {invalid_variants}. Toegestaan: {valid_variants}")

        if "enriched" in self.variants and "decoded" not in self.variants:
            raise ValueError("'enriched' vereist dat 'decoded' ook in variants staat.")

        invalid_formats = set(self.formats) - valid_formats
        if invalid_formats:
            raise ValueError(f"Ongeldige formats: {invalid_formats}. Toegestaan: {valid_formats}")

        if self.column_casing not in valid_casing:
            raise ValueError(f"Ongeldige column_casing: '{self.column_casing}'. Toegestaan: {valid_casing}")


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

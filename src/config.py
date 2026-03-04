# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Application configuration for 1CIJFERHO.
Supports dynamic DEMO_MODE via Streamlit session state.
"""

try:
    import streamlit as st
    _STREAMLIT_AVAILABLE = True
except ImportError:
    _STREAMLIT_AVAILABLE = False

# Default demo mode (used when not running in Streamlit or session state not initialized)
DEFAULT_DEMO_MODE: bool = True


def get_demo_mode() -> bool:
    """Get current demo mode from Streamlit session state or default."""
    if _STREAMLIT_AVAILABLE and hasattr(st, 'session_state') and 'demo_mode' in st.session_state:
        return st.session_state.demo_mode
    return DEFAULT_DEMO_MODE


def set_demo_mode(enabled: bool) -> None:
    """Set demo mode in Streamlit session state."""
    if _STREAMLIT_AVAILABLE and hasattr(st, 'session_state'):
        st.session_state.demo_mode = enabled


def get_input_dir() -> str:
    """Get input directory based on current demo mode."""
    return "data/01-input/DEMO" if get_demo_mode() else "data/01-input"


def get_output_dir() -> str:
    """Get output directory based on current demo mode."""
    return "data/02-output/DEMO" if get_demo_mode() else "data/02-output"


def get_decoder_input_dir() -> str:
    """Get decoder input directory (always at root, regardless of demo mode)."""
    return "data/01-input"


# Legacy constants for backward compatibility (use functions above for dynamic behavior)
DEMO_MODE: bool = DEFAULT_DEMO_MODE
INPUT_DIR: str = "data/01-input/DEMO" if DEFAULT_DEMO_MODE else "data/01-input"
OUTPUT_DIR: str = "data/02-output/DEMO" if DEFAULT_DEMO_MODE else "data/02-output"
DECODER_INPUT_DIR: str = "data/01-input"


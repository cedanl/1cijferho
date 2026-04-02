# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Application configuration for 1CIJFERHO.
Supports dynamic DEMO_MODE via Streamlit session state with file-based persistence.
"""

import json
import os

try:
    import streamlit as st
    _STREAMLIT_AVAILABLE = True
except ImportError:
    _STREAMLIT_AVAILABLE = False

# Default demo mode (used when no persisted setting exists)
DEFAULT_DEMO_MODE: bool = True

_USER_SETTINGS_FILE = "data/.user_settings.json"


def _load_persisted_demo_mode() -> bool:
    """Read demo mode from the persisted settings file, or return DEFAULT_DEMO_MODE."""
    try:
        if os.path.exists(_USER_SETTINGS_FILE):
            with open(_USER_SETTINGS_FILE, encoding="utf-8") as f:
                return bool(json.load(f).get("demo_mode", DEFAULT_DEMO_MODE))
    except Exception:
        pass
    return DEFAULT_DEMO_MODE


def _save_persisted_demo_mode(enabled: bool) -> None:
    """Write demo mode to the persisted settings file."""
    try:
        os.makedirs(os.path.dirname(_USER_SETTINGS_FILE), exist_ok=True)
        settings = {}
        if os.path.exists(_USER_SETTINGS_FILE):
            with open(_USER_SETTINGS_FILE, encoding="utf-8") as f:
                settings = json.load(f)
        settings["demo_mode"] = enabled
        with open(_USER_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
    except Exception:
        pass


def get_demo_mode() -> bool:
    """Get current demo mode from Streamlit session state or default."""
    if _STREAMLIT_AVAILABLE and hasattr(st, 'session_state') and 'demo_mode' in st.session_state:
        return st.session_state.demo_mode
    return DEFAULT_DEMO_MODE


def set_demo_mode(enabled: bool) -> None:
    """Set demo mode in Streamlit session state and persist to file."""
    _save_persisted_demo_mode(enabled)
    if _STREAMLIT_AVAILABLE and hasattr(st, 'session_state'):
        st.session_state.demo_mode = enabled


def get_initial_demo_mode() -> bool:
    """Get the initial demo mode value for session state initialization (file > default)."""
    return _load_persisted_demo_mode()


def get_input_dir() -> str:
    """Get input directory based on current demo mode."""
    return "data/01-input/DEMO" if get_demo_mode() else "data/01-input"


def get_output_dir() -> str:
    """Get output directory based on current demo mode."""
    return "data/02-output/DEMO" if get_demo_mode() else "data/02-output"


def get_decoder_input_dir() -> str:
    """Get decoder input directory based on current demo mode."""
    return "data/01-input/DEMO" if get_demo_mode() else "data/01-input"


def get_metadata_dir() -> str:
    """Get metadata directory (always data/00-metadata for the Streamlit app)."""
    return "data/00-metadata"


# Legacy constants for backward compatibility (use functions above for dynamic behavior)
DEMO_MODE: bool = DEFAULT_DEMO_MODE
INPUT_DIR: str = "data/01-input/DEMO" if DEFAULT_DEMO_MODE else "data/01-input"
OUTPUT_DIR: str = "data/02-output/DEMO" if DEFAULT_DEMO_MODE else "data/02-output"
DECODER_INPUT_DIR: str = "data/01-input/DEMO" if DEFAULT_DEMO_MODE else "data/01-input"
METADATA_DIR: str = "data/00-metadata"

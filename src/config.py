# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: 
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Application configuration for 1CIJFERHO.
Auto-detection enabled by default. Override if needed.
"""

import os
import glob

# Optional override: Set to True/False to force demo/production mode
# Leave as None to auto-detect (recommended, default behavior)
DEMO_MODE_OVERRIDE: bool | None = None


def _detect_demo_mode() -> bool:
    """
    Dynamically detect if app should run in demo mode.
    
    Logic:
    - If user files exist in data/01-input/ (non-DEMO) → DEMO_MODE = False
    - If only demo files in data/01-input/DEMO/ → DEMO_MODE = True
    - Default (fresh install, no files) → DEMO_MODE = True
    """
    # User files are .asc or .txt files in root, excluding DEMO subfolder and decoder reference
    user_files = [
        f for f in glob.glob("data/01-input/*.asc") + glob.glob("data/01-input/*.txt")
        if not any(excluded in os.path.basename(f) for excluded in ["_DEMO", "Dec_", "Bestandsbeschrijving"])
    ]
    
    # User uploaded non-demo data → use production mode
    if user_files:
        return False
    
    # Otherwise default to demo mode (covers fresh installs and demo-only scenarios)
    return True


# Determine actual demo mode: override takes precedence, otherwise auto-detect
def get_demo_mode() -> bool:
    """Get current demo mode, checking Streamlit session state first"""
    try:
        import streamlit as st
        # If in Streamlit context and override is set, use it
        if hasattr(st, 'session_state') and "demo_mode_override" in st.session_state:
            override = st.session_state.demo_mode_override
            if override is not None:
                return override
    except:
        pass
    
    # Fall back to config override or auto-detection
    if DEMO_MODE_OVERRIDE is not None:
        return DEMO_MODE_OVERRIDE
    return _detect_demo_mode()


DEMO_MODE: bool = get_demo_mode()

# Functions to get current paths (refreshed each call, respects session_state changes)
def get_input_dir() -> str:
    """Get current input directory based on demo mode"""
    demo = get_demo_mode()
    return "data/01-input/DEMO" if demo else "data/01-input"

def get_output_dir() -> str:
    """Get current output directory based on demo mode"""
    demo = get_demo_mode()
    return "data/02-output/DEMO" if demo else "data/02-output"

def get_decoder_input_dir() -> str:
    """Get decoder files directory based on demo mode"""
    demo = get_demo_mode()
    # Decoder reference files live in DEMO subfolder (with other demo files)
    return "data/01-input/DEMO" if demo else "data/01-input"

# Legacy constants (auto-detected once at import time, for backwards compatibility)
# Prefer using get_input_dir(), get_output_dir(), get_decoder_input_dir() functions in new code
INPUT_DIR: str = get_input_dir()
OUTPUT_DIR: str = get_output_dir()

# Decoder files location (also follows demo mode now)
DECODER_INPUT_DIR: str = get_decoder_input_dir()


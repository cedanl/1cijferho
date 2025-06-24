# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Main Entrypoint for the 1CIJFERHO App
"""
import streamlit as st
import glob
import os

# -----------------------------------------------------------------------------
# App Configuration - Must be first Streamlit command
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="1CijferHO | CEDA",
    page_icon="🚀",
    layout="centered",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Demo Detection Function
# -----------------------------------------------------------------------------
def check_demo_files():
    """Check if demo files exist in data/01-input directory"""
    demo_files = glob.glob("data/01-input/*_DEMO*")
    return len(demo_files) > 0, demo_files

def show_demo_notifications():
    """Show demo notifications in sidebar only"""
    demo_exists, demo_files = check_demo_files()
    
    if demo_exists:
        # Sidebar (persistent)
        with st.sidebar:
            st.warning("🎯 **Demo Mode Active**", icon="⚠️")
            st.write(f"{len(demo_files)} demo files active")
            st.error("Ready for your own data? Remove *_DEMO files from `data/01-input/`")
        
        return True
    return False

# -----------------------------------------------------------------------------
# Pages Overview - YOU CAN ADD MORE PAGES HERE
# -----------------------------------------------------------------------------
home_page = st.Page("frontend/Overview/Home.py", icon="🏠", title="Home")
documentation_page = st.Page("frontend/Overview/Documentation.py", icon="📚", title="Documentation")

data_upload_page = st.Page("frontend/Files/Upload_Data.py", icon="📁", title="Upload Data")

extract_page = st.Page("frontend/Modules/Extract_Metadata.py", icon="🔍", title="Extract Metadata")
validate_page = st.Page("frontend/Modules/Validate_Metadata.py", icon="🛡️", title="Validate Metadata")
turbo_convert_page = st.Page("frontend/Modules/Turbo_Convert.py", icon="⚡", title="Turbo Convert")

# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------
# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# Demo Detection
show_demo_notifications()

# Initialize Navigation
pg = st.navigation ( {
    "Overview": [home_page, documentation_page],
    "Files": [data_upload_page],
    "Modules": [extract_page, validate_page, turbo_convert_page],
})

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()
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
import requests
from pathlib import Path
import requests

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
            st.write(f"{len(demo_files)} demo files active ")
            st.error("⚠️ Ready for your own data? Remove all *_DEMO files from `data/01-input/`")
        
        return True
    return False



@st.cache_data(ttl=3600)  # Cache for 1 hour
def check_repo_version():
    """Check if local version matches latest GitHub release"""
    try:
        local_version = Path("VERSION").read_text().strip()
        response = requests.get(
            "https://api.github.com/repos/cedanl/1cijferho/releases/latest",
            timeout=5
        )
        
        if response.status_code == 200:
            latest_version = response.json()["tag_name"]
            return {
                "is_latest": local_version == latest_version,
                "local_version": local_version,
                "latest_version": latest_version
            }
    except:
        return None

def show_version_notification():
    """Show version notification in sidebar"""
    version_info = check_repo_version()
    
    if version_info and not version_info["is_latest"]:
        with st.sidebar:
            st.info(f"🔄 Update: `{version_info['local_version']}` → `{version_info['latest_version']}`")
            st.link_button("⬇️ Download", "https://github.com/cedanl/1cijferho/releases/latest", use_container_width=True)
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
tip_page = st.Page("frontend/Modules/Tip.py", icon="💡", title="Tip")

# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------
# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# Demo Detection
show_demo_notifications()
check_repo_version()
show_version_notification()

# Initialize Navigation
pg = st.navigation ( {
    "Overview": [home_page, documentation_page],
    "Files": [data_upload_page],
    "Modules": [extract_page, validate_page, turbo_convert_page, tip_page]
})

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()
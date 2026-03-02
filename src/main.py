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
import requests
from pathlib import Path
from config import INPUT_DIR, OUTPUT_DIR, _detect_demo_mode, get_input_dir

# Initialize session state for demo mode toggle
if "demo_mode_override" not in st.session_state:
    st.session_state.demo_mode_override = None  # None = auto-detect, True/False = manual override

# Determine actual DEMO_MODE (session override takes precedence)
DEMO_MODE = st.session_state.demo_mode_override if st.session_state.demo_mode_override is not None else _detect_demo_mode()

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
def check_demo_files() -> tuple[bool, list[str]]:
    """Check if demo files exist in the configured input directory"""
    demo_files = glob.glob(f"{INPUT_DIR}/*_DEMO*")
    return len(demo_files) > 0, demo_files

def show_demo_notifications() -> bool:
    """Show demo mode indicator and toggle in sidebar"""
    with st.sidebar:
        st.divider()
        st.subheader("⚙️ Modus")
        
        # Toggle between demo and production mode
        use_demo = st.toggle(
            "Demo bestanden gebruiken?",
            value=DEMO_MODE,
            help="Toggle tussen demo-bestanden en uw eigen data"
        )
        
        if use_demo != DEMO_MODE:
            st.session_state.demo_mode_override = use_demo
            st.rerun()
        
        # Show current directory info
        st.caption(f"📁 Ingang: `{get_input_dir()}/`")
        
        st.divider()



@st.cache_data(ttl=3600)  # Cache for 1 hour
def check_repo_version() -> dict | None:
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

def show_version_notification() -> bool:
    """Show version notification in sidebar"""
    version_info = check_repo_version()
    
    if version_info and not version_info["is_latest"]:
        with st.sidebar:
            st.info(f"🔄 Update beschikbaar: `{version_info['local_version']}` → `{version_info['latest_version']}`")
            st.link_button("⬇️ Download nieuwste versie", "https://github.com/cedanl/1cijferho/releases/latest", use_container_width=True)
        return True
    return False
# -----------------------------------------------------------------------------
# Pages Overview - YOU CAN ADD MORE PAGES HERE
# -----------------------------------------------------------------------------
home_page = st.Page("frontend/Overview/Home.py", icon="🏠", title="Startpagina")
documentation_page = st.Page("frontend/Overview/Documentation.py", icon="📚", title="Documentatie")

data_upload_page = st.Page("frontend/Files/Upload_Data.py", icon="📁", title="Bestanden uploaden")

extract_page = st.Page("frontend/Modules/Extract_Metadata.py", icon="🔍", title="Metadata extraheren")
validate_page = st.Page("frontend/Modules/Validate_Metadata.py", icon="🛡️", title="Metadata valideren")
turbo_convert_page = st.Page("frontend/Modules/Turbo_Convert.py", icon="⚡", title="Turbo Conversie")
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

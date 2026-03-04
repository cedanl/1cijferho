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
from config import get_demo_mode, set_demo_mode, get_input_dir, DEFAULT_DEMO_MODE

# -----------------------------------------------------------------------------
# App Configuration - Must be first Streamlit command
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="1CijferHO | CEDA",
    page_icon="🚀",
    layout="centered",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# Initialize Session State
# -----------------------------------------------------------------------------
if "demo_mode" not in st.session_state:
    st.session_state.demo_mode = DEFAULT_DEMO_MODE


# -----------------------------------------------------------------------------
# Demo Detection Function
# -----------------------------------------------------------------------------
def check_demo_files() -> tuple[bool, list[str]]:
    """Check if demo files exist in the configured input directory"""
    input_dir = get_input_dir()
    demo_files = glob.glob(f"{input_dir}/*_DEMO*")
    return len(demo_files) > 0, demo_files


def show_demo_toggle_and_notifications() -> bool:
    """Show demo mode toggle and notifications in sidebar"""
    with st.sidebar:
        st.markdown("---")
        st.subheader("⚙️ Data Modus")

        # Toggle for demo mode - uses 'demo_mode' key directly to persist state across pages
        st.toggle(
            "Demo Modus",
            value=st.session_state.demo_mode,
            help="Schakel tussen demo-bestanden en uw eigen data",
            key="demo_mode",
            on_change=lambda: None,  # State is automatically updated via key
        )

        # Show current paths
        demo_mode = st.session_state.demo_mode
        if demo_mode:
            st.info("📂 **Demo pad:** `data/01-input/DEMO/`")
            demo_exists, demo_files = check_demo_files()
            if demo_exists:
                st.success(f"✅ {len(demo_files)} demo-bestanden gevonden")
            else:
                st.warning("⚠️ Geen demo-bestanden gevonden")
        else:
            st.info("📂 **Data pad:** `data/01-input/`")
            st.caption("Plaats uw eigen bestanden in `data/01-input/`")

        st.markdown("---")

    return demo_mode


@st.cache_data(ttl=3600)  # Cache for 1 hour
def check_repo_version() -> dict | None:
    """Check if local version matches latest GitHub release"""
    try:
        local_version = Path("VERSION").read_text().strip()
        response = requests.get(
            "https://api.github.com/repos/cedanl/1cijferho/releases/latest", timeout=5
        )

        if response.status_code == 200:
            latest_version = response.json()["tag_name"]
            return {
                "is_latest": local_version == latest_version,
                "local_version": local_version,
                "latest_version": latest_version,
            }
    except:
        return None


def show_version_notification() -> bool:
    """Show version notification in sidebar"""
    version_info = check_repo_version()

    if version_info and not version_info["is_latest"]:
        with st.sidebar:
            st.info(
                f"🔄 Update beschikbaar: `{version_info['local_version']}` → `{version_info['latest_version']}`"
            )
            st.link_button(
                "⬇️ Download nieuwste versie",
                "https://github.com/cedanl/1cijferho/releases/latest",
                use_container_width=True,
            )
        return True
    return False


# -----------------------------------------------------------------------------
# Pages Overview - YOU CAN ADD MORE PAGES HERE
# -----------------------------------------------------------------------------
home_page = st.Page("frontend/Overview/Home.py", icon="🏠", title="Startpagina")
documentation_page = st.Page(
    "frontend/Overview/Documentation.py", icon="📚", title="Documentatie"
)

data_upload_page = st.Page(
    "frontend/Files/Upload_Data.py", icon="📁", title="Bestanden uploaden"
)

extract_page = st.Page(
    "frontend/Modules/Extract_Metadata.py", icon="🔍", title="Metadata extraheren"
)
validate_page = st.Page(
    "frontend/Modules/Validate_Metadata.py", icon="🛡️", title="Metadata valideren"
)
turbo_convert_page = st.Page(
    "frontend/Modules/Turbo_Convert.py", icon="⚡", title="Turbo Conversie"
)
tip_page = st.Page("frontend/Modules/Tip.py", icon="💡", title="Tip")

# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------
# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# Demo Mode Toggle & Notifications
show_demo_toggle_and_notifications()

# Version Check
check_repo_version()
show_version_notification()

# Initialize Navigation
pg = st.navigation(
    {
        "Overview": [home_page, documentation_page],
        "Files": [data_upload_page],
        "Modules": [extract_page, validate_page, turbo_convert_page, tip_page],
    }
)

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()

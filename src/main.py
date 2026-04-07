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
import requests
from pathlib import Path
from config import get_initial_demo_mode

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
# Global CSS — applied once, used across all pages
# -----------------------------------------------------------------------------
st.markdown("""
<style>
/* Consistent page intro strip — left accent + context */
.page-intro {
    border-left: 3px solid #667eea;
    padding: 0.4rem 0 0.4rem 1rem;
    margin-bottom: 1.25rem;
    color: #555;
    font-size: 0.95rem;
    line-height: 1.5;
}

/* Small step badge shown above page titles */
.step-badge {
    display: inline-block;
    background: #ebebf8;
    color: #5a5acc;
    font-size: 0.72rem;
    font-weight: 700;
    padding: 0.18rem 0.55rem;
    border-radius: 10px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.35rem;
}

/* Status row — compact inline status under title */
.status-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.9rem;
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Initialize Session State
# -----------------------------------------------------------------------------
if "demo_mode" not in st.session_state:
    st.session_state.demo_mode = get_initial_demo_mode()




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
    "frontend/Modules/Extract_Metadata.py", icon="🔍", title="Stap 1 · Metadata extraheren"
)
validate_page = st.Page(
    "frontend/Modules/Validate_Metadata.py", icon="🛡️", title="Stap 2 · Metadata valideren"
)
turbo_convert_page = st.Page(
    "frontend/Modules/Turbo_Convert.py", icon="⚡", title="Stap 3 · Turbo Conversie"
)
validate_output_page = st.Page(
    "frontend/Modules/Validate_Output.py", icon="🔎", title="Stap 4 · Output valideren"
)
tip_page = st.Page("frontend/Modules/Tip.py", icon="💡", title="Tip")
configure_columns_page = st.Page(
    "frontend/Modules/Configure_Columns.py", icon="⚙️", title="Kolomselectie"
)

# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------
# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# Version Check
check_repo_version()
show_version_notification()

# Initialize Navigation
pg = st.navigation(
    {
        "Overview": [home_page, documentation_page],
        "Files": [data_upload_page],
        "Modules": [extract_page, validate_page, turbo_convert_page, configure_columns_page, validate_output_page, tip_page],
    }
)

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()

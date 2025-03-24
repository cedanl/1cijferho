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

# -----------------------------------------------------------------------------
# Pages Overview - YOU CAN ADD MORE PAGES HERE
# -----------------------------------------------------------------------------
home_page = st.Page("frontend/Home.py", icon=":material/home:")
upload_files_page = st.Page("frontend/Files/Upload_Files.py", icon=":material/file_upload:")
data_explorer_page = st.Page("frontend/Data_Explorer.py", icon=":material/explore:")


# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------

# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# Initialize Navigation
pg = st.navigation ( {
    "Overview": [home_page],
    "Files": [data_explorer_page, upload_files_page],
})

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()
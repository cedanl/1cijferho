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
# Pages Configuration
# -----------------------------------------------------------------------------
# You can add more pages here

home_page = st.Page("frontend/Home.py", title="Home", icon=":material/home:", default=True)
upload_files_page = st.Page("frontend/Files/Upload_Files.py", title="Upload Files", icon=":material/file_upload:")



# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------

# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# 


pg = st.navigation ( {
    "Overview": [home_page],
    "Files": [upload_files_page]
})

# General Page Configuration
st.set_page_config(page_title="CEDA | 1cijferho", page_icon="📊")

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()
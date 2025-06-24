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
home_page = st.Page("frontend/Overview/Home.py", icon=":material/home:")

data_upload_page = st.Page("frontend/Files/Data_Upload.py", icon=":material/upload_file:")
data_explorer_page = st.Page("frontend/Files/Data_Explorer.py", icon=":material/explore:")
magic_converter_page = st.Page("frontend/Files/Magic_Converter.py", icon="✨")



# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------
# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# Initialize Navigation
pg = st.navigation ( {
    "Overview": [home_page],
    "Files": [data_upload_page],
    "Modules": [data_explorer_page, magic_converter_page],
})

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()
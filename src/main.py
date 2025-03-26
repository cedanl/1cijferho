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
data_explorer_page = st.Page("frontend/Files/Data_Explorer.py", icon=":material/explore:")
magic_converter_page = st.Page("frontend/Files/Magic_Converter.py", icon="✨")

ev_page = st.Page("frontend/Visualisations/EV.py", icon="📓")
vakhavw_page = st.Page("frontend/Visualisations/VAKHAVW.py", icon="📓")
# -----------------------------------------------------------------------------
# Session State Management
# -----------------------------------------------------------------------------
# Initialize session state if not already done
#if 'INPUT_FOLDER' not in st.session_state:
#    st.session_state.INPUT_FOLDER = "data/01-input"
    
# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------
# Add Logo
LOGO_URL = "src/assets/npuls_logo.png"
st.logo(LOGO_URL)

# Initialize Navigation
pg = st.navigation ( {
    "Overview": [home_page],
    "Files": [data_explorer_page, magic_converter_page],
    "Analytics": [ev_page, vakhavw_page]
})

# -----------------------------------------------------------------------------
# Run the app
# -----------------------------------------------------------------------------
pg.run()
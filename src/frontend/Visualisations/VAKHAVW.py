import streamlit as st
import frontend.Visualisations.helper as helper

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="VAKHAVW",
    layout="wide",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("VAKHAVW")
st.caption("DEMO - VAKHAVW Analytics")

# Logic 
# Load EV File 
dfVAKHAVW = helper.find_and_load_vakhavw_csv("data/02-output")
st.data_editor(dfVAKHAVW.head(1000))  # Show first 1000 rows

# - Inladen + Types + PascalCase
# - data summary -> if we have time and last tab
# - AfkortingVak Visualization
# - Get trends visualization
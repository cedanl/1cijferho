import streamlit as st
import frontend.Visualisations.EV_helper as helper

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="EV",
    layout="wide",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("EV")
st.caption("DEMO - EV Analytics")

# Load EV File 
dfEV = helper.find_and_load_ev_csv("data/02-output")
st.dataframe(dfEV.head(1000))  # Show first 1000 rows

# Logic

# - Inladen + Types + PascalCase
# - intake_visualization
# - data summary -> if we have time and last tab
# - diploma types
# - performance visualization
# - score visualisation
# - trends visualisation
import streamlit as st

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
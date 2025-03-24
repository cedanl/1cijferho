import streamlit as st

# TODO
# - Add gradient line between header and subtitle
# - Add button to data explorer page & explain what it does
# - Remove upload button & text

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="CEDA | 1cijferho ",
    layout="centered",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("🚀 1cijferho")
st.caption("Transform complex DUO datasets into actionable insights in minutes, not months. ✨")

# Features
st.caption("📊 Instant visualization • ⏱️ Time-saving automation • 🔍 Intelligent error reduction • 👥 Enhanced accessibility • 🔄 Batch processing")

# Overview
st.write("""
Our application decodes and delimits all 1CHO files, enabling researchers to access educational 
data without technical expertise. We also provide pre-made visualizations and data sets 
based on 1CHO for immediate insights.
""")


st.subheader("📢 Get Involved")
st.write("We're constantly improving based on your feedback! Share your ideas by emailing us at a.sewnandan@hhs.nl or t.iwan@vu.nl, or submit a feature request:")

# Adding an inline button for GitHub issues
st.link_button("Submit Feature Request", url="https://github.nl/cedanl/1cijferho/issues", help="Opens our GitHub issues page")

# Divider before Demo section
st.divider()

# -----------------------------------------------------------------------------
# Demo Section
# -----------------------------------------------------------------------------

# Demo section
st.header("✨ Try the Application")
st.write("Upload your 1CHO data and discover insights in minutes. Click below to get started.")
if st.button(":material/explore: Data Explorer", help="Opens the Data Explorer"):
    st.switch_page("frontend/Files/Data_Explorer.py")
# Footer section
st.write("© 2025 CEDA | Bridging institutions, sharing solutions, advancing education.")
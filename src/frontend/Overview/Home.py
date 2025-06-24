import streamlit as st

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="🚀 1cijferho | CEDA",
    layout="centered",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("🚀 1cijferho")

# Intro text
st.write("""
Transform complex DUO datasets into actionable insights in minutes, not months. Our application decodes and delimits all 1CHO files, enabling researchers to access educational data without technical expertise.
""")

# Beta version info
st.info("🔧 This is beta version (v0.9). Your feedback is appreciated!")

# Try the application section
st.write("Ready to get started? Upload your 1CHO data and discover insights in minutes:")

# Side-by-side buttons with equal width
col1, col2 = st.columns(2)

with col1:
    data_upload_clicked = st.button("📁 Upload Data", type="primary", use_container_width=True)

with col2:
    documentation_clicked = st.button("📚 Documentation", type="secondary", use_container_width=True)

# Handle button clicks
if data_upload_clicked:
    st.switch_page("frontend/Files/Upload_Data.py")  # Replace with your actual data upload page path

if documentation_clicked:
    st.switch_page("frontend/Documentation.py")  # Replace with your actual documentation page path

# Divider
st.divider()

# -----------------------------------------------------------------------------
# Get Involved Section
# -----------------------------------------------------------------------------
st.subheader("📢 Get Involved")
st.write("We're constantly improving based on your feedback! Share your ideas by emailing us at a.sewnandan@hhs.nl or t.iwan@vu.nl, or submit a feature request:")

# GitHub issues link
st.link_button("Submit Feature Request", url="https://github.nl/cedanl/1cijferho/issues", help="Opens our GitHub issues page")

# -----------------------------------------------------------------------------
# Footer Section
# -----------------------------------------------------------------------------
st.caption("© 2025 CEDA | Bridging institutions, sharing solutions, advancing education.")
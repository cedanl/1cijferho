import streamlit as st

# -----------------------------------------------------------------------------
# Custom CSS for sleek banner
# -----------------------------------------------------------------------------
st.markdown("""
<style>
.hero-banner {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1.5rem;
    border-radius: 8px;
    color: white;
    margin-bottom: 1.5rem;
    text-align: center;
}

.hero-banner h1 {
    margin: 0;
    font-size: 2.2rem;
    font-weight: 700;
}

.hero-banner p {
    margin: 0.5rem 0 0 0;
    font-size: 1.1rem;
    opacity: 0.9;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Hero Banner
# -----------------------------------------------------------------------------
st.markdown("""
<div class="hero-banner">
    <h1>🚀 Welcome to 1CijferHO</h1>
    <p>Unlock the power of your educational datasets</p>
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------

# Intro text
st.write("""
Transform complex DUO datasets into actionable insights in minutes, not months. Our application decodes and delimits all 1CHO files, enabling researchers to access educational data without technical expertise.
""")

# Beta version info
st.info("✨ v0.9 - All core features ready! Help us perfect it with your feedback.", icon="🎯")

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
    st.switch_page("frontend/Overview/Documentation.py")  # Replace with your actual documentation page path

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
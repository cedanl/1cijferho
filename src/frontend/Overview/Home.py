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
    <h1>🚀 Welkom bij 1CijferHO</h1>
    <p>Verwerk DUO-onderwijsdata naar onderzoeksklare bestanden</p>
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------

# Intro text
st.write("""
Zet DUO-bestanden automatisch om naar bruikbare CSV- en Parquet-bestanden. Geen technische kennis vereist.
""")

# Try the application section
st.write("Begin door uw databestanden hieronder te uploaden:")

# Side-by-side buttons with equal width
col1, col2 = st.columns(2)

with col1:
    data_upload_clicked = st.button("📁 Bestanden uploaden", type="primary", use_container_width=True)

with col2:
    documentation_clicked = st.button("📚 Documentatie", type="secondary", use_container_width=True)

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
st.subheader("📢 Doe mee")
st.write("We verbeteren continu op basis van uw feedback! Vragen of suggesties? Dien een verzoek in via GitHub Issues:")

# GitHub issues link
st.link_button("Dien een verzoek in", url="https://github.nl/cedanl/1cijferho/issues", help="Opent onze GitHub issues pagina", type="primary")

# -----------------------------------------------------------------------------
# Footer Section
# -----------------------------------------------------------------------------
st.caption("© 2025 CEDA | Verbinding tussen instellingen, kennisdeling, vooruitgang in onderwijs.")

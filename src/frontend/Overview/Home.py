import streamlit as st
from config import set_demo_mode

# -----------------------------------------------------------------------------
# Hero Banner
# -----------------------------------------------------------------------------
st.markdown("""
<style>
.hero-banner {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 2rem 1.5rem;
    border-radius: 10px;
    color: white;
    margin-bottom: 1.75rem;
    text-align: center;
}
.hero-banner h1 {
    margin: 0 0 0.4rem 0;
    font-size: 2.2rem;
    font-weight: 700;
    letter-spacing: -0.01em;
}
.hero-banner p {
    margin: 0;
    font-size: 1.05rem;
    opacity: 0.88;
    line-height: 1.5;
}
.how-card {
    background: #f8f8fd;
    border: 1px solid #e4e4f0;
    border-radius: 8px;
    padding: 1rem 1rem 0.75rem 1rem;
    text-align: center;
    height: 100%;
}
.how-card .step-num {
    font-size: 0.7rem;
    font-weight: 700;
    color: #9090c0;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 0.3rem;
}
.how-card .step-title {
    font-size: 1rem;
    font-weight: 600;
    color: #1a1a2e;
    margin-bottom: 0.3rem;
}
.how-card .step-desc {
    font-size: 0.83rem;
    color: #666;
    line-height: 1.45;
}
.mode-divider {
    text-align: center;
    color: #999;
    font-size: 0.82rem;
    margin: 0.25rem 0;
    font-style: italic;
}
</style>

<div class="hero-banner">
    <h1>1CijferHO</h1>
    <p>Zet ruwe DUO-onderwijsdata automatisch om naar CSV- en Parquet-bestanden —<br>klaar voor analyse, zonder technische kennis.</p>
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# How it works — 3 steps
# -----------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("""
    <div class="how-card">
        <div class="step-num">Stap 1</div>
        <div class="step-title">Bestanden plaatsen</div>
        <div class="step-desc">Kopieer uw DUO-bestanden naar de invoermap op de server.</div>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown("""
    <div class="how-card">
        <div class="step-num">Stap 2 – 3</div>
        <div class="step-title">Verwerken</div>
        <div class="step-desc">Extraheer metadata, valideer de structuur en converteer naar leesbare kolommen.</div>
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown("""
    <div class="how-card">
        <div class="step-num">Klaar</div>
        <div class="step-title">Bestanden ophalen</div>
        <div class="step-desc">CSV- en Parquet-bestanden staan klaar in de uitvoermap.</div>
    </div>
    """, unsafe_allow_html=True)

st.write("")

# -----------------------------------------------------------------------------
# Mode Choice — primary decision point
# -----------------------------------------------------------------------------
st.markdown("#### Waarmee wilt u beginnen?")

col_own, col_demo = st.columns(2)

with col_own:
    own_clicked = st.button(
        "Eigen data uploaden",
        type="primary",
        use_container_width=True,
        help="U heeft DUO-bestanden en wilt deze verwerken.",
    )
    st.caption("Ik heb DUO-bestanden klaarstaan.")

with col_demo:
    demo_clicked = st.button(
        "Probeer met demo",
        type="secondary",
        use_container_width=True,
        help="Geen eigen bestanden? Verken de tool met voorbeelddata.",
    )
    st.caption("Geen bestanden? Verken de tool met voorbeelddata.")

if own_clicked:
    set_demo_mode(False)
    st.switch_page("frontend/Files/Upload_Data.py")

if demo_clicked:
    set_demo_mode(True)
    st.switch_page("frontend/Files/Upload_Data.py")

# -----------------------------------------------------------------------------
# Footer
# -----------------------------------------------------------------------------
st.divider()
col_doc, col_footer = st.columns([1, 2])
with col_doc:
    if st.button("Documentatie", type="secondary", use_container_width=True):
        st.switch_page("frontend/Overview/Documentation.py")
with col_footer:
    st.caption("© 2026 CEDA · Npuls — Vragen of feedback? [GitHub Issues](https://github.com/cedanl/1cijferho/issues)")

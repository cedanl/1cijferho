import streamlit as st

st.markdown("""
<style>
.done-banner {
    background: linear-gradient(135deg, #43b89c 0%, #2d8a6e 100%);
    padding: 1.25rem 1.5rem;
    border-radius: 10px;
    color: white;
    margin-bottom: 1.5rem;
}
.done-banner h3 {
    margin: 0 0 0.2rem 0;
    font-size: 1.3rem;
    font-weight: 700;
}
.done-banner p {
    margin: 0;
    font-size: 0.92rem;
    opacity: 0.9;
}
</style>
<div class="done-banner">
    <h3>De pipeline is klaar</h3>
    <p>Uw bestanden staan klaar in <code>data/02-output/</code>. Hieronder vindt u voorbeelden om ze direct te laden in Python of R.</p>
</div>
""", unsafe_allow_html=True)

st.subheader("CSV- en Parquet-bestanden laden")
st.caption("Gebruik puntkomma als scheidingsteken en latin-1 codering voor CSV-bestanden.")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Python**")

    st.markdown("CSV")
    st.code("""\
import polars as pl

df = pl.read_csv(
    "bestand.csv",
    separator=";",
    encoding="latin1",
)""", language="python")

    st.markdown("Parquet")
    st.code("""\
import polars as pl

df = pl.read_parquet("bestand.parquet")""", language="python")

    st.caption("Bibliotheek: [polars](https://pola.rs/)")

with col2:
    st.markdown("**R**")

    st.markdown("CSV")
    st.code("""\
library(data.table)

df <- fread(
  "bestand.csv",
  sep = ";",
  encoding = "Latin-1"
)""", language="r")

    st.markdown("Parquet")
    st.code("""\
library(arrow)

df <- read_parquet("bestand.parquet")""", language="r")

    st.caption("Bibliotheken: [data.table](https://rdatatable.gitlab.io/data.table/) · [arrow](https://arrow.apache.org/docs/r/)")

st.divider()

col_home, col_spacer = st.columns([1, 2])
with col_home:
    if st.button("← Terug naar startpagina", type="secondary", use_container_width=True):
        st.switch_page("frontend/Overview/Home.py")

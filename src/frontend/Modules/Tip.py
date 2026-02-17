import streamlit as st  

st.title("💡 Tip")
         
st.write("**📊 CSV- en Parquet-bestanden lezen**")

st.markdown("Eenvoudige voorbeelden om bestanden sneller te lezen met **puntkomma als scheidingsteken** en **latin-1 codering**")
# Create two columns for Python and R
col1, col2 = st.columns(2)

with col1:
    st.header("🐍 Python")
    
    st.subheader("CSV met puntkomma als scheidingsteken")
    st.code("""
import polars as pl

# Read CSV with semicolon delimiter and latin-1 encoding
df = pl.read_csv('file.csv', 
                 separator=';', 
                 encoding='latin1')
""", language="python")
    
    st.subheader("Parquet-bestand")
    st.code("""
import polars as pl

# Read Parquet file
df = pl.read_parquet('file.parquet')
""", language="python")
    
    st.markdown("**Python Libraries:**")
    st.markdown("- [polars](https://pola.rs/)")

with col2:
    st.header("📈 R")
    
    st.subheader("CSV met puntkomma als scheidingsteken")
    st.code("""
library(data.table)

# Read CSV with semicolon delimiter and latin-1 encoding
df <- fread('file.csv', 
            sep = ';', 
            encoding = 'Latin-1')
""", language="r")
    
    st.subheader("Parquet-bestand")
    st.code("""
library(arrow)

# Read Parquet file
df <- read_parquet('file.parquet')
""", language="r")
    
    st.markdown("**R-pakketten:**")
    st.markdown("- [data.table](https://rdatatable.gitlab.io/data.table/)")
    st.markdown("- [arrow](https://arrow.apache.org/docs/r/) (for parquet)")
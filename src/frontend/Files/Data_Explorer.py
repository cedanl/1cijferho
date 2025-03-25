import streamlit as st
import os
import polars as pl
import frontend.Files.Data_Explorer_helper as de_helper

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Data Explorer",
    layout="wide",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title(":material/explore: Data Explorer")
st.write("""
Copy your 1CHO files to the `data/01-input` folder in the repository. This page will pre-process your Bestandsbeschrijvingen 
(stored as .xlsx files in `data/00-metadata`, which you can edit if needed) and display which of your files can be successfully matched. 
Once you've reviewed the matches, you can proceed to the magic converter to transform your data.
""")

st.warning("⚠️ Please copy your 1CHO files to the data/01-input folder in this repository to continue.")

#---------------------
### Overview Files
#----------------------
# Create subheader for the file tabs
st.subheader("Overview of your 1CHO Files")

# Tabs p
tab1, tab2, tab3 = st.tabs(["📑 Main Files", "🗃 Bestandsbeschrijvingen", "🔐 Decodeer Files",])
tab1.write("Overview of your Main files")
tab2.write("Overview of your Bestandsbeschrijvingen")
tab3.write("Overview of your Decodeer files")

st.divider()

df = de_helper.get_files_dataframe(st.session_state.INPUT_FOLDER)
if df is not None:
    st.table(df)


st.write(st.session_state.INPUT_FOLDER)

st.divider()
st.header("✨ Transform Your Data")
st.write("Ready to convert your 1CHO files? Our Magic Converter turns complex DUO datasets into clean, analysis-ready data formats with just a few clicks.")
if st.button("✨ Magic Converter", help="Opens the Magic Converter", type="primary"):
    st.switch_page("frontend/Files/Magic_Converter.py")
    

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

#---------------------
### Display Input Folder
#----------------------
st.title("Display Input Folder")
    # Access the session state variable

df = de_helper.get_files_dataframe(st.session_state.INPUT_FOLDER)
if df is not None:
    st.table(df)

st.divider()
st.header("✨ Transform Your Data")
st.write("Ready to convert your 1CHO files? Our Magic Converter turns complex DUO datasets into clean, analysis-ready data formats with just a few clicks.")
if st.button("✨ Magic Converter", help="Opens the Magic Converter"):
    st.switch_page("frontend/Files/Data_Explorer.py")
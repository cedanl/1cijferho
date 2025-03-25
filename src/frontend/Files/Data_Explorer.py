import streamlit as st
import os
import polars as pl
import frontend.Files.Data_Explorer_helper as de_helper
import tkinter as tk
from tkinter import filedialog
# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Data Explorer",
    layout="wide",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Header Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title(":material/explore: Data Explorer")
st.write("""
Copy your 1CHO files to the `data/01-input` folder in the repository. This page will pre-process your Bestandsbeschrijvingen 
(stored as .xlsx files in `data/00-metadata`, which you can edit if needed) and display which of your files can be successfully matched. 
Once you've reviewed the matches, you can proceed to the magic converter to transform your data.
""")

# -----------------------------------------------------------------------------
# Select Folder Section
# -----------------------------------------------------------------------------
# Select Folder Section
# Check if INPUT_FOLDER is already stored in session state
if "INPUT_FOLDER" not in st.session_state:
    st.session_state.INPUT_FOLDER = None

# Display appropriate message
if not st.session_state.INPUT_FOLDER:
    st.warning("⚠️ Please select your 1CHO folder to continue.")
else:
    st.success(f"✅ Selected folder: {st.session_state.INPUT_FOLDER}")

# Button to select folder
if st.button("Select Folder"):
    st.session_state.INPUT_FOLDER = de_helper.select_folder()

# Display selected folder path (optional, since success already shows it)
if st.session_state.INPUT_FOLDER:
    st.write("Selected folder path:", st.session_state.INPUT_FOLDER)

#---------------------
### Overview Files
#----------------------
st.subheader("Overview of your 1CHO Files")

df = de_helper.get_files_dataframe(st.session_state.INPUT_FOLDER)

# Configure Tabs
tab1, tab2, tab3 = st.tabs(["📑 Main Files", "🗃 Bestandsbeschrijvingen", "🔐 Decodeer Files",])

# Tab 1 - Main Files
with tab1.expander("Main Files"):
    tab1.table(de_helper.get_main_files(df))

# Tab 2 - Bestandsbeschrijvingen
tab2.table(de_helper.get_bestandsbeschrijving_files(df))

# Tab 3 - Decodeer Files
tab3.table(de_helper.get_dec_files(df))


st.divider()
st.header("✨ Transform Your Data")
st.write("Ready to convert your 1CHO files? Our Magic Converter turns complex DUO datasets into clean, analysis-ready data formats with just a few clicks.")
if st.button("✨ Magic Converter", help="Opens the Magic Converter", type="primary"):
    st.switch_page("frontend/Files/Magic_Converter.py")
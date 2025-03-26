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
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Header Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title(":material/explore: Data Explorer")
st.write("""
Select the folder containing your unzipped 1CHO files, or choose the demo folder if you don't have access.
Below you'll see an overview of the 3 types of files present. For the main files, make sure they are directly
in the selected directory (not nested): EV, VAKHAVW, Croho & Croho_vest.

If the Bestandbeschrijvingen are present, the application will ✨ **automatically** ✨ extract the necessary
information and save them in the `data/00-metadata` directory in this repository.

Once you've reviewed the matches, you can proceed to the magic converter to transform your data.
""")

# -----------------------------------------------------------------------------
# Select Folder Section
# -----------------------------------------------------------------------------
# Check if INPUT_FOLDER is already stored in session state
if "INPUT_FOLDER" not in st.session_state:
    st.session_state.INPUT_FOLDER = None

# Display appropriate message if not st.session_state.INPUT_FOLDER
if not st.session_state.INPUT_FOLDER:
    st.warning("⚠️ Please select your 1CHO folder to continue.")
else:
    st.success(f"✅ Selected folder: {st.session_state.INPUT_FOLDER}")

# Button to select folder
if st.button("Select Folder"):
    st.session_state.INPUT_FOLDER = de_helper.select_folder()

# Button to select demo folder
if st.button("Demo Folder"):
    st.session_state.INPUT_FOLDER = "data/01-input/demo"

# Display selected folder path (optional, since success already shows it)
if st.session_state.INPUT_FOLDER:
    st.write("Selected folder path:", st.session_state.INPUT_FOLDER)

    # -----------------------------------------------------------------------------
    # Overview Files Section - Only displayed when INPUT_FOLDER is present
    # -----------------------------------------------------------------------------
    st.subheader("Overview of your 1CHO Files")
    df = de_helper.get_files_dataframe(st.session_state.INPUT_FOLDER)
    
    # Configure Tabs
    tab1, tab2, tab3 = st.tabs(["📑 Main Files", "🗃 Bestandsbeschrijvingen", "🔐 Decodeer Files",])
    
    # Tab 1 - Main Files
    with tab1:
        with st.expander("Main Files", expanded=True):
            st.table(de_helper.get_main_files(df))
    
    # Tab 2 - Bestandsbeschrijvingen
    with tab2:
        st.table(de_helper.get_bestandsbeschrijving_files(df))
    
    # Tab 3 - Decodeer Files
    with tab3:
        st.table(de_helper.get_dec_files(df))

    st.divider()
    
    st.header("✨ Transform Your Data")
    st.write("Ready to convert your 1CHO files? Our Magic Converter turns complex DUO datasets into clean, analysis-ready data formats with just a few clicks.")
    
    if st.button("✨ Magic Converter", help="Opens the Magic Converter", type="primary"):
        st.switch_page("frontend/Files/Magic_Converter.py")
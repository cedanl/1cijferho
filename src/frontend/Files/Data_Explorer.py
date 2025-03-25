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
    
def select_folder():
   root = tk.Tk()
   root.withdraw()
   folder_path = filedialog.askdirectory(master=root)
   root.destroy()
   return folder_path

selected_folder_path = st.session_state.get("folder_path", None)
folder_select_button = st.button("Select Folder")
if folder_select_button:
  selected_folder_path = select_folder()
  st.session_state.folder_path = selected_folder_path

if selected_folder_path:
   st.write("Selected folder path:", selected_folder_path)
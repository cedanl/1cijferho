import streamlit as st
import os
import polars as pl
import frontend.Files.Data_Explorer_helper as de_helper
import tkinter as tk
from tkinter import filedialog
from PIL import Image
from backend.core import extractor as ex
#from src.backend.validation import extractor_validation as ex_val
#from src.backend.core import converter_match as cm

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Data Explorer",
    layout="centered",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Header Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title(":material/explore: Data Explorer")
st.write("""
Select the folder containing your unzipped 1CHO files, or choose the demo folders. Make sure the following files are present in the 
selected folder (not nested): 

**EV | VAKHAVW | Croho | Croho_vest**
""")

with st.expander("📂 View Example Directory Structure"):
    # Add some explanatory text
    st.write("""
    ### Example Folder
    
    Your folder should look similar to the structure shown below. 
    Note that the exact files may vary depending on your institution's specific requirements.
    """)
    
    # Path to the image
    st.image("src/assets/example_files.png") 

# -----------------------------------------------------------------------------
# Select Folder Section
# -----------------------------------------------------------------------------
# Check if INPUT_FOLDER is already stored in session state
if "INPUT_FOLDER" not in st.session_state:
    st.session_state.INPUT_FOLDER = None

# Display appropriate message if not st.session_state.INPUT_FOLDER
if not st.session_state.INPUT_FOLDER:
    st.warning("⚠️ Please select your 1CHO folder. Otherwise the demo folder will be used.")
    st.session_state.DEMO_FOLDER = True
else:
    st.info(f":material/info: Selected folder: {st.session_state.INPUT_FOLDER}")
    st.session_state.DEMO_FOLDER = False

# Button to select folder
if st.button("Select Folder"):
    st.session_state.INPUT_FOLDER = de_helper.select_folder()
    st.rerun()
    
if st.session_state.DEMO_FOLDER:
    st.warning("⚠️ The demo folder is currently selected")

st.divider()
# -----------------------------------------------------------------------------
# Overview Files Section - Only displayed when INPUT_FOLDER is present
# -----------------------------------------------------------------------------
if st.session_state.INPUT_FOLDER:
    
    st.subheader(" ⚙️ File Preparation")
    df = de_helper.get_files_dataframe(st.session_state.INPUT_FOLDER)
    df = de_helper.categorize_files(df)
    
     # Add explanation text
    st.markdown("""
    This page will extract specifications from the **Bestandsbeschrijving** files to determine proper file delimiters.
    It will then attempt to match them with **Main** and **Decodeer** files.
    
    ⚠️ **Note:** Results are saved as Excel files in `data/00-metadata`. Any existing files will be overwritten when you press "Find Matches".
    """)
    
    
    # Configure Tabs
    tab1, tab2 = st.tabs(["🔥 Matches","🗃 Files"])

    with tab1:
        # Check if required file types are present
        required_types = ["Main File", "Bestandsbeschrijving", "Decodeer File"]
        
        # Check which required types are present
        found_types = []
        for req_type in required_types:
            if df.filter(pl.col("Type") == req_type).height > 0:
                found_types.append(req_type)
        
        # Determine which types are missing
        missing_types = [req_type for req_type in required_types if req_type not in found_types]
        
        # Display appropriate message based on what was found
        if len(found_types) == len(required_types):
            st.success("All required files found in the data!")
            if st.button("Find Matches", type="primary"):
                ex.process_txt_folder(st.session_state.INPUT_FOLDER)
                ex.process_json_folder()
                ex_val.validate_metadata_folder()
                cm.match_metadata_inputs()

            
        elif len(found_types) > 0:
            st.warning(f"Warning! Some required files are missing: {', '.join(missing_types)}")
        else:
            st.warning("Warning! No required files found in the data, please check the files tab for more information.")
            
    with tab2:
        # Define the options for the segmented control
        filter_options = ["All", "Main Files", "Bestandbeschrijvingen", "Decodeer Files"]
        
        # Create the segmented control
        selected_filter = st.segmented_control(label="Filter by type", options=filter_options)
        
        # Apply filtering based on the selected option
        filtered_df = df
        if selected_filter == "Main Files":
            filtered_df = df.filter(pl.col("Type") == "Main File")
        elif selected_filter == "Bestandbeschrijvingen":
            filtered_df = df.filter(pl.col("Type") == "Bestandsbeschrijving")
        elif selected_filter == "Decodeer Files":
            filtered_df = df.filter(pl.col("Type") == "Decodeer File")
    
        # Display the filtered table
        st.table(filtered_df)
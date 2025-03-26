import streamlit as st
import os
import polars as pl
import frontend.Files.Data_Explorer_helper as de_helper
import tkinter as tk
from tkinter import filedialog
from backend.core import extractor as ex
from backend.validation import extractor_validation as ex_val
from backend.core import converter_match as cm
import time
import random

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
st.info("🔧 This is beta version (v0.5.3). Your feedback is appreciated!")
st.write("""
The Data Explorer processes your unzipped 1CHO files (place files directly in folder, not in subfolders). 

1. Select a folder below
2. Review your uploaded files below
3. Click "Match Files" to analyze compatibility 
4. When the "Magic Converter" appears, click to continue

Metadata will be saved to `data/00-metadata` in Excel format. If matching issues occur, either upload the correct files or manually edit the Bestandsbeschrijvingen Excel files.
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

# -----------------------------------------------------------------------------
# Overview Files Section - Only displayed when INPUT_FOLDER is present
# -----------------------------------------------------------------------------
if st.session_state.INPUT_FOLDER:
    df = de_helper.get_files_dataframe(st.session_state.INPUT_FOLDER)
    df = de_helper.categorize_files(df)
    
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
            st.info(":material/info: All required files found in the data! Press Match Files to continue.")
            
            if st.button("Match Files"):
                progress_bar = st.progress(0)
                
                # Process each step with progress updates and random delays
                progress_bar.progress(0, "Processing text folder...")
                ex.process_txt_folder(st.session_state.INPUT_FOLDER)
                time.sleep(random.uniform(0.7, 1))  # Random delay between 2-4 seconds
                
                progress_bar.progress(25, "Processing JSON folder...")
                ex.process_json_folder()
                time.sleep(random.uniform(0.7, 1))  # Random delay between 2-4 seconds
                
                progress_bar.progress(50, "Validating metadata folder...")
                ex_val.validate_metadata_folder()
                time.sleep(random.uniform(0.7, 1))  # Random delay between 2-4 seconds
                
                progress_bar.progress(75, "Matching metadata inputs...")
                cm.match_metadata_inputs()
                time.sleep(random.uniform(0.7, 1))  # Random delay between 2-4 seconds
                
                progress_bar.progress(100, "Complete!")
                
                # Set a session state flag to indicate matching is complete
                st.session_state.matching_complete = True
                
                with st.expander("✴️ Matching Results"):
                    # Display the matching results
                    dfMatch = pl.read_csv("data/00-metadata/logs/match.csv")
                    st.dataframe(dfMatch, use_container_width=True)
            
            # Check if matching is complete and show the Magic Converter button
            if st.session_state.get('matching_complete', False):
                if st.button("✨ Continue", help="Opens the Magic Converter", type="primary"):
                    # This will be executed on the next rerun after clicking
                    st.switch_page("frontend/Files/Magic_Converter.py")

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
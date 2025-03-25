import streamlit as st
import backend.core.converter as converter
import os
import sys
# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Magic Converter",
    layout="wide",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("✨ Magic Converter")
st.write("Transform complex DUO datasets into actionable insights in minutes, not months. ✨")

# Add the directory containing converter.py to the Python path if needed
# sys.path.append('/path/to/directory/containing/converter.py')

st.title("Fixed-width to CSV Batch Converter")

# Create input fields for file paths
matches_csv = st.text_input("Matches CSV Path", value="data/00-metadata/logs/match.csv")
input_folder = st.text_input("Input Folder", value="data/01-input")
metadata_folder = st.text_input("Metadata Folder", value="data/00-metadata")

#if st.button("Process All Matches"):
#    with st.spinner("Processing all matches..."):
#        try:
#            converter.run_conversions_from_matches(
#                matches_csv=matches_csv,
#                input_folder=input_folder,
#                metadata_folder=metadata_folder
#            )
#            st.success("All conversions completed!")
#        except Exception as e:
#            st.error(f"Error processing matches: {str(e)}")
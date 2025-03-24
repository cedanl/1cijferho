import streamlit as st
import os

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
if 'INPUT_FOLDER' in st.session_state:
    st.write(f"The INPUT_FOLDER path is: {st.session_state.INPUT_FOLDER}")
else:
    st.warning("INPUT_FOLDER has not been set yet. Please go back to the main page.")

def display_files_in_folder(folder_path):
    """
    Display all files in the specified folder using Streamlit
    
    Parameters:
    folder_path (str): Path to the folder containing files
    """
    # Check if the folder exists
    if not os.path.exists(folder_path):
        st.error(f"Folder not found: {folder_path}")
        return
    
    # Get list of files in the folder
    files = os.listdir(folder_path)
    
    # Filter out directories if you only want files
    files = [f for f in files if os.path.isfile(os.path.join(folder_path, f))]
    
    # Display the files
    if files:
        st.subheader("Files in folder:")
        for file in files:
            st.write(f"- {file}")
    else:
        st.info(f"No files found in {folder_path}")

# Example usage:
display_files_in_folder(st.session_state.INPUT_FOLDER)


st.divider()
st.header("✨ Transform Your Data")
st.write("Ready to convert your 1CHO files? Our Magic Converter turns complex DUO datasets into clean, analysis-ready data formats with just a few clicks.")
if st.button("✨ Magic Converter", help="Opens the Magic Converter"):
    st.switch_page("frontend/Files/Data_Explorer.py")
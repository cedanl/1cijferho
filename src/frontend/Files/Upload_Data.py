import streamlit as st
import os
import glob

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def categorize_files():
    """Check if the input directory exists and categorize files found"""
    input_dir = "data/01-input"

    # Create directory if it doesn't exist
    os.makedirs(input_dir, exist_ok=True)
    
    if not os.path.exists(input_dir):
        return False, {}, 0
    
    # Get all files in the directory (any extension, excluding .zip files)
    all_files_paths = glob.glob(os.path.join(input_dir, "*"))
    
    # Filter out directories and .zip files, keep only regular files
    all_files = []
    for file_path in all_files_paths:
        if os.path.isfile(file_path) and not file_path.lower().endswith('.zip'):
            all_files.append(os.path.basename(file_path))
    
    # Categorize files
    categorized_files = {
        "bestandsbeschrijvingen": [],
        "decodeer_files": [],
        "main_files": []
    }
    
    for filename in all_files:
        filename_lower = filename.lower()
        
        # Bestandsbeschrijvingen: .txt files with "bestandsbeschrijving" in name
        if filename_lower.endswith('.txt') and 'bestandsbeschrijving' in filename_lower:
            categorized_files["bestandsbeschrijvingen"].append(filename)
        
        # Decodeer Files: start with "Dec_"
        elif filename.startswith('Dec_'):
            categorized_files["decodeer_files"].append(filename)
        
        # Main Files: start with EV, VAKHAVW, Croho, or Croho_vest
        elif (filename.startswith('EV') or 
              filename.startswith('VAKHAVW') or 
              filename.startswith('Croho') or 
              filename.startswith('Croho_vest')):
            categorized_files["main_files"].append(filename)
    
    # Sort each category
    for category in categorized_files:
        categorized_files[category].sort()
    
    total_files = len(all_files)
    files_found = total_files > 0
    
    return files_found, categorized_files, total_files

# -----------------------------------------------------------------------------
# Header Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("📂 Data Upload")
st.write("""
Follow these steps to get started:

1. **Copy your 1CHO files** to the `data/01-input` directory of this repository
2. **Place files directly** in the folder (not in subfolders)
3. **Refresh this page** to see your uploaded files categorized by type
""")

# Side-by-side buttons for refresh and extract
col1, col2 = st.columns(2)

with col1:
    if st.button("🔄 Refresh Page", type="primary", use_container_width=True):
        st.rerun()

with col2:
    # Check if files exist to enable/disable the extract button
    files_found, _, _ = categorize_files()
    
    if st.button("➡️ Continue to Step 1", type="secondary", disabled=not files_found, use_container_width=True):
        st.switch_page("frontend/Modules/Extract_Metadata.py")

# -----------------------------------------------------------------------------
# Example Directory Structure
# -----------------------------------------------------------------------------
with st.expander("📂 View Example Directory Structure"):
    st.write("""
    ### Example Directory Structure
    
    Your `data/01-input` folder should look similar to the structure shown below. 
    Files will be automatically categorized into three types:
    
    - **📄 Bestandsbeschrijvingen**: .txt files containing "bestandsbeschrijving" in the name
    - **🔓 Decodeer Files**: Files starting with "Dec_"
    - **📊 Main Files**: Files starting with "EV", "VAKHAVW", "Croho", or "Croho_vest"
    
    **Important:** Place files directly in the `data/01-input` folder, not in subfolders.
    """)
    
    # Path to the image (if it exists)
    if os.path.exists("src/assets/example_files.png"):
        st.image("src/assets/example_files.png")

# -----------------------------------------------------------------------------
# File Detection and Categorization Section
# -----------------------------------------------------------------------------
files_found, categorized_files, total_files = categorize_files()

if not files_found:
    st.error("""
    🚨 **No files found in `data/01-input` directory**
    
    Please copy your unzipped 1CHO files to the `data/01-input` directory and refresh this page.
    """)
else:
    st.success(f"""
    ✅ **{total_files} files detected in `data/01-input` directory**
    
    Files have been automatically categorized by type. Review the categories below to ensure all expected files are present.
    """)

    st.markdown("---")
    
    # Display categorized files in uniform columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 📄 Beschrijvingen")
        count = len(categorized_files["bestandsbeschrijvingen"])
        st.metric("Count", count)
        
        if count > 0:
            with st.expander(f"View {count} files"):
                for filename in categorized_files["bestandsbeschrijvingen"]:
                    st.write(f"• `{filename}`")
        else:
            st.info("No files found")
    
    with col2:
        st.markdown("#### 🔓 Decodeer Files")
        count = len(categorized_files["decodeer_files"])
        st.metric("Count", count)
        
        if count > 0:
            with st.expander(f"View {count} files"):
                for filename in categorized_files["decodeer_files"]:
                    st.write(f"• `{filename}`")
        else:
            st.info("No files found")
    
    with col3:
        st.markdown("#### 📊 Main Files")
        count = len(categorized_files["main_files"])
        st.metric("Count", count)
        
        if count > 0:
            with st.expander(f"View {count} files"):
                for filename in categorized_files["main_files"]:
                    st.write(f"• `{filename}`")
        else:
            st.info("No files found")
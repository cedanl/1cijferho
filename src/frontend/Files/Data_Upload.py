import streamlit as st
import os
import glob

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Data Upload",
    layout="centered",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def check_input_directory():
    """Check if the input directory exists and return files found"""
    input_dir = "data/01-input"
    
    if not os.path.exists(input_dir):
        return False, []
    
    # Look for common 1CHO file patterns
    file_patterns = [
        "*.xlsx", "*.xls", "*.csv", "*.txt", "*.xml", "*.json"
    ]
    
    found_files = []
    for pattern in file_patterns:
        files = glob.glob(os.path.join(input_dir, pattern))
        found_files.extend([os.path.basename(f) for f in files])
    
    return len(found_files) > 0, sorted(found_files)

# -----------------------------------------------------------------------------
# Header Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title(":material/explore: Data Upload")
st.write("""
Follow these steps to get started:

1. **Copy your 1CHO files** to the `data/01-input` directory of this repository
2. **Place files directly** in the folder (not in subfolders)
3. **Refresh this page** to see your uploaded files
""")

# -----------------------------------------------------------------------------
# Example Directory Structure
# -----------------------------------------------------------------------------
with st.expander("📂 View Example Directory Structure"):
    st.write("""
    ### Example Directory Structure
    
    Your `data/01-input` folder should look similar to the structure shown below. 
    Note that the exact files may vary depending on your institution's specific requirements.
    
    **Important:** Place files directly in the `data/01-input` folder, not in subfolders.
    """)
    
    # Path to the image (if it exists)
    if os.path.exists("src/assets/example_files.png"):
        st.image("src/assets/example_files.png")


# -----------------------------------------------------------------------------
# File Detection Section
# -----------------------------------------------------------------------------
files_found, file_list = check_input_directory()

if not files_found:
    st.error("""
    🚨 **No files found in `data/01-input` directory**
    
    Please copy your unzipped 1CHO files to the `data/01-input` directory and refresh this page.
    """)
else:
    st.success(f"""
    ✅ **{len(file_list)} files detected in `data/01-input` directory**
    
    Files are ready for processing. You can now proceed with the "Match Files" step.
    """)
    
    # Show files in an expander
    with st.expander(f"📁 View {len(file_list)} detected files"):
        st.write("**Files found in `data/01-input`:**")
        for i, filename in enumerate(file_list, 1):
            st.write(f"{i}. `{filename}`")


# -----------------------------------------------------------------------------
# Next Steps Section
# -----------------------------------------------------------------------------
if files_found:
    st.markdown("---")
    st.write("### 🚀 Ready to Continue?")
    st.write("Your files have been detected. You can now proceed to the next step in your workflow.")
    
    # You can add action buttons here
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔍 Match Files", type="primary", use_container_width=True):
            st.info("File matching functionality would be implemented here.")
    
    with col2:
        if st.button("🔄 Refresh Page", use_container_width=True):
            st.rerun()
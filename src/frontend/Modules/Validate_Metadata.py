import streamlit as st
import os
import glob
import backend.utils.extractor_validation as ex_val
import backend.utils.converter_match as cm
import io
import contextlib

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="🛡️ Validate Metadata",
    layout="centered",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_metadata_files():
    """Get all metadata files from the metadata directory"""
    metadata_dir = "data/00-metadata"
    if not os.path.exists(metadata_dir):
        return []
    
    # Look for JSON and Excel files
    json_files = glob.glob(os.path.join(metadata_dir, "*.json"))
    xlsx_files = glob.glob(os.path.join(metadata_dir, "*.xlsx"))
    
    all_files = []
    
    # Add JSON files
    for file_path in json_files:
        filename = os.path.basename(file_path)
        all_files.append(filename)
    
    # Add Excel files
    for file_path in xlsx_files:
        filename = os.path.basename(file_path)
        all_files.append(filename)
    
    return sorted(all_files)

def check_validation_logs():
    """Check for validation issues in logs 3 and 4"""
    logs_dir = "data/00-metadata/logs"
    if not os.path.exists(logs_dir):
        return
    
    # Check log 3 (validation status)
    log3_path = os.path.join(logs_dir, "03_validation_report.txt")
    if os.path.exists(log3_path):
        try:
            with open(log3_path, 'r', encoding='utf-8') as f:
                log3_content = f.read()
            
            # Check for failed validations
            failed_files = []
            lines = log3_content.split('\n')
            for line in lines:
                if 'status' in line.lower() and 'success' not in line.lower():
                    # Extract filename from the line
                    if '.xlsx' in line or '.json' in line:
                        # Try to extract filename
                        parts = line.split()
                        for part in parts:
                            if '.xlsx' in part or '.json' in part:
                                failed_files.append(part.strip(',:'))
                                break
            
            if failed_files:
                st.warning(f"⚠️ **Validation Issues Found:** The following files have validation errors: {', '.join(set(failed_files))}. Please manually adjust them in the .xlsx files in `data/00-metadata/` and restart the validation.")
                
        except Exception as e:
            st.warning(f"⚠️ **Could not read validation log:** {str(e)}")
    
    # Check log 4 (file matching)
    log4_path = os.path.join(logs_dir, "04_file_matching_report.txt")
    if os.path.exists(log4_path):
        try:
            with open(log4_path, 'r', encoding='utf-8') as f:
                log4_content = f.read()
            
            # Check for unmatched files
            unmatched_files = []
            lines = log4_content.split('\n')
            for line in lines:
                if 'no match' in line.lower() or 'not matched' in line.lower() or 'unmatched' in line.lower():
                    # Extract filename from the line
                    if '.xlsx' in line or '.json' in line or '.csv' in line:
                        # Try to extract filename
                        parts = line.split()
                        for part in parts:
                            if '.xlsx' in part or '.json' in part or '.csv' in part:
                                unmatched_files.append(part.strip(',:'))
                                break
            
            if unmatched_files:
                st.warning(f"⚠️ **File Matching Issues Found:** The following files could not be matched: {', '.join(set(unmatched_files))}. Please check if the corresponding data files exist and restart the validation.")
                
        except Exception as e:
            st.warning(f"⚠️ **Could not read file matching log:** {str(e)}")

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("🛡️ Validate Metadata")

# Intro text
st.write("""
This page validates the extracted metadata files and matches them with corresponding data files for further processing.

The validation process will:
- Validate the structure and content of metadata files
- Check for required fields and data consistency
- Match metadata files with corresponding data files
- Generate validation reports and save them to `data/00-metadata/logs/`
""")

# Get files and display status
metadata_files = get_metadata_files()

# Set input folder
input_folder = "data/01-input"

if not metadata_files:
    st.error("🚨 **No metadata files found in `data/00-metadata`**")
    st.info("💡 Please run the extraction process first to generate metadata files.")
else:
    st.success(f"✅ **{len(metadata_files)} metadata file(s) found**")
    
    # Side-by-side buttons with equal width
    col1, col2 = st.columns(2)
    
    with col1:
        validate_clicked = st.button("⚡ Start Validation", type="primary", use_container_width=True)
    
    with col2:
        # Check if validation results exist to enable/disable the next page button
        # You can adjust this condition based on your next page requirements
        validation_exists = os.path.exists("data/00-metadata/logs") and os.listdir("data/00-metadata/logs")
        
        next_page_clicked = st.button("➡️ Next Step", type="secondary", disabled=not validation_exists, use_container_width=True)
    
    # Handle next page button click
    if next_page_clicked:
        # Replace with your actual next page path
        st.switch_page("frontend/Modules/Next_Page.py")
    
    # Handle validation logic
    if validate_clicked:
        # Reset console log at the start of each validation
        st.session_state.console_log = ""
        
        with st.spinner("Validating..."):
            try:
                st.session_state.console_log += "🔄 Starting validation process...\n"
                st.session_state.console_log += "🛡️ Validating metadata files...\n"
                
                # Capture stdout from validate_metadata_folder
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex_val.validate_metadata_folder()
                st.session_state.console_log += captured_output.getvalue()
                st.session_state.console_log += "✅ Metadata validation completed\n"
                
                st.session_state.console_log += "🔗 Matching files...\n"
                # Capture stdout from match_files
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    cm.match_files(input_folder)
                st.session_state.console_log += captured_output.getvalue()
                st.session_state.console_log += "✅ File matching completed\n"
                st.session_state.console_log += "🎉 Validation completed successfully!\n"
                
                st.success("✅ **Validation completed!** Results saved to `data/00-metadata/logs/`. You can now proceed to the next step.")
                
                # Check for validation issues after validation completes
                check_validation_logs()
                
                # Rerun to update the next step button state
                st.rerun()
                
            except Exception as e:
                st.session_state.console_log += f"❌ Error: {str(e)}\n"
                st.error(f"❌ **Validation failed:** {str(e)}")

    # Console Log expander
    with st.expander("📋 Console Log", expanded=True):
        if 'console_log' in st.session_state and st.session_state.console_log:
            st.code(st.session_state.console_log, language=None)
        else:
            st.info("No validation process started yet. Click 'Start Validation' to begin.")
    
    # Warning about existing validation results
    if os.path.exists("data/00-metadata/logs") and os.listdir("data/00-metadata/logs"):
        st.warning("⚠️ Validation will overwrite existing validation results in `data/00-metadata/logs/`")
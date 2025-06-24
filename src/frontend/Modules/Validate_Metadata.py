import streamlit as st
import os
import glob
import json
import backend.utils.extractor_validation as ex_val
import backend.utils.converter_match as cm
import io
import contextlib

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_metadata_files():
    """Get all metadata files from the metadata directory"""
    metadata_dir = "data/00-metadata"
    if not os.path.exists(metadata_dir):
        return []
    
    xlsx_files = glob.glob(os.path.join(metadata_dir, "*.xlsx"))
    
    all_files = []

    # Add Excel files
    for file_path in xlsx_files:
        filename = os.path.basename(file_path)
        all_files.append(filename)
    
    return sorted(all_files)

def load_validation_logs():
    """Load the latest validation logs and return failure information"""
    logs_dir = "data/00-metadata/logs"
    
    if not os.path.exists(logs_dir):
        return None, None
    
    # Find the latest log files
    xlsx_log_files = glob.glob(os.path.join(logs_dir, "*xlsx_validation_log_latest.json"))
    matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))
    
    xlsx_failures = []
    matching_failures = []
    
    # Load XLSX validation failures
    if xlsx_log_files:
        try:
            with open(xlsx_log_files[0], 'r') as f:
                xlsx_data = json.load(f)
                
            for file_info in xlsx_data.get('processed_files', []):
                if file_info.get('status') == 'failed':
                    issues = file_info.get('issues', {})
                    failure_details = []
                    
                    if issues.get('position_errors'):
                        failure_details.append(f"{len(issues['position_errors'])} position error(s)")
                    if issues.get('length_mismatch'):
                        failure_details.append("length mismatch")
                    if issues.get('duplicates'):
                        failure_details.append(f"{len(issues['duplicates'])} duplicate(s)")
                    
                    xlsx_failures.append({
                        'file': file_info['file'],
                        'details': ', '.join(failure_details) if failure_details else 'validation failed'
                    })
        except (json.JSONDecodeError, FileNotFoundError) as e:
            st.error(f"Error loading XLSX validation log: {e}")
    
    # Load file matching failures
    if matching_log_files:
        try:
            with open(matching_log_files[0], 'r') as f:
                matching_data = json.load(f)
            
            # Unmatched input files
            for file_info in matching_data.get('processed_files', []):
                if file_info.get('status') == 'unmatched':
                    matching_failures.append({
                        'type': 'Unmatched input file',
                        'file': file_info['input_file'],
                        'details': f"No corresponding metadata file found ({file_info.get('row_count', 0)} rows)"
                    })
                elif file_info.get('status') == 'matched':
                    # Check for failed validations within matches
                    for match in file_info.get('matches', []):
                        if match.get('validation_status') == 'failed':
                            matching_failures.append({
                                'type': 'Validation failed',
                                'file': f"{file_info['input_file']} → {match['validation_file']}",
                                'details': 'Metadata validation failed for this file pair'
                            })
            
            # Unmatched validation files
            for unmatched in matching_data.get('unmatched_validation', []):
                matching_failures.append({
                    'type': 'Unmatched metadata file',
                    'file': unmatched['validation_file'],
                    'details': 'No corresponding input file found'
                })
                
        except (json.JSONDecodeError, FileNotFoundError) as e:
            st.error(f"Error loading file matching log: {e}")
    
    return xlsx_failures, matching_failures

def clear_console_log():
    """Clear the console log in session state"""
    if 'validate_console_log' in st.session_state:
        del st.session_state['validate_console_log']

# -----------------------------------------------------------------------------
# Initialize/Clear Console Log on Page Load
# -----------------------------------------------------------------------------
# Clear console log when page is first loaded or refreshed
if 'page_initialized' not in st.session_state:
    clear_console_log()
    st.session_state.page_initialized = True

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("🛡️ Validate Metadata")

# Intro text
st.write("""
**Step 2: Validating & Matching Files**

We'll check your extracted Excel files for errors (DUO occasionally makes mistakes) and match them to your main/dec files so we know which structure belongs to which data.

What happens:
- Validate Excel metadata for issues
- Match Excel files to main/dec files
- Save validation reports to `data/00-metadata/logs/`

If validation or matching fails, you can edit the .xlsx files in `data/00-metadata/` and `data/01-input/` respectively. Check the log below to see which files need attention.
""")
# Get files and display status
metadata_files = get_metadata_files()

# Set input folder
input_folder = "data/01-input"

if not metadata_files:
    st.error("🚨 **No metadata files found in `data/00-metadata`**")
    st.info("💡 Please run the extraction process first to generate metadata files.")
else:
    st.success(f"✅ **{len(metadata_files)} Bestandsbeschrijving Metadata file(s) found**")
    st.info("💡 You are able to proceed, even if you have errors - do this with caution!")
    # Side-by-side buttons with equal width
    col1, col2 = st.columns(2)
    
    with col1:
        validate_clicked = st.button("🛡️ Start Validation", type="primary", use_container_width=True, key="validate_btn")
    
    with col2:
        # Check if validation results exist to enable/disable the next page button
        logs_dir = "data/00-metadata/logs"
        validation_complete = False
        
        if os.path.exists(logs_dir):
            # Check for both required validation log files
            xlsx_log_files = glob.glob(os.path.join(logs_dir, "*xlsx_validation_log_latest.json"))
            matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))
            validation_complete = len(xlsx_log_files) > 0 and len(matching_log_files) > 0
        
        next_page_clicked = st.button("➡️ Continue to Step 3", type="secondary", disabled=not validation_complete, use_container_width=True, key="next_step_btn")
    
    # Handle next page button click
    if next_page_clicked:
        st.switch_page("frontend/Modules/Turbo_Convert.py")
    
    # Load and display validation issues below the buttons
    xlsx_failures, matching_failures = load_validation_logs()
    
    # Show validation issues (no tabs, stacked vertically)
    if xlsx_failures:
        st.warning(f"⚠️ **{len(xlsx_failures)} validation failure(s)** - Check console log below for details or rerun validation") 
    
    # Show file matching issues
    unmatched_input = [f for f in matching_failures if f['type'] == 'Unmatched input file']
    unmatched_metadata = [f for f in matching_failures if f['type'] == 'Unmatched metadata file']
    total_unmatched = len(unmatched_input) + len(unmatched_metadata)
    
    if total_unmatched > 0:
        st.warning(f"⚠️ **{total_unmatched} unmatched file(s)** - Check console log below for details or rerun validation")
    
    # Handle validation logic
    if validate_clicked:
        # Clear console log at the start of each validation
        clear_console_log()
        st.session_state.validate_console_log = ""
        
        with st.spinner("Validating..."):
            try:
                st.session_state.validate_console_log += "🔄 Starting validation process...\n"
                st.session_state.validate_console_log += "🛡️ Validating metadata files...\n"
                
                # Capture stdout from validate_metadata_folder
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex_val.validate_metadata_folder()
                st.session_state.validate_console_log += captured_output.getvalue()
                st.session_state.validate_console_log += "✅ Metadata validation completed\n"
                
                st.session_state.validate_console_log += "🔗 Matching files...\n"
                # Capture stdout from match_files
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    cm.match_files(input_folder)
                st.session_state.validate_console_log += captured_output.getvalue()
                st.session_state.validate_console_log += "✅ File matching completed\n"
                st.session_state.validate_console_log += "🎉 Validation completed successfully!\n"
                
                st.success("✅ **Validation completed!** Results saved to `data/00-metadata/logs/`. You can now proceed to the next step.")
                # Rerun to update the next step button state and show new warnings
                st.rerun()
                
            except Exception as e:
                st.session_state.validate_console_log += f"❌ Error: {str(e)}\n"
                st.error(f"❌ **Validation failed:** {str(e)}")

    # Console Log expander
    with st.expander("📋 Console Log", expanded=True):
        if 'validate_console_log' in st.session_state and st.session_state.validate_console_log:
            st.code(st.session_state.validate_console_log, language=None)
        else:
            st.info("No validation process started yet. Click 'Start Validation' to begin.")
    
    # Warning about existing validation results
    if os.path.exists("data/00-metadata/logs") and os.listdir("data/00-metadata/logs"):
        st.warning("⚠️ Validation will overwrite existing validation results in `data/00-metadata/logs/`")
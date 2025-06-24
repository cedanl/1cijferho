import streamlit as st
import os
import glob
import backend.core.converter as conv
import io
import contextlib

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="⚡ Turbo Convert",
    layout="centered",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_matched_files():
    """Get all matched files from the validation logs"""
    logs_dir = "data/00-metadata/logs"
    if not os.path.exists(logs_dir):
        return []
    
    matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))
    
    matched_count = 0
    if matching_log_files:
        try:
            import json
            with open(matching_log_files[0], 'r') as f:
                matching_data = json.load(f)
            
            for file_info in matching_data.get('processed_files', []):
                if file_info.get('status') == 'matched':
                    matched_count += len(file_info.get('matches', []))
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    
    return matched_count

def clear_console_log():
    """Clear the console log in session state"""
    if 'convert_console_log' in st.session_state:
        del st.session_state['convert_console_log']

# -----------------------------------------------------------------------------
# Initialize/Clear Console Log on Page Load
# -----------------------------------------------------------------------------
# Clear console log when page is first loaded or refreshed
if 'page_initialized_convert' not in st.session_state:
    clear_console_log()
    st.session_state.page_initialized_convert = True

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("⚡ Turbo Convert")

# Intro text
st.write("""
**Step 3: Converting Fixed-Width Data**

We'll now use the validated metadata to split your main/dec files into properly delimited CSV format. This is where the magic happens - transforming fixed-width data into structured, readable CSV files.

What happens:
- Split fixed-width files using validated field positions
- Convert to CSV format with proper headers
- Save processed files to `data/02-output/`

If conversion fails, check the log below for details about which files had issues.
""")

# Get files and display status
matched_files_count = get_matched_files()

if matched_files_count == 0:
    st.error("🚨 **No matched files found**")
    st.info("💡 Please complete the validation step first to match your files.")
else:
    st.success(f"✅ **{matched_files_count} file pair(s) ready for conversion**")
    st.info("💡 You are able to proceed, even with errors - do this with caution!")
    
    # Centered button
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        convert_clicked = st.button("⚡ Start Turbo Convert 🚀", type="primary", use_container_width=True)

    # Handle conversion logic
    if convert_clicked:
        # Reset console log at the start of each conversion
        st.session_state.convert_console_log = ""
        
        with st.spinner("Converting..."):
            try:
                st.session_state.convert_console_log += "🔄 Starting conversion process...\n"
                st.session_state.convert_console_log += "⚡ Processing fixed-width files...\n"
                
                # Capture stdout from the conversion process
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    conv.process_matched_files("data/01-input", "data/00-metadata")
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Fixed-width files processed successfully\n"
                
                st.session_state.convert_console_log += "📊 Generating CSV files...\n"
                # Additional processing steps if needed
                st.session_state.convert_console_log += "✅ CSV files generated\n"
                st.session_state.convert_console_log += "🎉 Conversion completed successfully!\n"
                
                st.success("✅ **Conversion completed!** Files saved to `data/02-output/`, Logs saved to `data/00-metadata/logs/`")
                
                # Rerun to update any button states
                st.rerun()
                
            except Exception as e:
                st.session_state.convert_console_log += f"❌ Error: {str(e)}\n"
                st.error(f"❌ **Conversion failed:** {str(e)}")

    # Console Log expander
    with st.expander("📋 Console Log", expanded=True):
        if 'convert_console_log' in st.session_state and st.session_state.convert_console_log:
            st.code(st.session_state.convert_console_log, language=None)
        else:
            st.info("No conversion process started yet. Click 'Start Turbo Convert' to begin.")
    
    # Warning about existing files
    if os.path.exists("data/02-output") and os.listdir("data/02-output"):
        st.warning("⚠️ Conversion will overwrite existing files in `data/02-output/`")

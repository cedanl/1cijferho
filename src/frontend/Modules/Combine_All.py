import streamlit as st
import os
import subprocess
import io
import contextlib
import glob

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
#st.set_page_config(
#    page_title="🔗 Combine All",
#    layout="centered",
#    initial_sidebar_state="expanded"
#)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

def get_processed_files():
    """Get all processed files from data/02-processed"""
    processed_dir = "data/02-processed"
    if not os.path.exists(processed_dir):
        return []

    files = []
    for file in os.listdir(processed_dir):
        if file.endswith('.csv') and not file.endswith('_encrypted.csv'):
            file_path = os.path.join(processed_dir, file)
            file_size = os.path.getsize(file_path)
            files.append({
                'name': file,
                'size': file_size,
                'size_formatted': format_file_size(file_size)
            })

    # Sort files by name
    files.sort(key=lambda x: x['name'])
    return files

def get_combined_files():
    """Get all combined files from data/03-combined"""
    combined_dir = "data/03-combined"
    if not os.path.exists(combined_dir):
        return []

    files = []
    for file in os.listdir(combined_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(combined_dir, file)
            file_size = os.path.getsize(file_path)
            files.append({
                'name': file,
                'size': file_size,
                'size_formatted': format_file_size(file_size)
            })

    # Sort files by name
    files.sort(key=lambda x: x['name'])
    return files

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    size_names = ["B", "KB", "MB", "GB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"

def check_yaml_config():
    """Check if YAML configuration files exist"""
    decoding_exists = os.path.exists("decoding_files_config.yaml")
    mapping_exists = os.path.exists("mapping_tables_config.yaml")
    return decoding_exists and mapping_exists

def start_combination():
    """Callback function to start the combination process"""
    st.session_state.start_combine_all = True

def clear_console_log():
    """Clear the console log in session state"""
    if 'combine_console_log' in st.session_state:
        del st.session_state['combine_console_log']

# -----------------------------------------------------------------------------
# Initialize/Clear Console Log on Page Load
# -----------------------------------------------------------------------------
# Clear console log when page is loaded
clear_console_log()

# Initialize combination trigger
if 'start_combine_all' not in st.session_state:
    st.session_state.start_combine_all = False

# Set page initialization flag
st.session_state.page_initialized_combine = True

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("🔗 Combine All")

# Intro text
st.write("""
**Step 4: Combine Data with Decoder Files (Final Step)**

This final step combines your processed CSV files with all decoder files according to the YAML configuration. This creates enriched datasets with human-readable labels and additional context - your complete research-ready dataset!

What happens:
- Load processed CSV files from `data/02-processed/`
- Apply YAML-configured joins with decoder files
- **NEW:** Smart column prefixes that match your chosen case style
- **snake_case:** `code_land_naam_land`
- **camelCase:** `codeLandNaamLand`
- **PascalCase:** `CodeLandNaamLand`
- **original:** `Code land Naam land`
- Handle both simple and complex multi-column joins
- Verify uniqueness of join columns to prevent data duplication
- Create enriched datasets in `data/03-combined/`

The process uses the `decoding_files_config.yaml` and `mapping_tables_config.yaml` files to determine which decoder files to join and how.
""")

# Check prerequisites
processed_files = get_processed_files()
yaml_exists = check_yaml_config()

if not yaml_exists:
    st.error("🚨 **YAML Configuration Missing**")
    st.info("💡 The `decoding_files_config.yaml` and `mapping_tables_config.yaml` files are required. Please ensure they exist in the project root.")
elif not processed_files:
    st.error("🚨 **No processed files found**")
    st.info("💡 Please run the Turbo Convert process first to create processed CSV files.")
else:
    st.success(f"✅ **{len(processed_files)} processed file(s) ready for combination**")

    # Show processed files in compact expander
    with st.expander(f"📁 Processed Files ({len(processed_files)} files)", expanded=False):
        st.write("**Files ready for combination:**")
        for file in processed_files:
            st.write(f"• `{file['name']}` ({file['size_formatted']})")

    # Configuration options
    st.subheader("⚙️ Combination Settings")

    col_a, col_b = st.columns(2)

    with col_a:
        # Case style option - should match what was used in conversion
        case_style = st.selectbox(
            "🔤 Column Case Style",
            options=["snake_case", "camelCase", "PascalCase", "original"],
            index=0,  # Default to snake_case
            format_func=lambda x: {
                "original": "Originele casing (Student ID)",
                "snake_case": "snake_case (student_id)",
                "camelCase": "camelCase (studentId)",
                "PascalCase": "PascalCase (StudentId)"
            }[x],
            help="Select the case style that was used in the conversion step. This affects both column matching and prefix generation."
        )

    with col_b:
        # Debug logging option
        debug_logging = st.checkbox(
            "🐛 Enable Debug Logging",
            value=True,
            help="Show detailed information about the joining process"
        )

    # Show prefix example
    st.info(f"💡 **Column prefix example** (for case style '{case_style}'): " +
           {
               "snake_case": "`code_land_naam_land`",
               "camelCase": "`codeLandNaamLand`",
               "PascalCase": "`CodeLandNaamLand`",
               "original": "`Code land Naam land`"
           }[case_style])

    st.markdown("---")

    # Centered button
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        # Use callback-based button
        st.button("🔗 Start Combine All 🔗",
                 type="primary",
                 use_container_width=True,
                 key="combine_all_btn",
                 on_click=start_combination)

    # Handle combination logic using session state flag
    if st.session_state.start_combine_all:
        # Reset the flag immediately
        st.session_state.start_combine_all = False

        # Reset console log at the start of each combination
        st.session_state.combine_console_log = ""

        # Create progress bar and status containers
        progress_bar = st.progress(0)
        status_text = st.empty()
        console_container = st.empty()

        def update_console():
            """Update the console display"""
            with console_container.container():
                if st.session_state.combine_console_log:
                    st.code(st.session_state.combine_console_log, language=None)
                else:
                    st.info("Starting combination process...")

        try:
            st.session_state.combine_console_log += "🔄 Starting data combination pipeline...\n"
            st.session_state.combine_console_log += f"⚙️ Settings: case_style={case_style}, debug_logging={debug_logging}\n"
            update_console()
            progress_bar.progress(10)
            status_text.text("🔗 Combining data with decoder files...")

            # Build command with parameters
            cmd = ["uv", "run", "src/backend/core/combiner.py"]
            cmd.extend(["--case-style", case_style])

            # Execute combination process
            st.session_state.combine_console_log += "🔗 Executing data combination...\n"
            update_console()

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")
            if result.stdout:
                st.session_state.combine_console_log += result.stdout
            if result.stderr:
                st.session_state.combine_console_log += f"Warning: {result.stderr}\n"

            if result.returncode == 0:
                st.session_state.combine_console_log += "✅ Data combination completed successfully\n"
                update_console()
                progress_bar.progress(100)
                status_text.text("✅ Combination completed successfully!")

                st.success("✅ **Combination completed!** Combined datasets saved to `data/03-combined/`")

                # Show combined files
                combined_files = get_combined_files()
                if combined_files:
                    with st.expander(f"📁 Combined Files ({len(combined_files)} files)", expanded=True):
                        st.write("**Files successfully created in `data/03-combined/`:**")
                        for file in combined_files:
                            st.write(f"• `{file['name']}` ({file['size_formatted']})")

                # Celebrate with balloons - final step completed!
                st.balloons()
            else:
                st.session_state.combine_console_log += f"❌ Process failed with return code: {result.returncode}\n"
                update_console()
                progress_bar.progress(0)
                status_text.text("❌ Combination failed")
                st.error("❌ **Combination failed!** Check the console log for details.")

            # Clear the progress indicators after a moment
            import time
            time.sleep(2)
            progress_bar.empty()
            status_text.empty()
            console_container.empty()

            # Rerun to update any button states
            st.rerun()

        except Exception as e:
            st.session_state.combine_console_log += f"❌ Error: {str(e)}\n"
            update_console()
            progress_bar.progress(0)
            status_text.text("❌ Combination failed")
            st.error(f"❌ **Combination failed:** {str(e)}")

# Console Log expander
with st.expander("📋 Console Log", expanded=True):
    st.caption("�� Shows detailed information about the data combination process")
    if 'combine_console_log' in st.session_state and st.session_state.combine_console_log:
        st.code(st.session_state.combine_console_log, language=None)
    else:
        st.info("No combination process started yet. Click 'Start Combine All' to begin.")

# Show existing combined files (if any)
combined_files = get_combined_files()
if combined_files:
    with st.expander(f"📁 Existing Combined Files ({len(combined_files)} files)", expanded=False):
        st.write("**Files currently in `data/03-combined/`:**")
        for file in combined_files:
            st.write(f"• `{file['name']}` ({file['size_formatted']})")
else:
    st.info("📁 No combined files found in `data/03-combined/` yet.")

# Warning about existing files
if os.path.exists("data/03-combined") and os.listdir("data/03-combined"):
    st.warning("⚠️ New combination will overwrite existing files in `data/03-combined/`")

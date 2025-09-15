import streamlit as st
import os
import glob
import subprocess
import backend.utils.converter_validation as cv
import backend.utils.compressor as co
import backend.utils.encryptor as en
import io
import contextlib

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
#st.set_page_config(
#    page_title="⚡ Turbo Convert",
#    layout="centered",
#    initial_sidebar_state="expanded"
#)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_matched_files():
    """Get all matched files from the validation logs"""
    logs_dir = "data/00-metadata/logs"
    if not os.path.exists(logs_dir):
        return [], []

    matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))

    successful_pairs = []
    skipped_pairs = []

    if matching_log_files:
        try:
            import json
            with open(matching_log_files[0], 'r') as f:
                matching_data = json.load(f)

            for file_info in matching_data.get('processed_files', []):
                if file_info.get('status') == 'matched':
                    for match in file_info.get('matches', []):
                        pair_info = {
                            'input_file': file_info['input_file'],
                            'rows': file_info.get('row_count', 0),
                            'metadata_file': match['validation_file'].replace('Bestandsbeschrijving_', '').split('_')[0]
                        }

                        if match.get('validation_status') == 'success':
                            successful_pairs.append(pair_info)
                        else:
                            skipped_pairs.append(pair_info)
        except (json.JSONDecodeError, FileNotFoundError):
            pass

    return successful_pairs, skipped_pairs

def clear_console_log():
    """Clear the console log in session state"""
    if 'convert_console_log' in st.session_state:
        del st.session_state['convert_console_log']

def get_output_files():
    """Get all files from the output directory"""
    output_dir = "data/02-processed"
    if not os.path.exists(output_dir):
        return []

    files = []
    for file in os.listdir(output_dir):
        if os.path.isfile(os.path.join(output_dir, file)):
            file_path = os.path.join(output_dir, file)
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

def start_conversion():
    """Callback function to start the conversion process"""
    st.session_state.start_turbo_convert = True

# -----------------------------------------------------------------------------
# Initialize/Clear Console Log on Page Load
# -----------------------------------------------------------------------------
# Clear console log when page is loaded
clear_console_log()

# Initialize conversion trigger
if 'start_turbo_convert' not in st.session_state:
    st.session_state.start_turbo_convert = False

# Set page initialization flag
st.session_state.page_initialized_convert = True

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("⚡ Turbo Convert")

# Intro text
st.write("""
**Step 3: Final Converting & Processing Data**

We'll now use the validated metadata to convert your main/dec files through a complete processing pipeline. This transforms your fixed-width data into encrypted, compressed, ready-to-use files.

What happens:
- Convert fixed-width files to CSV format using validated field positions
- Delimiter: Pipe | - Encoding: Latin-1
- Validate the conversion results for accuracy
- Compress CSV files to efficient Parquet format
- Encrypt final files for secure storage
- Save all processed files to `data/02-processed/` + Balloons 🎈 when done!

If any step fails, check the log below for details about which files had issues.
""")

# Get files and display status
successful_pairs, skipped_pairs = get_matched_files()
total_pairs = len(successful_pairs) + len(skipped_pairs)

if total_pairs == 0:
    st.error("🚨 **No matched files found**")
    st.info("💡 Please run the validation process first to ensure your files are ready for conversion.")
else:
    st.success(f"✅ **{len(successful_pairs)} file pair(s) ready for conversion** ({len(skipped_pairs)} skipped validation)")

    # Show file pairs in compact expander - closed by default
    if successful_pairs or skipped_pairs:
        with st.expander(f"📁 File Details ({len(successful_pairs)} ready, {len(skipped_pairs)} skipped)", expanded=False):
            tab1, tab2 = st.tabs([f"✅ Ready ({len(successful_pairs)})", f"❌ Skipped ({len(skipped_pairs)})"])

            with tab1:
                if successful_pairs:
                    st.write("**Files ready for conversion:**")
                    for pair in successful_pairs:
                        st.write(f"• `{pair['input_file']}` ({pair['rows']:,} rows)")
                else:
                    st.info("No files ready for conversion.")

            with tab2:
                if skipped_pairs:
                    st.write("**Files with validation failures - check 🛡️ Validate Metadata + logs (3) & (4) for details :**")
                    for pair in skipped_pairs:
                        st.write(f"• `{pair['input_file']}` ({pair['rows']:,} rows)")
                else:
                    st.info("No validation failures.")

    # Centered button - only show if there are successful pairs
    if successful_pairs:
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            # Use callback-based button (recommended Streamlit pattern)
            st.button("⚡ Start Turbo Convert ⚡",
                     type="primary",
                     use_container_width=True,
                     key="turbo_convert_btn",
                     on_click=start_conversion)

        # Handle conversion logic using session state flag
        if st.session_state.start_turbo_convert:
            # Reset the flag immediately
            st.session_state.start_turbo_convert = False

            # Reset console log at the start of each conversion
            st.session_state.convert_console_log = ""

            # Create progress bar and status containers
            progress_bar = st.progress(0)
            status_text = st.empty()
            console_container = st.empty()

            def update_console():
                """Update the console display"""
                with console_container.container():
                    if st.session_state.convert_console_log:
                        st.code(st.session_state.convert_console_log, language=None)
                    else:
                        st.info("Starting conversion process...")

            try:
                st.session_state.convert_console_log += "🔄 Starting conversion pipeline...\n"
                update_console()
                progress_bar.progress(10)
                status_text.text("⚡ Step 3: Converting fixed-width files...")

                # Step 3: Convert Files
                st.session_state.convert_console_log += "⚡ Step 3: Converting fixed-width files...\n"
                update_console()
                result = subprocess.run(["uv", "run", "src/backend/core/converter.py"],
                                      capture_output=True, text=True, cwd=".")
                if result.stdout:
                    st.session_state.convert_console_log += result.stdout
                if result.stderr:
                    st.session_state.convert_console_log += f"Warning: {result.stderr}\n"
                st.session_state.convert_console_log += "✅ File conversion completed\n"
                update_console()
                progress_bar.progress(30)

                # Step 4: Validate Conversion
                status_text.text("🔍 Step 4: Validating conversion results...")
                st.session_state.convert_console_log += "🔍 Step 4: Validating conversion results...\n"
                update_console()
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    cv.converter_validation()
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Conversion validation completed\n"
                update_console()
                progress_bar.progress(50)

                # Step 5: Run Compressor
                status_text.text("🗜️ Step 5: Compressing to Parquet format...")
                st.session_state.convert_console_log += "🗜️ Step 5: Compressing to Parquet format...\n"
                update_console()
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    co.convert_csv_to_parquet()
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Compression completed\n"
                update_console()
                progress_bar.progress(75)

                # Step 6: Run Encryptor
                status_text.text("🔒 Step 6: Encrypting final files...")
                st.session_state.convert_console_log += "🔒 Step 6: Encrypting final files...\n"
                update_console()
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    en.encryptor()
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Encryption completed\n"
                st.session_state.convert_console_log += "🎉 Complete processing pipeline finished successfully!\n"
                update_console()
                progress_bar.progress(100)
                status_text.text("✅ Processing completed successfully!")

                st.success("✅ **Processing completed!** Files converted, validated, compressed, and encrypted. Results saved to `data/02-processed/`")

                # Show converted files
                output_files = get_output_files()
                if output_files:
                    with st.expander(f"📁 Converted Files ({len(output_files)} files)", expanded=True):
                        st.write("**Files successfully created in `data/02-processed/`:**")

                        # Group files by type for better organization
                        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv')]
                        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
                        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]

                        if csv_files:
                            st.write("**📄 CSV Files (Converted):**")
                            for file in csv_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if parquet_files:
                            st.write("**🗜️ Parquet Files (Compressed):**")
                            for file in parquet_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if encrypted_files:
                            st.write("**🔒 Encrypted Files (Final):**")
                            for file in encrypted_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                # Celebrate with balloons!
                st.balloons()

                # Clear the progress indicators after a moment
                import time
                time.sleep(2)
                progress_bar.empty()
                status_text.empty()
                console_container.empty()

                # Rerun to update any button states
                st.rerun()

            except Exception as e:
                st.session_state.convert_console_log += f"❌ Error: {str(e)}\n"
                update_console()
                progress_bar.progress(0)
                status_text.text("❌ Processing failed")
                st.error(f"❌ **Processing failed:** {str(e)}")
    else:
        st.warning("⚠️ No files ready for conversion. Please check validation results.")

# Console Log expander
with st.expander("📋 Console Log", expanded=True):
    st.caption("💡 Note: Messages like 'Could not determine dtype for column X, falling back to string' are harmless - just a quirk of the Polars Excel library.")
    if 'convert_console_log' in st.session_state and st.session_state.convert_console_log:
        st.code(st.session_state.convert_console_log, language=None)
    else:
        st.info("No conversion process started yet. Click 'Start Turbo Convert' to begin.")



# Show existing converted files (if any)
output_files = get_output_files()
if output_files:
    with st.expander(f"📁 Converted Files ({len(output_files)} files)", expanded=False):
        st.write("**Files currently in `data/02-processed/`:**")

        # Group files by type for better organization
        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv')]
        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]

        if csv_files:
            st.write("**📄 CSV Files (Converted):**")
            for file in csv_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")

        if parquet_files:
            st.write("**🗜️ Parquet Files (Compressed):**")
            for file in parquet_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")

        if encrypted_files:
            st.write("**🔒 Encrypted Files (Final):**")
            for file in encrypted_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")

else:
    st.info("📁 No converted files found in `data/02-processed/` yet.")

# Warning about existing files
if os.path.exists("data/02-processed") and os.listdir("data/02-processed"):
    st.warning("⚠️ New conversion will overwrite existing files in `data/02-processed/`")

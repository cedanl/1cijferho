import streamlit as st
import os
import subprocess
import io
import contextlib
import glob

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

def get_combined_files():
    """Get all combined files from data/03-combined"""
    combined_dir = "data/03-combined"
    if not os.path.exists(combined_dir):
        return []

    files = []
    for file in os.listdir(combined_dir):
        if file.endswith('.csv') and not file.startswith('Dec_'):
            file_path = os.path.join(combined_dir, file)
            file_size = os.path.getsize(file_path)
            files.append({
                'name': file,
                'path': file_path,
                'size': file_size,
                'size_formatted': format_file_size(file_size)
            })

    files.sort(key=lambda x: x['name'])
    return files

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"

def clear_console_log():
    """Clear the console log in session state"""
    if 'enrich_console_log' in st.session_state:
        del st.session_state['enrich_console_log']

def start_enrichment():
    """Callback function to start the enrichment process"""
    st.session_state.start_enrichment = True

# -----------------------------------------------------------------------------
# Initialize/Clear Console Log on Page Load
# -----------------------------------------------------------------------------
if 'page_initialized' not in st.session_state:
    clear_console_log()
    st.session_state.page_initialized = True

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("🧬 Enrich Data")

# Intro text
st.write("""
**Step 4: Data Enrichment**

Add calculated fields and switch analysis to your combined data. This process will:

- **Main Enrichment**: Add demographics, study progress, academic outcomes
- **Switch Analysis**: Track program switches, timing, and patterns
- **Case Handling**: Normalize column names during processing, restore original style

What happens:
- Load combined CSV files from `data/03-combined/`
- Add 50+ enrichment variables (VU + Avans analytics)
- Save enriched data with `_enriched.csv` suffix
- Generate processing reports in console below
""")

# Get files and display status
combined_files = get_combined_files()

if not combined_files:
    st.error("🚨 **No combined files found in `data/03-combined/`**")
    st.info("💡 Please run the **Combine All** process first to create combined files.")
    st.stop()

# File selection
st.subheader("📁 File Selection")
selected_file_name = st.selectbox(
    "Select file to enrich:",
    options=[f['name'] for f in combined_files],
    help="Choose a combined CSV file to add enrichment variables to"
)

selected_file = next(f for f in combined_files if f['name'] == selected_file_name)

# Show file info
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("📄 File", selected_file['name'])
with col2:
    st.metric("📊 Size", selected_file['size_formatted'])
with col3:
    # Quick column count
    try:
        import polars as pl
        df_sample = pl.read_csv(selected_file['path'], n_rows=0)
        col_count = len(df_sample.columns)
        st.metric("🔢 Columns", f"{col_count}")
    except:
        st.metric("🔢 Columns", "Unknown")

# Processing options
st.subheader("⚙️ Processing Options")

col1, col2 = st.columns(2)
with col1:
    case_style = st.radio(
        "Column naming style:",
        ["snake_case (default)", "preserve_original", "camelCase", "PascalCase"],
        index=0,
        help="Choose output column naming convention"
    )

with col2:
    processing_options = st.multiselect(
        "Additional options:",
        ["Robust CSV parsing", "Generate detailed logs", "Skip validation"],
        default=["Robust CSV parsing", "Generate detailed logs"],
        help="Extra processing configurations"
    )

# Status check
st.subheader("🚀 Run Enrichment")

# Check if already running
if 'start_enrichment' not in st.session_state:
    st.session_state.start_enrichment = False

# Control buttons
col1, col2 = st.columns(2)
with col1:
    if st.button("🚀 Start Enrichment",
                 type="primary",
                 use_container_width=True,
                 disabled=st.session_state.start_enrichment):
        start_enrichment()

with col2:
    if st.button("🧹 Clear Log", use_container_width=True):
        clear_console_log()

# -----------------------------------------------------------------------------
# Run Processing
# -----------------------------------------------------------------------------
if st.session_state.start_enrichment:

    # Create progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Console output container
    console_container = st.container()

    with console_container:
        st.subheader("📊 Processing Log")
        console_output = st.empty()

        # Capture subprocess output
        log_content = []

        try:
            # Prepare command arguments
            cmd = [
                "uv", "run", "python",
                "run_enrichment.py",
                "--input-file", selected_file['path'],
                "--case-style", case_style.split()[0],  # Extract just the style name
            ]

            # Add options
            if "Robust CSV parsing" in processing_options:
                cmd.append("--robust-parsing")
            if "Generate detailed logs" in processing_options:
                cmd.append("--verbose")
            if "Skip validation" in processing_options:
                cmd.append("--skip-validation")

            status_text.text("🔄 Starting enrichment process...")
            progress_bar.progress(10)

            # Run the enrichment process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                universal_newlines=True,
                cwd=os.getcwd()
            )

            # Stream output in real-time
            for line in iter(process.stdout.readline, ''):
                if line:
                    log_content.append(line.strip())

                    # Update progress based on output
                    if "Starting main enrichment" in line:
                        progress_bar.progress(30)
                        status_text.text("🔧 Running main enrichment...")
                    elif "Starting switch enrichment" in line:
                        progress_bar.progress(60)
                        status_text.text("🔀 Running switch enrichment...")
                    elif "Saving enriched data" in line:
                        progress_bar.progress(80)
                        status_text.text("💾 Saving results...")
                    elif "Complete enrichment pipeline finished" in line:
                        progress_bar.progress(100)
                        status_text.text("✅ Enrichment completed!")

                    # Update console display
                    console_output.code('\n'.join(log_content[-20:]))  # Show last 20 lines

            # Wait for process completion
            process.wait()

            # Final status
            if process.returncode == 0:
                st.success("🎉 **Enrichment completed successfully!**")

                # Show output files
                enriched_dir = "data/04-enriched"
                logs_dir = "data/04-enriched/logs"

                # Check for enriched data files
                output_files = []
                if os.path.exists(enriched_dir):
                    output_files = [f for f in os.listdir(enriched_dir)
                                  if f.endswith('_enriched.csv') and os.path.isfile(os.path.join(enriched_dir, f))]

                # Check for log files
                log_files = []
                if os.path.exists(logs_dir):
                    log_files = [f for f in os.listdir(logs_dir)
                               if f.endswith('.md') or f.endswith('.txt')]

                if output_files or log_files:
                    st.write("**Generated files:**")

                    # Show data files
                    for file in sorted(output_files):
                        file_path = f"{enriched_dir}/{file}"
                        file_size = format_file_size(os.path.getsize(file_path))
                        st.write(f"📄 `{file}` ({file_size})")

                    # Show log files
                    for file in sorted(log_files):
                        file_path = f"{logs_dir}/{file}"
                        file_size = format_file_size(os.path.getsize(file_path))
                        st.write(f"📋 `logs/{file}` ({file_size})")

            else:
                st.error(f"❌ **Enrichment failed** (exit code: {process.returncode})")
                st.write("Check the log above for error details.")

            # Store log in session state
            st.session_state['enrich_console_log'] = '\n'.join(log_content)

        except Exception as e:
            st.error(f"❌ **Error starting enrichment**: {str(e)}")
            progress_bar.progress(0)
            status_text.text("❌ Failed to start")

        finally:
            # Reset the start flag
            st.session_state.start_enrichment = False

# -----------------------------------------------------------------------------
# Show Previous Log (if exists)
# -----------------------------------------------------------------------------
if 'enrich_console_log' in st.session_state and not st.session_state.start_enrichment:
    with st.expander("📋 Previous Processing Log", expanded=False):
        st.code(st.session_state['enrich_console_log'])

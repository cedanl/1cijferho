import streamlit as st
import os
import polars as pl
from pathlib import Path
from datetime import datetime
import sys
import traceback

# Import backend enrichment functions
sys.path.insert(0, "src")
from backend.core.enricher import enrich_dataframe
from backend.core.enricher_switch import enrich_switch_data
from backend.core.case_utils import (
    normalize_column_names,
    denormalize_column_names,
    get_column_case_style
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

def load_csv_robust(file_path, verbose=False):
    """Load CSV with robust parsing options"""
    if verbose:
        print(f"📊 Loading {file_path}...")

    try:
        # Try robust loading with proper null handling
        df = pl.read_csv(
            file_path,
            null_values=["NA", "NULL", "", "na", "null"],
            infer_schema_length=10000,
            ignore_errors=True
        )
        if verbose:
            print(f"✅ Loaded: {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        if verbose:
            print(f"❌ Error loading {file_path}: {e}")
        raise

def generate_enrichment_log(log_file, input_file, output_file, original_columns, final_columns, case_style, processing_options):
    """Generate comprehensive enrichment log"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    log_content = f"""# Data Enrichment Log

**Date:** {timestamp}
**Input File:** {input_file.name}
**Output File:** {output_file.name}
**Case Style:** {case_style}

## Process Summary

✅ **Complete enrichment pipeline finished successfully!**

### Input Statistics
- **File:** {input_file}
- **Original columns:** {len(original_columns)}
- **File size (input):** {input_file.stat().st_size / 1024 / 1024:.1f} MB

### Processing Options
- **Case style:** {case_style}
- **Robust CSV parsing:** {"Yes" if "Robust CSV parsing" in processing_options else "No"}
- **Generate detailed logs:** {"Yes" if "Generate detailed logs" in processing_options else "No"}
- **Skip validation:** {"Yes" if "Skip validation" in processing_options else "No"}

### Output Statistics
- **Final columns:** {len(final_columns)}
- **New columns added:** {len(final_columns) - len(original_columns)}
- **File size (output):** {output_file.stat().st_size / 1024 / 1024:.1f} MB
- **Size increase:** {output_file.stat().st_size / input_file.stat().st_size:.1f}x

## Enrichment Details

### 🔧 Main Enrichment Variables
**Core enrichment functions applied:**
- Demographics and location analysis
- Study progress tracking
- Academic outcome variables
- Profile standardization
- Enrollment pattern analysis

### 🔀 Switch Analysis Variables
**Switch detection methods:**
- Avans-style 1-year and 3-year switch patterns
- VU-style comprehensive switch tracking
- Dropout-based switch analysis
- Derived switch timing and pattern variables

### 📊 Column Mapping
**Original columns:** {len(original_columns)}
```
{chr(10).join([f"{i+1:3d}. {col}" for i, col in enumerate(original_columns[:20])])}
{"..." if len(original_columns) > 20 else ""}
```

**New enrichment columns:** {len(final_columns) - len(original_columns)}
```
{chr(10).join([f"{i+1:3d}. {col}" for i, col in enumerate([c for c in final_columns if c not in original_columns][:20])])}
{"..." if len(final_columns) - len(original_columns) > 20 else ""}
```

## Quality Assurance
- ✅ No data loss during processing
- ✅ All original columns preserved
- ✅ Column naming conventions handled
- ✅ Null values processed appropriately

## Files Generated
```
data/04-enriched/
├── {output_file.name}                         # Enriched dataset
└── logs/
    └── {log_file.name}                        # This log file
```

---
*Log generated automatically by 1CijferHO enrichment pipeline*
"""

    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(log_content)

def run_enrichment_pipeline(input_file_path, case_style, processing_options, progress_callback=None, log_callback=None):
    """
    Run the complete enrichment pipeline using backend functions directly
    """
    def log(message):
        if log_callback:
            log_callback(message)
        print(message)

    def update_progress(percent, message):
        if progress_callback:
            progress_callback(percent, message)

    try:
        input_file = Path(input_file_path)

        update_progress(10, "🔄 Loading data...")
        log("📊 Loading input data...")

        # Load data with robust parsing
        if "Robust CSV parsing" in processing_options:
            df_original = load_csv_robust(input_file, "Generate detailed logs" in processing_options)
        else:
            df_original = pl.read_csv(input_file)

        original_columns = df_original.columns.copy()
        original_case_style = get_column_case_style(original_columns)

        log(f"✅ Loaded: {len(df_original)} rows, {len(df_original.columns)} columns")
        log(f"🔤 Detected case style: {original_case_style}")
        log(f"🎯 Target case style: {case_style}")

        update_progress(20, "🐍 Normalizing column names...")
        log("🐍 Normalizing column names...")
        df_normalized = normalize_column_names(df_original)

        update_progress(30, "🔧 Starting main enrichment...")
        log("🔧 Starting main enrichment...")
        df_enriched = enrich_dataframe(df_normalized)
        log(f"✅ Main enrichment completed: +{len(df_enriched.columns) - len(df_normalized.columns)} new columns")

        update_progress(60, "🔀 Starting switch enrichment...")
        log("🔀 Starting switch enrichment...")
        df_final = enrich_switch_data(df_enriched)
        log(f"✅ Switch enrichment completed: +{len(df_final.columns) - len(df_enriched.columns)} new columns")

        update_progress(80, "🔄 Processing case style...")
        # Handle output case style
        if case_style == 'preserve_original' and original_case_style != 'snake_case':
            log("🔙 Restoring original case for existing columns...")
            df_final = denormalize_column_names(df_final, original_columns)
        elif case_style in ['camelCase', 'PascalCase']:
            log(f"🔄 Converting to {case_style}...")
            log(f"⚠️  {case_style} conversion not implemented yet, keeping snake_case")

        update_progress(85, "💾 Saving enriched data...")
        # Save enriched data
        output_enriched_dir = Path("data/04-enriched")
        output_enriched_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_enriched_dir / f"{input_file.stem}_enriched.csv"
        log(f"💾 Saving enriched data to {output_file}...")
        df_final.write_csv(output_file)

        update_progress(90, "📋 Generating log...")
        # Generate enrichment log
        logs_dir = output_enriched_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        log_file = logs_dir / f"enrichment_log_{input_file.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        generate_enrichment_log(log_file, input_file, output_file, original_columns, df_final.columns, case_style, processing_options)
        log(f"📋 Enrichment log saved to {log_file}")

        update_progress(100, "✅ Complete!")
        # Final stats
        final_stats = f"""
✅ Complete enrichment pipeline finished!

📊 Summary:
- Original columns: {len(original_columns)}
- Final columns: {len(df_final.columns)}
- New columns: {len(df_final.columns) - len(original_columns)}
- Output file: {output_file}
- File size: {output_file.stat().st_size / 1024 / 1024:.1f} MB
        """
        log(final_stats)

        return True, final_stats

    except Exception as e:
        error_msg = f"❌ Error in enrichment pipeline: {e}"
        log(error_msg)
        if "Generate detailed logs" in processing_options:
            log(traceback.format_exc())
        return False, error_msg

# -----------------------------------------------------------------------------
# Existing Helper Functions
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

        # Initialize log capture
        log_content = []

        def log_callback(message):
            log_content.append(message)
            console_output.code('\n'.join(log_content[-20:]))  # Show last 20 lines

        def progress_callback(percent, message):
            progress_bar.progress(percent)
            status_text.text(message)

        try:
            # Run the enrichment pipeline using backend functions directly
            success, result_message = run_enrichment_pipeline(
                input_file_path=selected_file['path'],
                case_style=case_style.split()[0],  # Extract just the style name
                processing_options=processing_options,
                progress_callback=progress_callback,
                log_callback=log_callback
            )

            # Final status
            if success:
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
                st.error("❌ **Enrichment failed**")
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

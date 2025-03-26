import streamlit as st
import backend.core.converter as converter
import os
import sys
import polars as pl
import subprocess
import backend.core.compressor as compressor
import frontend.Files.Data_Explorer_helper as de_helper


# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Magic Converter",
    layout="centered",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("✨ Magic Converter")
st.caption("Transform complex DUO datasets into actionable insights in minutes, not months. ✨")
st.info("🔧 This is beta version (v0.5.3). Your feedback is appreciated!")

st.write("""
##### 🚀 Ready for Conversion

Great news! Your files are matched and ready for conversion.

##### 📋 Review & Next Steps
Below are all files eligible for conversion based on your matching criteria:
1. Review the matched files in the table
2. Click **✨ Convert ✨** to start transformation
3. Find converted files in the `02-output` directory

##### 💡 Usage Tips
For optimal performance:
- **Python**: Use Polars for fast processing
  ```python
  import polars as pl
  df = pl.read_parquet("path/to/your/file.parquet")
  df = pl.read_csv("path/to/your/file.csv")
  ```
- **R**: Use data.table for efficient manipulation
  ```r
  library(data.table)
  df <- fread("path/to/your/file.csv")
  # or compress to qs format for faster loading
  library(qs)
  qs::saveqs(df, "path/to/your/file.qs")
  df <- qs::readqs("path/to/your/file.qs")
  ```

When ready, hit Convert to transform your data!
""")

with st.expander("✴️ Matching Results"):
    # Check if the match.csv file exists
    file_path = "data/00-metadata/logs/match.csv"
    if os.path.exists(file_path):
        # Display the matching results
        dfMatch = pl.read_csv(file_path)
        st.dataframe(dfMatch, use_container_width=True)
    else:
        # Show a warning if the file doesn't exist
        st.warning("⚠️ No matching results found. Please create the match.csv file using the Data Explorer first.")



if st.button("✨Convert✨", help="Run conversion", type="primary"):
    with st.spinner("Converting files..."):
        try:
            # Call the converter script as a subprocess
            result = subprocess.run(
                ["uv", "run", "src/backend/core/converter.py"],
                capture_output=True,
                text=True,
                check=True
            )
            st.success("Conversion completed!")
            
            # Run the compressor script
            with st.spinner("Compressing files..."):
                compress_result = subprocess.run(
                    ["uv", "run", "src/backend/core/compressor.py"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                st.success("Compression completed!")
            
            dtResult = de_helper.get_files_dataframe("data/02-output")
            dtResult= dtResult.sort("Extension")
            st.dataframe(dtResult)
            
            # Show results in an expander
            with st.expander("View Process Results", expanded=False):
                st.subheader("Conversion Output")
                st.code(result.stdout)
                
                st.subheader("Compression Output")
                st.code(compress_result.stdout)
                
        except subprocess.CalledProcessError as e:
            st.error(f"Error during process: {e.stderr}")
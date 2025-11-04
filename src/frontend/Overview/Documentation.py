import streamlit as st

st.markdown("""
<style>
.hero-section {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 2rem;
    border-radius: 10px;
    color: white;
    margin-bottom: 2rem;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="hero-section">
    <h1>📚 Documentation</h1>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Overview", "How to Use", "Technical"])

with tab1:
    st.markdown("## The Problem")
    
    st.markdown("""
    DUO delivers educational data as fixed-width ASCII files with separate decode files and unstructured metadata:
    
    - Fixed-width strings: `821PL21PL506451B090    J2006Swo 50645...`
    - Decode files: `5001Canada`, `5002Frankrijk` 
    - Field positions buried in .txt files
    - Manual processing takes hours to days
    """)
    
    st.markdown("## What We Do")
    
    st.markdown("""
    Automatically parse DUO files to CSV/Parquet format ready for analysis.
    """)
    
    st.code("""
Before: 821PL21PL506451B090    J20NLSwo 50645...

After:  Persoonsgebonden_nummer: 821PL21PL506
        Type: J20
        Landcode: NL
    """, language=None)

with tab2:
    st.markdown("## Required Files")
    
    st.markdown("""
    - DUO Main Files (EV, VAKHAVW, CROHO, CROHO_VEST)
    - DUO Bestandsbeschrijvingen
    - DUO Dec Files
    """)
    
    st.markdown("## Process")
    
    st.markdown("""
    1. Upload your DUO files to `data/01-input/`
    2. Run Extract Metadata
    3. Run Validation
    4. Run Turbo Convert
    5. Find your files in `data/02-output/`
                
    """)
    
    st.markdown("## Troubleshooting")
    
    with st.expander("Common Issues"):
        st.markdown("""
        **No tables found in metadata file**  
        Check that .txt file contains "Startpositie" and "Aantal posities"
        
        **File matching failed**  
        Verify data file names match metadata file names
        
        **Processing is slow**  
        Close unnecessary applications, check disk space
        
        **Permission denied**  
        Close Excel files in output directory, check write permissions
        """)

with tab3:
    st.markdown("## Stack")
    
    st.markdown("""
    Python 3.13+ • Streamlit • Polars • uv
    """)
    
    st.markdown("## Architecture")
    
    st.markdown("""
    **Core Modules**
    
    - `src/backend/core/`
    - `extractor.py` — Extract field positions from metadata .txt files
    - `converter.py` — Convert fixed-width to CSV using multiprocessing
    - `decoder.py` — Process decode files (roadmap)
    
    **Utilities**
    
    - `src/backend/utils/`
    - `converter_match.py` — Match data files to metadata
    - `converter_validation.py` — Verify row counts and data integrity
    - `compressor.py` — Convert CSV to Parquet
    - `encryptor.py` — SHA256 hashing for BSN anonymization
    - `extractor_validation.py` — Validate metadata extraction
                
    **Frontend**
    
    - `src/frontend/`
    - `Overview/Home.py` — Home page
    - `Overview/Documentation.py` — Documentation page
    - `Files/Upload_Data.py` — Data upload page
    - `Modules/Extract_Metadata.py` — Extract metadata page
    - `Modules/Validate_Metadata.py` — Validate metadata page
    - `Modules/Turbo_Convert.py` — Turbo convert (parser) page
    - `Modules/Tip.py` — Tip page

    """)
    
    st.markdown("## Data Flow")
    
    st.code("""
Raw DUO Files
    ↓
Metadata Extraction (extractor.py)
    ↓
Table Structuring (JSON → Excel)
    ↓
File Matching (converter_match.py)
    ↓
Parallel Conversion (converter.py)
    ↓
Validation (converter_validation.py)
    ↓
Compression (compressor.py)
    ↓
Anonymization (encryptor.py)
    ↓
Research-Ready Output
    """, language=None)
    
    st.markdown("## Performance")
    
    st.markdown("""
    - Uses n-1 CPU cores for parallel processing
    - Streams large files to manage memory
    - Parquet compression reduces file size 60-80%
    - Cryptographic hashing for privacy
    """)

st.markdown("---")
st.markdown("Built by CEDA • Questions: a.sewnandan@hhs.nl | t.iwan@vu.nl")
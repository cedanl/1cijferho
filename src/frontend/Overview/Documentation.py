import streamlit as st
from pathlib import Path

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="📚 Documentation",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Custom CSS for better styling
# -----------------------------------------------------------------------------
st.markdown("""
<style>
.hero-section {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 2rem;
    border-radius: 10px;
    color: white;
    margin-bottom: 2rem;
}

.problem-box {
    background-color: #fff3cd;
    border: 1px solid #ffeaa7;
    border-radius: 8px;
    padding: 1.5rem;
    margin: 1rem 0;
}

.solution-box {
    background-color: #d1f2eb;
    border: 1px solid #52c41a;
    border-radius: 8px;
    padding: 1.5rem;
    margin: 1rem 0;
}

.feature-card {
    background-color: #f8f9fa;
    border: 1px solid #e9ecef;
    border-radius: 8px;
    padding: 1.5rem;
    margin: 1rem 0;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.tech-badge {
    display: inline-block;
    background-color: #007bff;
    color: white;
    padding: 0.25rem 0.75rem;
    border-radius: 15px;
    font-size: 0.8rem;
    margin: 0.25rem;
}

.step-number {
    background-color: #007bff;
    color: white;
    border-radius: 50%;
    width: 30px;
    height: 30px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-weight: bold;
    margin-right: 10px;
}

.contribute-section {
    background-color: #f8f9fa;
    border: 2px solid #007bff;
    border-radius: 8px;
    padding: 2rem;
    margin: 2rem 0;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Hero Section
# -----------------------------------------------------------------------------
st.markdown("""
<div class="hero-section">
    <h1>🎯 1CijferHO DUO File Processor</h1>
    <h3>Transforming Complex DUO Data into Research-Ready Insights</h3>
    <p>Breaking down the barriers between raw educational data and meaningful research for every HO and WO institution in the Netherlands.</p>
</div>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Main Content Tabs
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(["🎯 Overview", "⚡ How It Works", "🔧 Technical Details", "🚀 Getting Started"])

with tab1:
    # The Problem Section
    st.markdown("## 🚨 The Challenge We Solve")
    
    st.markdown("**📁 What DUO Gives Us:**")
    st.markdown("""
    - **Fixed-width ASCII files** - Giant strings of data with no clear separation
    - **Separate decode files** - Additional context files that need to be matched
    - **Unstructured .txt metadata** - Field positions buried in poorly formatted text files
    - **Manual processing nightmare** - Hours of work to make sense of a single dataset
    """)
    
    st.markdown("**💔 The Pain Points:**")
    st.markdown("""
    - 🤯 **Overwhelming complexity** - Researchers spend more time on data prep than research
    - ⏰ **Time sink** - What should take minutes takes hours or days
    - ❌ **Error-prone manual work** - Easy to misalign fields or lose data integrity
    - 🏢 **Institutional barriers** - Every HO/WO reinvents the wheel separately
    - 🔒 **Privacy concerns** - Sensitive data like BSN numbers need careful handling
    """)
    
    # The Solution Section
    st.markdown("## ✨ Our Solution")
    
    st.markdown("**🎯 What This App Does:**")
    st.markdown("We've created a **blazingly fast, automated pipeline** that transforms DUO's messy data into clean, research-ready formats saving you **days to months of manual work**, reducing processing time to just **minutes**.")
    
    st.markdown("**🚀 Key Benefits:**")
    st.markdown("""
    - ⚡ **Lightning Fast** - Process massive files in minutes using multiprocessing
    - 🎯 **Zero Manual Work** - Fully automated from upload to final output
    - ✅ **100% Data Integrity*** - Maintains data exactly as delivered by DUO
    - 🔒 **Privacy-First** - Automatic anonymization of sensitive columns (BSN, etc.)
    - 📊 **Research-Ready Output** - Clean CSV and compressed Parquet files
    - 🏢 **Institution-Friendly** - Designed for every HO and WO in the Netherlands
    """)
    
    st.caption("*Data integrity as received from DUO - we preserve exactly what's in the original files")
    
    # Impact Section
    st.markdown("## 🎉 The Impact")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="feature-card">
            <h3>⏱️ Time Savings</h3>
            <p><strong>From months/days to minutes</strong></p>
            <p>What used to take researchers days to months of manual work now takes minutes of automated processing.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="feature-card">
            <h3>🎯 Accuracy</h3>
            <p><strong>Error-free processing</strong></p>
            <p>Automated validation catches mistakes that manual processing would miss, ensuring research integrity.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="feature-card">
            <h3>🤝 Collaboration</h3>
            <p><strong>Shared solutions</strong></p>
            <p>Building towards standardized datasets that benefit the entire Dutch higher education research community.</p>
        </div>
        """, unsafe_allow_html=True)

with tab2:
    st.markdown("## ⚡ How It Works: The Magic Pipeline")
    
    st.markdown("### 🔄 The Complete Workflow")
    
    # Step-by-step process
    steps = [
        {
            "number": "1",
            "title": "📁 Smart Extraction",
            "description": "Automatically finds and extracts field position tables from messy DUO .txt files",
            "tech": "Uses regex patterns and intelligent text parsing to locate data structures"
        },
        {
            "number": "2", 
            "title": "📊 Table Processing",
            "description": "Converts extracted tables into clean, structured Excel files with proper field definitions",
            "tech": "Polars-powered data processing with automatic type detection"
        },
        {
            "number": "3",
            "title": "🔍 Intelligent Matching", 
            "description": "Automatically matches your main data files with their corresponding decode/metadata files",
            "tech": "Smart filename matching with fuzzy logic and validation"
        },
        {
            "number": "4",
            "title": "⚡ Turbo Conversion",
            "description": "Converts fixed-width files to CSV format using multiprocessing for blazing speed",
            "tech": "CPU-optimized chunking with parallel processing"
        },
        {
            "number": "5",
            "title": "✅ Quality Assurance",
            "description": "Validates conversion accuracy by comparing row counts and data integrity",
            "tech": "Multiple validation layers ensure zero data loss"
        },
        {
            "number": "6",
            "title": "🗜️ Optimization",
            "description": "Compresses CSV files to efficient Parquet format for faster analysis",
            "tech": "Apache Parquet format reduces file size by 60-80%"
        },
        {
            "number": "7",
            "title": "🔒 Privacy Protection",
            "description": "Automatically anonymizes sensitive columns like BSN numbers using SHA256 hashing",
            "tech": "Cryptographic hashing ensures privacy while maintaining research utility"
        }
    ]
    
    for step in steps:
        st.markdown(f"""
        <div class="feature-card">
            <div style="display: flex; align-items: center; margin-bottom: 1rem;">
                <div class="step-number">{step['number']}</div>
                <h3 style="margin: 0;">{step['title']}</h3>
            </div>
            <p style="font-size: 1.1rem; margin-bottom: 0.5rem;"><strong>{step['description']}</strong></p>
            <p style="font-size: 0.9rem; color: #666; font-style: italic;">{step['tech']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Performance metrics
    st.markdown("### 📈 Processing Speed")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **🚀 Average Runtime:**
        - Process time with 1CijferHO: **A few seconds to a couple of minutes**
        - Depends on your hardware specifications
        - Multi-core acceleration automatically adapts to your system
        """)
    
    with col2:
        st.markdown("""
        **💾 Output Quality:**
        - 100% data integrity as received from DUO*
        - 60-80% file size reduction (Parquet compression)
        - Cryptographically secure anonymization
        - Research-ready CSV and Parquet formats
        """)
        
    st.markdown("*Data integrity maintained exactly as delivered by DUO")

with tab3:
    st.markdown("## 🔧 Technical Architecture")
    
    # Tech stack
    st.markdown("### 🛠️ Technology Stack")
    
    st.markdown("""
    <div style="margin: 1rem 0;">
        <span class="tech-badge">Python 3.13+</span>
        <span class="tech-badge">Streamlit</span>
        <span class="tech-badge">Polars</span>
        <span class="tech-badge">Multiprocessing</span>
        <span class="tech-badge">Rich Console</span>
        <span class="tech-badge">uv Package Manager</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Backend architecture
    st.markdown("### 🏗️ Backend Components")
    
    # Core modules
    st.markdown("#### 📦 Core Processing Modules")
    
    core_modules = [
        {
            "name": "extractor.py",
            "purpose": "Table Extraction Engine",
            "functions": [
                "extract_tables_from_txt() - Parses DUO metadata files",
                "extract_excel_from_json() - Converts to structured Excel format"
            ],
            "note": "Two main process functions serve as wrappers for different input types"
        },
        {
            "name": "converter.py", 
            "purpose": "High-Performance File Conversion",
            "functions": [
                "process_chunk() - Parallel processing worker function",
                "converter() - Main fixed-width to CSV conversion",
                "run_conversions_from_matches() - Batch conversion orchestrator"
            ]
        },
        {
            "name": "decoder.py",
            "purpose": "Decode File Processing (On Roadmap)", 
            "functions": [
                "Advanced decode file parsing and validation",
                "Intelligent field mapping and verification"
            ],
            "note": "Currently paused due to complexity and dataset size. Part of the information is still in file descriptions (values) and requires reading descriptions to understand which decoder to use for which columns."
        }
    ]
    
    for module in core_modules:
        with st.expander(f"📄 {module['name']} - {module['purpose']}"):
            st.markdown(f"**Primary Functions:**")
            for func in module['functions']:
                st.markdown(f"- `{func}`")
            if 'note' in module:
                st.markdown(f"**Note:** {module['note']}")
    
    # Utility modules  
    st.markdown("#### 🔧 Utility Modules")
    
    util_modules = [
        {
            "name": "converter_match.py",
            "purpose": "Intelligent file matching using fuzzy logic and validation"
        },
        {
            "name": "converter_validation.py", 
            "purpose": "Data integrity validation and row count verification"
        },
        {
            "name": "compressor.py",
            "purpose": "CSV to Parquet conversion for optimal storage"
        },
        {
            "name": "encryptor.py",
            "purpose": "SHA256 hashing for sensitive column anonymization"
        },
        {
            "name": "extractor_validation.py",
            "purpose": "Metadata extraction validation and quality checks"
        }
    ]
    
    cols = st.columns(2)
    for i, module in enumerate(util_modules):
        with cols[i % 2]:
            st.markdown(f"""
            <div class="feature-card">
                <h4>📄 {module['name']}</h4>
                <p>{module['purpose']}</p>
            </div>
            """, unsafe_allow_html=True)
    
    # Performance optimizations
    st.markdown("### ⚡ Performance Optimizations")
    
    st.markdown("""
    **🚀 Multiprocessing Architecture:**
    - Automatic CPU core detection (uses n-1 cores)
    - Intelligent chunk sizing based on file size
    - Memory-efficient streaming for large files
    - Process pool management with proper cleanup
    
    **💾 Memory Management:**
    - Lazy evaluation using Polars DataFrames
    - Streaming file processing to handle GB+ files
    - Automatic garbage collection between chunks
    - Memory-mapped file access for optimal performance
    
    **🔄 Error Handling:**
    - Comprehensive logging at each pipeline stage
    - Graceful failure recovery with detailed error reporting
    - Automatic retry mechanisms for transient failures
    - Data integrity checks at every step
    """)
    
    # Data flow diagram
    st.markdown("### 📊 Data Flow Architecture")
    
    st.markdown("""
    ```
    📁 Raw DUO Files
         ↓
    🔍 Metadata Extraction (extractor.py)
         ↓
    📊 Table Structuring (JSON → Excel)
         ↓  
    🔗 File Matching (converter_match.py)
         ↓
    ⚡ Parallel Conversion (converter.py)
         ↓
    ✅ Validation (converter_validation.py)  
         ↓
    🗜️ Compression (compressor.py)
         ↓
    🔒 Anonymization (encryptor.py)
         ↓
    📋 Research-Ready Output
    ```
    """)

with tab4:
    st.markdown("## 🚀 Getting Started")
    
    # Quick start guide
    st.markdown("### ⚡ Quick Start Guide")
    
    st.markdown("""
    <div class="solution-box">
        <h3>🎯 Ready to Transform Your DUO Data?</h3>
        <p>Follow these simple steps to go from raw DUO files to research-ready data in minutes!</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Prerequisites
    st.markdown("#### 📋 What You Need")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **📁 Required Files:**
        - DUO main data files (fixed-width format)
        - DUO decode files (if applicable)  
        - DUO metadata .txt files (containing field positions)
        """)
    
    with col2:
        st.markdown("""
        **💻 System Requirements:**
        - Windows, macOS, or Linux
        - 4GB+ RAM (8GB+ recommended for large files)
        - 2GB+ free disk space
        """)
    
    # Step by step
    st.markdown("#### 🎯 Step-by-Step Process")
    
    process_steps = [
        {
            "step": "1. 📁 Upload Data",
            "description": "Copy your DUO files to the data/01-input directory",
            "action": "Use the 'Upload Data' page - copy files to data/01-input folder and refresh to see categorized files"
        },
        {
            "step": "2. 🔍 Extract Metadata", 
            "description": "Process the metadata .txt files to extract field positions",
            "action": "Go to 'Extract Metadata' page and click 'Start Extraction ⚡' to process bestandsbeschrijving files"
        },
        {
            "step": "3. 🛡️ Validate Metadata",
            "description": "Validate extracted metadata and match files automatically",
            "action": "Navigate to 'Validate Metadata' page and click 'Start Validation ⚡' to check for errors and match files"
        },
        {
            "step": "4. ⚡ Turbo Convert",
            "description": "Convert, validate, compress, and encrypt your data files", 
            "action": "Go to 'Turbo Convert' page and click 'Start Turbo Convert 🚀' to run the complete processing pipeline"
        },
        {
            "step": "5. 📊 Download Results",
            "description": "Get your research-ready files in CSV and Parquet formats",
            "action": "Find processed files in the 'data/02-output' folder with CSV and encrypted Parquet versions"
        }
    ]
    
    for step_info in process_steps:
        st.markdown(f"""
        <div class="feature-card">
            <h4>{step_info['step']}</h4>
            <p><strong>{step_info['description']}</strong></p>
            <p style="color: #007bff; font-style: italic;">→ {step_info['action']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Tips and best practices
    st.markdown("### 💡 Pro Tips & Best Practices")
    
    tips_col1, tips_col2 = st.columns(2)
    
    with tips_col1:
        st.markdown("""
        **⚡ Performance Tips:**
        - Close Excel/spreadsheet programs before processing
        - Use SSD storage for faster file I/O
        - Process during off-peak hours for best performance
        - Keep file paths short and simple
        """)
    
    with tips_col2:
        st.markdown("""
        **🔒 Data Safety:**
        - Always backup original files before processing
        - Verify output row counts match input expectations  
        - Check anonymized fields retain research utility
        - Review validation logs for any warnings
        """)
    
    # Common issues
    st.markdown("### 🔧 Troubleshooting")
    
    with st.expander("🚨 Common Issues & Solutions"):
        st.markdown("""
        **❌ "No tables found in metadata file"**
        - Check that your .txt file contains properly formatted field position tables
        - Ensure the file contains keywords like "Startpositie" and "Aantal posities"
        
        **⚠️ "File matching failed"** 
        - Verify that your data file names correspond to the metadata file names
        - Check that both main and decode files are in the correct folders
        
        **🐌 "Processing is slow"**
        - Close unnecessary applications to free up CPU and memory
        - Ensure you have sufficient disk space for output files
        - Consider processing smaller batches for very large datasets
        
        **🔒 "Permission denied errors"**
        - Close any Excel files that might be open in the output directory
        - Run the application with appropriate file system permissions
        - Check that output directories are not read-only
        """)
    
    # Next steps
    st.markdown("### 🎯 What's Next?")
    
    st.markdown("""
    <div class="solution-box">
        <h3>🚀 Future Roadmap</h3>
        <ul>
            <li><strong>🎨 Standard Dataset Templates</strong> - Pre-configured settings for common DUO datasets</li>
            <li><strong>📊 Advanced Analytics</strong> - Built-in data exploration and visualization tools</li>
            <li><strong>🤝 Community Contributions</strong> - Shared configurations and best practices</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    # Want to contribute section
    st.markdown("""
    <div class="contribute-section">
        <h3>🤝 Want to Contribute?</h3>
        <p>This tool is open-source and built for the community. Join us in making DUO data processing easier for everyone!</p>
        <p><strong>Ways to contribute:</strong></p>
        <ul style="text-align: left; display: inline-block;">
            <li>🐛 Report bugs and issues</li>
            <li>💡 Suggest new features</li>
            <li>📖 Improve documentation</li>
            <li>🔧 Submit code improvements</li>
            <li>🤝 Share your use cases and feedback</li>
        </ul>
        <p><strong>Get involved:</strong> Visit our <a href="https://github.com/cedanl/1cijferho" target="_blank">GitHub repository</a> or contact the development team</p>
    </div>
    """, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Footer
# -----------------------------------------------------------------------------
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; margin-top: 2rem;">
    <p>🎓 Built for the Dutch Higher Education Research Community</p>
    <p>💻 Developed by CEDA | 🤝 Supported by Npuls | ⭐ Open Source on GitHub</p>
    <p>Questions? Contact: <strong>a.sewnandan@hhs.nl</strong> | <strong>t.iwan@vu.nl</strong></p>
</div>
""", unsafe_allow_html=True)

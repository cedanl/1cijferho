# 🏗️ 1CijferHO Architecture

## 📦 Project Structure
```
1cijferho/
├── src/
│   ├── main.py           # Streamlit application entry point
│   ├── frontend/         # Streamlit UI modules and pages
│   └── backend/          # Core processing modules
│       ├── core/         # Main processing engines
│       │   ├── extractor.py       # Metadata extraction
│       │   ├── converter.py       # Data format conversion
│       │   ├── combiner.py        # Data combination engine
│       │   ├── enricher.py        # Main enrichment engine
│       │   └── enricher_switch.py # Switch analysis engine
│       └── utils/        # Helper functions and validation
├── data/                 # Data pipeline directories
│   ├── 00-metadata/      # Extracted metadata files
│   ├── 01-input/         # Raw DUO data files
│   ├── 02-processed/     # Converted CSV files
│   ├── 03-combined/      # Combined with decoder files
│   ├── 04-enriched/      # Final research-ready datasets
│   └── reference/        # Decoder and mapping tables
├── notebooks/            # R analysis examples (VU/Avans)
└── *.yaml               # Configuration files
```

## 🔧 Technical Stack

### Core Technologies
- **Python 3.x**: Primary development language
- **Streamlit**: Web application framework for data science
- **uv**: Modern Python package installer and resolver
- **Polars**: High-performance DataFrame library for data processing

### Key Dependencies
- **Data Processing**
  - Polars: Lightning-fast data manipulation and analysis
  - Rich: Enhanced console output and progress tracking
  - PyYAML: Configuration file processing
  - Pathlib: Modern file system path handling

- **File Processing**
  - CSV handling: Native Python with Polars integration
  - Fixed-width parsing: Custom extraction from DUO format
  - Parquet compression: Efficient data storage

- **Interface & UX**
  - Streamlit: Interactive web interface
  - Multi-page navigation: Organized workflow steps
  - Real-time progress tracking: Live subprocess monitoring

## 🔄 Data Flow

### 1. **Input Processing & Metadata Extraction**
   - Raw DUO data ingestion (fixed-width ASCII files)
   - Metadata extraction from Bestandsbeschrijving files (.txt → JSON → Excel)
   - Intelligent file matching and validation
   - Field position auto-detection from messy documentation

### 2. **Data Conversion Pipeline**
   - Fixed-width to CSV conversion with multiprocessing
   - Quality validation and error checking
   - File size optimization and compression
   - Privacy protection with cryptographic anonymization

### 3. **Advanced Data Combination (Combine All)**
   - **Intelligent Joins**: YAML-configured decoder file integration
   - **Multi-Source Integration**:
     - Decoder files (`decoding_files_config.yaml`)
     - Reference tables (`mapping_tables_config.yaml`)
     - Manual mappings (`manual_tables_config.yaml`)
   - **Smart Column Naming**: Case style preservation (snake_case, camelCase, PascalCase)
   - **Complex Mappings**: Multi-column join operations with uniqueness verification
   - **Output**: Enriched CSV files with human-readable labels in `data/03-combined/`

### 4. **Research Enrichment Pipeline**

   #### 🔧 **Main Enrichment (30+ Variables)**
   - **Demographics**: Living situation indicators (`student_uitwonend`)
   - **Study Progress**: Study year calculations (`studiejaar`) and timeline tracking
   - **Profile Analysis**: VWO/HAVO standardization (NT/NG/EM/CM profiles)
   - **Academic Outcomes**: Graduation tracking (`diploma_status`, `rendement_instelling_3_jaar`)
   - **Enrollment Patterns**: Gap year detection (`indicatie_tussenjaar`)

   #### 🔀 **Switch Analysis (27+ Variables)**
   - **Switch Detection**: Multi-timeframe analysis (1-year, 3-year, dropout-based)
   - **Direction Tracking**: Both FROM (`is_switch_from_record`) and TO (`is_switch_to_record`) records
   - **Timing Classification**: Early vs late switcher categorization
   - **Program Analytics**: Institution-wide switch rates (`program_switch_out_rate`)
   - **Sector Analysis**: Between-sector vs within-sector movement patterns

### 5. **Quality Assurance & Output**
   - **Robust Parsing**: Graceful handling of missing data and null values
   - **Column Detection**: Automatic 1CHO data structure identification
   - **Case Preservation**: Original column naming compatibility
   - **Detailed Logging**: Comprehensive enrichment reports in Markdown
   - **Final Output**: Research-ready datasets in `data/04-enriched/`

## 🔒 Security Considerations
- **Data Privacy**: Cryptographic anonymization of sensitive data (BSN, etc.)
- **Local Processing**: No external API dependencies for sensitive educational data
- **Input Validation**: Comprehensive file validation and sanitization
- **Memory Management**: Efficient handling of large DUO datasets (GB files)
- **Access Control**: Local file system permissions and workspace isolation

## 🚀 Performance Optimizations
- **Multiprocessing**: Parallel processing for data conversion and enrichment
- **Polars Engine**: Lightning-fast DataFrame operations vs traditional pandas
- **Parquet Compression**: 60-80% file size reduction for storage efficiency
- **Lazy Evaluation**: Memory-efficient processing of large datasets
- **Chunked Processing**: Batch processing to handle datasets larger than RAM
- **Smart Caching**: Processed metadata and reference table caching

## 🔄 Development Workflow
1. **Local Development**: Streamlit with hot-reload for rapid iteration
2. **Package Management**: uv for fast, reproducible dependency resolution
3. **Modular Architecture**: Separated frontend/backend for maintainability
4. **Configuration-Driven**: YAML files for flexible data processing pipelines
5. **Git-based Version Control**: Structured commits with data pipeline stages
6. **Documentation-First**: Comprehensive README and architecture docs

## 📈 Future Technical Roadmap
- [ ] **API Integration**: RESTful endpoints for headless operation
- [ ] **Database Support**: PostgreSQL/SQLite integration for large datasets
- [ ] **Advanced Analytics**: Integration with R/Python statistical packages
- [ ] **Multi-Institution Support**: Configurable pipelines for different universities
- [ ] **Real-time Processing**: Streaming data ingestion for live dashboards
- [ ] **Cloud Deployment**: Docker containerization and cloud-native architecture

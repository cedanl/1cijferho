# 📦 cijferho

**Professional Python package for processing Dutch educational data (DUO 1CHO files)**

Transform complex DUO datasets into research-ready insights with automated extraction, validation, conversion, and encryption.

[![PyPI version](https://badge.fury.io/py/cijferho.svg)](https://pypi.org/project/cijferho/)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Quick Start
```bash
pip install cijferho
```

```python
import cijferho

# Complete pipeline in one line
cijferho.quick_process("data/01-input")

# Or step by step
cijferho.process_txt_folder("data/01-input")
cijferho.validate_metadata_folder()
cijferho.match_files("data/01-input")
cijferho.run_conversions_from_matches("data/01-input")
cijferho.convert_csv_to_parquet()
cijferho.encryptor()
```

## ✨ Features

🔍 Smart Extraction - Auto-extract field positions from messy .txt files
✅ Intelligent Validation - Catch errors in metadata before processing
⚡ Turbo Conversion - Multiprocessing for blazing-fast file conversion
🗜️ Compression - 60-80% file size reduction with Parquet
🔒 Privacy Protection - SHA256 encryption for sensitive columns (BSN, etc.)
📊 Research-Ready - Clean CSV/Parquet output for immediate analysis

## 📖 Documentation

### Extraction

```python
# Extract metadata from Bestandsbeschrijving files
cijferho.process_txt_folder("data/01-input")

# Convert JSON to Excel
cijferho.process_json_folder()
```

### Validation

```python
# Validate metadata structure
cijferho.validate_metadata_folder()

# Match input files with metadata
cijferho.match_files("data/01-input")

# Validate conversion results
cijferho.converter_validation()
```

### Conversion

```python
# Convert fixed-width to CSV
cijferho.run_conversions_from_matches("data/01-input")

# Compress to Parquet
cijferho.convert_csv_to_parquet()
```

### Processing

```python
# Encrypt sensitive columns
cijferho.encryptor()
```

### 🔧 Advanced Usage

```python
from cijferho import (
    extract_tables_from_txt,
    converter,
    validate_metadata
)

# Process single file
extract_tables_from_txt("bestand.txt", "output/")

# Convert specific file
converter("input.dat", "metadata.xlsx")

# Validate specific metadata
result = validate_metadata("metadata.xlsx")
```

## 🤝 Contributing
Built for the Dutch Higher Education community by CEDA.

- 🐛 Report bugs
- 💡 Request features
- 📖 Read full docs

📄 License
MIT License - see LICENSE

## 🙏 Acknowledgements

Special thanks to:
- CEDA & Npuls for making this project possible
- All contributors to the 1CijferHO project

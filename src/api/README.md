# 🚀 CijferHO API

**REST API for processing Dutch educational data (DUO 1CHO files)**

Professional FastAPI service that exposes the cijferho package functionality through HTTP endpoints.

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com/)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Quick Start

### Local Development
```bash
# Install dependencies
pip install cijferho fastapi uvicorn

# Run the API
uvicorn src.api.main:app --reload

# Open interactive docs
open http://localhost:8000/docs
```
# API available at http://localhost:8000
# Docs at http://localhost:8000/docs
```

## 📚 API Endpoints
### Extraction
- ```POST /extraction/process-txt - Extract metadata from .txt files```
- ```POST /extraction/process-json - Convert JSON to Excel```

### Validation
- ```POST /validation/validate-metadata - Validate metadata structure```
- ```POST /validation/match-files - Match input with metadata files```
- ```POST /validation/validate-conversion - Validate conversion results```

### Conversion
- ```POST /conversion/convert-files - Convert fixed-width to CSV```

### Processing
- ```POST /processing/compress - Compress CSV to Parquet```
- ```POST /processing/encrypt - Encrypt sensitive columns```

### Pipeline
- ```POST /pipeline/quick-process - Run complete pipeline```

### Health
- ```GET / - API overview```
- ```GET /health - Health check```

## 💻 Usage Examples
### cURL
```bash
# Complete pipeline
curl -X POST "http://localhost:8000/pipeline/quick-process" \
  -H "Content-Type: application/json" \
  -d '{"input_folder": "data/01-input"}'

# Extract metadata
curl -X POST "http://localhost:8000/extraction/process-txt" \
  -H "Content-Type: application/json" \
  -d '{"input_folder": "data/01-input"}'
```

### Python
```python
import requests

API_URL = "http://localhost:8000"

# Run complete pipeline
response = requests.post(
    f"{API_URL}/pipeline/quick-process",
    json={"input_folder": "data/01-input"}
)
print(response.json())

# Extract metadata only
response = requests.post(
    f"{API_URL}/extraction/process-txt",
    json={"input_folder": "data/01-input"}
)
print(response.json())
```

## 📈 Performance

- Async endpoints for non-blocking operations
- Multiprocessing for file conversion
- Optimized for large file processing (GB+)

## 🤝 Contributing
- 🐛 Report bugs
- 💡 Request features

## 📄 License
- MIT License - see LICENSE

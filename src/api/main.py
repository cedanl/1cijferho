# src/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.routers import extraction, validation, conversion, processing
from api.models import HealthResponse, ProcessRequest, ProcessResponse
import cijferho

app = FastAPI(
    title="CijferHO API",
    description="Professional API for processing Dutch educational data (DUO files)",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware voor Streamlit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In productie: specifieke origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(extraction.router, prefix="/extraction", tags=["Extraction"])
app.include_router(validation.router, prefix="/validation", tags=["Validation"])
app.include_router(conversion.router, prefix="/conversion", tags=["Conversion"])
app.include_router(processing.router, prefix="/processing", tags=["Processing"])

@app.get("/", response_model=HealthResponse)
async def root():
    """API health check and endpoint overview"""
    return HealthResponse(
        status="healthy",
        version=cijferho.__version__,
        endpoints={
            "extraction": "/extraction",
            "validation": "/validation",
            "conversion": "/conversion",
            "processing": "/processing",
            "pipeline": "/pipeline/quick-process",
            "docs": "/docs",
        }
    )

@app.get("/health")
async def health():
    """Detailed health check"""
    return {
        "status": "healthy",
        "package_version": cijferho.__version__,
        "api_version": "0.1.0"
    }

@app.post("/pipeline/quick-process")
async def quick_process(request: ProcessRequest):
    """Run complete processing pipeline"""
    try:
        # Run synchronously for now (kan later background task worden)
        cijferho.quick_process(request.input_folder)
        return ProcessResponse(
            status="success",
            message=f"Complete pipeline finished for {request.input_folder}",
            details={"steps": 7, "folder": request.input_folder}
        )
    except Exception as e:
        return ProcessResponse(
            status="error",
            message=str(e),
            details={"folder": request.input_folder}
        )

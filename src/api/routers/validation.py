# src/api/routers/validation.py
from fastapi import APIRouter, HTTPException
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from api.models import ProcessRequest, ProcessResponse
import cijferho

router = APIRouter()

@router.post("/validate-metadata", response_model=ProcessResponse)
async def validate_metadata():
    """
    Validate extracted metadata Excel files

    Checks for:
    - Duplicate field names
    - Position gaps/overlaps
    - Length consistency
    """
    try:
        cijferho.validate_metadata_folder()
        return ProcessResponse(
            status="success",
            message="Metadata validation completed",
            details={}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/match-files", response_model=ProcessResponse)
async def match_files(request: ProcessRequest):
    """
    Match input data files with their metadata files

    Links DUO data files to their corresponding Bestandsbeschrijving
    metadata based on filename patterns.
    """
    try:
        cijferho.match_files(request.input_folder)
        return ProcessResponse(
            status="success",
            message=f"File matching completed for {request.input_folder}",
            details={"input_folder": request.input_folder}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/validate-conversion", response_model=ProcessResponse)
async def validate_conversion():
    """
    Validate that file conversion was successful

    Compares row counts between input and output files
    to ensure no data loss.
    """
    try:
        cijferho.converter_validation()
        return ProcessResponse(
            status="success",
            message="Conversion validation completed",
            details={}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

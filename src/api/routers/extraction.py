# src/api/routers/extraction.py
from fastapi import APIRouter, HTTPException
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from api.models import ProcessRequest, ProcessResponse
import cijferho

router = APIRouter()

@router.post("/process-txt", response_model=ProcessResponse)
async def extract_txt_files(request: ProcessRequest):
    """
    Extract metadata from Bestandsbeschrijving .txt files

    Processes all .txt files containing 'Bestandsbeschrijving' in the input folder
    and saves extracted tables as JSON files.
    """
    try:
        cijferho.process_txt_folder(request.input_folder)
        return ProcessResponse(
            status="success",
            message=f"Extracted metadata from {request.input_folder}",
            details={"input_folder": request.input_folder}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process-json", response_model=ProcessResponse)
async def convert_json_to_excel():
    """
    Convert extracted JSON metadata to Excel files

    Processes all JSON files in data/00-metadata/json and creates
    structured Excel files with field definitions.
    """
    try:
        cijferho.process_json_folder()
        return ProcessResponse(
            status="success",
            message="Converted JSON metadata to Excel",
            details={}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# src/api/routers/conversion.py
from fastapi import APIRouter, HTTPException
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from api.models import ProcessRequest, ProcessResponse
import cijferho

router = APIRouter()

@router.post("/convert-files", response_model=ProcessResponse)
async def convert_files(request: ProcessRequest):
    """
    Convert fixed-width DUO files to CSV format

    Uses multiprocessing for fast conversion of large files.
    Output: Pipe-delimited CSV files in data/02-output/
    """
    try:
        cijferho.run_conversions_from_matches(request.input_folder)
        return ProcessResponse(
            status="success",
            message=f"File conversion completed for {request.input_folder}",
            details={"input_folder": request.input_folder}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

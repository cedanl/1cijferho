# src/api/routers/processing.py
from fastapi import APIRouter, HTTPException
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from api.models import ProcessResponse
import cijferho

router = APIRouter()

@router.post("/compress", response_model=ProcessResponse)
async def compress_files():
    """
    Compress CSV files to Parquet format

    Reduces file size by 60-80% while maintaining full data fidelity.
    Skips files containing 'dec' in filename.
    """
    try:
        cijferho.convert_csv_to_parquet()
        return ProcessResponse(
            status="success",
            message="CSV files compressed to Parquet",
            details={}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/encrypt", response_model=ProcessResponse)
async def encrypt_files():
    """
    Encrypt sensitive columns using SHA256 hashing

    Encrypts: Persoonsgebonden nummer, Burgerservicenummer, Onderwijsnummer
    Only processes EV* and VAKHAVW* files.
    """
    try:
        cijferho.encryptor()
        return ProcessResponse(
            status="success",
            message="Sensitive data encrypted",
            details={}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# src/api/models.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict

class ProcessRequest(BaseModel):
    """Generic request for folder processing"""
    input_folder: str = Field(..., description="Path to input folder", example="data/01-input")

class ProcessResponse(BaseModel):
    """Generic response for processing tasks"""
    status: str = Field(..., description="Processing status", example="processing")
    message: str = Field(..., description="Status message")
    details: Optional[Dict] = Field(default=None, description="Additional details")

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    version: str
    endpoints: Dict[str, str]

class FileRequest(BaseModel):
    """Request for single file processing"""
    input_file: str = Field(..., description="Path to input file")
    metadata_file: str = Field(..., description="Path to metadata file")

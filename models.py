# models.py
from pydantic import BaseModel
from typing import List, Dict, Any


class Status(BaseModel):
    messageType: str
    message: str


class Metadata(BaseModel):
    pagination: Dict[str, Any] = {}
    status: List[Status] = []
    datafiles: List[Dict[str, Any]] = []


class ApiResponse(BaseModel):
    metadata: Metadata
    result: Any  # Use Any to allow different types of payloads


class Variant(BaseModel):
    chromosome: str
    position: int
    ref: str
    alt: str
    quality: float
    filter: str
    info: str


class QualSummary(BaseModel):
    chromosome: str
    quality: float

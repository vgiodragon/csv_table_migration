from fastapi import APIRouter

from .endpoints import upload_tables

router = APIRouter()
router.include_router(upload_tables.router, prefix="/historic_data", tags=["CSVTable"])

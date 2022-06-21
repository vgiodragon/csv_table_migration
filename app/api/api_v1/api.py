from fastapi import APIRouter

from .endpoints import upload_departments

router = APIRouter()
router.include_router(upload_departments.router, prefix="/historic_data", tags=["CSVTable"])

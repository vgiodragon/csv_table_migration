from fastapi import APIRouter

from .endpoints import metrics, upload_tables

router = APIRouter()
router.include_router(upload_tables.router, prefix="/historic_data", tags=["CSVTable"])
router.include_router(metrics.router, prefix="/metrics", tags=["Metrics"])

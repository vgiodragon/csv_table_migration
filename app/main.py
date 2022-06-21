import os

from app.api.api_v1.api import router as api_router
from fastapi import FastAPI
from mangum import Mangum

stage = os.environ.get('STAGE', None)
root_path = f"/{stage}" if stage else "/"

app = FastAPI(title="POC_Globant", root_path=root_path)  # Here is the magic

app.include_router(api_router, prefix="/api/v1")

handler = Mangum(app)

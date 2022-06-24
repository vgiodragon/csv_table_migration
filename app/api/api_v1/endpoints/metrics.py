import json
import logging

from app.api.api_v1.services import settings
from app.api.api_v1.services.metric_generator import MetricGenerator
from app.api.api_v1.services.util import METRIC
from fastapi import APIRouter

router = APIRouter()


@router.get("/quarter/")
async def metric_quarter(year: str):
    metric_generator = MetricGenerator(
        METRIC.QUARTER,
        settings.DB_CREDENTIALS,
        settings.DB_REGION
    )
    metric = metric_generator.get_metric(year)
    return {'data': json.dumps(metric)}

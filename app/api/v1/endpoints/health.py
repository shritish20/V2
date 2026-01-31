"""
Health and Metrics Endpoints
"""
from fastapi import APIRouter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

from app.infrastructure.metrics import PROM_REGISTRY

router = APIRouter()

@router.get("/")
def health_check():
    return {"status": "healthy", "service": "volguard-api"}

@router.get("/metrics")
def metrics():
    return Response(content=generate_latest(PROM_REGISTRY), media_type=CONTENT_TYPE_LATEST)

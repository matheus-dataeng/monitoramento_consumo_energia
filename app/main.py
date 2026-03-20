import logging as log
from fastapi import FastAPI
from src.utils.logger_setup import setup_log
from app.routers.dim_tempo import router as tempo_router
from app.routers.dim_subsistema import router as subsistema_router
from app.routers.fato_carga_energia import router as fato_router
from app.routers.metrics import router as metrics_router

setup_log()
logger = log.getLogger(__name__)

app = FastAPI(
    title= "API para consulta ao consumo de energia no Brasil em 2025",
    version="1.0.0",
)

app.include_router(
    tempo_router,
    prefix="/tempo",
    tags=["Tempo ⏱️"]
)

app.include_router(
    subsistema_router,
    prefix="/subsistema",
    tags=["Subsistema 🧩"]
)

app.include_router(
    fato_router,
    prefix="/carga_energia",
    tags=["Carga_energia ⚡"]
)

app.include_router(
    metrics_router,
    prefix="/metricas",
    tags=["Metricas 📊"]
)
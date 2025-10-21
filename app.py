"""
app.py — API base con FastAPI lista para producción (gunicorn + uvicorn workers)

Características:
- Health checks: /healthz (liveness), /ready (readiness)
- Versionado: /version
- CORS configurable por variables de entorno
- Compresión GZip
- Logging estructurado (JSON-like) y nivel configurable
- Endpoints de ejemplo: /api/v1/ping, /api/v1/echo, /api/v1/time
- Lifespan con hooks de startup/shutdown

Requisitos (requirements.txt):
fastapi>=0.115.0
uvicorn[standard]>=0.30.0
gunicorn>=22.0.0
pydantic-settings>=2.3.0
python-dotenv>=1.0.1
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- imports nuevos ---
from typing import Literal
from fastapi import BackgroundTasks, HTTPException
import logging

# Si volcado_clientes.py está en la raíz del repo:
from volcado_clientes import (
    pipeline_incremental_horario,
    pipeline_upsert_nocturno,
    run_query,
)

logger = logging.getLogger("etl")

def _run_etl(mode: Literal["incremental", "nocturno"]) -> None:
    """Ejecuta el ETL en el mismo proceso."""
    try:
        if mode == "incremental":
            logger.info("ETL: iniciando incremental")
            pipeline_incremental_horario(run_query)
            logger.info("ETL: incremental finalizado OK")
        elif mode == "nocturno":
            logger.info("ETL: iniciando nocturno")
            pipeline_upsert_nocturno(run_query)
            logger.info("ETL: nocturno finalizado OK")
        else:
            raise ValueError(f"Modo ETL desconocido: {mode}")
    except Exception:
        logger.exception("ETL: error ejecutando modo=%s", mode)
        # aquí podrías notificar/registrar a donde quieras
        raise



from pydantic import BaseModel

class EtlRequest(BaseModel):
    mode: Literal["incremental", "nocturno"] = "incremental"





# -----------------------------
# Configuración vía variables de entorno
# -----------------------------
class Settings(BaseSettings):
    APP_NAME: str = "beAPI"
    APP_ENV: str = "production"  # production | staging | development
    APP_VERSION: str = os.getenv("APP_VERSION", "1.0.0")
    LOG_LEVEL: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    PORT: int = 8000
    HOST: str = "0.0.0.0"

    # Seguridad/red
    ALLOWED_ORIGINS: List[str] = ["*"]
    TRUSTED_HOSTS: List[str] = ["*"]  # p.ej. ["api.tudominio.com", "localhost"]

    # Readiness: si tienes dependencias externas, cámbialo a False hasta que estén OK
    READY_ON_START: bool = True

    model_config = SettingsConfigDict(env_prefix="APP_", env_file=(".env", ".env.local",), extra="ignore")

    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_up = (v or "INFO").upper()
        return v_up if v_up in allowed else "INFO"


settings = Settings()


# -----------------------------
# Logging JSON-like
# -----------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


def configure_logging(level: str = settings.LOG_LEVEL) -> None:
    root = logging.getLogger()
    root.setLevel(level)

    # Limpia handlers por si se recarga
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)

    # Ajusta loggers de uvicorn
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers = [handler]
        logging.getLogger(name).setLevel(level)


configure_logging()
logger = logging.getLogger("app")


# -----------------------------
# Aplicación FastAPI
# -----------------------------
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Middlewares
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.TRUSTED_HOSTS)


# Estado interno simple para readiness
class AppState:
    ready: bool = settings.READY_ON_START


state = AppState()


# -----------------------------
# Modelos de ejemplo
# -----------------------------
class EchoIn(BaseModel):
    message: str


class EchoOut(BaseModel):
    message: str
    received_at: datetime


# -----------------------------
# Eventos de ciclo de vida
# -----------------------------
@app.on_event("startup")
async def on_startup():
    logger.info("startup: initializing app")
    # Aquí podrías hacer pings a DBs, colas, etc. y fijar readiness
    # state.ready = await check_dependencies()


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("shutdown: stopping app")


# -----------------------------
# Endpoints básicos
# -----------------------------
@app.get("/", summary="Raíz: info de la API")
async def root():
    return {
        "name": settings.APP_NAME,
        "env": settings.APP_ENV,
        "version": settings.APP_VERSION,
        "utc_now": datetime.now(timezone.utc).isoformat(),
        "docs": "/docs",
        "healthz": "/healthz",
        "ready": "/ready",
    }


@app.get("/healthz", summary="Liveness probe")
async def healthz():
    return {"status": "ok"}


@app.get("/ready", summary="Readiness probe")
async def ready():
    return {"ready": bool(state.ready)}


@app.get("/version", summary="Versión de la app")
async def version():
    return {"name": settings.APP_NAME, "version": settings.APP_VERSION}


@app.get("/api/v1/ping", summary="Ping simple")
async def ping():
    return {"pong": True, "ts": datetime.now(timezone.utc).isoformat()}


@app.post("/api/v1/echo", response_model=EchoOut, summary="Devuelve el mismo mensaje")
async def echo(payload: EchoIn):
    return EchoOut(message=payload.message, received_at=datetime.now(timezone.utc))


@app.get("/api/v1/time", summary="Hora UTC del servidor")
async def time():
    return {"utc_now": datetime.now(timezone.utc).isoformat()}


# -----------------------------
# Middleware simple de logging por petición
# -----------------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = datetime.now(timezone.utc)
    response: Optional[Response] = None
    try:
        response = await call_next(request)
        return response
    finally:
        duration_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        logger.info(
            "request",
        )
        # Log de acceso estilo JSON (mínimo)
        logging.getLogger("uvicorn.access").info(
            json.dumps(
                {
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code if response else 500,
                    "ms": round(duration_ms, 2),
                    "client": request.client.host if request.client else None,
                },
                ensure_ascii=False,
            )
        )


@app.post("/api/v1/etl", summary="Dispara el ETL (incremental o nocturno)")
async def trigger_etl(payload: EtlRequest, background: BackgroundTasks):
    mode = payload.mode
    try:
        # Lanza en background para no bloquear la respuesta HTTP
        background.add_task(_run_etl, mode)
        return {"status": "accepted", "mode": mode}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception:
        logger.exception("Fallo al encolar el ETL")
        raise HTTPException(status_code=500, detail="No se pudo lanzar el ETL")


# --- NUEVOS IMPORTS (ajusta módulos reales si cambian nombres) ---
from typing import Literal, Optional
from fastapi import BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

# Clientes (ya lo tienes)
from volcado_clientes import (
    pipeline_incremental_horario as clientes_incremental,
    pipeline_upsert_nocturno as clientes_nocturno,
    run_query,
)

# >>>> AJUSTA ESTOS IMPORTS SEGÚN TU CÓDIGO REAL <<<<
# Reservas:
# p.ej. volcado_reservas.py o etl/reservas.py

from volcado_reservas import (
        pipeline_incremental as reservas_incremental,
        pipeline_nocturno as reservas_nocturno,
)


from autoschedule_confirmados import run_incremental_confirmados        # def run_incremental_confirmados(window_hours: int = 24) -> None

from autoschedule_viajes import (plan_como_ha_ido_para_dia,           # def plan_como_ha_ido_para_dia(target_day: str) -> None
        plan_vuestra_aventura_para_dia      # def plan_vuestra_aventura_para_dia(target_day: str, require_como_ha_ido: bool = True) -> None
    )


MAD = ZoneInfo("Europe/Madrid")

# ========= MODELOS DE ENTRADA =========
class ModeRequest(BaseModel):
    mode: Literal["incremental", "nocturno"]

class DayRequest(BaseModel):
    target_day: str = Field(..., description="Formato YYYY-MM-DD")

    @field_validator("target_day")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        # Validación sencilla de formato YYYY-MM-DD
        try:
            from datetime import date
            y, m, d = (int(x) for x in v.split("-"))
            date(y, m, d)
        except Exception:
            raise ValueError("target_day debe tener formato YYYY-MM-DD")
        return v


# ========= WRAPPERS SENCILLOS =========
def _run_clientes(mode: Literal["incremental", "nocturno"]) -> None:
    try:
        if mode == "incremental":
            logger.info("ETL clientes incremental: INICIO")
            clientes_incremental(run_query)
        else:
            logger.info("ETL clientes nocturno: INICIO")
            clientes_nocturno(run_query)
        logger.info("ETL clientes: OK (%s)", mode)
    except Exception:
        logger.exception("ETL clientes: fallo (%s)", mode)
        raise

def _run_reservas(mode: Literal["incremental", "nocturno"]) -> None:
    if reservas_incremental is None or reservas_nocturno is None:
        raise RuntimeError("Reservas: pipelines no importados. Revisa los imports.")
    try:
        if mode == "incremental":
            logger.info("ETL reservas incremental: INICIO")
            reservas_incremental()
        else:
            logger.info("ETL reservas nocturno: INICIO")
            reservas_nocturno()
        logger.info("ETL reservas: OK (%s)", mode)
    except Exception:
        logger.exception("ETL reservas: fallo (%s)", mode)
        raise

def _run_confirmados() -> None:
    if run_incremental_confirmados is None:
        raise RuntimeError("run_incremental_confirmados no importado. Revisa el módulo autoschedule.")
    try:
        logger.info("Autoschedule confirmados (horario): INICIO")
        # Sin argumentos: usará su ventana y lógica por defecto
        run_incremental_confirmados()
        logger.info("Autoschedule confirmados (horario): OK")
    except Exception:
        logger.exception("Autoschedule confirmados (horario): fallo")
        raise

def _run_como_ha_ido(target_day: str) -> None:
    if plan_como_ha_ido_para_dia is None:
        raise RuntimeError("plan_como_ha_ido_para_dia no importado. Revisa el módulo autoschedule.")
    try:
        logger.info("Autoschedule como_ha_ido: INICIO (day=%s)", target_day)
        plan_como_ha_ido_para_dia(target_day)
        logger.info("Autoschedule como_ha_ido: OK (day=%s)", target_day)
    except Exception:
        logger.exception("Autoschedule como_ha_ido: fallo (day=%s)", target_day)
        raise

def _run_vuestra_aventura(target_day: str) -> None:
    if plan_vuestra_aventura_para_dia is None:
        raise RuntimeError("plan_vuestra_aventura_para_dia no importado. Revisa el módulo autoschedule.")
    try:
        logger.info("Autoschedule vuestra_aventura: INICIO (day=%s)", target_day)
        plan_vuestra_aventura_para_dia(target_day)
        logger.info("Autoschedule vuestra_aventura: OK (day=%s)", target_day)
    except Exception:
        logger.exception("Autoschedule vuestra_aventura: fallo (day=%s)", target_day)
        raise


# ========= ENDPOINTS, UNO POR GRUPO =========

# 1) CLIENTES
@app.post("/api/v1/etl/clientes", summary="ETL clientes (mode: incremental | nocturno)")
async def etl_clientes(payload: ModeRequest, background: BackgroundTasks):
    try:
        background.add_task(_run_clientes, payload.mode)
        return {"status": "accepted", "group": "clientes", "mode": payload.mode}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 2) RESERVAS
@app.post("/api/v1/etl/reservas", summary="ETL reservas (mode: incremental | nocturno)")
async def etl_reservas(payload: ModeRequest, background: BackgroundTasks):
    try:
        background.add_task(_run_reservas, payload.mode)
        return {"status": "accepted", "group": "reservas", "mode": payload.mode}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3) CONFIRMADOS
@app.post("/api/v1/autoschedule/confirmados", summary="Autoschedule confirmados (horario, sin parámetros)")
async def autoschedule_confirmados(background: BackgroundTasks):
    try:
        background.add_task(_run_confirmados)
        return {"status": "accepted", "group": "confirmados"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 4) COMO_HA_IDO (solo fecha)
@app.post("/api/v1/autoschedule/como_ha_ido", summary="Autoschedule 'como_ha_ido' para un día (YYYY-MM-DD)")
async def autoschedule_como_ha_ido(payload: DayRequest, background: BackgroundTasks):
    try:
        background.add_task(_run_como_ha_ido, payload.target_day)
        return {"status": "accepted", "group": "como_ha_ido", "target_day": payload.target_day}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 5) VUESTRA_AVENTURA (solo fecha)
@app.post("/api/v1/autoschedule/vuestra_aventura", summary="Autoschedule 'vuestra_aventura' para un día (YYYY-MM-DD)")
async def autoschedule_vuestra_aventura(payload: DayRequest, background: BackgroundTasks):
    try:
        background.add_task(_run_vuestra_aventura, payload.target_day)
        return {"status": "accepted", "group": "vuestra_aventura", "target_day": payload.target_day}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# Punto de entrada para desarrollo local (no usar en producción)
# -----------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=os.getenv("RELOAD", "false").lower() in {"1", "true", "yes"},
        log_level=settings.LOG_LEVEL.lower(),
    )

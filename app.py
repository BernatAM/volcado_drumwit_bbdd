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

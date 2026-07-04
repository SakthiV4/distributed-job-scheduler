import logging
import sys
import json
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings


# --- Structured JSON Logger ---

class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "request_id"):
            log_entry["request_id"] = record.request_id
        return json.dumps(log_entry)


def setup_logging() -> None:
    settings = get_settings()
    root_logger = logging.getLogger()
    root_logger.setLevel(settings.LOG_LEVEL.upper())

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # Quiet down noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


logger = logging.getLogger("scheduler")


# --- Application Lifespan ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logger.info("Job Scheduler API starting up")
    
    # Start reaper background task
    from worker.reaper import run_reaper_loop
    reaper_task = asyncio.create_task(run_reaper_loop())
    
    yield
    
    # Cancel reaper background task on shutdown
    reaper_task.cancel()
    try:
        await reaper_task
    except asyncio.CancelledError:
        pass
    logger.info("Job Scheduler API shutting down")


# --- FastAPI App ---

app = FastAPI(
    title="Distributed Job Scheduler",
    description="Production-inspired distributed job scheduling platform",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS
settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Request Logging Middleware ---

@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    import uuid
    request_id = str(uuid.uuid4())[:8]
    logger.info(
        "%s %s [%s]",
        request.method,
        request.url.path,
        request_id,
        extra={"request_id": request_id},
    )
    response: Response = await call_next(request)
    logger.info(
        "%s %s → %d [%s]",
        request.method,
        request.url.path,
        response.status_code,
        request_id,
        extra={"request_id": request_id},
    )
    return response


# --- Health Check ---

@app.get("/health", tags=["system"])
async def health_check():
    return {"status": "healthy"}


from app.routers import auth, organizations, projects, queues, jobs, retry_policies, dlq, system  # noqa: E402

app.include_router(auth.router, prefix="/api/v1/auth", tags=["auth"])
app.include_router(organizations.router, prefix="/api/v1/organizations", tags=["organizations"])
app.include_router(projects.router, prefix="/api/v1/projects", tags=["projects"])
app.include_router(queues.router, prefix="/api/v1", tags=["queues"])
app.include_router(jobs.router, prefix="/api/v1", tags=["jobs"])
app.include_router(retry_policies.router, prefix="/api/v1", tags=["retry-policies"])
app.include_router(dlq.router, prefix="/api/v1", tags=["dlq"])
app.include_router(system.router, prefix="/api/v1", tags=["system"])


# --- Static Files mounting for React Dashboard ---
import os
from fastapi.staticfiles import StaticFiles

# Resolve path to frontend build directory
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
frontend_dist = os.path.join(base_dir, "frontend", "dist")

if os.path.exists(frontend_dist):
    app.mount("/dashboard", StaticFiles(directory=frontend_dist, html=True), name="dashboard")
    logger.info("Mounted static frontend dashboard at /dashboard")
else:
    # Fallback for local workspace paths
    local_dist = os.path.abspath(os.path.join(base_dir, "..", "frontend", "dist"))
    if os.path.exists(local_dist):
        app.mount("/dashboard", StaticFiles(directory=local_dist, html=True), name="dashboard")
        logger.info("Mounted local workspace static frontend dashboard at /dashboard")
    else:
        logger.warning(f"Frontend dist directory not found at {frontend_dist} or {local_dist}. Static dashboard will not be served.")


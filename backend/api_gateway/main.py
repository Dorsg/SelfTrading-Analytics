from fastapi import FastAPI
from api_gateway.routes import auth_routes
from api_gateway.routes import analytics_routes
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# ───────── database initialisation ─────────
from database.db_core import engine
from database.models import Base
from sqlalchemy import inspect, text
import os
import logging
from backend.logger_config import setup_logging as setup_analytics_logging


def _configure_logging() -> None:
    # Use shared analytics logger config (mirrors main app)
    setup_analytics_logging()
    # Ensure uvicorn/fastapi propagate into our configured root
    for name in ["uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"]:
        lg = logging.getLogger(name)
        lg.setLevel(logging.INFO)
        lg.propagate = True

@app.on_event("startup")
async def _init_db() -> None:
    """Ensure all core tables exist before serving requests."""
    _configure_logging()
    logger = logging.getLogger(__name__)
    try:
        # Safety check: only create tables if they don't exist
        inspector = inspect(engine)
        if inspector.has_table("users"):
            logger.info("Database tables already exist, skipping initialization")
            # Verify database integrity
            with engine.connect() as conn:
                user_count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
                logger.info(f"Database verified: {user_count} users found")
            return
        
        # Respect production flag – do not create tables when disabled
        allow_create = os.getenv("DB_ALLOW_AUTO_CREATE_TABLES", "false").lower() == "true"
        if not allow_create:
            logger.warning("Tables missing but auto-create disabled. Waiting for migration/restore.")
            return
        logger.info("Creating database tables (tables missing)")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
        
        # Verify creation
        inspector_verify = inspect(engine)
        if inspector_verify.has_table("users"):
            logger.info("Database table creation verified")
        else:
            logger.error("Table creation verification failed!")
            
    except Exception as exc:
        logger.exception("Failed to initialize database tables on startup: %s", exc)
        # Don't raise - let the application start even if DB init fails
        # The existing retry logic in the database layer will handle it

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount all routers under /api
app.include_router(auth_routes.router,      prefix="/api")
app.include_router(analytics_routes.router, prefix="/api")
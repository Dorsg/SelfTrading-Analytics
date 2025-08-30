# backend/database/db_core.py
from __future__ import annotations

import re
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator, Optional
from sqlalchemy import inspect            
from database.models import Base          
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import (
    DisconnectionError,
    OperationalError,
    TimeoutError,
    UnboundExecutionError,
    DBAPIError,
)
from sqlalchemy.orm import sessionmaker


logger = logging.getLogger(__name__)

# ────────────────────────────── config ──────────────────────────────
# Analytics always uses PostgreSQL - build URL from environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "YUVAL142sabag")
POSTGRES_DB = os.getenv("POSTGRES_DB", "selftrading_analytics_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

logger.info(f"Using PostgreSQL database: {POSTGRES_USER}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))  # seconds
_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))    # seconds to wait for a free conn
_POOL_RECYCLE_ON_RECOVERY = int(os.getenv("DB_POOL_RECYCLE_ON_RECOVERY", "30"))  # force pool recycle on recovery mode

# Backoff window (shared between sync / async helpers)
_BACKOFF_UNTIL: float = 0.0
_INITIAL_BACKOFF = float(os.getenv("DB_INITIAL_BACKOFF", "2"))
_MAX_BACKOFF = float(os.getenv("DB_MAX_BACKOFF", "60"))
_RECOVERY_BACKOFF = float(os.getenv("DB_RECOVERY_BACKOFF", "10"))  # Special backoff for recovery mode
# controls for auto-creation (disabled in production via env)
_ALLOW_AUTO_CREATE_DB = os.getenv("DB_ALLOW_AUTO_CREATE_DB", "false").lower() == "true"
_ALLOW_AUTO_CREATE_TABLES = os.getenv("DB_ALLOW_AUTO_CREATE_TABLES", "false").lower() == "true"

# one-time DB creation guard
_DB_CREATED: bool = False

# rate-limit engine rebuilds
_LAST_REBUILD_AT: float = 0.0
_REBUILD_MIN_INTERVAL = float(os.getenv("DB_REBUILD_MIN_INTERVAL", "30"))

_RECOVERY_RE = re.compile(r"in recovery mode", re.I)
# Recovery tracking
_LAST_RECOVERY_DETECTED: float = 0.0
_RECOVERY_POOL_INVALIDATED: bool = False
# Globals
_ENGINE: Optional[Engine] = None
SessionLocal: sessionmaker  # initialised in _init_engine()

# Keep a legacy alias always pointing at the current engine
engine: Optional[Engine] = None

# Safety flag to prevent accidental data loss
_TABLES_VERIFIED_SAFE: bool = False


def _is_retryable_error(exc: BaseException) -> bool:
    """Decide if we should retry / rebuild the engine for this exception."""
    logger.debug(f"Checking if error is retryable: {type(exc).__name__}: {exc}")
    
    if isinstance(exc, (OperationalError, DisconnectionError, TimeoutError, UnboundExecutionError)):
        logger.debug(f"Error is retryable (SQLAlchemy exception): {type(exc).__name__}")
        return True

    if isinstance(exc, DBAPIError):
        # SQLAlchemy marks these when the underlying connection is dead.
        if getattr(exc, "connection_invalidated", False):
            logger.debug("Error is retryable (connection invalidated)")
            return True
        msg = str(exc).lower()
        if "server closed the connection unexpectedly" in msg:
            logger.debug("Error is retryable (server closed connection unexpectedly)")
            return True
        if "connection not open" in msg or "terminating connection due to" in msg:
            logger.debug("Error is retryable (connection not open or terminating)")
            return True
        if "in recovery mode" in msg:
            logger.debug("Error is retryable (database in recovery mode)")
            return True
        if "connection to server" in msg and "failed" in msg:
            logger.debug("Error is retryable (connection to server failed)")
            return True

    logger.debug(f"Error is NOT retryable: {type(exc).__name__}")
    return False

def _is_undefined_database_error(exc: BaseException) -> bool:
    """
    True when the OperationalError is 'database "<name>" does not exist'
    (psycopg2 SQLSTATE 3D000). Works both on wrapped SA exceptions and raw DBAPI.
    """
    logger.debug(f"Checking if error is undefined database: {type(exc).__name__}: {exc}")
    
    try:
        # SQLAlchemy OperationalError → .orig is the psycopg2 error
        orig = getattr(exc, "orig", exc)
        pgcode = getattr(orig, "pgcode", None)
        if pgcode == "3D000":  # UndefinedDatabase
            logger.debug(f"Error is undefined database (pgcode 3D000): {pgcode}")
            return True
    except Exception as e:
        logger.debug(f"Exception while checking pgcode: {e}")

    msg = str(exc).lower()
    is_undefined = ("does not exist" in msg) and ("database" in msg)
    if is_undefined:
        logger.debug(f"Error is undefined database (message match): {msg}")
    else:
        logger.debug(f"Error is NOT undefined database (message): {msg}")
    return is_undefined


def _create_engine() -> Engine:
    """
    Build an Engine that:
      • pre‑pings every connection (drops dead ones immediately)
      • enforces short statement timeouts so "stuck" transactions don't sit forever
      • uses TCP keepalives to detect half‑open sockets quickly
    """
    # Database-specific connection arguments
    connect_args = {}
    url = make_url(DATABASE_URL)
    
    if url.get_backend_name() == "postgresql":
        connect_args = {
            "options": "-c statement_timeout=60000",  # 60s
            "keepalives": 1,
            "keepalives_idle": 60,
            "keepalives_interval": 10,
            "keepalives_count": 5,
            "connect_timeout": 10,
        }
    # SQLite doesn't need special connect_args

    eng = create_engine(
        DATABASE_URL,
        pool_size=_POOL_SIZE,
        max_overflow=_MAX_OVERFLOW,
        pool_timeout=_POOL_TIMEOUT,
        pool_recycle=_POOL_RECYCLE,
        pool_pre_ping=True,                 # cheap ping before using a pooled conn
        pool_reset_on_return="rollback",    # fine; we just need to handle dead conns
        connect_args=connect_args,
        future=True,
    )

    @event.listens_for(eng, "engine_connect")
    def _ping_connection(conn, branch):
        """
        A stronger guard than pool_pre_ping for connections that survived in the pool
        but the server reset them meanwhile. If this fails, we force SA to reconnect.
        """
        if branch:
            return
        try:
            conn.scalar(text("SELECT 1"))
        except Exception as exc:
            # Signal SQLAlchemy that this DBAPI connection is dead
            if _is_retryable_error(exc):
                raise DisconnectionError() from exc
            raise
        finally:
            if conn.in_transaction():
                # ensure no leftover tx
                conn.rollback()

    @event.listens_for(eng, "checkout")
    def _validate_checkout(dbapi_con, con_record, con_proxy):
        """
        Hard kill obviously-dead sockets immediately on checkout.
        pool_pre_ping already handles most cases; this is belt-and-braces.
        """
        try:
            cur = dbapi_con.cursor()
            cur.execute("SELECT 1")
            cur.close()
        except Exception as exc:  # pragma: no cover
            raise DisconnectionError() from exc

    return eng


# ─────────────────────── ensure core schema exists ───────────────────────
def _ensure_core_schema(engine):
    """
    SAFETY: Creates tables only if they don't exist. 
    Never drops or modifies existing tables.
    Safe to call multiple times - idempotent operation.
    """
    try:
        insp = inspect(engine)
        if insp.has_table("users"):  # quick existence probe
            logger.debug("Core schema already exists - skipping creation")
            return
        
        logger.info("Core schema missing - creating tables")
        Base.metadata.create_all(bind=engine)
        logger.info("Core schema created successfully")
        
        # Verify creation worked
        insp_verify = inspect(engine)
        if insp_verify.has_table("users"):
            logger.info("Core schema creation verified")
        else:
            logger.error("Core schema creation verification failed!")
            
    except Exception as exc:
        if _is_undefined_database_error(exc):
            try:
                logger.warning("Database does not exist - attempting to create it")
                _ensure_database_exists(DATABASE_URL)
                rebuild_engine()
                eng = _ENGINE or engine
                insp = inspect(eng)
                if not insp.has_table("users"):
                    logger.info("Creating core schema after database creation")
                    Base.metadata.create_all(bind=eng)
                    logger.info("Core schema created after database creation")
                return
            except Exception as create_exc:
                logger.exception(f"Failed to auto-create database and tables: {type(create_exc).__name__}: {create_exc}")
                # Still try to create tables with existing engine as fallback
                try:
                    Base.metadata.create_all(bind=engine)
                    logger.info("Core schema created with fallback method")
                    return
                except Exception as fallback_exc:
                    logger.exception(f"Fallback table creation also failed: {type(fallback_exc).__name__}: {fallback_exc}")
                return
        logger.exception("ensure_core_schema: unexpected error (will retry later)")



def _ensure_database_exists(database_url: str) -> None:
    """
    SAFETY: Only creates database if it doesn't exist.
    Never drops or modifies existing databases.
    """
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        return

    target_db = url.database
    maint_url = url.set(database="postgres")

    eng = create_engine(maint_url, isolation_level="AUTOCOMMIT", future=True)
    try:
        with eng.connect() as conn:
            exists = conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :d"),
                {"d": target_db},
            )
            if not exists:
                logger.warning(f"Database '{target_db}' does not exist - creating it")
                conn.execute(text(f'CREATE DATABASE "{target_db}"'))
                logger.info("Database %s created automatically", target_db)
            else:
                logger.debug(f"Database '{target_db}' already exists - no action needed")
    finally:
        eng.dispose()

def _ensure_database_exists_once(url: str) -> None:
    global _DB_CREATED
    if _DB_CREATED:
        return
    if not _ALLOW_AUTO_CREATE_DB:
        logger.info("Auto-create database disabled. Skipping ensure_database_exists_once().")
        return
    try:
        _ensure_database_exists(url)
        _DB_CREATED = True
    except Exception:
        logger.exception("ensure_database_exists_once failed – continuing without creating DB")


def _init_engine() -> None:
    global _ENGINE, engine, SessionLocal
    _ensure_database_exists_once(DATABASE_URL)
    _ENGINE = _create_engine()
    engine = _ENGINE  # keep legacy alias in sync
    SessionLocal = sessionmaker(
        bind=_ENGINE,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )



# initialise on import
_init_engine()
if _ALLOW_AUTO_CREATE_TABLES:
    _ensure_core_schema(engine)
else:
    logger.info("Auto-create tables disabled. Skipping metadata.create_all().")

# ────────────────────────────── backoff ──────────────────────────────
def _set_backoff_on_recovery(exc: BaseException) -> None:
    """
    If Postgres reports 'in recovery mode', extend the backoff window immediately
    and invalidate the connection pool to force fresh connections.
    """
    global _LAST_RECOVERY_DETECTED, _RECOVERY_POOL_INVALIDATED
    msg = str(exc)
    if _RECOVERY_RE.search(msg):
        now = time.time()
        _LAST_RECOVERY_DETECTED = now
        
        # Force longer backoff for recovery mode
        global _BACKOFF_UNTIL
        _BACKOFF_UNTIL = now + _RECOVERY_BACKOFF
        
        # Invalidate connection pool to force fresh connections
        if not _RECOVERY_POOL_INVALIDATED and _ENGINE is not None:
            try:
                logger.warning("Postgres in recovery mode - invalidating connection pool")
                _ENGINE.dispose()
                _RECOVERY_POOL_INVALIDATED = True
            except Exception as e:
                logger.exception(f"Failed to dispose engine during recovery: {e}")
        
        logger.warning(f"Postgres in recovery mode – backing off for {_RECOVERY_BACKOFF}s")


def _increase_backoff() -> None:
    global _BACKOFF_UNTIL
    now = time.time()
    if _BACKOFF_UNTIL < now:
        _BACKOFF_UNTIL = now + _INITIAL_BACKOFF
    else:
        _BACKOFF_UNTIL = min(_BACKOFF_UNTIL * 2, now + _MAX_BACKOFF)


def _clear_backoff() -> None:
    global _BACKOFF_UNTIL
    _BACKOFF_UNTIL = 0.0


def _sleep_blocking(seconds: float) -> None:
    # tiny blocking sleep – never > 0.2s to avoid freezing the event loop for long
    time.sleep(min(seconds, 0.2))


def _sleep_if_backing_off(max_wait: float = 0.5) -> None:
    """
    **Synchronous** variant – do NOT block for more than `max_wait` (default 0.5s).
    This keeps API/gateway threads “snappy” even when the DB is down.
    """
    delay = _BACKOFF_UNTIL - time.time()
    if delay <= 0:
        return

    if ping_database(timeout=1):
        logger.info("DB reachable again – clearing back-off window")
        _clear_backoff()
        return

    _sleep_blocking(min(delay, max_wait))


async def a_sleep_if_backing_off(max_wait: float = 2.0) -> None:
    """
    **Async** variant – yields control to the loop while we wait.
    Used from asyncio code paths (scheduler, sync tasks).
    """
    delay = _BACKOFF_UNTIL - time.time()
    if delay <= 0:
        return

    if ping_database(timeout=1):
        _clear_backoff()
        return

    await asyncio.sleep(min(delay, max_wait))


# ────────────────────────────── helpers ──────────────────────────────
def rebuild_engine() -> None:
    global _ENGINE, engine, _LAST_REBUILD_AT, _RECOVERY_POOL_INVALIDATED
    now = time.time()
    if now - _LAST_REBUILD_AT < _REBUILD_MIN_INTERVAL:
        logger.debug("rebuild_engine skipped – throttled")
        return
    _LAST_REBUILD_AT = now

    logger.info("Starting engine rebuild...")
    try:
        if _ENGINE is not None:
            logger.debug("Disposing old engine...")
            _ENGINE.dispose()
            logger.debug("Old engine disposed successfully")
    except Exception:
        logger.exception("Error while disposing old engine")

    logger.debug("Initializing new engine...")
    _init_engine()
    engine = _ENGINE
    _RECOVERY_POOL_INVALIDATED = False  # Reset recovery flag after rebuild
    logger.info("SQLAlchemy engine rebuilt successfully")



def ping_database(timeout: int = 2) -> bool:
    global _RECOVERY_POOL_INVALIDATED
    if _ENGINE is None:
        logger.debug("Ping failed - engine is None")
        return False
    try:
        logger.debug(f"Pinging database with timeout {timeout}s...")
        with _ENGINE.connect() as conn:
            conn.execution_options(timeout=timeout).scalar(text("SELECT 1"))
        logger.debug("Database ping successful")
        
        # Clear recovery state on successful ping
        if _RECOVERY_POOL_INVALIDATED:
            logger.info("Database recovered - clearing recovery state")
            _RECOVERY_POOL_INVALIDATED = False
            _clear_backoff()
        
        return True
    except (OperationalError, DBAPIError) as exc:
        logger.debug(f"Database ping failed: {type(exc).__name__}: {exc}")
        # Create the DB on the fly if it doesn't exist yet, then signal "not ready"
        if _is_undefined_database_error(exc):
            if _ALLOW_AUTO_CREATE_DB:
                logger.info("Database does not exist, attempting to create it...")
                try:
                    _ensure_database_exists(DATABASE_URL)
                    rebuild_engine()
                    logger.info("Database created and engine rebuilt successfully")
                except Exception:
                    logger.exception("Auto-create database from ping failed")
            else:
                logger.error("Database missing and auto-create disabled. Will retry later.")
            return False

        _set_backoff_on_recovery(exc)
        return False




def ensure_database_available_blocking() -> None:
    """Blocking probe (used by sync code)."""
    while not ping_database():
        _increase_backoff()
        _sleep_if_backing_off()


async def ensure_database_available_async() -> None:
    """Async probe (used by async code)."""
    while not ping_database():
        _increase_backoff()
        await a_sleep_if_backing_off()


# ────────────────────────────── sessions ──────────────────────────────
def new_session_blocking(retries: int = 3):
    """
    Get a fresh SessionLocal(), rebuilding the Engine on transient errors.
    Also self-creates the target database if it doesn't exist yet.
    Never blocks for long: sleeps are capped.
    """
    for attempt in range(1, retries + 1):
        _sleep_if_backing_off()
        try:
            return SessionLocal()
        except Exception as exc:
            # If the target DB doesn't exist yet, create it and retry immediately.
            if _is_undefined_database_error(exc):
                if _ALLOW_AUTO_CREATE_DB:
                    try:
                        _ensure_database_exists(DATABASE_URL)
                        rebuild_engine()
                        continue
                    except Exception:
                        logger.exception("Auto-create database failed")
                # fall through to normal retry handling

            _set_backoff_on_recovery(exc)
            if not _is_retryable_error(exc) or attempt == retries:
                raise
            logger.warning(
                "DB connect failed (%s) – attempt %d/%d",
                exc.__class__.__name__, attempt, retries,
            )
            rebuild_engine()

    raise OperationalError("Could not obtain a DB session after retries", None, None)



async def new_session_async(retries: int = 3):
    """
    Async wrapper – yields to the loop instead of blocking.
    Also self-creates the target database if it doesn't exist yet.
    """
    for attempt in range(1, retries + 1):
        await a_sleep_if_backing_off()
        try:
            return SessionLocal()
        except Exception as exc:
            if _is_undefined_database_error(exc):
                if _ALLOW_AUTO_CREATE_DB:
                    try:
                        _ensure_database_exists(DATABASE_URL)
                        rebuild_engine()
                        continue
                    except Exception:
                        logger.exception("Auto-create database (async) failed")

            _set_backoff_on_recovery(exc)
            if not _is_retryable_error(exc) or attempt == retries:
                raise
            logger.warning(
                "DB connect failed (%s) – attempt %d/%d",
                exc.__class__.__name__, attempt, retries,
            )
            rebuild_engine()

    raise OperationalError("Could not obtain a DB session after retries", None, None)





@contextmanager
def session_scope() -> Generator:
    db = new_session_blocking()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@asynccontextmanager
async def async_session_scope() -> AsyncGenerator:
    db = await new_session_async()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()



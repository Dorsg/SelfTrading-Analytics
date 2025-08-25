from __future__ import annotations

import functools
import logging
import time
import uuid
from datetime import datetime, timezone
import os
from typing import Callable, TypeVar

from sqlalchemy.exc import OperationalError

import database.db_core as dbc

T = TypeVar("T")
logger = logging.getLogger(__name__)

aware_utc_now = lambda: datetime.now(timezone.utc)

def canonical_cycle_seq(perm_id: int | None) -> str:
    """
    Deterministic cycle_seq for anything tied to an IB perm_id.
    Guarantees UI grouping stays intact across restarts/resyncs.

    NOTE: We intentionally keep the original behavior:
    for a concrete perm_id → "perm-{id}" (stable & deterministic).
    """
    return f"perm-{perm_id}" if perm_id is not None else uuid.uuid4().hex


def with_retry(max_attempts: int = 3, backoff: float = 2.0) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Retry transient DB failures. Uses db_core._is_retryable_error() to decide.
    The behavior mirrors the original implementation.
    """
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            delay = backoff
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    if not dbc._is_retryable_error(exc) or attempt == max_attempts:
                        raise
                    
                    # Handle recovery mode with special handling
                    if "in recovery mode" in str(exc).lower():
                        dbc._set_backoff_on_recovery(exc)
                        sleep_time = min(delay * 2, 10.0)  # Longer delay for recovery
                        logging.getLogger(fn.__module__).warning(
                            "%s failed (%d/%d) – Database in recovery mode – retrying in %.1fs",
                            fn.__name__, attempt, max_attempts, sleep_time,
                        )
                    else:
                        sleep_time = min(delay, 0.2)  # tiny blocking sleep
                        logging.getLogger(fn.__module__).warning(
                            "%s failed (%d/%d) – %s – retrying in %.1fs",
                            fn.__name__, attempt, max_attempts,
                            exc.__class__.__name__, sleep_time,
                        )
                    
                    dbc.rebuild_engine()
                    time.sleep(sleep_time)
                    delay *= backoff
        return wrapper
    return decorator


def new_session_with_retry(retries: int = 5):
    """
    Obtain a *fresh* SessionLocal(), rebuilding the Engine if necessary.
    Now also self-creates the target database if it doesn't exist yet (SQLSTATE 3D000).
    Uses the NON-blocking backoff from db_core.
    Additionally, verifies core tables exist; if missing, creates them once.
    """
    for attempt in range(1, retries + 1):
        dbc._sleep_if_backing_off(max_wait=0.5)
        try:
            session = dbc.SessionLocal()
            # Safety: ensure core tables exist (idempotent)
            try:
                from sqlalchemy import inspect
                from database.models import Base
                insp = inspect(dbc.engine)
                if not insp.has_table("users"):
                    allow_tables = os.getenv("DB_ALLOW_AUTO_CREATE_TABLES", "false").lower() == "true"
                    if allow_tables:
                        logger.warning("Core tables missing – creating schema now (safe, idempotent)")
                        Base.metadata.create_all(bind=dbc.engine)
                    else:
                        logger.warning("Core tables missing – auto-create disabled; waiting for migration/restore")
            except Exception:
                logger.exception("Failed to verify/create core tables")
            return session
        except Exception as exc:
            # If the target DB doesn't exist yet, create it and retry immediately.
            if dbc._is_undefined_database_error(exc):
                try:
                    dbc._ensure_database_exists(dbc.DATABASE_URL)
                    dbc.rebuild_engine()
                    continue  # try again right away on the fresh engine
                except Exception:
                    logger.exception("Auto-create database failed in new_session_with_retry")

            # Normal transient handling / backoff
            dbc._set_backoff_on_recovery(exc)
            if not dbc._is_retryable_error(exc) or attempt == retries:
                logger.error(
                    "DB connect failed permanently (%s) – attempt %d/%d",
                    exc.__class__.__name__, attempt, retries
                )
                raise
            
            # Handle recovery mode with special delay
            if "in recovery mode" in str(exc).lower():
                sleep_time = min(attempt * 2.0, 10.0)  # Longer delay for recovery
                logger.warning(
                    "DB connect failed (Database in recovery mode) – attempt %d/%d, retrying in %.1fs...",
                    attempt, retries, sleep_time
                )
            else:
                sleep_time = min(attempt * 0.5, 2.0)
                logger.warning(
                    "DB connect failed (%s) – attempt %d/%d, retrying in %.1fs...",
                    exc.__class__.__name__, attempt, retries, sleep_time
                )
            
            dbc.rebuild_engine()
            time.sleep(sleep_time)

    raise OperationalError("Could not obtain a database session after retries", None, None)


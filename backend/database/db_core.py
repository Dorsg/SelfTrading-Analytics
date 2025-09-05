from __future__ import annotations

import os
import time
import socket
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session, DeclarativeBase

log = logging.getLogger("database.db_core")


def _first_resolvable_host(candidates: list[str]) -> str:
    """Return the first host that resolves via DNS; fall back to the first item."""
    for h in candidates:
        try:
            socket.getaddrinfo(h, None)
            return h
        except Exception:
            continue
    return candidates[0]


def _build_url() -> str:
    user = os.getenv("DB_USER") or os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "postgres")
    # Prefer explicit envs; otherwise probe a few common service names
    cand = []
    if os.getenv("POSTGRES_HOST"):
        cand.append(os.getenv("POSTGRES_HOST", ""))
    if os.getenv("DB_HOST"):
        cand.append(os.getenv("DB_HOST", ""))
    cand += ["db", "postgres", "localhost", "127.0.0.1"]
    host = _first_resolvable_host([h for h in cand if h])

    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "selftrading_analytics_db")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


class Base(DeclarativeBase):
    pass


# Accept common envs in this order: DATABASE_URL (12-factor), DATABASE_URL_DOCKER, or build.
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_URL_DOCKER") or _build_url()

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
MAX_OVER = int(os.getenv("DB_MAX_OVERFLOW", "20"))
RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))

engine = create_engine(
    DATABASE_URL,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVER,
    pool_recycle=RECYCLE,
    pool_pre_ping=True,
    connect_args={"connect_timeout": CONNECT_TIMEOUT},
)

SessionLocal = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False))


def wait_for_db_ready(max_wait_seconds: int | None = None) -> None:
    """
    Ping the DB until it responds or time elapses.
    max_wait_seconds: environment DB_CONNECT_MAX_WAIT (default 30) if None.
    """
    deadline = time.monotonic() + int(os.getenv("DB_CONNECT_MAX_WAIT", "30") if max_wait_seconds is None else max_wait_seconds)
    attempt = 0
    while True:
        attempt += 1
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            if attempt > 1:
                log.info("Database is ready (after %d attempts).", attempt)
            return
        except Exception as e:
            if time.monotonic() >= deadline:
                log.error("Database not ready after retries: %s", e)
                raise
            time.sleep(min(0.5 * attempt, 3.0))

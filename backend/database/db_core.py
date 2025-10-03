from __future__ import annotations

import os
import time
import socket
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session, DeclarativeBase
from sqlalchemy.engine import make_url

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


def _sqlite_path_candidates() -> list[str]:
    # Custom env override first
    p = os.getenv("SQLITE_DB_PATH")
    cand = [p] if p else []
    # Common container path
    cand.append("/app/data/analytics.db")
    # Repo-local path
    cand.append(str(Path.cwd() / "data" / "analytics.db"))
    # Known workspace absolute path (Cursor/local)
    cand.append("/root/projects/SelfTrading Analytics/data/analytics.db")
    # De-dup while preserving order
    out: list[str] = []
    for c in cand:
        if c and c not in out:
            out.append(c)
    return out


def _build_url() -> str:
    # Optional: allow automatic SQLite fallback for local/dev usage when no DATABASE_URL provided
    allow_sqlite = os.getenv("ALLOW_SQLITE_FALLBACK", "1") == "1"
    if allow_sqlite:
        for p in _sqlite_path_candidates():
            try:
                if p and Path(p).exists():
                    url = f"sqlite:////{Path(p).resolve()}"
                    log.info("database.db_core: Using SQLite fallback at %s", p)
                    return url
            except Exception:
                continue

    # Default to Postgres (docker/dev/prod)
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

# Configure engine per driver
try:
    url = make_url(DATABASE_URL)
    driver = (url.drivername or "").lower()
except Exception:
    url = None
    driver = ""

connect_args: dict = {}
engine_kwargs: dict = {
    "pool_size": POOL_SIZE,
    "max_overflow": MAX_OVER,
    "pool_recycle": RECYCLE,
    "pool_pre_ping": True,
}

if driver.startswith("postgres"):
    connect_args = {"connect_timeout": CONNECT_TIMEOUT}
elif driver.startswith("sqlite"):
    # Safe defaults for local sqlite use; pool params are ignored by sqlite driver
    connect_args = {"check_same_thread": False}
    # Reduce kwargs that sqlite doesn't like
    engine_kwargs = {"pool_pre_ping": True}

engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    **engine_kwargs,
)

# Proactively validate connectivity. If Postgres is unreachable and SQLite
# fallback is allowed with a present DB file, reconfigure the engine to SQLite
# so local/dev environments work without a running PG service.
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except Exception as e:
    try:
        if os.getenv("ALLOW_SQLITE_FALLBACK", "1") == "1" and driver.startswith("postgres"):
            for p in _sqlite_path_candidates():
                try:
                    if p and Path(p).exists():
                        sqlite_url = f"sqlite:////{Path(p).resolve()}"
                        log.warning(
                            "database.db_core: Postgres unreachable (%s). Falling back to SQLite at %s",
                            str(e).splitlines()[0], p,
                        )
                        DATABASE_URL = sqlite_url  # type: ignore[assignment]
                        engine = create_engine(
                            sqlite_url,
                            connect_args={"check_same_thread": False},
                            pool_pre_ping=True,
                        )
                        # Validate fallback
                        with engine.connect() as conn:
                            conn.execute(text("SELECT 1"))
                        break
                except Exception:
                    continue
    except Exception:
        pass

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

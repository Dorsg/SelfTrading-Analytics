from __future__ import annotations

import os
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func

from logger_config import setup_logging
from database.db_core import engine
from database.models import HistoricalDailyBar, HistoricalMinuteBar

# Initialize logging using the shared RotatingFileHandler setup
setup_logging()
logger = logging.getLogger("analytics-importer")

# Prefer container path; can still be overridden via env
SQLITE_PATH = os.getenv(
    "ANALYTICS_SQLITE_PATH",
    "/app/tools/finnhub_downloader/data/daily_bars.sqlite",
)


def _yield_daily_rows(cur) -> Iterable[dict]:
    for sym, date_epoch, o, h, l, c, v in cur:
        yield {
            "symbol": str(sym).upper(),
            "date": datetime.fromtimestamp(int(date_epoch), tz=timezone.utc),
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "volume": int(v),
        }


def _yield_minute_rows(cur) -> Iterable[dict]:
    for sym, ts_epoch, interval_min, o, h, l, c, v in cur:
        yield {
            "symbol": str(sym).upper(),
            "ts": datetime.fromtimestamp(int(ts_epoch), tz=timezone.utc),
            "interval_min": int(interval_min),
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "volume": int(v),
        }


def _upsert_daily(pg_conn, rows: list[dict]) -> None:
    if not rows:
        return
    ins = insert(HistoricalDailyBar).values(rows)
    update_cols = {c.name: getattr(ins.excluded, c.name) for c in HistoricalDailyBar.__table__.columns if c.name != "id"}
    pg_conn.execute(ins.on_conflict_do_update(index_elements=["symbol", "date"], set_=update_cols))


def _upsert_minute(pg_conn, rows: list[dict]) -> None:
    if not rows:
        return
    ins = insert(HistoricalMinuteBar).values(rows)
    update_cols = {c.name: getattr(ins.excluded, c.name) for c in HistoricalMinuteBar.__table__.columns if c.name != "id"}
    pg_conn.execute(ins.on_conflict_do_update(index_elements=["symbol", "ts", "interval_min"], set_=update_cols))


def import_sqlite(sqlite_path: str = SQLITE_PATH, batch_size: int = 5000) -> None:
    """
    Idempotent importer:
      • First checks Postgres for existing data; if present, skips without requiring the SQLite file.
      • If DB is empty, requires a readable SQLite file and imports daily + 5m bars.
      • Creates a simple 'import completed' marker to avoid repeat work in the same container.
    """
    # Run light migrations so required tables/columns exist before import
    try:
        from database.init_db import _apply_light_migrations
        _apply_light_migrations()
    except Exception:
        logger.exception("Light migrations failed at importer startup")

    logger.info("=== Starting Analytics Data Import ===")
    logger.info("SQLite source: %s", sqlite_path)

    import_marker = "/app/data/.import_completed"

    # 0) Quick DB check — skip early if data exists
    try:
        with engine.connect() as pg_check:
            daily_ct = int(pg_check.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0)
            minute_ct = int(pg_check.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0)
            if (daily_ct + minute_ct) > 0:
                logger.info(
                    "Existing historical data detected in Postgres (daily=%d, minute=%d) — skipping import.",
                    daily_ct, minute_ct
                )
                # Create/refresh marker for observability
                os.makedirs(os.path.dirname(import_marker), exist_ok=True)
                with open(import_marker, "w") as f:
                    f.write("Import skipped: data already present")
                return
    except Exception as e:
        logger.warning("Pre-check of existing data failed (tables may not exist yet): %s", e)

    # 1) Container-scoped marker (best-effort)
    if os.path.exists(import_marker):
        logger.info("Import marker present at %s — assuming already imported for this container.", import_marker)
        return

    # 2) Only now require the SQLite file (DB was empty)
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"SQLite file not found: {sqlite_path}")

    # 3) Perform import
    logger.info("Connecting to SQLite database...")
    conn = sqlite3.connect(sqlite_path)
    try:
        with engine.begin() as pg:
            # Daily bars
            logger.info("=== Importing Daily Bars ===")
            cur = conn.cursor()
            cur.execute("SELECT symbol, date, open, high, low, close, volume FROM daily_bars ORDER BY symbol, date")
            buf: list[dict] = []
            count = 0
            for row in _yield_daily_rows(cur):
                buf.append(row)
                if len(buf) >= batch_size:
                    _upsert_daily(pg, buf)
                    count += len(buf)
                    buf.clear()
            if buf:
                _upsert_daily(pg, buf)
                count += len(buf)
            logger.info("Daily bars imported: %d", count)

            # Minute bars (5m)
            logger.info("=== Importing Minute Bars (5m) ===")
            cur = conn.cursor()
            cur.execute("SELECT symbol, ts, interval, open, high, low, close, volume FROM minute_bars WHERE interval=5 ORDER BY symbol, ts")
            buf = []
            count = 0
            for row in _yield_minute_rows(cur):
                buf.append(row)
                if len(buf) >= batch_size:
                    _upsert_minute(pg, buf)
                    count += len(buf)
                    buf.clear()
            if buf:
                _upsert_minute(pg, buf)
                count += len(buf)
            logger.info("Minute bars imported: %d", count)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 4) Write marker
    os.makedirs(os.path.dirname(import_marker), exist_ok=True)
    with open(import_marker, "w") as f:
        f.write("Import completed")
    logger.info("=== Historical data import completed ===")

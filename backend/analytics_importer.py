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

# Optional fast bootstrap filters
IMPORT_SYMBOLS = [s.strip().upper() for s in os.getenv("IMPORT_SYMBOLS", "").split(",") if s.strip()]
IMPORT_START_DATE = os.getenv("IMPORT_START_DATE", "")  # YYYY-MM-DD for daily; epoch seconds for minute if numeric
IMPORT_END_DATE = os.getenv("IMPORT_END_DATE", "")
IMPORT_LIMIT_MINUTE_ROWS = int(os.getenv("IMPORT_LIMIT_MINUTE_ROWS", "0") or "0")
IMPORT_LIMIT_DAILY_ROWS = int(os.getenv("IMPORT_LIMIT_DAILY_ROWS", "0") or "0")


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


def _sql_in_list(items: list[str]) -> str:
    return ",".join([f"'{i.replace("'","''")}'" for i in items])


def import_sqlite(sqlite_path: str = SQLITE_PATH, batch_size: int = 5000) -> None:
    """
    Idempotent importer with optional fast-bootstrap filters via env:
      • IMPORT_SYMBOLS=SPY,AAPL
      • IMPORT_START_DATE=2021-03-01  IMPORT_END_DATE=2021-04-01  (daily)
      • IMPORT_LIMIT_MINUTE_ROWS=500000  IMPORT_LIMIT_DAILY_ROWS=100000
    """
    # Run light migrations so required tables/columns exist before import
    try:
        from database.init_db import _apply_light_migrations
        _apply_light_migrations()
    except Exception:
        logger.exception("Light migrations failed at importer startup")

    logger.info("=== Starting Analytics Data Import ===")
    logger.info("SQLite source: %s", sqlite_path)
    if IMPORT_SYMBOLS:
        logger.info("Filter symbols: %s", ",".join(IMPORT_SYMBOLS))
    if IMPORT_START_DATE or IMPORT_END_DATE:
        logger.info("Filter date range: %s -> %s", IMPORT_START_DATE or "-inf", IMPORT_END_DATE or "+inf")
    if IMPORT_LIMIT_DAILY_ROWS or IMPORT_LIMIT_MINUTE_ROWS:
        logger.info("Row limits: daily=%s minute=%s", IMPORT_LIMIT_DAILY_ROWS or "unlimited", IMPORT_LIMIT_MINUTE_ROWS or "unlimited")

    import_marker = "/app/data/.import_completed"

    # 0) Quick DB check — skip early if data exists
    try:
        with engine.connect() as pg_check:
            daily_ct = int(pg_check.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0)
            minute_ct = int(pg_check.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0)
            if (daily_ct + minute_ct) > 0 and not IMPORT_SYMBOLS and not IMPORT_LIMIT_MINUTE_ROWS and not IMPORT_LIMIT_DAILY_ROWS:
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
    if os.path.exists(import_marker) and not (IMPORT_SYMBOLS or IMPORT_LIMIT_MINUTE_ROWS or IMPORT_LIMIT_DAILY_ROWS):
        logger.info("Import marker present at %s — assuming already imported for this container.", import_marker)
        return

    # 2) Only now require the SQLite file (DB was empty or filters requested)
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"SQLite file not found: {sqlite_path}")

    # 3) Perform import
    logger.info("Connecting to SQLite database...")
    # Open read-only to support read-only bind mounts; immutable avoids WAL/SHM creation
    uri = f"file:{sqlite_path}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True)
    try:
        with engine.connect() as pg:
            # Daily bars
            logger.info("=== Importing Daily Bars ===")
            cur = conn.cursor()
            try:
                base = "SELECT symbol, date, open, high, low, close, volume FROM daily_bars"
                where = []
                if IMPORT_SYMBOLS:
                    where.append(f"symbol IN ({_sql_in_list(IMPORT_SYMBOLS)})")
                if IMPORT_START_DATE:
                    where.append(f"date >= strftime('%s','{IMPORT_START_DATE}')")
                if IMPORT_END_DATE:
                    where.append(f"date < strftime('%s','{IMPORT_END_DATE}')")
                sql = base + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY symbol, date"
                if IMPORT_LIMIT_DAILY_ROWS:
                    sql += f" LIMIT {IMPORT_LIMIT_DAILY_ROWS}"
                logger.debug("Daily SQL: %s", sql)
                cur.execute(sql)
            except Exception as e:
                logger.exception("Preparing daily query failed: %s", e)
                raise
            buf: list[dict] = []
            count = 0
            last_log = 0
            for row in _yield_daily_rows(cur):
                buf.append(row)
                if len(buf) >= batch_size:
                    with pg.begin():
                        _upsert_daily(pg, buf)
                    count += len(buf)
                    buf.clear()
                    if count - last_log >= 50000:
                        logger.info("Daily import progress: %d", count)
                        last_log = count
            if buf:
                with pg.begin():
                    _upsert_daily(pg, buf)
                count += len(buf)
            logger.info("Daily bars imported: %d", count)

            # Minute bars (5m)
            logger.info("=== Importing Minute Bars (5m) ===")
            cur = conn.cursor()
            try:
                base = "SELECT symbol, ts, interval, open, high, low, close, volume FROM minute_bars WHERE interval=5"
                where = []
                if IMPORT_SYMBOLS:
                    where.append(f"symbol IN ({_sql_in_list(IMPORT_SYMBOLS)})")
                # For minute bars, IMPORT_START_DATE/END_DATE can be YYYY-MM-DD (convert to epoch) or epoch seconds
                def to_epoch(s: str) -> str:
                    if not s:
                        return ""
                    if s.isdigit():
                        return s
                    try:
                        return str(int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()))
                    except Exception:
                        return s
                if IMPORT_START_DATE:
                    where.append(f"ts >= {to_epoch(IMPORT_START_DATE)}")
                if IMPORT_END_DATE:
                    where.append(f"ts < {to_epoch(IMPORT_END_DATE)}")
                sql = base + (" AND " + " AND ".join(where) if where else "") + " ORDER BY symbol, ts"
                if IMPORT_LIMIT_MINUTE_ROWS:
                    sql += f" LIMIT {IMPORT_LIMIT_MINUTE_ROWS}"
                logger.debug("Minute SQL: %s", sql)
                cur.execute(sql)
            except Exception as e:
                logger.exception("Preparing minute query failed: %s", e)
                raise
            buf = []
            count = 0
            last_log = 0
            for row in _yield_minute_rows(cur):
                buf.append(row)
                if len(buf) >= batch_size:
                    with pg.begin():
                        _upsert_minute(pg, buf)
                    count += len(buf)
                    buf.clear()
                    if count - last_log >= 50000:
                        logger.info("Minute(5m) import progress: %d", count)
                        last_log = count
            if buf:
                with pg.begin():
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

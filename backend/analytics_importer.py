from __future__ import annotations
import os
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func

from database.db_core import engine
from database.models import HistoricalDailyBar, HistoricalMinuteBar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-8s %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/analytics_importer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('analytics_importer')


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


def import_sqlite(sqlite_path: str = SQLITE_PATH, batch_size: int = 5000) -> None:
    logger.info("=== Starting Analytics Data Import ===")
    logger.info(f"SQLite source: {sqlite_path}")
    logger.info(f"Batch size: {batch_size}")
    
    if not os.path.exists(sqlite_path):
        error_msg = f"SQLite file not found: {sqlite_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # Skip if data already present (idempotent one-time import)
    import_marker = "/app/data/.import_completed"
    if os.path.exists(import_marker):
        logger.info("Import already completed (marker file exists). Skipping.")
        return
        
    with engine.connect() as pg:
        try:
            existing = pg.execute(
                "SELECT (SELECT COUNT(1) FROM historical_daily_bars) + (SELECT COUNT(1) FROM historical_minute_bars)"
            ).scalar()
            if existing and int(existing) > 0:
                logger.info(f"Import already completed ({existing} records found). Creating marker.")
                os.makedirs(os.path.dirname(import_marker), exist_ok=True)
                with open(import_marker, 'w') as f:
                    f.write("Import completed successfully")
                return
        except Exception as e:
            # Tables may not exist yet; proceed with import
            logger.warning(f"Could not check existing data (tables may not exist): {e}")

    logger.info("Connecting to SQLite database...")
    conn = sqlite3.connect(sqlite_path)
    try:
        with engine.begin() as pg:
            logger.info("Connected to PostgreSQL. Starting data import...")
            
            # Daily bars
            logger.info("=== Importing Daily Bars ===")
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM daily_bars")
            total_daily = cur.fetchone()[0]
            logger.info(f"Total daily bars to import: {total_daily}")
            
            # Check existing data to resume from last position
            existing_daily = pg.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0
            if existing_daily > 0:
                logger.info(f"Found {existing_daily} existing daily bars - resuming import")
                # Get the last imported record to resume from that point
                last_record = pg.execute(
                    select(HistoricalDailyBar.symbol, HistoricalDailyBar.date)
                    .order_by(HistoricalDailyBar.symbol.desc(), HistoricalDailyBar.date.desc())
                    .limit(1)
                ).first()
                if last_record:
                    last_symbol, last_date = last_record
                    last_date_str = last_date.strftime('%Y-%m-%d')
                    logger.info(f"Resuming from after: {last_symbol} on {last_date_str}")
                    # Query SQLite starting from after the last imported record
                    cur.execute("""
                        SELECT symbol, date, open, high, low, close, volume 
                        FROM daily_bars 
                        WHERE (symbol > ? OR (symbol = ? AND date > ?))
                        ORDER BY symbol, date
                    """, (last_symbol, last_symbol, int(last_date.timestamp())))
                else:
                    cur.execute("SELECT symbol, date, open, high, low, close, volume FROM daily_bars ORDER BY symbol, date")
            else:
                logger.info("Starting fresh daily bars import")
                cur.execute("SELECT symbol, date, open, high, low, close, volume FROM daily_bars ORDER BY symbol, date")
                
            buf: list[dict] = []
            imported_daily = existing_daily
            batch_count = 0
            for row in _yield_daily_rows(cur):
                buf.append(row)
                batch_count += 1
                if len(buf) >= batch_size:
                    _upsert_daily(pg, buf)
                    imported_daily += len(buf)
                    buf.clear()
                    if batch_count % (batch_size * 10) == 0:  # Log every 10 batches
                        logger.info(f"Daily bars progress: {imported_daily}/{total_daily} ({imported_daily/total_daily*100:.1f}%)")
            if buf:
                _upsert_daily(pg, buf)
                imported_daily += len(buf)
            logger.info(f"Daily bars import completed: {imported_daily}/{total_daily}")

            # Minute bars (5m)
            logger.info("=== Importing Minute Bars (5m) ===")
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM minute_bars WHERE interval=5")
            total_minute = cur.fetchone()[0]
            logger.info(f"Total 5-minute bars to import: {total_minute}")
            
            # Check existing minute bars data to resume from last position
            existing_minute = pg.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0
            if existing_minute > 0:
                logger.info(f"Found {existing_minute} existing minute bars - resuming import")
                # Get the last imported minute bar record
                last_minute_record = pg.execute(
                    select(HistoricalMinuteBar.symbol, HistoricalMinuteBar.ts)
                    .order_by(HistoricalMinuteBar.symbol.desc(), HistoricalMinuteBar.ts.desc())
                    .limit(1)
                ).first()
                if last_minute_record:
                    last_symbol, last_ts = last_minute_record
                    last_ts_epoch = int(last_ts.timestamp())
                    logger.info(f"Resuming minute bars from after: {last_symbol} at {last_ts}")
                    # Query SQLite starting from after the last imported record
                    cur.execute("""
                        SELECT symbol, ts, interval, open, high, low, close, volume 
                        FROM minute_bars 
                        WHERE interval=5 AND (symbol > ? OR (symbol = ? AND ts > ?))
                        ORDER BY symbol, ts
                    """, (last_symbol, last_symbol, last_ts_epoch))
                else:
                    cur.execute("SELECT symbol, ts, interval, open, high, low, close, volume FROM minute_bars WHERE interval=5 ORDER BY symbol, ts")
            else:
                logger.info("Starting fresh minute bars import")
                cur.execute("SELECT symbol, ts, interval, open, high, low, close, volume FROM minute_bars WHERE interval=5 ORDER BY symbol, ts")
            
            buf = []
            imported_minute = existing_minute
            batch_count = 0
            for row in _yield_minute_rows(cur):
                buf.append(row)
                batch_count += 1
                if len(buf) >= batch_size:
                    _upsert_minute(pg, buf)
                    imported_minute += len(buf)
                    buf.clear()
                    if batch_count % (batch_size * 10) == 0:  # Log every 10 batches
                        logger.info(f"Minute bars progress: {imported_minute}/{total_minute} ({imported_minute/total_minute*100:.1f}%)")
            if buf:
                _upsert_minute(pg, buf)
                imported_minute += len(buf)
            logger.info(f"Minute bars import completed: {imported_minute}/{total_minute}")

    finally:
        try:
            conn.close()
            logger.info("SQLite connection closed.")
        except Exception as e:
            logger.warning(f"Error closing SQLite connection: {e}")
    
    # Create completion marker
    logger.info("Creating completion marker...")
    import_marker = "/app/data/.import_completed"
    os.makedirs(os.path.dirname(import_marker), exist_ok=True)
    with open(import_marker, 'w') as f:
        f.write("Import completed successfully")
    logger.info("=== Historical data import completed successfully ===")
    logger.info(f"Completion marker created: {import_marker}")


def _upsert_daily(pg_conn, rows: list[dict]) -> None:
    if not rows:
        return
    ins = insert(HistoricalDailyBar).values(rows)
    update_cols = {c.name: getattr(ins.excluded, c.name) for c in HistoricalDailyBar.__table__.columns if c.name not in ("id",)}
    pg_conn.execute(ins.on_conflict_do_update(index_elements=["symbol", "date"], set_=update_cols))


def _upsert_minute(pg_conn, rows: list[dict]) -> None:
    if not rows:
        return
    ins = insert(HistoricalMinuteBar).values(rows)
    update_cols = {c.name: getattr(ins.excluded, c.name) for c in HistoricalMinuteBar.__table__.columns if c.name not in ("id",)}
    pg_conn.execute(
        ins.on_conflict_do_update(index_elements=["symbol", "ts", "interval_min"], set_=update_cols)
    )


if __name__ == "__main__":
    try:
        import_sqlite()
    except Exception as e:
        logger.error(f"Import failed with error: {e}", exc_info=True)
        raise



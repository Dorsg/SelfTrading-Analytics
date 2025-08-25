from __future__ import annotations
import os
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy.dialects.postgresql import insert

from database.db_core import engine
from database.models import HistoricalDailyBar, HistoricalMinuteBar


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
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"SQLite file not found: {sqlite_path}")

    # Skip if data already present (idempotent one-time import)
    with engine.connect() as pg:
        try:
            existing = pg.execute(
                "SELECT (SELECT COUNT(1) FROM historical_daily_bars) + (SELECT COUNT(1) FROM historical_minute_bars)"
            ).scalar()
            if existing and int(existing) > 0:
                return
        except Exception:
            # Tables may not exist yet; proceed with import
            pass

    conn = sqlite3.connect(sqlite_path)
    try:
        with engine.begin() as pg:
            # Daily bars
            cur = conn.cursor()
            cur.execute("SELECT symbol, date, open, high, low, close, volume FROM daily_bars ORDER BY symbol, date")
            buf: list[dict] = []
            for row in _yield_daily_rows(cur):
                buf.append(row)
                if len(buf) >= batch_size:
                    _upsert_daily(pg, buf)
                    buf.clear()
            if buf:
                _upsert_daily(pg, buf)

            # Minute bars (5m)
            cur = conn.cursor()
            cur.execute("SELECT symbol, ts, interval, open, high, low, close, volume FROM minute_bars WHERE interval=5 ORDER BY symbol, ts")
            buf = []
            for row in _yield_minute_rows(cur):
                buf.append(row)
                if len(buf) >= batch_size:
                    _upsert_minute(pg, buf)
                    buf.clear()
            if buf:
                _upsert_minute(pg, buf)

    finally:
        try:
            conn.close()
        except Exception:
            pass


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
    import_sqlite()



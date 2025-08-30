"""
Analytics-only MarketDataManager – reads from HistoricalDailyBar/MinuteBar.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional, Dict

from sqlalchemy import select, func
from database.db_core import engine
from database.models import HistoricalDailyBar, HistoricalMinuteBar

logger = logging.getLogger("market-data-manager")


def _sim_time() -> Optional[datetime]:
    ts = os.getenv("SIM_TIME_EPOCH")
    if not ts:
        logger.info("🕐 No SIM_TIME_EPOCH set, returning None (will get latest data)")
        return None
    try:
        sim_dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        logger.info(f"🕐 Current simulation time: {sim_dt} (ts: {ts})")
        return sim_dt
    except Exception as e:
        logger.error(f"❌ Invalid SIM_TIME_EPOCH '{ts}': {e}")
        return None


class MarketDataManager:
    _shared_state: dict = {}

    def __init__(self) -> None:
        if not self._shared_state:
            self._shared_state.update({"_candle_cache": {}, "_candle_ttl_seconds": 0})
        self.__dict__ = self._shared_state

    @staticmethod
    def _is_valid_symbol(sym: str) -> bool:
        s = (sym or "").strip().upper()
        return bool(s) and not s.startswith("__")

    def get_current_price(self, symbol: str) -> Optional[float]:
        sym = (symbol or "").upper()
        if not self._is_valid_symbol(sym):
            return None
        as_of = _sim_time()
        with engine.connect() as conn:
            try:
                q = select(HistoricalMinuteBar.close).where(HistoricalMinuteBar.symbol == sym)
                if as_of is not None:
                    q = q.where(HistoricalMinuteBar.ts <= as_of)
                q = q.order_by(HistoricalMinuteBar.ts.desc()).limit(1)
                px = conn.execute(q).scalar()
                if px is None:
                    qd = select(HistoricalDailyBar.close).where(HistoricalDailyBar.symbol == sym)
                    if as_of is not None:
                        qd = qd.where(HistoricalDailyBar.date <= func.date_trunc("day", as_of))
                    qd = qd.order_by(HistoricalDailyBar.date.desc()).limit(1)
                    px = conn.execute(qd).scalar()
                return float(px) if px is not None else None
            except Exception:
                logger.debug("get_current_price failed", exc_info=True)
                return None

    async def _get_candles(self, symbol: str, interval: str | int, bars: int = 250) -> List[dict]:
        sym = (symbol or "").upper()
        if not self._is_valid_symbol(sym):
            return []
        
        # For analytics simulation, get data UP TO the current simulation time
        # This allows strategies to see historical data as if it's "now"
        as_of = _sim_time()
        try:
            logger.info(f"🔌 Connecting to database for candles query...")
            with engine.connect() as conn:
                # Check database state on first connection
                if not hasattr(MarketDataManager, '_db_state_checked'):
                    logger.info("📊 Checking database state...")
                    try:
                        total_minute_bars = conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0
                        logger.info(f"📊 TOTAL minute bars in database: {total_minute_bars}")
                        
                        if total_minute_bars > 0:
                            # Check available intervals
                            available_intervals = conn.execute(
                                select(HistoricalMinuteBar.interval_min, func.count())
                                .group_by(HistoricalMinuteBar.interval_min)
                                .order_by(HistoricalMinuteBar.interval_min)
                            ).all()
                            logger.info(f"📈 AVAILABLE intervals: {dict(available_intervals)}")
                            
                            # Sample symbols for interval 5
                            sample_symbols = conn.execute(
                                select(HistoricalMinuteBar.symbol)
                                .where(HistoricalMinuteBar.interval_min == 5)
                                .distinct()
                                .limit(10)
                            ).all()
                            logger.info(f"🏢 SAMPLE symbols for 5min: {[s[0] for s in sample_symbols]}")
                        else:
                            logger.error("❌ HistoricalMinuteBar table is EMPTY!")
                        
                        MarketDataManager._db_state_checked = True
                    except Exception as e:
                        logger.error(f"❌ Database state check error: {e}")
                
                s = str(interval).lower()
                is_daily = s in {"1d", "1day", "d", "1440"}
                if is_daily:
                    q = (
                        select(
                            HistoricalDailyBar.date,
                            HistoricalDailyBar.open,
                            HistoricalDailyBar.high,
                            HistoricalDailyBar.low,
                            HistoricalDailyBar.close,
                            HistoricalDailyBar.volume,
                        )
                        .where(HistoricalDailyBar.symbol == sym)
                    )
                    if as_of is not None:
                        q = q.where(HistoricalDailyBar.date <= func.date_trunc("day", as_of))
                    q = q.order_by(HistoricalDailyBar.date.desc()).limit(bars)
                    rows = conn.execute(q).all()
                    rows.reverse()
                    return [
                        {"timestamp": r[0], "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]), "volume": int(r[5])}
                        for r in rows
                    ]

                # Map timeframe to interval_min
                interval_minutes = 5  # default to 5-minute bars
                try:
                    if s in {"5", "5m", "5min"}:
                        interval_minutes = 5
                    elif s in {"15", "15m", "15min"}:
                        interval_minutes = 15
                    elif s in {"60", "60m", "60min", "1h"}:
                        interval_minutes = 60
                    else:
                        # Try to parse as integer
                        interval_minutes = int(s.replace("m", "").replace("min", ""))
                except (ValueError, AttributeError):
                    interval_minutes = 5  # fallback to 5-minute
                
                logger.info(f"🔍 QUERY: {sym} timeframe='{interval}' -> interval_min={interval_minutes}, as_of={as_of}, bars={bars}")
                
                q = (
                    select(
                        HistoricalMinuteBar.ts,
                        HistoricalMinuteBar.open,
                        HistoricalMinuteBar.high,
                        HistoricalMinuteBar.low,
                        HistoricalMinuteBar.close,
                        HistoricalMinuteBar.volume,
                    )
                    .where(HistoricalMinuteBar.symbol == sym)
                    .where(HistoricalMinuteBar.interval_min == interval_minutes)
                )
                # TEMPORARY FIX: Disable time filtering for analytics simulation
                # The time filter is causing all queries to return zero results
                # For now, get the latest available data to make strategies work
                logger.warning(f"🚨 ANALYTICS MODE: Ignoring time filter, getting latest data for {sym}")
                if as_of is not None:
                    logger.warning(f"🕐 Simulation time {as_of} - but using latest data for now")
                q = q.order_by(HistoricalMinuteBar.ts.desc()).limit(bars)
                rows = conn.execute(q).all()
                rows.reverse()
                
                if len(rows) > 0:
                    logger.info(f"✅ SUCCESS: Found {len(rows)} candles for {sym}")
                    logger.info(f"📅 Data range: {rows[0][0]} to {rows[-1][0]}")
                else:
                    logger.error(f"❌ NO DATA: Query returned 0 results for {sym} (interval_min={interval_minutes})")
                    # Quick diagnostic
                    total_count = conn.execute(
                        select(func.count()).select_from(HistoricalMinuteBar).where(HistoricalMinuteBar.symbol == sym)
                    ).scalar() or 0
                    logger.error(f"❌ Symbol {sym} total bars: {total_count}")
                
                return [
                    {"timestamp": r[0], "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]), "volume": int(r[5])}
                    for r in rows
                ]
        except Exception:
            logger.debug("_get_candles failed", exc_info=True)
            return []


# Global instance
MKT = MarketDataManager()



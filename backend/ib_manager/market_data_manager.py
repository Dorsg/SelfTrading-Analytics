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
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
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
        as_of = _sim_time()
        try:
            with engine.connect() as conn:
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
                )
                if as_of is not None:
                    q = q.where(HistoricalMinuteBar.ts <= as_of)
                q = q.order_by(HistoricalMinuteBar.ts.desc()).limit(bars)
                rows = conn.execute(q).all()
                rows.reverse()
                return [
                    {"timestamp": r[0], "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4]), "volume": int(r[5])}
                    for r in rows
                ]
        except Exception:
            logger.debug("_get_candles failed", exc_info=True)
            return []


# Global instance
MKT = MarketDataManager()



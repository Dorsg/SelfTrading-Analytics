from __future__ import annotations

import os
import logging
from datetime import datetime, timezone, timedelta, time, date
from typing import List, Dict, Any, Tuple, Optional, Iterable

from sqlalchemy import select, func

from database.db_core import engine
from database.models import HistoricalMinuteBar, HistoricalDailyBar

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo  # type: ignore
    _NY = ZoneInfo("America/New_York")
except Exception:
    _NY = None  # Fallback handled below

log = logging.getLogger("market-data-manager")


def _ensure_utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _et_bounds_for_date(et_day: date) -> Tuple[datetime, datetime]:
    """
    Return NY session bounds [open, close] as UTC datetimes for a given ET calendar date.
    """
    if _NY is None:
        open_utc = datetime(et_day.year, et_day.month, et_day.day, 9, 30, tzinfo=timezone.utc)
        close_utc = datetime(et_day.year, et_day.month, et_day.day, 16, 0, tzinfo=timezone.utc)
        return open_utc, close_utc

    open_et = datetime(et_day.year, et_day.month, et_day.day, 9, 30, tzinfo=_NY)
    close_et = datetime(et_day.year, et_day.month, et_day.day, 16, 0, tzinfo=_NY)
    return open_et.astimezone(timezone.utc), close_et.astimezone(timezone.utc)


def _is_weekday(et_dt: datetime) -> bool:
    return et_dt.weekday() < 5  # Mon-Fri


def _is_regular_market_minute(ts_utc: datetime) -> bool:
    """
    True iff ts_utc lies inside a regular-hours NYSE minute (Mon-Fri, 09:30..16:00 ET).
    Holidays are not detected here; we filter those by actual data presence elsewhere.
    """
    ts_utc = _ensure_utc(ts_utc)
    if _NY is None:
        t = ts_utc.time()
        return ts_utc.weekday() < 5 and time(13, 30) <= t <= time(20, 0)

    et = ts_utc.astimezone(_NY)
    if not _is_weekday(et):
        return False
    t = et.time()
    return time(9, 30) <= t <= time(16, 0)


def _detect_and_log_gaps(candles: List[Dict[str, Any]], symbol: str, interval_min: int, gap_threshold_pct: float = 2.0):
    """Helper to find and log significant price gaps between consecutive candles."""
    if len(candles) < 2:
        return
    
    for i in range(1, len(candles)):
        prev_candle = candles[i-1]
        curr_candle = candles[i]
        
        prev_close = float(prev_candle["close"])
        curr_open = float(curr_candle["open"])
        
        if prev_close <= 0:
            continue
            
        gap_pct = abs(curr_open / prev_close - 1.0) * 100.0
        
        if gap_pct > gap_threshold_pct:
            log.info(
                "Price gap detected for %s (%dm): %.2f%% gap from %.2f to %.2f between %s and %s",
                symbol,
                interval_min,
                gap_pct,
                prev_close,
                curr_open,
                prev_candle["ts"].isoformat(),
                curr_candle["ts"].isoformat(),
            )


class MarketDataManager:
    """
    Historical data access + helpers.
    • RTH filtering for intraday.
    • Session-aware 'next tick' that respects holidays/DST using actual bars.
    • Reference clock selection & global fallback.
    """

    def __init__(self) -> None:
        self._last_session: Tuple[Optional[datetime], str] = (None, "regular-hours")
        self._clock_symbol = os.getenv("SIM_REFERENCE_CLOCK_SYMBOL", "SPY").upper()

    # ─────────────────────────── coverage primitives ───────────────────────────

    def has_minute_bars(self, symbol: str, interval_min: int) -> bool:
        symbol = (symbol or "").upper()
        with engine.connect() as conn:
            ts = conn.execute(
                select(func.min(HistoricalMinuteBar.ts))
                .where(HistoricalMinuteBar.symbol == symbol)
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
            ).scalar()
            return ts is not None

    def has_daily_bars(self, symbol: str) -> bool:
        symbol = (symbol or "").upper()
        with engine.connect() as conn:
            dt = conn.execute(
                select(func.min(HistoricalDailyBar.date))
                .where(HistoricalDailyBar.symbol == symbol)
            ).scalar()
            return dt is not None

    def get_earliest_bar(self, symbol: str, interval_min: int) -> Optional[datetime]:
        """
        Earliest available bar timestamp for (symbol, interval).
        Daily uses the date; minute uses ts at interval_min.
        """
        s = (symbol or "").upper()
        with engine.connect() as conn:
            if int(interval_min) >= 1440:
                dt = conn.execute(
                    select(func.min(HistoricalDailyBar.date))
                    .where(HistoricalDailyBar.symbol == s)
                ).scalar()
                if dt is None:
                    return None
                return dt if getattr(dt, "tzinfo", None) else dt.replace(tzinfo=timezone.utc)

            ts = conn.execute(
                select(func.min(HistoricalMinuteBar.ts))
                .where(HistoricalMinuteBar.symbol == s)
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
            ).scalar()
            if ts is None:
                return None
            return ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)

    def pick_reference_symbol(
        self,
        interval_min: int = 5,
        prefer: Optional[Iterable[str]] = None,
    ) -> Optional[str]:
        prefer_list = [s.strip().upper() for s in (prefer or
                        os.getenv("SIM_REFERENCE_CANDIDATES", "SPY,QQQ,AAPL,MSFT,TSLA,AMD,NVDA,GOOGL,AMZN").split(","))]
        for s in prefer_list:
            try:
                if s and self.has_minute_bars(s, interval_min):
                    return s
            except Exception:
                continue

        with engine.connect() as conn:
            rows = conn.execute(
                select(HistoricalMinuteBar.symbol, func.count().label("n"))
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
                .group_by(HistoricalMinuteBar.symbol)
                .order_by(func.count().desc())
                .limit(1)
            ).all()
            if rows:
                return rows[0]._mapping["symbol"]
        return None

    # ─────────────────────────── data access (single symbol) ───────────────────────────

    def get_candles_until(
        self,
        symbol: str,
        interval_min: int,
        as_of: datetime,
        lookback: int = 250,
        *,
        regular_hours_only: bool = True,
    ) -> List[Dict[str, Any]]:
        symbol = symbol.upper()
        as_of = _ensure_utc(as_of)

        with engine.connect() as conn:
            if int(interval_min) == 1440:
                stmt = (
                    select(
                        HistoricalDailyBar.date,
                        HistoricalDailyBar.open,
                        HistoricalDailyBar.high,
                        HistoricalDailyBar.low,
                        HistoricalDailyBar.close,
                        HistoricalDailyBar.volume,
                    )
                    .where(HistoricalDailyBar.symbol == symbol)
                    .where(HistoricalDailyBar.date <= as_of)
                    .order_by(HistoricalDailyBar.date.desc())
                    .limit(lookback)
                )
                rows = conn.execute(stmt).all()
                rows.reverse()
                out: List[Dict[str, Any]] = []
                for row in rows:
                    m = row._mapping
                    out.append(
                        {
                            "ts": m["date"],
                            "open": m["open"],
                            "high": m["high"],
                            "low": m["low"],
                            "close": m["close"],
                            "volume": m["volume"],
                        }
                    )
                _detect_and_log_gaps(out, symbol, interval_min)
                return out

            raw_limit = lookback * (3 if regular_hours_only else 1)
            stmt = (
                select(
                    HistoricalMinuteBar.ts,
                    HistoricalMinuteBar.open,
                    HistoricalMinuteBar.high,
                    HistoricalMinuteBar.low,
                    HistoricalMinuteBar.close,
                    HistoricalMinuteBar.volume,
                )
                .where(HistoricalMinuteBar.symbol == symbol)
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
                .where(HistoricalMinuteBar.ts <= as_of)
                .order_by(HistoricalMinuteBar.ts.desc())
                .limit(raw_limit)
            )
            rows = conn.execute(stmt).all()
            rows.reverse()

            out: List[Dict[str, Any]] = []
            for row in rows:
                m = row._mapping
                ts = m["ts"]
                ts = ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)
                if (not regular_hours_only) or _is_regular_market_minute(ts):
                    out.append(
                        {
                            "ts": ts,
                            "open": m["open"],
                            "high": m["high"],
                            "low": m["low"],
                            "close": m["close"],
                            "volume": m["volume"],
                        }
                    )
            if regular_hours_only and len(out) > lookback:
                out = out[-lookback:]
            _detect_and_log_gaps(out, symbol, interval_min)
            return out

    # ─────────────────────────── data access (BULK) ───────────────────────────

    def get_candles_bulk_until(
        self,
        symbols: List[str],
        interval_min: int,
        as_of: datetime,
        lookback: int = 250,
        *,
        regular_hours_only: bool = True,
    ) -> Dict[str, List[Dict[str, Any]]]:
        if not symbols:
            return {}
        syms = [s.upper() for s in symbols]
        as_of = _ensure_utc(as_of)

        out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in syms}

        with engine.connect() as conn:
            if int(interval_min) == 1440:
                rn = func.row_number().over(
                    partition_by=HistoricalDailyBar.symbol,
                    order_by=HistoricalDailyBar.date.desc(),
                ).label("rn")
                base = (
                    select(
                        HistoricalDailyBar.symbol.label("symbol"),
                        HistoricalDailyBar.date.label("ts"),
                        HistoricalDailyBar.open.label("open"),
                        HistoricalDailyBar.high.label("high"),
                        HistoricalDailyBar.low.label("low"),
                        HistoricalDailyBar.close.label("close"),
                        HistoricalDailyBar.volume.label("volume"),
                        rn,
                    )
                    .where(HistoricalDailyBar.symbol.in_(syms))
                    .where(HistoricalDailyBar.date <= as_of)
                ).subquery("d")
                stmt = (
                    select(
                        base.c.symbol,
                        base.c.ts,
                        base.c.open,
                        base.c.high,
                        base.c.low,
                        base.c.close,
                        base.c.volume,
                    )
                    .where(base.c.rn <= int(lookback))
                    .order_by(base.c.symbol.asc(), base.c.ts.asc())
                )
                rows = conn.execute(stmt).all()
                for row in rows:
                    m = row._mapping
                    out[m["symbol"]].append(
                        {
                            "ts": m["ts"],
                            "open": m["open"],
                            "high": m["high"],
                            "low": m["low"],
                            "close": m["close"],
                            "volume": m["volume"],
                        }
                    )
            else:
                rn = func.row_number().over(
                    partition_by=HistoricalMinuteBar.symbol,
                    order_by=HistoricalMinuteBar.ts.desc(),
                ).label("rn")
                raw_limit = lookback * (3 if regular_hours_only else 1)
                base = (
                    select(
                        HistoricalMinuteBar.symbol.label("symbol"),
                        HistoricalMinuteBar.ts.label("ts"),
                        HistoricalMinuteBar.open.label("open"),
                        HistoricalMinuteBar.high.label("high"),
                        HistoricalMinuteBar.low.label("low"),
                        HistoricalMinuteBar.close.label("close"),
                        HistoricalMinuteBar.volume.label("volume"),
                        rn,
                    )
                    .where(HistoricalMinuteBar.symbol.in_(syms))
                    .where(HistoricalMinuteBar.interval_min == int(interval_min))
                    .where(HistoricalMinuteBar.ts <= as_of)
                ).subquery("m")
                stmt = (
                    select(
                        base.c.symbol,
                        base.c.ts,
                        base.c.open,
                        base.c.high,
                        base.c.low,
                        base.c.close,
                        base.c.volume,
                    )
                    .where(base.c.rn <= int(raw_limit))
                    .order_by(base.c.symbol.asc(), base.c.ts.asc())
                )
                rows = conn.execute(stmt).all()
                for row in rows:
                    m = row._mapping
                    ts = m["ts"]
                    ts = ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)
                    if (not regular_hours_only) or _is_regular_market_minute(ts):
                        out[m["symbol"]].append(
                            {
                                "ts": ts,
                                "open": m["open"],
                                "high": m["high"],
                                "low": m["low"],
                                "close": m["close"],
                                "volume": m["volume"],
                            }
                        )

                if regular_hours_only:
                    for s in syms:
                        if len(out[s]) > lookback:
                            out[s] = out[s][-lookback:]

        return out

    # ─────────────────────────── session-aware tick helpers ───────────────────────────

    def get_next_session_ts(
        self,
        as_of: datetime,
        interval_min: int = 5,
        *,
        reference_symbol: Optional[str] = None,
    ) -> Optional[datetime]:
        as_of = _ensure_utc(as_of)
        clock_sym = ((reference_symbol or self._clock_symbol or "SPY") or "").upper()

        start_et = as_of.astimezone(_NY) if _NY else as_of
        day = start_et.date()
        epsilon = timedelta(seconds=1)

        with engine.connect() as conn:
            for _ in range(400):
                open_utc, close_utc = _et_bounds_for_date(day)

                if as_of >= close_utc:
                    day = (datetime.combine(day, time(0, 0)) + timedelta(days=1)).date()
                    continue

                search_from = max(as_of + epsilon, open_utc)
                if search_from > close_utc:
                    day = (datetime.combine(day, time(0, 0)) + timedelta(days=1)).date()
                    continue

                next_ts = None

                if clock_sym:
                    next_ts = conn.execute(
                        select(func.min(HistoricalMinuteBar.ts))
                        .where(HistoricalMinuteBar.symbol == clock_sym)
                        .where(HistoricalMinuteBar.interval_min == int(interval_min))
                        .where(HistoricalMinuteBar.ts >= search_from)
                        .where(HistoricalMinuteBar.ts <= close_utc)
                    ).scalar()

                if next_ts is None:
                    next_ts = conn.execute(
                        select(func.min(HistoricalMinuteBar.ts))
                        .where(HistoricalMinuteBar.interval_min == int(interval_min))
                        .where(HistoricalMinuteBar.ts >= search_from)
                        .where(HistoricalMinuteBar.ts <= close_utc)
                    ).scalar()
                    if next_ts:
                        log.debug(
                            "next_session_ts: using GLOBAL fallback at %s (tf=%dm) because clock '%s' had no bar.",
                            next_ts, interval_min, (clock_sym or "<none>")
                        )

                if next_ts:
                    ts = next_ts if getattr(next_ts, "tzinfo", None) else next_ts.replace(tzinfo=timezone.utc)
                    log.debug(
                        "next_session_ts: as_of=%s -> %s (clock=%s, tf=%dm)",
                        as_of.isoformat(),
                        ts.isoformat(),
                        (clock_sym or "<auto>"),
                        interval_min,
                    )
                    return ts

                day = (datetime.combine(day, time(0, 0)) + timedelta(days=1)).date()

        log.warning(
            "next_session_ts: No further bars found after %s (clock=%s, tf=%dm)",
            as_of.isoformat(),
            (clock_sym or "<auto>"),
            interval_min,
        )
        return None

    def get_next_session_ts_global(self, as_of: datetime, interval_min: int = 5) -> Optional[datetime]:
        return self.get_next_session_ts(as_of, interval_min=interval_min, reference_symbol=None)

    # ─────────────────────────── indicators ───────────────────────────

    @staticmethod
    def calculate_sma(candles: List[Dict[str, Any]], period: int) -> float:
        if len(candles) < period:
            return float("nan")
        closes = [float(c["close"]) for c in candles[-period:]]
        return sum(closes) / len(closes)

    @staticmethod
    def calculate_ema(candles: List[Dict[str, Any]], period: int) -> float:
        if len(candles) < period:
            return float("nan")
        
        closes = [float(c["close"]) for c in candles]
        
        # Initialize EMA with the SMA of the first 'period' values
        sma = sum(closes[-period:]) / period
        
        # The first EMA value is the SMA
        if len(candles) == period:
            return sma
        
        # Start with the SMA and apply EMA formula for the rest
        k = 2.0 / (period + 1.0)
        ema = sma
        
        # This implementation is for a rolling EMA calculation.
        # However, the current structure recalculates on each call.
        # For a more accurate EMA, we should calculate it iteratively over the series.
        # Let's re-calculate from the start of the provided candle window for better accuracy.
        
        window_closes = closes[-period:]
        ema = sum(window_closes) / period # Initial SMA for the window
        
        # The candles are assumed to be sorted chronologically
        # This method is repeatedly called with a sliding window of candles.
        # A simple iterative EMA calculation:
        
        ema_values = []
        # Initial SMA
        initial_sma = sum(c["close"] for c in candles[:period]) / period
        ema_values.append(initial_sma)

        for i in range(period, len(candles)):
            close = float(candles[i]["close"])
            new_ema = (close - ema_values[-1]) * k + ema_values[-1]
            ema_values.append(new_ema)
            
        return ema_values[-1] if ema_values else float("nan")

    @staticmethod
    def calculate_rsi(candles: List[Dict[str, Any]], period: int = 14) -> float:
        if len(candles) < period + 1:
            return float("nan")
        gains = 0.0
        losses = 0.0
        closes = [float(c["close"]) for c in candles[-(period + 1):]]
        for i in range(1, len(closes)):
            delta = closes[i] - closes[i - 1]
            if delta >= 0:
                gains += delta
            else:
                losses -= delta
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def calculate_atr(candles: List[Dict[str, Any]], period: int = 14) -> float:
        if len(candles) < period + 1:
            return float("nan")
        trs = []
        for i in range(1, period + 1):
            h = float(candles[-i]["high"])
            l = float(candles[-i]["low"])
            pc = float(candles[-i - 1]["close"])
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return sum(trs) / len(trs)

    @staticmethod
    def average_volume(candles: List[Dict[str, Any]], period: int) -> float:
        if len(candles) < period:
            return 0.0
        vols = [float(c["volume"]) for c in candles[-period:]]
        return sum(vols) / float(period)

    @staticmethod
    def donchian_channel(
        candles: List[Dict[str, Any]], lookback: int
    ) -> Tuple[Optional[float], Optional[float]]:
        if len(candles) < lookback:
            return (None, None)
        window = candles[-lookback:]
        highs = [float(c["high"]) for c in window]
        lows = [float(c["low"]) for c in window]
        return (max(highs), min(lows))

    @staticmethod
    def calculate_macd(
        candles: List[Dict[str, Any]], 
        fast_period: int = 12, 
        slow_period: int = 26, 
        signal_period: int = 9
    ) -> Tuple[Optional[float], Optional[float]]:
        """Calculate MACD line and signal line."""
        if len(candles) < slow_period + signal_period:
            return (None, None)
        
        # Calculate fast and slow EMAs
        fast_k = 2.0 / (fast_period + 1.0)
        slow_k = 2.0 / (slow_period + 1.0)
        
        # Initialize fast EMA
        fast_ema = float(candles[-slow_period]["close"])
        for c in candles[-slow_period + 1:]:
            fast_ema = c["close"] * fast_k + fast_ema * (1.0 - fast_k)
        
        # Initialize slow EMA
        slow_ema = float(candles[-slow_period]["close"])
        for c in candles[-slow_period + 1:]:
            slow_ema = c["close"] * slow_k + slow_ema * (1.0 - slow_k)
        
        # MACD line = fast EMA - slow EMA
        macd_line = fast_ema - slow_ema
        
        # Calculate signal line (EMA of MACD)
        # We need at least signal_period of MACD values
        if len(candles) < slow_period + signal_period:
            return (macd_line, None)
        
        # Calculate MACD values for signal period
        macd_values = []
        for i in range(signal_period):
            idx = -(signal_period - i)
            if abs(idx) <= len(candles):
                subset = candles[:idx] if idx < 0 else candles
                if len(subset) >= slow_period:
                    f_ema = float(subset[-slow_period]["close"])
                    s_ema = float(subset[-slow_period]["close"])
                    for c in subset[-slow_period + 1:]:
                        f_ema = c["close"] * fast_k + f_ema * (1.0 - fast_k)
                        s_ema = c["close"] * slow_k + s_ema * (1.0 - slow_k)
                    macd_values.append(f_ema - s_ema)
        
        if len(macd_values) < signal_period:
            return (macd_line, None)
        
        signal_k = 2.0 / (signal_period + 1.0)
        signal_line = macd_values[0]
        for mv in macd_values[1:]:
            signal_line = mv * signal_k + signal_line * (1.0 - signal_k)
        
        return (macd_line, signal_line)

    @staticmethod
    def calculate_stochastic(
        candles: List[Dict[str, Any]], 
        k_period: int = 14, 
        d_period: int = 3
    ) -> Tuple[Optional[float], Optional[float]]:
        """Calculate Stochastic Oscillator %K and %D."""
        if len(candles) < k_period + d_period:
            return (None, None)
        
        # Calculate %K
        window = candles[-k_period:]
        highs = [float(c["high"]) for c in window]
        lows = [float(c["low"]) for c in window]
        current_close = float(candles[-1]["close"])
        
        highest_high = max(highs)
        lowest_low = min(lows)
        
        if highest_high == lowest_low:
            stoch_k = 50.0
        else:
            stoch_k = 100.0 * (current_close - lowest_low) / (highest_high - lowest_low)
        
        # Calculate %D (SMA of %K)
        k_values = []
        for i in range(d_period):
            idx = -(d_period - i)
            subset = candles[:idx] if idx < 0 else candles
            if len(subset) >= k_period:
                w = subset[-k_period:]
                h = [float(c["high"]) for c in w]
                l = [float(c["low"]) for c in w]
                cc = float(subset[-1]["close"])
                hh = max(h)
                ll = min(l)
                if hh == ll:
                    k_values.append(50.0)
                else:
                    k_values.append(100.0 * (cc - ll) / (hh - ll))
        
        stoch_d = sum(k_values) / len(k_values) if k_values else None
        
        return (stoch_k, stoch_d)

    @staticmethod
    def calculate_bollinger_bands(
        candles: List[Dict[str, Any]], 
        period: int = 20, 
        std_dev: float = 2.0
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Calculate Bollinger Bands (upper, middle, lower)."""
        if len(candles) < period:
            return (None, None, None)
        
        closes = [float(c["close"]) for c in candles[-period:]]
        middle = sum(closes) / len(closes)
        
        # Calculate standard deviation
        variance = sum((x - middle) ** 2 for x in closes) / len(closes)
        std = variance ** 0.5
        
        upper = middle + (std_dev * std)
        lower = middle - (std_dev * std)
        
        return (upper, middle, lower)

    # ─────────────────────────── advanced helpers (strategy) ───────────────────────────

    @staticmethod
    def atr_percent(candles: List[Dict[str, Any]], period: int = 14) -> float:
        """
        ATR as percent of current close. Returns NaN when insufficient data.
        """
        if not candles:
            return float("nan")
        atr = MarketDataManager.calculate_atr(candles, period)
        px = float(candles[-1]["close"]) if candles else float("nan")
        if px <= 0:
            return float("nan")
        return (atr / px) * 100.0

    @staticmethod
    def realized_volatility_pct(candles: List[Dict[str, Any]], period: int = 20) -> float:
        """
        Simple close-to-close realized volatility (not annualized) as percent.
        Computes stddev of simple returns over the window, expressed in percent.
        """
        if len(candles) < period + 1:
            return float("nan")
        closes = [float(c["close"]) for c in candles[-(period + 1):]]
        rets: List[float] = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            cur = closes[i]
            if prev <= 0:
                continue
            rets.append((cur - prev) / prev)
        if not rets:
            return float("nan")
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        std = var ** 0.5
        return std * 100.0

    @staticmethod
    def relative_strength(
        subject_candles: List[Dict[str, Any]],
        benchmark_candles: List[Dict[str, Any]],
        period: int = 60,
    ) -> Optional[float]:
        """
        Simple RS = (subject return) - (benchmark return) over window, in percent.
        Positive means subject outperformed benchmark.
        """
        if len(subject_candles) < period + 1 or len(benchmark_candles) < period + 1:
            return None
        s0 = float(subject_candles[-(period + 1)]["close"])
        s1 = float(subject_candles[-1]["close"])
        b0 = float(benchmark_candles[-(period + 1)]["close"])
        b1 = float(benchmark_candles[-1]["close"])
        if s0 <= 0 or b0 <= 0:
            return None
        subj_ret = (s1 - s0) / s0
        bench_ret = (b1 - b0) / b0
        return (subj_ret - bench_ret) * 100.0

    # ─────────────────────────── mark-to-market helpers ───────────────────────────

    def get_last_close_for_symbols(
        self,
        symbols: List[str],
        minutes: int,
        as_of: datetime,
        *,
        regular_hours_only: bool = True,
    ) -> Dict[str, float]:
        as_of = _ensure_utc(as_of)
        if not symbols:
            return {}
        syms = [s.upper() for s in symbols]

        out: Dict[str, float] = {}

        with engine.connect() as conn:
            if int(minutes) >= 1440:
                rn = func.row_number().over(
                    partition_by=HistoricalDailyBar.symbol,
                    order_by=HistoricalDailyBar.date.desc(),
                ).label("rn")
                base = (
                    select(
                        HistoricalDailyBar.symbol.label("symbol"),
                        HistoricalDailyBar.date.label("ts"),
                        HistoricalDailyBar.close.label("close"),
                        rn,
                    )
                    .where(HistoricalDailyBar.symbol.in_(syms))
                    .where(HistoricalDailyBar.date <= as_of)
                ).subquery("d_last")
                stmt = select(base.c.symbol, base.c.close).where(base.c.rn == 1)
                for row in conn.execute(stmt).all():
                    m = row._mapping
                    try:
                        out[m["symbol"]] = float(m["close"])
                    except Exception:
                        continue
                return out

            rn = func.row_number().over(
                partition_by=HistoricalMinuteBar.symbol,
                order_by=HistoricalMinuteBar.ts.desc(),
            ).label("rn")

            base = (
                select(
                    HistoricalMinuteBar.symbol.label("symbol"),
                    HistoricalMinuteBar.ts.label("ts"),
                    HistoricalMinuteBar.close.label("close"),
                    rn,
                )
                .where(HistoricalMinuteBar.symbol.in_(syms))
                .where(HistoricalMinuteBar.interval_min == int(minutes))
                .where(HistoricalMinuteBar.ts <= as_of)
            ).subquery("m_last")

            stmt = select(base.c.symbol, base.c.ts, base.c.close).where(base.c.rn <= 3)
            rows = conn.execute(stmt).all()

            grouped: Dict[str, List[Tuple[datetime, float]]] = {}
            for row in rows:
                m = row._mapping
                ts = m["ts"]
                ts = ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)
                grouped.setdefault(m["symbol"], []).append((ts, float(m["close"])))

            for s, items in grouped.items():
                if regular_hours_only:
                    items = [(ts, px) for (ts, px) in items if _is_regular_market_minute(ts)]
                if not items:
                    continue
                items.sort(key=lambda x: x[0], reverse=True)
                out[s] = items[0][1]

        return out


    def earliest_daily_date(self, symbol: str) -> Optional[datetime]:
        """
        Earliest available DAILY bar datetime for a symbol (timezone-aware).
        Convenience wrapper used by UniverseManager.
        """
        s = (symbol or "").upper()
        with engine.connect() as conn:
            dt = conn.execute(
                select(func.min(HistoricalDailyBar.date))
                .where(HistoricalDailyBar.symbol == s)
            ).scalar()
            if dt is None:
                return None
            return dt if getattr(dt, "tzinfo", None) else dt.replace(tzinfo=timezone.utc)

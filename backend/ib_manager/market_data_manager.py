from __future__ import annotations

import os
import logging
from datetime import datetime, timezone, timedelta, time, date
from typing import List, Dict, Any, Tuple, Optional, Iterable, DefaultDict
from collections import defaultdict

from sqlalchemy import select, func, and_

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
        # Fallback: treat ET == UTC (won't shift with DST; not ideal but safe)
        open_utc = datetime(et_day.year, et_day.month, et_day.day, 9, 30, tzinfo=timezone.utc)
        close_utc = datetime(et_day.year, et_day.month, et_day.day, 16, 0, tzinfo=timezone.utc)
        return open_utc, close_utc

    open_et = datetime(et_day.year, et_day.month, et_day.day, 9, 30, tzinfo=_NY)
    close_et = datetime(et_day.year, et_day.month, et_day.day, 16, 0, tzinfo=_NY)
    return open_et.astimezone(timezone.utc), close_et.astimezone(timezone.utc)


def _is_weekday(et_dt: datetime) -> bool:
    # Monday=0 .. Sunday=6
    return et_dt.weekday() < 5


def _is_regular_market_minute(ts_utc: datetime) -> bool:
    """
    True iff ts_utc lies inside a regular-hours NYSE minute (Mon-Fri, 09:30<=t<=16:00 ET).
    Holidays are not detected here; we filter those by data presence elsewhere.
    """
    ts_utc = _ensure_utc(ts_utc)
    if _NY is None:
        # Conservative fallback: accept only weekday UTC 13:30..20:00 (roughly 09:30..16:00 ET, no DST)
        t = ts_utc.time()
        return ts_utc.weekday() < 5 and time(13, 30) <= t <= time(20, 0)

    et = ts_utc.astimezone(_NY)
    if not _is_weekday(et):
        return False
    t = et.time()
    return time(9, 30) <= t <= time(16, 0)


class MarketDataManager:
    """
    Historical data access + small helpers.
    • RTH filtering for intraday.
    • Session-aware 'next tick' that respects holidays/DST using actual bars.
    • Resilient session clock: auto-pick a valid symbol and global fallback.
    """

    def __init__(self) -> None:
        self._last_session: Tuple[Optional[datetime], str] = (None, "regular-hours")
        self._clock_symbol = os.getenv("SIM_REFERENCE_CLOCK_SYMBOL", "SPY").upper()

    # ─────────────────────────── diagnostics / coverage ───────────────────────────

    def has_minute_bars(self, symbol: str, interval_min: int) -> bool:
        """Return True if any minute bars exist for symbol/interval."""
        symbol = (symbol or "").upper()
        with engine.connect() as conn:
            ts = conn.execute(
                select(func.min(HistoricalMinuteBar.ts))
                .where(HistoricalMinuteBar.symbol == symbol)
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
            ).scalar()
            return ts is not None

    def has_daily_bars(self, symbol: str) -> bool:
        """Return True if any daily bars exist for symbol."""
        symbol = (symbol or "").upper()
        with engine.connect() as conn:
            dt = conn.execute(
                select(func.min(HistoricalDailyBar.date))
                .where(HistoricalDailyBar.symbol == symbol)
            ).scalar()
            return dt is not None

    def earliest_daily_date(self, symbol: str) -> Optional[datetime]:
        """Return the earliest daily 'date' for the symbol, or None if absent."""
        symbol = (symbol or "").upper()
        with engine.connect() as conn:
            dt = conn.execute(
                select(func.min(HistoricalDailyBar.date))
                .where(HistoricalDailyBar.symbol == symbol)
            ).scalar()
            return dt

    def pick_reference_symbol(
        self,
        interval_min: int = 5,
        prefer: Optional[Iterable[str]] = None,
    ) -> Optional[str]:
        """
        Choose a good clock symbol for the given interval:
          1) first available from 'prefer' list
          2) the symbol with the most rows at that interval
        """
        prefer_list = [s.strip().upper() for s in (prefer or
                        os.getenv("SIM_REFERENCE_CANDIDATES", "SPY,QQQ,AAPL,MSFT,TSLA,AMD,NVDA,GOOGL,AMZN").split(","))]
        for s in prefer_list:
            try:
                if s and self.has_minute_bars(s, interval_min):
                    return s
            except Exception:
                continue

        # Fallback: most-populated symbol at this interval
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
                return out

            # minute bars
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
        """
        Return up to `lookback` bars PER symbol for many symbols in a single query.
        Uses row_number() OVER (PARTITION BY symbol ORDER BY ts/date DESC).

        Output: { "AAPL": [ {ts, open, high, low, close, volume}, ... ], ... }
        Bars are oldest→newest for each symbol.
        """
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

                # Trim to lookback per symbol if we over-fetched for filtering
                if regular_hours_only:
                    for s in syms:
                        if len(out[s]) > lookback:
                            out[s] = out[s][-lookback:]

        return out

    # ─────────────────────────── resample helpers (5m → DAILY) ───────────────────────────

    def _minutes_grouped_by_et_day(
        self,
        *,
        symbol: str,
        start: datetime,
        end: datetime,
        interval_min: int = 5,
        rth_only: bool = True,
    ) -> DefaultDict[date, List[Dict[str, Any]]]:
        """
        Return {ET_date: [minute bars]} within [start, end] (UTC datetimes).
        """
        symbol = symbol.upper()
        start = _ensure_utc(start)
        end = _ensure_utc(end)

        grouped: DefaultDict[date, List[Dict[str, Any]]] = defaultdict(list)

        with engine.connect() as conn:
            rows = conn.execute(
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
                .where(and_(HistoricalMinuteBar.ts >= start, HistoricalMinuteBar.ts < end))
                .order_by(HistoricalMinuteBar.ts.asc())
            ).all()

        for row in rows:
            m = row._mapping
            ts = m["ts"]
            ts = ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)
            if rth_only and not _is_regular_market_minute(ts):
                continue
            et_day = (ts.astimezone(_NY) if _NY else ts).date()
            grouped[et_day].append(
                {
                    "ts": ts,
                    "open": float(m["open"]),
                    "high": float(m["high"]),
                    "low": float(m["low"]),
                    "close": float(m["close"]),
                    "volume": int(m["volume"] or 0),
                }
            )
        return grouped

    def ensure_daily_bars_for_symbols(
        self,
        symbols: Iterable[str],
        *,
        end_date: datetime,
        lookback_days: int = 3650,
        source_interval_min: int = 5,
        rth_only: bool = True,
    ) -> int:
        """
        Ensure DAILY bars exist (derived from minutes) for all ET days within the
        window [end_date - lookback_days, end_date] for the provided symbols.

        Returns:
            count of daily rows inserted across all symbols.
        """
        end_date = _ensure_utc(end_date)
        start_date = end_date - timedelta(days=int(lookback_days))

        total_inserted = 0

        with engine.begin() as conn:
            for raw in symbols:
                s = (raw or "").upper()
                if not s:
                    continue

                grouped = self._minutes_grouped_by_et_day(
                    symbol=s, start=start_date, end=end_date,
                    interval_min=source_interval_min, rth_only=rth_only
                )
                if not grouped:
                    continue

                # Existing daily dates to skip
                existing = {
                    r._mapping["date"].date()
                    for r in conn.execute(
                        select(HistoricalDailyBar.date)
                        .where(HistoricalDailyBar.symbol == s)
                        .where(and_(HistoricalDailyBar.date >= start_date, HistoricalDailyBar.date <= end_date))
                    ).all()
                }

                for et_day, bars in grouped.items():
                    # If we already have a daily row for this ET day, skip
                    if any((d == et_day) for d in existing):
                        continue
                    bars.sort(key=lambda b: b["ts"])  # oldest → newest
                    o = bars[0]["open"]
                    h = max(b["high"] for b in bars)
                    l = min(b["low"] for b in bars)
                    c = bars[-1]["close"]
                    v = sum(b["volume"] for b in bars)

                    # Store daily 'date' as ET midnight (tz-aware), then to UTC
                    if _NY is not None:
                        dt_et = datetime(et_day.year, et_day.month, et_day.day, 0, 0, tzinfo=_NY)
                        dt_store = dt_et.astimezone(timezone.utc)
                    else:
                        dt_store = datetime(et_day.year, et_day.month, et_day.day, 0, 0, tzinfo=timezone.utc)

                    conn.execute(
                        HistoricalDailyBar.__table__.insert().values(
                            symbol=s,
                            date=dt_store,
                            open=float(o),
                            high=float(h),
                            low=float(l),
                            close=float(c),
                            volume=int(v),
                        )
                    )
                    total_inserted += 1

        if total_inserted > 0:
            log.info("Resampled %d DAILY bars from minutes for %d symbols (up to %s).",
                     total_inserted, len(list(symbols)), end_date.isoformat())
        return total_inserted

    # ─────────────────────────── session-aware tick helpers ───────────────────────────

    def get_next_session_ts(
        self,
        as_of: datetime,
        interval_min: int = 5,
        *,
        reference_symbol: Optional[str] = None,
    ) -> Optional[datetime]:
        """
        Return the next candle timestamp (>= as_of+ε) that lies within a regular-hours
        NY session for `reference_symbol` at `interval_min`.

        If `reference_symbol` is absent or has no coverage for the session/day,
        we automatically fallback to the earliest bar **from ANY symbol** within
        the same RTH window. Holidays & DST still respected because we only return
        timestamps that actually exist in the database.
        """
        as_of = _ensure_utc(as_of)
        clock_sym = ((reference_symbol or self._clock_symbol or "SPY") or "").upper()

        # Try up to 400 consecutive ET days forward (safety bound)
        start_et = as_of.astimezone(_NY) if _NY else as_of
        day = start_et.date()
        epsilon = timedelta(seconds=1)

        with engine.connect() as conn:
            for _ in range(400):
                open_utc, close_utc = _et_bounds_for_date(day)

                if as_of >= close_utc:
                    # Past today's close — move to next calendar day
                    day = (datetime.combine(day, time(0, 0)) + timedelta(days=1)).date()
                    continue

                search_from = max(as_of + epsilon, open_utc)
                if search_from > close_utc:
                    # Before open or after close — skip to next day
                    day = (datetime.combine(day, time(0, 0)) + timedelta(days=1)).date()
                    continue

                next_ts = None

                # 1) Preferred clock symbol
                if clock_sym:
                    next_ts = conn.execute(
                        select(func.min(HistoricalMinuteBar.ts))
                        .where(HistoricalMinuteBar.symbol == clock_sym)
                        .where(HistoricalMinuteBar.interval_min == int(interval_min))
                        .where(HistoricalMinuteBar.ts >= search_from)
                        .where(HistoricalMinuteBar.ts <= close_utc)
                    ).scalar()

                # 2) Fallback: any symbol at this interval in the same RTH window
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

                # No bars in this session → likely holiday or the dataset lacks coverage for this day.
                day = (datetime.combine(day, time(0, 0)) + timedelta(days=1)).date()

        log.warning(
            "next_session_ts: No further bars found after %s (clock=%s, tf=%dm)",
            as_of.isoformat(),
            (clock_sym or "<auto>"),
            interval_min,
        )
        return None

    def get_next_session_ts_global(self, as_of: datetime, interval_min: int = 5) -> Optional[datetime]:
        """Convenience wrapper for callers who explicitly want the global fallback."""
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
        k = 2.0 / (period + 1.0)
        ema = float(candles[-period]["close"])
        for c in candles[-period + 1:]:
            ema = c["close"] * k + ema * (1.0 - k)
        return float(ema)

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

    # ─────────────────────────── mark-to-market helpers ───────────────────────────

    def get_last_close_for_symbols(
        self,
        symbols: List[str],
        minutes: int,
        as_of: datetime,
        *,
        regular_hours_only: bool = True,
    ) -> Dict[str, float]:
        """
        Return a mapping {symbol: last_close_price} using the most recent candle
        at or before `as_of` for the requested timeframe.

        • minutes >= 1440 → use daily bars (<= as_of date)
        • minutes < 1440  → use minute bars with interval_min=minutes
        • When regular_hours_only=True for intraday, only RTH minutes are considered.
        • Symbols with no price are omitted from the result.
        """
        as_of = _ensure_utc(as_of)
        if not symbols:
            return {}
        syms = [s.upper() for s in symbols]

        out: Dict[str, float] = {}

        with engine.connect() as conn:
            # DAILY
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
                stmt = (
                    select(base.c.symbol, base.c.close)
                    .where(base.c.rn == 1)
                )
                for row in conn.execute(stmt).all():
                    m = row._mapping
                    try:
                        out[m["symbol"]] = float(m["close"])
                    except Exception:
                        continue
                return out

            # MINUTE
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

            # Filter for RTH if requested and pick the first valid
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


    def earliest_minute_ts(self, symbol: str, interval_min: int) -> Optional[datetime]:
        """Earliest timestamp for (symbol, interval_min) or None."""
        symbol = (symbol or "").upper()
        with engine.connect() as conn:
            ts = conn.execute(
                select(func.min(HistoricalMinuteBar.ts))
                .where(HistoricalMinuteBar.symbol == symbol)
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
            ).scalar()
            if ts is None:
                return None
            return ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)

    def latest_minute_ts(self, symbol: str, interval_min: int) -> Optional[datetime]:
        """Latest timestamp for (symbol, interval_min) or None."""
        symbol = (symbol or "").upper()
        with engine.connect() as conn:
            ts = conn.execute(
                select(func.max(HistoricalMinuteBar.ts))
                .where(HistoricalMinuteBar.symbol == symbol)
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
            ).scalar()
            if ts is None:
                return None
            return ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)

    def has_bar_at(self, symbol: str, interval_min: int, ts_utc: datetime) -> bool:
        """True iff a bar exists exactly at ts_utc for (symbol, interval_min)."""
        symbol = (symbol or "").upper()
        ts_utc = _ensure_utc(ts_utc)
        with engine.connect() as conn:
            exists = conn.execute(
                select(func.count())
                .select_from(HistoricalMinuteBar)
                .where(HistoricalMinuteBar.symbol == symbol)
                .where(HistoricalMinuteBar.interval_min == int(interval_min))
                .where(HistoricalMinuteBar.ts == ts_utc)
            ).scalar()
            try:
                return int(exists or 0) > 0
            except Exception:
                return bool(exists)


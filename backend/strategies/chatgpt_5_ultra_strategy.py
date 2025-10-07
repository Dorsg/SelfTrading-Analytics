from __future__ import annotations

import os
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from backend.strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("chatgpt_5_ultra_strategy")


class ChatGPT5UltraStrategy:
    """
    High-conviction long-only strategy aiming for strong absolute P&L across 1D and 5m.

    Core principles:
      - Trade only in strong uptrends (EMA50 > EMA200, price > EMA200)
      - Prefer high-quality breakouts (Donchian + Squeeze + Volume)
      - Allow momentum pullback entries when trend is intact (RSI healthy, MACD supportive)
      - Risk managed by ATR-based trailing stop (session-aware limit wiggle)

    Indicators used (single timeframe provided by the runner):
      - EMA(50), EMA(200) for trend
      - Donchian(55) for breakout trigger
      - Bollinger Bands(20, 2.0) for squeeze/volatility regime
      - RSI(14) for momentum filter
      - MACD(12,26,9) for momentum confirmation
      - ATR(14) for dynamic trailing stops
      - Volume MA(20) for breakout confirmation
    """

    name = "ChatGPT5UltraStrategy"

    # Trend
    ema_fast_period = 50
    ema_slow_period = 200

    # Breakout and volatility
    donchian_lookback = 55
    bb_period = 20
    bb_std_dev = 2.0
    squeeze_lookback = 60  # number of widths to compare current width against
    squeeze_percentile = 0.35  # current width should be in the lower 35% of recent widths

    # Momentum
    rsi_period = 14
    rsi_min = 50.0
    rsi_max = 85.0

    # MACD
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9

    # Volume
    volume_ma_period = 20
    volume_surge_multiplier = 1.15

    # ATR trailing stop
    atr_period = 14
    trail_min_pct = 0.6
    trail_max_pct = 9.0

    # Entry buffer (percent) above Donchian for cleaner breakouts
    buy_buffer_pct = 0.25

    # Regime and filters
    rs_period = 60  # relative strength lookback (same timeframe); daily used when tf=5
    min_rs_pct = 0.0  # require outperformance vs SPY
    max_realized_vol_pct = 5.5  # avoid hyper-volatile names on entry

    # Limit order wiggle by session
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

    # Basic quality filters
    min_price = 10.0  # avoid penny and microcaps
    min_vol_5m = 50000.0  # avg 5m volume threshold
    min_vol_1d = 1000000.0  # avg daily volume threshold

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    # ─────────────────────────── BUY ────────────────────────────
    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles: List[Dict] = info.candles or []

        min_bars = max(
            self.ema_slow_period + 1,
            self.donchian_lookback + 1,
            self.bb_period + 1,
            self.rsi_period + 1,
            self.atr_period + 1,
            self.volume_ma_period + 1,
            self.macd_slow + self.macd_signal + 1,
        )
        if len(candles) < min_bars:
            res = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "candles_count": len(candles),
                "required_bars": min_bars,
                "explanation": f"Need ≥{min_bars} bars for multi-indicator analysis",
                "checks": [
                    {"label": "Minimum bars", "ok": False, "actual": len(candles), "wanted": min_bars, "direction": ">="}
                ],
            }
            log.info("%s NO_ACTION - insufficient_data @ %s", symbol, res["price"]) 
            return res

        # Indicators
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        ema_slow = self.mkt.calculate_ema(candles, self.ema_slow_period)
        upper, lower = self.mkt.donchian_channel(candles, self.donchian_lookback)
        bb_upper, bb_mid, bb_lower = self.mkt.calculate_bollinger_bands(candles, self.bb_period, self.bb_std_dev)
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        macd_line, macd_sig = self.mkt.calculate_macd(candles, self.macd_fast, self.macd_slow, self.macd_signal)
        atr = self.mkt.calculate_atr(candles, self.atr_period)
        vol_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        cur_vol = float(candles[-1].get("volume", 0.0) or 0.0)

        # Session gating (avoid XRTH if configured globally)
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        if os.getenv("SIM_REGULAR_HOURS_ONLY", "1") == "1" and session != "regular-hours":
            return {
                "action": "NO_ACTION",
                "reason": "extended_hours_disabled",
                "price": round(price, 4),
                "checks": [{"label": "Regular hours", "ok": False, "actual": session, "wanted": "regular-hours"}],
            }

        # Validate indicators
        if any(
            x is None or x != x  # NaN/None
            for x in [ema_fast, ema_slow, upper, lower, bb_upper, bb_mid, bb_lower, rsi, atr, vol_ma]
        ):
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "explanation": "Missing/NaN indicators",
                "checks": [{"label": "Indicators valid", "ok": False, "actual": "NaN/None", "wanted": "valid"}],
            }

        # Basic liquidity/price gates
        tf = int(getattr(info.runner, "time_frame", 5) or 5)
        min_vol_gate = (self.min_vol_5m if tf == 5 else self.min_vol_1d)
        if price < self.min_price or (vol_ma is not None and vol_ma < min_vol_gate):
            return {
                "action": "NO_ACTION",
                "reason": "liquidity_price_filter",
                "price": round(price, 4),
                "explanation": f"price>={self.min_price} and vol_ma>={min_vol_gate} required",
                "checks": [
                    {"label": "Min price", "ok": price >= self.min_price, "actual": price, "wanted": self.min_price, "direction": ">="},
                    {"label": "Avg volume", "ok": vol_ma >= min_vol_gate if vol_ma is not None else False, "actual": vol_ma, "wanted": min_vol_gate, "direction": ">="},
                ],
            }

        # Squeeze: current BB width vs recent widths
        squeeze_ok = False
        try:
            cur_width = (bb_upper - bb_lower) / bb_mid if bb_mid else float("inf")
            widths: List[float] = []
            # compute widths for last N closes if enough candles
            look = min(self.squeeze_lookback, max(0, len(candles) - self.bb_period))
            for i in range(look):
                sub = candles[: -(i)] if i > 0 else candles
                u, m, l = self.mkt.calculate_bollinger_bands(sub, self.bb_period, self.bb_std_dev)
                if u and l and m:
                    widths.append((u - l) / m)
            if widths:
                widths_sorted = sorted(widths)
                idx = max(0, min(len(widths_sorted) - 1, int(len(widths_sorted) * self.squeeze_percentile)))
                perc = widths_sorted[idx]
                squeeze_ok = cur_width <= perc
        except Exception:
            squeeze_ok = False

        # Regime filters
        rs_val = None
        rs_daily_ok = True
        daily_trend_ok = True
        try:
            bench = "SPY"
            if getattr(info.runner, "time_frame", 5) == 5:
                bench_min = self.mkt.get_candles_until(bench, 5, candles[-1]["ts"], lookback=max(self.rs_period + 5, 120))
                rs_val = self.mkt.relative_strength(candles, bench_min, period=min(self.rs_period, len(bench_min) - 1)) if bench_min else None
                daily_sym = self.mkt.get_candles_until(symbol, 1440, candles[-1]["ts"], lookback=200)
                daily_bench = self.mkt.get_candles_until(bench, 1440, candles[-1]["ts"], lookback=200)
                if daily_sym and daily_bench:
                    rs_daily = self.mkt.relative_strength(daily_sym, daily_bench, period=min(120, len(daily_sym) - 1, len(daily_bench) - 1))
                    rs_daily_ok = (rs_daily is None) or (rs_daily >= self.min_rs_pct)
                # Daily trend confirmation (50EMA)
                if daily_sym and len(daily_sym) >= 50:
                    daily_ema50 = self.mkt.calculate_ema(daily_sym, 50)
                    last_daily_close = float(daily_sym[-1].get("close", 0) or 0)
                    daily_trend_ok = (daily_ema50 is not None) and (last_daily_close > daily_ema50)
            else:
                bench_day = self.mkt.get_candles_until(bench, 1440, candles[-1]["ts"], lookback=max(self.rs_period + 5, 200))
                rs_val = self.mkt.relative_strength(candles, bench_day, period=min(self.rs_period, len(bench_day) - 1)) if bench_day else None
        except Exception:
            rs_val = None
            rs_daily_ok = True
            daily_trend_ok = True

        rv = self.mkt.realized_volatility_pct(candles, period=20)

        # Entry logic
        breakout_level = (upper or price) * (1.0 + (self.buy_buffer_pct / 100.0)) if upper else price
        trend_ok = (price > ema_slow) and (ema_fast > ema_slow)
        breakout_ok = price >= breakout_level
        momentum_ok = self.rsi_min <= rsi <= self.rsi_max
        macd_ok = (macd_line is not None and macd_sig is not None and macd_line >= macd_sig * 0.98)
        vol_ok = cur_vol >= vol_ma * max(self.volume_surge_multiplier, 1.25) if vol_ma > 0 else False
        pullback_ready = (bb_lower is not None) and (price <= bb_lower * 1.02) and trend_ok and (rsi >= self.rsi_min)

        rs_ok = (rs_val is None) or (rs_val >= self.min_rs_pct)
        rv_ok = (rv == rv) and (rv <= self.max_realized_vol_pct)

        # Score-based acceptance: even tighter, require daily trend confirm on 5m
        primary_signals = [trend_ok, breakout_ok, momentum_ok, macd_ok, vol_ok, rs_ok, rv_ok, (squeeze_ok or False)]
        score = sum(bool(x) for x in primary_signals)
        is_5m = int(getattr(info.runner, "time_frame", 5) or 5) == 5
        breakout_accept = (
            breakout_ok and trend_ok and macd_ok and vol_ok and rs_ok and rv_ok and (score >= 7)
            and ((not is_5m) or (rs_daily_ok and daily_trend_ok and squeeze_ok))
        )
        pullback_accept = (
            pullback_ready and macd_ok and rs_ok and rv_ok and ((not is_5m) or (rs_daily_ok and daily_trend_ok)) and squeeze_ok
        )
        accept = breakout_accept or pullback_accept

        checklist = [
            {"label": "Trend (EMA50>EMA200 & px>EMA200)", "ok": trend_ok, "actual": price, "wanted": ema_slow},
            {"label": "Breakout ≥ Donchian+buf", "ok": breakout_ok, "actual": price, "wanted": breakout_level},
            {"label": "RSI range", "ok": momentum_ok, "actual": rsi, "wanted": (self.rsi_min, self.rsi_max), "direction": "range"},
            {"label": "MACD bullish", "ok": macd_ok, "actual": (macd_line or 0.0), "wanted": (macd_sig or 0.0), "direction": ">="},
            {"label": "Volume surge", "ok": vol_ok, "actual": cur_vol, "wanted": vol_ma * self.volume_surge_multiplier, "direction": ">="},
            {"label": "Squeeze (low BB width)", "ok": squeeze_ok, "actual": "low" if squeeze_ok else "high", "wanted": "lower_percentile"},
            {"label": "Alt: Pullback ready", "ok": pullback_ready, "actual": price, "wanted": (bb_lower * 1.02 if bb_lower else None), "direction": "<="},
            {"label": "Relative strength vs SPY", "ok": rs_ok, "actual": rs_val if rs_val is not None else "n/a", "wanted": self.min_rs_pct, "direction": ">="},
            {"label": "Realized vol below cap", "ok": rv_ok, "actual": rv, "wanted": self.max_realized_vol_pct, "direction": "<="},
            {"label": "Daily RS confirm (5m only)", "ok": rs_daily_ok, "actual": None, "wanted": None},
        ]

        if not accept:
            return {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "price": round(price, 4),
                "signal_strength": f"{score}/7",
                "explanation": format_checklist(checklist),
                "checks": checklist,
            }

        # Order details
        atr_pct = (atr / price) * 100.0 if price > 0 else self.trail_min_pct
        widen = 1.25 if (rv == rv and rv > 3.5) else 1.10
        trail_pct = min(max(atr_pct * widen, self.trail_min_pct), self.trail_max_pct)
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 + wiggle)

        res = {
            "action": "BUY",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_stop_order": {
                "action": "SELL",
                "order_type": "TRAIL_LIMIT",
                "trailing_percent": round(trail_pct, 2),
            },
            "signal_strength": f"{score}/7",
            "explanation": format_checklist(checklist),
            "checks": checklist,
        }
        log.debug(
            "BUY %s price=%s limit=%s trail_pct=%s score=%s",
            symbol,
            res["price"],
            res["limit_price"],
            res["trail_stop_order"]["trailing_percent"],
            res["signal_strength"],
        )
        return res

    # ─────────────────────────── SELL ───────────────────────────
    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles: List[Dict] = info.candles or []

        if len(candles) < max(self.atr_period + 1, self.ema_fast_period + 1):
            return {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "explanation": f"Need ≥{max(self.atr_period + 1, self.ema_fast_period + 1)} bars",
                "checks": [
                    {"label": "Minimum bars", "ok": False, "actual": len(candles), "wanted": max(self.atr_period + 1, self.ema_fast_period + 1), "direction": ">="}
                ],
            }

        atr = self.mkt.calculate_atr(candles, self.atr_period)
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        ema_slow = self.mkt.calculate_ema(candles, self.ema_slow_period)
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        macd_line, macd_sig = self.mkt.calculate_macd(candles, self.macd_fast, self.macd_slow, self.macd_signal)

        if atr != atr:  # NaN
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "checks": [{"label": "ATR valid", "ok": False, "actual": "NaN", "wanted": "valid"}],
            }

        # Min-hold guard to avoid same-bar churn
        try:
            pos = info.position
            if pos and getattr(pos, "created_at", None) and candles and candles[-1].get("ts"):
                now_ts = candles[-1]["ts"]
                now_epoch = int((now_ts if getattr(now_ts, "tzinfo", None) else now_ts.replace(tzinfo=timezone.utc)).timestamp())
                pos_epoch = int((pos.created_at if pos.created_at.tzinfo else pos.created_at.replace(tzinfo=timezone.utc)).timestamp())
                held_secs = max(0, now_epoch - pos_epoch)
                tf = int(getattr(info.runner, "time_frame", 5) or 5)
                step_sec = int(os.getenv("SIM_STEP_SECONDS", "300") or "300")
                min_hold = step_sec * 2 if tf == 5 else 86400
                if held_secs < min_hold:
                    return {"action": "NO_ACTION", "reason": "min_hold_guard", "price": round(price, 4)}
        except Exception:
            pass

        # Discretionary exit if strong deterioration or profit fade
        trend_break = price < ema_fast  # react faster on deterioration
        momentum_break = (rsi == rsi and rsi < 45.0)
        macd_bearish = (macd_line is not None and macd_sig is not None and macd_line < macd_sig * 0.99)
        up_pct = 0.0
        if info.position and getattr(info.position, "avg_price", 0) > 0:
            up_pct = (price - info.position.avg_price) / info.position.avg_price * 100.0
        profit_fade = (up_pct > 8.0) and (price < ema_fast)
        discretionary_exit = (trend_break and (momentum_break or macd_bearish)) or profit_fade

        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)

        atr_pct = (atr / price) * 100.0 if price > 0 else self.trail_min_pct
        widen = 1.0 if (price < ema_fast) else 1.15  # tighter stop when below EMA50
        trail_pct = min(max(atr_pct * widen, self.trail_min_pct), self.trail_max_pct)

        if discretionary_exit:
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "reason": "discretionary_reversal",
                "explanation": "Trend/momentum deterioration or profit fade",
                "checks": [
                    {"label": "Trend break (px < EMA200)", "ok": trend_break, "actual": price, "wanted": ema_slow, "direction": "<="},
                    {"label": "RSI < 38", "ok": momentum_break, "actual": rsi, "wanted": 38.0, "direction": "<="},
                    {"label": "MACD bearish", "ok": macd_bearish, "actual": (macd_line or 0.0), "wanted": (macd_sig or 0.0), "direction": "<="},
                    {"label": ">8% up and below EMA50", "ok": profit_fade, "actual": up_pct, "wanted": 8.0, "direction": ">"},
                ],
            }

        # Default: keep position; trailing is managed by broker from initial BUY trail_stop_order
        return {"action": "NO_ACTION", "reason": "hold_with_trailing", "price": round(price, 4)}



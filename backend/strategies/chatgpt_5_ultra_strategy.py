from __future__ import annotations

import os
import logging
from typing import Any, Dict, List

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from backend.strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("chatgpt-5-ultra-strategy")


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
    rsi_min = 52.0
    rsi_max = 80.0

    # MACD
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9

    # Volume
    volume_ma_period = 20
    volume_surge_multiplier = 1.25

    # ATR trailing stop
    atr_period = 14
    trail_min_pct = 0.6
    trail_max_pct = 7.5

    # Entry buffer (percent) above Donchian for cleaner breakouts
    buy_buffer_pct = 0.08

    # Limit order wiggle by session
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

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

        # Entry logic
        breakout_level = (upper or price) * (1.0 + (self.buy_buffer_pct / 100.0)) if upper else price
        trend_ok = (price > ema_slow) and (ema_fast > ema_slow)
        breakout_ok = price >= breakout_level
        momentum_ok = self.rsi_min <= rsi <= self.rsi_max
        macd_ok = (macd_line is not None and macd_sig is not None and macd_line >= macd_sig * 0.98)
        vol_ok = cur_vol >= vol_ma * self.volume_surge_multiplier if vol_ma > 0 else False
        pullback_ready = (bb_lower is not None) and (price <= bb_lower * 1.02) and trend_ok and (rsi >= self.rsi_min)

        # Score-based acceptance: breakout+trend with confirmations, or high-quality pullback
        primary_signals = [trend_ok, breakout_ok, momentum_ok, macd_ok, vol_ok]
        score = sum(bool(x) for x in primary_signals) + (1 if squeeze_ok else 0)
        accept = (breakout_ok and trend_ok and (score >= 4)) or (pullback_ready and macd_ok)

        checklist = [
            {"label": "Trend (EMA50>EMA200 & px>EMA200)", "ok": trend_ok, "actual": price, "wanted": ema_slow},
            {"label": "Breakout ≥ Donchian+buf", "ok": breakout_ok, "actual": price, "wanted": breakout_level},
            {"label": "RSI range", "ok": momentum_ok, "actual": rsi, "wanted": (self.rsi_min, self.rsi_max), "direction": "range"},
            {"label": "MACD bullish", "ok": macd_ok, "actual": (macd_line or 0.0), "wanted": (macd_sig or 0.0), "direction": ">="},
            {"label": "Volume surge", "ok": vol_ok, "actual": cur_vol, "wanted": vol_ma * self.volume_surge_multiplier, "direction": ">="},
            {"label": "Squeeze (low BB width)", "ok": squeeze_ok, "actual": "low" if squeeze_ok else "high", "wanted": "lower_percentile"},
            {"label": "Alt: Pullback ready", "ok": pullback_ready, "actual": price, "wanted": (bb_lower * 1.02 if bb_lower else None), "direction": "<="},
        ]

        if not accept:
            return {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "price": round(price, 4),
                "signal_strength": f"{score}/6",
                "explanation": format_checklist(checklist),
                "checks": checklist,
            }

        # Order details
        atr_pct = (atr / price) * 100.0 if price > 0 else self.trail_min_pct
        trail_pct = min(max(atr_pct * 1.2, self.trail_min_pct), self.trail_max_pct)
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
            "signal_strength": f"{score}/6",
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

        # Discretionary exit if strong deterioration
        trend_break = price < ema_slow
        momentum_break = (rsi == rsi and rsi < 35.0)
        macd_bearish = (macd_line is not None and macd_sig is not None and macd_line < macd_sig * 0.98)
        discretionary_exit = (trend_break and (momentum_break or macd_bearish)) or (price < ema_fast * 0.97)

        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)

        atr_pct = (atr / price) * 100.0 if price > 0 else self.trail_min_pct
        trail_pct = min(max(atr_pct * 1.2, self.trail_min_pct), self.trail_max_pct)

        if discretionary_exit:
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "reason": "discretionary_reversal",
                "explanation": "Trend/momentum deterioration (EMA/RSI/MACD)",
                "checks": [
                    {"label": "Trend break (px < EMA200)", "ok": trend_break, "actual": price, "wanted": ema_slow, "direction": "<="},
                    {"label": "RSI < 35", "ok": momentum_break, "actual": rsi, "wanted": 35.0, "direction": "<="},
                    {"label": "MACD bearish", "ok": macd_bearish, "actual": (macd_line or 0.0), "wanted": (macd_sig or 0.0), "direction": "<="},
                ],
            }

        return {
            "action": "SELL",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_percent": round(trail_pct, 2),
            "explanation": f"Trailing stop at {trail_pct:.2f}% (ATR-adaptive)",
            "checks": [{"label": "ATR-based trail", "ok": True, "actual": trail_pct, "wanted": "within min/max"}],
        }



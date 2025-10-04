from __future__ import annotations

import logging
import os
from typing import Any, Dict

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from backend.strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("gemini-2-5-pro-strategy")


class Gemini25ProStrategy:
    """
    Long-only strategy combining trend, momentum, and volatility indicators.
      • MACD for trend confirmation
      • Stochastic Oscillator for momentum
      • Bollinger Bands for volatility-based entries
      • Volume confirmation
      • ATR-based trailing stop for exits
    """

    name = "Gemini25ProStrategy"

    # MACD settings
    macd_fast_period = 12
    macd_slow_period = 26
    macd_signal_period = 9

    # Stochastic Oscillator settings
    stoch_k_period = 14
    stoch_d_period = 3
    stoch_oversold = 30.0
    stoch_overbought = 80.0

    # Bollinger Bands settings
    bb_period = 20
    bb_std_dev = 2.0

    # Volume settings
    volume_ma_period = 20

    # ATR settings for trailing stop
    atr_period = 14
    trail_min_pct = 1.0
    trail_max_pct = 10.0

    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        min_bars = max(
            self.macd_slow_period + self.macd_signal_period,
            self.stoch_k_period + self.stoch_d_period,
            self.bb_period,
            self.volume_ma_period,
            self.atr_period + 1,
        )

        if len(candles) < min_bars:
            return {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "explanation": f"Need at least {min_bars} bars",
            }

        macd_line, signal_line = self.mkt.calculate_macd(
            candles, self.macd_fast_period, self.macd_slow_period, self.macd_signal_period
        )
        stoch_k, stoch_d = self.mkt.calculate_stochastic(
            candles, self.stoch_k_period, self.stoch_d_period
        )
        bb_upper, bb_middle, bb_lower = self.mkt.calculate_bollinger_bands(
            candles, self.bb_period, self.bb_std_dev
        )
        volume_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        current_volume = candles[-1]["volume"]
        atr_val = self.mkt.calculate_atr(candles, period=self.atr_period)

        if any(
            v is None or v != v
            for v in [
                macd_line,
                signal_line,
                stoch_k,
                bb_lower,
                volume_ma,
                atr_val,
            ]
        ):
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "explanation": "One or more indicators returned NaN",
            }
        
        # Buy conditions
        trend_ok = macd_line > signal_line
        momentum_ok = stoch_k < self.stoch_oversold
        volatility_ok = price <= bb_lower * 1.005  # Price is near or below the lower band
        volume_ok = current_volume > volume_ma

        checklist = [
            {"label": "Trend (MACD > Signal)", "ok": trend_ok},
            {"label": f"Momentum (Stoch %K < {self.stoch_oversold})", "ok": momentum_ok},
            {"label": "Volatility (Price near BB Lower)", "ok": volatility_ok},
            {"label": "Volume (Current > MA)", "ok": volume_ok},
        ]

        if not all(c["ok"] for c in checklist):
            return {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "explanation": format_checklist(checklist),
                "checks": checklist,
            }
        
        trail_pct = min(
            max((atr_val / price) * 100.0, self.trail_min_pct), self.trail_max_pct
        )
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = (
            self.limit_wiggle_xrth
            if session == "extended-hours"
            else self.limit_wiggle_rth
        )
        limit_price = price * (1 + wiggle)

        log.info(f"BUY signal for {symbol} at {price}")
        return {
            "action": "BUY",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_stop_order": {
                "action": "SELL",
                "order_type": "TRAIL_LIMIT",
                "trailing_percent": round(trail_pct, 2),
            },
            "explanation": format_checklist(checklist),
            "checks": checklist,
        }

    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        # The trailing stop is attached at buy time, so this method is for discretionary sells.
        # For now, we rely solely on the ATR trailing stop.
        # A more complex version could implement a discretionary sell based on indicators.
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        # Sell conditions for discretionary exit (e.g., strong reversal signal)
        stoch_k, _ = self.mkt.calculate_stochastic(
            candles, self.stoch_k_period, self.stoch_d_period
        )
        
        if stoch_k is not None and stoch_k > self.stoch_overbought:
            log.info(f"Discretionary SELL for {symbol} at {price} due to overbought condition")
            return {"action": "SELL", "order_type": "MKT", "reason": "overbought_stochastic"}

        return {"action": "NO_ACTION", "reason": "no_sell_signal"}

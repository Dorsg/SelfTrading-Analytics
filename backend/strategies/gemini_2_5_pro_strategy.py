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
    New and Improved Gemini 2.5 Pro Strategy.

    This is a robust, trend-following strategy designed to capture gains in established uptrends
    while managing risk. It combines multi-layered trend confirmation with precise entry
    triggers and an intelligent, multi-faceted exit plan.

    Core Principles:
    1.  **Triple-Confirmation Trend Filter**: Establishes a strong, confirmed uptrend.
    2.  **Dynamic Pullback Entry**: Enters on healthy pullbacks, not contradictory signals.
    3.  **Volatility-Aware Risk Management**: Avoids excessively volatile stocks and uses
        an ATR-based trailing stop to let winners run.
    4.  **Proactive Exit Strategy**: Takes profits and cuts losses based on signs of trend
        weakness, not just a trailing stop.
    """

    name = "Gemini25ProStrategy"

    # Trend Confirmation
    ema_fast_period = 50
    ema_slow_period = 200

    # MACD for momentum
    macd_fast_period = 12
    macd_slow_period = 26
    macd_signal_period = 9

    # Stochastic Oscillator for entry triggers
    stoch_k_period = 14
    stoch_d_period = 3
    stoch_oversold = 20.0
    stoch_overbought = 80.0

    # Volume
    volume_ma_period = 20

    # Risk Management & Profit Taking
    atr_period = 14
    atr_volatility_threshold_pct = 5.0  # Skip trades if ATR is > 5% of price
    trail_stop_atr_multiplier = 2.0  # Trail stop at 2x ATR
    take_profit_atr_multiplier = 4.0  # Take profit at 4x ATR from entry

    # System settings
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        # Basic Filters: Price and Data
        if price < 10.0:
            return {
                "action": "NO_ACTION",
                "reason": "price_too_low",
                "explanation": f"Price {price:.2f} is below the minimum threshold of $10",
            }

        min_bars = self.ema_slow_period + 1
        if len(candles) < min_bars:
            return {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "explanation": f"Need at least {min_bars} bars for slow EMA",
            }

        # --- Indicator Calculations ---
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        ema_slow = self.mkt.calculate_ema(candles, self.ema_slow_period)
        macd_line, signal_line = self.mkt.calculate_macd(
            candles, self.macd_fast_period, self.macd_slow_period, self.macd_signal_period
        )
        stoch_k, stoch_d = self.mkt.calculate_stochastic(
            candles, self.stoch_k_period, self.stoch_d_period
        )
        volume_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        current_volume = candles[-1]["volume"]
        atr_val = self.mkt.calculate_atr(candles, period=self.atr_period)

        # Check for invalid indicator values
        if any(v is None or v != v for v in [ema_fast, ema_slow, macd_line, signal_line, stoch_k, stoch_d, atr_val]):
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "explanation": "One or more core indicators returned NaN",
            }

        # --- Buy Conditions ---
        # 1. Trend Filter: Must be in a strong, confirmed uptrend.
        trend_ok = ema_fast > ema_slow and price > ema_fast
        
        # 2. Momentum Filter: Bullish momentum must be present.
        momentum_ok = macd_line > signal_line and macd_line > 0
        
        # 3. Entry Trigger: Enter on a pullback, as signaled by Stochastic.
        # We check the previous stochastic to catch the crossover.
        prev_stoch_k, _ = self.mkt.calculate_stochastic(candles[:-1], self.stoch_k_period, self.stoch_d_period) if len(candles) > min_bars else (None, None)
        entry_trigger_ok = prev_stoch_k is not None and prev_stoch_k < self.stoch_oversold and stoch_k >= self.stoch_oversold

        # 4. Volume Confirmation: Ensure there's conviction behind the move.
        volume_ok = current_volume > volume_ma

        # 5. Volatility Filter: Avoid overly risky trades.
        atr_pct = (atr_val / price) * 100.0 if price > 0 else 0
        volatility_ok = atr_pct < self.atr_volatility_threshold_pct

        # --- Decision ---
        checklist = [
            {"label": f"Trend (Price > EMA{self.ema_fast_period} > EMA{self.ema_slow_period})", "ok": trend_ok},
            {"label": "Momentum (MACD Bullish & > 0)", "ok": momentum_ok},
            {"label": f"Entry (Stoch Cross > {self.stoch_oversold})", "ok": entry_trigger_ok},
            {"label": "Volume (Current > MA)", "ok": volume_ok},
            {"label": f"Volatility (ATR < {self.atr_volatility_threshold_pct}%)", "ok": volatility_ok},
        ]

        if not all(c["ok"] for c in checklist):
            return {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "explanation": format_checklist(checklist),
                "checks": checklist,
            }

        # --- Order Calculation ---
        trail_pct = (atr_val * self.trail_stop_atr_multiplier / price) * 100.0
        
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 + wiggle)
        
        # Calculate take profit price
        take_profit_price = price + (atr_val * self.take_profit_atr_multiplier)

        log.info(f"BUY signal for {symbol} at {price:.2f}. Trailing stop: {trail_pct:.2f}%, Take profit: {take_profit_price:.2f}")
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
            "take_profit_order": {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(take_profit_price, 2),
            },
            "explanation": format_checklist(checklist),
            "checks": checklist,
        }

    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        """
        Discretionary sell logic. The primary exit is the trailing stop or take profit order
        placed at buy time. This function provides an additional layer of risk management
        by exiting if the trend shows clear signs of reversal.
        """
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []
        position = info.position

        if not position or not candles or len(candles) < self.ema_fast_period:
            return {"action": "NO_ACTION", "reason": "no_position_or_data"}

        # --- Indicator Calculations for Sell ---
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        macd_line, signal_line = self.mkt.calculate_macd(
            candles, self.macd_fast_period, self.macd_slow_period, self.macd_signal_period
        )
        
        if any(v is None or v != v for v in [ema_fast, macd_line, signal_line]):
            return {"action": "NO_ACTION", "reason": "sell_indicator_unavailable"}

        # --- Sell Conditions ---
        # 1. Trend Reversal Signal 1: Price breaks below the fast EMA.
        trend_reversal_ma = price < ema_fast
        
        # 2. Trend Reversal Signal 2: MACD bearish crossover.
        trend_reversal_macd = macd_line < signal_line

        if trend_reversal_ma and trend_reversal_macd:
            log.info(f"Discretionary SELL for {symbol} at {price:.2f} due to trend reversal signals.")
            return {
                "action": "SELL",
                "order_type": "MKT",
                "reason": "trend_reversal_signal",
                "explanation": "Price crossed below fast EMA and MACD turned bearish."
            }

        return {"action": "NO_ACTION", "reason": "no_sell_signal"}

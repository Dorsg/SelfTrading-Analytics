from __future__ import annotations
import math
from typing import List, Dict, Any

import numpy as np
import pandas as pd


def calculate_performance_metrics(trades: List[Dict[str, Any]], runners: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Compute robust per-strategy KPIs from realized trades only.

    We avoid relying on Runner budgets (which may be unset or stale) and instead:
      - Total P&L (%): compounded per-trade return, product(1 + r_i) - 1
      - Profit Factor: sum wins / abs(sum losses)
      - Max Drawdown (%): from normalized equity built by compounding trade returns
      - Sharpe Ratio: mean(returns) / std(returns) * sqrt(252) [returns per trade]
    """
    if not trades:
        return {}

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return {}

    # Filter to closed trades with necessary fields
    if "sell_ts" in trades_df.columns:
        trades_df = trades_df[trades_df["sell_ts"].notna()]
    if trades_df.empty:
        return {}

    # Ensure numeric types are floats to avoid Decimal/float conflicts
    for col in ("pnl_amount", "pnl_percent"):
        if col in trades_df.columns:
            trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").astype(float)
        else:
            trades_df[col] = 0.0

    # Parse timestamps and sort within each strategy
    trades_df["sell_ts"] = pd.to_datetime(trades_df["sell_ts"], errors="coerce")
    trades_df = trades_df.dropna(subset=["sell_ts"]).copy()
    if trades_df.empty:
        return {}

    # Guard strategy column
    if "strategy" not in trades_df.columns:
        trades_df["strategy"] = "-"

    results: Dict[str, Dict[str, Any]] = {}
    for strategy_name, group in trades_df.groupby("strategy"):
        group = group.sort_values(by="sell_ts")

        # Returns series as decimal per trade
        returns = (group["pnl_percent"].fillna(0.0) / 100.0).astype(float)
        # Cap losses at -100% to avoid impossible compounding below -100%
        try:
            returns = returns.clip(lower=-1.0)
        except Exception:
            # If clip not available due to dtype issues, fallback via numpy
            returns = pd.Series(np.maximum(returns.values, -1.0), index=returns.index)

        # Compounded P&L: sequentially compound per-trade returns
        try:
            if len(returns) > 0:
                compounded = float(np.prod(1.0 + returns) - 1.0)
            else:
                compounded = 0.0
        except Exception:
            compounded = 0.0

        compounded_pnl_pct = compounded * 100.0

        # Profit Factor
        try:
            gross_profit = float(group.loc[group["pnl_amount"] > 0, "pnl_amount"].sum())
            gross_loss = float(abs(group.loc[group["pnl_amount"] < 0, "pnl_amount"].sum()))
            profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (0.0 if gross_profit == 0 else float("inf"))
            if not np.isfinite(profit_factor):
                # Represent infinity as a large number for UI readability (optional)
                profit_factor = 0.0
        except Exception:
            profit_factor = 0.0

        # Max Drawdown from normalized equity curve
        try:
            equity = np.cumprod(1.0 + returns.values) if len(returns) > 0 else np.array([1.0])
            peaks = np.maximum.accumulate(equity)
            dd = (peaks - equity) / np.where(peaks > 0, peaks, 1.0)
            max_drawdown_pct = float(np.max(dd) * 100.0) if dd.size else 0.0
        except Exception:
            max_drawdown_pct = 0.0

        # Sharpe Ratio (per-trade returns, RF=0), annualized with sqrt(252)
        try:
            std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
            mean = float(returns.mean()) if len(returns) > 0 else 0.0
            if std > 0:
                sharpe_ratio = (mean / std) * math.sqrt(252.0)
            else:
                sharpe_ratio = 0.0
        except Exception:
            sharpe_ratio = 0.0

        results[strategy_name] = {
            "compounded_pnl_pct": float(compounded_pnl_pct),
            "profit_factor": float(profit_factor),
            "max_drawdown_pct": float(max_drawdown_pct),
            "sharpe_ratio": float(sharpe_ratio),
        }

    return results

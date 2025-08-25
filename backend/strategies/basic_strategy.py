from __future__ import annotations
import random, logging
from typing import Any
from backend.strategies.base import Strategy
from strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("basic-strategy")

class BasicStrategy(Strategy):
    name: str = "BasicStrategy"

    # flat → buy half the time
    def decide_buy(self, info: RunnerDecisionInfo) -> dict[str, Any] | None:
        if random.random() < 0.5:
            return {"action": "NO_ACTION", "reason": "coin_flip"}
        return {
            "action":     "BUY",
            "symbol":     info.runner.stock,
            "quantity":   1,
            "order_type": "MKT",
        }

    # in-position → sell half the time
    def decide_sell(self, info: RunnerDecisionInfo) -> dict[str, Any] | None:
        if random.random() < 0.5:
            return {"action": "NO_ACTION", "reason": "hold_coin_flip"}
        return {
            "action":   "SELL",
            "symbol":   info.runner.stock,
            "quantity": info.position.quantity,
            "order_type": "MKT",
        }

    def decide_refresh(self, info: RunnerDecisionInfo) -> dict[str, Any] | None:
        return {"action": None}

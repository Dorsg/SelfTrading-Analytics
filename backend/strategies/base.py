from __future__ import annotations

import abc
import logging
from abc import ABC
from backend.ib_manager.market_data_manager import MarketDataManager
from strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger(__name__)
MKT = MarketDataManager()


class Strategy(ABC):

    # ------------------------------------------------------------------ public
    @abc.abstractmethod
    def decide_buy(self, info: RunnerDecisionInfo) -> dict | None: ...
    
    @abc.abstractmethod
    def decide_sell(self, info: RunnerDecisionInfo) -> dict | None: ...
    
    @abc.abstractmethod
    def decide_refresh(self, info: RunnerDecisionInfo) -> dict | None: ...
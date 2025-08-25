from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional
from database.models import Runner, OpenPosition

@dataclass(slots=True)
class RunnerDecisionInfo:
    """
    Tiny, immutable context object passed to strategies.
    """
    runner: Runner
    position: Optional[OpenPosition]
    current_price: float
    candles: List[Dict]  # last ≤250 bars
    distance_from_time_limit: Optional[float]

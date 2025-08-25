from __future__ import annotations
from datetime import datetime, time
from zoneinfo import ZoneInfo

# ─────────── clock constants ───────────
ET = ZoneInfo("America/New_York")

PRE_MARKET_OPEN      = time(4, 0)
REGULAR_MARKET_OPEN  = time(9, 30)
REGULAR_MARKET_CLOSE = time(16, 0)
AFTER_MARKET_CLOSE   = time(20, 0)

TF_DAILY = 1_440  # minutes in a day

# ─────────── helpers ───────────
def now_et() -> datetime:
    return datetime.now(tz=ET)

def to_et(dt: datetime) -> datetime:
    return dt.astimezone(ET)

def minutes_since_session_open(ts: datetime) -> int:
    """Minutes elapsed in the CURRENT trading session (pre/regular/after)."""
    if PRE_MARKET_OPEN <= ts.time() < REGULAR_MARKET_OPEN:
        open_dt = ts.replace(hour=PRE_MARKET_OPEN.hour, minute=0, second=0, microsecond=0)
    elif REGULAR_MARKET_OPEN <= ts.time() < REGULAR_MARKET_CLOSE:
        open_dt = ts.replace(hour=REGULAR_MARKET_OPEN.hour, minute=30, second=0, microsecond=0)
    else:  # after-market
        open_dt = ts.replace(hour=REGULAR_MARKET_CLOSE.hour, minute=0, second=0, microsecond=0)
    return int((ts - open_dt).total_seconds() // 60)


from datetime import datetime, timezone
from backend.broker.mock_broker import MockBroker
from database.db_manager import DBManager

class _R:
    def __init__(self, rid, sym="AAPL", strategy="chatgpt_5_strategy", tf=5):
        self.id = rid
        self.stock = sym
        self.strategy = strategy
        self.time_frame = tf
        self.parameters = {}

def _utc(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)

def test_trailing_stop_cross_exits():
    broker = MockBroker()
    r = _R(1)
    t0 = _utc(2020,1,1,14,30)

    # create user+runner scaffolding
    with DBManager() as db:
        u = db.get_or_create_user("analytics", "a@a", "x")
        uid = u.id

    # open position
    assert broker.buy(user_id=uid, runner=r, symbol="AAPL", price=100.0, quantity=10, decision={}, at=t0)
    broker.arm_trailing_stop_once(user_id=uid, runner=r, entry_price=100.0, trail_pct=5.0, at=t0)

    # price goes up, then falls below 5% trail -> exit
    broker.on_tick(user_id=uid, runner=r, price=110.0, at=_utc(2020,1,1,15,0))
    # trail stop is at 110*(1-0.05)=104.5, drop to 104.49 triggers
    out = broker.on_tick(user_id=uid, runner=r, price=104.49, at=_utc(2020,1,1,15,5))
    assert out["stop_cross_exits"] == 1

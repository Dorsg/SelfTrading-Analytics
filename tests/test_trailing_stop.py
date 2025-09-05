"""
Integration tests for broker-managed trailing stops.

💡 How to run
-------------
1) Ensure you can reach the same Postgres instance your app uses. The tests
   use the project's SQLAlchemy engine, so set DATABASE_URL (or POSTGRES_*)
   as you normally do for the app (docker-compose envs work fine).

   Example:
     export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/selftrading_analytics_db'

2) (Optional) Create a clean throwaway DB/schema for tests.

3) Run:
     pytest -q tests/test_trailing_stop.py

What this validates
-------------------
• A trailing stop can be armed once after BUY and will:
    - respect a one-bar activation delay (no stop-out on the same bar)
    - track the highest price after activation
    - update the stop price as highest_price * (1 - trail_pct/100)
    - close the position when price <= stop

These tests exercise the broker directly (no strategy logic or market data
fetching is required), and persist/inspect state through the DB models.

If Postgres is not reachable, the whole module is skipped with an informative
message.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

# Ensure we're in analytics mode for lenient validations elsewhere (defensive).
os.environ.setdefault("RUNNING_ENV", "analytics")

# Try to connect early; skip the whole module if DB is not reachable.
from database.db_core import engine, Base  # noqa: E402
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
except Exception as e:  # pragma: no cover - environment guard
    pytest.skip(
        f"Postgres not available (set DATABASE_URL / POSTGRES_*). Reason: {e}",
        allow_module_level=True,
    )

# Import remaining app bits only after the engine is proven usable.
from backend.broker.mock_broker import MockBroker  # noqa: E402
from backend.database.db_manager import DBManager  # noqa: E402
from backend.database.models import Runner, OpenPosition, ExecutedTrade  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def create_schema():
    """
    Create tables if they don't exist yet (safe to call on an existing DB).
    """
    Base.metadata.create_all(bind=engine)


@pytest.fixture()
def analytics_user_and_account():
    """
    Ensure the 'analytics' user and a funded mock account exist.
    Yields the user_id for convenience.
    """
    with DBManager() as db:
        user = db.get_or_create_user("analytics", "analytics@example.com", "analytics")
        db.ensure_account(user_id=user.id, name="mock", cash=10_000_000)
        yield int(user.id)


def _mk_runner(db: DBManager, user_id: int, *, symbol: str = "AAPL", tf_min: int = 5, trail_pct: float = 5.0) -> Runner:
    r = Runner(
        user_id=user_id,
        name="test-runner-trail",
        strategy="chatgpt_5_strategy",
        budget=10_000,
        current_budget=10_000,
        stock=symbol.upper(),
        time_frame=tf_min,
        parameters={"trailing_stop_percent": trail_pct},
        exit_strategy="hold_forever",
        activation="active",
    )
    db.db.add(r)
    db.db.commit()
    return r


def _get_pos(db: DBManager, rid: int) -> OpenPosition | None:
    return db.db.query(OpenPosition).filter(OpenPosition.runner_id == rid).first()


def _last_trade(db: DBManager, rid: int) -> ExecutedTrade | None:
    return (
        db.db.query(ExecutedTrade)
        .filter(ExecutedTrade.runner_id == rid)
        .order_by(ExecutedTrade.id.desc())
        .first()
    )


def test_trailing_stop_activation_delay_and_trigger(analytics_user_and_account):
    """
    Scenario:
      • BUY @ 100, arm 5% trailing stop, timeframe=5m
      • Same-bar plunge to 90 → should NOT stop out (activation delay)
      • Next bar: price 100 (activates), then rally to 110 (highest=110, stop=104.5)
      • Next tick drop to 104.40 → should SELL (<= 104.50 stop)
    """
    uid = analytics_user_and_account
    broker = MockBroker()

    # Pick deterministic times around a real NY session (UTC) for readability.
    T0 = datetime(2021, 1, 4, 14, 30, tzinfo=timezone.utc)   # ~09:30 ET
    TF = 5  # minutes

    with DBManager() as db:
        runner = _mk_runner(db, uid, symbol="AAPL", tf_min=TF, trail_pct=5.0)

        # BUY and arm trailing stop
        ok = broker.buy(
            user_id=uid,
            runner=runner,
            symbol=runner.stock,
            price=100.0,
            quantity=10,
            decision={"action": "BUY", "order_type": "MKT"},
            at=T0,
        )
        assert ok is True, "BUY should succeed with funded mock account"

        # Arm once with a 1-bar activation delay
        broker.arm_trailing_stop_once(
            user_id=uid,
            runner=runner,
            entry_price=100.0,
            trail_pct=5.0,
            at=T0,
            interval_min=TF,
        )

        # Same-bar plunge — should NOT trigger due to activation delay.
        broker.on_tick(user_id=uid, runner=runner, price=90.0, at=T0)

        pos = _get_pos(db, runner.id)
        assert pos is not None, "Position must still be open on same-bar plunge"
        assert float(pos.quantity) == 10
        # Highest price may or may not update pre-activation; tolerate either but ensure no exit.
        assert db.get_open_position(runner.id) is not None

        # Next bar boundary: advance 5 minutes → activation eligible
        T1 = T0 + timedelta(minutes=TF)
        broker.on_tick(user_id=uid, runner=runner, price=100.0, at=T1)

        pos = _get_pos(db, runner.id)
        assert pos is not None, "Still open after activation bar if price not at/under stop"
        # Rally to new high (110) → stop should trail to 110 * 0.95 = 104.5
        T2 = T1 + timedelta(minutes=1)
        broker.on_tick(user_id=uid, runner=runner, price=110.0, at=T2)

        pos = _get_pos(db, runner.id)
        assert pos is not None, "Position should remain open at new highs"
        assert pytest.approx(float(pos.highest_price or 0.0), rel=0, abs=1e-9) >= 110.0
        assert pytest.approx(float(pos.stop_price or 0.0), rel=0, abs=1e-6) == 110.0 * 0.95

        # Drop below the trailed stop (<= 104.5) → should SELL on this tick
        T3 = T2 + timedelta(minutes=1)
        broker.on_tick(user_id=uid, runner=runner, price=104.40, at=T3)

        pos_after = _get_pos(db, runner.id)
        assert pos_after is None, "Trailing stop should have closed the position"

        tr = _last_trade(db, runner.id)
        assert tr is not None, "Executed trade should be recorded on stop-out"
        assert float(tr.buy_price) == pytest.approx(100.0)
        assert float(tr.sell_price) == pytest.approx(104.40)
        assert tr.sell_ts is not None and tr.sell_ts.replace(tzinfo=timezone.utc) == T3


def test_trailing_stop_updates_high_and_stop(analytics_user_and_account):
    """
    Scenario:
      • BUY @ 50, arm 8% trailing, 5m TF
      • New highs at 51 → stop becomes 51 * 0.92
      • New highs at 55 → stop lifts to 55 * 0.92
      • Pullback to just above stop → remains open
      • Pullback to below stop → closes
    """
    uid = analytics_user_and_account
    broker = MockBroker()
    TF = 5

    T0 = datetime(2021, 1, 5, 14, 30, tzinfo=timezone.utc)

    with DBManager() as db:
        runner = _mk_runner(db, uid, symbol="MSFT", tf_min=TF, trail_pct=8.0)

        assert broker.buy(
            user_id=uid,
            runner=runner,
            symbol=runner.stock,
            price=50.0,
            quantity=5,
            decision={"action": "BUY", "order_type": "MKT"},
            at=T0,
        )

        broker.arm_trailing_stop_once(
            user_id=uid,
            runner=runner,
            entry_price=50.0,
            trail_pct=8.0,
            at=T0,
            interval_min=TF,
        )

        # Activate on next bar, and make a small new high
        T1 = T0 + timedelta(minutes=TF)
        broker.on_tick(user_id=uid, runner=runner, price=51.0, at=T1)

        with DBManager() as db2:
            pos = _get_pos(db2, runner.id)
            assert pos is not None
            assert pytest.approx(float(pos.highest_price or 0.0), rel=0, abs=1e-9) >= 51.0
            assert pytest.approx(float(pos.stop_price or 0.0), rel=0, abs=1e-6) == 51.0 * 0.92

        # Another higher high → stop should lift
        T2 = T1 + timedelta(minutes=1)
        broker.on_tick(user_id=uid, runner=runner, price=55.0, at=T2)

        with DBManager() as db3:
            pos = _get_pos(db3, runner.id)
            assert pos is not None
            assert pytest.approx(float(pos.highest_price or 0.0), rel=0, abs=1e-9) >= 55.0
            assert pytest.approx(float(pos.stop_price or 0.0), rel=0, abs=1e-6) == 55.0 * 0.92

        # Pull back to just ABOVE the stop → stay open
        near_stop = 55.0 * 0.92 + 0.01
        T3 = T2 + timedelta(minutes=1)
        broker.on_tick(user_id=uid, runner=runner, price=near_stop, at=T3)

        with DBManager() as db4:
            assert _get_pos(db4, runner.id) is not None

        # Cross below the stop → should close
        below_stop = 55.0 * 0.92 - 0.02
        T4 = T3 + timedelta(minutes=1)
        broker.on_tick(user_id=uid, runner=runner, price=below_stop, at=T4)

        with DBManager() as db5:
            assert _get_pos(db5, runner.id) is None
            tr = _last_trade(db5, runner.id)
            assert tr is not None
            assert float(tr.sell_price) == pytest.approx(below_stop)
            assert tr.sell_ts is not None and tr.sell_ts.replace(tzinfo=timezone.utc) == T4

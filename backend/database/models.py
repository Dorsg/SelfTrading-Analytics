# database/models.py
from __future__ import annotations
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, foreign 


aware_utc_now = lambda: datetime.now(timezone.utc)

Base = declarative_base()

# ───────────────────────────── Users ─────────────────────────────
class User(Base):
    __tablename__ = "users"

    id              = Column(Integer, primary_key=True, index=True)
    email           = Column(String, unique=True, nullable=False, index=True)
    username        = Column(String, unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)

    # optional IB creds (NULL → read-only / paper user)
    ib_account_id = Column(String)
    ib_username   = Column(String)
    ib_password   = Column(String)

    created_at = Column(DateTime(timezone=True), default=aware_utc_now)
    updated_at = Column(DateTime(timezone=True), default=aware_utc_now, onupdate=aware_utc_now)

    runners = relationship(
        "Runner", back_populates="user", cascade="all, delete-orphan"
    )

# ───────────────────── Account snapshots ─────────────────────
class AccountSnapshot(Base):
    __tablename__ = "account_snapshots"

    id        = Column(Integer, primary_key=True, index=True)
    user_id   = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    timestamp = Column(DateTime(timezone=True), default=aware_utc_now)
    account   = Column(String, nullable=False)

    total_cash_value     = Column(Float)
    net_liquidation      = Column(Float)
    available_funds      = Column(Float)
    buying_power         = Column(Float)
    unrealized_pnl       = Column(Float)
    realized_pnl         = Column(Float)
    excess_liquidity     = Column(Float)
    gross_position_value = Column(Float)

# ─────────────────────── Open positions ───────────────────────
class OpenPosition(Base):
    __tablename__ = "open_positions"

    id      = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    symbol      = Column(String, nullable=False)
    quantity    = Column(Float, nullable=False)
    avg_price   = Column(Float, nullable=False)
    account     = Column(String, nullable=False)
    last_update = Column(DateTime(timezone=True), default=aware_utc_now)

# ─────────────────────────── Runner ────────────────────────────
class Runner(Base):
    __tablename__  = "runners"
    __table_args__ = (UniqueConstraint("user_id", "name", name="uix_user_runner_name"),)

    id      = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    name             = Column(String, nullable=False, index=True)
    strategy         = Column(String, nullable=False)
    budget           = Column(Float,  nullable=False)
    current_budget  = Column(Float,   nullable=False)         
    stock            = Column(String, nullable=False)
    time_frame       = Column(Integer, nullable=False)
    time_range_from  = Column(DateTime(timezone=True))
    time_range_to    = Column(DateTime(timezone=True))
    exit_strategy    = Column(String,  nullable=False)
    activation       = Column(String,  default="active", nullable=False)
    created_at       = Column(DateTime(timezone=True), default=aware_utc_now)
    updated_at       = Column(DateTime(timezone=True), default=aware_utc_now, onupdate=aware_utc_now)
    parameters = Column(JSONB, nullable=False, default=dict)

    user    = relationship("User", back_populates="runners")
    orders  = relationship(                       # live / open orders only
        "Order",
        back_populates="runner",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    trades  = relationship(                       # full trade history
        "ExecutedTrade",
        primaryjoin="Runner.id==ExecutedTrade.runner_id",
        back_populates="runner",
        viewonly=True,
    )


# ─────────────────────────── Order ────────────────────────────
class Order(Base):
    """
    Live / open orders only.  Row is deleted as soon as IBKR stops
    reporting it, so never relied upon for trade history.
    """
    __tablename__ = "orders"

    id        = Column(Integer, primary_key=True, index=True)
    user_id   = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    runner_id = Column(Integer, ForeignKey("runners.id", ondelete="SET NULL"), nullable=True, index=True)

    ibkr_perm_id = Column(Integer, nullable=False, unique=True, index=True)

    symbol      = Column(String, nullable=False)
    action      = Column(String, nullable=False)
    order_type  = Column(String, nullable=False)
    quantity    = Column(Float,  nullable=False)
    limit_price = Column(Float)
    stop_price  = Column(Float)

    status          = Column(String, nullable=False)
    filled_quantity = Column(Float)
    avg_fill_price  = Column(Float)

    account      = Column(String)
    created_at   = Column(DateTime(timezone=True), default=aware_utc_now)
    last_updated = Column(DateTime(timezone=True), default=aware_utc_now, onupdate=aware_utc_now)

    runner = relationship("Runner", back_populates="orders")

    # view-only helper so legacy code can still reach order.trades if the
    # row survives; has **no** FK, so nothing breaks when the order is gone.
    trades = relationship(
        "ExecutedTrade",
        primaryjoin="foreign(Order.ibkr_perm_id)==ExecutedTrade.perm_id",
        viewonly=True,
    )


# ─────────────────────── Executed trade ───────────────────────
class ExecutedTrade(Base):
    """
    Permanent, append-only record of every fill.

    • `commission` is stored per merged fill ( **positive number** → fee paid ).
    • `pnl_amount` / `pnl_percent` are written **only** on the SELL/BUY
      that reduces the open position (FIFO).
    """
    __tablename__  = "executed_trades"
    __table_args__ = (
        UniqueConstraint("perm_id", "price", name="uix_perm_id_price"),
    )

    id        = Column(Integer, primary_key=True)
    user_id   = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"),
                       nullable=False, index=True)
    runner_id = Column(Integer, ForeignKey("runners.id", ondelete="SET NULL"),
                       index=True)

    perm_id    = Column(Integer, nullable=False, index=True)
    symbol     = Column(String, index=True)
    action     = Column(String)              # BUY | SELL
    order_type = Column(String)
    quantity   = Column(Float)               # always “positive” shares
    price      = Column(Float)
    commission = Column(Float, default=0.0)  # broker fee for *this* fill (always ≥0)
    fill_time  = Column(DateTime(timezone=True), index=True)
    account    = Column(String)

    # realised profit / loss **for this closing trade**
    pnl_amount  = Column(Float)              # +10.42 / –7.25 (account currency)
    pnl_percent = Column(Float)              # +0.032  / –0.018 (vs. entry cost)

    # 👉 legacy convenience; may be None once the Order row is gone
    order = relationship(
        "Order",
        primaryjoin="foreign(ExecutedTrade.perm_id)==Order.ibkr_perm_id",
        viewonly=True,
    )
    runner = relationship("Runner", back_populates="trades")




# ─────────────────── Runner execution log ────────────────────
class RunnerExecution(Base):
    __tablename__ = "runner_executions"

    id             = Column(Integer, primary_key=True, index=True)
    user_id        = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    runner_id      = Column(Integer, ForeignKey("runners.id", ondelete="CASCADE"), nullable=False, index=True)

    cycle_seq      = Column(String(36), nullable=False, index=True)

    execution_time = Column(DateTime(timezone=True), default=aware_utc_now, nullable=False)
    perm_id        = Column(Integer, index=True)
    status         = Column(String, nullable=False)
    limit_price    = Column(Float)
    symbol         = Column(String)
    reason         = Column(String)
    details        = Column(String)
    strategy       = Column(String)

    # easy join back to the runner if needed
    runner = relationship("Runner", lazy="joined")


# ─────────────────────── Analytics / Historical ───────────────────────
class HistoricalDailyBar(Base):
    __tablename__ = "historical_daily_bars"
    __table_args__ = (
        UniqueConstraint("symbol", "date", name="uix_hist_daily_symbol_date"),
    )

    id      = Column(Integer, primary_key=True, index=True)
    symbol  = Column(String, nullable=False, index=True)
    date    = Column(DateTime(timezone=True), nullable=False, index=True)  # UTC midnight
    open    = Column(Float, nullable=False)
    high    = Column(Float, nullable=False)
    low     = Column(Float, nullable=False)
    close   = Column(Float, nullable=False)
    volume  = Column(Integer, nullable=False)


class HistoricalMinuteBar(Base):
    __tablename__ = "historical_minute_bars"
    __table_args__ = (
        UniqueConstraint("symbol", "ts", "interval_min", name="uix_hist_min_symbol_ts_interval"),
    )

    id           = Column(Integer, primary_key=True, index=True)
    symbol       = Column(String, nullable=False, index=True)
    ts           = Column(DateTime(timezone=True), nullable=False, index=True)  # UTC timestamp
    interval_min = Column(Integer, nullable=False, index=True)  # e.g., 5
    open         = Column(Float, nullable=False)
    high         = Column(Float, nullable=False)
    low          = Column(Float, nullable=False)
    close        = Column(Float, nullable=False)
    volume       = Column(Integer, nullable=False)


class AnalyticsResult(Base):
    __tablename__ = "analytics_results"
    __table_args__ = (
        UniqueConstraint("symbol", "strategy", "timeframe", name="uix_analytics_unique_combo"),
    )

    id        = Column(Integer, primary_key=True, index=True)
    symbol    = Column(String, nullable=False, index=True)
    strategy  = Column(String, nullable=False, index=True)
    timeframe = Column(String, nullable=False, index=True)  # "1d" / "5m"

    start_ts  = Column(DateTime(timezone=True))
    end_ts    = Column(DateTime(timezone=True))

    final_pnl_amount  = Column(Float)
    final_pnl_percent = Column(Float)
    trades_count      = Column(Integer)
    max_drawdown      = Column(Float)
    details           = Column(String)
    created_at        = Column(DateTime(timezone=True), default=aware_utc_now)
    updated_at        = Column(DateTime(timezone=True), default=aware_utc_now, onupdate=aware_utc_now)


# ─────────────────────── Simulation State ───────────────────────
class SimulationState(Base):
    __tablename__ = "simulation_state"
    __table_args__ = (UniqueConstraint("user_id", name="uix_sim_state_user"),)

    id         = Column(Integer, primary_key=True, index=True)
    user_id    = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    is_running = Column(String, default="false")  # "true" / "false"
    last_ts    = Column(DateTime(timezone=True))    # last simulated timestamp
    updated_at = Column(DateTime(timezone=True), default=aware_utc_now, onupdate=aware_utc_now)
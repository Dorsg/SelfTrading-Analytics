from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String, Integer, Float, DateTime, ForeignKey, UniqueConstraint, Index, JSON, Text,
    BigInteger, Numeric
)
from sqlalchemy.orm import Mapped, mapped_column
from database.db_core import Base

# ───────── Users ─────────
class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    email: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


# ───────── Runners ─────────
class Runner(Base):
    __tablename__ = "runners"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    strategy: Mapped[str] = mapped_column(String(100), index=True)
    budget: Mapped[float] = mapped_column(Float, default=0.0)
    # NEW: track remaining budget; legacy DBs may already have NOT NULL constraint
    current_budget: Mapped[float] = mapped_column(Float, default=0.0)
    stock: Mapped[str] = mapped_column(String(20), index=True)
    time_frame: Mapped[int] = mapped_column(Integer, default=5)  # minutes; 1440 = 1d
    parameters: Mapped[dict] = mapped_column(JSON, default=dict)  # ← safe default
    time_range_from: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    time_range_to:   Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    exit_strategy: Mapped[str] = mapped_column(String(100), default="hold_forever")
    activation: Mapped[str] = mapped_column(String(20), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (Index("ix_runner_user_active", "user_id", "activation"),)


# ───────── Simulation state ─────────
class SimulationState(Base):
    __tablename__ = "simulation_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), unique=True)
    is_running: Mapped[str] = mapped_column(String(5), default="false")  # "true"/"false"
    last_ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


# ───────── Historical data ─────────
class HistoricalDailyBar(Base):
    __tablename__ = "historical_daily_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low:  Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)

    __table_args__ = (UniqueConstraint("symbol", "date", name="uq_daily_symbol_date"),)


class HistoricalMinuteBar(Base):
    __tablename__ = "historical_minute_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    interval_min: Mapped[int] = mapped_column(Integer, index=True)  # 5 for 5m
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low:  Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)

    __table_args__ = (UniqueConstraint("symbol", "ts", "interval_min", name="uq_min_symbol_ts_interval"),)


# ───────── Mock Broker ─────────
class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(50), default="mock")
    cash: Mapped[float] = mapped_column(Float, default=0.0)
    equity: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_account_user_name"),)


class OpenPosition(Base):
    __tablename__ = "open_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    runner_id: Mapped[int] = mapped_column(ForeignKey("runners.id", ondelete="CASCADE"), index=True, unique=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)

    # IMPORTANT: 'account' exists in the DB and is NOT NULL; reflect it here.
    account: Mapped[str] = mapped_column(String(50), default="mock", nullable=False, index=True)

    quantity: Mapped[int] = mapped_column(Integer)
    avg_price: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    stop_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    trail_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    highest_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    runner_id: Mapped[int] = mapped_column(Integer, index=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    side: Mapped[str] = mapped_column(String(4))  # BUY/SELL
    order_type: Mapped[str] = mapped_column(String(20))  # MKT/LMT/etc
    quantity: Mapped[int] = mapped_column(Integer)
    limit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    stop_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="filled")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    filled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class ExecutedTrade(Base):
    __tablename__ = "executed_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # BROKER permanent id (live). Optional in simulation.
    perm_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)  # ← sim-safe

    # Ownership / attribution
    user_id: Mapped[Optional[int]] = mapped_column(Integer, index=True, nullable=True)
    runner_id: Mapped[Optional[int]] = mapped_column(Integer, index=True, nullable=True)

    # Instrument
    symbol: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # Timestamps
    buy_ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    sell_ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    # Prices & qty
    buy_price: Mapped[Optional[float]] = mapped_column(Numeric(18, 6), nullable=True)
    sell_price: Mapped[Optional[float]] = mapped_column(Numeric(18, 6), nullable=True)
    quantity: Mapped[Optional[float]] = mapped_column(Numeric(18, 6), nullable=True)

    # PnL (absolute and percent)
    pnl_amount: Mapped[Optional[float]] = mapped_column(Numeric(18, 6), nullable=True)
    pnl_percent: Mapped[Optional[float]] = mapped_column(Numeric(9, 6), nullable=True)

    # Strategy labeling
    strategy: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    timeframe: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)


class RunnerExecution(Base):
    __tablename__ = "runner_executions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    runner_id: Mapped[int] = mapped_column(Integer, index=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    symbol: Mapped[str] = mapped_column(String(20))
    strategy: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(50))
    reason: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # NEW: simulation cycle sequence (e.g., epoch seconds of the tick)
    cycle_seq: Mapped[int] = mapped_column(Integer, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    execution_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint("runner_id", "symbol", "strategy", "execution_time", name="uq_runner_exec_key"),
    )


class AnalyticsResult(Base):
    __tablename__ = "analytics_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    strategy: Mapped[str] = mapped_column(String(100), index=True)
    timeframe: Mapped[str] = mapped_column(String(10), index=True)

    start_ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    end_ts:   Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    final_pnl_amount: Mapped[float] = mapped_column(Float, default=0.0)
    final_pnl_percent: Mapped[float] = mapped_column(Float, default=0.0)
    trades_count: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (UniqueConstraint("symbol", "strategy", "timeframe", name="uq_result_key"),)

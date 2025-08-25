"""
sync_service.py – keep your database in lock-step with IBKR.

Flow per user:
    1. snapshot      – daily account summary (once per day)
    2. positions     – current open positions
    3. orders        – all live / recently-completed orders
    4. executions    – trade fills

Each sub-task logs its own start/end + outcome and can be unit-tested in isolation.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Sequence

from database.models import User
from database.db_manager import DBManager
import os
from ib_manager.ib_connector import IBBusinessManager
from backend.analytics.mock_broker import MockBusinessManager

log = logging.getLogger("sync-service")

# ───────────────────────── public entry-point ──────────────────────────
async def sync_account(user: User, db: DBManager, ib: IBBusinessManager) -> None:
    """
    Kick off all sync subtasks for *one* user.
    Runs tasks concurrently – the slowest network call dictates total latency.
    """
    log.info("Account-sync START user=%s(id=%d)", user.username, user.id)

    try:
        broker = ib
        if os.getenv("ANALYTICS_MODE", "false").lower() == "true" and not isinstance(ib, MockBusinessManager):
            broker = MockBusinessManager(user)
        await asyncio.gather(
            sync_snapshot(user, db, broker),
            sync_positions(user, db, broker),
            _sync_orders(user, db, broker),
            _sync_executions(user, db, broker),
        )
        log.info("Account-sync DONE  user=%s(id=%d)", user.username, user.id)

    except Exception:
        # Any subtask already logged its own stack trace – this log ties them together
        log.exception("Account-sync FAILED user=%s(id=%d)", user.username, user.id)

# ───────────────────────── helpers ──────────────────────────
async def sync_snapshot(user: User, db: DBManager, ib: IBBusinessManager) -> None:
    """
    Every call will either insert a new snapshot for today
    or update the existing one with fresh data.
    """
    t0 = time.perf_counter()
    try:
        snapshot = await ib.get_account_information()
        if snapshot:
            # This method now upserts
            db.create_account_snapshot(user_id=user.id, snapshot_data=snapshot)
            log.info(
                "Snapshot saved/updated user=%s fields=%d (%.0f ms)",
                user.username,
                len(snapshot),
                (time.perf_counter() - t0) * 1_000,
            )
        else:
            log.warning(
                "Empty snapshot received from IBKR user=%s (%.0f ms)",
                user.username,
                (time.perf_counter() - t0) * 1_000,
            )
    except Exception:
        log.exception("Snapshot subtask FAILED user=%s", user.username)

# ---------------------------------------------------------------------- #
async def sync_positions(user: User, db: DBManager, ib: IBBusinessManager) -> None:
    t0 = time.perf_counter()
    try:
        positions = ib.get_open_positions()
        db.update_open_positions(user_id=user.id, positions=positions)
        log.debug(
            "Positions synced user=%s count=%d (%.0f ms)",
            user.username,
            len(positions),
            (time.perf_counter() - t0) * 1_000,
        )
    except Exception:
        log.exception("Positions subtask FAILED user=%s", user.username)

# ---------------------------------------------------------------------- #
async def _sync_orders(user: User, db: DBManager, ib: IBBusinessManager) -> None:
    t0 = time.perf_counter()
    try:
        # Fetch IBKR’s current orders; if this raises, we skip cleaning
        orders = await ib.sync_orders_from_ibkr(user_id=user.id)
    except Exception:
        log.exception("Orders fetch FAILED – skipping sync for user=%s", user.username)
        return

    # Always call sync_orders, even if orders == [] (will clear all if truly empty)
    db.sync_orders(user.id, orders)

    log.debug(
        "Orders synced user=%s count=%d (%.0f ms)",
        user.username,
        len(orders),
        (time.perf_counter() - t0) * 1_000,
    )

# ---------------------------------------------------------------------- #
async def _sync_executions(user: User, db: DBManager, ib: IBBusinessManager) -> None:
    t0 = time.perf_counter()
    try:
        trades: Sequence[dict] = ib.sync_executed_trades(user_id=user.id)
        if trades:
            db.sync_executed_trades(trades)
        log.debug(
            "Executions synced user=%s count=%d (%.0f ms)",
            user.username,
            len(trades),
            (time.perf_counter() - t0) * 1_000,
        )
    except Exception:
        log.exception("Executions subtask FAILED user=%s", user.username)

        

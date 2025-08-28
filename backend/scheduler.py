# backend/scheduler.py
"""
Single-process scheduler driving:
    • runner decisions every 5 min
    • lightweight account sync every minute in-between

It keeps one **persistent** IB connection per user with quadratic
back-off and skips attempts entirely during the IBKR maintenance window.
"""
from __future__ import annotations

import asyncio
import logging
import random
import psutil
import os
from typing import Sequence

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.ib_manager.ib_connector import IBBusinessManager, _in_maintenance_window
from backend.ib_manager.market_data_manager import MarketDataManager


class MockMarketDataManager:
    """Mock market data manager for analytics mode - always returns market active"""
    async def is_us_market_active(self):
        return True
from database.db_manager import DBManager, get_users_with_ib_safe
from database.models import User
from sqlalchemy import text
from runner_service import run_due_runners
from sync_service import sync_account
from backend.utils import now_et
from logger_config import setup_logging
from database.db_init_safeguard import ensure_tables_exist

setup_logging()
log = logging.getLogger("Scheduler")

RUN_LOCK = asyncio.Lock()             # protects the 5‑min runner job
MINUTE_SYNC_LOCK = asyncio.Lock()     # protects the 1‑min sync job
IB_POOL: dict[int, "PoolEntry"] = {}
# Always use mock market data manager for analytics simulation
MKT = MockMarketDataManager()

# Cache users to reduce database queries
_USERS_CACHE = None
_USERS_CACHE_TIME = 0.0
_USERS_CACHE_TTL = 30.0  # Cache users for 30 seconds


async def _load_users_with_ib() -> Sequence[User]:
    """Load users with IB credentials, using cache to reduce database queries."""
    global _USERS_CACHE, _USERS_CACHE_TIME
    
    now = asyncio.get_running_loop().time()
    
    # Return cached users if cache is still valid
    if _USERS_CACHE is not None and (now - _USERS_CACHE_TIME) < _USERS_CACHE_TTL:
        return _USERS_CACHE
    
    # Cache is stale or empty, fetch fresh data
    users = await asyncio.to_thread(get_users_with_ib_safe)
    _USERS_CACHE = users
    _USERS_CACHE_TIME = now
    
    return users


# ───────────────────────── connection pool entry ─────────────────────────
class PoolEntry:
    def __init__(self, user):
        self.user = user
        self.manager = IBBusinessManager(user, component="scheduler")
        self.backoff_until = 0.0
        self.backoff_pow = 1  # 1,2,4,8 … capped at 256 s
        self.last_health_check = 0.0  # Track last health check time
        self.health_check_interval = 60.0  # Only health check once per minute
        self.created_at = asyncio.get_running_loop().time()  # Track when entry was created
        self.last_used = asyncio.get_running_loop().time()  # Track last usage
        self.max_age = 3600.0  # Maximum age before forced cleanup (1 hour)
        self.idle_timeout = 1800.0  # Idle timeout (30 minutes)

    async def ensure_connected(self) -> bool:
        """Return True when a live IB connection is ready to use."""
        # Analytics simulation - always return True (no IB connection needed)
        return True

    def should_cleanup(self, now: float) -> bool:
        """Check if this pool entry should be cleaned up due to age or idle time."""
        age = now - self.created_at
        idle_time = now - self.last_used
        
        # Force cleanup if too old
        if age > self.max_age:
            log.debug("Pool entry for %s is too old (%.0fs > %.0fs)", 
                     self.user.username, age, self.max_age)
            return True
            
        # Force cleanup if idle too long
        if idle_time > self.idle_timeout:
            log.debug("Pool entry for %s has been idle too long (%.0fs > %.0fs)", 
                     self.user.username, idle_time, self.idle_timeout)
            return True
            
        return False

    def disconnect(self) -> None:
        self.manager.disconnect()

    async def cleanup(self) -> None:
        """Clean up resources and remove from pool."""
        self.disconnect()
        if self.user.id in IB_POOL:
            del IB_POOL[self.user.id]


async def _entry(user) -> PoolEntry:
    entry = IB_POOL.get(user.id)
    if entry is None:
        entry = IB_POOL[user.id] = PoolEntry(user)
        log.debug("Created new pool entry for user %s", user.username)
    else:
        # Check if existing entry should be replaced due to staleness
        now = asyncio.get_running_loop().time()
        if entry.should_cleanup(now):
            log.info("Replacing stale pool entry for user %s", user.username)
            await entry.cleanup()
            entry = IB_POOL[user.id] = PoolEntry(user)
    return entry


# ───────────────────────── per-user task runner ─────────────────────────
async def _process_user(user, *, do_runners: bool, do_sync: bool) -> None:
    entry = await _entry(user)
    if not await entry.ensure_connected():
        return

    ib = entry.manager
    try:
        async with DBManager() as db:
            if do_runners and await MKT.is_us_market_active():
                await run_due_runners(user, db, ib)
            if do_sync:
                await sync_account(user, db, ib)
    except Exception:
        log.exception("Error while handling user %s", user.username)
        entry.disconnect()  # force fresh connect next time


# ───────────────────────── scheduled jobs ─────────────────────────
async def runners_every_5min() -> None:
    async with RUN_LOCK:  # avoid overlap
        log_memory_usage("5min task start")
        try:
            users: Sequence[User] = await _load_users_with_ib()
        except Exception:
            log.exception("runners_every_5min: failed to load users_with_ib; skipping this tick")
            return

        await asyncio.gather(*[
            _process_user(u, do_runners=True, do_sync=True) for u in users
        ])
        log_memory_usage("5min task end")


async def minute_sync() -> None:
    # Skip if the 5-minute runner task is currently running or just ran
    if RUN_LOCK.locked():
        log.debug("minute_sync skipped: 5-minute runner task is active")
        return
    
    # Skip on minutes divisible by 5 to avoid overlap with 5-minute task
    current_minute = now_et().minute
    if current_minute % 5 == 0:
        log.debug("minute_sync skipped: 5-minute task scheduled for this minute")
        return

    # Don't start a new minute sync if the previous one is still running
    if MINUTE_SYNC_LOCK.locked():
        log.debug("minute_sync skipped: previous run still in progress")
        return

    async with MINUTE_SYNC_LOCK:
        try:
            users: Sequence[User] = await _load_users_with_ib()
        except Exception:
            log.exception("minute_sync: failed to load users_with_ib; skipping this tick")
            return

        if not users:
            log.debug("minute_sync: no users with IB credentials found")
            return

        log.info("minute_sync: processing %d users", len(users))
        await asyncio.gather(*[
            _process_user(u, do_runners=False, do_sync=True) for u in users
        ])


def log_memory_usage(context: str = "") -> None:
    """Log current memory usage for monitoring."""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        # Convert bytes to MB
        rss_mb = memory_info.rss / 1024 / 1024
        vms_mb = memory_info.vms / 1024 / 1024
        
        log.info("Memory usage%s: RSS=%.1fMB VMS=%.1fMB (%.1f%% of system) Pool=%d entries", 
                f" ({context})" if context else "", rss_mb, vms_mb, memory_percent, len(IB_POOL))
                
    except Exception as e:
        log.warning("Failed to get memory usage: %s", e)

async def monitor_database_health() -> None:
    """Monitor database health to detect potential wipeout issues."""
    try:
        with DBManager() as db:
            # Check table existence and basic counts
            tables_info = {}
            
            # Check users table
            try:
                user_count = db.db.execute(text("SELECT COUNT(*) FROM users")).scalar()
                tables_info['users'] = user_count
            except Exception as e:
                tables_info['users'] = f"ERROR: {e}"
                log.error("Failed to query users table: %s", e)
            
            # Check runners table
            try:
                # Combine runner queries 
                runner_result = db.db.execute(text(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE activation = 'active') FROM runners"
                )).fetchone()
                runner_count, active_runner_count = runner_result
                tables_info['runners'] = f"total={runner_count}, active={active_runner_count}"
            except Exception as e:
                tables_info['runners'] = f"ERROR: {e}"
                log.error("Failed to query runners table: %s", e)
            
            # Check account_snapshots table
            try:
                # Combine snapshot queries
                snapshot_result = db.db.execute(text(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '24 hours') FROM account_snapshots"
                )).fetchone()
                snapshot_count, recent_snapshots = snapshot_result
                tables_info['snapshots'] = f"total={snapshot_count}, recent_24h={recent_snapshots}"
            except Exception as e:
                tables_info['snapshots'] = f"ERROR: {e}"
                log.error("Failed to query snapshots table: %s", e)
            
            # Check open_positions table
            try:
                positions_count = db.db.execute(text("SELECT COUNT(*) FROM open_positions")).scalar()
                tables_info['positions'] = positions_count
            except Exception as e:
                tables_info['positions'] = f"ERROR: {e}"
                log.error("Failed to query positions table: %s", e)
            
            # Log comprehensive database status
            log.info("Database health check: %s", tables_info)
            
            # Check for critical issues
            if isinstance(tables_info.get('users'), int) and tables_info['users'] == 0:
                log.critical("DATABASE ALERT: Users table is empty! Potential wipeout detected!")
            
            if 'ERROR' in str(tables_info.get('users', '')):
                log.critical("DATABASE ALERT: Cannot access users table! %s", tables_info['users'])
            
            # Check database connectivity
            try:
                db.db.execute(text("SELECT 1")).scalar()
                log.debug("Database connectivity check: OK")
            except Exception as e:
                log.critical("DATABASE ALERT: Connectivity test failed: %s", e)
                
    except Exception as e:
        log.critical("DATABASE HEALTH CHECK FAILED: %s", e)

async def cleanup_connections() -> None:
    """Periodically cleanup stale connections from the pool."""
    try:
        log_memory_usage("before cleanup")
        now = asyncio.get_running_loop().time()
        cleanup_count = 0
        
        # Get current valid users
        current_users = {user.id for user in await _load_users_with_ib()}
        
        # Create list of user_ids to avoid modifying dict during iteration
        user_ids = list(IB_POOL.keys())
        log.debug("Checking %d pool entries for cleanup", len(user_ids))
        
        for user_id in user_ids:
            entry = IB_POOL.get(user_id)  # Use get() in case entry was removed
            if entry is None:
                continue
                
            should_cleanup = False
            cleanup_reason = ""
            
            # Check if user no longer exists
            if user_id not in current_users:
                should_cleanup = True
                cleanup_reason = "user no longer exists"
            # Check if connection is dead
            elif not entry.manager.ib.isConnected():
                should_cleanup = True
                cleanup_reason = "disconnected"
            # Check if entry should be cleaned up due to age/idle
            elif entry.should_cleanup(now):
                should_cleanup = True
                cleanup_reason = "stale (age/idle)"
            
            if should_cleanup:
                log.info("Cleaning up pool entry for user %d (%s)", user_id, cleanup_reason)
                await entry.cleanup()
                cleanup_count += 1
        
        if cleanup_count > 0:
            log.info("Cleaned up %d stale pool entries", cleanup_count)
            log_memory_usage("after cleanup")
        else:
            log.debug("No pool entries needed cleanup")

    except Exception:
        log.exception("cleanup_connections failed")


# ───────────────────────────── main loop ─────────────────────────────
async def main() -> None:
    # Ensure database integrity on startup
    log.info("Performing database integrity check on startup...")
    try:
        ensure_tables_exist()
        log.info("Database integrity check passed")
    except Exception as e:
        log.error(f"Database integrity check failed: {e}")
        # Continue anyway - the retry logic will handle it
    
    sched = AsyncIOScheduler(timezone="America/New_York")

    # runners every 5 minutes
    sched.add_job(
        runners_every_5min,
        CronTrigger.from_crontab("*/5 * * * *", timezone="America/New_York"),
        max_instances=1,
        coalesce=True,
        misfire_grace_time=90,  # tolerate a short stall while DB is down
    )

    # light sync every minute (but the 5‑min job already does it on its tick)
    sched.add_job(
        minute_sync,
        CronTrigger.from_crontab("* * * * *", timezone="America/New_York"),
        # Allow a second instance to start; our own MINUTE_SYNC_LOCK prevents overlap
        # and will make the second instance exit immediately and quietly.
        max_instances=2,
        coalesce=True,
        misfire_grace_time=30,
    )

    # cleanup stale connections every 30 minutes (reduced frequency)
    sched.add_job(
        cleanup_connections,
        CronTrigger.from_crontab("*/30 * * * *", timezone="America/New_York"),
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    # database health monitoring every 30 minutes (reduced for performance)
    sched.add_job(
        monitor_database_health,
        CronTrigger.from_crontab("*/30 * * * *", timezone="America/New_York"),
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )

    sched.start()
    log.info("Scheduler up – runners every 5 min, sync every min, cleanup every 30 min, DB health every 30 min")
    
    # Perform initial database health check
    log.info("Performing initial database health check...")
    await monitor_database_health()
    log_memory_usage("startup")
    
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

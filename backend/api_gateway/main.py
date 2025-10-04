from __future__ import annotations

import asyncio
import logging
import os
from fastapi import FastAPI
from datetime import datetime, timezone
from fastapi.middleware.cors import CORSMiddleware

from api_gateway.routes import auth_routes, analytics_routes
from logger_config import setup_logging as setup_analytics_logging

# ───────── database initialisation ─────────
from sqlalchemy import inspect, text
from database.db_core import engine, wait_for_db_ready
from database.models import Base
from backend.database.init_db import _apply_light_migrations  # reuse the tiny migration

app = FastAPI()

def _configure_logging() -> None:
    setup_analytics_logging()
    # Honor LOG_LEVEL instead of forcing INFO
    lvl = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    for name in ["uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"]:
        lg = logging.getLogger(name)
        lg.setLevel(lvl)
        lg.propagate = True

async def _bootstrap_everything() -> None:
    """
    1) Ensure historical data imported (idempotent)
    2) Ensure mock user & account exist
    3) Ensure runners exist for each symbol for both strategies/timeframes
    4) Optionally auto-start simulation (SIM_AUTO_START=1) WITHOUT resetting last_ts backwards
    """
    log = logging.getLogger("api-gateway")
    try:
        # Make sure the DB is actually reachable before any imports/selects
        await asyncio.get_running_loop().run_in_executor(None, wait_for_db_ready)
    except Exception:
        log.exception("Database readiness check failed")
        return

    # Import daily/minute data if needed (idempotent)
    try:
        from backend.analytics_importer import import_sqlite
        await asyncio.get_running_loop().run_in_executor(None, import_sqlite)
        log.info("Historical import ensured (idempotent).")
    except Exception:
        log.exception("Historical import failed")

    try:
        from database.db_manager import DBManager
        from sqlalchemy import select, func
        from database.models import HistoricalDailyBar, HistoricalMinuteBar, SimulationState

        with DBManager() as db:
            user = db.get_or_create_user(
                username="analytics",
                email="analytics@example.com",
                password="analytics",
            )
            start_cash = float(os.getenv("SIM_START_CASH", "10000000"))
            db.ensure_account(user_id=user.id, name="mock", cash=start_cash)

            # Bootstrap runners (idempotent create attempts)
            with engine.connect() as conn:
                syms = [r[0] for r in conn.execute(
                    select(HistoricalDailyBar.symbol).distinct().order_by(HistoricalDailyBar.symbol.asc())
                ).fetchall()]

            if syms:
                try:
                    from backend.strategies.factory import list_available_strategy_keys as _list_strats
                    strategies = _list_strats()
                except Exception:
                    strategies = ["chatgpt_5_strategy", "chatgpt_5_ultra_strategy", "grok_4_strategy", "gemini_2_5_pro_strategy", "claude_4_5_sonnet_strategy", "deepseek_v3_1_strategy"]
                timeframes = [5, 1440]
                created = 0
                for sym in syms:
                    for strat in strategies:
                        for tf in timeframes:
                            name = f"{sym}-{strat}-{('5m' if tf == 5 else '1d')}"
                            try:
                                db.create_runner(
                                    user_id=user.id,
                                    data={
                                        "name": name,
                                        "strategy": strat,
                                        "budget": start_cash * 10,
                                        "stock": sym,
                                        "time_frame": tf,
                                        "parameters": {},
                                        "exit_strategy": "hold_forever",
                                        "activation": "active",
                                    },
                                )
                                created += 1
                            except Exception:
                                try:
                                    db.db.rollback()
                                except Exception:
                                    pass
                log.info("Bootstrap runners ensured; created=%d", created)
            else:
                log.warning("No symbols found; runners will be created later when data appears.")

            # Forward-only initialization of SimulationState.last_ts
            with engine.connect() as conn:
                min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()

            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
            if not st:
                st = SimulationState(user_id=user.id, is_running="false", last_ts=None)
                db.db.add(st)
                db.db.flush()

            if min_ts and max_ts:
                warmup_bars = int(os.getenv("SIM_WARMUP_BARS", os.getenv("WARMUP_BARS", "30")))
                step_sec = int(os.getenv("SIM_STEP_SECONDS", "300"))  # default 5m

                min_epoch = int((min_ts if min_ts.tzinfo else min_ts.replace(tzinfo=timezone.utc)).timestamp())
                max_epoch = int((max_ts if max_ts.tzinfo else max_ts.replace(tzinfo=timezone.utc)).timestamp())
                desired_start_epoch = min(min_epoch + warmup_bars * step_sec, max_epoch)

                existing_epoch = (
                    int((st.last_ts if (st.last_ts and st.last_ts.tzinfo) else (st.last_ts or datetime.fromtimestamp(0, tz=timezone.utc)).replace(tzinfo=timezone.utc)).timestamp())
                    if st.last_ts else None
                )
                new_epoch = desired_start_epoch if existing_epoch is None else max(existing_epoch, desired_start_epoch)
                if existing_epoch != new_epoch:
                    st.last_ts = datetime.fromtimestamp(new_epoch, tz=timezone.utc)
                    log.info("SimulationState initialized (forward-only) to %s", st.last_ts.isoformat())
                else:
                    if st.last_ts:
                        log.info("SimulationState kept at %s (forward-only).", st.last_ts.isoformat())
            else:
                log.warning("No minute bars present; SimulationState left unchanged.")

            db.db.commit()

    except Exception:
        log.exception("Bootstrap user/account/runners/state failed")

    # Respect auto-start ONLY by setting state; do NOT trigger a runtime start here.
    # Triggering the scheduler directly during API bootstrap caused confusing "auto-start"
    # behavior where the sim began without an explicit user request. Keep this opt-in
    # by only writing the desired state and requiring an explicit API call to actually
    # advance time if needed.
    if os.getenv("SIM_AUTO_START", "0") == "1":
        log.info("SIM_AUTO_START=1 detected: simulation state may be set to running, but auto-start call is suppressed. Use /api/analytics/simulation/start to start processing.")


@app.on_event("startup")
async def _init_db(force: bool = False) -> None:
    _configure_logging()
    logger = logging.getLogger("api-gateway")

    # Ensure DB is reachable before we inspect/touch it
    try:
        await asyncio.get_running_loop().run_in_executor(None, wait_for_db_ready)
    except Exception as exc:
        logger.exception("DB not ready at startup: %s", exc)
        # We still continue; background tasks will retry

    try:
        inspector = inspect(engine)
        allow_create = os.getenv("DB_ALLOW_AUTO_CREATE_TABLES", "true").lower() == "true"
        if not inspector.has_table("users"):
            if allow_create:
                Base.metadata.create_all(bind=engine)
                logger.info("Created database tables (auto-create enabled)")
            else:
                logger.warning("Tables missing and auto-create disabled.")
        _apply_light_migrations()
    except Exception as exc:
        logger.exception("DB initialization failed: %s", exc)

    asyncio.create_task(_bootstrap_everything())

    try:
        external = os.getenv("EXTERNAL_SCHEDULER", "0") == "1"
        if not external:
            import backend.analytics.sim_scheduler as sim_scheduler
            asyncio.create_task(sim_scheduler.main())
            logger.info("Internal analytics scheduler started (background).")
        else:
            logger.info("External scheduler configured; internal scheduler not started.")
    except Exception:
        logger.exception("Failed to start internal analytics scheduler")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

app.include_router(auth_routes.router,      prefix="/api")
app.include_router(analytics_routes.router, prefix="/api")

@app.post('/api/analytics/simulation/reset')
async def _bridge_reset():
    # Prefer the router reset if present
    try:
        from api_gateway.routes.analytics_routes import api_reset_simulation as rr
        return rr()
    except Exception:
        pass
    # Fallback to app.reset_sim if available
    try:
        from backend.api_gateway.app import reset_sim, ResetRequest
        return reset_sim(ResetRequest())
    except Exception:
        return {"ok": False, "error": "reset unavailable"}

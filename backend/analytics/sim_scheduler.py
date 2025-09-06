from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta, date, time

from logger_config import setup_logging  # ensure file handlers & levels
from database.db_manager import DBManager
from database.models import SimulationState
from database.db_core import engine
from backend.analytics.runner_service import RunnerService
from backend.ib_manager.market_data_manager import MarketDataManager
from backend.universe import UniverseManager

# Configure logging for this process
setup_logging()
log = logging.getLogger("AnalyticsScheduler")

PACE_FILE = "/tmp/sim_auto_advance.json"
HEARTBEAT_FILE = "/tmp/sim_scheduler.heartbeat"

# ──────────────────────────────────────────────────────────────────────────────
# Tunables (sane defaults; all overridable via env)
# ──────────────────────────────────────────────────────────────────────────────

# Minimum fully-completed bars strategies expect (RSI14/MA/Fib/Donchian windows).
# Keep this at 21 unless you change strategy periods materially.
MIN_REQUIRED_BARS = int(os.getenv("SIM_MIN_REQUIRED_BARS", "21"))

# Default: use the next real 5m market candle as our step; still keep this for warmup math.
def _step_seconds() -> int:
    return int(os.getenv("SIM_STEP_SECONDS", "300"))  # 5 minutes per tick


def _read_pace_seconds() -> float:
    try:
        if os.path.exists(PACE_FILE):
            with open(PACE_FILE, "r") as f:
                data = json.load(f)
                if not data.get("enabled", True):
                    # If disabled, sleep a little so we don't hot-spin the loop
                    return 0.5
                pace = float(data.get("pace_seconds", 0.0))
                return max(0.0, pace)
    except Exception:
        pass
    return float(os.getenv("SIM_PACE_SECONDS", "0"))  # default: run at full speed


def _warmup_bars_default() -> int:
    """
    Bars to skip from the global min 5m timestamp so strategies have enough data.
    Default = 50 (> 21) so first indicator windows are fully stable.
    Override with SIM_WARMUP_BARS or WARMUP_BARS.
    """
    return int(os.getenv("SIM_WARMUP_BARS", os.getenv("WARMUP_BARS", "50")))


def _daily_warmup_days_default() -> int:
    """
    Extra guard for DAILY runners: start the whole sim only after at least this
    many daily candles exist globally. Default 30 days. Override via:
      SIM_DAILY_WARMUP_DAYS or DAILY_WARMUP_DAYS
    """
    return int(os.getenv("SIM_DAILY_WARMUP_DAYS", os.getenv("DAILY_WARMUP_DAYS", "30")))


def _session_warmup_bars_default() -> int:
    """
    Number of 5m bars to have **after NYSE open** in the *current day* before we run strategies.
    We require MIN_REQUIRED_BARS completed bars **plus one** to avoid off-by-one when a bar
    is still forming or the data provider is mid-commit.
    Default 22; override via SIM_SESSION_WARMUP_BARS or SESSION_WARMUP_BARS.
    """
    fallback = max(MIN_REQUIRED_BARS + 1, 22)
    return int(os.getenv("SIM_SESSION_WARMUP_BARS", os.getenv("SESSION_WARMUP_BARS", str(fallback))))


def _ny_open_epoch_for_day(dt_utc: datetime) -> int:
    """
    Return the UTC epoch for 09:30 ET on the ET calendar date of dt_utc.
    """
    dt_utc = dt_utc if dt_utc.tzinfo else dt_utc.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo  # type: ignore
        ny = ZoneInfo("America/New_York")
        et = dt_utc.astimezone(ny)
        et_day = et.date()
        open_et = datetime(et_day.year, et_day.month, et_day.day, 9, 30, tzinfo=ny)
        open_utc = open_et.astimezone(timezone.utc)
        return int(open_utc.timestamp())
    except Exception:
        # Conservative fallback if zoneinfo unavailable: 13:30 UTC ≈ 09:30 ET (no DST correction)
        approx = dt_utc.replace(hour=13, minute=30, second=0, microsecond=0, tzinfo=timezone.utc)
        return int(approx.timestamp())


async def _heartbeat() -> None:
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())
    except Exception:
        pass


async def _advance_one_tick(rs: RunnerService, ts: int) -> tuple[int, dict]:
    # NOTE: this is now controlled by session-aware stepping outside; we keep the signature
    stats = await rs.run_tick(datetime.fromtimestamp(ts, tz=timezone.utc))
    return ts, stats  # next epoch chosen separately


def _ts(dt: datetime | None) -> int | None:
    if not dt:
        return None
    return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp())


async def main() -> None:
    # Ensure tiny migrations also run when the scheduler/runner is started without the API process.
    try:
        from backend.database.init_db import _apply_light_migrations
        _apply_light_migrations()
    except Exception:
        log.exception("Failed to apply light migrations at scheduler startup")

    tick_log_every = max(1, int(os.getenv("TICK_LOG_EVERY", "1")))
    boundary_refresh_ticks = int(os.getenv("SIM_BOUNDARY_REFRESH_TICKS", "0"))  # 0 = never refresh

    rs = RunnerService()
    mkt = MarketDataManager()

    # Decide the session clock symbol up-front (resilient)
    step_sec = _step_seconds()
    tf_min = step_sec // 60
    requested_clock = os.getenv("SIM_REFERENCE_CLOCK_SYMBOL", "SPY").upper()
    clock_sym = requested_clock
    if not mkt.has_minute_bars(clock_sym, tf_min):
        picked = mkt.pick_reference_symbol(interval_min=tf_min)
        if picked:
            log.info(
                "Session clock '%s' unavailable at %dm — switching to '%s'. "
                "Provide SIM_REFERENCE_CLOCK_SYMBOL to override.",
                requested_clock, tf_min, picked
            )
            clock_sym = picked
        else:
            log.warning("No obvious clock symbol found for %dm. Will rely on GLOBAL fallback (any symbol).", tf_min)

    log.info(
        "Simulation scheduler loop started. LOG_LEVEL=%s  tick_log_every=%s  boundary_refresh_ticks=%s  clock_symbol=%s",
        os.getenv("LOG_LEVEL", "DEBUG"),
        tick_log_every,
        boundary_refresh_ticks,
        (clock_sym or "<global>"),
    )

    # Log 5m data boundaries once at startup (and fetch daily min date)
    min_5m_dt = max_5m_dt = min_daily_dt = None
    try:
        from sqlalchemy import select, func
        from database.models import HistoricalMinuteBar, HistoricalDailyBar
        with DBManager() as db:
            with db.db.bind.connect() as conn:  # type: ignore[attr-defined]
                min_5m_dt = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                max_5m_dt = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                min_daily_dt = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
        log.info("Historical 5m data range: start=%s end=%s", min_5m_dt, max_5m_dt)
    except Exception:
        log.exception("Failed to log historical range at startup")

    cached_min_ts = min_5m_dt
    cached_max_ts = max_5m_dt
    cached_min_daily = min_daily_dt

    state_epoch: int | None = None  # seconds since epoch, UTC
    tick = 0
    while True:
        pace = _read_pace_seconds()
        try:
            await _heartbeat()

            from sqlalchemy import select, func, text
            from database.models import HistoricalMinuteBar, HistoricalDailyBar

            with DBManager() as db:
                user = db.get_user_by_username("analytics")
                if not user:
                    await asyncio.sleep(1.0)
                    continue

                uid = int(getattr(user, "id"))
                st = db.db.query(SimulationState).filter(SimulationState.user_id == uid).first()
                if not st:
                    st = SimulationState(user_id=uid, is_running="false")
                    db.db.add(st)
                    db.db.commit()
                    await asyncio.sleep(1.0)
                    continue

                if str(st.is_running).lower() not in {"true", "1"}:
                    if tick % 10 == 0:
                        log.debug("Idle: simulation not running")
                    await asyncio.sleep(1.0)
                    tick += 1
                    continue

                if (
                    cached_min_ts is None or
                    cached_max_ts is None or
                    (boundary_refresh_ticks > 0 and tick % boundary_refresh_ticks == 0)
                ):
                    with db.db.bind.connect() as conn:  # type: ignore[attr-defined]
                        cached_min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                        cached_max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                        cached_min_daily = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()

                if not cached_min_ts or not cached_max_ts:
                    log.warning("No minute bars present; pausing run.")
                    await asyncio.sleep(1.0)
                    tick += 1
                    continue

                step_sec = _step_seconds()
                warmup_bars = _warmup_bars_default()
                daily_warmup_days = _daily_warmup_days_default()
                session_warmup_bars = _session_warmup_bars_default()

                min_epoch = int(cached_min_ts.replace(tzinfo=timezone.utc).timestamp())
                max_epoch = int(cached_max_ts.replace(tzinfo=timezone.utc).timestamp())

                base_start_epoch = min_epoch + warmup_bars * step_sec

                if cached_min_daily:
                    min_daily_epoch = int(cached_min_daily.replace(tzinfo=timezone.utc).timestamp())
                    daily_guard_epoch = min_daily_epoch + daily_warmup_days * 86400
                    desired_start = max(base_start_epoch, daily_guard_epoch)
                else:
                    desired_start = base_start_epoch

                aligned_dt = mkt.get_next_session_ts(
                    datetime.fromtimestamp(desired_start, tz=timezone.utc),
                    interval_min=step_sec // 60,
                    reference_symbol=clock_sym if clock_sym else None,
                )
                if aligned_dt is None:
                    aligned_dt = mkt.get_next_session_ts_global(
                        datetime.fromtimestamp(desired_start, tz=timezone.utc),
                        interval_min=step_sec // 60,
                    )
                if aligned_dt is not None:
                    desired_start = min(int(aligned_dt.timestamp()), max_epoch)

                db_epoch = _ts(st.last_ts)

                if state_epoch is None:
                    base = db_epoch if (db_epoch is not None) else desired_start
                    base_dt = datetime.fromtimestamp(min(max(base, desired_start), max_epoch), tz=timezone.utc)
                    next_dt = mkt.get_next_session_ts(
                        base_dt,
                        interval_min=step_sec // 60,
                        reference_symbol=clock_sym if clock_sym else None,
                    )
                    if next_dt is None:
                        next_dt = mkt.get_next_session_ts_global(base_dt, interval_min=step_sec // 60)
                    if next_dt is None:
                        st.is_running = "false"
                        db.db.commit()
                        log.info("No session ticks available at/after %s. Stopping.", base_dt.isoformat())
                        await asyncio.sleep(1.0)
                        tick += 1
                        continue

                    state_epoch = int(next_dt.timestamp())

                    open_epoch = _ny_open_epoch_for_day(next_dt)
                    warmup_epoch = open_epoch + session_warmup_bars * step_sec
                    if state_epoch < warmup_epoch <= max_epoch:
                        log.debug(
                            "Session warmup: skipping to %s after NY open (%d bars).",
                            datetime.fromtimestamp(warmup_epoch, tz=timezone.utc).isoformat(),
                            session_warmup_bars,
                        )
                        state_epoch = warmup_epoch

                    target_dt = datetime.fromtimestamp(state_epoch, tz=timezone.utc)
                    db.db.execute(
                        text(
                            "UPDATE simulation_state "
                            "   SET last_ts = CASE WHEN last_ts IS NULL OR last_ts < :ts THEN :ts ELSE last_ts END "
                            " WHERE user_id = :uid"
                        ),
                        {"ts": target_dt, "uid": uid},
                    )
                    db.db.commit()

                    st.last_ts = target_dt
                    log.info(
                        "Initialized simulation clock: db_epoch=%s desired_start(aligned)=%s -> start_at=%s (clock=%s)",
                        db_epoch, desired_start, st.last_ts.isoformat(), (clock_sym or "<global>")
                    )
                else:
                    db_epoch = _ts(
                        db.db.query(SimulationState.last_ts)
                        .filter(SimulationState.user_id == uid)
                        .scalar()
                    )

                    if db_epoch is not None and (db_epoch + step_sec) < state_epoch:
                        log.warning(
                            "Detected DB last_ts regression (%s < %s). Overwriting with monotonic clock.",
                            db_epoch, state_epoch
                        )
                        target_dt = datetime.fromtimestamp(state_epoch, tz=timezone.utc)
                        db.db.execute(
                            text(
                                "UPDATE simulation_state "
                                "   SET last_ts = CASE WHEN last_ts IS NULL OR last_ts < :ts THEN :ts ELSE last_ts END "
                                " WHERE user_id = :uid"
                            ),
                            {"ts": target_dt, "uid": uid},
                        )
                        db.db.commit()

                    if db_epoch is not None and db_epoch > state_epoch + step_sec:
                        log.info(
                            "Adopting DB fast-forward: state_epoch=%s -> db_epoch=%s",
                            state_epoch, db_epoch
                        )
                        jump_dt = datetime.fromtimestamp(db_epoch, tz=timezone.utc)
                        next_dt = mkt.get_next_session_ts(
                            jump_dt,
                            interval_min=step_sec // 60,
                            reference_symbol=clock_sym if clock_sym else None,
                        )
                        if next_dt is None:
                            next_dt = mkt.get_next_session_ts_global(jump_dt, interval_min=step_sec // 60)
                        if next_dt is None:
                            st.is_running = "false"
                            db.db.commit()
                            log.info("No session ticks available at/after %s. Stopping.", jump_dt.isoformat())
                            await asyncio.sleep(1.0)
                            tick += 1
                            continue
                        state_epoch = int(next_dt.timestamp())

                if state_epoch >= max_epoch:
                    st.is_running = "false"
                    db.db.commit()
                    log.info("Reached end of historical data (%s). Stopping simulation.", cached_max_ts.isoformat())
                    await asyncio.sleep(1.0)
                    tick += 1
                    continue

                cur_dt = datetime.fromtimestamp(state_epoch, tz=timezone.utc)
                cur_ts, stats = await _advance_one_tick(rs, state_epoch)

                next_dt = mkt.get_next_session_ts(
                    cur_dt,
                    interval_min=_step_seconds() // 60,
                    reference_symbol=clock_sym if clock_sym else None,
                )
                if next_dt is None:
                    next_dt = mkt.get_next_session_ts_global(cur_dt, interval_min=_step_seconds() // 60)
                if next_dt is None:
                    st.is_running = "false"
                    db.db.commit()
                    log.info("No further session ticks after %s. Stopping simulation.", cur_dt.isoformat())
                    await asyncio.sleep(1.0)
                    tick += 1
                    continue

                state_epoch = int(next_dt.timestamp())

                target_dt = datetime.fromtimestamp(state_epoch, tz=timezone.utc)
                db.db.execute(
                    text(
                        "UPDATE simulation_state "
                        "   SET last_ts = CASE WHEN last_ts IS NULL OR last_ts < :ts THEN :ts ELSE last_ts END "
                        " WHERE user_id = :uid"
                    ),
                    {"ts": target_dt, "uid": uid},
                )
                db.db.commit()
                st.last_ts = target_dt

                start_epoch = int(desired_start)
                total_span = max(1, max_epoch - start_epoch)
                done_span = max(0, cur_ts - start_epoch)
                pct = max(0.0, min(100.0, (done_span / total_span) * 100.0))

                if tick % tick_log_every == 0:
                    as_of_s = datetime.fromtimestamp(cur_ts, tz=timezone.utc).isoformat()
                    next_s = datetime.fromtimestamp(state_epoch, tz=timezone.utc).isoformat()
                    pace_label = "full-speed" if pace <= 0 else f"{pace:.2f}s delay"
                    log.debug(
                        "TICK #%d as_of=%s → next=%s | runners: processed=%d buys=%d sells=%d "
                        "no_action=%d skipped_no_data=%d skipped_no_budget=%d errors=%d | "
                        "progress=%.4f%% (session-aware; clock=%s; pace=%s)",
                        tick,
                        as_of_s,
                        next_s,
                        int(stats.get("processed", 0)),
                        int(stats.get("buys", 0)),
                        int(stats.get("sells", 0)),
                        int(stats.get("no_action", 0)),
                        int(stats.get("skipped_no_data", 0)),
                        int(stats.get("skipped_no_budget", 0)),
                        int(stats.get("errors", 0)),
                        pct,
                        (clock_sym or "<global>"),
                        pace_label,
                    )

                await asyncio.sleep(pace if pace > 0 else 0)
                tick += 1

        except Exception:
            log.exception("Scheduler loop error")
            await asyncio.sleep(0.5)
            tick += 1

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

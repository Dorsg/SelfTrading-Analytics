from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta, date

# Ensure the project root is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from backend.logger_config import setup_logging  # ensure file handlers & levels
from database.db_manager import DBManager
from database.models import SimulationState
from database.db_core import engine, wait_for_db_ready
from backend.analytics.runner_service import RunnerService
from backend.ib_manager.market_data_manager import MarketDataManager
from backend.universe import UniverseManager

# Configure logging for this process
setup_logging()
log = logging.getLogger("AnalyticsScheduler")

PACE_FILE = "/tmp/sim_auto_advance.json"
HEARTBEAT_FILE = "/tmp/sim_scheduler.heartbeat"
SNAPSHOT_FILE = os.getenv("SIM_PROGRESS_SNAPSHOT", "/app/data/sim_last_progress.json")
WATCHDOG_IDLE_SECONDS = int(os.getenv("SIM_WATCHDOG_IDLE_SEC", "600"))  # restart if no progress

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


def _compute_eta(cur_ts: int, pace: float, total_span: int, done_span: int, step_sec: int, tick_times: list) -> dict:
    """Computes estimated finish time and returns a dictionary with ETA fields."""
    try:
        pace_seconds = pace if pace > 0 else None
        est_secs = None
        remaining = max(0, total_span - done_span)
        remaining_ticks = remaining / step_sec if step_sec > 0 else None
        pct = max(0.0, min(100.0, (done_span / total_span) * 100.0)) if total_span > 0 else 0

        if remaining_ticks is None:
            return {}

        # 1) If explicit pacing is configured, use it
        if pace_seconds and pct < 100.0:
            est_secs = int(remaining_ticks * pace_seconds)

        # 2) Else infer from observed tick wall-times
        if est_secs is None:
            try:
                if len(tick_times) >= 2:
                    intervals = [t2 - t1 for t1, t2 in zip(list(tick_times)[:-1], list(tick_times)[1:])]
                    if intervals:
                        avg = sum(intervals) / len(intervals)
                        est_secs = int(remaining_ticks * avg)
            except Exception:
                est_secs = None

        if est_secs is None:
            return {}

        payload = {
            "estimated_finish_seconds": est_secs,
            "estimated_finish_iso": datetime.fromtimestamp(cur_ts + est_secs, tz=timezone.utc).isoformat()
        }
        # Human-friendly string for convenience
        hh = est_secs // 3600
        mm = (est_secs % 3600) // 60
        ss = est_secs % 60
        if hh > 0:
            payload["estimated_finish"] = f"~{hh}h {mm}m"
        else:
            payload["estimated_finish"] = f"~{mm}m {ss}s"
        return payload
    except Exception:
        return {}


async def _heartbeat() -> None:
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())
    except Exception:
        pass


def _write_snapshot_atomic(payload: dict, path: str | None = None) -> None:
    """Write a JSON snapshot atomically to disk. Non-fatal on failure.

    This ensures the API can read a consistent snapshot even when the scheduler
    is interrupted or the DB is flaky.
    """
    try:
        import json
        p = path or SNAPSHOT_FILE
        tmp = f"{p}.tmp"
        log.debug("Preparing to write snapshot to %s via %s", p, tmp)
        try:
            d = os.path.dirname(p)
            if d and not os.path.exists(d):
                log.debug("Creating snapshot directory %s", d)
                os.makedirs(d, exist_ok=True)
        except Exception:
            log.exception("Failed to create snapshot directory for %s", p)
            return

        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass  # fsync may not be available
        log.debug("Successfully wrote content to temporary snapshot %s", tmp)

        try:
            os.replace(tmp, p)
            log.info("Successfully published progress snapshot to %s", p)
        except Exception:
            # fallback for cross-device or other issues
            try:
                os.rename(tmp, p)
                log.info("Successfully published progress snapshot via rename to %s", p)
            except Exception:
                log.exception("Failed to atomically move snapshot from %s to %s", tmp, p)
    except Exception:
        log.exception("Failed to write snapshot")


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
        wait_for_db_ready()
        from backend.database.init_db import _apply_light_migrations
        _apply_light_migrations()
    except Exception:
        log.exception("Failed to apply light migrations at scheduler startup")

    # Default to logging every 5 ticks unless explicitly overridden to reduce IO
    tick_log_every = max(1, int(os.getenv("TICK_LOG_EVERY", "5")))
    boundary_refresh_ticks = int(os.getenv("SIM_BOUNDARY_REFRESH_TICKS", "0"))  # 0 = never refresh

    rs = RunnerService()
    mkt = MarketDataManager()

    # Decide the session clock symbol up-front (resilient)
    step_sec = _step_seconds()
    tf_min = step_sec // 60
    requested_clock = os.getenv("SIM_REFERENCE_CLOCK_SYMBOL", "SPY").upper()
    clock_sym = requested_clock
    try:
        has_bars = mkt.has_minute_bars(clock_sym, tf_min)
    except Exception as e:
        # DB may be down/unavailable; write a db_unavailable snapshot and retry until available
        log.exception("Failed to check minute bars (DB may be down). Will retry.")
        _write_snapshot_atomic({"state": "db_unavailable", "error": str(e)})
        backoff = 1.0
        while True:
            await asyncio.sleep(backoff)
            try:
                if mkt.has_minute_bars(clock_sym, tf_min):
                    break
            except Exception as e2:
                log.debug("Retrying minute bars check failed: %s", e2)
                _write_snapshot_atomic({"state": "db_unavailable", "error": str(e2)})
                backoff = min(backoff * 2.0, 8.0)
        has_bars = True

    if not has_bars:
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

    # Optional: Only clear lingering running state on boot if explicitly requested.
    # Default behavior is to preserve prior state so manual starts survive restarts.
    try:
        if os.getenv("SIM_CLEAR_RUNNING_ON_BOOT", "0") == "1" and os.getenv("SIM_AUTO_START", "0") != "1":
            with DBManager() as db:
                user = db.get_user_by_username("analytics")
                if user:
                    st = db.db.query(SimulationState).filter(SimulationState.user_id == int(getattr(user, "id"))).first()
                    if st and str(st.is_running).lower() in {"true", "1"}:
                        log.info("Clearing simulation_state.is_running on boot for user=%s (SIM_CLEAR_RUNNING_ON_BOOT=1).", user.id)
                        st.is_running = "false"
                        db.db.commit()
    except Exception:
        log.exception("Failed to apply SIM_CLEAR_RUNNING_ON_BOOT policy at scheduler startup")

    state_epoch: int | None = None  # seconds since epoch, UTC
    tick = 0
    last_db_running: bool | None = None
    # cumulative counters for UI-friendly totals (since scheduler start)
    cumulative_processed = 0
    cumulative_buys = 0
    cumulative_sells = 0
    # track recent tick wall-times to estimate tick rate when running at full speed
    try:
        from collections import deque
    except Exception:
        deque = None
    tick_times = deque(maxlen=64) if deque is not None else []
    # watchdog trackers
    last_progress_wall = time.time()
    last_seen_db_epoch: int | None = None
    enforced_stop_applied = False
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
                # detect DB-level start/stop transitions for observability
                try:
                    cur_db_running = str(st.is_running).lower() in {"true", "1"} if st else False
                    if last_db_running is None:
                        last_db_running = cur_db_running
                    else:
                        if not last_db_running and cur_db_running:
                            log.info("Detected SimulationState transition: STOPPED -> RUNNING for user_id=%s", uid)
                        if last_db_running and not cur_db_running:
                            log.info("Detected SimulationState transition: RUNNING -> STOPPED for user_id=%s", uid)
                        last_db_running = cur_db_running
                except Exception:
                    pass
                if not st:
                    st = SimulationState(user_id=uid, is_running="false")
                    db.db.add(st)
                    db.db.commit()
                    await asyncio.sleep(1.0)
                    continue

                # Enforce default stopped state on boot if SIM_AUTO_START!=1
                if not enforced_stop_applied and os.getenv("SIM_AUTO_START", "0") != "1":
                    if str(st.is_running).lower() in {"true", "1"}:
                        log.info("Scheduler boot: SIM_AUTO_START!=1 → forcing simulation_state.is_running=false (user_id=%s)", uid)
                        st.is_running = "false"
                        db.db.commit()
                    enforced_stop_applied = True

                # Auto-resume if requested via env and state is stopped
                try:
                    if os.getenv("SIM_AUTO_START", "0") == "1" and str(st.is_running).lower() not in {"true", "1"}:
                        st.is_running = "true"
                        db.db.commit()
                        log.info("SIM_AUTO_START=1: marked simulation as running on scheduler startup for user_id=%s", uid)
                except Exception:
                    log.exception("Failed to apply SIM_AUTO_START in scheduler")

                # Debug: surface SimulationState read so we can trace API start/stop visibility
                try:
                    log.debug(
                        "SimulationState read for user_id=%s -> is_running=%s last_ts=%s",
                        uid,
                        getattr(st, "is_running", None),
                        getattr(st, "last_ts", None),
                    )
                except Exception:
                    pass

                if str(st.is_running).lower() not in {"true", "1"}:
                    if tick % 10 == 0:
                        log.debug("Idle: simulation not running")
                    # If we just transitioned to not running, clear state_epoch so next start re-initializes
                    state_epoch = None
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
                    # No intraday data available. Auto-stop (do not burn CPU) and surface a snapshot reason.
                    if str(st.is_running).lower() in {"true", "1"}:
                        st.is_running = "false"
                        db.db.commit()
                        log.warning("No minute bars present; auto-stopping simulation. Import minute bars or switch to 1d mode.")
                    try:
                        _write_snapshot_atomic({
                            "state": "no_data",
                            "reason": "no_minute_bars",
                            "message": "No 5m bars found. Import data or run daily timeframe.",
                        })
                    except Exception:
                        pass
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
                if db_epoch is not None and db_epoch != last_seen_db_epoch:
                    last_seen_db_epoch = db_epoch
                    last_progress_wall = time.time()

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
                before_tick = datetime.now(timezone.utc).timestamp()
                cur_ts, stats = await _advance_one_tick(rs, state_epoch)
                after_tick = datetime.now(timezone.utc).timestamp()
                # update cumulative totals
                try:
                    cumulative_processed += int(stats.get("processed", 0))
                except Exception:
                    pass
                try:
                    cumulative_buys += int(stats.get("buys", 0))
                except Exception:
                    pass
                try:
                    cumulative_sells += int(stats.get("sells", 0))
                except Exception:
                    pass
                # record tick wall-time
                try:
                    if deque is not None:
                        tick_times.append(after_tick)
                    else:
                        tick_times.append(after_tick)
                except Exception:
                    pass

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

                # Persist a small last-progress snapshot less frequently to reduce disk IO
                try:
                    snapshot_every = max(1, int(os.getenv("SNAPSHOT_EVERY_TICKS", str(tick_log_every))))
                except Exception:
                    snapshot_every = tick_log_every
                if tick % snapshot_every == 0:
                    try:
                        _write_snapshot_atomic({
                            "sim_time_epoch": cur_ts,
                            "sim_time_iso": datetime.fromtimestamp(cur_ts, tz=timezone.utc).isoformat(),
                            "timeframes": {"5m": {"ticks_done": done_span // step_sec if step_sec > 0 else 0,
                                                  "ticks_total": total_span // step_sec if step_sec > 0 else 0,
                                                  "percent": pct}},
                            "counters": {"executions_all_time": int(cumulative_processed),
                                         "trades_all_time": int(cumulative_buys + cumulative_sells)},
                            "total_buys": int(cumulative_buys),
                            "total_sells": int(cumulative_sells),
                            "progress_percent": pct,
                            "state": "running",
                            "tick_number": tick,
                            "logged_progress": pct,
                            "current_runner_info": {
                                "timeframe": f"{int(step_sec // 60)}m" if step_sec % 60 == 0 else f"{step_sec}s",
                                "symbol": (clock_sym or "<global>"),
                                "as_of_iso": datetime.fromtimestamp(cur_ts, tz=timezone.utc).isoformat(),
                            },
                            **_compute_eta(cur_ts, pace, total_span, done_span, step_sec, tick_times)
                        })
                    except Exception:
                        log.exception("Failed to write progress snapshot")

                await asyncio.sleep(pace if pace > 0 else 0)
                tick += 1

        except Exception:
            log.exception("Scheduler loop error")
            await asyncio.sleep(0.5)
            tick += 1

        # Watchdog: if sim is marked running but no last_ts progress for too long, exit for supervisor restart
        try:
            if WATCHDOG_IDLE_SECONDS > 0:
                with DBManager() as db:
                    user = db.get_user_by_username("analytics")
                    st = db.db.query(SimulationState).filter(SimulationState.user_id == int(getattr(user, "id"))).first() if user else None
                running = bool(st and str(st.is_running).lower() in {"true", "1"})
                if running and last_seen_db_epoch is not None and (time.time() - last_progress_wall) > WATCHDOG_IDLE_SECONDS:
                    log.error(
                        "Watchdog: no SimulationState.last_ts progress for %ss while running (last_epoch=%s). Exiting to let supervisor restart.",
                        WATCHDOG_IDLE_SECONDS,
                        last_seen_db_epoch,
                    )
                    try:
                        _write_snapshot_atomic({"state": "watchdog_restart", "last_epoch": last_seen_db_epoch, "at": datetime.now(timezone.utc).isoformat()})
                    finally:
                        os._exit(100)
        except Exception:
            pass

if __name__ == "__main__":
    import asyncio
    import sys

    if "reset" in sys.argv:
        print("Resetting simulation state...")
        try:
            with DBManager() as db:
                user = db.get_user_by_username("analytics")
                if user:
                    st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
                    if st:
                        st.last_ts = None
                        st.is_running = "false"
                        db.db.commit()
                        print("Simulation state reset.")
        except Exception as e:
            print(f"Failed to reset simulation state: {e}")
    else:
        asyncio.run(main())

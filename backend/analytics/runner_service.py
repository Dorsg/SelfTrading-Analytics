from __future__ import annotations

import os
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple, Set

from database.db_manager import DBManager
from database.models import Runner, OpenPosition, User
from backend.ib_manager.market_data_manager import MarketDataManager
from backend.broker.mock_broker import MockBroker
from backend.strategies.runner_decision_info import RunnerDecisionInfo
from backend.strategies.factory import select_strategy, resolve_strategy_key
from backend.strategies.contracts import validate_decision
from backend.universe import UniverseManager

log = logging.getLogger("runner-service")
_exec_log = logging.getLogger("runner-executions")


@dataclass(slots=True)
class _RunnerCtx:
    runner: Any
    position: Optional[OpenPosition]
    price: float
    candles: List[Dict[str, Any]]


@dataclass(slots=True)
class RunnerView:
    id: int
    user_id: int
    name: str
    strategy: str
    budget: float
    current_budget: float
    stock: str
    time_frame: int
    parameters: dict
    exit_strategy: str
    activation: str


class RunnerService:
    """
    Executes one decision tick across active runners.

    Exit rules enforced:
    • Hold after BUY until either:
        (1) broker stop hits (MockBroker.on_tick closes and logs once), or
        (2) strategy SELL triggers (we call MockBroker.sell_all, which logs).
    • The trailing stop is broker-managed and is armed once at BUY.
      Strategy shouldn't cause immediate sell-only-because-a-trailing-stop-exists.

    Additional safeguards:
    • Skip trading when the latest candle is stale (older than TF or from a prior day).
    • NEW: Only honor strategy-driven BUY/SELL when the candle/bar has advanced
      since the last action we evaluated for that runner+timeframe. Broker stops
      remain evaluated every tick.

    P1:
    • Universe hygiene: filter out post-cutoff IPOs and symbols lacking coverage.
      Snapshot allowlist supported via UNIVERSE_SNAPSHOT_PATH.

    P2:
    • Reduce log noise: summarize same-bar skips; suppress 1440m same-bar spam.
    • Re-entry cooldown: after a broker stop-out, wait N bars before next BUY.
    • Enforce a minimum intraday trailing % when arming stops.
    """

    def __init__(self) -> None:
        self.mkt = MarketDataManager()
        self.broker = MockBroker()
        self._cache_seq: Optional[int] = None
        self._candle_cache: Dict[Tuple[str, int, int], List[Dict[str, Any]]] = {}

        self._log_no_action = os.getenv("SIM_LOG_NO_ACTION", "0") == "1"
        self._thin_no_action_details = os.getenv("SIM_THIN_NO_ACTION_DETAILS", "1") == "1"

        # Fixed per-runner budget (env override supported)
        self._unit_budget_usd = float(os.getenv("SIM_RUNNER_UNIT_BUDGET", "2000"))

        # Minimum cash safeguard for simulator
        self._min_cash_floor = float(os.getenv("SIM_MIN_CASH", "5000000"))
        self._topup_cash_to = float(os.getenv("SIM_TOPUP_CASH_TO", "10000000"))

        # Stale price guard (always on by default)
        self._skip_stale_price = os.getenv("SIM_SKIP_STALE_PRICE", "1") == "1"

        # NEW: Require bar advancement for strategy-driven entries/exits
        self._require_bar_advance = os.getenv("SIM_REQUIRE_BAR_ADVANCE", "1") == "1"

        # NEW: Remember last processed candle TS per (runner_id, timeframe)
        self._last_bar_ts: Dict[Tuple[int, int], datetime] = {}

        # NEW: Only use regular-hours candles by default (can be disabled)
        self._regular_hours_only = os.getenv("SIM_REGULAR_HOURS_ONLY", "1") == "1"

        # NEW: Warn once per (symbol, tf, ET date) if no candles available
        self._warn_no_data_once: Set[Tuple[str, int, str]] = set()

        # NEW: Track when we had to fall back to extended hours to fetch candles
        self._x_hours_fallback_used: Set[Tuple[str, int, int]] = set()  # (symbol, tf, cycle_seq)

        # P1 Universe manager
        self._uni = UniverseManager()

        # P2 log noise controls
        self._summarize_same_bar = os.getenv("SIM_SUMMARIZE_SAME_BAR", "1") == "1"
        self._suppress_daily_same_bar = os.getenv("SIM_LOG_DAILY_SAMEBAR", "0") == "1"  # default: suppress

        # P2 re-entry cooldown
        self._cooldown_after_stop_bars = int(os.getenv("SIM_COOLDOWN_BARS_AFTER_STOP", "3"))
        self._cooldown: Dict[Tuple[int, int], int] = {}  # (runner_id, tf) -> remaining bars

        # P2 min intraday trailing percent (if strategy didn't enforce bigger)
        self._min_intraday_trail_pct = float(os.getenv("SIM_MIN_INTRADAY_TRAIL_PCT", "1.25"))


    # ───────────────────────── internals ─────────────────────────
    def _get_candles_cached(
        self,
        symbol: str,
        interval_min: int,
        as_of: datetime,
        lookback: int = 300
    ) -> List[Dict[str, Any]]:
        as_of = as_of.astimezone(timezone.utc)
        seq = int(as_of.timestamp())
        sym = symbol.upper()
        key = (sym, int(interval_min), seq)

        if self._cache_seq != seq:
            self._cache_seq = seq
            self._candle_cache.clear()
            self._x_hours_fallback_used.clear()

        if key in self._candle_cache:
            return self._candle_cache[key]

        # First attempt respects regular-hours flag
        candles = self.mkt.get_candles_until(
            sym,
            int(interval_min),
            as_of,
            lookback=lookback,
            regular_hours_only=self._regular_hours_only,
        )

        # One-shot retry: if RTH-only returned nothing but coverage exists, try extended hours
        if (
            not candles
            and self._regular_hours_only
            and int(interval_min) < 1440
            and self.mkt.has_minute_bars(sym, int(interval_min))
        ):
            alt = self.mkt.get_candles_until(
                sym,
                int(interval_min),
                as_of,
                lookback=lookback,
                regular_hours_only=False,
            )
            if alt:
                candles = alt
                self._x_hours_fallback_used.add(key)

        self._candle_cache[key] = candles
        return candles


    def _prefetch_candles_for_runners(self, runners: List[RunnerView], as_of: datetime) -> None:
        if not runners:
            return
        as_of = as_of.astimezone(timezone.utc)
        seq = int(as_of.timestamp())

        self._cache_seq = seq
        self._candle_cache.clear()

        # Map symbols via universe (e.g., META→FB) to avoid "no candles" spam
        syms_5 = sorted({self._uni.map_symbol(r.stock, as_of=as_of) for r in runners if int(r.time_frame or 5) == 5})
        syms_1d = sorted({self._uni.map_symbol(r.stock, as_of=as_of) for r in runners if int(r.time_frame or 5) == 1440})

        if syms_5:
            data5 = self.mkt.get_candles_bulk_until(
                syms_5, 5, as_of, lookback=300, regular_hours_only=self._regular_hours_only
            )
            for s, candles in data5.items():
                self._candle_cache[(s, 5, seq)] = candles
        if syms_1d:
            data1d = self.mkt.get_candles_bulk_until(
                syms_1d, 1440, as_of, lookback=300, regular_hours_only=False  # daily unaffected by RTH filter
            )
            for s, candles in data1d.items():
                self._candle_cache[(s, 1440, seq)] = candles

        for s in syms_5:
            self._candle_cache.setdefault((s, 5, seq), [])
        for s in syms_1d:
            self._candle_cache.setdefault((s, 1440, seq), [])

    @staticmethod
    def _last_candle_ts(candles: List[Dict[str, Any]]) -> Optional[datetime]:
        if not candles:
            return None
        ts = candles[-1].get("ts")
        if ts is None:
            return None
        # Historical tables already store timezone-aware datetimes; ensure UTC
        return (ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)

    @staticmethod
    def _is_stale_candle(last_ts: Optional[datetime], tf_min: int, as_of: datetime) -> bool:
        if last_ts is None:
            return True
        # If the last bar is from a prior day → stale (pre-open for that TF)
        if last_ts.date() < as_of.date():
            return True
        # If the last bar is older than one TF window → stale
        age_sec = (as_of - last_ts).total_seconds()
        return age_sec > (tf_min * 60 + 1)

    def _decide(self, ctx: _RunnerCtx, strategy_obj=None) -> dict:
        info = RunnerDecisionInfo(
            runner=ctx.runner,
            position=ctx.position,
            current_price=ctx.price,
            candles=ctx.candles,
            distance_from_time_limit=None,
        )
        strat = strategy_obj or select_strategy(ctx.runner)
        raw = strat.decide_buy(info) if ctx.position is None else strat.decide_sell(info)
        decision = validate_decision(raw, is_exit=ctx.position is not None) or {"action": "NO_ACTION"}

        # Inject a static stop at BUY if strategy didn't provide any stop
        if (decision.get("action") or "NO_ACTION").upper() == "BUY":
            has_trail = isinstance(decision.get("trail_stop_order"), dict)
            has_static = isinstance(decision.get("static_stop_order"), dict)
            if not has_trail and not has_static:
                try:
                    params = getattr(ctx.runner, "parameters", {}) or {}
                    sl_pct = float(params.get("default_stop_loss_percent", 0.0) or 0.0)
                except Exception:
                    sl_pct = 0.0
                if sl_pct > 0:
                    decision["static_stop_order"] = {
                        "action": "SELL",
                        "order_type": "STOP",
                        "stop_price": round(ctx.price * (1.0 - sl_pct / 100.0), 4),
                    }
        return decision

    def _qty_from_budget(self, db: DBManager, r: RunnerView, price: float) -> int:
        try:
            if price is None or price <= 0:
                return 0
            qty = int(self._unit_budget_usd // max(price, 0.01))
            return max(qty, 0)
        except Exception:
            return 0

    @staticmethod
    def _snapshot_runner(r: Runner) -> RunnerView:
        try:
            return RunnerView(
                id=int(getattr(r, "id")),
                user_id=int(getattr(r, "user_id")),
                name=str(getattr(r, "name", "") or ""),
                strategy=str(getattr(r, "strategy", "") or ""),
                budget=float(getattr(r, "budget", 0.0) or 0.0),
                current_budget=float(getattr(r, "current_budget", 0.0) or 0.0),
                stock=str(getattr(r, "stock", "UNKNOWN") or "UNKNOWN").upper(),
                time_frame=int(getattr(r, "time_frame", 5) or 5),
                parameters=dict(getattr(r, "parameters", {}) or {}),
                exit_strategy=str(getattr(r, "exit_strategy", "hold_forever") or "hold_forever"),
                activation=str(getattr(r, "activation", "active") or "active"),
            )
        except Exception:
            return RunnerView(
                id=int(getattr(r, "id", 0) or 0),
                user_id=int(getattr(r, "user_id", 0) or 0),
                name=str(getattr(r, "name", "") or ""),
                strategy=str(getattr(r, "strategy", "") or ""),
                budget=float(getattr(r, "budget", 0.0) or 0.0),
                current_budget=float(getattr(r, "current_budget", 0.0) or 0.0),
                stock=str(getattr(r, "stock", "UNKNOWN") or "UNKNOWN").upper(),
                time_frame=int(getattr(r, "time_frame", 5) or 5),
                parameters=dict(getattr(r, "parameters", {}) or {}),
                exit_strategy=str(getattr(r, "exit_strategy", "hold_forever") or "hold_forever"),
                activation=str(getattr(r, "activation", "active") or "active"),
            )

    # ───────────────────────── public ─────────────────────────
    async def run_tick(self, as_of: datetime) -> dict:
        as_of = as_of.astimezone(timezone.utc)
        seq = int(as_of.timestamp())

        stats = {
            "processed": 0,
            "buys": 0,
            "sells": 0,
            "no_action": 0,
            "skipped_no_data": 0,
            "skipped_no_budget": 0,
            "errors": 0,
            "post_tick_errors": 0,
        }

        exec_buffer: List[dict] = []
        same_bar_summary: Dict[Tuple[int, int], int] = {}  # (runner_id, tf) -> count
        no_candles_by_tf: Dict[int, Set[str]] = {}  # NEW: aggregate per-tick no-candles

        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            if not user:
                log.warning("No analytics user found yet.")
                return stats

            uid = int(getattr(user, "id"))

            # Ensure account exists and is funded for simulation
            try:
                acct = db.ensure_account(user_id=uid, name="mock")
                try:
                    current_cash = float(getattr(acct, "cash", 0.0) or 0.0)
                except Exception:
                    current_cash = 0.0
                if current_cash < self._min_cash_floor:
                    try:
                        setattr(acct, "cash", self._topup_cash_to)
                        db.db.commit()
                        log.info(
                            "Top-upped mock account cash to $%.2f for user_id=%s (previous=%.2f, floor=%.2f)",
                            self._topup_cash_to, uid, current_cash, self._min_cash_floor
                        )
                    except Exception:
                        log.exception("Failed to top-up mock account cash for user_id=%s", uid)
                        try:
                            db.db.rollback()
                        except Exception:
                            pass
            except Exception:
                log.exception("ensure_account failed for user_id=%s", uid)
                try:
                    db.db.rollback()
                except Exception:
                    pass

            runners_orm = db.get_runners_by_user(user_id=uid, activation="active")
            runners: List[RunnerView] = [self._snapshot_runner(r) for r in runners_orm]

            # Universe hygiene (P1): evaluate once per run against active runners
            try:
                self._uni.ensure_loaded([r.stock for r in runners], self.mkt)
            except Exception:
                log.exception("Universe evaluation failed; proceeding without filter")

            self._prefetch_candles_for_runners(runners, as_of)

            for r in runners:
                try:
                    rid = int(getattr(r, "id", 0) or 0)
                    if rid == 0:
                        exec_buffer.append({
                            "runner_id": 0,
                            "user_id": uid,
                            "symbol": (getattr(r, "stock", "") or "UNKNOWN").upper(),
                            "strategy": str(getattr(r, "strategy", "")),
                            "status": "skipped-invalid-runner",
                            "reason": "no_primary_key",
                            "details": json.dumps({"error": "runner row missing primary key"}, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    # Universe filter (P1)
                    if not self._uni.is_allowed(r.stock, self.mkt):
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": str(getattr(r, "strategy", "")),
                            "status": "completed",
                            "reason": "skipped-excluded-universe",
                            "details": (None if self._thin_no_action_details else json.dumps({"reason": self._uni.reason_for(r.stock)}, ensure_ascii=False)),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    canon = resolve_strategy_key(getattr(r, "strategy", None))
                    if not canon:
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": str(getattr(r, "strategy", "")),
                            "status": "skipped-unknown-strategy",
                            "reason": "unknown_strategy",
                            "details": json.dumps({"strategy": getattr(r, "strategy", None)}, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    tf = int(getattr(r, "time_frame", 5) or 5)
                    # Use mapped symbol for data access (e.g., META→FB), but keep original in records
                    data_sym = self._uni.map_symbol(r.stock, as_of=as_of)
                    candles = self._get_candles_cached(data_sym, tf, as_of, lookback=300)
                    if not candles:
                        # Warn once per (symbol, tf, ET date), enriched with coverage window
                        try:
                            from zoneinfo import ZoneInfo  # type: ignore
                            ny = ZoneInfo("America/New_York")
                            et_day = as_of.astimezone(ny).date().isoformat()
                        except Exception:
                            et_day = as_of.date().isoformat()
                        key = (r.stock, tf, et_day)

                        has_cov = self.mkt.has_daily_bars(r.stock) if tf == 1440 else self.mkt.has_minute_bars(r.stock, tf)
                        earliest = self.mkt.earliest_minute_ts(data_sym, tf) if tf < 1440 else None
                        latest   = self.mkt.latest_minute_ts(data_sym, tf) if tf < 1440 else None
                        rng = f"range=[{earliest.isoformat() if earliest else 'None'}, {latest.isoformat() if latest else 'None'}]"
                        msg = (
                            f"No historical candles for {r.stock} (data_sym={data_sym}) "
                            f"tf={tf}m at {as_of.isoformat()} (regular_hours_only={self._regular_hours_only}, "
                            f"coverage={has_cov}) {rng}"
                        )
                        if key not in self._warn_no_data_once:
                            self._warn_no_data_once.add(key)
                            if has_cov:
                                log.warning(msg)
                            else:
                                log.info(msg + " — likely pre-IPO or outside data coverage; skipping.")

                        no_candles_by_tf.setdefault(tf, set()).add(r.stock)

                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": r.strategy,
                            "status": "skipped-no-data",
                            "reason": "insufficient_candles",
                            "details": (None if self._thin_no_action_details else json.dumps({"message": "no candles available at as_of", "tf": tf}, ensure_ascii=False)),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        stats["skipped_no_data"] += 1
                        stats["processed"] += 1
                        continue

                    last_ts = self._last_candle_ts(candles)
                    is_stale = self._skip_stale_price and self._is_stale_candle(last_ts, tf, as_of)

                    # If stale, do NOT ping broker (so we don't evaluate stops on frozen prices)
                    if is_stale:
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": r.strategy,
                            "status": "completed",
                            "reason": "skipped-stale-price",
                            "details": (None if self._thin_no_action_details else json.dumps({
                                "message": "last candle is stale for timeframe",
                                "tf_min": tf,
                                "last_ts": (last_ts.isoformat() if last_ts else None),
                                "as_of": as_of.isoformat(),
                            }, ensure_ascii=False)),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        if self._log_no_action:
                            log.debug("NO_ACTION %s tf=%dm — stale candle (last_ts=%s, as_of=%s)",
                                    r.stock, tf, (last_ts.isoformat() if last_ts else "None"), as_of.isoformat())
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    # Position before broker tick (to detect broker-driven exits)
                    try:
                        pos_before: Optional[OpenPosition] = (
                            db.db.query(OpenPosition)
                            .filter(OpenPosition.runner_id == r.id)
                            .first()
                        )
                    except Exception:
                        pos_before = None

                    # Fresh price → broker first (so stops can close if truly hit)
                    price = float(candles[-1]["close"])
                    self.broker.on_tick(user_id=uid, runner=r, price=price, at=as_of)

                    # Re-sync ORM after broker activity
                    try:
                        db.db.expire_all()
                    except Exception:
                        pass

                    # Refresh position AFTER broker.on_tick
                    try:
                        pos: Optional[OpenPosition] = (
                            db.db.query(OpenPosition)
                            .filter(OpenPosition.runner_id == r.id)
                            .first()
                        )
                    except Exception:
                        log.exception("Failed to refresh position for runner %s", r.id)
                        pos = None

                    # Detect broker-driven exit (stop/market exit) and apply cooldown (P2)
                    if pos_before is not None and pos is None:
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": r.strategy,
                            "status": "completed",
                            "reason": "sell",
                            "details": json.dumps({"message": "broker_stop_triggered", "price": round(price, 6)}, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        stats["sells"] += 1
                        stats["processed"] += 1
                        if self._cooldown_after_stop_bars > 0:
                            self._cooldown[(r.id, tf)] = self._cooldown_after_stop_bars

                    # NEW: bar-advance guard and cooldown decrement
                    bar_key = (r.id, tf)
                    prev_bar_ts = self._last_bar_ts.get(bar_key)
                    bar_advanced = (prev_bar_ts is None) or (last_ts is not None and last_ts > prev_bar_ts)
                    if bar_advanced and (bar_key in self._cooldown) and self._cooldown[bar_key] > 0:
                        self._cooldown[bar_key] = max(0, self._cooldown[bar_key] - 1)

                    if not bar_advanced and self._require_bar_advance:
                        if self._summarize_same_bar and (tf == 1440 and not self._suppress_daily_same_bar):
                            pass  # allow daily same-bar if explicitly enabled
                        elif self._summarize_same_bar:
                            same_bar_summary[bar_key] = same_bar_summary.get(bar_key, 0) + 1
                            stats["no_action"] += 1
                            stats["processed"] += 1
                            continue
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": r.strategy,
                            "status": "completed",
                            "reason": "skipped-same-bar",
                            "details": (None if self._thin_no_action_details else json.dumps({
                                "message": "bar has not advanced; ignoring strategy signals this tick",
                                "tf_min": tf,
                                "last_bar_ts": (last_ts.isoformat() if last_ts else None),
                                "prev_bar_ts": (prev_bar_ts.isoformat() if prev_bar_ts else None),
                                "as_of": as_of.isoformat(),
                            }, ensure_ascii=False)),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        if self._log_no_action:
                            log.debug("NO_ACTION %s tf=%dm — same bar (last=%s, prev=%s).",
                                    r.stock, tf,
                                    (last_ts.isoformat() if last_ts else "None"),
                                    (prev_bar_ts.isoformat() if prev_bar_ts else "None"))
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    ctx = _RunnerCtx(runner=r, position=pos, price=price, candles=candles)
                    decision = self._decide(ctx)
                    action = (decision.get("action") or "NO_ACTION").upper()
                    explanation = decision.get("explanation")
                    checks = decision.get("checks")

                    details_payload = {
                        "price": round(ctx.price, 6),
                        "position_open": bool(ctx.position is not None),
                        "timeframe_min": tf,
                        "stale": False,
                        "last_ts": last_ts.isoformat() if last_ts else None,
                        "decision": {k: v for k, v in decision.items() if k not in {"action"}},
                        "checks": checks,
                    }
                    # If we used an alias for data, surface that for traceability
                    if data_sym != r.stock:
                        details_payload["mapped_symbol"] = data_sym
                    # NEW: tag if we used extended-hours fallback to get candles
                    if (data_sym, tf, seq) in self._x_hours_fallback_used:
                        details_payload["x_hours_fallback"] = True

                    details_json = json.dumps(details_payload, ensure_ascii=False)

                    # BUY (only if no position and after bar advance)
                    if action == "BUY" and ctx.position is None:
                        cd_left = int(self._cooldown.get(bar_key, 0))
                        if cd_left > 0:
                            exec_buffer.append({
                                "runner_id": r.id,
                                "user_id": uid,
                                "symbol": r.stock,
                                "strategy": r.strategy,
                                "status": "completed",
                                "reason": "skipped-cooldown",
                                "details": (None if self._thin_no_action_details else json.dumps({"cooldown_bars_left": cd_left}, ensure_ascii=False)),
                                "execution_time": as_of,
                                "cycle_seq": seq,
                            })
                            stats["no_action"] += 1
                            stats["processed"] += 1
                            if last_ts is not None:
                                self._last_bar_ts[bar_key] = last_ts
                            continue

                        qty = int(decision.get("quantity") or 0)
                        if qty <= 0:
                            qty = self._qty_from_budget(db, r, ctx.price)
                        if qty <= 0:
                            msg = {"reason": "qty=0", "explanation": explanation or "insufficient budget"}
                            exec_buffer.append({
                                "runner_id": r.id,
                                "user_id": uid,
                                "symbol": r.stock,
                                "strategy": r.strategy,
                                "status": "skipped-no-budget",
                                "reason": "qty=0",
                                "details": (None if self._thin_no_action_details else json.dumps(msg, ensure_ascii=False)),
                                "execution_time": as_of,
                                "cycle_seq": seq,
                            })
                            if self._log_no_action:
                                log.debug("NO_BUY %s tf=%dm — qty=0 | %s",
                                        r.stock, tf, (explanation or "").replace("\n", " | "))
                            stats["skipped_no_budget"] += 1
                            stats["processed"] += 1
                            if last_ts is not None:
                                self._last_bar_ts[bar_key] = last_ts
                            continue

                        ok: bool = False
                        try:
                            ok = bool(self.broker.buy(
                                user_id=uid,
                                runner=r,
                                symbol=r.stock,
                                price=ctx.price,
                                quantity=qty,
                                decision=decision,
                                at=as_of,
                            ))
                        except Exception:
                            ok = False
                            log.exception("Broker BUY failed for %s", r.stock)

                        if not ok:
                            exec_buffer.append({
                                "runner_id": r.id,
                                "user_id": uid,
                                "symbol": r.stock,
                                "strategy": r.strategy,
                                "status": "skipped-no-budget",
                                "reason": "broker_rejected_buy",
                                "details": (None if self._thin_no_action_details else details_json),
                                "execution_time": as_of,
                                "cycle_seq": seq,
                            })
                            if self._log_no_action:
                                log.debug("NO_BUY %s tf=%dm — broker rejected BUY (likely cash guard).",
                                        r.stock, tf)
                            stats["skipped_no_budget"] += 1
                            stats["processed"] += 1
                            if last_ts is not None:
                                self._last_bar_ts[bar_key] = last_ts
                            continue

                        # Arm trailing stop once at BUY (broker-managed), honoring strategy % but enforcing min for intraday
                        try:
                            strategy_trail = None
                            try:
                                tspec = decision.get("trail_stop_order") or {}
                                if isinstance(tspec, dict):
                                    strategy_trail = float(tspec.get("trailing_percent") or 0.0)
                            except Exception:
                                strategy_trail = None

                            params = getattr(r, "parameters", {}) or {}
                            param_trail = float(params.get("trailing_stop_percent", 0.0) or 0.0)
                            trail_pct = float(strategy_trail if (strategy_trail is not None and strategy_trail > 0.0) else param_trail)
                        except Exception:
                            trail_pct = 0.0

                        if tf < 1440 and self._min_intraday_trail_pct > 0.0:
                            trail_pct = max(trail_pct, self._min_intraday_trail_pct)

                        if trail_pct > 0.0:
                            self.broker.arm_trailing_stop_once(
                                user_id=uid,
                                runner=r,
                                entry_price=ctx.price,
                                trail_pct=trail_pct,
                                at=as_of,
                                interval_min=tf,
                            )

                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": r.strategy,
                            "status": "completed",
                            "reason": "buy",
                            "details": details_json,
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        log.debug("BUY %s qty=%d @ %.4f tf=%dm | %s",
                                r.stock, qty, ctx.price, tf, (explanation or "").replace("\n", " | "))
                        stats["buys"] += 1
                        stats["processed"] += 1
                        if last_ts is not None:
                            self._last_bar_ts[bar_key] = last_ts
                        continue

                    # SELL (strategy-driven; only after bar advance)
                    if action == "SELL" and ctx.position is not None:
                        ok = False
                        try:
                            reason = str(decision.get("reason") or decision.get("explanation") or "strategy_sell")
                            ok = self.broker.sell_all(
                                user_id=uid,
                                runner=r,
                                symbol=r.stock,
                                price=ctx.price,
                                decision=decision,
                                at=as_of,
                                reason_override=reason,
                            )
                        except Exception:
                            ok = False
                            log.exception("Broker SELL failed for %s", r.stock)

                        try:
                            db.db.expire_all()
                        except Exception:
                            pass

                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": r.strategy,
                            "status": "completed" if ok else "error",
                            "reason": "sell" if ok else "broker_sell_failed",
                            "details": details_json,
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                        if ok:
                            log.debug("SELL %s @ %.4f tf=%dm | %s",
                                    r.stock, ctx.price, tf, (explanation or "").replace("\n", " | "))
                            stats["sells"] += 1
                        else:
                            stats["errors"] += 1
                        stats["processed"] += 1
                        if last_ts is not None:
                            self._last_bar_ts[bar_key] = last_ts
                        continue

                    # NO_ACTION
                    exec_buffer.append({
                        "runner_id": r.id,
                        "user_id": uid,
                        "symbol": r.stock,
                        "strategy": r.strategy,
                        "status": "completed",
                        "reason": str(decision.get("reason") or "no_action"),
                        "details": (None if self._thin_no_action_details else details_json),
                        "execution_time": as_of,
                        "cycle_seq": seq,
                    })
                    if self._log_no_action:
                        if explanation:
                            log.debug("NO_ACTION %s tf=%dm — %s", r.stock, tf, explanation.replace("\n", " | "))
                        else:
                            log.debug("NO_ACTION %s tf=%dm", r.stock, tf)
                    stats["no_action"] += 1
                    stats["processed"] += 1
                    if last_ts is not None:
                        self._last_bar_ts[bar_key] = last_ts

                except Exception:
                    try:
                        label = getattr(r, "name", None) or f"#{getattr(r, 'id', 'unknown')}"
                    except Exception:
                        label = "unknown"
                    log.exception("Runner %s tick failed", label)
                    try:
                        exec_buffer.append({
                            "runner_id": int(getattr(r, "id", 0) or 0),
                            "user_id": uid,
                            "symbol": (getattr(r, "stock", "") or "").upper() or "UNKNOWN",
                            "strategy": (getattr(r, "strategy", "") or "unknown"),
                            "status": "error",
                            "reason": "exception",
                            "details": "see logs",
                            "execution_time": as_of,
                            "cycle_seq": seq,
                        })
                    except Exception:
                        pass
                    stats["errors"] += 1
                    try:
                        db.db.rollback()
                    except Exception:
                        pass
                    stats["processed"] += 1

            # P2: Emit one log line summarizing same-bar skips (no DB write)
            if self._summarize_same_bar and same_bar_summary:
                summary = [
                    {"runner_id": rid, "tf": tf, "skipped": n}
                    for (rid, tf), n in sorted(same_bar_summary.items(), key=lambda x: (x[0][1], x[0][0]))
                ]
                try:
                    _exec_log.info(
                        "cycle=%s time=%s runner=%s user=%s symbol=%s strategy=%s status=%s reason=%s details=%s",
                        seq,
                        as_of.isoformat(),
                        "*",
                        uid,
                        "*",
                        "summary",
                        "completed",
                        "skipped-same-bar-summary",
                        json.dumps(summary, ensure_ascii=False),
                    )
                except Exception:
                    pass

            # NEW: per-tick summary for no-candles symbols by timeframe
            if no_candles_by_tf:
                try:
                    for tf_k, syms in sorted(no_candles_by_tf.items()):
                        log.info(
                            "%d symbols had no %dm bars at %s; sample: %s",
                            len(syms), tf_k, as_of.isoformat(),
                            ", ".join(sorted(list(syms))[:8])
                        )
                except Exception:
                    pass

            # Batch persist executions
            try:
                if exec_buffer:
                    db.bulk_record_runner_executions(exec_buffer)
            except Exception:
                log.exception("Bulk insert of runner executions failed")
                try:
                    db.db.rollback()
                except Exception:
                    pass
                stats["post_tick_errors"] += 1

            # Account mark-to-market (mock)
            try:
                self.broker.mark_to_market_all(user_id=uid, at=as_of)
            except Exception:
                log.exception("Mark-to-market after tick failed")
                try:
                    db.db.rollback()
                except Exception:
                    pass
                stats["post_tick_errors"] += 1

        log.debug(
            "tick@%s processed=%d buys=%d sells=%d no_action=%d skipped_no_data=%d skipped_no_budget=%d errors=%d post_tick_errors=%d",
            as_of.isoformat(),
            stats["processed"],
            stats["buys"],
            stats["sells"],
            stats["no_action"],
            stats["skipped_no_data"],
            stats["skipped_no_budget"],
            stats["errors"],
            stats["post_tick_errors"],
        )
        return stats

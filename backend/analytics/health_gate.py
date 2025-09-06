from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Tuple, Optional, List, Any, Set

from backend.ib_manager.market_data_manager import MarketDataManager

log = logging.getLogger("runner-health-gate")


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@dataclass
class _PairState:
    state: str = "HEALTHY"  # HEALTHY | DEGRADED | EXCLUDED
    reason: Optional[str] = None
    # consecutive counters inside the *current* session/day
    consecutive_no_data: int = 0
    consecutive_errors: int = 0
    # rolling window: per-ET-day counts for last ~N days
    day_counts: Dict[str, int] = field(default_factory=dict)
    excluded_until: Optional[datetime] = None  # TTL for EXCLUDED
    first_seen_earliest: Optional[datetime] = None  # provider earliest for diagnostics


class HealthGate:
    """
    Per-(symbol,timeframe) health FSM with TTL auto-exclude.

    Transitions:
      HEALTHY  → DEGRADED     when ≥ degrade_threshold consecutive no_data/errors
      DEGRADED → EXCLUDED     when >= exclude_threshold over last `window_days` ET days
      *        → EXCLUDED     immediately when provider earliest > sim_start (coverage impossible)
      EXCLUDED → HEALTHY      when excluded_until < now (TTL expired), on first clean pass

    All decisions are *pair-scoped* (symbol, timeframe minutes).
    """

    def __init__(
        self,
        *,
        ttl_days: int = 5,
        degrade_threshold: int = 3,
        exclude_threshold_sessions: int = 10,
        window_days: int = 5,
    ) -> None:
        self._ttl = int(ttl_days)
        self._deg = int(degrade_threshold)
        self._exc = int(exclude_threshold_sessions)
        self._win = int(window_days)
        self._pairs: Dict[Tuple[str, int], _PairState] = {}
        self._bootstrapped = False

    # ─────────────────────────────────────────────────────────────────────────────

    def _get(self, sym: str, tf: int) -> _PairState:
        key = (sym.upper(), int(tf))
        state = self._pairs.get(key)
        if state is None:
            state = _PairState()
            self._pairs[key] = state
        return state

    def is_excluded(self, sym: str, tf: int, now: datetime) -> Tuple[bool, Optional[str]]:
        st = self._get(sym, tf)
        if st.state == "EXCLUDED":
            if st.excluded_until and _utc(now) >= st.excluded_until:
                # TTL expired → re-admit
                st.state = "HEALTHY"
                st.reason = None
                st.consecutive_errors = st.consecutive_no_data = 0
                return (False, None)
            return (True, st.reason or "excluded")
        return (False, None)

    # ─────────────────────────────────────────────────────────────────────────────

    def _roll_day(self, st: _PairState, et_day: str) -> None:
        # Keep only the last window_days entries
        if et_day not in st.day_counts:
            st.day_counts[et_day] = 0
        # prune
        if len(st.day_counts) > (self._win + 2):
            keys = sorted(st.day_counts.keys())[-self._win :]
            st.day_counts = {k: st.day_counts[k] for k in keys}

    def _sum_recent(self, st: _PairState) -> int:
        if not st.day_counts:
            return 0
        keys = sorted(st.day_counts.keys())[-self._win :]
        return sum(st.day_counts[k] for k in keys)

    def note_no_data(self, *, sym: str, tf: int, now: datetime, et_day: str) -> None:
        st = self._get(sym, tf)
        st.consecutive_no_data += 1
        self._roll_day(st, et_day)
        st.day_counts[et_day] += 1
        if st.consecutive_no_data >= self._deg and st.state == "HEALTHY":
            st.state = "DEGRADED"
            st.reason = "no_data"
        if self._sum_recent(st) >= self._exc and st.state in {"HEALTHY", "DEGRADED"}:
            st.state = "EXCLUDED"
            st.reason = "errors_over_sessions"
            st.excluded_until = _utc(now) + timedelta(days=self._ttl)

    def note_error(self, *, sym: str, tf: int, now: datetime, et_day: str) -> None:
        st = self._get(sym, tf)
        st.consecutive_errors += 1
        self._roll_day(st, et_day)
        st.day_counts[et_day] += 1
        if st.consecutive_errors >= self._deg and st.state == "HEALTHY":
            st.state = "DEGRADED"
            st.reason = "errors"
        if self._sum_recent(st) >= self._exc and st.state in {"HEALTHY", "DEGRADED"}:
            st.state = "EXCLUDED"
            st.reason = "errors_over_sessions"
            st.excluded_until = _utc(now) + timedelta(days=self._ttl)

    # ─────────────────────────────────────────────────────────────────────────────

    def exclude_coverage(self, *, sym: str, tf: int, earliest: Optional[datetime], sim_start: datetime, now: datetime) -> None:
        """Exclude immediately due to coverage gap (provider earliest > sim_start)."""
        st = self._get(sym, tf)
        st.first_seen_earliest = earliest
        st.state = "EXCLUDED"
        st.reason = "coverage"
        st.excluded_until = _utc(now) + timedelta(days=self._ttl)

    # ─────────────────────────────────────────────────────────────────────────────

    def bootstrap_coverage_scan(
        self,
        *,
        runners: List[Any],
        sim_start: datetime,
        market: MarketDataManager,
        now: datetime,
    ) -> None:
        """
        One-time scan at the first tick to quarantine pairs with impossible coverage.
        DEDUPES (symbol, timeframe) so the same pair is not processed/logged twice.
        """
        if self._bootstrapped:
            return
        self._bootstrapped = True

        sim_start = _utc(sim_start)

        seen: Set[Tuple[str, int]] = set()

        for r in runners:
            sym = (getattr(r, "stock", "") or "UNKNOWN").upper()
            tf = int(getattr(r, "time_frame", 5) or 5)

            key = (sym, tf)
            if key in seen:
                # Avoid duplicate processing/logging for identical (sym, tf)
                continue
            seen.add(key)

            earliest = market.get_earliest_bar(sym, tf)
            if earliest is None:
                # No bars at all — also treat as coverage
                self.exclude_coverage(sym=sym, tf=tf, earliest=None, sim_start=sim_start, now=now)
                log.info("HealthGate: EXCLUDED %s tf=%dm (no coverage at all); TTL=%dd", sym, tf, self._ttl)
                continue

            if earliest > sim_start:
                self.exclude_coverage(sym=sym, tf=tf, earliest=earliest, sim_start=sim_start, now=now)
                log.info(
                    "HealthGate: EXCLUDED %s tf=%dm (earliest=%s > sim_start=%s); TTL=%dd",
                    sym, tf, earliest.isoformat(), sim_start.isoformat(), self._ttl
                )

    # ─────────────────────────────────────────────────────────────────────────────

    def mark_clean_pass(self, *, sym: str, tf: int) -> None:
        """Reset consecutive counters when a clean bar is processed without incidents."""
        st = self._get(sym, tf)
        st.consecutive_no_data = 0
        st.consecutive_errors = 0

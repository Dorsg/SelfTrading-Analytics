from __future__ import annotations

import os
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Iterable, Optional, Tuple, Dict, Set

from backend.ib_manager.market_data_manager import MarketDataManager

log = logging.getLogger("universe")


def _parse_date(s: str) -> date:
    return datetime.fromisoformat(s.strip()).date()


class UniverseManager:
    """
    Universe hygiene for historical sims.

    Rules:
      • If a snapshot file exists (one symbol per line), use it as the allowlist.
      • Otherwise, allow symbols whose earliest DAILY bar date <= cutoff (default 2020-09-18).
      • Explicit post-IPO symbols after cutoff can be denied via EXCLUDE_POST_IPO env.
      • Optionally exclude symbols with missing minute coverage (e.g., known gaps).
      • Provide stable alias mapping for renames (e.g., META→FB, ELV→ANTM, BALL→BLL, AXON→AAXN, etc.).
    """

    def __init__(
        self,
        *,
        cutoff: Optional[date] = None,
        snapshot_path: Optional[str] = None,
        exclude_known_post_ipo: Optional[Iterable[str]] = None,
        patch_exclude_minutes: Optional[Iterable[str]] = None,
    ) -> None:
        cutoff_str = os.getenv("UNIVERSE_CUTOFF_DATE", "2020-09-18")
        self.cutoff: date = cutoff or _parse_date(cutoff_str)

        snap_env = os.getenv("UNIVERSE_SNAPSHOT_PATH", "") or (snapshot_path or "")
        self.snapshot_path = Path(snap_env).expanduser() if snap_env else None

        # Known post-IPO names you want auto-excluded in a 2020 backtest
        env_post_ipo = os.getenv(
            "EXCLUDE_POST_IPO",
            "ABNB,APP,ARM,CEG,GFS,KVUE,WBD,VTRS,TKO,RVTY"
        )
        self._exclude_post_ipo: Set[str] = {s.strip().upper() for s in env_post_ipo.split(",") if s.strip()}

        # Alias map for renames (env-extendable)
        # Default includes the expanded set from the fix plan.
        # Format: "META:FB,ELV:ANTM,BALL:BLL,AXON:AAXN,EG:RE,WTW:WLTW,DAY:CDAY,RVTY:PKI,VTRS:MYL,WBD:DISCA"
        alias_env = os.getenv(
            "UNIVERSE_ALIAS_MAP",
            "META:FB,ELV:ANTM,BALL:BLL,AXON:AAXN,EG:RE,WTW:WLTW,DAY:CDAY,RVTY:PKI,VTRS:MYL,WBD:DISCA"
        )
        self._alias: Dict[str, str] = {}
        for pair in alias_env.split(","):
            if ":" in pair:
                k, v = pair.split(":", 1)
                k = k.strip().upper()
                v = v.strip().upper()
                if k and v:
                    self._alias[k] = v

        # State populated by ensure_loaded()
        self._loaded_syms: Set[str] = set()
        self._allowed: Set[str] = set()
        self._reason: Dict[str, str] = {}
        self._mapped: Dict[str, str] = {}

        # Optionally exclude tickers with known minute gaps (not used by default)
        self._patch_exclude_minutes: Set[str] = {s.strip().upper() for s in (patch_exclude_minutes or [])}

    def ensure_loaded(self, symbols: Iterable[str], mkt: MarketDataManager) -> None:
        syms = {str(s or "").upper() for s in symbols if str(s or "").strip()}
        self._loaded_syms = syms
        self._allowed.clear()
        self._reason.clear()
        self._mapped.clear()

        # Snapshot allowlist overrides everything when present
        snapshot_allowed: Optional[Set[str]] = None
        if self.snapshot_path and self.snapshot_path.exists():
            try:
                lines = [l.strip().upper() for l in self.snapshot_path.read_text().splitlines()]
                snapshot_allowed = {l for l in lines if l}
            except Exception:
                log.exception("Failed to read universe snapshot at %s", self.snapshot_path)

        for s in syms:
            # Manual exclusions
            if s in self._exclude_post_ipo:
                self._reason[s] = "post-IPO excluded by policy"
                continue
            if s in self._patch_exclude_minutes:
                self._reason[s] = "excluded due to known minute-data gaps"
                continue

            # Snapshot allowlist (if present)
            if snapshot_allowed is not None and s not in snapshot_allowed:
                self._reason[s] = "excluded by snapshot"
                continue

            # Alias mapping first (e.g., META→FB)
            mapped = self._alias.get(s, s)
            self._mapped[s] = mapped

            # Earliest DAILY date gate vs cutoff
            try:
                first_dt = mkt.earliest_daily_date(mapped)
            except Exception:
                first_dt = None
            if first_dt is None:
                # No coverage at all → treat as post-IPO/missing
                self._reason[s] = "no daily coverage (likely post-IPO)"
                continue
            if first_dt.date() > self.cutoff:
                self._reason[s] = f"post-IPO after cutoff {self.cutoff.isoformat()}"
                continue

            # Additional hygiene: require some minute coverage (5m) to avoid later spam
            try:
                if not mkt.has_minute_bars(mapped, 5):
                    self._reason[s] = "no minute coverage (5m)"
                    continue
            except Exception:
                # If check fails, don't block the symbol; we'll catch it during runner tick
                pass

            # Allowed
            self._allowed.add(s)
            self._reason[s] = "allowed"

        log.info(
            "Universe loaded: allowed=%d / total=%d (cutoff=%s, snapshot=%s)",
            len(self._allowed), len(syms), self.cutoff.isoformat(),
            str(self.snapshot_path) if self.snapshot_path else "none"
        )

    def is_allowed(self, symbol: str, mkt: MarketDataManager) -> bool:
        s = (symbol or "").upper()
        if not self._loaded_syms:
            # If ensure_loaded wasn't called, do a one-off evaluation
            self.ensure_loaded([s], mkt)
        return s in self._allowed

    def reason_for(self, symbol: str) -> str:
        return self._reason.get((symbol or "").upper(), "unknown")

    def map_symbol(self, symbol: str, *, as_of: Optional[datetime] = None) -> str:
        """
        Return the mapped/historical equivalent if known, else the symbol itself.
        Mapping is static; `as_of` only exists for interface symmetry and possible future use.
        """
        s = (symbol or "").upper()
        # Prefer the mapping we computed in ensure_loaded for current run; else fall back to alias map.
        return self._mapped.get(s, self._alias.get(s, s))

    def allowed_symbols(self) -> Set[str]:
        """
        Return the *mapped* symbols for currently allowed runners (used for resample pre-warm).
        """
        out: Set[str] = set()
        for s in self._allowed:
            mapped = self._mapped.get(s, self._alias.get(s, s))
            if mapped:
                out.add(mapped)
        return out

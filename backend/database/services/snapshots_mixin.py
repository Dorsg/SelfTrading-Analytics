from __future__ import annotations

from datetime import date
from sqlalchemy import func
from database.db_utils import aware_utc_now
from database.models import AccountSnapshot


class SnapshotsMixin:
    """
    Account snapshot CRUD. Requires:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
    """

    def get_today_snapshot(self, user_id: int) -> AccountSnapshot | None:
        return (
            self.db.query(AccountSnapshot)
            .filter(
                AccountSnapshot.user_id == user_id,
                func.date(AccountSnapshot.timestamp) == date.today(),
            )
            .first()
        )

    def create_account_snapshot(
        self, *, user_id: int, snapshot_data: dict
    ) -> AccountSnapshot | None:
        """
        If today’s snapshot exists → update, else insert.
        """
        existing = self.get_today_snapshot(user_id)
        if existing:
            existing.timestamp            = aware_utc_now()
            existing.total_cash_value     = snapshot_data.get("TotalCashValue (USD)")
            existing.net_liquidation      = snapshot_data.get("NetLiquidation (USD)")
            existing.available_funds      = snapshot_data.get("AvailableFunds (USD)")
            existing.buying_power         = snapshot_data.get("BuyingPower (USD)")
            existing.unrealized_pnl       = snapshot_data.get("UnrealizedPnL (USD)")
            existing.realized_pnl         = snapshot_data.get("RealizedPnL (USD)")
            existing.excess_liquidity     = snapshot_data.get("ExcessLiquidity (USD)")
            existing.gross_position_value = snapshot_data.get("GrossPositionValue (USD)")
            existing.account              = snapshot_data.get("account")
            return existing if self._commit("Update snapshot") else None

        snap = AccountSnapshot(
            user_id=user_id,
            timestamp=aware_utc_now(),
            total_cash_value     = snapshot_data.get("TotalCashValue (USD)"),
            net_liquidation      = snapshot_data.get("NetLiquidation (USD)"),
            available_funds      = snapshot_data.get("AvailableFunds (USD)"),
            buying_power         = snapshot_data.get("BuyingPower (USD)"),
            unrealized_pnl       = snapshot_data.get("UnrealizedPnL (USD)"),
            realized_pnl         = snapshot_data.get("RealizedPnL (USD)"),
            excess_liquidity     = snapshot_data.get("ExcessLiquidity (USD)"),
            gross_position_value = snapshot_data.get("GrossPositionValue (USD)"),
            account              = snapshot_data.get("account"),
        )
        self.db.add(snap)
        return snap if self._commit("Insert snapshot") else None

    def get_all_snapshots(self, *, user_id: int) -> list[AccountSnapshot]:
        return (
            self.db.query(AccountSnapshot)
            .filter(AccountSnapshot.user_id == user_id)
            .order_by(AccountSnapshot.timestamp.desc())
            .all()
        )

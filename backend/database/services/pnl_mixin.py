from __future__ import annotations

from sqlalchemy import func

from database.models import ExecutedTrade
from database.db_utils import aware_utc_now


class PnLMixin:
    """
    P&L calculations and realizations. Requires:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
      self.get_open_position_for_stock(...)
      self._update_runner_current_budget(...)
    """

    def calculate_runner_pnl(
        self,
        *,
        user_id: int,
        runner_id: int,
        symbol: str,
        current_price: float | None,
    ) -> float:
        """
        Return **total** P&L for one runner:
            realised  + unrealised (long-only).
        """
        realised = (
            self.db.query(func.coalesce(func.sum(ExecutedTrade.pnl_amount), 0.0))
            .filter(
                ExecutedTrade.runner_id == runner_id,
                ExecutedTrade.pnl_amount.isnot(None),
            )
            .scalar()
            or 0.0
        )

        unrealised = 0.0
        if current_price is not None:
            pos = self.get_open_position_for_stock(
                user_id=user_id,
                symbol=symbol.upper(),
            )
            if pos:
                unrealised = (current_price - pos.avg_price) * pos.quantity

        return round(realised + unrealised, 2)

    def calculate_runner_performance(
        self,
        *,
        user_id: int,
        runner_id: int,
        symbol: str,
        budget: float,
        current_price: float | None,
    ) -> dict[str, float]:
        """
        Return %-based performance numbers, each rounded to 2 dp:
          • realized_pct
          • unrealized_pct
          • total_pct
        """
        try:
            realized_amt = (
                self.db.query(func.coalesce(func.sum(ExecutedTrade.pnl_amount), 0.0))
                .filter(
                    ExecutedTrade.runner_id == runner_id,
                    ExecutedTrade.pnl_amount.isnot(None),
                )
                .scalar()
                or 0.0
            )
        except Exception as e:
            # Log the error and return safe defaults
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to calculate realized PnL for runner {runner_id}: {e}")
            realized_amt = 0.0

        unrealized_amt = 0.0
        if current_price is not None and budget > 0:
            pos = self.get_open_position_for_stock(
                user_id=user_id,
                symbol=symbol.upper(),
            )
            if pos:
                unrealized_amt = (current_price - pos.avg_price) * pos.quantity

        if budget <= 0:
            return dict(realized_pct=0.0, unrealized_pct=0.0, total_pct=0.0)

        realized_pct   = round(realized_amt   / budget * 100, 2)
        unrealized_pct = round(unrealized_amt / budget * 100, 2)
        total_pct      = round(realized_pct + unrealized_pct, 2)

        return dict(
            realized_pct=realized_pct,
            unrealized_pct=unrealized_pct,
            total_pct=total_pct,
        )

    def _recalculate_pnl(self, *, runner_id: int, symbol: str) -> None:
        """
        Walk every trade (FIFO) for one `(runner_id, symbol)` pair and attach
        realised P&L **including commissions**.
        """
        trades = (
            self.db.query(ExecutedTrade)
            .filter(ExecutedTrade.runner_id == runner_id,
                    ExecutedTrade.symbol    == symbol)
            .order_by(ExecutedTrade.fill_time.asc())
            .all()
        )

        open_qty  = 0.0   # running open position
        open_cost = 0.0   # total $ cost basis (price*qty + buy fees)

        for t in trades:
            qty = float(t.quantity)
            fee = abs(t.commission or 0.0)

            if t.action == "BUY":
                open_cost += qty * t.price + fee
                open_qty  += qty
                t.pnl_amount = t.pnl_percent = None

            elif t.action == "SELL":
                if open_qty == 0:
                    t.pnl_amount = t.pnl_percent = None
                    continue

                closed_qty = min(qty, open_qty)
                avg_cost   = open_cost / open_qty
                proceeds   = (t.price * closed_qty) - fee

                realised   = proceeds - (avg_cost * closed_qty)
                pct        = realised / (avg_cost * closed_qty) if avg_cost else 0.0

                t.pnl_amount  = round(realised, 4)
                t.pnl_percent = round(pct,      6)

                open_qty  -= closed_qty
                open_cost -= avg_cost * closed_qty

            else:
                t.pnl_amount = t.pnl_percent = None

        self._commit("Recalculate realised P&L (with commissions)")
        self._update_runner_current_budget(runner_id=runner_id)

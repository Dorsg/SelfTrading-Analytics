from __future__ import annotations

import logging
from typing import Sequence

from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import OpenPosition, Runner

logger = logging.getLogger(__name__)


class PositionsMixin:
    """
    Open positions management. Requires:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
    """

    def update_open_positions(self, *, user_id: int, positions: list[dict]) -> None:
        """
        Replace the user’s open_positions snapshot, retrying once on lost connections.
        Uses synchronize_session=False for speed / less memory.
        """
        from sqlalchemy.exc import OperationalError, DisconnectionError, UnboundExecutionError

        def _do():
            # Capture current symbols before replacing snapshot to detect new entries
            existing_symbols = {
                p.symbol.upper()
                for p in self.db.query(OpenPosition.symbol).filter(OpenPosition.user_id == user_id).all()
            }

            self.db.query(OpenPosition).filter(OpenPosition.user_id == user_id)\
                .delete(synchronize_session=False)

            self.db.bulk_save_objects(
                OpenPosition(
                    user_id=user_id,
                    symbol=p["symbol"],
                    quantity=p["quantity"],
                    avg_price=p["avgCost"],
                    account=p["account"],
                )
                for p in positions
            )
            self._commit("Update open positions")

            # Detect symbols that just became open positions and bump runner entry counters
            try:
                new_symbols = {str(p.get("symbol", "")).upper() for p in positions if (p.get("quantity") or 0) > 0}
                newly_opened = new_symbols - existing_symbols
                if newly_opened:
                    for sym in newly_opened:
                        runner = (
                            self.db.query(Runner)
                            .filter(
                                Runner.user_id == user_id,
                                func.upper(Runner.stock) == sym,
                            )
                            .first()
                        )
                        if not runner:
                            continue
                        params = dict(runner.parameters or {})
                        params["entry_count"] = int(params.get("entry_count") or 0) + 1
                        runner.parameters = params
                    self._commit("Increment runner entry_count on new positions")
            except Exception:
                # Do not fail position sync if counter update fails
                logger.warning("Failed to update entry_count for some runners", exc_info=True)

        try:
            _do()
        except (OperationalError, DisconnectionError, UnboundExecutionError):
            logger.warning("update_open_positions – connection died, retrying once on a new engine")
            self.db.rollback()
            import database.db_core as dbc
            dbc.rebuild_engine()
            _do()

    def get_open_position_for_stock(
        self, *, user_id: int, symbol: str
    ) -> OpenPosition | None:
        return (
            self.db.query(OpenPosition)
            .filter(OpenPosition.user_id == user_id, OpenPosition.symbol == symbol)
            .first()
        )

    def get_open_positions(self, *, user_id: int) -> Sequence[OpenPosition]:
        return (
            self.db.query(OpenPosition)
            .filter(OpenPosition.user_id == user_id)
            .all()
        )

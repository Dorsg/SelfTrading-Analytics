from __future__ import annotations

import logging
from typing import List, Sequence

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from database.db_utils import aware_utc_now
from database.models import ExecutedTrade, OpenPosition, Order, Runner

logger = logging.getLogger(__name__)


class RunnersMixin:
    """
    Runner lifecycle and helpers. Requires:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
      self.get_open_position_for_stock(...)
      self.get_open_buy_orders_for_stock(...)
    """

    def create_runner(self, *, user_id: int, data: dict) -> Runner:
        stock = (data.get("stock") or "").upper()
        if self.has_runner_for_stock(user_id=user_id, stock=stock, include_removed=False):
            raise ValueError("Runner for this stock already exists")

        runner = Runner(user_id=user_id, **data)
        runner.current_budget = runner.budget  # initialise

        self.db.add(runner)
        try:
            self.db.commit()
            logger.info("Create runner – OK")
            return runner
        except IntegrityError:
            self.db.rollback()
            raise ValueError("Runner name already exists")
        except Exception:
            self.db.rollback()
            logger.exception("Create runner – FAILED")
            raise

    def get_or_create_system_runner_id(self, user_id: int) -> int:
        """
        Return the id of a hidden per-user 'system/unassigned' runner that we can
        safely attach orphan (no-runner) executions/orders to so NOT NULL FKs are
        respected.

        The runner is activation='removed' so it never participates in logic.
        """
        name = "__system_unassigned__"
        row = (
            self.db.query(Runner.id)
            .filter(
                Runner.user_id == user_id,
                Runner.name == name,
                Runner.strategy == "__system__",
            )
            .first()
        )
        if row:
            return row.id

        sys_runner = Runner(
            user_id=user_id,
            name=name,
            strategy="__system__",
            budget=0.0,
            current_budget=0.0,
            stock="__NONE__",
            time_frame=0,
            time_range_from=None,
            time_range_to=None,
            exit_strategy="none",
            activation="removed",
            parameters={},
        )
        self.db.add(sys_runner)
        if not self._commit("Create __system_unassigned__ runner"):
            # try to re-read in case of race between parallel writers
            row2 = (
                self.db.query(Runner.id)
                .filter(
                    Runner.user_id == user_id,
                    Runner.name == name,
                    Runner.strategy == "__system__",
                )
                .first()
            )
            if row2:
                return row2.id
            raise RuntimeError("Failed to create system runner")

        return sys_runner.id

    def delete_runners(self, *, user_id: int, ids: List[int]) -> int:
        rows = (
            self.db.query(Runner)
            .filter(Runner.user_id == user_id, Runner.id.in_(ids))
            .delete(synchronize_session=False)
        )
        self._commit(f"Delete {rows} runner(s)")
        return rows

    def update_runners_activation(
        self, *, user_id: int, ids: List[int], activation: str
    ) -> int:
        rows = (
            self.db.query(Runner)
            .filter(Runner.user_id == user_id, Runner.id.in_(ids))
            .update(
                {"activation": activation, "updated_at": aware_utc_now()},
                synchronize_session=False,
            )
        )
        self._commit(f"{activation.capitalize()} {rows} runner(s)")
        return rows

    def get_active_runners(self, *, user_id: int) -> Sequence[Runner]:
        return (
            self.db.query(Runner)
            .filter(Runner.user_id == user_id, Runner.activation == "active")
            .all()
        )

    def get_existing_runner_id(self, user_id: int) -> int | None:
        runner = (
            self.db.query(Runner.id)
            .filter(Runner.user_id == user_id)
            .order_by(Runner.created_at.asc())
            .first()
        )
        return runner.id if runner else None

    def get_runners_by_ids(
        self, *, user_id: int, ids: Sequence[int]
    ) -> Sequence[Runner]:
        return (
            self.db
            .query(Runner)
            .filter(Runner.user_id == user_id, Runner.id.in_(ids))
            .all()
        )

    def get_runners_by_user(self, user_id: int) -> Sequence[Runner]:
        """
        Light‑weight projection used by hot‑path helpers.
        """
        return (
            self.db.query(Runner.id, Runner.activation)
            .filter(Runner.user_id == user_id)
            .all()
        )

    def _update_runner_current_budget(self, *, runner_id: int) -> None:
        """
        Re-compute live `current_budget` for one runner.
        """
        runner = (
            self.db.query(Runner)
            .filter(Runner.id == runner_id)
            .one_or_none()
        )
        if not runner:
            return

        # 1) inventory already on the books
        pos = self.get_open_position_for_stock(
            user_id=runner.user_id, symbol=runner.stock
        )
        invested = (pos.avg_price * pos.quantity) if pos else 0.0

        # 2) cash tied up in still-live BUY orders
        pending_orders = self.get_open_buy_orders_for_stock(
            user_id=runner.user_id, symbol=runner.stock
        )
        reserved = sum(
            (o.limit_price or 0.0) * o.quantity for o in pending_orders
        )

        # 3) realised P&L on completed round-trips
        realised = (
            self.db.query(func.coalesce(func.sum(ExecutedTrade.pnl_amount), 0.0))
            .filter(
                ExecutedTrade.runner_id == runner_id,
                ExecutedTrade.pnl_amount.isnot(None),
            )
            .scalar()
            or 0.0
        )

        # Remaining cash available for the runner.
        # Return principal on SELL and incorporate realised P&L (both profit and loss).
        # This matches the live decision path in `calculate_budget`.
        left = runner.budget - invested - reserved + realised

        runner.current_budget = max(0.0, left)
        runner.updated_at     = aware_utc_now()
        self._commit("Update runner.current_budget (clamped)")

    def soft_remove_runners(self, *, user_id: int, ids: List[int]) -> int:
        """
        Mark runners as **removed** instead of deleting them so all FK links
        remain valid.
        """
        utc_now = aware_utc_now()

        runners = (
            self.db.query(Runner)
                   .filter(Runner.user_id == user_id, Runner.id.in_(ids))
                   .all()
        )
        if not runners:
            logger.info("Soft-remove – no matching runner ids=%s for user=%d", ids, user_id)
            return 0

        taken_names = {
            n for (n,) in (
                self.db.query(Runner.name)
                       .filter(Runner.user_id == user_id)
                       .all()
            )
        }

        changed = 0
        for r in runners:
            base = r.name.split(" (removed")[0]

            if f"{base} (removed)" not in taken_names:
                new_name = f"{base} (removed)"
            else:
                i = 2
                while True:
                    candidate = f"{base} (removed {i})"
                    if candidate not in taken_names:
                        new_name = candidate
                        break
                    i += 1

            taken_names.add(new_name)
            r.name        = new_name
            r.activation  = "removed"
            r.updated_at  = utc_now
            changed      += 1

        ok = self._commit(f"Soft-removed {changed} runner(s)")
        return changed if ok else 0

    def has_runner_for_stock(
        self, *, user_id: int, stock: str, include_removed: bool = False
    ) -> bool:
        q = (
            self.db.query(Runner.id)
            .filter(
                Runner.user_id == user_id,
                func.upper(Runner.stock) == stock.upper(),
            )
        )
        if not include_removed:
            q = q.filter(func.coalesce(Runner.activation, "active") != "removed")

        return self.db.query(q.exists()).scalar()

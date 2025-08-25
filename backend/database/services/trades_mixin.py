from __future__ import annotations

import json
from typing import List

from sqlalchemy.dialects.postgresql import insert

from database.db_utils import canonical_cycle_seq
from database.models import ExecutedTrade, Order, RunnerExecution


class TradesMixin:
    """
    Execution sync & reads. Requires:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
      self.get_runners_by_user(user_id)
      self._recalculate_pnl(runner_id, symbol)
    """

    def sync_executed_trades(self, trades: list[dict]) -> None:
        """
        • Up-sert every new IBKR execution **once per permId** – all partial
          fills are aggregated into a single row (qty, commission summed).
        • Exactly **one** `RunnerExecution` row is written per order.
        • Refresh budgets/P&L for touched runners.
        """
        trades = [t for t in trades if t.get("perm_id")]
        if not trades:
            return

        user_id = trades[0]["user_id"]

        valid_runner_ids = {
            r.id
            for r in self.get_runners_by_user(user_id)
            if (r.activation or "").lower() != "removed"
        }
        for t in trades:
            if t.get("runner_id") not in valid_runner_ids:
                t["runner_id"] = None

        buckets: dict[int, dict] = {}
        for t in trades:
            b = buckets.setdefault(
                t["perm_id"],
                {
                    **{k: v for k, v in t.items() if k not in {"quantity", "commission", "price", "fill_time"}},
                    "quantity": 0,
                    "commission": 0.0,
                    "price": t["price"],
                    "fill_time": t["fill_time"],
                },
            )
            b["quantity"]   += t["quantity"]
            b["commission"] += abs(t["commission"] or 0.0)

            if t["fill_time"] > b["fill_time"]:
                b["fill_time"] = t["fill_time"]

        aggregated: list[dict] = list(buckets.values())

        if self.db.bind.dialect.name == "postgresql":
            ins = insert(ExecutedTrade).values(aggregated)
            up_cols = {
                c.name: getattr(ins.excluded, c.name)
                for c in ExecutedTrade.__table__.columns
                if c.name not in ("id", "perm_id", "price")  # PK columns
            }
            self.db.execute(
                ins.on_conflict_do_update(
                    constraint="uix_perm_id_price",
                    set_=up_cols,
                )
            )
        else:
            for row in aggregated:
                obj = (
                    self.db.query(ExecutedTrade)
                    .filter(
                        ExecutedTrade.perm_id == row["perm_id"],
                        ExecutedTrade.price == row["price"],
                    )
                    .first()
                )
                if obj:
                    for k, v in row.items():
                        setattr(obj, k, v)
                else:
                    self.db.add(ExecutedTrade(**row))

        # 1 RunnerExecution ➜ trade_executed per order (perm_id)
        for t in aggregated:
            rid = t.get("runner_id")
            if not rid:
                continue

            has_exec_row = (
                self.db.query(RunnerExecution)
                .filter(
                    RunnerExecution.perm_id == t["perm_id"],
                    RunnerExecution.status == "trade_executed",
                )
                .first()
            )
            if has_exec_row:
                continue

            self.db.add(
                RunnerExecution(
                    user_id   = t["user_id"],
                    runner_id = rid,
                    perm_id   = t["perm_id"],
                    cycle_seq = canonical_cycle_seq(t["perm_id"]),
                    status    = "trade_executed",
                    symbol    = t["symbol"],
                    execution_time = t["fill_time"],
                    details   = json.dumps(
                        {
                            "quantity": t["quantity"],
                            "price": t["price"],
                            "action": t["action"],
                        },
                        default=str,
                    ),
                )
            )

        self._commit(f"Sync {len(aggregated)} aggregated execution(s)")

        touched = {
            (t["runner_id"], t["symbol"])
            for t in aggregated
            if t.get("runner_id") and t.get("symbol")
        }
        for rid, sym in touched:
            self._recalculate_pnl(runner_id=rid, symbol=sym)

    # Reads for trades (with runner info)
    def get_all_executed_trades(self, *, user_id: int):
        from sqlalchemy.orm import joinedload

        return (
            self.db.query(ExecutedTrade)
            .options(
                joinedload(ExecutedTrade.order).joinedload(Order.runner)
            )
            .filter(ExecutedTrade.user_id == user_id)
            .order_by(ExecutedTrade.fill_time.desc())
            .all()
        )

    def get_runner_trades(
        self, *, user_id: int, runner_id: int
    ):
        subq = (
            self.db.query(Order.ibkr_perm_id)
            .filter(Order.user_id == user_id, Order.runner_id == runner_id)
            .subquery()
        )
        return (
            self.db.query(ExecutedTrade)
            .filter(ExecutedTrade.user_id == user_id, ExecutedTrade.perm_id.in_(subq))
            .order_by(ExecutedTrade.fill_time.desc())
            .all()
        )

    # Hard purge
    def purge_runner_history(
        self, *, user_id: int, ids: list[int]
    ) -> dict[str, int]:
        """
        Irrevocably delete *all* artefacts linked to the supplied runner IDs.
        """
        counts: dict[str, int] = {}

        counts["executed_trades"] = (
            self.db.query(ExecutedTrade)
            .filter(ExecutedTrade.user_id == user_id,
                    ExecutedTrade.runner_id.in_(ids))
            .delete(synchronize_session=False)
        )

        counts["runner_executions"] = (
            self.db.query(RunnerExecution)
            .filter(RunnerExecution.user_id == user_id,
                    RunnerExecution.runner_id.in_(ids))
            .delete(synchronize_session=False)
        )

        counts["orders"] = (
            self.db.query(Order)
            .filter(Order.user_id == user_id,
                    Order.runner_id.in_(ids))
            .delete(synchronize_session=False)
        )

        from database.models import Runner
        counts["runners"] = (
            self.db.query(Runner)
            .filter(Runner.user_id == user_id,
                    Runner.id.in_(ids))
            .delete(synchronize_session=False)
        )

        self._commit(f"Hard-purged runners={ids}")
        return counts

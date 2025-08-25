from __future__ import annotations

import json
from datetime import timezone
from typing import List

from dateutil import tz
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import exists as sa_exists

from backend.utils import now_et
from database.db_utils import aware_utc_now, canonical_cycle_seq
from database.models import ExecutedTrade, Order, Runner, RunnerExecution


class OrdersMixin:
    """
    Orders CRUD & sync. Requires:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
      self.get_runners_by_user(user_id)
      self.get_or_create_system_runner_id(user_id)
    """

    def save_order(self, order_data: dict) -> Order | None:
        """
        Idempotent insert/update (“upsert”) of a single order row keyed by
        `ibkr_perm_id`. Also guarantees a NOT NULL `order_type`.
        """
        if "user_id" not in order_data:
            raise ValueError("order_data must include user_id")

        data = dict(order_data)  # never mutate caller

        ts = data.pop("submitted_time", None)
        if ts:
            data["created_at"]   = ts
            data["last_updated"] = ts

        if "perm_id" in data:
            data["ibkr_perm_id"] = data.pop("perm_id")

        if "filled_qty" in data:
            data["filled_quantity"] = data.pop("filled_qty")

        for k in ("child_perm_id", "child_status", "trail_percent"):
            data.pop(k, None)

        allowed = {c.name for c in Order.__table__.columns}
        cleaned  = {k: v for k, v in data.items() if k in allowed}

        cleaned.setdefault("created_at", aware_utc_now())
        cleaned.setdefault("last_updated", aware_utc_now())

        if not cleaned.get("order_type"):
            cleaned["order_type"] = "LMT" if cleaned.get("limit_price") is not None else "MKT"

        perm_id = cleaned.get("ibkr_perm_id")
        if perm_id is None:
            raise ValueError("order_data must include ibkr_perm_id / perm_id")

        if self.db.bind.dialect.name == "postgresql":
            ins = insert(Order).values(cleaned)
            up_cols = {
                "runner_id": func.coalesce(ins.excluded.runner_id, Order.runner_id),
                **{
                    c.name: getattr(ins.excluded, c.name)
                    for c in Order.__table__.columns
                    if c.name not in ("id", "ibkr_perm_id", "created_at")
                },
            }

            self.db.execute(
                ins.on_conflict_do_update(
                    index_elements=["ibkr_perm_id"],
                    set_=up_cols,
                )
            )
            ok = self._commit("Upsert order")
            if not ok:
                return None

            return (
                self.db.query(Order)
                .filter(Order.ibkr_perm_id == perm_id)
                .one()
            )

        obj = (
            self.db.query(Order)
            .filter(Order.ibkr_perm_id == perm_id)
            .first()
        )
        if obj:
            for k, v in cleaned.items():
                if k in ("id", "ibkr_perm_id"):
                    continue
                setattr(obj, k, v)
        else:
            obj = Order(**cleaned)
            self.db.add(obj)

        return obj if self._commit("Upsert order (fallback)") else None

    def get_open_order_for_stock(self, *, user_id: int, symbol: str) -> Order | None:
        return (self.db.query(Order)
                .filter(Order.user_id == user_id,
                        Order.symbol == symbol,
                        Order.status.notin_(("Filled", "Cancelled")))
                .first())

    def sync_orders(self, user_id: int, orders: List[dict]) -> None:
        """
        Up‑sert today’s IBKR orders into our DB and purge obsolete rows.

        IMPORTANT:
        - runner_executions.runner_id is NOT NULL in the schema.
        - Orders that originate outside of any runner must still produce a valid
        RunnerExecution row; we therefore attach them to a hidden per-user
        system runner (__system_unassigned__).
        """
        tz_et = tz.gettz("America/New_York")
        today_start_et = now_et().astimezone(tz_et).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        cutoff_utc = today_start_et.astimezone(timezone.utc)

        # Purge prior-day orders that never produced trades
        purged_old = (
            self.db.query(Order)
            .filter(
                Order.user_id == user_id,
                Order.created_at < cutoff_utc,
                ~sa_exists().where(ExecutedTrade.perm_id == Order.ibkr_perm_id),
            )
            .delete(synchronize_session=False)
        )

        valid_runner_ids = {
            r.id for r in self.get_runners_by_user(user_id)
            if (r.activation or "").lower() != "removed"
        }

        system_runner_id: int | None = None
        todays: list[dict] = []

        for o in orders:
            ts = o.get("last_updated")
            if ts is None:
                continue
            if ts.astimezone(tz_et).date() != today_start_et.date():
                continue

            rid = o.get("runner_id")
            if rid not in valid_runner_ids:
                o["runner_id"] = None

            o.setdefault("created_at", ts)
            o["last_updated"] = ts
            todays.append(o)

        if not todays:
            removed_today = (
                self.db.query(Order)
                .filter(Order.user_id == user_id, Order.created_at >= cutoff_utc)
                .delete(synchronize_session=False)
            )
            self._commit(
                f"Purged {purged_old} old order(s); "
                f"removed {removed_today} order(s) from today – no eligible orders"
            )
            return

        perm_ids = [o["ibkr_perm_id"] for o in todays]

        if self.db.bind.dialect.name == "postgresql":
            ins = insert(Order).values(todays)
            up_cols = {
                "runner_id": func.coalesce(ins.excluded.runner_id, Order.runner_id),
                **{
                    c.name: getattr(ins.excluded, c.name)
                    for c in Order.__table__.columns
                    if c.name not in ("id", "ibkr_perm_id", "created_at")
                },
            }
            self.db.execute(
                ins.on_conflict_do_update(
                    index_elements=["ibkr_perm_id"],
                    set_=up_cols,
                )
            )
        else:
            for data in todays:
                obj = (
                    self.db.query(Order)
                    .filter(Order.ibkr_perm_id == data["ibkr_perm_id"])
                    .first()
                )
                if obj:
                    for k, v in data.items():
                        if k == "runner_id" and v is None and obj.runner_id:
                            continue
                        setattr(obj, k, v)
                else:
                    self.db.add(Order(**data))

        removed_missing = (
            self.db.query(Order)
            .filter(
                Order.user_id == user_id,
                Order.created_at >= cutoff_utc,
                Order.ibkr_perm_id.notin_(perm_ids),
            )
            .delete(synchronize_session=False)
        )

        # Backfill RunnerExecution(order_placed) where missing
        for o in todays:
            pid = o["ibkr_perm_id"]

            exists = (
                self.db.query(RunnerExecution.id)
                .filter(
                    RunnerExecution.perm_id == pid,
                    RunnerExecution.status == "order_placed",
                )
                .first()
            )
            if exists:
                continue

            rid = o.get("runner_id")
            if rid is None:
                if system_runner_id is None:
                    try:
                        system_runner_id = self.get_or_create_system_runner_id(user_id)
                    except Exception:
                        import logging
                        logging.exception(
                            "Failed creating system runner; skipping backfill for permId=%s", pid
                        )
                        continue
                rid = system_runner_id

            if rid is None:
                import logging
                logging.warning(
                    "Skipping order_placed backfill for permId=%s – no runner_id and system runner unavailable",
                    pid
                )
                continue

            self.db.add(
                RunnerExecution(
                    user_id        = user_id,
                    runner_id      = rid,
                    perm_id        = pid,
                    cycle_seq      = canonical_cycle_seq(pid),
                    status         = "order_placed",
                    symbol         = o.get("symbol"),
                    execution_time = o.get("last_updated") or aware_utc_now(),
                    limit_price    = o.get("limit_price"),
                    strategy       = None,
                    details        = json.dumps(o, default=str),
                )
            )

        self._commit(
            f"Up‑serted {len(todays)} eligible order(s); "
            f"removed {removed_missing} vanished order(s) from today; "
            f"purged {purged_old} prior‑day order(s) + backfilled order_placed safely"
        )

    def delete_orders_by_perm_ids(self, *, user_id: int, perm_ids: list[int]) -> int:
        if not perm_ids:
            return 0

        rows = (
            self.db.query(Order)
            .filter(
                Order.user_id == user_id,
                Order.ibkr_perm_id.in_(perm_ids),
                ~sa_exists().where(ExecutedTrade.perm_id == Order.ibkr_perm_id),
            )
            .delete(synchronize_session=False)
        )
        self._commit(f"Deleted {rows} order(s) by perm_ids (without trades)")
        return rows

    # Reads
    def get_all_orders(self, *, user_id: int):
        return (
            self.db.query(Order)
            .options(joinedload(Order.runner))
            .filter(Order.user_id == user_id)
            .order_by(Order.created_at.desc())
            .all()
        )

    def get_open_buy_orders_for_stock(self, *, user_id: int, symbol: str) -> list[Order]:
        """
        Return every *BUY* order for this symbol that is still live
        (status NOT Filled / Cancelled / Inactive).
        """
        return (
            self.db.query(Order)
            .filter(
                Order.user_id == user_id,
                Order.symbol == symbol,
                Order.action == "BUY",
                Order.status.notin_(("Filled", "Cancelled", "Inactive")),
            )
            .all()
        )

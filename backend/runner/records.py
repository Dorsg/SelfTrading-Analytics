from __future__ import annotations
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from database.db_manager import DBManager, canonical_cycle_seq
from backend.runner.types import ExecutionStatus

def record_exec(
    db: DBManager,
    *,
    user_id: int,
    runner_id: int,
    status: ExecutionStatus | str,
    cycle_seq: str | None = None,
    ts: datetime | None = None,
    symbol: str | None = None,
    reason: str | None = None,
    details: str | dict | None = None,
    perm_id: int | None = None,
    **extra,
) -> None:
    if cycle_seq is None:
        cycle_seq = canonical_cycle_seq(perm_id) if perm_id is not None else uuid.uuid4().hex

    payload: Dict[str, Any] = {
        "user_id":        user_id,
        "runner_id":      runner_id,
        "cycle_seq":      cycle_seq,
        "status":         status.value if hasattr(status, "value") else str(status),
        "execution_time": ts or datetime.now(timezone.utc),
    }
    if symbol:
        payload["symbol"] = symbol
    if perm_id is not None:
        payload["perm_id"] = perm_id
    if reason:
        payload["reason"] = reason
    if details is not None:
        payload["details"] = (
            json.dumps(details, default=str)
            if isinstance(details, (dict, list))
            else str(details)
        )
    payload.update({k: v for k, v in extra.items() if v is not None})

    db.save_runner_execution(payload)

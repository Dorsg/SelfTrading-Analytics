from datetime import datetime, timezone
from database.db_manager import DBManager

def test_upsert_idempotent():
    with DBManager() as db:
        u = db.get_or_create_user("analytics", "a@a", "x")
        uid = u.id
        ts = datetime(2020,1,1,14,30, tzinfo=timezone.utc)
        rows = [{
            "runner_id": 1,
            "user_id": uid,
            "symbol": "AAPL",
            "strategy": "chatgpt_5_strategy",
            "status": "completed",
            "reason": "no_action",
            "details": None,
            "execution_time": ts,
            "cycle_seq": int(ts.timestamp()),
            "timeframe": 5,
        }]
        db.bulk_upsert_runner_executions(rows)
        # Update same natural key -> should UPDATE, not duplicate
        rows[0]["status"] = "completed_2"
        db.bulk_upsert_runner_executions(rows)

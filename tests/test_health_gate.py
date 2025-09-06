from datetime import datetime, timezone, timedelta
from backend.analytics.health_gate import HealthGate

def _utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)

def test_health_gate_degrade_and_exclude():
    h = HealthGate(ttl_days=1, degrade_threshold=3, exclude_threshold_sessions=4, window_days=5)
    now = _utc("2020-01-01T15:00:00")

    # 3 consecutive no_data -> DEGRADED
    for _ in range(3):
        h.note_no_data(sym="CMCSA", tf=5, now=now, et_day="2020-01-01")
    excluded, _ = h.is_excluded("CMCSA", 5, now)
    assert not excluded

    # accumulate to exclude threshold across days
    h.note_no_data(sym="CMCSA", tf=5, now=now + timedelta(days=1), et_day="2020-01-02")
    excluded, reason = h.is_excluded("CMCSA", 5, now + timedelta(days=1))
    assert excluded and reason == "errors_over_sessions"

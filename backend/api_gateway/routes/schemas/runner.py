# api_gateway/routes/schemas/runner.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, PositiveFloat, PositiveInt, field_validator, ConfigDict
from dateutil import parser as dtparser

class RunnerCreate(BaseModel):
    # ─────────── fields expected from UI ───────────
    id: Optional[int] = None              # ignored by backend
    created_at: Optional[datetime] = None # ignored by backend

    name: str
    strategy: str
    budget: PositiveFloat
    stock: str
    time_frame: PositiveInt

    # ─── new free-form container for strategy params ───
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Strategy-specific parameters (e.g. stop_loss, take_profit, commission_ratio)",
    )

    time_range_from: Optional[datetime] = None
    time_range_to:   Optional[datetime] = None

    exit_strategy: str
    activation: str = "active"

    # accept unknown keys but ignore them
    model_config = ConfigDict(extra="ignore")

    # ─────────── validators ───────────
    @field_validator("stock")
    @classmethod
    def _upper_ticker(cls, v: str) -> str:
        return v.upper()

    @field_validator(
        "time_range_from",
        "time_range_to",
        "created_at",
        mode="before",
    )
    @classmethod
    def _parse_dt(cls, v):
        """
        Accepts:
            • null / ""                → None
            • milliseconds since epoch → aware-UTC datetime
            • ISO string               → parsed; if no tz, assume UTC
            • datetime                 → left as-is if tz-aware, else set UTC
        """
        if v is None or (isinstance(v, str) and not v.strip()):
            return None

        # 1️⃣ epoch milliseconds from JS date-picker
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)

        # 2️⃣ already a datetime object
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)

        # 3️⃣ ISO string
        try:
            dt = dtparser.parse(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise ValueError(f"Invalid datetime value: {v!r}") from exc


class RunnerIds(BaseModel):
    ids: list[int] = Field(..., min_items=1, description="Runner IDs to act on")

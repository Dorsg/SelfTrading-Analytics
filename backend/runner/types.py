from __future__ import annotations
from enum import Enum

class ExecutionStatus(str, Enum):
    STARTED                 = "started"
    SKIPPED_NOT_IN_TIME     = "skipped_not_in_time"
    SKIPPED_BUILD_FAILED    = "skipped_build_decision_info"
    SKIPPED_OPEN_ORDER      = "skipped_existing_open_order"
    RUNNER_REMOVED          = "runner_removed"
    SKIPPED_NO_FUNDS        = "skipped_no_funds"
    NO_BUY_ACTION           = "no_buy_action"
    NO_SELL_ACTION          = "no_sell_action"
    DECISION_MADE           = "decision_made"
    TRADE_EXECUTED          = "trade_executed"
    ORDER_PLACE_FAILED      = "order_place_failed"
    ORDER_PLACED            = "order_placed"
    DEACTIVATE_FLAT         = "deactivate_flat"
    DEACTIVATE_NO_POSITION  = "deactivate_no_position"
    ORDER_NOT_FILLED        = "order_not_filled"
    TIME_EXIT_SELL          = "time_exit_sell"
    TIME_EXIT_NO_POSITION   = "time_exit_no_position"
    ERROR                   = "error"

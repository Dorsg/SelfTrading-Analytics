from backend.strategies.basic_strategy import BasicStrategy
from backend.strategies.base import Strategy
from backend.strategies.chatgpt_5_strategy import ChatGPT5Strategy
from backend.strategies.below_above_strategy import BelowAboveStrategy
from backend.strategies.grok_4_strategy import Grok4Strategy
from backend.strategies.fibonacci_yuval import FibonacciYuvalStrategy


def select_strategy(runner) -> Strategy:
    match runner.strategy.lower():
        case "test":
            return BasicStrategy()
        case "triple_top_break":
            return ChatGPT5Strategy()
        case "below_above":
            return BelowAboveStrategy()
        case "chatgpt_5_strategy":
            return ChatGPT5Strategy()
        case "grok_4_strategy":
            return Grok4Strategy()
        case "fibonacci_yuval" | "fibonacci yuval":
            return FibonacciYuvalStrategy()
        case _:
            raise ValueError(f"Unknown strategy '{runner.strategy}'")

"""
Trading strategies package.

This package contains all trading strategy implementations.
Strategies are automatically discovered by factory.py when they:
1. Are placed in this directory
2. Define a class with decide_buy() and decide_sell() methods
3. Have a unique module filename ending with '_strategy.py'
"""

# Import all strategies for easier access
from backend.strategies.chatgpt_5_ultra_strategy import ChatGPT5UltraStrategy
from backend.strategies.gemini_2_5_pro_strategy import Gemini25ProStrategy
from backend.strategies.claude_4_5_sonnet_strategy import Claude45SonnetStrategy
from backend.strategies.deepseek_v3_1_strategy import DeepSeekV31Strategy

__all__ = [
    'ChatGPT5UltraStrategy',
    'Grok4Strategy', 
    'Gemini25ProStrategy',
    'Claude45SonnetStrategy',
    'DeepSeekV31Strategy'
]

from __future__ import annotations

"""
Dynamic strategy discovery & resolution.

Drop a new strategy file under backend/strategies/ that defines a class with:
  - decide_buy(self, info)
  - decide_sell(self, info)
  - optional: name (for nicer aliasing)
  - optional: aliases (list[str]) extra keys mapping to this strategy

This module will:
  • Discover it automatically
  • Expose a stable canonical key based on the module file name (e.g., "grok_4_strategy")
  • Allow common aliases ("grok4", "grok", snake-cased class name, etc.)
"""

import importlib
import inspect
import os
import pkgutil
import re
import logging
from types import ModuleType
from typing import Type, Any, Optional

# Logger for this module
log = logging.getLogger("strategy-factory")

# Internal registries
_CLASSES: dict[str, Type[Any]] = {}   # canonical_key -> class
_ALIASES: dict[str, str] = {}         # alias -> canonical_key
_DISCOVERED: bool = False

# Files to ignore when importing strategy modules
_SKIP_MODULES = {
    "factory",
    "contracts",
    "runner_decision_info",
    "explain",
    "__init__",
}


def _snake_case(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    out = re.sub("__+", "_", s2).lower()
    return out


def _ensure_strategy_suffix(key: str) -> str:
    k = key.lower()
    if not k.endswith("_strategy"):
        k = f"{k}_strategy"
    return k


def _add_alias(alias: str, canonical: str) -> None:
    alias = alias.strip().lower()
    if not alias:
        return
    _ALIASES[alias] = canonical


def _discover() -> None:
    """Populate _CLASSES and _ALIASES once per process."""
    global _DISCOVERED
    if _DISCOVERED:
        return

    pkg_dir = os.path.dirname(__file__)
    pkg_name = __package__  # "backend.strategies"

    for modinfo in pkgutil.iter_modules([pkg_dir]):
        mod_name = modinfo.name
        if mod_name in _SKIP_MODULES or mod_name.startswith("_"):
            continue

        try:
            mod: ModuleType = importlib.import_module(f"{pkg_name}.{mod_name}")
        except Exception as e:
            # Skip broken modules but emit a debug so issues aren't invisible
            log.debug("Skipping strategy module %s due to import error: %s", mod_name, e)
            continue

        # Canonical key is the *module filename* (e.g., grok_4_strategy)
        canonical_key = mod_name.lower()

        # Find classes in module that look like strategies
        for _, cls in inspect.getmembers(mod, inspect.isclass):
            if cls.__module__ != mod.__name__:
                continue
            if not hasattr(cls, "decide_buy") or not hasattr(cls, "decide_sell"):
                continue  # not a strategy

            # Register the class exactly once per canonical module key
            if canonical_key not in _CLASSES:
                _CLASSES[canonical_key] = cls

                # Build a rich set of aliases
                class_name_snake = _snake_case(getattr(cls, "name", cls.__name__))
                class_alias = _ensure_strategy_suffix(class_name_snake)

                # Primary aliases
                _add_alias(canonical_key, canonical_key)
                _add_alias(class_alias, canonical_key)

                # Loose variants for convenience
                _add_alias(class_alias.replace("_strategy", ""), canonical_key)     # e.g., "grok_4"
                _add_alias(class_alias.replace("_", ""), canonical_key)            # e.g., "chatgpt5strategy"
                _add_alias(class_alias.replace("_strategy", "").replace("_", ""), canonical_key)  # e.g., "grok4"

                # Any explicit aliases on the class
                for a in getattr(cls, "aliases", []):
                    _add_alias(a, canonical_key)

            # We only need one class per module as the canonical strategy class
            break

    _DISCOVERED = True


def list_available_strategy_keys() -> list[str]:
    """Canonical strategy keys (module filenames), e.g. ["chatgpt_5_strategy", "grok_4_strategy"]."""
    _discover()
    return sorted(_CLASSES.keys())


def resolve_strategy_key(key: str | None) -> Optional[str]:
    """Return the canonical key for any alias, or None if unknown."""
    if not key:
        return None
    _discover()
    k = key.strip().lower()
    return _ALIASES.get(k)


def select_strategy(runner) -> Any:
    """
    Resolve runner.strategy → strategy instance.
    Raises ValueError if unknown.
    """
    _discover()
    canonical = resolve_strategy_key(getattr(runner, "strategy", None))
    if not canonical or canonical not in _CLASSES:
        raise ValueError(f"Unknown or unsupported strategy '{getattr(runner, 'strategy', None)}'")
    return _ClassesFactory.create(canonical)


class _ClassesFactory:
    """Tiny helper to keep instantiation logic in one place (future DI etc.)."""

    @staticmethod
    def create(canonical_key: str) -> Any:
        cls = _CLASSES[canonical_key]
        return cls()  # strategies accept no required ctor args today

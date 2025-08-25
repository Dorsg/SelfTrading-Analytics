from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple


def _format_number(name: str, value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        return str(value)

    lname = name.lower()
    if "rsi" in lname:
        return f"{v:.1f}"
    # default to 2 decimals for price-like numbers
    return f"{v:.2f}"


def _relation_symbol(actual: float, wanted: float) -> str:
    try:
        a = float(actual)
        w = float(wanted)
    except Exception:
        return "?"
    if a < w:
        return "<"
    if a > w:
        return ">"
    return "="


def format_actual_vs_wanted(pairs: Iterable[Dict[str, Any]]) -> str:
    """
    Build a compact explanation string like:
    "actual price 50.00 < wanted trigger 60.00 | actual rsi 40.0 < wanted rsi_min 50.0"

    Each pair item supports keys:
      - actual_label: str
      - actual: number
      - wanted_label: str
      - wanted: number | (min, max) when direction == "range"
      - direction: one of ">=", "<=", "range" (used to label the target side)
    """
    parts: list[str] = []
    for p in pairs:
        direction = str(p.get("direction", ">=")).lower()
        actual_label = str(p.get("actual_label", "value"))
        wanted_label = str(p.get("wanted_label", "target"))
        actual_val = p.get("actual")
        wanted_val = p.get("wanted")

        if direction == "range":
            try:
                low, high = wanted_val  # type: ignore[misc]
            except Exception:
                # fallback to simple representation
                parts.append(
                    f"actual {actual_label} {_format_number(actual_label, actual_val)} vs wanted {wanted_label} {_format_number(wanted_label, wanted_val)}"
                )
                continue

            a = float(actual_val)
            if a < float(low):
                parts.append(
                    f"actual {actual_label}: {_format_number(actual_label, a)} < wanted {wanted_label} min: {_format_number(wanted_label, low)}"
                )
            elif a > float(high):
                parts.append(
                    f"actual {actual_label}: {_format_number(actual_label, a)} > wanted {wanted_label} max: {_format_number(wanted_label, high)}"
                )
            else:
                parts.append(
                    f"actual {actual_label}: {_format_number(actual_label, a)} within wanted {wanted_label}: [{_format_number(wanted_label, low)}..{_format_number(wanted_label, high)}]"
                )
            continue

        rel = _relation_symbol(actual_val, wanted_val)
        parts.append(
            f"actual {actual_label}: {_format_number(actual_label, actual_val)} {rel} wanted {wanted_label}: {_format_number(wanted_label, wanted_val)}"
        )

    return " | ".join(parts)


def format_checklist(items: Iterable[Dict[str, Any]]) -> str:
    """
    Build a vertical checklist where each line is either ✅ (ok) or ❌ (failed).

    Each item supports keys:
      - label: str (mandatory)
      - ok: bool (mandatory)
      - actual: number | None
      - wanted: number | (min, max) when direction == "range"
      - direction: one of ">=", "<=", "range" (default ">=")
      - wanted_label: optional str (defaults to label)

    Rules:
      - ok=True → "✅ label: <actual>"
      - ok=False → "❌ label: <actual> <rel> wanted <wanted>" (or range message)
    """
    lines: list[str] = []
    for it in items:
        label = str(it.get("label", "value"))
        ok = bool(it.get("ok", False))
        direction = str(it.get("direction", ">=")).lower()
        actual = it.get("actual")
        wanted = it.get("wanted")
        wanted_label = str(it.get("wanted_label", label))

        if ok:
            if actual is None:
                lines.append(f"✅ {label}")
            else:
                lines.append(f"✅ {label}: {_format_number(label, actual)}")
            continue

        # failed case
        if direction == "range":
            try:
                low, high = wanted  # type: ignore[misc]
                a = float(actual)
                if a < float(low):
                    lines.append(
                        f"❌ {label}: {_format_number(label, a)} < wanted {wanted_label} min: {_format_number(wanted_label, low)}"
                    )
                elif a > float(high):
                    lines.append(
                        f"❌ {label}: {_format_number(label, a)} > wanted {wanted_label} max: {_format_number(wanted_label, high)}"
                    )
                else:
                    lines.append(
                        f"❌ {label}: {_format_number(label, a)} outside wanted {wanted_label}: [{_format_number(wanted_label, low)}..{_format_number(wanted_label, high)}]"
                    )
            except Exception:
                lines.append(
                    f"❌ {label}: {_format_number(label, actual)} vs wanted {wanted_label}: {_format_number(wanted_label, wanted)}"
                )
            continue

        rel = _relation_symbol(actual, wanted)
        lines.append(
            f"❌ {label}: {_format_number(label, actual)} {rel} wanted {wanted_label}: {_format_number(wanted_label, wanted)}"
        )

    return "\n".join(lines)



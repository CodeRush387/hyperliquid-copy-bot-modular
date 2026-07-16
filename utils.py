from decimal import Decimal
from typing import Any

from config import ZERO


def dec(value: Any) -> Decimal:
    if value is None or value == "":
        return ZERO
    return Decimal(str(value))


def sign(value: Decimal) -> int:
    return 1 if value > ZERO else -1 if value < ZERO else 0


def fmt(value: Decimal | None, places: int = 8) -> str:
    if value is None:
        return "UNAVAILABLE"
    return f"{value:.{places}f}"

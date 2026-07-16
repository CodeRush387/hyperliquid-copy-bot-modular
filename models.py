import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any

from config import ZERO
from utils import dec, sign


class LifecycleStatus(str, Enum):
    FULL = "FULL_HISTORY"
    PARTIAL = "PARTIAL_HISTORY"
    INSUFFICIENT = "INSUFFICIENT_HISTORY"
    CLOSED = "CLOSED_POSITION"


class FillKind(str, Enum):
    OPEN = "OPEN"
    INCREASE = "INCREASE"
    REDUCTION = "REDUCTION"
    FULL_CLOSE = "FULL_CLOSE"
    REVERSAL = "REVERSAL"
    UNKNOWN = "UNKNOWN"


class Decision(str, Enum):
    NO_ACTION = "NO_ACTION_NO_VALID_SIGNAL"
    INITIAL_ENTRY = "INITIAL_ENTRY_SIGNAL"
    HOLD = "HOLD_CURRENT"
    ROTATE_SIGNAL = "ROTATE_SIGNAL"
    SIMULATED = "SIMULATED_ROTATION"
    CLOSE_FAILED = "ROTATION_ABORTED_CLOSE_FAILED"
    COMPLETED = "ROTATION_COMPLETED"


@dataclass(frozen=True)
class Fill:
    wallet: str
    coin: str
    price: Decimal
    quantity: Decimal
    side: str
    timestamp_ms: int
    start_position: Decimal
    tid: str
    oid: str
    direction_label: str
    raw: dict[str, Any] = field(compare=False, repr=False)

    @classmethod
    def from_raw(cls, wallet: str, raw: dict[str, Any]) -> "Fill":
        return cls(
            wallet=wallet.lower(),
            coin=str(raw.get("coin", "")).strip(),
            price=dec(raw.get("px")),
            quantity=abs(dec(raw.get("sz"))),
            side=str(raw.get("side", "")).upper(),
            timestamp_ms=int(raw.get("time", 0) or 0),
            start_position=dec(raw.get("startPosition")),
            tid=str(raw.get("tid", "")),
            oid=str(raw.get("oid", "")),
            direction_label=str(raw.get("dir", "")),
            raw=raw,
        )

    @property
    def after_position(self) -> Decimal:
        if self.side == "B":
            return self.start_position + self.quantity
        if self.side == "A":
            return self.start_position - self.quantity
        raise ValueError(f"Unknown fill side {self.side!r}")

    @property
    def event_id(self) -> str:
        if self.tid:
            return f"tid:{self.tid}"
        return "|".join(
            [self.coin, self.oid, str(self.timestamp_ms), self.side, str(self.quantity), str(self.price)]
        )

    @property
    def kind(self) -> FillKind:
        before, after = self.start_position, self.after_position
        if before == ZERO and after != ZERO:
            return FillKind.OPEN
        if after == ZERO and before != ZERO:
            return FillKind.FULL_CLOSE
        if sign(before) != 0 and sign(after) != 0 and sign(before) != sign(after):
            return FillKind.REVERSAL
        if sign(before) == sign(after) and abs(after) > abs(before):
            return FillKind.INCREASE
        if sign(before) == sign(after) and abs(after) < abs(before):
            return FillKind.REDUCTION
        return FillKind.UNKNOWN


@dataclass
class CapitalEvent:
    fill: Fill
    capital_added: Decimal
    interval_seconds: Decimal | None = None
    velocity: Decimal | None = None
    acceleration: Decimal | None = None


@dataclass
class Lifecycle:
    coin: str
    direction: str
    status: LifecycleStatus
    reason: str
    fills: list[Fill] = field(default_factory=list)
    capital_events: list[CapitalEvent] = field(default_factory=list)
    final_size: Decimal = ZERO
    current_capital: Decimal = ZERO
    share: Decimal = ZERO
    previous_share: Decimal = ZERO
    share_change: Decimal = ZERO

    @property
    def valid(self) -> bool:
        return self.status in {LifecycleStatus.FULL, LifecycleStatus.PARTIAL}

    @property
    def latest_fill(self) -> Fill | None:
        return self.fills[-1] if self.fills else None

    @property
    def latest_capital_event(self) -> CapitalEvent | None:
        return self.capital_events[-1] if self.capital_events else None

    @property
    def velocity(self) -> Decimal | None:
        event = self.latest_capital_event
        return event.velocity if event else None

    @property
    def acceleration(self) -> Decimal | None:
        event = self.latest_capital_event
        return event.acceleration if event else None


@dataclass
class WalletState:
    wallet: str
    snapshot_positions: dict[str, Decimal] = field(default_factory=dict)
    snapshot_capital: dict[str, Decimal] = field(default_factory=dict)
    fills_by_coin: dict[str, list[Fill]] = field(default_factory=dict)
    seen_events: set[str] = field(default_factory=set)
    lifecycles: dict[str, Lifecycle] = field(default_factory=dict)
    held_asset: str | None = None
    held_side: str | None = None
    race_ready: bool = False
    race_reason: str = "startup reconstruction has not completed"


@dataclass
class FollowerState:
    positions: dict[str, Decimal] = field(default_factory=dict)
    changed: asyncio.Condition = field(default_factory=asyncio.Condition)

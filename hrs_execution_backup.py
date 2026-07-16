"""Production Hyperliquid Rotation Strategy driven by QuickNode gRPC events."""
import asyncio
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, getcontext
from enum import Enum
from pathlib import Path
from typing import Any

import grpc
import requests
from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

PROTO_DIR = Path(__file__).with_name("quicknode_proto")
sys.path.insert(0, str(PROTO_DIR))
import hyperliquid_pb2 as hypercore_pb2  # noqa: E402
import hyperliquid_pb2_grpc as hypercore_grpc  # noqa: E402
import orderbook_pb2 as orderbook_pb2  # noqa: E402
import orderbook_pb2_grpc as orderbook_grpc  # noqa: E402

getcontext().prec = 40
ZERO = Decimal("0")


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"Invalid boolean environment variable: {name}")


def env_decimal(name: str, default: str) -> Decimal:
    try:
        return Decimal(os.getenv(name, default).strip() or default)
    except InvalidOperation as exc:
        raise RuntimeError(f"Invalid decimal environment variable: {name}") from exc


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_leaders() -> tuple[str, ...]:
    raw = (
        os.getenv("HRS_LEADER_ADDRESSES")
        or os.getenv("LEADER_ADDRESSES")
        or os.getenv("LEADER_ADDRESS")
        or ""
    )
    leaders = tuple(dict.fromkeys(part.strip().lower() for part in raw.split(",") if part.strip()))
    if not leaders:
        raise RuntimeError("HRS_LEADER_ADDRESSES or LEADER_ADDRESS is required")
    for wallet in leaders:
        if not wallet.startswith("0x") or len(wallet) != 42:
            raise RuntimeError(f"Invalid leader wallet address: {wallet}")
    return leaders


LEADERS = parse_leaders()
FOLLOWER = os.getenv("FOLLOWER_ADDRESS", "").strip().lower()
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
GRPC_ENDPOINT = required("HC_GRPC_ENDPOINT")
GRPC_TOKEN = required("HC_GRPC_TOKEN")
GRPC_SERVER_NAME = os.getenv("HC_GRPC_SERVER_NAME", "").strip() or GRPC_ENDPOINT.rsplit(":", 1)[0]
QUICKNODE_INFO_URL = os.getenv("QUICKNODE_INFO_URL", "").strip()
if not QUICKNODE_INFO_URL:
    QUICKNODE_INFO_URL = f"https://{GRPC_SERVER_NAME}/{GRPC_TOKEN}/info"

EXECUTION_ENABLED = env_bool("HRS_EXECUTION_ENABLED", False)
ALLOCATION_GAP_PCT = env_decimal("HRS_ALLOCATION_GAP_PCT", "3.0")
FIXED_NOTIONAL_USD = env_decimal("FIXED_NOTIONAL_USD", "12")
SLIPPAGE = float(os.getenv("SLIPPAGE", "0.05"))
EXECUTION_CONFIRM_TIMEOUT = float(os.getenv("HRS_EXECUTION_CONFIRM_TIMEOUT_SECONDS", "15"))
TESTNET = env_bool("TESTNET", False)
EXCHANGE_URL = constants.TESTNET_API_URL if TESTNET else constants.MAINNET_API_URL

if EXECUTION_ENABLED and (not FOLLOWER or not PRIVATE_KEY):
    raise RuntimeError("FOLLOWER_ADDRESS and PRIVATE_KEY are required when HRS_EXECUTION_ENABLED=true")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hrs")


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


wallet_states = {wallet: WalletState(wallet=wallet) for wallet in LEADERS}
follower_state = FollowerState()
size_decimals: dict[str, int] = {}
mid_prices: dict[str, Decimal] = {}
trade_queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
grpc_ready = asyncio.Event()
exchange: Exchange | None = None
trade_lock = asyncio.Lock()


def startup_info(payload: dict[str, Any]) -> Any:
    """One-time startup HTTPS request with bounded rate-limit retry."""
    for attempt in range(5):
        response = requests.post(QUICKNODE_INFO_URL, json=payload, timeout=15)
        if response.status_code != 429:
            response.raise_for_status()
            return response.json()
        if attempt == 4:
            response.raise_for_status()
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("unreachable")


def snapshot_portfolio(user: str) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    data = startup_info({"type": "clearinghouseState", "user": user})
    positions: dict[str, Decimal] = {}
    capital: dict[str, Decimal] = {}
    for item in data.get("assetPositions", []) or []:
        position = item.get("position", {}) or {}
        coin = str(position.get("coin", "")).strip()
        size = dec(position.get("szi"))
        entry_price = dec(position.get("entryPx"))
        if coin and size != ZERO:
            positions[coin] = size
            if entry_price > ZERO:
                capital[coin] = abs(size) * entry_price
    return positions, capital


def startup_fills(user: str) -> list[Fill]:
    data = startup_info({"type": "userFills", "user": user, "aggregateByTime": False})
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected userFills response for {user}")
    fills = [Fill.from_raw(user, raw) for raw in data]
    return sorted(fills, key=lambda fill: (fill.timestamp_ms, fill.tid, fill.oid))


def load_startup_state() -> None:
    metadata = startup_info({"type": "meta"})
    for asset in metadata.get("universe", []) or []:
        name = str(asset.get("name", "")).strip()
        if name:
            size_decimals[name] = int(asset.get("szDecimals", 6))

    for wallet, state in wallet_states.items():
        state.snapshot_positions, state.snapshot_capital = snapshot_portfolio(wallet)
        for fill in startup_fills(wallet):
            state.fills_by_coin.setdefault(fill.coin, []).append(fill)
            state.seen_events.add(fill.event_id)
        rebuild_wallet(state, startup=True)

    if FOLLOWER:
        follower_state.positions, _ = snapshot_portfolio(FOLLOWER)
    log.info(
        "[STARTUP] leaders=%s assets=%s follower_positions=%s execution_enabled=%s",
        len(LEADERS),
        sum(len(state.lifecycles) for state in wallet_states.values()),
        len(follower_state.positions),
        EXECUTION_ENABLED,
    )



def reconstruct_lifecycle(
    coin: str,
    fills: list[Fill],
    expected_size: Decimal,
    authoritative_capital: Decimal | None = None,
) -> Lifecycle:
    if expected_size == ZERO:
        return Lifecycle(
            coin=coin,
            direction="FLAT",
            status=LifecycleStatus.CLOSED,
            reason="authoritative position is closed",
        )

    direction = "LONG" if expected_size > ZERO else "SHORT"
    ordered = sorted(fills, key=lambda fill: (fill.timestamp_ms, fill.tid, fill.oid))
    if not ordered:
        return Lifecycle(
            coin=coin,
            direction=direction,
            status=LifecycleStatus.INSUFFICIENT,
            reason="no verified fills are available for the open position",
            final_size=expected_size,
            current_capital=authoritative_capital or ZERO,
        )

    if ordered[-1].after_position != expected_size:
        return Lifecycle(
            coin=coin,
            direction=direction,
            status=LifecycleStatus.INSUFFICIENT,
            reason=(
                f"latest verified fill ends at {ordered[-1].after_position}, "
                f"not authoritative exposure {expected_size}"
            ),
            final_size=ordered[-1].after_position,
            current_capital=authoritative_capital or ZERO,
        )

    suffix_start = len(ordered) - 1
    while (
        suffix_start > 0
        and ordered[suffix_start - 1].after_position
        == ordered[suffix_start].start_position
    ):
        suffix_start -= 1
    verified_suffix = ordered[suffix_start:]

    expected_sign = sign(expected_size)
    boundary_offset: int | None = None
    for index, fill in enumerate(verified_suffix):
        if (
            sign(fill.after_position) == expected_sign
            and fill.after_position != ZERO
            and sign(fill.start_position) != expected_sign
        ):
            boundary_offset = index

    full_history = boundary_offset is not None
    lifecycle_fills = (
        verified_suffix[boundary_offset:]
        if boundary_offset is not None
        else verified_suffix
    )
    current_size = lifecycle_fills[0].start_position
    reconstructed_capital = ZERO
    capital_events: list[CapitalEvent] = []
    previous_capital_event: CapitalEvent | None = None

    for fill in lifecycle_fills:
        if fill.start_position != current_size:
            return Lifecycle(
                coin=coin,
                direction=direction,
                status=LifecycleStatus.INSUFFICIENT,
                reason=(
                    f"verified history gap before {fill.event_id}: "
                    f"expected {current_size}, received {fill.start_position}"
                ),
                fills=lifecycle_fills,
                final_size=current_size,
                current_capital=authoritative_capital or ZERO,
            )

        before, after, kind = fill.start_position, fill.after_position, fill.kind
        if kind in {FillKind.OPEN, FillKind.INCREASE, FillKind.REVERSAL}:
            added = (
                abs(after) * fill.price
                if kind == FillKind.REVERSAL
                else fill.quantity * fill.price
            )
            event = CapitalEvent(fill=fill, capital_added=added)
            if previous_capital_event is not None:
                seconds = Decimal(
                    fill.timestamp_ms - previous_capital_event.fill.timestamp_ms
                ) / Decimal(1000)
                if seconds > ZERO:
                    event.interval_seconds = seconds
                    event.velocity = added / seconds
                    if previous_capital_event.velocity is not None:
                        event.acceleration = (
                            event.velocity - previous_capital_event.velocity
                        ) / seconds
            capital_events.append(event)
            previous_capital_event = event
            if kind == FillKind.REVERSAL:
                reconstructed_capital = added
            else:
                reconstructed_capital += added
        elif kind == FillKind.REDUCTION:
            if before == ZERO:
                return Lifecycle(
                    coin=coin,
                    direction=direction,
                    status=LifecycleStatus.INSUFFICIENT,
                    reason="verified reduction has zero starting exposure",
                    fills=lifecycle_fills,
                    current_capital=authoritative_capital or ZERO,
                )
            reconstructed_capital *= abs(after) / abs(before)
        elif kind == FillKind.FULL_CLOSE:
            reconstructed_capital = ZERO
        else:
            return Lifecycle(
                coin=coin,
                direction=direction,
                status=LifecycleStatus.INSUFFICIENT,
                reason=f"unclassifiable verified fill {fill.event_id}",
                fills=lifecycle_fills,
                current_capital=authoritative_capital or ZERO,
            )
        current_size = after

    if current_size != expected_size:
        return Lifecycle(
            coin=coin,
            direction=direction,
            status=LifecycleStatus.INSUFFICIENT,
            reason=(
                f"verified suffix reconstructs {current_size}, "
                f"not authoritative exposure {expected_size}"
            ),
            fills=lifecycle_fills,
            capital_events=capital_events,
            final_size=current_size,
            current_capital=authoritative_capital or ZERO,
        )

    if full_history:
        status = LifecycleStatus.FULL
        reason = "zero/open or reversal boundary and every subsequent fill are verified"
        current_capital = (
            authoritative_capital
            if authoritative_capital is not None and authoritative_capital > ZERO
            else reconstructed_capital
        )
    elif (
        authoritative_capital is not None
        and authoritative_capital > ZERO
        and capital_events
        and capital_events[-1].velocity is not None
        and capital_events[-1].acceleration is not None
    ):
        status = LifecycleStatus.PARTIAL
        reason = (
            f"verified contiguous suffix of {len(lifecycle_fills)} fills provides "
            "authoritative allocation, velocity, and acceleration"
        )
        current_capital = authoritative_capital
    elif authoritative_capital is not None and authoritative_capital > ZERO:
        status = LifecycleStatus.INSUFFICIENT
        reason = (
            "verified partial suffix and authoritative allocation are available, "
            "but timed capital-entry events are insufficient for acceleration"
        )
        current_capital = authoritative_capital
    else:
        status = LifecycleStatus.INSUFFICIENT
        reason = "partial fills are verified but authoritative current allocation is unavailable"
        current_capital = ZERO

    return Lifecycle(
        coin=coin,
        direction=direction,
        status=status,
        reason=reason,
        fills=lifecycle_fills,
        capital_events=capital_events,
        final_size=current_size,
        current_capital=current_capital,
    )

def rebuild_wallet(
    state: WalletState,
    startup: bool = False,
    previous: dict[str, Decimal] | None = None,
) -> None:
    previous = previous or {}
    rebuilt: dict[str, Lifecycle] = {}
    for coin, expected_size in state.snapshot_positions.items():
        lifecycle = reconstruct_lifecycle(
            coin,
            state.fills_by_coin.get(coin, []),
            expected_size,
            state.snapshot_capital.get(coin),
        )
        lifecycle.previous_share = previous.get(coin, ZERO)
        rebuilt[coin] = lifecycle
        log.info(
            "[LIFECYCLE] wallet=%s asset=%s status=%s reason=%s",
            state.wallet,
            coin,
            lifecycle.status.value,
            lifecycle.reason,
        )

    reliable = [item for item in rebuilt.values() if item.current_capital > ZERO]
    total_capital = sum((item.current_capital for item in reliable), ZERO)
    state.race_ready = total_capital > ZERO
    counts = {
        status: sum(1 for item in rebuilt.values() if item.status == status)
        for status in (
            LifecycleStatus.FULL,
            LifecycleStatus.PARTIAL,
            LifecycleStatus.INSUFFICIENT,
        )
    }
    state.race_reason = (
        f"maximum reliable universe allocations={len(reliable)}/{len(rebuilt)} "
        f"full={counts[LifecycleStatus.FULL]} "
        f"partial={counts[LifecycleStatus.PARTIAL]} "
        f"insufficient={counts[LifecycleStatus.INSUFFICIENT]}"
        if state.race_ready
        else "no authoritative open capital is available"
    )
    for lifecycle in rebuilt.values():
        lifecycle.share = (
            lifecycle.current_capital / total_capital
            if lifecycle.current_capital > ZERO and total_capital > ZERO
            else ZERO
        )
        lifecycle.share_change = (
            ZERO if startup else lifecycle.share - lifecycle.previous_share
        )
    state.lifecycles = rebuilt

def is_eligible(lifecycle: Lifecycle, trigger: Fill, held_asset: str | None) -> bool:
    latest = lifecycle.latest_capital_event
    return bool(
        lifecycle.coin != held_asset
        and lifecycle.valid
        and lifecycle.acceleration is not None
        and lifecycle.acceleration > ZERO
        and lifecycle.share_change > ZERO
        and latest is not None
        and latest.fill.event_id == trigger.event_id
        and trigger.kind in {FillKind.OPEN, FillKind.INCREASE}
    )


def leaderboard(state: WalletState, trigger: Fill) -> list[Lifecycle]:
    def key(item: Lifecycle) -> tuple[Decimal, Decimal]:
        return (item.acceleration if item.acceleration is not None else Decimal("-Infinity"), item.current_capital)

    return sorted(state.lifecycles.values(), key=key, reverse=True)


def log_race(
    state: WalletState,
    trigger: Fill,
    ranked: list[Lifecycle],
    challenger: Lifecycle | None,
    held_share: Decimal,
    gap: Decimal,
    conditions_met: bool,
    decision: Decision,
    held_asset_before: str | None,
) -> None:
    log.info("================ HRS WALLET RACE ================")
    log.info("Wallet: %s", state.wallet)
    log.info("Event Timestamp: %s", trigger.timestamp_ms)
    log.info(
        "Trigger Fill: asset=%s kind=%s tid=%s quantity=%s price=%s",
        trigger.coin,
        trigger.kind.value,
        trigger.tid or "UNAVAILABLE",
        trigger.quantity,
        trigger.price,
    )
    log.info("Execution Enabled: %s", EXECUTION_ENABLED)
    log.info("Currently Held Asset: %s", held_asset_before or "NONE")
    log.info("Race Ready: %s (%s)", state.race_ready, state.race_reason)
    log.info(
        "Rank | Asset | Side | Lifecycle Status | Capital | Capital Share | Share Change | "
        "Latest Capital Added | Latest Interval | Velocity | Acceleration | Eligible"
    )
    for rank, lifecycle in enumerate(ranked, start=1):
        event = lifecycle.latest_capital_event
        log.info(
            "%s | %s | %s | %s | %s | %s%% | %spp | %s | %s | %s | %s | %s",
            rank,
            lifecycle.coin,
            lifecycle.direction,
            lifecycle.status.value,
            fmt(lifecycle.current_capital, 4) if lifecycle.current_capital > ZERO else "UNAVAILABLE",
            fmt(lifecycle.share * 100, 6) if lifecycle.current_capital > ZERO else "UNAVAILABLE",
            fmt(lifecycle.share_change * 100, 6) if lifecycle.current_capital > ZERO else "UNAVAILABLE",
            fmt(event.capital_added, 4) if event else "UNAVAILABLE",
            fmt(event.interval_seconds, 6) if event else "UNAVAILABLE",
            fmt(lifecycle.velocity, 8),
            fmt(lifecycle.acceleration, 12),
            is_eligible(lifecycle, trigger, held_asset_before) and state.race_ready,
        )

    acceleration_positive = bool(challenger and challenger.acceleration and challenger.acceleration > ZERO)
    share_increasing = bool(challenger and challenger.share_change > ZERO)
    lifecycle_valid = bool(challenger and challenger.valid)
    log.info("Challenger: %s", challenger.coin if challenger else "NONE")
    log.info("Challenger Acceleration: %s", fmt(challenger.acceleration, 12) if challenger else "UNAVAILABLE")
    log.info("Challenger Share: %s%%", fmt(challenger.share * 100, 6) if challenger else "UNAVAILABLE")
    log.info("Held Asset Share: %s%%", fmt(held_share * 100, 6))
    log.info("Allocation Gap: %s percentage points", fmt(gap, 6))
    log.info("Required Gap: %s percentage points", fmt(ALLOCATION_GAP_PCT, 6))
    log.info("Acceleration Positive: %s", acceleration_positive)
    log.info("Share Increasing: %s", share_increasing)
    log.info("Lifecycle Valid: %s", lifecycle_valid)
    log.info("Rotation Conditions Met: %s", conditions_met)
    log.info("Decision: %s", decision.value)


async def evaluate_wallet(state: WalletState, trigger: Fill) -> Decision:
    held_asset_before = state.held_asset
    ranked = leaderboard(state, trigger)
    eligible = [
        lifecycle
        for lifecycle in ranked
        if state.race_ready and is_eligible(lifecycle, trigger, state.held_asset)
    ]
    challenger = eligible[0] if eligible else None
    held = state.lifecycles.get(state.held_asset or "")
    held_share = held.share if held else ZERO
    gap = ((challenger.share - held_share) * 100) if challenger else ZERO

    if state.held_asset is None:
        if challenger is None:
            decision = Decision.NO_ACTION
            conditions_met = False
        elif EXECUTION_ENABLED:
            conditions_met = True
            decision = await execute_initial_entry(state, challenger)
        else:
            conditions_met = True
            state.held_asset = challenger.coin
            state.held_side = challenger.direction
            decision = Decision.INITIAL_ENTRY
            log.info(
                "[SIMULATED_INITIAL_ENTRY] wallet=%s asset=%s side=%s",
                state.wallet,
                challenger.coin,
                challenger.direction,
            )
    elif challenger is None:
        conditions_met = False
        decision = Decision.HOLD
    else:
        conditions_met = gap >= ALLOCATION_GAP_PCT
        if not conditions_met:
            decision = Decision.HOLD
        elif EXECUTION_ENABLED:
            log.info("[ROTATE_SIGNAL] wallet=%s challenger=%s", state.wallet, challenger.coin)
            decision = await execute_rotation(state, challenger)
        else:
            state.held_asset = challenger.coin
            state.held_side = challenger.direction
            decision = Decision.SIMULATED
            log.info(
                "[SIMULATED_ROTATION] wallet=%s challenger=%s side=%s",
                state.wallet,
                challenger.coin,
                challenger.direction,
            )

    log_race(
        state,
        trigger,
        ranked,
        challenger,
        held_share,
        gap,
        conditions_met,
        decision,
        held_asset_before,
    )
    return decision


def update_snapshot_capital(state: WalletState, fill: Fill) -> None:
    kind = fill.kind
    prior_capital = state.snapshot_capital.get(fill.coin)
    if kind in {FillKind.OPEN, FillKind.REVERSAL}:
        state.snapshot_capital[fill.coin] = abs(fill.after_position) * fill.price
    elif kind == FillKind.INCREASE and prior_capital is not None:
        state.snapshot_capital[fill.coin] = prior_capital + fill.quantity * fill.price
    elif (
        kind == FillKind.REDUCTION
        and prior_capital is not None
        and fill.start_position != ZERO
    ):
        state.snapshot_capital[fill.coin] = (
            prior_capital * abs(fill.after_position) / abs(fill.start_position)
        )
    elif kind == FillKind.FULL_CLOSE:
        state.snapshot_capital.pop(fill.coin, None)
    else:
        state.snapshot_capital.pop(fill.coin, None)
        log.warning(
            "[CAPITAL_UNAVAILABLE] wallet=%s asset=%s kind=%s "
            "reason=prior_authoritative_allocation_unavailable",
            state.wallet,
            fill.coin,
            kind.value,
        )

async def process_leader_fill(wallet: str, raw: dict[str, Any]) -> None:
    state = wallet_states[wallet]
    fill = Fill.from_raw(wallet, raw)
    if not fill.coin or fill.quantity <= ZERO or fill.price <= ZERO:
        log.warning("[FILL_REJECTED] wallet=%s reason=invalid_fields raw=%s", wallet, raw)
        return
    if fill.event_id in state.seen_events:
        return

    previous_shares = {coin: lifecycle.share for coin, lifecycle in state.lifecycles.items()}
    prior_position = state.snapshot_positions.get(fill.coin, ZERO)
    if prior_position != fill.start_position:
        log.error(
            "[LIVE_POSITION_MISMATCH] wallet=%s asset=%s tracked=%s fill_start=%s",
            wallet,
            fill.coin,
            prior_position,
            fill.start_position,
        )

    state.seen_events.add(fill.event_id)
    state.fills_by_coin.setdefault(fill.coin, []).append(fill)

    update_snapshot_capital(state, fill)


    if fill.after_position == ZERO:
        state.snapshot_positions.pop(fill.coin, None)
        state.lifecycles[fill.coin] = Lifecycle(
            coin=fill.coin,
            direction="FLAT",
            status=LifecycleStatus.CLOSED,
            reason="live fill returned exposure to zero",
            fills=[fill],
        )
        log.info(
            "[LIFECYCLE] wallet=%s asset=%s status=%s reason=live_full_close",
            wallet,
            fill.coin,
            LifecycleStatus.CLOSED.value,
        )
    else:
        state.snapshot_positions[fill.coin] = fill.after_position

    rebuild_wallet(state, startup=False, previous=previous_shares)
    await evaluate_wallet(state, fill)


async def observe_follower_fill(raw: dict[str, Any]) -> None:
    if not FOLLOWER:
        return
    fill = Fill.from_raw(FOLLOWER, raw)
    async with follower_state.changed:
        if fill.after_position == ZERO:
            follower_state.positions.pop(fill.coin, None)
        else:
            follower_state.positions[fill.coin] = fill.after_position
        follower_state.changed.notify_all()
    log.info(
        "[FOLLOWER_FILL_CONFIRMED] asset=%s start=%s final=%s tid=%s",
        fill.coin,
        fill.start_position,
        fill.after_position,
        fill.tid or "UNAVAILABLE",
    )


async def wait_for_follower_position(coin: str, predicate) -> bool:
    async def wait_loop() -> None:
        async with follower_state.changed:
            await follower_state.changed.wait_for(
                lambda: predicate(follower_state.positions.get(coin, ZERO))
            )

    if predicate(follower_state.positions.get(coin, ZERO)):
        return True
    try:
        await asyncio.wait_for(wait_loop(), timeout=EXECUTION_CONFIRM_TIMEOUT)
        return True
    except asyncio.TimeoutError:
        return False


def order_ok(result: Any) -> bool:
    return isinstance(result, dict) and result.get("status") == "ok"


def close_order(coin: str, position: Decimal) -> Any:
    if exchange is None:
        raise RuntimeError("execution client is unavailable")
    is_buy = position < ZERO
    price = exchange._slippage_price(coin, is_buy, SLIPPAGE, None)
    return exchange.order(
        coin,
        is_buy,
        float(abs(position)),
        price,
        order_type={"limit": {"tif": "Ioc"}},
        reduce_only=True,
    )


def open_order(lifecycle: Lifecycle) -> Any:
    if exchange is None:
        raise RuntimeError("execution client is unavailable")
    price = mid_prices.get(lifecycle.coin)
    if price is None or price <= ZERO:
        raise RuntimeError(f"No gRPC BBO midpoint available for {lifecycle.coin}")
    precision = size_decimals.get(lifecycle.coin, 6)
    size = math.floor(float(FIXED_NOTIONAL_USD / price) * 10**precision) / 10**precision
    if size <= 0:
        raise RuntimeError(f"Calculated zero order size for {lifecycle.coin}")
    return exchange.market_open(
        lifecycle.coin,
        lifecycle.direction == "LONG",
        size,
        slippage=SLIPPAGE,
    )


async def execute_initial_entry(state: WalletState, challenger: Lifecycle) -> Decision:
    async with trade_lock:
        result = await asyncio.to_thread(open_order, challenger)
        if not order_ok(result):
            log.error("[ROTATION_ABORTED_CLOSE_FAILED] initial open rejected result=%s", result)
            return Decision.CLOSE_FAILED
        expected_sign = 1 if challenger.direction == "LONG" else -1
        confirmed = await wait_for_follower_position(
            challenger.coin, lambda value: sign(value) == expected_sign
        )
        if not confirmed:
            log.error("[ROTATION_ABORTED_CLOSE_FAILED] initial open not confirmed")
            return Decision.CLOSE_FAILED
        state.held_asset = challenger.coin
        state.held_side = challenger.direction
        return Decision.COMPLETED


async def execute_rotation(state: WalletState, challenger: Lifecycle) -> Decision:
    if not state.held_asset:
        return await execute_initial_entry(state, challenger)
    async with trade_lock:
        held_coin = state.held_asset
        assert held_coin is not None
        held_position = follower_state.positions.get(held_coin, ZERO)
        if held_position != ZERO:
            close_result = await asyncio.to_thread(close_order, held_coin, held_position)
            if not order_ok(close_result):
                log.error(
                    "[ROTATION_ABORTED_CLOSE_FAILED] wallet=%s asset=%s result=%s",
                    state.wallet,
                    held_coin,
                    close_result,
                )
                return Decision.CLOSE_FAILED
            if not await wait_for_follower_position(held_coin, lambda value: value == ZERO):
                log.error(
                    "[ROTATION_ABORTED_CLOSE_FAILED] wallet=%s asset=%s reason=not_flat",
                    state.wallet,
                    held_coin,
                )
                return Decision.CLOSE_FAILED

        open_result = await asyncio.to_thread(open_order, challenger)
        if not order_ok(open_result):
            log.error("[ROTATION_OPEN_FAILED] wallet=%s result=%s", state.wallet, open_result)
            return Decision.CLOSE_FAILED
        expected_sign = 1 if challenger.direction == "LONG" else -1
        confirmed = await wait_for_follower_position(
            challenger.coin, lambda value: sign(value) == expected_sign
        )
        if not confirmed:
            log.error("[ROTATION_OPEN_FAILED] wallet=%s reason=entry_not_confirmed", state.wallet)
            return Decision.CLOSE_FAILED
        state.held_asset = challenger.coin
        state.held_side = challenger.direction
        return Decision.COMPLETED


def stream_events(payload: str) -> list[tuple[str, dict[str, Any]]]:
    decoded = json.loads(payload)
    if isinstance(decoded, dict) and isinstance(decoded.get("data"), dict):
        decoded = decoded["data"]
    events = decoded.get("events", []) if isinstance(decoded, dict) else decoded
    result: list[tuple[str, dict[str, Any]]] = []
    if not isinstance(events, list):
        return result
    for event in events:
        if isinstance(event, list) and len(event) == 2 and isinstance(event[1], dict):
            result.append((str(event[0]).lower(), event[1]))
        elif isinstance(event, dict):
            user = str(event.get("user", "")).lower()
            data = event.get("data") if isinstance(event.get("data"), dict) else event
            if user:
                result.append((user, data))
    return result


async def subscription_requests(stream_type: int, users: list[str], name: str):
    filters = {}
    if users:
        filters["user"] = hypercore_pb2.FilterValues(values=users)
    yield hypercore_pb2.SubscribeRequest(
        subscribe=hypercore_pb2.StreamSubscribe(
            stream_type=stream_type,
            filters=filters,
            filter_name=name,
        )
    )
    if stream_type == hypercore_pb2.TRADES:
        grpc_ready.set()
    while True:
        await asyncio.sleep(25)
        yield hypercore_pb2.SubscribeRequest(
            ping=hypercore_pb2.Ping(timestamp=int(time.time() * 1000))
        )


async def stream_data_loop(
    stub: hypercore_grpc.StreamingStub,
    stream_type: int,
    users: list[str],
    name: str,
) -> None:
    metadata = (("x-token", GRPC_TOKEN),)
    while True:
        try:
            responses = stub.StreamData(
                subscription_requests(stream_type, users, name),
                metadata=metadata,
            )
            log.info("[GRPC_SUBSCRIBED] stream=%s users=%s", name, len(users))
            async for update in responses:
                if not update.HasField("data"):
                    continue
                for user, event in stream_events(update.data.data):
                    await trade_queue.put((user, event))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("[GRPC_STREAM_ERROR] stream=%s error=%s", name, exc)
            await asyncio.sleep(1)


async def bbo_loop(stub: orderbook_grpc.OrderBookStreamingStub) -> None:
    metadata = (("x-token", GRPC_TOKEN),)
    while True:
        try:
            responses = stub.StreamBboBook(orderbook_pb2.BboBookRequest(), metadata=metadata)
            log.info("[GRPC_SUBSCRIBED] stream=BBO")
            async for update in responses:
                if update.HasField("bid") and update.HasField("ask"):
                    bid, ask = dec(update.bid.px), dec(update.ask.px)
                    if bid > ZERO and ask > ZERO:
                        mid_prices[update.coin] = (bid + ask) / Decimal(2)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("[GRPC_STREAM_ERROR] stream=BBO error=%s", exc)
            await asyncio.sleep(1)


async def event_consumer() -> None:
    while True:
        user, event = await trade_queue.get()
        try:
            if user in wallet_states and "px" in event and "sz" in event:
                await process_leader_fill(user, event)
            elif FOLLOWER and user == FOLLOWER and "px" in event and "sz" in event:
                await observe_follower_fill(event)
            elif FOLLOWER and user == FOLLOWER:
                log.info(
                    "[FOLLOWER_ORDER_EVENT] asset=%s status=%s oid=%s",
                    event.get("coin"),
                    event.get("status") or event.get("orderStatus"),
                    event.get("oid"),
                )
        except Exception:
            log.exception("[EVENT_PROCESSING_ERROR] user=%s", user)
        finally:
            trade_queue.task_done()


async def main() -> None:
    global exchange
    credentials = grpc.ssl_channel_credentials()
    options = [
        ("grpc.ssl_target_name_override", GRPC_SERVER_NAME),
        ("grpc.default_authority", GRPC_SERVER_NAME),
        ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ("grpc.keepalive_time_ms", 20_000),
        ("grpc.keepalive_timeout_ms", 10_000),
    ]
    channel = grpc.aio.secure_channel(GRPC_ENDPOINT, credentials, options=options)
    await asyncio.wait_for(channel.channel_ready(), timeout=20)
    stream_stub = hypercore_grpc.StreamingStub(channel)
    book_stub = orderbook_grpc.OrderBookStreamingStub(channel)

    users = list(LEADERS)
    if FOLLOWER:
        users.append(FOLLOWER)
    tasks = [
        asyncio.create_task(
            stream_data_loop(stream_stub, hypercore_pb2.TRADES, users, "TRADES"),
            name="grpc-trades",
        ),
        asyncio.create_task(bbo_loop(book_stub), name="grpc-bbo"),
    ]
    if FOLLOWER:
        tasks.append(
            asyncio.create_task(
                stream_data_loop(stream_stub, hypercore_pb2.ORDERS, [FOLLOWER], "ORDERS"),
                name="grpc-orders",
            )
        )

    await asyncio.wait_for(grpc_ready.wait(), timeout=10)
    await asyncio.to_thread(load_startup_state)
    if EXECUTION_ENABLED:
        exchange = Exchange(
            Account.from_key(PRIVATE_KEY),
            EXCHANGE_URL,
            account_address=FOLLOWER,
        )
    else:
        log.warning("[EXECUTION_DISABLED] observation only; no order client created")

    tasks.append(asyncio.create_task(event_consumer(), name="event-consumer"))
    log.info(
        "[BOOT] production HRS ready leaders=%s execution_enabled=%s polling=false",
        len(LEADERS),
        EXECUTION_ENABLED,
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(main())

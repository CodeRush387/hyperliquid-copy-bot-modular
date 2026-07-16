from decimal import Decimal

from config import ALLOCATION_GAP_PCT, EXECUTION_ENABLED, ZERO, log
from models import CapitalEvent, Decision, Fill, FillKind, Lifecycle, LifecycleStatus, WalletState
from utils import fmt, sign


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
        log.debug(
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
        return (
            item.acceleration
            if item.acceleration is not None
            else Decimal("-Infinity"),
            item.current_capital,
        )

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
    log.debug("================ HRS WALLET RACE ================")
    log.debug("Wallet: %s", state.wallet)
    log.debug("Event Timestamp: %s", trigger.timestamp_ms)
    log.debug(
        "Trigger Fill: asset=%s kind=%s tid=%s quantity=%s price=%s",
        trigger.coin,
        trigger.kind.value,
        trigger.tid or "UNAVAILABLE",
        trigger.quantity,
        trigger.price,
    )
    log.debug("Execution Enabled: %s", EXECUTION_ENABLED)
    log.debug("Currently Held Asset: %s", held_asset_before or "NONE")
    log.debug("Race Ready: %s (%s)", state.race_ready, state.race_reason)
    log.debug(
        "Rank | Asset | Side | Lifecycle Status | Capital | Capital Share | Share Change | "
        "Latest Capital Added | Latest Interval | Velocity | Acceleration | Eligible"
    )
    for rank, lifecycle in enumerate(ranked, start=1):
        event = lifecycle.latest_capital_event
        log.debug(
            "%s | %s | %s | %s | %s | %s%% | %spp | %s | %s | %s | %s | %s",
            rank,
            lifecycle.coin,
            lifecycle.direction,
            lifecycle.status.value,
            fmt(lifecycle.current_capital, 4)
            if lifecycle.current_capital > ZERO
            else "UNAVAILABLE",
            fmt(lifecycle.share * 100, 6)
            if lifecycle.current_capital > ZERO
            else "UNAVAILABLE",
            fmt(lifecycle.share_change * 100, 6)
            if lifecycle.current_capital > ZERO
            else "UNAVAILABLE",
            fmt(event.capital_added, 4) if event else "UNAVAILABLE",
            fmt(event.interval_seconds, 6) if event else "UNAVAILABLE",
            fmt(lifecycle.velocity, 8),
            fmt(lifecycle.acceleration, 12),
            is_eligible(lifecycle, trigger, held_asset_before)
            and state.race_ready,
        )

    acceleration_positive = bool(
        challenger
        and challenger.acceleration
        and challenger.acceleration > ZERO
    )
    share_increasing = bool(challenger and challenger.share_change > ZERO)
    lifecycle_valid = bool(challenger and challenger.valid)
    log.debug("Challenger: %s", challenger.coin if challenger else "NONE")
    log.debug(
        "Challenger Acceleration: %s",
        fmt(challenger.acceleration, 12) if challenger else "UNAVAILABLE",
    )
    log.debug(
        "Challenger Share: %s%%",
        fmt(challenger.share * 100, 6) if challenger else "UNAVAILABLE",
    )
    log.debug("Held Asset Share: %s%%", fmt(held_share * 100, 6))
    log.debug("Allocation Gap: %s percentage points", fmt(gap, 6))
    log.debug(
        "Required Gap: %s percentage points",
        fmt(ALLOCATION_GAP_PCT, 6),
    )
    log.debug("Acceleration Positive: %s", acceleration_positive)
    log.debug("Share Increasing: %s", share_increasing)
    log.debug("Lifecycle Valid: %s", lifecycle_valid)
    log.debug("Rotation Conditions Met: %s", conditions_met)
    log.debug("Decision: %s", decision.value)

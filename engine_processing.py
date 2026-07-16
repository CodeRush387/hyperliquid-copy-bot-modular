from decimal import Decimal
from typing import Any

from config import ALLOCATION_GAP_PCT, EXECUTION_ENABLED, FOLLOWER, ZERO, log
from engine_execution import execute_initial_entry, execute_rotation
from engine_lifecycle import is_eligible, leaderboard, log_race, rebuild_wallet
from models import Decision, Fill, FillKind, Lifecycle, LifecycleStatus, WalletState
from state import follower_state
from engine_projection import require_persisted_wallet

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
            log.info(
                "[ROTATE_SIGNAL] wallet=%s challenger=%s",
                state.wallet,
                challenger.coin,
            )
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
        state.snapshot_capital[fill.coin] = (
            prior_capital + fill.quantity * fill.price
        )
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
    state = require_persisted_wallet(wallet)
    fill = Fill.from_raw(wallet, raw)
    if not fill.coin or fill.quantity <= ZERO or fill.price <= ZERO:
        log.warning(
            "[FILL_REJECTED] wallet=%s reason=invalid_fields raw=%s",
            wallet,
            raw,
        )
        return

    if fill.event_id in state.seen_events:
        return

    previous_shares = {
        coin: lifecycle.share
        for coin, lifecycle in state.lifecycles.items()
    }

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


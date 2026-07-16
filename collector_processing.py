import asyncio
from typing import Any

from config import ZERO, log
from lifecycle import rebuild_wallet
from models import Fill, FillKind, Lifecycle, LifecycleStatus, WalletState
from engine_projection import update as update_projection
from persistence import save_current_state
from state import wallet_states


def update_snapshot_capital(state: WalletState, fill: Fill) -> None:
    kind = fill.kind
    prior_capital = state.snapshot_capital.get(fill.coin)
    if kind in {FillKind.OPEN, FillKind.REVERSAL}:
        state.snapshot_capital[fill.coin] = abs(fill.after_position) * fill.price
    elif kind == FillKind.INCREASE:
        if prior_capital is not None:
            state.snapshot_capital[fill.coin] = prior_capital + fill.quantity * fill.price
        else:
            state.snapshot_capital[fill.coin] = abs(fill.after_position) * fill.price
    elif kind == FillKind.REDUCTION:
        if prior_capital is not None and fill.start_position != ZERO:
            state.snapshot_capital[fill.coin] = (
                prior_capital * abs(fill.after_position) / abs(fill.start_position)
            )
        else:
            state.snapshot_capital[fill.coin] = abs(fill.after_position) * fill.price
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
    prior_position = state.snapshot_positions.get(fill.coin)

    if prior_position is None:
        state.snapshot_positions[fill.coin] = fill.start_position
        prior_position = fill.start_position

    if prior_position != fill.start_position:
        log.warning(
            "[LIVE_POSITION_RESYNC] wallet=%s asset=%s tracked=%s fill_start=%s",
            wallet,
            fill.coin,
            prior_position,
            fill.start_position,
        )
        state.snapshot_positions[fill.coin] = fill.start_position
        prior_position = fill.start_position

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
    update_projection(wallet, state)
    await asyncio.to_thread(save_current_state)











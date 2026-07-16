import asyncio
from typing import Any

from config import ZERO, log
from engine_projection import update as update_projection
from lifecycle import rebuild_wallet
from models import Fill, FillKind, Lifecycle, LifecycleStatus, WalletState
from persistence import save_current_state
from startup import snapshot_portfolio
from state import wallet_states


def update_snapshot_capital(state: WalletState, fill: Fill) -> None:
    kind = fill.kind
    prior_capital = state.snapshot_capital.get(fill.coin)

    if kind in {FillKind.OPEN, FillKind.REVERSAL}:
        state.snapshot_capital[fill.coin] = (
            abs(fill.after_position) * fill.price
        )

    elif kind == FillKind.INCREASE:
        if prior_capital is not None:
            state.snapshot_capital[fill.coin] = (
                prior_capital + fill.quantity * fill.price
            )
        else:
            state.snapshot_capital[fill.coin] = (
                abs(fill.after_position) * fill.price
            )

    elif kind == FillKind.REDUCTION:
        if prior_capital is not None and fill.start_position != ZERO:
            state.snapshot_capital[fill.coin] = (
                prior_capital
                * abs(fill.after_position)
                / abs(fill.start_position)
            )
        else:
            state.snapshot_capital[fill.coin] = (
                abs(fill.after_position) * fill.price
            )

    elif kind == FillKind.FULL_CLOSE:
        state.snapshot_capital.pop(fill.coin, None)

    else:
        state.snapshot_capital.pop(fill.coin, None)
        log.warning(
            "[CAPITAL_UNAVAILABLE] wallet=%s asset=%s kind=%s "
            "reason=authoritative_allocation_unavailable",
            state.wallet,
            fill.coin,
            kind.value,
        )


async def persist_state(wallet: str, state: WalletState) -> None:
    await asyncio.to_thread(update_projection, wallet, state)
    await asyncio.to_thread(save_current_state)


async def authoritative_reconcile(
    wallet: str,
    state: WalletState,
    fill: Fill,
) -> bool:
    """
    Refresh the wallet directly from Hyperliquid after a position gap.

    Returns True only when the incoming fill still needs to be applied.
    Returns False when the authoritative snapshot already contains this
    fill or a newer state.
    """
    positions, capital = await asyncio.to_thread(
        snapshot_portfolio,
        wallet,
    )

    state.snapshot_positions = positions
    state.snapshot_capital = capital

    authoritative_position = positions.get(fill.coin, ZERO)

    if (
        authoritative_position == ZERO
        and (
            ":" in fill.coin
            or fill.coin.startswith("@")
        )
    ):
        log.warning(
            "[AUTHORITATIVE_DEX_POSITION_UNAVAILABLE] "
            "wallet=%s asset=%s fill_start=%s fill_after=%s",
            wallet,
            fill.coin,
            fill.start_position,
            fill.after_position,
        )
        return True

    log.warning(
        "[LIVE_POSITION_RECONCILED] wallet=%s asset=%s "
        "fill_start=%s fill_after=%s authoritative=%s",
        wallet,
        fill.coin,
        fill.start_position,
        fill.after_position,
        authoritative_position,
    )

    # الـsnapshot لم يستلم هذا الحدث بعد؛ طبّق الحدث الآن.
    if authoritative_position == fill.start_position:
        return True

    # الـsnapshot يحتوي هذا الحدث بالفعل.
    if authoritative_position == fill.after_position:
        state.seen_events.add(fill.event_id)

        rebuild_wallet(
            state,
            startup=False,
            previous={
                coin: lifecycle.share
                for coin, lifecycle in state.lifecycles.items()
            },
        )

        await persist_state(wallet, state)

        log.info(
            "[BUFFERED_FILL_ALREADY_REFLECTED] "
            "wallet=%s asset=%s tid=%s",
            wallet,
            fill.coin,
            fill.tid or "UNAVAILABLE",
        )
        return False

    # الـsnapshot أصبح أحدث من الحدث؛ لا نرجّع الحالة للخلف.
    state.seen_events.add(fill.event_id)

    rebuild_wallet(
        state,
        startup=False,
        previous={
            coin: lifecycle.share
            for coin, lifecycle in state.lifecycles.items()
        },
    )

    await persist_state(wallet, state)

    log.info(
        "[STALE_BUFFERED_FILL_SKIPPED] wallet=%s asset=%s "
        "fill_start=%s fill_after=%s authoritative=%s tid=%s",
        wallet,
        fill.coin,
        fill.start_position,
        fill.after_position,
        authoritative_position,
        fill.tid or "UNAVAILABLE",
    )
    return False


async def process_leader_fill(
    wallet: str,
    raw: dict[str, Any],
) -> None:
    state = wallet_states[wallet]
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

    tracked_position = state.snapshot_positions.get(
        fill.coin,
        ZERO,
    )

    if tracked_position != fill.start_position:
        log.warning(
            "[LIVE_POSITION_GAP] wallet=%s asset=%s "
            "tracked=%s fill_start=%s",
            wallet,
            fill.coin,
            tracked_position,
            fill.start_position,
        )

        should_apply = await authoritative_reconcile(
            wallet,
            state,
            fill,
        )

        if not should_apply:
            return

        previous_shares = {
            coin: lifecycle.share
            for coin, lifecycle in state.lifecycles.items()
        }

    state.seen_events.add(fill.event_id)
    state.fills_by_coin.setdefault(
        fill.coin,
        [],
    ).append(fill)

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
            "[LIFECYCLE] wallet=%s asset=%s "
            "status=%s reason=live_full_close",
            wallet,
            fill.coin,
            LifecycleStatus.CLOSED.value,
        )
    else:
        state.snapshot_positions[fill.coin] = (
            fill.after_position
        )

    rebuild_wallet(
        state,
        startup=False,
        previous=previous_shares,
    )

    await persist_state(wallet, state)



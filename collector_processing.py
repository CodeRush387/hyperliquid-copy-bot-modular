import asyncio
from typing import Any

from config import ZERO, log
from engine_projection import update as update_projection
from lifecycle import rebuild_wallet
from models import Fill, FillKind, Lifecycle, LifecycleStatus, WalletState
from persistence import save_current_state
from startup import snapshot_portfolio
from state import wallet_states


# Lazy startup reconciliation:
# one authoritative snapshot per wallet, not one snapshot per fill.
_lazy_reconciled_wallets: set[str] = set()
_seeded_spot_assets: set[tuple[str, str]] = set()


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


async def lazy_reconcile_once(
    wallet: str,
    state: WalletState,
    fill: Fill,
) -> bool:
    """
    Perform at most one authoritative portfolio request per wallet.

    Old buffered fills are skipped using the same snapshot. Once a fill's
    start position matches the tracked position, normal sequential processing
    resumes without another authoritative request.
    """

    # Spot assets such as @107 are not returned by clearinghouseState.
    # Seed them once from the first buffered fill, then process sequentially.
    if fill.coin.startswith("@"):
        spot_key = (wallet, fill.coin)

        if spot_key not in _seeded_spot_assets:
            state.snapshot_positions[fill.coin] = fill.start_position
            _seeded_spot_assets.add(spot_key)

            log.info(
                "[LAZY_SPOT_SEEDED] wallet=%s asset=%s position=%s",
                wallet,
                fill.coin,
                fill.start_position,
            )

        tracked_position = state.snapshot_positions.get(fill.coin, ZERO)

        if tracked_position == fill.start_position:
            return True

        if tracked_position == fill.after_position:
            state.seen_events.add(fill.event_id)
            log.info(
                "[BUFFERED_FILL_ALREADY_REFLECTED] "
                "wallet=%s asset=%s tid=%s",
                wallet,
                fill.coin,
                fill.tid or "UNAVAILABLE",
            )
            return False

        state.seen_events.add(fill.event_id)
        log.info(
            "[STALE_BUFFERED_FILL_SKIPPED] wallet=%s asset=%s "
            "fill_start=%s fill_after=%s tracked=%s tid=%s",
            wallet,
            fill.coin,
            fill.start_position,
            fill.after_position,
            tracked_position,
            fill.tid or "UNAVAILABLE",
        )
        return False

    # Only the first gap for this wallet performs the expensive all-DEX pass.
    if wallet not in _lazy_reconciled_wallets:
        positions, capital = await asyncio.to_thread(
            snapshot_portfolio,
            wallet,
        )

        state.snapshot_positions = positions
        state.snapshot_capital = capital
        _lazy_reconciled_wallets.add(wallet)

        rebuild_wallet(
            state,
            startup=False,
            previous={
                coin: lifecycle.share
                for coin, lifecycle in state.lifecycles.items()
            },
        )

        await persist_state(wallet, state)

        log.warning(
            "[LAZY_WALLET_RECONCILED] wallet=%s "
            "positions=%s triggering_asset=%s",
            wallet,
            len(positions),
            fill.coin,
        )

    authoritative_position = state.snapshot_positions.get(
        fill.coin,
        ZERO,
    )

    # Snapshot is immediately before this fill: apply it.
    if authoritative_position == fill.start_position:
        log.info(
            "[BUFFER_CAUGHT_UP] wallet=%s asset=%s position=%s",
            wallet,
            fill.coin,
            authoritative_position,
        )
        return True

    # Snapshot already includes this exact fill.
    if authoritative_position == fill.after_position:
        state.seen_events.add(fill.event_id)

        log.info(
            "[BUFFERED_FILL_ALREADY_REFLECTED] "
            "wallet=%s asset=%s tid=%s",
            wallet,
            fill.coin,
            fill.tid or "UNAVAILABLE",
        )
        return False

    # Snapshot is newer than this buffered fill. Skip without another API call.
    state.seen_events.add(fill.event_id)

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

        should_apply = await lazy_reconcile_once(
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
        state.snapshot_positions[fill.coin] = fill.after_position

    rebuild_wallet(
        state,
        startup=False,
        previous=previous_shares,
    )

    await persist_state(wallet, state)

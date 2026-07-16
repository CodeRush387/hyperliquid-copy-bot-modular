import asyncio
import math
from decimal import Decimal
from typing import Any

from config import EXECUTION_CONFIRM_TIMEOUT, FIXED_NOTIONAL_USD, SLIPPAGE, ZERO, log
from models import Decision, Lifecycle, WalletState
import state as runtime
from utils import sign


async def wait_for_follower_position(coin: str, predicate) -> bool:
    async def wait_loop() -> None:
        async with runtime.follower_state.changed:
            await runtime.follower_state.changed.wait_for(
                lambda: predicate(runtime.follower_state.positions.get(coin, ZERO))
            )

    if predicate(runtime.follower_state.positions.get(coin, ZERO)):
        return True
    try:
        await asyncio.wait_for(wait_loop(), timeout=EXECUTION_CONFIRM_TIMEOUT)
        return True
    except asyncio.TimeoutError:
        return False


def order_ok(result: Any) -> bool:
    return isinstance(result, dict) and result.get("status") == "ok"


def close_order(coin: str, position: Decimal) -> Any:
    if runtime.exchange is None:
        raise RuntimeError("execution client is unavailable")
    is_buy = position < ZERO
    price = runtime.exchange._slippage_price(coin, is_buy, SLIPPAGE, None)
    return runtime.exchange.order(
        coin,
        is_buy,
        float(abs(position)),
        price,
        order_type={"limit": {"tif": "Ioc"}},
        reduce_only=True,
    )


def open_order(lifecycle: Lifecycle) -> Any:
    if runtime.exchange is None:
        raise RuntimeError("execution client is unavailable")
    price = runtime.mid_prices.get(lifecycle.coin)
    if price is None or price <= ZERO:
        raise RuntimeError(f"No gRPC BBO midpoint available for {lifecycle.coin}")
    precision = runtime.size_decimals.get(lifecycle.coin, 6)
    size = math.floor(float(FIXED_NOTIONAL_USD / price) * 10**precision) / 10**precision
    if size <= 0:
        raise RuntimeError(f"Calculated zero order size for {lifecycle.coin}")
    return runtime.exchange.market_open(
        lifecycle.coin,
        lifecycle.direction == "LONG",
        size,
        slippage=SLIPPAGE,
    )


async def execute_initial_entry(state: WalletState, challenger: Lifecycle) -> Decision:
    async with runtime.trade_lock:
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
    async with runtime.trade_lock:
        held_coin = state.held_asset
        assert held_coin is not None
        held_position = runtime.follower_state.positions.get(held_coin, ZERO)
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

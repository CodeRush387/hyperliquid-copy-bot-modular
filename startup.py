import time
from decimal import Decimal
from typing import Any

import requests

from config import EXECUTION_ENABLED, FOLLOWER, LEADERS, QUICKNODE_INFO_URL, log
from lifecycle import rebuild_wallet
from models import Fill
from state import follower_state, size_decimals, wallet_states
from utils import dec


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
        if coin and size != 0:
            positions[coin] = size
            if entry_price > 0:
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
        for fill in startup_fills(wallet):
            state.fills_by_coin.setdefault(fill.coin, []).append(fill)
            state.seen_events.add(fill.event_id)

        state.snapshot_positions, state.snapshot_capital = snapshot_portfolio(wallet)

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


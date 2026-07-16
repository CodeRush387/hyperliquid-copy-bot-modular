import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from config import log
from models import CapitalEvent, Fill, Lifecycle, WalletState
from state import mid_prices, size_decimals, wallet_states


STATE_DB_PATH = Path(os.getenv("COLLECTOR_STATE_DB_PATH", "/data/collector_state.db"))


def _fill_payload(fill: Fill) -> dict[str, Any]:
    return {
        "wallet": fill.wallet,
        "coin": fill.coin,
        "price": str(fill.price),
        "quantity": str(fill.quantity),
        "side": fill.side,
        "timestamp_ms": fill.timestamp_ms,
        "start_position": str(fill.start_position),
        "after_position": str(fill.after_position),
        "tid": fill.tid,
        "oid": fill.oid,
        "direction_label": fill.direction_label,
        "event_id": fill.event_id,
        "kind": fill.kind.value,
        "raw": fill.raw,
    }


def _capital_event_payload(event: CapitalEvent) -> dict[str, Any]:
    return {
        "fill_event_id": event.fill.event_id,
        "capital_added": str(event.capital_added),
        "interval_seconds": str(event.interval_seconds) if event.interval_seconds is not None else None,
        "velocity": str(event.velocity) if event.velocity is not None else None,
        "acceleration": str(event.acceleration) if event.acceleration is not None else None,
    }


def _lifecycle_payload(lifecycle: Lifecycle) -> dict[str, Any]:
    return {
        "coin": lifecycle.coin,
        "direction": lifecycle.direction,
        "status": lifecycle.status.value,
        "reason": lifecycle.reason,
        "fills": [_fill_payload(fill) for fill in lifecycle.fills],
        "capital_events": [
            _capital_event_payload(event) for event in lifecycle.capital_events
        ],
        "final_size": str(lifecycle.final_size),
        "current_capital": str(lifecycle.current_capital),
        "share": str(lifecycle.share),
        "previous_share": str(lifecycle.previous_share),
        "share_change": str(lifecycle.share_change),
        "velocity": str(lifecycle.velocity) if lifecycle.velocity is not None else None,
        "acceleration": (
            str(lifecycle.acceleration)
            if lifecycle.acceleration is not None
            else None
        ),
    }


def _wallet_payload(state: WalletState) -> dict[str, Any]:
    return {
        "wallet": state.wallet,
        "snapshot_positions": {
            coin: str(value) for coin, value in state.snapshot_positions.items()
        },
        "snapshot_capital": {
            coin: str(value) for coin, value in state.snapshot_capital.items()
        },
        "fills_by_coin": {
            coin: [_fill_payload(fill) for fill in fills]
            for coin, fills in state.fills_by_coin.items()
        },
        "seen_events": sorted(state.seen_events),
        "lifecycles": {
            coin: _lifecycle_payload(lifecycle)
            for coin, lifecycle in state.lifecycles.items()
        },
        "held_asset": state.held_asset,
        "held_side": state.held_side,
        "race_ready": state.race_ready,
        "race_reason": state.race_reason,
    }


def current_state_payload() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_at_ms": int(time.time() * 1000),
        "wallets": {
            wallet: _wallet_payload(state)
            for wallet, state in wallet_states.items()
        },
        "size_decimals": dict(size_decimals),
        "mid_prices": {coin: str(price) for coin, price in mid_prices.items()},
    }


def initialize_state_store() -> None:
    STATE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(STATE_DB_PATH) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS collector_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                updated_at_ms INTEGER NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        connection.commit()
    log.info("[COLLECTOR_STATE_STORE_READY] path=%s", STATE_DB_PATH)


def save_current_state() -> None:
    payload = current_state_payload()
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(STATE_DB_PATH) as connection:
        connection.execute(
            """
            INSERT INTO collector_state (id, updated_at_ms, payload)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                updated_at_ms=excluded.updated_at_ms,
                payload=excluded.payload
            """,
            (payload["updated_at_ms"], encoded),
        )
        connection.commit()


def read_current_state() -> dict[str, Any] | None:
    if not STATE_DB_PATH.exists():
        return None
    with sqlite3.connect(STATE_DB_PATH) as connection:
        row = connection.execute(
            "SELECT payload FROM collector_state WHERE id = 1"
        ).fetchone()
    return json.loads(row[0]) if row else None

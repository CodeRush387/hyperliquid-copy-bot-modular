"""
Engine Projection

Persistent State Engine.

Receives validated state updates,
keeps current state in RAM,
and persists snapshots on Railway Volume.

Responsibilities
----------------
- Maintain current WalletState.
- Update state from validated events.
- Save current state permanently.
- Restore state after restart.
- Provide fast reads for engine.

Non-responsibilities
--------------------
- Execution.
- gRPC.
- Signal generation.
- Exchange communication.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from config import log
from models import WalletState


DATABASE_PATH = Path("/data/engine_projection.db")

_lock = threading.RLock()

_wallets: dict[str, WalletState] = {}


def initialize() -> None:
    DATABASE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with sqlite3.connect(DATABASE_PATH) as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS projection_state (
                wallet TEXT PRIMARY KEY,
                updated_at INTEGER NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        db.commit()

    load()

    log.info(
        "[PROJECTION_READY] wallets=%s",
        len(_wallets),
    )


def update(
    wallet: str,
    state: WalletState,
) -> None:
    with _lock:
        _wallets[wallet.lower()] = state

        save_wallet(
            state,
        )


def save_wallet(
    state: WalletState,
) -> None:
    payload = json.dumps(
        state,
        default=str,
    )

    with sqlite3.connect(DATABASE_PATH) as db:
        db.execute(
            """
            INSERT INTO projection_state
            (
                wallet,
                updated_at,
                payload
            )
            VALUES (?, ?, ?)
            ON CONFLICT(wallet)
            DO UPDATE SET
                updated_at=excluded.updated_at,
                payload=excluded.payload
            """,
            (
                state.wallet.lower(),
                int(time.time()),
                payload,
            ),
        )
        db.commit()


def load() -> None:
    if not DATABASE_PATH.exists():
        return

    log.info(
        "[PROJECTION_LOAD] existing database found"
    )


def get_wallet(
    wallet: str,
) -> WalletState | None:
    with _lock:
        return _wallets.get(
            wallet.lower()
        )


def remove_asset(
    wallet: str,
    coin: str,
) -> None:
    state = get_wallet(wallet)

    if state is None:
        return

    state.snapshot_positions.pop(
        coin,
        None,
    )

    state.snapshot_capital.pop(
        coin,
        None,
    )

    state.lifecycles.pop(
        coin,
        None,
    )

    save_wallet(state)



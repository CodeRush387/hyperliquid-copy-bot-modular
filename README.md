# Hyperliquid HRS Engine

This service follows the capital rotation of one or more Hyperliquid leader wallets. It is not a copy-trading engine.

Each leader wallet has independent fills, position lifecycles, capital metrics, race leadership, and simulated/real follower-held state. History quality is evaluated per asset as `FULL_HISTORY`, `PARTIAL_HISTORY`, or `INSUFFICIENT_HISTORY`. Full history contains the verified opening/reversal boundary and every subsequent fill. Partial history is a verified contiguous suffix that reconciles to the authoritative current position and supplies real velocity and acceleration; its current capital comes from the authoritative entry price, never from an inferred opening fill. Assets with reliable partial metrics may race immediately. Insufficient assets remain visible but cannot become challengers, and they do not block reliable assets in the same wallet.

## Data flow

Normal live operation is event-driven:

- Leader fills: QuickNode HyperCore gRPC `StreamData/TRADES`, filtered by leader wallet.
- Market prices for execution sizing: QuickNode gRPC `StreamBboBook`.
- Follower confirmations: QuickNode gRPC `TRADES` and `ORDERS`, filtered by follower wallet.
- Startup only: QuickNode HTTPS `/info` for current positions, fill history, asset metadata, and follower position.
- Order submission only: official Hyperliquid execution API, because the existing QuickNode gRPC protocol exposes market data but no trading RPC.

There is no HTTPS polling loop, legacy WebSocket, or timer-generated race snapshot.

## Safety

`HRS_EXECUTION_ENABLED` defaults to `false`. In that mode the service reconstructs lifecycles, calculates metrics, evaluates every real fill, and logs simulated actions without creating an exchange client or submitting any order.

When execution is explicitly enabled, `FOLLOWER_ADDRESS` and `PRIVATE_KEY` are required. A rotation closes the current follower position and waits for gRPC confirmation that it is flat before opening the challenger. A failed or partial close aborts the entry.

## Configuration

Copy `.env.example` and provide the existing QuickNode gRPC values. `HRS_LEADER_ADDRESSES` accepts a comma-separated list. `HRS_ALLOCATION_GAP_PCT=3.0` means three percentage points of the leader wallet's total open-capital share.

Keep private keys out of Git. If a key has appeared in source or logs, rotate it before funding the wallet.

## Local verification

```bash
python -m unittest -v test_hrs.py
```

```bash
docker build -t hyperliquid-hrs .
docker run --env-file .env hyperliquid-hrs
```
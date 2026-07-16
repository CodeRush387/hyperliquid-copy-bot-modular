"""Production Hyperliquid Rotation Strategy driven by QuickNode gRPC events."""
import asyncio

import grpc
from eth_account import Account
from hyperliquid.exchange import Exchange

from config import (
    EXCHANGE_URL,
    EXECUTION_ENABLED,
    FOLLOWER,
    GRPC_ENDPOINT,
    GRPC_SERVER_NAME,
    LEADERS,
    PRIVATE_KEY,
    log,
)
from grpc_streams import (
    bbo_loop,
    event_consumer,
    hypercore_grpc,
    hypercore_pb2,
    orderbook_grpc,
    stream_data_loop,
)
from startup import load_startup_state
import state as runtime


async def main() -> None:
    credentials = grpc.ssl_channel_credentials()
    options = [
        ("grpc.ssl_target_name_override", GRPC_SERVER_NAME),
        ("grpc.default_authority", GRPC_SERVER_NAME),
        ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ("grpc.keepalive_time_ms", 20_000),
        ("grpc.keepalive_timeout_ms", 10_000),
    ]
    channel = grpc.aio.secure_channel(GRPC_ENDPOINT, credentials, options=options)
    await asyncio.wait_for(channel.channel_ready(), timeout=20)
    stream_stub = hypercore_grpc.StreamingStub(channel)
    book_stub = orderbook_grpc.OrderBookStreamingStub(channel)

    users = list(LEADERS)
    if FOLLOWER:
        users.append(FOLLOWER)
    tasks = [
        asyncio.create_task(
            stream_data_loop(stream_stub, hypercore_pb2.TRADES, users, "TRADES"),
            name="grpc-trades",
        ),
        asyncio.create_task(bbo_loop(book_stub), name="grpc-bbo"),
    ]
    if FOLLOWER:
        tasks.append(
            asyncio.create_task(
                stream_data_loop(stream_stub, hypercore_pb2.ORDERS, [FOLLOWER], "ORDERS"),
                name="grpc-orders",
            )
        )

    await asyncio.wait_for(runtime.grpc_ready.wait(), timeout=10)
    await asyncio.to_thread(load_startup_state)
    if EXECUTION_ENABLED:
        runtime.exchange = Exchange(
            Account.from_key(PRIVATE_KEY),
            EXCHANGE_URL,
            account_address=FOLLOWER,
        )
    else:
        log.warning("[EXECUTION_DISABLED] observation only; no order client created")

    tasks.append(asyncio.create_task(event_consumer(), name="event-consumer"))
    log.info(
        "[BOOT] production HRS ready leaders=%s execution_enabled=%s polling=false",
        len(LEADERS),
        EXECUTION_ENABLED,
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(main())

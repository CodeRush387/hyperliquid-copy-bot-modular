"""Continuous QuickNode data collector without trading execution."""
import asyncio

import grpc

from collector_grpc import (
    event_consumer,
    hypercore_grpc,
    hypercore_pb2,
    stream_data_loop,
)
from config import GRPC_ENDPOINT, GRPC_SERVER_NAME, LEADERS, log
from persistence import initialize_state_store, save_current_state
from startup import load_startup_state
from state import grpc_ready


MAX_LEADER_WALLETS = 25


async def main() -> None:
    if len(LEADERS) > MAX_LEADER_WALLETS:
        raise RuntimeError(
            f"HRS_LEADER_ADDRESSES supports at most {MAX_LEADER_WALLETS} wallets"
        )

    initialize_state_store()
    log.info("============= ACTIVE LEADER WALLETS =============")
    for index, wallet in enumerate(LEADERS, start=1):
        log.info("%s. %s", index, wallet)

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

    users = list(LEADERS)
    tasks = [
        asyncio.create_task(
            stream_data_loop(stream_stub, hypercore_pb2.TRADES, users, "TRADES"),
            name="grpc-trades",
        )
    ]

    await asyncio.wait_for(grpc_ready.wait(), timeout=10)
    await asyncio.to_thread(load_startup_state)
    await asyncio.to_thread(save_current_state)

    tasks.append(asyncio.create_task(event_consumer(), name="event-consumer"))
    log.info(
        "[COLLECTOR_BOOT] ready leaders=%s execution=false polling=false",
        len(LEADERS),
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(main())

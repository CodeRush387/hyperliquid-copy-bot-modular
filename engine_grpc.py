import asyncio
import json
import sys
import time
from pathlib import Path

PROTO_DIR = Path(__file__).with_name("quicknode_proto")
sys.path.insert(0, str(PROTO_DIR))
import hyperliquid_pb2 as hypercore_pb2  # noqa: E402
import hyperliquid_pb2_grpc as hypercore_grpc  # noqa: E402
import orderbook_pb2 as orderbook_pb2  # noqa: E402
import orderbook_pb2_grpc as orderbook_grpc  # noqa: E402

from config import FOLLOWER, GRPC_TOKEN, ZERO, log
from engine_processing import observe_follower_fill, process_leader_fill
from state import grpc_ready, mid_prices, trade_queue, wallet_states
from utils import dec


def stream_events(payload: str) -> list[tuple[str, dict]]:
    decoded = json.loads(payload)

    if isinstance(decoded, dict) and isinstance(decoded.get("data"), dict):
        decoded = decoded["data"]

    events = (
        decoded.get("events", [])
        if isinstance(decoded, dict)
        else decoded
    )

    result: list[tuple[str, dict]] = []

    if not isinstance(events, list):
        return result

    for event in events:
        if (
            isinstance(event, list)
            and len(event) == 2
            and isinstance(event[1], dict)
        ):
            result.append(
                (
                    str(event[0]).lower(),
                    event[1],
                )
            )
        elif isinstance(event, dict):
            user = str(event.get("user", "")).lower()
            data = (
                event.get("data")
                if isinstance(event.get("data"), dict)
                else event
            )
            if user:
                result.append((user, data))

    return result


async def subscription_requests(
    stream_type: int,
    users: list[str],
    name: str,
):
    filters = {}

    if users:
        filters["user"] = hypercore_pb2.FilterValues(
            values=users
        )

    yield hypercore_pb2.SubscribeRequest(
        subscribe=hypercore_pb2.StreamSubscribe(
            stream_type=stream_type,
            filters=filters,
            filter_name=name,
        )
    )

    if stream_type == hypercore_pb2.TRADES:
        grpc_ready.set()

    while True:
        await asyncio.sleep(25)
        yield hypercore_pb2.SubscribeRequest(
            ping=hypercore_pb2.Ping(
                timestamp=int(time.time() * 1000)
            )
        )


async def stream_data_loop(
    stub: hypercore_grpc.StreamingStub,
    stream_type: int,
    users: list[str],
    name: str,
) -> None:
    metadata = (("x-token", GRPC_TOKEN),)

    while True:
        try:
            responses = stub.StreamData(
                subscription_requests(
                    stream_type,
                    users,
                    name,
                ),
                metadata=metadata,
            )

            log.info(
                "[GRPC_SUBSCRIBED] stream=%s users=%s",
                name,
                len(users),
            )

            async for update in responses:
                if not update.HasField("data"):
                    continue

                for user, event in stream_events(update.data.data):
                    await trade_queue.put((user, event))

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log.error(
                "[GRPC_STREAM_ERROR] stream=%s error=%s",
                name,
                exc,
            )
            await asyncio.sleep(1)


async def bbo_loop(
    stub: orderbook_grpc.OrderBookStreamingStub,
) -> None:
    metadata = (("x-token", GRPC_TOKEN),)

    while True:
        try:
            responses = stub.StreamBboBook(
                orderbook_pb2.BboBookRequest(),
                metadata=metadata,
            )

            log.info("[GRPC_SUBSCRIBED] stream=BBO")

            async for update in responses:
                if update.HasField("bid") and update.HasField("ask"):
                    bid = dec(update.bid.px)
                    ask = dec(update.ask.px)

                    if bid > ZERO and ask > ZERO:
                        mid_prices[update.coin] = (bid + ask) / 2

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            log.error(
                "[GRPC_STREAM_ERROR] stream=BBO error=%s",
                exc,
            )
            await asyncio.sleep(1)


async def event_consumer() -> None:
    while True:
        user, event = await trade_queue.get()

        try:
            if (
                user in wallet_states
                and "px" in event
                and "sz" in event
            ):
                await process_leader_fill(user, event)

            elif (
                FOLLOWER
                and user == FOLLOWER
                and "px" in event
                and "sz" in event
            ):
                await observe_follower_fill(event)

            elif FOLLOWER and user == FOLLOWER:
                log.info(
                    "[FOLLOWER_ORDER_EVENT] asset=%s status=%s oid=%s",
                    event.get("coin"),
                    event.get("status") or event.get("orderStatus"),
                    event.get("oid"),
                )

        except Exception:
            log.exception(
                "[EVENT_PROCESSING_ERROR] user=%s",
                user,
            )

        finally:
            trade_queue.task_done()


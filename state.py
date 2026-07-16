import asyncio
from decimal import Decimal

from hyperliquid.exchange import Exchange

from config import LEADERS
from models import FollowerState, WalletState


wallet_states = {wallet: WalletState(wallet=wallet) for wallet in LEADERS}
follower_state = FollowerState()
size_decimals: dict[str, int] = {}
mid_prices: dict[str, Decimal] = {}
trade_queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue()
grpc_ready = asyncio.Event()
exchange: Exchange | None = None
trade_lock = asyncio.Lock()

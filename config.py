import logging
import os
from decimal import Decimal, InvalidOperation, getcontext

from hyperliquid.utils import constants

getcontext().prec = 40
ZERO = Decimal("0")


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"Invalid boolean environment variable: {name}")


def env_decimal(name: str, default: str) -> Decimal:
    try:
        return Decimal(os.getenv(name, default).strip() or default)
    except InvalidOperation as exc:
        raise RuntimeError(f"Invalid decimal environment variable: {name}") from exc


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_leaders() -> tuple[str, ...]:
    raw = (
        os.getenv("HRS_LEADER_ADDRESSES")
        or os.getenv("LEADER_ADDRESSES")
        or os.getenv("LEADER_ADDRESS")
        or ""
    )
    leaders = tuple(dict.fromkeys(part.strip().lower() for part in raw.split(",") if part.strip()))
    if not leaders:
        raise RuntimeError("HRS_LEADER_ADDRESSES or LEADER_ADDRESS is required")
    for wallet in leaders:
        if not wallet.startswith("0x") or len(wallet) != 42:
            raise RuntimeError(f"Invalid leader wallet address: {wallet}")
    return leaders


LEADERS = parse_leaders()
FOLLOWER = os.getenv("FOLLOWER_ADDRESS", "").strip().lower()
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
GRPC_ENDPOINT = required("HC_GRPC_ENDPOINT")
GRPC_TOKEN = required("HC_GRPC_TOKEN")
GRPC_SERVER_NAME = os.getenv("HC_GRPC_SERVER_NAME", "").strip() or GRPC_ENDPOINT.rsplit(":", 1)[0]
QUICKNODE_INFO_URL = os.getenv("QUICKNODE_INFO_URL", "").strip()
if not QUICKNODE_INFO_URL:
    QUICKNODE_INFO_URL = f"https://{GRPC_SERVER_NAME}/{GRPC_TOKEN}/info"

EXECUTION_ENABLED = env_bool("HRS_EXECUTION_ENABLED", False)
ALLOCATION_GAP_PCT = env_decimal("HRS_ALLOCATION_GAP_PCT", "3.0")
FIXED_NOTIONAL_USD = env_decimal("FIXED_NOTIONAL_USD", "12")
SLIPPAGE = float(os.getenv("SLIPPAGE", "0.05"))
EXECUTION_CONFIRM_TIMEOUT = float(os.getenv("HRS_EXECUTION_CONFIRM_TIMEOUT_SECONDS", "15"))
TESTNET = env_bool("TESTNET", False)
EXCHANGE_URL = constants.TESTNET_API_URL if TESTNET else constants.MAINNET_API_URL

if EXECUTION_ENABLED and (not FOLLOWER or not PRIVATE_KEY):
    raise RuntimeError("FOLLOWER_ADDRESS and PRIVATE_KEY are required when HRS_EXECUTION_ENABLED=true")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hrs")

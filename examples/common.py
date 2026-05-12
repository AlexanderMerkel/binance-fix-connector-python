"""Shared utilities for example scripts."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path
from typing import Any, NamedTuple

import aiohttp
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
from simplefix import FixMessage

from binance_fix_connector_async.fix_connector import BinanceFixConnector
from binance_fix_connector_async.utils import get_api_key, get_private_key

logger = logging.getLogger(__name__)

TESTNET_OE_URL = "tcp+tls://fix-oe.testnet.binance.vision:9000"
TESTNET_MD_URL = "tcp+tls://fix-md.testnet.binance.vision:9000"
TESTNET_DC_URL = "tcp+tls://fix-dc.testnet.binance.vision:9000"
TESTNET_REST_URL = "https://testnet.binance.vision"

ORD_STATUS = {
    "0": "NEW",
    "1": "PARTIALLY_FILLED",
    "2": "FILLED",
    "4": "CANCELED",
    "6": "PENDING_CANCEL",
    "8": "REJECTED",
    "A": "PENDING_NEW",
    "C": "EXPIRED",
}
ORD_TYPES = {"1": "MARKET", "2": "LIMIT", "3": "STOP", "4": "STOP_LIMIT"}
SIDES = {"1": "BUY", "2": "SELL"}
TIME_IN_FORCE = {
    "1": "GOOD_TILL_CANCEL",
    "3": "IMMEDIATE_OR_CANCEL",
    "4": "FILL_OR_KILL",
}


class SafeLimitOrderParams(NamedTuple):
    price: str
    quantity: str


def load_credentials(
    config_dir: Path | None = None,
) -> tuple[str, PrivateKeyTypes]:
    """
    Load API key and private key from config.json or config.ini.

    Args:
        config_dir: Directory containing config files. Defaults to examples/.

    Returns:
        Tuple of (api_key, private_key)
    """
    if config_dir is None:
        config_dirs = [Path(__file__).resolve().parent.parent, Path(__file__).parent]
    else:
        config_dirs = [config_dir]

    for directory in config_dirs:
        for config_name in ("config.json", "config.ini"):
            config_path = directory / config_name
            if config_path.exists():
                api_key, private_key_path = get_api_key(str(config_path))
                return api_key, get_private_key(private_key_path)

    fix_env = os.getenv("BINANCE_FIX_ENV")
    if fix_env is None:
        fix_env = "mainnet" if os.getenv("USE_TESTNET", "true").lower() == "false" else "testnet"

    if fix_env == "testnet":
        env_key = os.getenv("BINANCE_TESTNET_FIX_KEY") or os.getenv("BINANCE_TESTNET_API_KEY")
        env_pem = os.getenv("BINANCE_TESTNET_FIX_PRIVATE_KEY_PATH") or os.getenv("BINANCE_TESTNET_PRIVATE_KEY_PATH")
    else:
        env_key = os.getenv("BINANCE_FIX_KEY") or os.getenv("BINANCE_API_KEY")
        env_pem = os.getenv("BINANCE_FIX_PRIVATE_KEY_PATH")

    if env_key and env_pem:
        return env_key, get_private_key(env_pem)
    if env_key and not env_pem:
        raise FileNotFoundError("FIX API key is set, but no matching private key path environment variable is set.")

    raise FileNotFoundError(
        "No credentials found. Provide config.json, config.ini, or set "
        "BINANCE_FIX_KEY and BINANCE_FIX_PRIVATE_KEY_PATH environment variables."
    )


def get_field(message, tag: int) -> str | None:
    """Extract a decoded string field from a FIX message, or None if absent."""
    raw = message.get(tag)
    return raw.decode("utf-8") if raw else None


async def run_stream_loop(session, handler, duration=30, poll_interval=1) -> int:
    """Poll for messages and pass each to handler. Returns count of handled messages."""
    start_time = time.time()
    count = 0
    while time.time() - start_time < duration:
        for msg in await session.get_all_new_messages_received():
            if handler(msg):
                count += 1
        await asyncio.sleep(poll_interval)
    return count


async def wait_for_logon(session: BinanceFixConnector, timeout: int = 10) -> bool:
    """Wait for logon response. Returns True if logon succeeded."""
    responses = await session.retrieve_messages_until(message_type=["A", "3", "5"], timeout_seconds=timeout)
    if not responses:
        session.logger.error("Failed to receive logon response")
        return False
    for response in responses:
        msg_type = get_field(response, 35)
        if msg_type == "A":
            session.logger.info("Successfully logged in")
            return True
        if msg_type in {"3", "5"}:
            session.logger.warning("Server rejected logon: %s", get_field(response, 58))
            return False
    session.logger.error("Failed to receive logon response")
    return False


async def graceful_logout(session: BinanceFixConnector) -> None:
    """Logout and disconnect cleanly."""
    await session.logout()
    await session.retrieve_messages_until(message_type="5", timeout_seconds=5)
    await session.disconnect()


def is_order_open(status: str | None) -> bool:
    return status in {"0", "1", "A"}


async def cancel_order_by_cl_ord_id(
    session: BinanceFixConnector,
    cl_ord_id: str,
    symbol: str,
    *,
    timeout_seconds: float = 10,
) -> FixMessage | None:
    """Cancel an open order by original ClOrdID."""
    cancel_id = f"CANCEL_{time.time_ns()}"
    msg = await session.create_fix_message_with_basic_header("F")
    msg.append_pair(11, cancel_id)
    msg.append_pair(41, cl_ord_id)
    msg.append_pair(55, symbol)
    await session.send_message(msg)

    responses = await session.retrieve_messages_until(message_type=["3", "8", "9"], timeout_seconds=timeout_seconds)
    for response in responses:
        msg_type = get_field(response, 35)
        if msg_type == "3" or get_field(response, 11) == cancel_id:
            return response
    return None


async def get_safe_limit_price(symbol: str, side: str, *, base_url: str = TESTNET_REST_URL) -> str:
    """Return a current-market non-marketable limit price for testnet examples."""
    return (await get_safe_limit_order_params(symbol, side, base_url=base_url)).price


async def get_safe_limit_order_params(
    symbol: str,
    side: str,
    *,
    base_url: str = TESTNET_REST_URL,
    target_quantity: str | Decimal = "1",
) -> SafeLimitOrderParams:
    """Return exchange-filter-compliant non-marketable limit order parameters."""
    async with aiohttp.ClientSession() as session:
        ticker = await _fetch_json(session, f"{base_url}/api/v3/ticker/bookTicker", {"symbol": symbol})
        avg_price = await _fetch_json(session, f"{base_url}/api/v3/avgPrice", {"symbol": symbol})
        exchange_info = await _fetch_json(session, f"{base_url}/api/v3/exchangeInfo", {"symbol": symbol})

    filters = _symbol_filters(exchange_info)
    price = _safe_limit_price_from_filters(ticker, avg_price, filters, side)
    quantity = _safe_quantity_from_filters(price, Decimal(str(target_quantity)), filters)
    return SafeLimitOrderParams(format(price, "f"), format(quantity, "f"))


async def _fetch_json(session: aiohttp.ClientSession, url: str, params: dict[str, str]) -> Any:
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return await response.json()


def _symbol_filters(exchange_info: dict[str, Any]) -> dict[str, dict[str, Any]]:
    symbols = exchange_info.get("symbols") or []
    if not symbols:
        raise ValueError("Exchange info response did not include symbol filters")
    return {item["filterType"]: item for item in symbols[0].get("filters", [])}


def _round_to_step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    if step == 0:
        return value
    return (value / step).to_integral_value(rounding=rounding) * step


def _safe_limit_price_from_filters(
    ticker: dict[str, str],
    avg_price: dict[str, str],
    filters: dict[str, dict[str, Any]],
    side: str,
) -> Decimal:
    price_filter = filters.get("PRICE_FILTER", {})
    tick_size = Decimal(price_filter.get("tickSize", "0.01"))
    min_price = Decimal(price_filter.get("minPrice", "0"))
    max_price = Decimal(price_filter.get("maxPrice", "0"))
    reference_price = Decimal(avg_price["price"])

    low_bound = min_price
    high_bound = max_price if max_price else Decimal("Infinity")
    percent_filter = filters.get("PERCENT_PRICE_BY_SIDE")
    if percent_filter:
        if side == "1":
            low_bound = max(low_bound, reference_price * Decimal(percent_filter["bidMultiplierDown"]))
            high_bound = min(high_bound, reference_price * Decimal(percent_filter["bidMultiplierUp"]))
        elif side == "2":
            low_bound = max(low_bound, reference_price * Decimal(percent_filter["askMultiplierDown"]))
            high_bound = min(high_bound, reference_price * Decimal(percent_filter["askMultiplierUp"]))
    elif "PERCENT_PRICE" in filters:
        percent_filter = filters["PERCENT_PRICE"]
        low_bound = max(low_bound, reference_price * Decimal(percent_filter["multiplierDown"]))
        high_bound = min(high_bound, reference_price * Decimal(percent_filter["multiplierUp"]))

    bid = Decimal(ticker["bidPrice"])
    ask = Decimal(ticker["askPrice"])
    if side == "1":
        price = min(max(bid * Decimal("0.99"), low_bound), high_bound)
        price = _round_to_step(price, tick_size, ROUND_DOWN)
        if price < low_bound:
            price = _round_to_step(low_bound, tick_size, ROUND_UP)
        if price >= ask:
            raise ValueError(f"Could not derive a non-marketable buy price for {ticker}")
        return price
    if side == "2":
        price = max(min(ask * Decimal("1.01"), high_bound), low_bound)
        price = _round_to_step(price, tick_size, ROUND_UP)
        if price > high_bound:
            price = _round_to_step(high_bound, tick_size, ROUND_DOWN)
        if price <= bid:
            raise ValueError(f"Could not derive a non-marketable sell price for {ticker}")
        return price
    raise ValueError(f"Unsupported FIX side: {side}")


def _safe_quantity_from_filters(
    price: Decimal, target_quantity: Decimal, filters: dict[str, dict[str, Any]]
) -> Decimal:
    lot_filter = filters.get("LOT_SIZE", {})
    step_size = Decimal(lot_filter.get("stepSize", "0.00000001"))
    min_qty = Decimal(lot_filter.get("minQty", "0"))
    max_qty = Decimal(lot_filter.get("maxQty", "0"))

    quantity = _round_to_step(target_quantity, step_size, ROUND_DOWN)
    if quantity < min_qty:
        quantity = _round_to_step(min_qty, step_size, ROUND_UP)

    min_notional = Decimal("0")
    if "MIN_NOTIONAL" in filters:
        min_notional = max(min_notional, Decimal(filters["MIN_NOTIONAL"]["minNotional"]))
    if "NOTIONAL" in filters:
        min_notional = max(min_notional, Decimal(filters["NOTIONAL"].get("minNotional", "0")))
        max_notional_raw = filters["NOTIONAL"].get("maxNotional")
        if max_notional_raw and Decimal(max_notional_raw) > 0 and price * quantity > Decimal(max_notional_raw):
            raise ValueError("Target order exceeds max notional filter")

    if min_notional and price * quantity < min_notional:
        quantity = _round_to_step(min_notional / price, step_size, ROUND_UP)

    if max_qty and quantity > max_qty:
        raise ValueError("Target order exceeds max quantity filter")
    if quantity <= 0:
        raise ValueError("Target order quantity is not positive after filter rounding")
    return quantity


async def create_market_data_request(
    session: BinanceFixConnector,
    symbol: str,
    md_req_type: str,
    *,
    prefix: str = "MD",
    depth: str = "1",
    entry_types: list[str] | None = None,
    md_req_id: str | None = None,
    aggregated_book: str = "Y",
) -> FixMessage:
    """Build a MarketDataRequest (V) message."""
    if entry_types is None:
        entry_types = ["0", "1"]

    msg = await session.create_fix_message_with_basic_header("V", "5000")
    msg.append_pair("262", md_req_id or f"{prefix}_{int(time.time())}_{symbol}")
    msg.append_pair("263", md_req_type)
    msg.append_pair("264", depth)
    msg.append_pair("266", aggregated_book)
    msg.append_pair("146", "1")
    msg.append_pair("55", symbol)
    msg.append_pair("267", str(len(entry_types)))
    for et in entry_types:
        msg.append_pair("269", et)
    return msg

#!/usr/bin/env python3
"""
Async Current Messages Limit Rate Example

Queries and displays current session limits for order entry and market data.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import (
    TESTNET_MD_URL,
    TESTNET_OE_URL,
    get_field,
    graceful_logout,
    load_credentials,
    wait_for_logon,
)

API_KEY, PRIVATE_KEY = load_credentials()

from binance_fix_connector_async import (
    BinanceFixConnector,
    create_market_data_session,
    create_order_entry_session,
)

RESOLUTIONS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY"}
LIMIT_TYPES = {"1": "ORDER_LIMIT", "2": "MESSAGE_LIMIT", "3": "SUBSCRIPTION_LIMIT"}


async def show_rendered_limit_session(client: BinanceFixConnector) -> None:
    """Query and display the current session limits."""
    msg = await client.create_fix_message_with_basic_header("XLQ")
    msg.append_pair(6136, "current_message_rate")
    await client.send_message(msg)

    responses = await client.retrieve_messages_until(message_type="XLR")
    for resp in responses:
        if resp.message_type.decode("utf-8") != "XLR":
            continue
        count_raw = resp.get(25003)
        limits = int(count_raw.decode("utf-8")) if count_raw else 0
        client.logger.info("Limits: (%s)", limits)
        for i in range(limits):
            idx = i + 1
            limit_type = (
                get_field(resp, 25004)
                if i == 0
                else (resp.get(25004, idx).decode("utf-8") if resp.get(25004, idx) else None)
            )
            limit_count = resp.get(25005, idx)
            limit_max = resp.get(25006, idx)
            interval = resp.get(25007, idx)
            interval_res = resp.get(25008, idx)

            lt = LIMIT_TYPES.get(limit_type, limit_type) if limit_type else "?"
            lc = limit_count.decode("utf-8") if limit_count else "?"
            lm = limit_max.decode("utf-8") if limit_max else "?"
            interval_str = ""
            if interval:
                res_str = RESOLUTIONS.get(
                    interval_res.decode("utf-8") if interval_res else "",
                    interval_res.decode("utf-8") if interval_res else "",
                )
                interval_str = f" | Interval: {interval.decode('utf-8')} {res_str}"
            client.logger.info("Type: %s | Count: %s | Max: %s%s", lt, lc, lm, interval_str)


async def main():
    for label, create_fn, url in [
        ("Order Entry", create_order_entry_session, TESTNET_OE_URL),
        ("Market Data", create_market_data_session, TESTNET_MD_URL),
    ]:
        client = await create_fn(api_key=API_KEY, private_key=PRIVATE_KEY, endpoint=url)
        try:
            if not await wait_for_logon(client):
                continue
            client.logger.info("--- %s Limits ---", label)
            await show_rendered_limit_session(client)
        finally:
            await graceful_logout(client)


if __name__ == "__main__":
    asyncio.run(main())
